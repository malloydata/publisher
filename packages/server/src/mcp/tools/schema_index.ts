// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createHash } from "crypto";
import type { EmbeddingModel } from "../../providers/types";
import lunr from "lunr";
import { logger } from "../../logger";
import {
   EMBEDDING_BATCH_TIMEOUT_MS,
   EMBEDDING_QUERY_TIMEOUT_MS,
} from "../../service/embedding_provider";
// Shared with the package index so a query is humanized the same way
// whichever index answers it.
import { humanizeName } from "./embedding_index";
import {
   PROVIDER_FAILURE_COOLDOWN_MS,
   providerKeyFor,
   sanitizeLunrQuery,
} from "./retrieval_shared";

/**
 * Ranking layer for `search_database_schema`: rank a schema's introspected
 * tables against a plain-English query, lexically by default and semantically
 * when the operator has opted in.
 *
 * Two deliberate limits:
 *
 * - **One entity per table, never per column.** A table's index text carries
 *   its column names, so "customer email" still finds `sales.customers`, but
 *   a per-column index would not fit a large warehouse without a vector
 *   database, which we do not have in-process.
 * - **Identifiers only, never values.** No row value is returned, logged or
 *   embedded (type inference over a CSV or JSON file reads its head inside
 *   the warehouse; only names and types leave). An agent that needs values
 *   runs `execute_query` against a model sharing the connection.
 *
 * So in every mode the only thing that can leave the process is names and
 * types.
 */

/** A column as introspection returns it. `type` is absent on some dialects. */
export interface SchemaColumn {
   name: string;
   type?: string;
}

/** One table, the unit this module indexes and ranks. */
export interface SchemaTableEntity {
   connectionName: string;
   schemaName: string;
   /** Bare table name, the last segment of `resource`. */
   tableName: string;
   /**
    * Fully-qualified dotted path exactly as introspection produced it
    * (`schema.table`, or `catalog.schema.table` on Snowflake/Trino). It goes
    * inside a Malloy `table('...')` call, so it is never re-derived.
    */
   resource: string;
   columns: SchemaColumn[];
}

export interface RankedTable extends SchemaTableEntity {
   /** Lexical BM25 score, or cosine similarity when ranked semantically. */
   score: number;
}

export type RankingMode = "lexical" | "semantic";

/**
 * Ranked hits, plus how many tables matched BEFORE the limit was applied, so
 * an agent can tell "matched 60, returned 20" from "matched exactly 20".
 */
export interface RankedResult {
   hits: RankedTable[];
   matched: number;
   /**
    * True when the query carried no searchable content (empty, whitespace, or
    * only lunr operators), so an agent does not broaden a query that never ran.
    */
   emptyQuery?: boolean;
}

/**
 * Tables above this cap are not embedded; the schema stays lexical. Mirrors
 * the package index's entity cap: the first embed of a schema that large is
 * minutes of provider calls.
 */
export const MAX_INDEXED_TABLES = 5_000;

/**
 * Column names folded into a table's index text. Past a few dozen they stop
 * discriminating between tables and just dilute the vector.
 */
export const MAX_COLUMNS_IN_INDEX_TEXT = 60;

/**
 * Schemas whose vectors are kept, evicted least-recently-used past this.
 * Unbounded, a handful of large schemas would pin around a gigabyte of float
 * arrays for the process lifetime.
 */
export const MAX_CACHED_SCHEMAS = 8;

/**
 * The text indexed for one table: its humanized schema and table name, then
 * its humanized column names (`orders_line_item` -> "orders line item"), then
 * types last, since they are weak signal and would crowd out column names
 * under the provider's input cap.
 */
export function tableIndexText(entity: SchemaTableEntity): string {
   const name = humanizeName(entity.tableName) || entity.tableName;
   const schema = humanizeName(entity.schemaName) || entity.schemaName;
   const columns = entity.columns.slice(0, MAX_COLUMNS_IN_INDEX_TEXT);
   const columnNames = columns
      .map((c) => humanizeName(c.name) || c.name)
      .filter(Boolean)
      .join(", ");
   const types = Array.from(
      new Set(
         columns.map((c) => c.type).filter((t): t is string => Boolean(t)),
      ),
   ).join(" ");
   return [schema, name, columnNames, types].filter(Boolean).join(" ");
}

/**
 * A content fingerprint over the tables' identity and shape; any table added,
 * dropped, renamed, or re-columned invalidates the embedding cache. Column
 * order is included: a reordered table is cheap to re-embed, and ignoring
 * order would need a sort on every call.
 */
export function schemaFingerprint(tables: SchemaTableEntity[]): string {
   const canonical = tables
      .map(
         (t) =>
            `${t.resource}\x00${t.columns.map((c) => `${c.name}:${c.type ?? ""}`).join(",")}`,
      )
      .sort()
      .join("");
   return createHash("sha256").update(canonical).digest("hex");
}

/**
 * Rank `tables` against `query` with lunr/BM25 over the same text the semantic
 * path embeds. Returns [] for an empty or operator-only query, and never
 * throws: a lunr failure is logged and degrades to no hits.
 */
export function rankLexically(
   tables: SchemaTableEntity[],
   query: string,
   limit: number,
   cacheKey?: string,
): RankedResult {
   const sanitized = sanitizeLunrQuery(query);
   if (!sanitized || tables.length === 0) return { hits: [], matched: 0 };

   const fingerprint = cacheKey ? schemaFingerprint(tables) : "";
   let cached = cacheKey ? lexicalCache.get(cacheKey) : undefined;
   if (cached && cached.fingerprint !== fingerprint) cached = undefined;

   const byResource =
      cached?.byResource ?? new Map(tables.map((t) => [t.resource, t]));
   const index =
      cached?.index ??
      lunr(function () {
         this.ref("resource");
         this.field("text");
         // Positional metadata is unused here and roughly doubles index size.
         this.metadataWhitelist = [];
         for (const table of tables) {
            this.add({ resource: table.resource, text: tableIndexText(table) });
         }
      });
   if (cacheKey && !cached) {
      lexicalCache.delete(cacheKey);
      lexicalCache.set(cacheKey, { index, byResource, fingerprint });
      while (lexicalCache.size > MAX_CACHED_SCHEMAS) {
         const oldest = lexicalCache.keys().next();
         if (oldest.done) break;
         lexicalCache.delete(oldest.value);
      }
   } else if (cacheKey && cached) {
      // Mark most-recently-used.
      lexicalCache.delete(cacheKey);
      lexicalCache.set(cacheKey, cached);
   }

   let hits: lunr.Index.Result[];
   try {
      hits = index.search(sanitized);
   } catch (error) {
      logger.warn("[MCP Tool searchDatabaseSchema] lunr search failed", {
         error: error instanceof Error ? error.message : String(error),
      });
      return { hits: [], matched: 0 };
   }

   return {
      hits: hits
         .slice(0, limit)
         .map((hit) => {
            const table = byResource.get(hit.ref);
            return table ? { ...table, score: hit.score } : undefined;
         })
         .filter((t): t is RankedTable => t !== undefined),
      matched: hits.length,
   };
}

function cosineSimilarity(a: number[], b: number[]): number {
   if (a.length !== b.length || a.length === 0) return 0;
   let dot = 0;
   let magA = 0;
   let magB = 0;
   for (let i = 0; i < a.length; i++) {
      dot += a[i] * b[i];
      magA += a[i] * a[i];
      magB += b[i] * b[i];
   }
   if (magA === 0 || magB === 0) return 0;
   return dot / (Math.sqrt(magA) * Math.sqrt(magB));
}

interface VectorCacheEntry {
   /** Keyed by table resource. */
   vectors: Map<string, number[]>;
   fingerprint: string;
   providerKey: string;
}

/**
 * Embedded schema vectors, in memory only. Deliberately NOT persisted:
 * rebuilding is one batch call per schema, and writing a customer's warehouse
 * identifiers to disk is a separate disclosure decision.
 */
const vectorCache = new Map<string, VectorCacheEntry>();

interface LexicalCacheEntry {
   index: lunr.Index;
   byResource: Map<string, SchemaTableEntity>;
   fingerprint: string;
}

/**
 * Built lunr indexes, keyed and bounded like the vector cache. This is the
 * DEFAULT path, and the largest schemas (above MAX_INDEXED_TABLES) land on it.
 * Rebuilding per call blocks the event loop (roughly 180ms for 5,000 tables,
 * growing linearly). Keyed by fingerprint so a changed schema rebuilds.
 */
const lexicalCache = new Map<string, LexicalCacheEntry>();
const cooldownUntilMs = new Map<string, number>();

/** Read an entry and mark it most-recently-used (Map insertion order is the LRU). */
function touchCacheEntry(key: string): VectorCacheEntry | undefined {
   const entry = vectorCache.get(key);
   if (entry) {
      vectorCache.delete(key);
      vectorCache.set(key, entry);
   }
   return entry;
}

/** Store an entry, evicting the least-recently-used until within the cap. */
function storeCacheEntry(key: string, entry: VectorCacheEntry): void {
   vectorCache.delete(key);
   vectorCache.set(key, entry);
   while (vectorCache.size > MAX_CACHED_SCHEMAS) {
      const oldest = vectorCache.keys().next();
      if (oldest.done) break;
      vectorCache.delete(oldest.value);
   }
}

/**
 * Drop expired cooldowns. Called when a cooldown is SET, the only regime that
 * grows this map: a persistently failing provider never reaches the success
 * path, so pruning there would let it add an entry per schema forever.
 */
function pruneCooldowns(): void {
   if (cooldownUntilMs.size <= MAX_CACHED_SCHEMAS * 4) return;
   const now = Date.now();
   for (const [k, until] of cooldownUntilMs) {
      if (until <= now) cooldownUntilMs.delete(k);
   }
}

/** Test seam: drop all cached state. */
export function _resetSchemaIndexStateForTests(): void {
   vectorCache.clear();
   lexicalCache.clear();
   cooldownUntilMs.clear();
}

/**
 * Rank semantically, or return null to mean "use lexical". Never throws. An
 * empty result is a real semantic answer; null means the path could not run.
 */
async function tryRankSemantically(args: {
   tables: SchemaTableEntity[];
   query: string;
   limit: number;
   provider: EmbeddingModel;
   cacheKey: string;
}): Promise<RankedResult | null> {
   const { tables, query, limit, provider, cacheKey } = args;

   if (tables.length === 0) {
      // Nothing to embed. An unknown schema name (several dialects answer
      // [] rather than an error) must not take an LRU slot or spend a
      // billable query embed. null, not an empty result, so `ranking` does
      // not claim "semantic" when no embedding ran.
      return null;
   }

   if (tables.length > MAX_INDEXED_TABLES) {
      logger.warn(
         "[MCP Tool searchDatabaseSchema] Schema exceeds the semantic index cap; ranking lexically",
         { tableCount: tables.length, cap: MAX_INDEXED_TABLES },
      );
      return null;
   }

   const until = cooldownUntilMs.get(cacheKey);
   if (until !== undefined && Date.now() < until) return null;

   const fingerprint = schemaFingerprint(tables);
   const providerKey = providerKeyFor(provider);
   let entry = touchCacheEntry(cacheKey);

   try {
      if (
         !entry ||
         entry.fingerprint !== fingerprint ||
         entry.providerKey !== providerKey
      ) {
         const texts = tables.map(
            (t) => (provider.documentPrefix ?? "") + tableIndexText(t),
         );
         const vectors = await provider.embedBatch(
            texts,
            EMBEDDING_BATCH_TIMEOUT_MS,
         );
         if (vectors.length !== tables.length) {
            throw new Error(
               `Embedding provider returned ${vectors.length} vectors for ${tables.length} inputs`,
            );
         }
         entry = {
            vectors: new Map(
               tables.map((t, i) => [t.resource, vectors[i]] as const),
            ),
            fingerprint,
            providerKey,
         };
         storeCacheEntry(cacheKey, entry);
      }

      const [queryVector] = await provider.embedBatch(
         [(provider.queryPrefix ?? "") + query],
         EMBEDDING_QUERY_TIMEOUT_MS,
      );
      if (!queryVector) throw new Error("Empty query embedding");

      const scored: RankedTable[] = [];
      for (const table of tables) {
         const vector = entry.vectors.get(table.resource);
         if (!vector) continue;
         const score = cosineSimilarity(queryVector, vector);
         // Below the floor is dropped, so "nothing here matches" stays a
         // distinguishable answer.
         if (score >= provider.minSimilarity) scored.push({ ...table, score });
      }
      scored.sort((a, b) => b.score - a.score);
      return { hits: scored.slice(0, limit), matched: scored.length };
   } catch (error) {
      cooldownUntilMs.set(cacheKey, Date.now() + PROVIDER_FAILURE_COOLDOWN_MS);
      pruneCooldowns();
      // The message can carry a provider body excerpt but never the API key,
      // which stays in the Authorization header.
      logger.warn(
         "[MCP Tool searchDatabaseSchema] Embedding failed; ranking lexically",
         {
            cacheKey,
            error: error instanceof Error ? error.message : String(error),
         },
      );
      return null;
   }
}

/**
 * Rank a schema's tables against a plain-English query: semantic when a
 * provider is supplied (the caller checked the key and the schema opt-in) and
 * answers, lexical otherwise. The mode is reported so a weak result set is
 * interpretable.
 */
export async function rankTables(args: {
   tables: SchemaTableEntity[];
   query: string;
   limit: number;
   provider: EmbeddingModel | null;
   cacheKey: string;
}): Promise<RankedResult & { ranking: RankingMode }> {
   const { tables, query, limit, provider, cacheKey } = args;

   // Checked here, not only in rankLexically: the embedding provider
   // substitutes a placeholder for empty input, so a whitespace-only query
   // would be embedded and return arbitrary tables as ranked matches.
   if (!sanitizeLunrQuery(query)) {
      // "lexical": `ranking` reports what happened, and no embedding ran.
      return { hits: [], matched: 0, ranking: "lexical", emptyQuery: true };
   }

   if (provider) {
      const semantic = await tryRankSemantically({
         tables,
         query,
         limit,
         provider,
         cacheKey,
      });
      if (semantic !== null) return { ...semantic, ranking: "semantic" };
   }
   return {
      ...rankLexically(tables, query, limit, cacheKey),
      ranking: "lexical",
   };
}
