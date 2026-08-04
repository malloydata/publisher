import { createHash } from "crypto";
import lunr from "lunr";
import { logger } from "../../logger";
import {
   EMBEDDING_BATCH_TIMEOUT_MS,
   EMBEDDING_QUERY_TIMEOUT_MS,
   EmbeddingProvider,
} from "../../service/embedding_provider";
// humanizeName and the similarity floor are shared with the package index on
// purpose: a query is humanized the same way whichever index answers it.
import { MIN_SIMILARITY, humanizeName } from "./embedding_index";

/**
 * Ranking layer for `malloy_searchDatabaseSchema`.
 *
 * Publisher already introspects ten dialects (db_utils.getSchemasForConnection /
 * listTablesForSchema) and already serves that over REST. What was missing is the
 * layer between introspection and an agent's plain-English question, which is all
 * this module is: rank a schema's tables against a query, lexically by default and
 * semantically when the operator has explicitly opted in.
 *
 * Two deliberate limits, both recorded in the feature's design notes:
 *
 * - **One entity per table, never per column.** A table's index text carries its
 *   column names, so "customer email" still finds `sales.customers`, but a
 *   warehouse with thousands of tables and tens of columns each would blow any
 *   per-column budget. This is the same trade the hosted pipeline avoids by
 *   having a vector database; we do not have one in-process.
 * - **Identifiers only, never values.** No row value is returned, logged or
 *   embedded. (Type inference over a CSV or JSON file does read the head of
 *   that file, inside the warehouse; only the inferred names and types leave.)
 *   The analogous
 *   non-goal to `get_context`'s "no dimensional_value indexing": an agent that
 *   needs real values runs `execute_query` with a `group_by` over the column,
 *   against a model that shares the connection.
 *
 * The second limit is also what makes the egress story defensible: in every mode,
 * the only thing that can leave the process is names and types.
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
    * (`schema.table`, or `catalog.schema.table` on Snowflake/Trino). This is
    * what goes inside a Malloy `table('...')` call, so it is passed through
    * verbatim and never re-derived.
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
 * Ranked hits, plus how many tables matched BEFORE the limit was applied.
 *
 * `matched` is separate from the caller's `totalAvailable` (every table in the
 * schema) on purpose. Without it, a search over a 500-table schema that matched
 * 60 and returned 20 is indistinguishable from one that matched exactly 20, and
 * the agent stops having never seen the other 40.
 */
export interface RankedResult {
   hits: RankedTable[];
   matched: number;
   /**
    * True when the query carried no searchable content (empty, whitespace, or
    * only lunr operators). Distinguishes "nothing matched" from "nothing was
    * asked", which otherwise look identical to the caller and send an agent off
    * to broaden a query that was never run.
    */
   emptyQuery?: boolean;
}

/**
 * Tables above this cap are not embedded; the schema stays lexical. Mirrors
 * MAX_EMBEDDED_ENTITIES in the package index for the same reason: the first
 * embed of a schema that large is minutes of provider calls.
 */
export const MAX_INDEXED_TABLES = 5_000;

/**
 * Column names folded into a table's index text. A wide fact table can carry
 * hundreds; past a few dozen they stop discriminating between tables and just
 * dilute the vector (and the provider caps input at 1024 chars anyway).
 */
export const MAX_COLUMNS_IN_INDEX_TEXT = 60;

/**
 * Schemas whose vectors are kept. Evicted least-recently-used past this.
 *
 * Without a bound this map is retained for the process lifetime: an operator
 * with 40 searchable schemas of ~3,000 tables at 1536 dims holds on the order
 * of a gigabyte of float arrays that nothing ever frees, in a process that
 * ships OOM guards. A schema searched once and never again would be pinned
 * forever.
 */
export const MAX_CACHED_SCHEMAS = 8;

/**
 * After a provider failure, this schema stays lexical for this long, so a down
 * or misconfigured endpoint costs one timeout per window rather than one per
 * call. Same shape as the package index's cooldown.
 */
export const PROVIDER_FAILURE_COOLDOWN_MS = 60_000;

/**
 * The text indexed for one table: its humanized schema and table name, then its
 * humanized column names. `orders_line_item` becomes "orders line item", so a
 * query for "order line items" matches where a raw-identifier match could not.
 *
 * Types are included but last: they are weak signal individually and would
 * otherwise crowd out column names under the provider's input cap.
 *
 * Exported for the eval harness and specs, which pin the recipe rather than
 * merely its presence.
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
 * A content fingerprint over the tables' identity and shape. Any table added,
 * dropped, renamed, or re-columned changes it, which is what invalidates the
 * embedding cache. Column ORDER is deliberately included: a reordered table is
 * cheap to re-embed and pretending otherwise needs a sort on every call.
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
 * lunr treats several characters as query operators; strip them so a
 * plain-English query never throws and is matched as an OR of its terms.
 * Same treatment malloy_searchDocs applies for the same reason.
 */
export function sanitizeQuery(query: string): string {
   return query.replace(/[~^:*+\-"]/g, " ").trim();
}

/**
 * Rank `tables` against `query` with lunr/BM25 over the same text the semantic
 * path embeds, so the two modes see one recipe. Returns [] for an empty or
 * operator-only query, and never throws: a lunr failure is logged and degrades
 * to no hits rather than failing the tool call.
 */
export function rankLexically(
   tables: SchemaTableEntity[],
   query: string,
   limit: number,
): RankedResult {
   const sanitized = sanitizeQuery(query);
   if (!sanitized || tables.length === 0) return { hits: [], matched: 0 };

   const byResource = new Map(tables.map((t) => [t.resource, t]));
   const index = lunr(function () {
      this.ref("resource");
      this.field("text");
      // Positional metadata is unused here and roughly doubles index size.
      this.metadataWhitelist = [];
      for (const table of tables) {
         this.add({ resource: table.resource, text: tableIndexText(table) });
      }
   });

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
 * Embedded schema vectors, in memory only.
 *
 * Deliberately NOT persisted to the DuckDB store the package index uses. A
 * schema index is cheap to rebuild (one batch call per schema, not per package
 * entity), and persisting a customer's warehouse identifiers to disk is a
 * second, separate disclosure decision that this slice does not take. The cost
 * is one re-embed per process start, per schema actually searched.
 */
const vectorCache = new Map<string, VectorCacheEntry>();
const cooldownUntilMs = new Map<string, number>();

/**
 * Read an entry and mark it most-recently-used. A Map iterates in insertion
 * order, so deleting and re-setting is the whole LRU.
 */
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
 * Drop expired cooldowns.
 *
 * Called when a cooldown is SET, which is the only regime that grows this map:
 * a provider that is down never reaches the success path. Pruning here instead
 * of alongside the cache eviction is the whole point, since a persistently
 * failing provider would otherwise add an entry per distinct schema searched
 * and never remove one.
 */
function pruneCooldowns(): void {
   if (cooldownUntilMs.size <= MAX_CACHED_SCHEMAS * 4) return;
   const now = Date.now();
   for (const [k, until] of cooldownUntilMs) {
      if (until <= now) cooldownUntilMs.delete(k);
   }
}

/** Test seam: drop all cached vectors and cooldowns. */
export function _resetSchemaIndexStateForTests(): void {
   vectorCache.clear();
   cooldownUntilMs.clear();
}

function providerKeyFor(provider: EmbeddingProvider): string {
   return `${provider.model}\x00${provider.dimensions ?? ""}`;
}

/**
 * Rank semantically, or return null to mean "use lexical". Never throws.
 *
 * Returning null rather than an empty list is load-bearing: an empty semantic
 * result is a real answer ("nothing in this schema is about that"), while null
 * means the semantic path could not run at all.
 */
async function tryRankSemantically(args: {
   tables: SchemaTableEntity[];
   query: string;
   limit: number;
   provider: EmbeddingProvider;
   cacheKey: string;
}): Promise<RankedResult | null> {
   const { tables, query, limit, provider, cacheKey } = args;

   if (tables.length === 0) {
      // Nothing to embed. Without this an unknown-but-well-formed schema name
      // (which several dialects answer with [] rather than an error) still
      // stored a cache entry, taking one of the few LRU slots from a real
      // schema, and still spent a billable round trip embedding the query.
      return { hits: [], matched: 0 };
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
         const texts = tables.map(tableIndexText);
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
         [query],
         EMBEDDING_QUERY_TIMEOUT_MS,
      );
      if (!queryVector) throw new Error("Empty query embedding");

      const scored: RankedTable[] = [];
      for (const table of tables) {
         const vector = entry.vectors.get(table.resource);
         if (!vector) continue;
         const score = cosineSimilarity(queryVector, vector);
         // Below the floor is dropped rather than returned as a weak hit, so
         // "nothing here matches" stays a distinguishable answer.
         if (score >= MIN_SIMILARITY) scored.push({ ...table, score });
      }
      scored.sort((a, b) => b.score - a.score);
      return { hits: scored.slice(0, limit), matched: scored.length };
   } catch (error) {
      cooldownUntilMs.set(cacheKey, Date.now() + PROVIDER_FAILURE_COOLDOWN_MS);
      pruneCooldowns();
      // The message can carry a provider body excerpt but never the API key,
      // which EmbeddingProvider keeps to the Authorization header.
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
 * Rank a schema's tables against a plain-English query.
 *
 * Semantic when a provider is supplied (the caller has already checked both the
 * API key and the explicit schema opt-in) and the provider answers; lexical in
 * every other case, including provider failure. The returned mode is reported
 * to the agent so a weak result set is interpretable.
 */
export async function rankTables(args: {
   tables: SchemaTableEntity[];
   query: string;
   limit: number;
   provider: EmbeddingProvider | null;
   cacheKey: string;
}): Promise<RankedResult & { ranking: RankingMode }> {
   const { tables, query, limit, provider, cacheKey } = args;

   // A query with no searchable content is answered the same way in both
   // modes. Checked HERE rather than only inside rankLexically: the embedding
   // provider substitutes a placeholder token for empty input, so a
   // whitespace-only query would otherwise be embedded as "-" and every table
   // scored against it, returning arbitrary tables presented as ranked matches
   // for a query that has none. Same input, same schema, two different answers
   // depending on an operator's env var.
   if (!sanitizeQuery(query)) {
      // "lexical", not "semantic": no embedding call was made, and `ranking`
      // reports what actually happened everywhere else in this file (it
      // degrades to lexical on cooldown, above the table cap, and on provider
      // failure). Claiming "semantic" here would be the one place the field
      // describes configuration rather than behaviour.
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
   return { ...rankLexically(tables, query, limit), ranking: "lexical" };
}
