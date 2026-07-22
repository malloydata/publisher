import { createHash } from "crypto";
import { Mutex } from "async-mutex";
import { logger } from "../../logger";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import type { Package } from "../../service/package";
import {
   EmbeddingProvider,
   EMBEDDING_BATCH_TIMEOUT_MS,
   EMBEDDING_QUERY_TIMEOUT_MS,
   prepareEmbeddingInput,
} from "../../service/embedding_provider";

/**
 * Minimum cosine similarity for a semantic hit. Below this the entity is
 * dropped, so a query about something the package does not model returns
 * an empty result rather than the k least-unrelated entities. Agents are
 * taught to treat an empty result as "not in this package"; unfiltered
 * top-k would destroy that signal. 0.20 matches the hosted retrieval
 * pipeline's min_score; tune against get_context_eval.ts.
 */
export const MIN_SIMILARITY = 0.2;
/**
 * Packages with more entities than this stay lexical: the first embed of
 * such a package would take minutes of provider calls and rate limits.
 * The bundled examples sit around a few hundred entities.
 */
export const MAX_EMBEDDED_ENTITIES = 5_000;
/**
 * After a provider failure the semantic path short-circuits to lexical
 * for this long, so a down or misconfigured endpoint costs one timeout
 * per window, not one per call.
 */
export const PROVIDER_FAILURE_COOLDOWN_MS = 60_000;

/** The subset of the tool's Entity shape the index needs. */
export interface EmbeddableEntity {
   kind: string;
   name: string;
   source: string | undefined;
   modelPath: string;
   doc: string;
}

export interface SemanticHit {
   kind: string;
   name: string;
   source: string | undefined;
   score: number;
}

export type SemanticUnavailableReason =
   | "cooldown"
   | "too-many-entities"
   | "indexing"
   | "error";

export type SemanticSearchResult =
   | { hits: SemanticHit[] }
   | { unavailable: SemanticUnavailableReason; detail?: string };

/**
 * Turn an identifier into the words a person would search with:
 * `dep_delay` -> "dep delay", `totalSales` -> "total sales". This is the
 * heart of closing the lexical token gap; the embedding model then maps
 * "departure delay" near "dep delay" where BM25 could not.
 */
export function humanizeName(name: string): string {
   return name
      .replace(/[_\-./]+/g, " ")
      .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
      .toLowerCase()
      .replace(/\s+/g, " ")
      .trim();
}

/**
 * The text embedded per entity: humanized name plus its `#(doc)` text.
 * Deliberately minimal (no kind, no parent source); the recipe is
 * eval-tunable via get_context_eval.ts.
 */
export function embeddingText(entity: EmbeddableEntity): string {
   const name = humanizeName(entity.name);
   return entity.doc ? `${name}: ${entity.doc}` : name;
}

function contentHash(text: string): string {
   return createHash("sha256").update(text).digest("hex");
}

/** '' in the entity_source column encodes "no parent source". */
function sourceColumn(source: string | undefined): string {
   return source ?? "";
}

function entityRowKey(kind: string, source: string, name: string): string {
   return `${kind}|${source}|${name}`;
}

// Sync state, two layers.
//
// Per package NAME (`syncMeta`): a mutex serializing every read-diff-write
// section (sync AND the heal's purge) so a reload racing an in-flight sync
// cannot tear rows; a `generation` counter bumped by every purge, so a
// purge invalidates the memo of EVERY Package instance, not just the
// caller's (a reloaded instance's `done` memo must not survive a purge
// over a now-empty table); and `lastPurgeAtMs`, which bounds how often the
// heal may purge (a backend serving inconsistent dimensionalities
// otherwise causes an unbounded purge / full-re-embed loop).
//
// Per package INSTANCE (`syncState`, WeakMap): memoizes "this instance is
// synced" (reload swaps the instance, so entity-set staleness clears
// itself, same contract as the tool's lunr cache). A rejected sync
// promise is evicted so one transient failure is not permanent.
// `providerKey` records which model/dims request-config the sync used, so
// switching EMBEDDING_MODEL or EMBEDDING_DIMENSIONS re-syncs promptly.
interface PackageSyncMeta {
   mutex: Mutex;
   generation: number;
   lastPurgeAtMs: number;
}
interface SyncState {
   promise: Promise<void>;
   done: boolean;
   providerKey: string;
   generation: number;
}
const syncState = new WeakMap<Package, SyncState>();
const syncMeta = new Map<string, PackageSyncMeta>();
let providerFailureAtMs = 0;
const oversizeWarned = new Set<string>();

function metaFor(
   environmentName: string,
   packageName: string,
): PackageSyncMeta {
   const key = `${environmentName}\x00${packageName}`;
   let meta = syncMeta.get(key);
   if (!meta) {
      meta = { mutex: new Mutex(), generation: 0, lastPurgeAtMs: 0 };
      syncMeta.set(key, meta);
   }
   return meta;
}

function markProviderFailure(): void {
   providerFailureAtMs = Date.now();
}

function inCooldown(): boolean {
   return Date.now() - providerFailureAtMs < PROVIDER_FAILURE_COOLDOWN_MS;
}

/** Test seam: forget cool-down, purge, and oversize-warning state. */
export function _resetEmbeddingIndexStateForTests(): void {
   providerFailureAtMs = 0;
   oversizeWarned.clear();
   syncMeta.clear();
}

interface ExistingRow {
   entity_kind: string;
   entity_source: string;
   entity_name: string;
   content_hash: string;
   embedding_model: string;
   dims: number;
}

/**
 * Bring the entity_embeddings rows for one package in line with the
 * current entity set: embed new/changed entities (content-hash diff, so
 * unchanged entities never re-embed, across restarts too), upsert them,
 * and delete rows for entities that no longer exist. Runs under the
 * package-name mutex. Returns the package generation the sync ran under.
 * Throws on provider or storage failure; partial writes are safe because
 * the hash diff self-heals on the next sync.
 */
async function syncPackageEmbeddings(
   db: DuckDBConnection,
   provider: EmbeddingProvider,
   environmentName: string,
   packageName: string,
   entities: EmbeddableEntity[],
): Promise<number> {
   const meta = metaFor(environmentName, packageName);
   return meta.mutex.runExclusive(async () => {
      // Captured under the mutex: a purge (which also runs under this
      // mutex) either finished before this sync started or starts after
      // it ends, so the generation cannot move mid-sync.
      const generation = meta.generation;
      const existingRows = await db.all<ExistingRow>(
         `SELECT entity_kind, entity_source, entity_name, content_hash,
                 embedding_model, CAST(dims AS INTEGER) AS dims
          FROM entity_embeddings
          WHERE environment_name = ? AND package_name = ?`,
         [environmentName, packageName],
      );
      const existing = new Map(
         existingRows.map((r) => [
            entityRowKey(r.entity_kind, r.entity_source, r.entity_name),
            r,
         ]),
      );

      const desired = entities.map((entity) => {
         const text = prepareEmbeddingInput(embeddingText(entity));
         return { entity, text, hash: contentHash(text) };
      });
      const desiredKeys = new Set(
         desired.map((d) =>
            entityRowKey(
               d.entity.kind,
               sourceColumn(d.entity.source),
               d.entity.name,
            ),
         ),
      );

      // A row is current when its text hash and model match; a model
      // switch re-embeds in place (upsert) rather than colliding on the
      // primary key. Dimensionality is deliberately NOT part of this
      // check: `dims` stores the ACTUAL response vector length, and a
      // provider that ignores the `dimensions` request parameter (e.g.
      // Ollama) would otherwise mismatch the configured value forever
      // and re-embed the whole package on every instance swap. A real
      // dims change is caught at query time by the empty-search heal.
      const toEmbed = desired.filter((d) => {
         const row = existing.get(
            entityRowKey(
               d.entity.kind,
               sourceColumn(d.entity.source),
               d.entity.name,
            ),
         );
         if (!row) return true;
         if (row.content_hash !== d.hash) return true;
         if (row.embedding_model !== provider.model) return true;
         return false;
      });

      if (toEmbed.length > 0) {
         const vectors = await provider.embedBatch(
            toEmbed.map((d) => d.text),
            EMBEDDING_BATCH_TIMEOUT_MS,
         );
         const now = new Date().toISOString();
         for (let i = 0; i < toEmbed.length; i++) {
            const d = toEmbed[i];
            const vector = vectors[i];
            await db.run(
               `INSERT INTO entity_embeddings (
                  environment_name, package_name, entity_kind, entity_source,
                  entity_name, model_path, content_hash, embedding_model,
                  dims, embedding, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS FLOAT[]), ?)
                ON CONFLICT (environment_name, package_name, entity_kind, entity_source, entity_name)
                DO UPDATE SET
                  model_path = EXCLUDED.model_path,
                  content_hash = EXCLUDED.content_hash,
                  embedding_model = EXCLUDED.embedding_model,
                  dims = EXCLUDED.dims,
                  embedding = EXCLUDED.embedding,
                  updated_at = EXCLUDED.updated_at`,
               [
                  environmentName,
                  packageName,
                  d.entity.kind,
                  sourceColumn(d.entity.source),
                  d.entity.name,
                  d.entity.modelPath,
                  d.hash,
                  provider.model,
                  vector.length,
                  JSON.stringify(vector),
                  now,
               ],
            );
         }
      }

      let deleted = 0;
      for (const [rowKey, row] of existing) {
         if (!desiredKeys.has(rowKey)) {
            await db.run(
               `DELETE FROM entity_embeddings
                WHERE environment_name = ? AND package_name = ?
                  AND entity_kind = ? AND entity_source = ? AND entity_name = ?`,
               [
                  environmentName,
                  packageName,
                  row.entity_kind,
                  row.entity_source,
                  row.entity_name,
               ],
            );
            deleted++;
         }
      }

      logger.debug("[MCP Tool getContext] Synced entity embeddings", {
         environmentName,
         packageName,
         entityCount: entities.length,
         embedded: toEmbed.length,
         deleted,
      });
      return generation;
   });
}

/**
 * Semantic retrieval for tier 4 of malloy_getContext. Returns ranked
 * hits, or a reason the semantic path is unavailable so the caller can
 * fall back to lexical. Never throws.
 *
 * Cold-start contract: the first call for a Package instance kicks off
 * the embedding sync in the background and reports `indexing`, so no
 * call ever waits on a bulk embed; subsequent calls are semantic once the
 * sync lands.
 */
export async function trySemanticSearch(args: {
   db: DuckDBConnection;
   provider: EmbeddingProvider;
   pkg: Package;
   environmentName: string;
   packageName: string;
   entities: EmbeddableEntity[];
   query: string;
   limit: number;
   sourceName?: string;
}): Promise<SemanticSearchResult> {
   const {
      db,
      provider,
      pkg,
      environmentName,
      packageName,
      entities,
      query,
      limit,
      sourceName,
   } = args;

   if (inCooldown()) {
      return { unavailable: "cooldown" };
   }

   if (entities.length > MAX_EMBEDDED_ENTITIES) {
      const key = `${environmentName}\x00${packageName}`;
      if (!oversizeWarned.has(key)) {
         oversizeWarned.add(key);
         logger.warn(
            "[MCP Tool getContext] Package exceeds the semantic index entity cap; using lexical ranking",
            {
               environmentName,
               packageName,
               entityCount: entities.length,
               cap: MAX_EMBEDDED_ENTITIES,
            },
         );
      }
      return { unavailable: "too-many-entities" };
   }

   const providerKey = `${provider.model}\x00${provider.dimensions ?? ""}`;
   const meta = metaFor(environmentName, packageName);
   let state = syncState.get(pkg);
   if (
      !state ||
      state.providerKey !== providerKey ||
      // A purge bumped the generation after this instance synced: its
      // rows are gone, so a `done` memo must not be trusted.
      (state.done && state.generation !== meta.generation)
   ) {
      // No await between the get above and the set below: single-threaded
      // JS therefore guarantees concurrent calls cannot both kick a sync
      // for the same instance. (The per-name mutex still guards the
      // cross-instance reload race.)
      const tracked: SyncState = {
         done: false,
         providerKey,
         generation: meta.generation,
         promise: undefined as unknown as Promise<void>,
      };
      const run = syncPackageEmbeddings(
         db,
         provider,
         environmentName,
         packageName,
         entities,
      ).then(
         (generation) => {
            tracked.generation = generation;
            tracked.done = true;
         },
         (error: unknown) => {
            if (syncState.get(pkg) === tracked) {
               syncState.delete(pkg);
            }
            markProviderFailure();
            logger.warn(
               "[MCP Tool getContext] Embedding sync failed; semantic ranking cooling down",
               {
                  environmentName,
                  packageName,
                  error: error instanceof Error ? error.message : String(error),
               },
            );
         },
      );
      tracked.promise = run;
      syncState.set(pkg, tracked);
      state = tracked;
   }
   if (!state.done) {
      return { unavailable: "indexing" };
   }

   let queryVector: number[];
   try {
      [queryVector] = await provider.embedBatch(
         [query],
         EMBEDDING_QUERY_TIMEOUT_MS,
      );
   } catch (error) {
      markProviderFailure();
      const message = error instanceof Error ? error.message : String(error);
      logger.warn(
         "[MCP Tool getContext] Query embedding failed; falling back to lexical ranking",
         { environmentName, packageName, error: message },
      );
      return { unavailable: "error", detail: message };
   }

   try {
      const rows = await db.all<{
         entity_kind: string;
         entity_source: string;
         entity_name: string;
         score: number;
      }>(
         `SELECT * FROM (
            SELECT entity_kind, entity_source, entity_name,
                   list_cosine_similarity(embedding, CAST(? AS FLOAT[])) AS score
            FROM entity_embeddings
            WHERE environment_name = ? AND package_name = ?
              AND embedding_model = ? AND dims = ?
              ${sourceName !== undefined ? "AND entity_source = ?" : ""}
         )
         WHERE score >= ?
         ORDER BY score DESC, entity_name
         LIMIT ?`,
         [
            JSON.stringify(queryVector),
            environmentName,
            packageName,
            provider.model,
            queryVector.length,
            ...(sourceName !== undefined ? [sourceName] : []),
            MIN_SIMILARITY,
            limit,
         ],
      );
      if (rows.length === 0) {
         // An empty result is legitimate (nothing above the similarity
         // floor) ONLY when compatible rows exist. If the package has
         // rows but none at this model + dimensionality, the endpoint
         // behind the model name changed what it returns (the sync diff
         // cannot see a dims change, since the row hashes still match).
         // Purge and re-sync rather than serving permanently empty
         // "semantic" results. The whole check-and-purge runs under the
         // package-name mutex so it cannot interleave with a sync, and
         // the generation bump invalidates every instance's memo, not
         // just this caller's.
         const outcome: "none" | "purged" | "backoff" =
            await meta.mutex.runExclusive(async () => {
               const compatible = await db.get<{ n: number }>(
                  `SELECT CAST(COUNT(*) AS INTEGER) AS n FROM entity_embeddings
                   WHERE environment_name = ? AND package_name = ?
                     AND embedding_model = ? AND dims = ?`,
                  [
                     environmentName,
                     packageName,
                     provider.model,
                     queryVector.length,
                  ],
               );
               if ((compatible?.n ?? 0) > 0) return "none";
               const total = await db.get<{ n: number }>(
                  `SELECT CAST(COUNT(*) AS INTEGER) AS n FROM entity_embeddings
                   WHERE environment_name = ? AND package_name = ?`,
                  [environmentName, packageName],
               );
               if ((total?.n ?? 0) === 0) return "none";
               // Backoff: at most one purge per cool-down window. A
               // second mismatch inside the window means the endpoint is
               // serving inconsistent dimensionalities (e.g. mid-upgrade
               // replicas); purging again would loop full re-embeds
               // indefinitely, so treat it as provider instability.
               const now = Date.now();
               if (now - meta.lastPurgeAtMs < PROVIDER_FAILURE_COOLDOWN_MS) {
                  markProviderFailure();
                  logger.warn(
                     "[MCP Tool getContext] Repeated embedding dimensionality mismatch; the endpoint looks inconsistent, cooling down",
                     {
                        environmentName,
                        packageName,
                        model: provider.model,
                        queryDims: queryVector.length,
                     },
                  );
                  return "backoff";
               }
               logger.warn(
                  "[MCP Tool getContext] Cached embeddings do not match the provider's current model/dimensions; purging and re-syncing",
                  {
                     environmentName,
                     packageName,
                     model: provider.model,
                     queryDims: queryVector.length,
                  },
               );
               await db.run(
                  `DELETE FROM entity_embeddings
                   WHERE environment_name = ? AND package_name = ?`,
                  [environmentName, packageName],
               );
               meta.lastPurgeAtMs = now;
               meta.generation++;
               return "purged";
            });
         if (outcome === "purged") {
            syncState.delete(pkg);
            return { unavailable: "indexing" };
         }
         if (outcome === "backoff") {
            return { unavailable: "cooldown" };
         }
      }

      return {
         hits: rows.map((row) => ({
            kind: row.entity_kind,
            source: row.entity_source === "" ? undefined : row.entity_source,
            name: row.entity_name,
            score: row.score,
         })),
      };
   } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      logger.warn(
         "[MCP Tool getContext] Semantic search failed; falling back to lexical ranking",
         { environmentName, packageName, error: message },
      );
      return { unavailable: "error", detail: message };
   }
}
