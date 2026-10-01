// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createHash } from "node:crypto";
import { logger } from "../logger";
import type { EmbeddingProvider } from "../service/embedding_provider";
import type { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";
import { globToRegExp } from "./index_annotation";
import type { RetrievalConfig } from "./retrieval_config";

/**
 * Dimensional value indexing: the distinct values of chosen dimensions,
 * stored and embedded so that "Premium" or "NYC" in a question can be found as
 * a value of `customers.tier` or `stores.city` without a query to discover it.
 *
 * Three rules keep it small and safe.
 *
 * - **Capped, never streamed.** A dimension keeps its top N values by row
 *   count (`maxValuesPerDimension`, or the author's own `#(index n=..)` if
 *   smaller), the package keeps `maxValuesPerPackage`, and the whole sync
 *   stops at `indexing.deadlineMs`. A dimension with more values than its cap
 *   is marked truncated, so a response can say a value may be missing from the
 *   index rather than from the data. The service embeds every distinct value;
 *   this is the deliberate difference that keeps a local sync to minutes.
 * - **Never a gated source.** The index is package-wide and answers every
 *   caller from the same rows, while `#(access_filter)`, `#(authorize)` and a
 *   required `#(filter)` decide, per caller, what rows may be seen. Indexing a
 *   gated source's values would show one caller another's data. There is no
 *   override: such a source is skipped, and its values are found by querying
 *   it, where the gate applies.
 * - **Useful without embeddings.** Values are stored as text even with no
 *   provider, and the lexical arm (exact, prefix, contains, near-match) needs
 *   nothing else.
 */

const SEP = "\u0000";
/** The count column's alias in the fetch query; any dimension name is quoted around it. */
const WEIGHT = "value_weight__";
/** Retry a dimension that failed this long after the failure. */
const RETRY_AFTER_FAILURE_MS = 600_000;
/**
 * Nearest-spelling floor for the lexical arm (Jaro-Winkler), and the width the
 * scores above it are squeezed into. Exact is 1, a prefix .95, a substring .9;
 * a near spelling maps [floor, 1] onto [floor, floor + span] = [.85, .89], so
 * it can never outrank a text match, whatever its similarity.
 */
const NEAR_MATCH_FLOOR = 0.85;
const NEAR_MATCH_SPAN = 0.04 / (1 - NEAR_MATCH_FLOOR);
const EMBED_TIMEOUT_MS = 30_000;

export interface ValueDimension {
   source: string;
   dimension: string;
   modelPath: string;
   /** Most values to keep: the smaller of the author's `n` and the config cap. */
   cap: number;
}

/** What discovery needs to know about an entity. */
export interface ValueCandidate {
   kind: string;
   name: string;
   source?: string;
   modelPath: string;
   dataType?: string;
   joinPath?: string;
   aliasOf?: string;
   indexValues?: { n?: number };
}

/**
 * The dimensions to index under `cfg`.
 *
 * `annotated` takes those carrying `#(index)`; `auto` takes every string
 * dimension. `include` (globs of `source.dimension`) narrows `auto` and adds to
 * `annotated`; `exclude` removes from both. Only a source's own dimensions
 * qualify: a joined field is another source's, indexed under that source. One
 * entry per (source, dimension) whatever number of files resolve it, the first
 * path winning.
 */
export function discoverValueDimensions(
   entities: readonly ValueCandidate[],
   cfg: RetrievalConfig["dimensionalValues"],
   isGated: (modelPath: string, source: string) => boolean,
): ValueDimension[] {
   if (cfg.mode === "off") return [];
   const include = cfg.include.map(globToRegExp);
   const exclude = cfg.exclude.map(globToRegExp);
   const seen = new Set<string>();
   const out: ValueDimension[] = [];
   for (const e of entities) {
      if (e.kind !== "dimension" || e.joinPath || e.aliasOf || !e.source)
         continue;
      const label = `${e.source}.${e.name}`;
      const tagged = e.indexValues !== undefined;
      const isString = e.dataType === "string";
      const included = include.some((re) => re.test(label));
      const wanted =
         cfg.mode === "annotated"
            ? tagged || (included && isString)
            : isString && (include.length === 0 || included);
      if (!wanted) continue;
      if (exclude.some((re) => re.test(label))) continue;
      // Fail closed: see the module docblock.
      if (isGated(e.modelPath, e.source)) continue;
      const key = [e.source, e.name].join(SEP);
      if (seen.has(key)) continue;
      seen.add(key);
      out.push({
         source: e.source,
         dimension: e.name,
         modelPath: e.modelPath,
         cap: Math.min(e.indexValues?.n ?? Infinity, cfg.maxValuesPerDimension),
      });
   }
   return out;
}

// ---------------------------------------------------------------------------
// Fetching
// ---------------------------------------------------------------------------

/** Runs a Malloy query against a model file and returns its rows. */
export type RunQuery = (
   modelPath: string,
   query: string,
   signal: AbortSignal,
) => Promise<ReadonlyArray<Record<string, unknown>>>;

/** Backtick-quote a Malloy identifier, as the model does for a name it interpolates. */
export function quoteIdentifier(name: string): string {
   return "`" + name.replace(/\\/g, "\\\\").replace(/`/g, "\\`") + "`";
}

/** The query that reads a dimension's most common values, one over the cap. */
export function valuesQuery(
   source: string,
   dimension: string,
   limit: number,
): string {
   const s = quoteIdentifier(source);
   const d = quoteIdentifier(dimension);
   return `run: ${s} -> { group_by: ${d}; aggregate: ${quoteIdentifier(WEIGHT)} is count(); order_by: ${quoteIdentifier(WEIGHT)} desc; limit: ${limit} }`;
}

export interface FetchedValues {
   values: Array<{ value: string; weight: number }>;
   /** More distinct values exist than were kept. */
   truncated: boolean;
   /** How many the query saw (the cap, plus one if it was cut). */
   distinctSeen: number;
}

function asValue(v: unknown): string | null {
   if (typeof v === "string") return v;
   if (
      typeof v === "number" ||
      typeof v === "boolean" ||
      typeof v === "bigint"
   ) {
      return String(v);
   }
   return null; // null, dates and objects are not text a person would type
}

export async function fetchDimensionValues(
   run: RunQuery,
   dim: ValueDimension,
   cap: number,
   maxValueChars: number,
   timeoutMs: number,
): Promise<FetchedValues> {
   const rows = await run(
      dim.modelPath,
      valuesQuery(dim.source, dim.dimension, cap + 1),
      AbortSignal.timeout(timeoutMs),
   );
   const truncated = rows.length > cap;
   const values: FetchedValues["values"] = [];
   for (const row of rows.slice(0, cap)) {
      const value = asValue(row[dim.dimension]);
      if (value === null) continue;
      const trimmed = value.trim();
      if (trimmed === "" || trimmed.length > maxValueChars) continue;
      values.push({ value: trimmed, weight: Number(row[WEIGHT] ?? 0) });
   }
   return { values, truncated, distinctSeen: rows.length };
}

// ---------------------------------------------------------------------------
// Storage and sync
// ---------------------------------------------------------------------------

export type ValueIndexState = "building" | "ready" | "partial" | "failed";

export interface ValueIndexStatus {
   status: ValueIndexState;
   /** Dimensions selected for indexing. */
   dimensions: number;
   /** Values kept across all of them. */
   values: number;
   /** Dimensions whose values were cut at a cap. */
   truncated: number;
   /** Dimensions that could not be read, or were left for a later run. */
   failed: number;
   updatedAt?: string;
}

const statuses = new Map<string, ValueIndexStatus>();
let kicked = new WeakMap<object, { at: number; done: boolean }>();
const running = new Map<string, Promise<void>>();

const pkgKey = (env: string, pkg: string) => `${env}${SEP}${pkg}`;

export function getValueIndexStatus(
   environmentName: string,
   packageName: string,
): ValueIndexStatus | undefined {
   return statuses.get(pkgKey(environmentName, packageName));
}

export function _resetValueIndexStateForTests(): void {
   statuses.clear();
   running.clear();
   kicked = new WeakMap();
}

const renderValue = (
   template: string,
   d: ValueDimension,
   value: string,
): string =>
   template
      .replaceAll("{value}", () => value)
      .replaceAll("{dimension}", () => d.dimension.replace(/_/g, " "))
      .replaceAll("{source}", () => d.source.replace(/_/g, " "));

const hashOf = (text: string): string =>
   createHash("sha256").update(text).digest("hex");

export interface ValueSyncArgs {
   db: DuckDBConnection;
   /** Null still stores the text, so the lexical arm works. */
   provider: EmbeddingProvider | null;
   environmentName: string;
   packageName: string;
   dims: ValueDimension[];
   run: RunQuery;
   config: RetrievalConfig;
   /** Rows the package may still add across all indexes (values share the item budget). */
   itemBudget: number;
   now?: () => number;
}

interface StoredRow {
   value: string;
   content_hash: string;
   embedding_model: string | null;
   has_embedding: boolean;
}

async function syncOne(
   args: ValueSyncArgs,
   dim: ValueDimension,
   fetched: FetchedValues,
): Promise<void> {
   const { db, provider, environmentName, packageName, config } = args;
   const cfg = config.dimensionalValues;
   const existing = new Map(
      (
         await db.all<StoredRow>(
            `SELECT value, content_hash, embedding_model,
                    embedding IS NOT NULL AS has_embedding
             FROM dimension_values
             WHERE environment_name = ? AND package_name = ?
               AND source_name = ? AND dimension_name = ?`,
            [environmentName, packageName, dim.source, dim.dimension],
         )
      ).map((r) => [r.value, r]),
   );
   const model = provider?.rowModel ?? null;
   const desired = fetched.values.map((v) => {
      const text = renderValue(cfg.template, dim, v.value);
      return { ...v, text, hash: hashOf(text) };
   });

   // A value needs a vector when it is new, its text changed, it was embedded
   // under another model or setup, or it has none and a provider now exists.
   const toEmbed =
      cfg.embed && provider
         ? desired.filter((d) => {
              const row = existing.get(d.value);
              return (
                 !row ||
                 row.content_hash !== d.hash ||
                 row.embedding_model !== model ||
                 !row.has_embedding
              );
           })
         : [];

   const vectors = new Map<string, number[]>();
   if (toEmbed.length > 0 && provider) {
      try {
         const out = await provider.embedBatch(
            toEmbed.map((d) => d.text),
            EMBED_TIMEOUT_MS,
            "document",
         );
         toEmbed.forEach((d, i) => vectors.set(d.value, out[i]));
      } catch (error) {
         // Keep the text; the lexical arm still finds it, and the next run
         // sees the missing vector and tries again.
         logger.warn(
            "[MCP Tool getContext] Embedding dimensional values failed; keeping them as text",
            {
               environmentName,
               packageName,
               source: dim.source,
               dimension: dim.dimension,
               error: error instanceof Error ? error.message : String(error),
            },
         );
      }
   }

   const now = new Date().toISOString();
   for (const d of desired) {
      const vector = vectors.get(d.value);
      const keep = existing.get(d.value);
      const reuse = !vector && keep && keep.content_hash === d.hash;
      await db.run(
         `INSERT INTO dimension_values (
            environment_name, package_name, source_name, dimension_name, value,
            weight, content_hash, embedding_model, dims, embedding, embedded_text, updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ${vector ? "CAST(? AS FLOAT[])" : "NULL"}, ?, ?)
          ON CONFLICT (environment_name, package_name, source_name, dimension_name, value)
          DO UPDATE SET
            weight = EXCLUDED.weight,
            content_hash = EXCLUDED.content_hash,
            ${
               vector
                  ? `embedding_model = EXCLUDED.embedding_model, dims = EXCLUDED.dims,
            embedding = EXCLUDED.embedding, embedded_text = EXCLUDED.embedded_text,`
                  : reuse
                    ? ""
                    : `embedding_model = NULL, dims = NULL, embedding = NULL, embedded_text = EXCLUDED.embedded_text,`
            }
            updated_at = EXCLUDED.updated_at`,
         [
            environmentName,
            packageName,
            dim.source,
            dim.dimension,
            d.value,
            d.weight,
            d.hash,
            vector ? model : null,
            vector ? vector.length : null,
            ...(vector ? [JSON.stringify(vector)] : []),
            d.text,
            now,
         ],
      );
   }
   const keepValues = new Set(desired.map((d) => d.value));
   for (const value of existing.keys()) {
      if (keepValues.has(value)) continue;
      await db.run(
         `DELETE FROM dimension_values
          WHERE environment_name = ? AND package_name = ?
            AND source_name = ? AND dimension_name = ? AND value = ?`,
         [environmentName, packageName, dim.source, dim.dimension, value],
      );
   }
}

async function writeState(
   args: ValueSyncArgs,
   dim: ValueDimension,
   state: {
      distinct: number;
      kept: number;
      truncated: boolean;
      status: string;
      error?: string;
   },
): Promise<void> {
   await args.db.run(
      `INSERT INTO dimension_value_state (
         environment_name, package_name, source_name, dimension_name,
         distinct_seen, kept, truncated, status, last_error, fetched_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT (environment_name, package_name, source_name, dimension_name)
       DO UPDATE SET
         distinct_seen = EXCLUDED.distinct_seen, kept = EXCLUDED.kept,
         truncated = EXCLUDED.truncated, status = EXCLUDED.status,
         last_error = EXCLUDED.last_error, fetched_at = EXCLUDED.fetched_at`,
      [
         args.environmentName,
         args.packageName,
         dim.source,
         dim.dimension,
         state.distinct,
         state.kept,
         state.truncated,
         state.status,
         state.error?.slice(0, 500) ?? null,
         new Date().toISOString(),
      ],
   );
}

/**
 * Fetch, cap, store and embed the values of `args.dims`. Never throws: a
 * dimension that fails is recorded and left for the next run.
 */
export async function syncDimensionValues(
   args: ValueSyncArgs,
): Promise<ValueIndexStatus> {
   const { db, environmentName, packageName, config, dims } = args;
   const cfg = config.dimensionalValues;
   const clock = args.now ?? Date.now;
   const deadline = clock() + config.indexing.deadlineMs;
   const key = pkgKey(environmentName, packageName);
   const set = (s: Omit<ValueIndexStatus, "updatedAt">) => {
      statuses.set(key, { ...s, updatedAt: new Date().toISOString() });
      return statuses.get(key)!;
   };
   set({
      status: "building",
      dimensions: dims.length,
      values: 0,
      truncated: 0,
      failed: 0,
   });

   const selected = new Set(dims.map((d) => [d.source, d.dimension].join(SEP)));
   let kept = 0;
   let truncatedDims = 0;
   let failed = 0;
   try {
      const state = new Map(
         (
            await db.all<{
               source_name: string;
               dimension_name: string;
               kept: number;
               truncated: boolean;
               status: string;
               fetched_ms: number;
            }>(
               `SELECT source_name, dimension_name, CAST(kept AS INTEGER) AS kept,
                       truncated, status,
                       CAST(epoch_ms(fetched_at) AS DOUBLE) AS fetched_ms
                FROM dimension_value_state
                WHERE environment_name = ? AND package_name = ?`,
               [environmentName, packageName],
            )
         ).map((r) => [
            [r.source_name, r.dimension_name].join(SEP),
            { ...r, age_ms: clock() - r.fetched_ms },
         ]),
      );

      // A dimension no longer selected (the tag was removed, or the config
      // narrowed) leaves nothing behind to be searched.
      for (const [k, row] of state) {
         if (selected.has(k)) continue;
         await db.run(
            `DELETE FROM dimension_values WHERE environment_name = ? AND package_name = ? AND source_name = ? AND dimension_name = ?`,
            [environmentName, packageName, row.source_name, row.dimension_name],
         );
         await db.run(
            `DELETE FROM dimension_value_state WHERE environment_name = ? AND package_name = ? AND source_name = ? AND dimension_name = ?`,
            [environmentName, packageName, row.source_name, row.dimension_name],
         );
      }

      for (const dim of dims) {
         const k = [dim.source, dim.dimension].join(SEP);
         const prior = state.get(k);
         const fresh =
            prior &&
            prior.status === "ok" &&
            prior.age_ms < cfg.refreshMinutes * 60_000;
         const failedRecently =
            prior &&
            prior.status === "failed" &&
            prior.age_ms < RETRY_AFTER_FAILURE_MS;

         const remaining = Math.min(
            cfg.maxValuesPerPackage - kept,
            args.itemBudget - kept,
         );
         const cap = Math.min(dim.cap, remaining);
         if (cap <= 0 || clock() >= deadline) {
            failed++; // left for a later run
            continue;
         }
         if (failedRecently) {
            failed++;
            continue;
         }
         try {
            if (fresh) {
               // Values are current; still give any without a vector another
               // chance, since a provider may have come up since.
               const stored = await db.all<{ value: string; weight: number }>(
                  `SELECT value, CAST(weight AS DOUBLE) AS weight FROM dimension_values
                   WHERE environment_name = ? AND package_name = ? AND source_name = ? AND dimension_name = ?
                   ORDER BY weight DESC LIMIT ?`,
                  [
                     environmentName,
                     packageName,
                     dim.source,
                     dim.dimension,
                     cap,
                  ],
               );
               await syncOne(args, dim, {
                  values: stored.map((r) => ({
                     value: r.value,
                     weight: Number(r.weight),
                  })),
                  truncated: prior!.truncated,
                  distinctSeen: prior!.kept,
               });
               kept += stored.length;
               if (prior!.truncated) truncatedDims++;
               continue;
            }
            const fetched = await fetchDimensionValues(
               args.run,
               dim,
               cap,
               cfg.maxValueChars,
               cfg.queryTimeoutMs,
            );
            if (fetched.truncated && cfg.onOverflow === "skip") {
               await db.run(
                  `DELETE FROM dimension_values WHERE environment_name = ? AND package_name = ? AND source_name = ? AND dimension_name = ?`,
                  [environmentName, packageName, dim.source, dim.dimension],
               );
               await writeState(args, dim, {
                  distinct: fetched.distinctSeen,
                  kept: 0,
                  truncated: true,
                  status: "skipped",
               });
               truncatedDims++;
               continue;
            }
            await syncOne(args, dim, fetched);
            await writeState(args, dim, {
               distinct: fetched.distinctSeen,
               kept: fetched.values.length,
               truncated: fetched.truncated,
               status: "ok",
            });
            kept += fetched.values.length;
            if (fetched.truncated) truncatedDims++;
         } catch (error) {
            failed++;
            const message =
               error instanceof Error ? error.message : String(error);
            logger.warn(
               "[MCP Tool getContext] Could not index a dimension's values",
               {
                  environmentName,
                  packageName,
                  source: dim.source,
                  dimension: dim.dimension,
                  error: message,
               },
            );
            await writeState(args, dim, {
               distinct: 0,
               kept: 0,
               truncated: false,
               status: "failed",
               error: message,
            }).catch(() => undefined);
         }
      }
      return set({
         status: failed > 0 ? "partial" : "ready",
         dimensions: dims.length,
         values: kept,
         truncated: truncatedDims,
         failed,
      });
   } catch (error) {
      logger.warn("[MCP Tool getContext] Value indexing failed", {
         environmentName,
         packageName,
         error: error instanceof Error ? error.message : String(error),
      });
      return set({
         status: "failed",
         dimensions: dims.length,
         values: kept,
         truncated: truncatedDims,
         failed: dims.length,
      });
   }
}

/**
 * Start a value-index run for this Package instance unless one is running or
 * finished recently. Non-blocking: a question never waits on the warehouse.
 * Re-runs once `refreshMinutes` has passed (values drift), or sooner for a run
 * that left dimensions unfinished.
 */
export function kickValueIndex(
   instance: object,
   args: ValueSyncArgs,
   now: number = Date.now(),
): void {
   const key = pkgKey(args.environmentName, args.packageName);
   if (running.has(key) || args.dims.length === 0) return;
   const cfg = args.config.dimensionalValues;
   const last = kicked.get(instance);
   if (last) {
      const wait = last.done
         ? cfg.refreshMinutes * 60_000
         : RETRY_AFTER_FAILURE_MS;
      if (now - last.at < wait) return;
   }
   if (!statuses.has(key)) {
      statuses.set(key, {
         status: "building",
         dimensions: args.dims.length,
         values: 0,
         truncated: 0,
         failed: 0,
      });
   }
   kicked.set(instance, { at: now, done: false });
   const promise = syncDimensionValues(args)
      .then((s) => {
         kicked.set(instance, { at: Date.now(), done: s.status === "ready" });
      })
      .finally(() => running.delete(key));
   running.set(key, promise);
}

export async function _settleValueIndexForTests(
   environmentName: string,
   packageName: string,
): Promise<void> {
   await running.get(pkgKey(environmentName, packageName));
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

export interface ValueHit {
   targetIndex: number;
   source: string;
   dimension: string;
   value: string;
   /** [0, 1]: cosine, or 1 / .95 / .9 / near-match for the lexical arm. */
   score: number;
   weight: number;
}

/** Whether the dimension's values were cut at a cap, keyed source + NUL + dimension. */
export type TruncationMap = Map<string, boolean>;

export async function loadTruncation(
   db: DuckDBConnection,
   environmentName: string,
   packageName: string,
): Promise<TruncationMap> {
   try {
      const rows = await db.all<{
         source_name: string;
         dimension_name: string;
         truncated: boolean;
      }>(
         `SELECT source_name, dimension_name, truncated FROM dimension_value_state
          WHERE environment_name = ? AND package_name = ?`,
         [environmentName, packageName],
      );
      return new Map(
         rows.map((r) => [
            [r.source_name, r.dimension_name].join(SEP),
            Boolean(r.truncated),
         ]),
      );
   } catch {
      return new Map();
   }
}

/**
 * Find values matching each query text. Two arms over the same rows: the
 * semantic one (cosine over stored vectors, when a provider and vectors exist)
 * and the lexical one (exact 1.0, prefix .95, contains .9, or a near spelling
 * at Jaro-Winkler >= .85), which needs no provider and sends nothing anywhere.
 * A value found by both keeps the higher score.
 */
export async function searchDimensionValues(args: {
   db: DuckDBConnection;
   provider: EmbeddingProvider | null;
   environmentName: string;
   packageName: string;
   queries: Array<{ targetIndex: number; text: string }>;
   config: RetrievalConfig;
   /** Restrict to one source (a drill-down). */
   sourceName?: string;
   /** Cosine floor: the config's, else the provider's. */
   minSimilarity: number;
}): Promise<ValueHit[]> {
   const { db, provider, environmentName, packageName, config } = args;
   const cfg = config.dimensionalValues;
   const best = new Map<string, ValueHit>();
   const offer = (h: ValueHit) => {
      const k = [h.targetIndex, h.source, h.dimension, h.value].join(SEP);
      const at = best.get(k);
      if (!at || h.score > at.score) best.set(k, h);
   };
   const scope = args.sourceName !== undefined ? "AND source_name = ?" : "";
   const scopeParam = args.sourceName !== undefined ? [args.sourceName] : [];

   if (cfg.lexical) {
      for (const q of args.queries) {
         const lq = q.text.trim().toLowerCase();
         if (!lq) continue;
         const rows = await db.all<{
            source_name: string;
            dimension_name: string;
            value: string;
            weight: number;
            score: number;
         }>(
            `SELECT source_name, dimension_name, value, CAST(weight AS DOUBLE) AS weight, score FROM (
               SELECT source_name, dimension_name, value, weight,
                      CASE WHEN lower(value) = ? THEN 1.0
                           WHEN starts_with(lower(value), ?) THEN 0.95
                           WHEN contains(lower(value), ?) THEN 0.9
                           ELSE ${NEAR_MATCH_FLOOR} + (jaro_winkler_similarity(lower(value), ?) - ${NEAR_MATCH_FLOOR}) * ${NEAR_MATCH_SPAN} END AS score
               FROM dimension_values
               WHERE environment_name = ? AND package_name = ? ${scope}
             ) WHERE score >= ${NEAR_MATCH_FLOOR}
             ORDER BY score DESC, weight DESC LIMIT ?`,
            [
               lq,
               lq,
               lq,
               lq,
               environmentName,
               packageName,
               ...scopeParam,
               cfg.maxHitsPerTarget,
            ],
         );
         for (const r of rows) {
            offer({
               targetIndex: q.targetIndex,
               source: r.source_name,
               dimension: r.dimension_name,
               value: r.value,
               score: Math.round(r.score * 10_000) / 10_000,
               weight: Number(r.weight),
            });
         }
      }
   }

   if (cfg.embed && provider) {
      let vectors: number[][] | undefined;
      try {
         vectors = await provider.embedBatch(
            args.queries.map((q) => q.text),
            5_000,
            "query",
         );
      } catch (error) {
         logger.warn(
            "[MCP Tool getContext] Query embedding for values failed; using the text arm only",
            {
               error: error instanceof Error ? error.message : String(error),
            },
         );
      }
      if (vectors) {
         for (let i = 0; i < args.queries.length; i++) {
            const rows = await db.all<{
               source_name: string;
               dimension_name: string;
               value: string;
               weight: number;
               score: number;
            }>(
               `SELECT source_name, dimension_name, value, CAST(weight AS DOUBLE) AS weight,
                       list_cosine_similarity(embedding, CAST(? AS FLOAT[])) AS score
                FROM dimension_values
                WHERE environment_name = ? AND package_name = ? AND embedding IS NOT NULL
                  AND embedding_model = ? ${scope}
                ORDER BY score DESC LIMIT ?`,
               [
                  JSON.stringify(vectors[i]),
                  environmentName,
                  packageName,
                  provider.rowModel,
                  ...scopeParam,
                  cfg.maxHitsPerTarget,
               ],
            );
            for (const r of rows) {
               if (r.score < args.minSimilarity) continue;
               offer({
                  targetIndex: args.queries[i].targetIndex,
                  source: r.source_name,
                  dimension: r.dimension_name,
                  value: r.value,
                  score: Math.round(r.score * 10_000) / 10_000,
                  weight: Number(r.weight),
               });
            }
         }
      }
   }
   return [...best.values()].sort(
      (a, b) =>
         b.score - a.score ||
         b.weight - a.weight ||
         (a.value < b.value ? -1 : 1),
   );
}
