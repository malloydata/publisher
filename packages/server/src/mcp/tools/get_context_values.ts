// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Value search: the rank stage that answers a `dimensional_value` target.
 *
 * The question it answers is "which string does the data store for what the
 * user typed?" ("Auto Intenders" -> `Auto Intenders (In-Market)` on
 * `segment_name`). It does not keep a value index. For each candidate string
 * dimension it runs a bounded `group_by` through the NORMAL query path
 * (`Model.getQueryResults`, the call `execute_query` makes) and matches the
 * phrase against what comes back.
 *
 * Why that shape: a value is data, and data has a gate. Going through the
 * query path means the gate is enforced by the one code that already enforces
 * it. An `#(authorize)` lock the caller's givens do not pass answers
 * AccessDeniedError and the source contributes nothing. An `#(access_filter)`
 * is grafted as a `where:`, so values come only from rows the caller can see.
 * No gate logic is repeated here.
 *
 * Cache: the distinct values of a dimension are kept per Package instance (a
 * reload drops them) for `retrieval.dimensionValues.cacheTtlMs`. The key holds
 * a hash of the caller's givens and filter params, so a value list read under
 * one caller's givens is never served for another's. A list is stored only
 * after a query that passed its gate.
 */

import { createHash } from "crypto";
import type { GivenValue } from "@malloydata/malloy";
import { getEmbeddingProvider } from "../../service/embedding_provider";
import {
   getEgressPreset,
   getDimensionValueSettings,
} from "../../retrieval_config";
import {
   AccessDeniedError,
   QueryTimeoutError,
   ServiceUnavailableError,
} from "../../errors";
import { logger } from "../../logger";
import { tryAcquireQuerySlot } from "../../query_concurrency";
import { runWithQueryTimeout } from "../../query_timeout";
import { quoteMalloyIdentifier } from "../../service/authorize";
import type { EnvironmentStore } from "../../service/environment_store";
import { getModelForQuery } from "../handler_utils";
import { compareRanked } from "./get_context_assembly";
import type {
   PipelineContext,
   RankStage,
   RankedState,
} from "./get_context_pipeline";
import {
   cosine,
   filterStem,
   matchValues,
   MAX_VALUES_PER_DIMENSION_HIT,
   type ValueHit,
} from "./get_context_value_match";
import { matchesScope } from "./get_context_request";
import { projectEntity } from "./get_context_response";
import { sanitizeLunrQuery as sanitize } from "./retrieval_shared";
import { type Entity, type ResultEntity } from "./get_context_types";

/** What one lookup query returns. */
export interface DistinctValues {
   values: string[];
   /** More distinct values exist than were read. */
   truncated: boolean;
}

/** One lookup query. A seam so a test can stand in for the warehouse. */
export interface ValueFetchRequest {
   environmentStore: EnvironmentStore;
   environmentName: string;
   packageName: string;
   modelPath: string;
   source: string;
   dimension: string;
   /** Distinct values to read; one more is asked for to learn whether there are more. */
   limit: number;
   /** When set, only values containing this text (case-insensitive) are read. */
   stem?: string;
   givens?: Record<string, unknown>;
   filterParams?: Record<string, string | string[]>;
   timeoutMs: number;
}

export type ValueFetcher = (
   request: ValueFetchRequest,
) => Promise<DistinctValues>;

/** Values longer than this are free text, not a category worth matching. */
const MAX_VALUE_CHARS = 200;

/** Most values of one dimension embedded for meaning matching. */
export const MAX_EMBEDDED_VALUES = 500;

/**
 * Cosine under which a meaning match is not reported. Short strings embed close
 * to each other (a live check put "Travel Seekers" at 0.52 against the phrase
 * "Home Shoppers"), so this is a floor against noise, not a verdict.
 */
export const SEMANTIC_VALUE_FLOOR = 0.4;

/**
 * A spelling-based score at or above this is an answer: the phrase's own words
 * are in the value, so meaning matches would only add guesses beside it. The
 * dimension is then not embedded at all, which also keeps its values off the
 * wire to the embedding provider.
 */
export const SEMANTIC_SKIP_ABOVE = 0.75;

/** Lookup queries in flight at once. */
const LOOKUP_CONCURRENCY = 4;

/** Entries one Package's cache holds before the oldest goes. */
const CACHE_MAX_ENTRIES = 500;

/**
 * The lookup query: read up to `limit + 1` distinct non-null values of one
 * dimension, through the normal query path with the caller's givens.
 */
export const defaultValueFetcher: ValueFetcher = async (request) => {
   const found = await getModelForQuery(
      request.environmentStore,
      request.environmentName,
      request.packageName,
      request.modelPath,
   );
   if ("error" in found) throw new Error(found.error.message);
   const dim = quoteMalloyIdentifier(request.dimension);
   // The stem is letters and digits only (see filterStem), so it needs no escaping.
   const where = request.stem
      ? `${dim} != null and lower(${dim}) ~ '%${request.stem}%'`
      : `${dim} != null`;
   const query = `run: ${quoteMalloyIdentifier(request.source)} -> { where: ${where} group_by: v is ${dim} limit: ${request.limit + 1} }`;
   const slot = tryAcquireQuerySlot("mcp:getContext:values");
   try {
      const { compactResult } = await runWithQueryTimeout(
         (signal) =>
            found.model.getQueryResults(
               undefined,
               undefined,
               query,
               request.filterParams,
               undefined,
               request.givens as Record<string, GivenValue> | undefined,
               signal,
               undefined,
               "compact",
               // MCP sends no #(authorize) bypass, ever; nor does value search.
               false,
               false,
            ),
         request.timeoutMs,
      );
      const rows = compactResult as unknown as Array<Record<string, unknown>>;
      const values: string[] = [];
      for (const row of rows) {
         const raw = row.v;
         if (raw === null || raw === undefined) continue;
         const text = String(raw);
         if (text.trim() === "" || text.length > MAX_VALUE_CHARS) continue;
         values.push(text);
      }
      return {
         values: values.slice(0, request.limit),
         truncated: rows.length > request.limit,
      };
   } finally {
      slot.release();
   }
};

interface CacheEntry {
   at: number;
   values: string[];
   truncated: boolean;
   /** Embeddings of `values`, filled when meaning matching first needs them. */
   vectors?: Map<string, number[]>;
}

// Per Package instance, so a reload (a new instance) starts empty.
const caches = new WeakMap<object, Map<string, CacheEntry>>();
const counters = { hits: 0, misses: 0 };

/** Test seam: cache hits and misses since the last reset. */
export function _valueCacheCounters(): { hits: number; misses: number } {
   return { ...counters };
}

let fetcherOverride: ValueFetcher | undefined;

/** Test seam: stand in for the warehouse. Pass undefined to undo. */
export function _setValueFetcherForTests(
   fetcher: ValueFetcher | undefined,
): void {
   fetcherOverride = fetcher;
}

export function _resetValueCacheForTests(): void {
   counters.hits = 0;
   counters.misses = 0;
}

function hashOf(value: unknown): string {
   if (value === undefined) return "";
   const stable = (v: unknown): unknown =>
      Array.isArray(v)
         ? v.map(stable)
         : v && typeof v === "object"
           ? Object.fromEntries(
                Object.entries(v as Record<string, unknown>)
                   .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
                   .map(([k, x]) => [k, stable(x)]),
             )
           : v;
   return createHash("sha256")
      .update(JSON.stringify(stable(value)))
      .digest("hex")
      .slice(0, 16);
}

/** Everything the cached list depends on, joined on a control character. */
function cacheKey(
   ctx: PipelineContext,
   e: Entity,
   stem: string | undefined,
): string {
   const { request } = ctx;
   return [
      request.environmentName,
      request.packageName,
      e.modelPath,
      e.source ?? "",
      e.name,
      hashOf(request.givens),
      hashOf(request.filterParams),
      stem ?? "",
   ].join("\u0000");
}

type Looked =
   | { kind: "values"; entry: CacheEntry; filtered: boolean }
   | { kind: "withheld" }
   | { kind: "skipped"; reason: string };

const sourceKey = (e: { modelPath: string; source?: string }) =>
   `${e.modelPath}\u0000${e.source ?? ""}`;

/** The string dimensions of a source's own (not joined copies) in scope. */
function candidatesOf(ctx: PipelineContext, state: RankedState): Entity[] {
   const { pkgIndex, request } = ctx;
   const dims = pkgIndex.directEntities.filter(
      (e) =>
         e.kind === "dimension" &&
         e.dataType === "string" &&
         e.source !== undefined &&
         matchesScope(e, request),
   );
   // Best first, so the cap keeps the likeliest: a dimension the ranker
   // already returned, then one the phrase matches by name or doc, then the rest
   // in model order.
   const ranked = new Set(
      state.rows
         .filter((r) => r.kind === "dimension")
         .map((r) => `${r.modelPath}\u0000${r.source ?? ""}\u0000${r.name}`),
   );
   const hit = new Set<string>();
   for (const search of request.valueSearches) {
      const text = sanitize(search.text);
      if (!text) continue;
      try {
         for (const h of pkgIndex.index.search(text)) hit.add(h.ref);
      } catch {
         // A phrase lunr cannot parse only loses the ordering hint.
      }
   }
   const tier = (e: Entity) =>
      ranked.has(`${e.modelPath}\u0000${e.source ?? ""}\u0000${e.name}`)
         ? 0
         : hit.has(e.id)
           ? 1
           : 2;
   return dims
      .map((e, i) => ({ e, i }))
      .sort((a, b) => tier(a.e) - tier(b.e) || a.i - b.i)
      .map(({ e }) => e);
}

async function runPool<T>(
   items: T[],
   size: number,
   work: (item: T) => Promise<void>,
): Promise<void> {
   let next = 0;
   await Promise.all(
      Array.from({ length: Math.min(size, items.length) }, async () => {
         while (next < items.length) {
            const item = items[next++];
            await work(item);
         }
      }),
   );
}

const listOf = (names: string[], max = 4) =>
   names.length <= max
      ? names.join(", ")
      : `${names.slice(0, max).join(", ")} and ${names.length - max} more`;

/**
 * Adds a `dimension` row, carrying `values`, for every dimension whose stored
 * strings match a `dimensional_value` target. Runs last, after refine, so the
 * LLM step never sees or reorders value rows.
 */
export const valuesStage: RankStage = {
   name: "values",
   enabled: (ctx) =>
      ctx.request.valueSearches.length > 0 &&
      getDimensionValueSettings().enabled,
   async run(state, ctx) {
      const settings = getDimensionValueSettings();
      const { request, pkgIndex } = ctx;
      const fetchDistinct =
         ctx.valueFetcher ?? fetcherOverride ?? defaultValueFetcher;
      const started = performance.now();
      const budgetLeft = () =>
         settings.budgetMs - (performance.now() - started);

      const all = candidatesOf(ctx, state);
      const candidates = all.slice(0, settings.maxDimensions);
      if (all.length > candidates.length) {
         ctx.warnings.push(
            `Value search covered ${candidates.length} of ${all.length} string dimensions in scope. Scope the call to the source that holds the value to search the rest, or raise retrieval.dimensionValues.maxDimensions.`,
         );
      }
      if (candidates.length === 0) {
         ctx.warnings.push(
            `No string dimension in scope to search for the value${request.valueSearches.length > 1 ? "s" : ""} ${request.valueSearches.map((s) => `"${s.text}"`).join(", ")}. Check the scope, or read distinct values with execute_query.`,
         );
         return state;
      }

      const cache = caches.get(pkgIndex.pkg) ?? new Map<string, CacheEntry>();
      caches.set(pkgIndex.pkg, cache);
      const ttl = settings.cacheTtlMs;

      const withheldSources = new Set<string>();
      const skipped = new Map<string, string[]>();
      const truncatedNames: string[] = [];
      const looked = new Map<Entity, Looked>();

      const read = async (
         e: Entity,
         stem: string | undefined,
      ): Promise<Looked> => {
         const key = cacheKey(ctx, e, stem);
         const cached = cache.get(key);
         if (cached && ttl > 0 && Date.now() - cached.at < ttl) {
            counters.hits += 1;
            return {
               kind: "values",
               entry: cached,
               filtered: stem !== undefined,
            };
         }
         counters.misses += 1;
         const left = budgetLeft();
         if (left <= 0)
            return { kind: "skipped", reason: "the time budget ran out" };
         const timeoutMs = Math.max(1, Math.min(settings.timeoutMs, left));
         try {
            // The query path cancels a query through an abort signal, and a
            // driver that cannot honour it (DuckDB) finishes anyway: the
            // timeout then returns the late rows as a success. The wait is
            // bounded here as well, so the call answers on time. The query is
            // left to finish and its rows go into the cache, so the next call
            // finds them.
            const work = fetchDistinct({
               environmentStore: ctx.environmentStore,
               environmentName: request.environmentName,
               packageName: request.packageName,
               modelPath: e.modelPath,
               source: e.source as string,
               dimension: e.name,
               limit: settings.maxValuesPerDimension,
               ...(stem ? { stem } : {}),
               ...(request.givens ? { givens: request.givens } : {}),
               ...(request.filterParams
                  ? { filterParams: request.filterParams }
                  : {}),
               timeoutMs,
            }).then((fetched) => {
               const entry: CacheEntry = { at: Date.now(), ...fetched };
               if (ttl > 0) {
                  cache.set(key, entry);
                  while (cache.size > CACHE_MAX_ENTRIES) {
                     const oldest = cache.keys().next().value;
                     if (oldest === undefined) break;
                     cache.delete(oldest);
                  }
               }
               return entry;
            });
            // A failure after the wait is over has no caller left to tell.
            work.catch(() => undefined);
            let timer: ReturnType<typeof setTimeout> | undefined;
            const entry = await Promise.race([
               work,
               new Promise<"timeout">((resolve) => {
                  timer = setTimeout(() => resolve("timeout"), timeoutMs);
               }),
            ]).finally(() => clearTimeout(timer));
            if (entry === "timeout") {
               return { kind: "skipped", reason: "the query timed out" };
            }
            return { kind: "values", entry, filtered: stem !== undefined };
         } catch (error) {
            if (error instanceof AccessDeniedError) return { kind: "withheld" };
            if (error instanceof QueryTimeoutError) {
               return { kind: "skipped", reason: "the query timed out" };
            }
            if (error instanceof ServiceUnavailableError) {
               return {
                  kind: "skipped",
                  reason: "the server is at its query limit",
               };
            }
            logger.warn("[MCP Tool getContext] Value lookup failed", {
               source: e.source,
               dimension: e.name,
               error: error instanceof Error ? error.message : String(error),
            });
            return { kind: "skipped", reason: "the query failed" };
         }
      };

      await runPool(candidates, LOOKUP_CONCURRENCY, async (e) => {
         if (withheldSources.has(sourceKey(e))) {
            looked.set(e, { kind: "withheld" });
            return;
         }
         const result = await read(e, undefined);
         if (result.kind === "withheld") withheldSources.add(sourceKey(e));
         looked.set(e, result);
      });

      // A dimension with more values than were read is searched again with a
      // text filter built from each phrase, so a value past the read cap can
      // still be found.
      const filteredFor = new Map<string, Looked>();
      const truncatedDims = candidates.filter((e) => {
         const first = looked.get(e);
         return first?.kind === "values" && first.entry.truncated;
      });
      for (const e of truncatedDims)
         truncatedNames.push(`${e.source}.${e.name}`);
      const stemJobs = truncatedDims.flatMap((e) =>
         request.valueSearches.flatMap((s) => {
            const stem = filterStem(s.text);
            return stem ? [{ e, stem }] : [];
         }),
      );
      await runPool(stemJobs, LOOKUP_CONCURRENCY, async ({ e, stem }) => {
         filteredFor.set(`${cacheKey(ctx, e, stem)}`, await read(e, stem));
      });

      // Meaning matching: only when the operator allows values to leave the
      // process (egress full) and an embedding provider is configured.
      let provider: ReturnType<typeof getEmbeddingProvider> = null;
      if (getEgressPreset() === "full") {
         try {
            provider = getEmbeddingProvider();
         } catch {
            provider = null;
         }
      }
      let phraseVectors: number[][] | undefined;
      let semanticFailed = false;
      if (provider) {
         try {
            phraseVectors = await provider.embedBatch(
               request.valueSearches.map(
                  (s) => (provider?.queryPrefix ?? "") + s.text,
               ),
               Math.max(1, Math.min(settings.timeoutMs, budgetLeft())),
            );
         } catch (error) {
            semanticFailed = true;
            logger.warn("[MCP Tool getContext] Value query embedding failed", {
               error: error instanceof Error ? error.message : String(error),
            });
         }
      }

      const matched = new Map<Entity, { hits: Map<number, ValueHit[]> }>();
      const searchedSources = new Set<string>();
      let searchedDims = 0;
      for (const e of candidates) {
         const first = looked.get(e);
         if (!first) continue;
         if (first.kind === "skipped") {
            const list = skipped.get(first.reason) ?? [];
            list.push(`${e.source}.${e.name}`);
            skipped.set(first.reason, list);
            continue;
         }
         if (first.kind === "withheld") continue;
         searchedDims += 1;
         searchedSources.add(sourceKey(e));
         const hitsByTarget = new Map<number, ValueHit[]>();
         for (const [i, search] of request.valueSearches.entries()) {
            let entry = first.entry;
            if (entry.truncated) {
               const stem = filterStem(search.text);
               const filtered = stem
                  ? filteredFor.get(cacheKey(ctx, e, stem))
                  : undefined;
               if (filtered?.kind === "values") entry = filtered.entry;
            }
            const lexical = matchValues(search.text, entry.values);
            const merged = new Map(lexical.map((h) => [h.value, h.score]));
            if (
               provider &&
               phraseVectors &&
               (lexical[0]?.score ?? 0) < SEMANTIC_SKIP_ABOVE &&
               !entry.truncated &&
               entry.values.length > 0 &&
               entry.values.length <= MAX_EMBEDDED_VALUES
            ) {
               try {
                  if (!entry.vectors) {
                     const vectors = await provider.embedBatch(
                        entry.values.map(
                           (v) => (provider?.documentPrefix ?? "") + v,
                        ),
                        Math.max(1, Math.min(settings.timeoutMs, budgetLeft())),
                     );
                     entry.vectors = new Map(
                        entry.values.map((v, k) => [v, vectors[k]]),
                     );
                  }
                  const floor = Math.max(
                     SEMANTIC_VALUE_FLOOR,
                     provider.minSimilarity,
                  );
                  for (const [value, vector] of entry.vectors) {
                     const score = cosine(phraseVectors[i], vector);
                     if (score < floor) continue;
                     const rounded = Math.round(score * 10_000) / 10_000;
                     if (rounded > (merged.get(value) ?? 0)) {
                        merged.set(value, rounded);
                     }
                  }
               } catch (error) {
                  semanticFailed = true;
                  logger.warn("[MCP Tool getContext] Value embedding failed", {
                     source: e.source,
                     dimension: e.name,
                     error:
                        error instanceof Error ? error.message : String(error),
                  });
               }
            }
            const hits = [...merged.entries()]
               .map(([value, score]) => ({ value, score }))
               .sort(
                  (a, b) =>
                     b.score - a.score ||
                     a.value.length - b.value.length ||
                     (a.value < b.value ? -1 : a.value > b.value ? 1 : 0),
               )
               .slice(0, MAX_VALUES_PER_DIMENSION_HIT);
            if (hits.length > 0) hitsByTarget.set(i, hits);
         }
         if (hitsByTarget.size > 0) matched.set(e, { hits: hitsByTarget });
      }

      // Rows. A dimension the ranker already returned gets the values
      // attached; any other matching dimension becomes a new row.
      const rows = [...state.rows];
      const rowKey = (r: {
         modelPath: string;
         source?: string;
         name: string;
      }) => `${r.modelPath}\u0000${r.source ?? ""}\u0000${r.name}`;
      const byKey = new Map(
         rows.filter((r) => r.kind === "dimension").map((r) => [rowKey(r), r]),
      );
      const firstTarget = request.valueSearches[0].targetIndex;
      for (const [e, { hits }] of matched) {
         const values: NonNullable<ResultEntity["values"]> = [];
         const targetScores = new Map<number, number>();
         for (const [i, list] of hits) {
            const search = request.valueSearches[i];
            targetScores.set(search.targetIndex, list[0].score);
            for (const h of list) {
               const existing = values.find((v) => v.value === h.value);
               if (existing) {
                  if (h.score > existing.score) {
                     existing.score = h.score;
                     existing.searchText = search.text;
                  }
               } else {
                  values.push({
                     value: h.value,
                     score: h.score,
                     searchText: search.text,
                  });
               }
            }
         }
         values.sort(
            (a, b) =>
               b.score - a.score ||
               a.value.length - b.value.length ||
               (a.value < b.value ? -1 : a.value > b.value ? 1 : 0),
         );
         const best = values[0].score;
         const bestTarget = [...targetScores.entries()].sort(
            (a, b) => b[1] - a[1],
         )[0][0];
         const existing = byKey.get(rowKey(e));
         if (existing) {
            existing.values = values;
            existing.targetScores = new Map([
               ...(existing.targetScores ?? []),
               ...targetScores,
            ]);
            if (existing.score === undefined || best > existing.score) {
               existing.score = best;
               existing.rawScore = best;
            }
         } else {
            const row: ResultEntity = {
               ...projectEntity(
                  e,
                  request.environmentName,
                  request.packageName,
               ),
               score: best,
               rawScore: best,
               targetScores,
               bestTarget,
               values,
               fromValueSearch: true,
            };
            rows.push(row);
            byKey.set(rowKey(e), row);
         }
      }

      // A gated source whose values were withheld still answers with its
      // dimensions, no values, so the caller knows what to query directly.
      const withheldDims = candidates.filter(
         (e) => looked.get(e)?.kind === "withheld",
      );
      for (const e of withheldDims) {
         if (byKey.has(rowKey(e))) continue;
         const row: ResultEntity = {
            ...projectEntity(e, request.environmentName, request.packageName),
            bestTarget: firstTarget,
            fromValueSearch: true,
         };
         rows.push(row);
         byKey.set(rowKey(e), row);
      }
      rows.sort(compareRanked);

      // Warnings: each names what was not searched and what to do.
      if (withheldSources.size > 0) {
         const names = [
            ...new Set(withheldDims.map((e) => e.source as string)),
         ];
         ctx.warnings.push(
            `Dimension values were not searched in ${listOf(names)} because an access gate applies to ${names.length === 1 ? "it" : "them"} and the givens sent with this call did not pass it. Send the givens the gate reads (the same ones execute_query takes), or read the dimension's distinct values with execute_query.`,
         );
      }
      for (const [reason, names] of skipped) {
         ctx.warnings.push(
            `Value search skipped ${listOf(names)}: ${reason}. Scope the call to the source that holds the value, or see retrieval.dimensionValues in the server configuration.`,
         );
      }
      if (truncatedNames.length > 0) {
         ctx.warnings.push(
            `${listOf(truncatedNames)} ${truncatedNames.length === 1 ? "has" : "have"} more than ${settings.maxValuesPerDimension} distinct values, so ${truncatedNames.length === 1 ? "it was" : "they were"} searched with a text filter on the main word instead of in full. A value spelled very differently may be missed.`,
         );
      }
      if (semanticFailed) {
         ctx.warnings.push(
            "Matching by meaning was skipped for some dimensions because the embedding request failed; spelling-based matches are still returned.",
         );
      }
      for (const [i, search] of request.valueSearches.entries()) {
         const any = [...matched.values()].some((m) => m.hits.has(i));
         if (!any && searchedDims > 0) {
            ctx.warnings.push(
               `No stored value matched "${search.text}" in ${searchedDims} string ${searchedDims === 1 ? "dimension" : "dimensions"} of ${searchedSources.size} ${searchedSources.size === 1 ? "source" : "sources"}. Check the spelling, or read a dimension's distinct values with execute_query.`,
            );
         }
      }
      return { ...state, rows };
   },
};
