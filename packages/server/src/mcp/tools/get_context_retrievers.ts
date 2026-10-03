// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The two ways get_context ranks a package's entities: semantic (cosine over
 * cached embeddings) and lexical (lunr). Each is a Retriever, so the
 * orchestrator in get_context_tool.ts can pick one without knowing how either
 * scores: semantic when an embedding provider is configured (and it never
 * falls back to lexical, which would rank differently from the finished
 * index), lexical otherwise. Later stages (refine/prune, rerank) do not belong
 * here; they plug in as RankStage or CardStage after retrieval.
 */

import type lunr from "lunr";
import type { EmbeddingModel } from "../../providers/types";
import { getEmbeddingProvider } from "../../service/embedding_provider";
import { logger } from "../../logger";
import {
   entityRowKey,
   getEmbeddingIndexStatus,
   trySemanticSearch,
   type EmbeddingIndexStatus,
} from "./embedding_index";
import { compareRanked, scopeKeysWithJoins } from "./get_context_assembly";
import type { PipelineContext, Retriever } from "./get_context_pipeline";
import {
   REASON_BY_UNAVAILABLE,
   bestTargetOf,
   embeddedEntitiesOf,
   entityCardKey,
   matchesScope,
   projectEntity,
   sanitize,
   scopeKeysFor,
   type Entity,
   type ResultEntity,
   type RetrievalReason,
} from "./get_context_tool";

/**
 * The package's index state, read to explain why a search could not be
 * answered (progress while indexing, the last error and when the next try is).
 * Undefined when it cannot be read; the caller then words the reason without it.
 */
async function indexStatusFor(
   ctx: PipelineContext,
   provider: EmbeddingModel,
): Promise<EmbeddingIndexStatus | undefined> {
   const { request, environmentStore, pkgIndex } = ctx;
   try {
      return await getEmbeddingIndexStatus(
         environmentStore.storageManager.getDuckDbConnection(),
         provider,
         request.environmentName,
         request.packageName,
         embeddedEntitiesOf(pkgIndex, ctx.settings),
         pkgIndex.pkg,
      );
   } catch (error) {
      logger.warn("[MCP Tool getContext] Could not read the index state", {
         error: error instanceof Error ? error.message : String(error),
      });
      return undefined;
   }
}

export const semanticRetriever: Retriever = {
   name: "semantic",
   async retrieve(ctx: PipelineContext) {
      const { request, environmentStore, pkgIndex } = ctx;
      const { environmentName, packageName, sourceName } = request;
      const { byId } = pkgIndex;
      // Joined copies are made at assembly, so the scan reads direct fields
      // only and the scope has to be written in terms of what assembly can
      // reach (see scopeKeysWithJoins). Otherwise the scan is the index's.
      const assembling = ctx.settings.joins === "assembly";
      if (!ctx.embeddingConfigured) return { unavailable: "unconfigured" };
      let provider: EmbeddingModel | null = null;
      try {
         provider = getEmbeddingProvider();
      } catch (error) {
         const message = error instanceof Error ? error.message : String(error);
         logger.warn(
            "[MCP Tool getContext] Embedding configuration invalid; semantic search unavailable",
            { error: message },
         );
         return { unavailable: "unavailable", detail: message };
      }
      if (provider) {
         try {
            // Every target is scored in one scan and the rows are merged on
            // score. A max ACROSS targets is meaningful here and only here:
            // cosine is an absolute scale, so 0.7 from the measure target and
            // 0.7 from the dimension target mean the same thing. (The lexical
            // path below has to normalize first, because lunr scores are
            // relative to their own query.)
            const merged = new Map<string, ResultEntity>();
            let searchFailure: RetrievalReason | undefined;
            let unionTotalEntities: number | undefined;
            let unionBelowCutoff: number | undefined;
            let cutBySource: Map<string, number> | undefined;
            {
               // ONE call for every target: it batches the embeddings into a
               // single provider request and scores them in a single pass
               // over the vector cache, returning each hit's per-target
               // scores. The raw text embeds better than the lunr-sanitized
               // form; sanitize() only exists to strip lunr operators.
               const semantic = await trySemanticSearch({
                  db: environmentStore.storageManager.getDuckDbConnection(),
                  provider,
                  pkg: pkgIndex.pkg,
                  environmentName,
                  packageName,
                  entities: embeddedEntitiesOf(pkgIndex, ctx.settings),
                  // Each target carries the kinds it may claim, and the scan
                  // applies that BEFORE cutting the target's window. Applied
                  // here afterwards, a `measure` target whose nearest rows were
                  // dimensions came back empty with below_cutoff_count 0.
                  queries: request.searches.map((search) => ({
                     targetIndex: search.targetIndex,
                     text: search.text,
                     kinds: search.kinds,
                  })),
                  // Per source and per target, never global: `limit` counts
                  // SOURCE CARDS and is applied after assembly, so the scan
                  // must not decide how many sources are returned.
                  perSourceWindow: ctx.settings.entityWindow.perSourcePerTarget,
                  // "" means no drill-down, matching the lexical
                  // path's truthiness filter.
                  sourceName: assembling ? undefined : sourceName || undefined,
                  // The rest of the scope, as rows the scan can join on. The
                  // cache has no model_path column and an entity_name scope
                  // exempts source rows, so neither is expressible as a
                  // predicate -- but both have to be applied INSIDE the scan
                  // anyway, because that is where belowCutoffCount and
                  // totalEntities are counted. Filtering only the returned
                  // rows left those two describing the unpinned set: an
                  // entity_name that matched nothing answered with no sources
                  // beside a belowCutoffCount of 0, which the tool
                  // description tells the agent means "nothing cleared the
                  // floor", so it had no reason to retry with another name.
                  scopeKeys: assembling
                     ? sourceName || request.modelPath || request.entityName
                        ? scopeKeysWithJoins(
                             pkgIndex.directEntities,
                             pkgIndex.topology,
                             request,
                             ctx.settings.joinMaxDepth,
                          )
                        : undefined
                     : request.modelPath || request.entityName
                       ? scopeKeysFor(byId.values(), request)
                       : undefined,
               });
               if ("hits" in semantic) {
                  // One row per (kind, source, name) is EMBEDDED — the
                  // text is identical for every model path that reaches
                  // the entity, so the vector is stored once — but
                  // several live entities can share that key, one per
                  // path. Fan the hit out to all of them; a 1:1 map
                  // silently kept only whichever was seen last.
                  const byKey = new Map<string, Entity[]>();
                  for (const e of byId.values()) {
                     const k = entityRowKey(e.kind, e.source ?? "", e.name);
                     const at = byKey.get(k);
                     if (at) at.push(e);
                     else byKey.set(k, [e]);
                  }
                  // Rows are only a vector cache: modelPath and doc
                  // come from the live entity, and a hit with no live
                  // entity (deleted since the last sync) is dropped.
                  const ranked = semantic.hits.flatMap((hit) => {
                     const matches = (
                        byKey.get(
                           entityRowKey(hit.kind, hit.source ?? "", hit.name),
                        ) ?? []
                     )
                        // The scan filters on sourceName only, so the scope's
                        // model_path and entity_name have to be applied to
                        // what it returns -- the same place the lexical path
                        // applies them (see matchesScope's other call site).
                        // Without this, pinning an entity narrowed nothing
                        // wherever an embedding provider is configured, and
                        // a caller who pinned one entity got the whole ranked
                        // set back, definitions included, because a pinned
                        // entity_name also turns include_code on. When
                        // assembly makes the joined copies it applies the
                        // scope itself, to the copies as well: a row of a
                        // source outside the scope may still be the field a
                        // scoped source reaches through a join.
                        .filter((e) => assembling || matchesScope(e, request));
                     return matches.map((e) => ({
                        ...projectEntity(e, environmentName, packageName),
                        score: Math.round(hit.score * 10_000) / 10_000,
                        targetScores: hit.targetScores,
                     }));
                  });
                  for (const row of ranked) {
                     // The scan scored this row only against targets that may
                     // claim its kind, so every score it carries is from a
                     // target that can return it, and its `score` is already
                     // the best of those.
                     // Keyed per CARD, like the fan-out just above produced:
                     // one embedded row legitimately becomes several live
                     // entities, one per model path. Keying this on the bare
                     // (kind, source, name) collapsed them straight back into
                     // one and kept whichever landed last -- undoing the
                     // fan-out, and making the semantic path answer with one
                     // model_path where the lexical path answers with every
                     // resolving one. lunr has no such problem because its
                     // ref IS the per-path entity id.
                     const key = entityCardKey(row);
                     merged.set(key, {
                        ...row,
                        bestTarget: bestTargetOf(row.targetScores ?? new Map()),
                     });
                  }
                  // The denominator counts the package's entities, not the
                  // query's hits, so it is the same whichever target asked.
                  unionTotalEntities = semantic.totalEntities;
                  unionBelowCutoff = semantic.belowCutoffCount;
                  cutBySource = semantic.cutBySource;
               } else {
                  searchFailure = REASON_BY_UNAVAILABLE[semantic.unavailable];
               }
            }
            if (merged.size > 0 || searchFailure === undefined) {
               const ranked = [...merged.values()].sort(compareRanked);
               // Collapse, windowing and serialization are finishRanked's,
               // shared with the lexical path so the two cannot drift.
               // Straight from the scan, which counts entities whose BEST
               // score across the targets that may claim them fell under the
               // floor. Deriving it from the returned rows would fold the page
               // limit into it and report a crowded-out entity -- one that
               // cleared the floor and simply did not fit -- as rejected,
               // which is the opposite of what this number tells a caller.
               // The whole scope goes into the scan (sourceName as a column
               // predicate, the rest as scopeKeys), so the count and the rows
               // still describe the same set.
               return {
                  rows: ranked,
                  belowCutoffCount: unionBelowCutoff ?? 0,
                  totalEntities: unionTotalEntities,
                  entitiesCutBySource: cutBySource,
               };
            }
            return {
               unavailable: searchFailure,
               status: await indexStatusFor(ctx, provider),
            };
         } catch (error) {
            // Defensive: trySemanticSearch does not throw, but the
            // storage handle lookup can (e.g. before initialization
            // or under a partial test double). Reported as unavailable
            // rather than allowed to take the tool down.
            const message =
               error instanceof Error ? error.message : String(error);
            logger.warn(
               "[MCP Tool getContext] Semantic retrieval unavailable",
               { error: message },
            );
            // The cause goes to the log, not the response: it is an internal
            // exception message, which differs by runtime and says nothing a
            // caller can act on.
            return {
               unavailable: "unavailable",
               detail:
                  "the semantic index storage could not be reached (see the server log)",
            };
         }
      }
      return { unavailable: "unconfigured" };
   },
};

export const lexicalRetriever: Retriever = {
   name: "lexical",
   async retrieve(ctx: PipelineContext) {
      const { request, pkgIndex } = ctx;
      const { environmentName, packageName, sourceName } = request;
      const { byId, index } = pkgIndex;
      // One lunr pass per target, merged. Each target's hits are normalized
      // against ITS OWN top hit before merging, which is what makes two
      // targets' scores comparable at all: raw lunr scores are relative to
      // the query that produced them. All targets share one index, so the
      // IDF corpus is the same and the normalization is the only correction
      // needed.
      const bestByRef = new Map<string, Map<number, number>>();
      for (const search of request.searches) {
         const sanitized = sanitize(search.text);
         if (!sanitized) continue;
         let targetHits: lunr.Index.Result[] = [];
         try {
            targetHits = index.search(sanitized);
         } catch (error) {
            logger.warn("[MCP Tool getContext] lunr search failed", {
               query: search.text,
               error: error instanceof Error ? error.message : String(error),
            });
            continue;
         }
         const top = targetHits[0]?.score ?? 0;
         for (const hit of targetHits) {
            const entity = byId.get(hit.ref);
            // A target only claims the kinds it selects, so a `measure`
            // target never surfaces a dimension that happened to match.
            if (!entity || !search.kinds.includes(entity.kind)) continue;
            const score = top > 0 ? hit.score / top : 0;
            const scores = bestByRef.get(hit.ref) ?? new Map<number, number>();
            scores.set(
               search.targetIndex,
               Math.max(scores.get(search.targetIndex) ?? 0, score),
            );
            bestByRef.set(hit.ref, scores);
         }
      }
      // Already normalized per target and merged, so this is the ranked list
      // rather than raw lunr output -- no fake lunr Result needs constructing
      // to carry it.
      const ranking = [...bestByRef.entries()]
         .flatMap(([ref, targetScores]) => {
            const e = byId.get(ref);
            // Defensive: skip a ref missing from the entity map.
            if (!e) return [];
            // Drill-down: narrow to one source when sourceName is set.
            if (sourceName && e.source !== sourceName) return [];
            if (!matchesScope(e, request)) return [];
            // The row ranks on its best target, the same MAX-over-facets rule
            // the semantic path uses one level down.
            const score = Math.max(...targetScores.values());
            return [{ e, score, targetScores }];
         })
         .sort((a, b) => b.score - a.score);

      const scored: ResultEntity[] = ranking.map(({ e, targetScores }) => ({
         ...projectEntity(e, environmentName, packageName),
         // WHICH target found the row is carried, for the per-target window;
         // HOW WELL is not. targetScores would reach the wire as
         // matched_targets[].relevance, whose relevance is required and would
         // therefore publish a lexical score -- the exact number this path
         // withholds from the entity's own `relevance`, because a lunr score is
         // relative to its own query and comparing two of them means nothing.
         // Budgeting needs only the index, so the two concerns separate cleanly.
         bestTarget: bestTargetOf(targetScores),
      }));
      return { rows: scored, belowCutoffCount: 0 };
   },
};
