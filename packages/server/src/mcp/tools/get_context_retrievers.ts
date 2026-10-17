// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The two ways get_context ranks a package's entities: semantic (cosine over
 * cached embeddings) when an embedding provider is configured, lexical (lunr)
 * otherwise. Later stages (refine, rerank) plug in as RankStage or CardStage.
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
import { matchesScope, scopeKeysFor } from "./get_context_request";
import {
   REASON_BY_UNAVAILABLE,
   bestTargetOf,
   projectEntity,
} from "./get_context_response";
import type {
   Entity,
   ResultEntity,
   RetrievalReason,
} from "./get_context_types";
import { embeddedEntitiesOf, entityCardKey } from "./package_entities";
import { round4, sanitizeLunrQuery } from "./retrieval_shared";

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
            // A max across targets is meaningful here because cosine is an
            // absolute scale (the lexical path must normalize first).
            const merged = new Map<string, ResultEntity>();
            let searchFailure: RetrievalReason | undefined;
            let unionTotalEntities: number | undefined;
            let unionBelowCutoff: number | undefined;
            let cutBySource: Map<string, number> | undefined;
            {
               // One call for every target: one provider request, one pass
               // over the vector cache. Raw text, not the lunr-sanitized form,
               // because it embeds better.
               const semantic = await trySemanticSearch({
                  db: environmentStore.storageManager.getDuckDbConnection(),
                  provider,
                  pkg: pkgIndex.pkg,
                  environmentName,
                  packageName,
                  entities: embeddedEntitiesOf(pkgIndex, ctx.settings),
                  // The scan applies each target's kinds BEFORE cutting its
                  // window, or a `measure` target whose nearest rows were
                  // dimensions would come back empty.
                  queries: request.searches.map((search) => ({
                     targetIndex: search.targetIndex,
                     text: search.text,
                     kinds: search.kinds,
                  })),
                  // Per source and target, never global: `limit` counts cards
                  // after assembly, so the scan must not decide it.
                  perSourceWindow: ctx.settings.entityWindow.perSourcePerTarget,
                  // Dotted rows the index keeps get their own window, so they
                  // cannot take the source's own fields' slots.
                  perSourceJoinedWindow:
                     ctx.settings.entityWindow.joinedPerSourcePerTarget,
                  // "" means no drill-down, matching the lexical path.
                  sourceName: assembling ? undefined : sourceName || undefined,
                  // The rest of the scope, as rows the scan can join on. It
                  // must apply inside the scan, where belowCutoffCount and
                  // totalEntities are counted, or those would describe the
                  // unscoped set.
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
                  // One vector per (kind, source, name), but several live
                  // entities (one per model path) can share that key, so fan
                  // each hit out to all of them.
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
                        // The scan filters on sourceName only, so model_path
                        // and entity_name apply here, as on the lexical path.
                        // Under assembly the scope is applied there instead,
                        // because a row outside the scope may be a field a
                        // scoped source reaches through a join.
                        .filter((e) => assembling || matchesScope(e, request));
                     return matches.map((e) => ({
                        ...projectEntity(e, environmentName, packageName),
                        score: round4(hit.score),
                        rawScore: hit.score,
                        targetScores: hit.targetScores,
                     }));
                  });
                  for (const row of ranked) {
                     // Keyed per card (model path included), so this does not
                     // collapse the fan-out above back into one entity.
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
               // belowCutoffCount comes straight from the scan: derived from
               // the returned rows it would count a crowded-out entity (one
               // that cleared the floor but did not fit) as rejected.
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
            // trySemanticSearch does not throw, but the storage handle lookup
            // can (e.g. before initialization).
            const message =
               error instanceof Error ? error.message : String(error);
            logger.warn(
               "[MCP Tool getContext] Semantic retrieval unavailable",
               { error: message },
            );
            // The cause goes to the log only: nothing in it is actionable.
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
      // One lunr pass per target, each normalized against its own top hit
      // before merging: raw lunr scores are relative to their own query.
      const bestByRef = new Map<string, Map<number, number>>();
      for (const search of request.searches) {
         const sanitized = sanitizeLunrQuery(search.text);
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
      const ranking = [...bestByRef.entries()]
         .flatMap(([ref, targetScores]) => {
            const e = byId.get(ref);
            if (!e) return [];
            if (sourceName && e.source !== sourceName) return [];
            if (!matchesScope(e, request)) return [];
            // The row ranks on its best target, as on the semantic path.
            const score = Math.max(...targetScores.values());
            return [{ e, score, targetScores }];
         })
         .sort((a, b) => b.score - a.score);

      const scored: ResultEntity[] = ranking.map(({ e, targetScores }) => ({
         ...projectEntity(e, environmentName, packageName),
         // Which target found the row is carried, for the per-target window;
         // how well is not: targetScores would publish a lexical score as
         // matched_targets[].relevance, and lunr scores are not comparable.
         bestTarget: bestTargetOf(targetScores),
      }));
      return { rows: scored, belowCutoffCount: 0 };
   },
};
