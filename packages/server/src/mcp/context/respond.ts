// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The answer of the fcj engine, in the shipped response's shape plus three
 * blocks the shipped engine does not have.
 *
 * `sources[]` is built from the judge's picks with the shipped card builder,
 * so a caller that reads cards keeps working. A pick on the home source (or a
 * twin) is on its own card. A pick on a source the home reaches by joins is
 * presented on the home's card under every route the home has to it
 * (`origin.region_name` and `arrival.region_name` are two answers, not one),
 * each with a fixed sentence saying the field is seen through a left join.
 * Picking one route would pick a population; listing them leaves that choice
 * to the reader, who has the question.
 *
 * Values matched in the value index ride under their dimension as `values[]`,
 * on its own card only, never on a joined copy. A `dimensional_value` target
 * whose dimension is on a gated source is answered by the query-time lookup
 * with the caller's givens, so the gate decides, as it does for a query.
 *
 * `clusters` (home, reach, which targets each answers, the picks and the
 * judge's caveats), `coverage` (target by cluster, when there is more than
 * one cluster) and `unresolved_targets` (targets no cluster answered) are
 * this engine's own, and appear only on a package that opted into it.
 */

import type { EnvironmentStore } from "../../service/environment_store";
import {
   defaultValueFetcher,
   type ValueFetcher,
} from "../tools/get_context_values";
import { matchValues } from "../tools/get_context_value_match";
import {
   fitBudget,
   projectEntity,
   sourceContextKey,
   summaryViewFor,
   toSourceResults,
   type PackageIndex,
   type ResolvedRequest,
   type ResultEntity,
} from "../tools/get_context_tool";
import type { CatalogNode, CatalogSource } from "./catalog";
import type { Cluster, ConnectResult } from "./connect";
import type { FindTarget } from "./find";
import type { FcjIndex } from "./index_build";
import type { JudgeVerdict } from "./judge";

/** The response budget, the shipped engine's. */
export const FCJ_MAX_CHARS = 35_000;
const FCJ_RESERVE_CHARS = 1_000;

/** Distinct values the query-time lookup reads for a gated dimension. */
const GATED_LOOKUP_LIMIT = 2_000;
const GATED_LOOKUP_TIMEOUT_MS = 8_000;

export interface FcjAnswer {
   sources: unknown[];
   clusters: unknown[];
   coverage?: unknown[];
   unresolved_targets: unknown[];
   /** Whole cards the size budget removed. */
   budgetDropped: number;
   warnings: string[];
}

/** The model path a source's card is built under: the request's, else the defining file's. */
export function cardModelPath(
   source: CatalogSource,
   request: Pick<ResolvedRequest, "modelPath">,
): string {
   if (request.modelPath && source.modelPaths.includes(request.modelPath)) {
      return request.modelPath;
   }
   return source.modelPaths.includes(source.definedIn)
      ? source.definedIn
      : source.modelPaths[0];
}

/** The sentence a field reached through a join carries. */
export function populationNote(home: string, path: readonly string[]): string {
   return `Reached from ${home} through ${path.join(".")}, a left join: a ${home} row with no match keeps a null here, so grouping or counting by it follows ${home}'s rows.`;
}

function entityAt(node: CatalogNode, modelPath: string) {
   return (
      node.entities.find((e) => e.modelPath === modelPath) ?? node.entities[0]
   );
}

export async function respond(args: {
   index: FcjIndex;
   request: ResolvedRequest;
   pkgIndex: PackageIndex;
   connected: ConnectResult;
   verdicts: readonly JudgeVerdict[];
   targets: readonly FindTarget[];
   environmentStore: EnvironmentStore;
   /** Test seam for the gated lookup; the query path otherwise. */
   valueFetcher?: ValueFetcher;
}): Promise<FcjAnswer> {
   const { index, request, connected, verdicts, targets } = args;
   const warnings: string[] = [];
   const targetOf = new Map(targets.map((t) => [t.index, t]));
   const rows = new Map<string, ResultEntity>();
   const order: string[] = [];
   // Published relevance: a pick's in-cluster score over the best score of
   // the request, so the scale reads like the shipped engine's (0 to 1).
   let maxScore = 0;
   for (const c of connected.clusters) {
      for (const list of c.candidates.values()) {
         for (const x of list) maxScore = Math.max(maxScore, x.score);
      }
   }
   const publish = (s: number) => (maxScore > 0 ? s / maxScore : 0);

   const put = (
      key: string,
      row: ResultEntity,
      target?: number,
      score?: number,
      reason?: string,
   ) => {
      const prev = rows.get(key);
      const target_ = target;
      if (prev) {
         if (target_ !== undefined && score !== undefined) {
            prev.targetScores ??= new Map();
            prev.targetScores.set(
               target_,
               Math.max(prev.targetScores.get(target_) ?? 0, score),
            );
            prev.score = Math.max(prev.score ?? 0, score);
            prev.rawScore = prev.score;
         }
         if (reason && target_ !== undefined) {
            prev.targetReasons ??= new Map();
            prev.targetReasons.set(target_, reason);
         }
         return prev;
      }
      if (target_ !== undefined && score !== undefined) {
         row.targetScores = new Map([[target_, score]]);
         row.score = score;
         row.rawScore = score;
      }
      if (reason && target_ !== undefined)
         row.targetReasons = new Map([[target_, reason]]);
      rows.set(key, row);
      order.push(key);
      return row;
   };

   const clustersOut: unknown[] = [];
   for (const verdict of verdicts) {
      const cluster = connected.clusters.find(
         (c) => c.home === verdict.cluster,
      );
      if (!cluster) continue;
      const home = index.catalog.sources.get(cluster.home)!;
      const homePath = cardModelPath(home, request);
      const homeRowKey = `${cluster.home}\u0000source`;
      put(homeRowKey, {
         ...projectEntity(
            entityAt(home.node, homePath),
            request.environmentName,
            request.packageName,
         ),
         modelPath: homePath,
         score: publish(
            cluster.candidates.get(cluster.covers[0] ?? -1)?.[0]?.score ?? 0,
         ),
      });
      const picksOut: Record<number, string[]> = {};
      for (const pick of verdict.picks) {
         const target = targetOf.get(pick.target);
         for (const key of pick.keys) {
            const node = index.catalog.nodes.get(key);
            if (!node) continue;
            const found = cluster.candidates
               .get(pick.target)
               ?.find((c) => c.key === key);
            const score = publish(found?.score ?? 0);
            const placed = await place(
               args,
               cluster,
               node,
               home,
               homePath,
               target,
               found?.found.values,
               warnings,
            );
            for (const p of placed) {
               put(p.key, p.row, pick.target, score, pick.reason);
               (picksOut[pick.target] ??= []).push(p.id);
            }
         }
      }
      clustersOut.push({
         home: { source: home.name, model_path: homePath },
         reach: [...cluster.reach.values()]
            .filter((r) => r.hops > 0)
            .map((r) => ({
               source: index.catalog.sources.get(r.source)?.name,
               joins: r.paths.map((p) => p.join(".")),
            })),
         ...(cluster.twins.length > 0
            ? {
                 twins: cluster.twins.map(
                    (t) => index.catalog.sources.get(t)?.name,
                 ),
              }
            : {}),
         ...(cluster.contenders.length > 0
            ? {
                 contenders: cluster.contenders.map(
                    (t) => index.catalog.sources.get(t)?.name,
                 ),
              }
            : {}),
         covers: cluster.covers,
         picks: picksOut,
         ...(verdict.caveats.length > 0 ? { caveats: verdict.caveats } : {}),
      });
   }

   // Every card a pick lands on is built under one model path per source.
   const ordered = order.map((k) => rows.get(k)!);
   const stored = new Map<
      string,
      { summary: string; oneLineSummary: string }
   >();
   for (const source of index.catalog.sources.values()) {
      const line = index.summaries.get(source.key);
      if (!line) continue;
      for (const mp of source.modelPaths) {
         stored.set(sourceContextKey(mp, source.name), {
            summary: line,
            oneLineSummary: line,
         });
      }
   }
   const view = summaryViewFor(stored, request, []);
   const built = toSourceResults(
      ordered,
      args.pkgIndex.sourceContext,
      request.environmentName,
      request.packageName,
      new Map(targets.map((t) => [t.index, t.text])),
      request.includeCode,
      args.pkgIndex.droppedSources,
      view,
   );
   const fitted = fitBudget(built, FCJ_MAX_CHARS, FCJ_RESERVE_CHARS);

   const picked = new Set<number>();
   for (const v of verdicts)
      for (const p of v.picks) if (p.keys.length > 0) picked.add(p.target);
   const unresolved = targets
      .filter((t) => !picked.has(t.index))
      .map((t) => ({
         target_index: t.index,
         target_type: t.type,
         search_text: t.text,
      }));
   const coverage =
      connected.clusters.length > 1
         ? targets.map((t) => ({
              target_index: t.index,
              search_text: t.text,
              clusters: connected.clusters.map((c) =>
                 c.covers.includes(t.index),
              ),
           }))
         : undefined;
   return {
      sources: fitted.cards,
      clusters: clustersOut,
      ...(coverage ? { coverage } : {}),
      unresolved_targets: unresolved,
      budgetDropped: fitted.dropped,
      warnings,
   };
}

/**
 * Where one pick goes: its own card when it is on the home or a twin, or the
 * home's card under each route the home has to it.
 */
async function place(
   args: Parameters<typeof respond>[0],
   cluster: Cluster,
   node: CatalogNode,
   home: CatalogSource,
   homePath: string,
   target: FindTarget | undefined,
   matched: ReadonlyArray<{ value: string; score: number }> | undefined,
   warnings: string[],
): Promise<Array<{ key: string; id: string; row: ResultEntity }>> {
   const { index, request } = args;
   const owner = index.catalog.sources.get(node.sourceKey)!;
   // Routes are the home's own: a field a twin reaches is shown on its own card.
   const entry = cluster.homeReach.get(node.sourceKey);
   const own =
      node.sourceKey === cluster.home ||
      cluster.twins.includes(node.sourceKey) ||
      cluster.contenders.includes(node.sourceKey) ||
      !entry ||
      entry.hops === 0;
   if (own) {
      const mp = cardModelPath(owner, request);
      const row: ResultEntity = {
         ...projectEntity(
            entityAt(node, mp),
            request.environmentName,
            request.packageName,
         ),
         modelPath: mp,
      };
      const values = await valuesFor(
         args,
         node,
         owner,
         target,
         matched,
         warnings,
      );
      if (values.length > 0) row.values = values;
      return [
         {
            key: `${node.sourceKey}\u0000${node.kind}\u0000${node.name}`,
            id: node.id,
            row,
         },
      ];
   }
   // Through joins: one row per route, on the home's card.
   const out: Array<{ key: string; id: string; row: ResultEntity }> = [];
   for (const path of entry.paths) {
      if (path.length === 0) continue;
      const name = `${path.join(".")}.${node.name}`;
      const doc = node.displayDoc
         ? `${node.displayDoc} ${populationNote(home.name, path)}`
         : populationNote(home.name, path);
      const row: ResultEntity = {
         kind: node.kind,
         name,
         source: home.name,
         environmentName: request.environmentName,
         packageName: request.packageName,
         modelPath: homePath,
         doc,
         ...(node.doc ? { embedDoc: node.doc } : {}),
         joinPath: path.join("."),
         ...(entry.fanout ? { relationship: entry.fanout } : {}),
         ...(node.dataType ? { dataType: node.dataType } : {}),
         ...(node.code ? { code: node.code } : {}),
      };
      out.push({
         key: `${cluster.home}\u0000${node.kind}\u0000${name}`,
         id: `${node.kind}:${home.name}:${name}`,
         row,
      });
   }
   return out;
}

/** The stored values a dimension pick carries for a value target. */
async function valuesFor(
   args: Parameters<typeof respond>[0],
   node: CatalogNode,
   owner: CatalogSource,
   target: FindTarget | undefined,
   matched: ReadonlyArray<{ value: string; score: number }> | undefined,
   warnings: string[],
): Promise<Array<{ value: string; score: number; searchText: string }>> {
   if (!target || node.kind !== "dimension") return [];
   if (target.type !== "dimension" && target.type !== "dimensional_value")
      return [];
   const phrase = target.exampleValues?.[0] ?? target.text;
   if (matched && matched.length > 0) {
      return matched
         .slice(0, 10)
         .map((v) => ({ value: v.value, score: v.score, searchText: phrase }));
   }
   // A gated source's values are never in the shared index; a value target
   // asks the query path with the caller's givens instead.
   if (target.type !== "dimensional_value" || !owner.gated) return [];
   if (node.dataType && node.dataType !== "string") return [];
   const { request } = args;
   try {
      const got = await (args.valueFetcher ?? defaultValueFetcher)({
         environmentStore: args.environmentStore,
         environmentName: request.environmentName,
         packageName: request.packageName,
         modelPath: cardModelPath(owner, request),
         source: node.source,
         dimension: node.name,
         limit: GATED_LOOKUP_LIMIT,
         ...(request.givens ? { givens: request.givens } : {}),
         ...(request.filterParams
            ? { filterParams: request.filterParams }
            : {}),
         timeoutMs: GATED_LOOKUP_TIMEOUT_MS,
      });
      return matchValues(phrase, got.values).map((h) => ({
         value: h.value,
         score: h.score,
         searchText: phrase,
      }));
   } catch (error) {
      warnings.push(
         `The values of ${node.source}.${node.name} were not searched: ${error instanceof Error ? error.message.slice(0, 200) : String(error)}. The source has an access gate; send the givens it needs, or read its values with execute_query.`,
      );
      return [];
   }
}
