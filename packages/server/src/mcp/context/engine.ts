// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A ranked get_context request answered by find, connect, judge. Reached only
 * for a package whose publisher.json sets `retrieval.engine: "fcj"`; listings
 * and every other package take the shipped path in get_context_tool.ts,
 * unchanged.
 *
 *   find (Stage 1)    per target, the N best nodes of the whole catalog
 *   connect (Stage 2) the few sources whose reach explains the targets together
 *   judge (Stage 3)   one model call per cluster picks what to return
 *
 * Each stage is timed and traced the way the shipped stages are, so the
 * `X-Publisher-Retrieval-Trace: summary` header gives the same block, and
 * `full` adds what each stage kept and why.
 */

import { embeddingConfigured } from "../../service/embedding_provider";
import type { EnvironmentStore } from "../../service/environment_store";
import { fcjSettingsOf } from "../../service/package_retrieval";
import { EMBEDDING_QUERY_TIMEOUT_MS } from "../../service/embedding_provider";
import { logger } from "../../logger";
import { jsonResource, jsonToolError } from "../tool_response";
import {
   projectEntity,
   toSourceResults,
   type PackageIndex,
   type ResolvedRequest,
   type ResultEntity,
} from "../tools/get_context_tool";
import type { CatalogNode } from "./catalog";
import { fcjStateOf, kickFcjBuild } from "./engine_state";
import {
   find,
   KINDS_BY_FIND_TARGET,
   type FindResult,
   type FindTarget,
} from "./find";
import type { FcjIndex } from "./index_build";
import { connect, type ConnectResult } from "./connect";
import { routesOf } from "./routes";
import { toUnit } from "./vectors";

export type TraceMode = "none" | "summary" | "full";

/** One row of the stage trace, in the shipped trace's shape. */
interface StageRow {
   name: string;
   status: "ran" | "skipped" | "failed";
   ms: number;
   in: number;
   out: number;
   llm_calls: number;
   tokens: { input: number; output: number };
}

/** The targets the engine works on, from the resolved request, in the caller's order. */
export function findTargetsOf(request: ResolvedRequest): FindTarget[] {
   const targets: FindTarget[] = [
      ...request.searches.map((s) => ({
         index: s.targetIndex,
         type: s.targetType as FindTarget["type"],
         text: s.text,
         kinds: KINDS_BY_FIND_TARGET[s.targetType as FindTarget["type"]],
         ...(request.exampleValues?.get(s.targetIndex)
            ? { exampleValues: request.exampleValues.get(s.targetIndex) }
            : {}),
      })),
      ...request.valueSearches.map((s) => ({
         index: s.targetIndex,
         type: "dimensional_value" as const,
         text: s.text,
         kinds: KINDS_BY_FIND_TARGET.dimensional_value,
         ...(request.exampleValues?.get(s.targetIndex)
            ? { exampleValues: request.exampleValues.get(s.targetIndex) }
            : {}),
      })),
   ];
   return targets.sort((a, b) => a.index - b.index);
}

/** Whether a node survives the request's model, source and entity scope. */
export function inScope(request: ResolvedRequest) {
   return (node: CatalogNode): boolean => {
      if (request.modelPath && !node.modelPaths.includes(request.modelPath)) {
         return false;
      }
      if (request.sourceName && node.source !== request.sourceName)
         return false;
      if (
         request.entityName &&
         node.kind !== "source" &&
         node.name !== request.entityName
      ) {
         return false;
      }
      return true;
   };
}

/** Embed every target's text in one call; an empty map with no provider. */
async function embedTargets(
   index: FcjIndex,
   targets: readonly FindTarget[],
): Promise<Map<number, Float32Array>> {
   const out = new Map<number, Float32Array>();
   const provider = index.provider;
   if (!provider || targets.length === 0) return out;
   const vectors = await provider.embedBatch(
      targets.map((t) => (provider.queryPrefix ?? "") + t.text),
      EMBEDDING_QUERY_TIMEOUT_MS,
   );
   targets.forEach((t, i) => out.set(t.index, toUnit(vectors[i])));
   return out;
}

/** A node as the shipped card builder wants it, under one of its model paths. */
export function resultRowOf(
   node: CatalogNode,
   request: ResolvedRequest,
   score: number | undefined,
   targetScores?: Map<number, number>,
): ResultEntity {
   const entity =
      node.entities.find((e) => e.modelPath === request.modelPath) ??
      node.entities[0];
   return {
      ...projectEntity(entity, request.environmentName, request.packageName),
      ...(score !== undefined
         ? { score: Math.round(score * 10_000) / 10_000, rawScore: score }
         : {}),
      ...(targetScores ? { targetScores } : {}),
   };
}

/** The find stage's part of the full trace. */
export function findTrace(index: FcjIndex, results: readonly FindResult[]) {
   return results.map((r) => ({
      target_index: r.target.index,
      target_type: r.target.type,
      text: r.target.text,
      lists: r.lists,
      value_phrases: r.valuePhrases,
      candidates: r.candidates.map((c, rank) => {
         const node = index.catalog.nodes.get(c.key)!;
         return {
            rank: rank + 1,
            id: node.id,
            key: c.key,
            score: Math.round(c.score * 1e6) / 1e6,
            ranks: c.ranks,
            ...(c.cosine !== undefined
               ? { cosine: c.cosine, facet: c.facet }
               : {}),
            ...(c.bm25 !== undefined ? { bm25: c.bm25 } : {}),
            ...(c.values
               ? { values: c.values.slice(0, 3).map((v) => v.value) }
               : {}),
            routes: routesOf(index.catalog, node),
         };
      }),
   }));
}

export async function runFcjQuery(args: {
   request: ResolvedRequest;
   environmentStore: EnvironmentStore;
   pkgIndex: PackageIndex;
   uri: string;
   warnings: string[];
   traceMode: TraceMode;
}): Promise<ReturnType<typeof jsonResource>> {
   const { request, environmentStore, pkgIndex, uri, traceMode } = args;
   const pkg = pkgIndex.pkg;
   const state = fcjStateOf(pkg);
   if (!state || state.status !== "ready") {
      kickFcjBuild({
         environmentStore,
         environmentName: request.environmentName,
         packageName: request.packageName,
         pkg,
      });
      if (state?.status === "error") {
         return jsonToolError(
            uri,
            {
               message: `The fcj search index for this package could not be built: ${state.message}`,
               suggestions: [
                  "Ask again in a minute; the build is retried by the next question after a short cool-down.",
                  "The operator should read the server log. To use the shipped search instead, remove retrieval.engine from the package's publisher.json.",
               ],
            },
            {
               sources: [],
               retrieval: "error",
               retrieval_reason: "unavailable",
            },
         );
      }
      return jsonResource(uri, {
         sources: [],
         ranking: "relevance" as const,
         total_available: 0,
         returned: 0,
         retrieval: "indexing",
         warnings: [
            ...args.warnings,
            "This package's search index is still being built, so there are no results yet. Nothing is wrong: ask the same question again in a few seconds. A request with no search_text (a listing) works now.",
         ],
      });
   }
   const index = state.index;
   const settings = fcjSettingsOf(pkg.getRetrievalSettings?.());
   const stages: StageRow[] = [];
   const targets = findTargetsOf(request);
   const allow = inScope(request);

   // Stage 1.
   let started = performance.now();
   let queries = new Map<number, Float32Array>();
   try {
      queries = embeddingConfigured()
         ? await embedTargets(index, targets)
         : new Map();
   } catch (error) {
      logger.warn("[get_context fcj] The search text could not be embedded", {
         error: error instanceof Error ? error.message : String(error),
      });
      return jsonToolError(
         uri,
         {
            message: `The embedding provider failed to embed the search text: ${error instanceof Error ? error.message : String(error)}`,
            suggestions: ["Try again in a minute."],
         },
         {
            sources: [],
            retrieval: "error",
            retrieval_reason: "provider-error",
         },
      );
   }
   const found = find({
      index,
      targets,
      queries,
      perTarget: settings.perTarget,
      keyword: settings.keyword,
      values: settings.values,
      allow,
   });
   stages.push({
      name: "find",
      status: "ran",
      ms: Math.round(performance.now() - started),
      in: targets.length,
      out: found.reduce((n, r) => n + r.candidates.length, 0),
      llm_calls: 0,
      tokens: { input: 0, output: 0 },
   });

   // Stage 2.
   started = performance.now();
   const connected = connect({ index, found, queries, settings, allow });
   stages.push({
      name: "connect",
      status: "ran",
      ms: Math.round(performance.now() - started),
      in: found.reduce((n, r) => n + r.candidates.length, 0),
      out: connected.clusters.length,
      llm_calls: 0,
      tokens: { input: 0, output: 0 },
   });

   // Until the judge exists, each cluster answers each target it covers with
   // its best few in-cluster candidates.
   started = performance.now();
   const rows: ResultEntity[] = [];
   const searchTexts = new Map(targets.map((t) => [t.index, t.text]));
   const typeOf = new Map(targets.map((t) => [t.index, t.type]));
   for (const cluster of connected.clusters) {
      for (const t of cluster.covers) {
         const list = cluster.candidates.get(t) ?? [];
         for (const c of list.slice(0, unjudgedPicks(typeOf.get(t)))) {
            const node = index.catalog.nodes.get(c.key)!;
            rows.push(
               resultRowOf(node, request, c.score, new Map([[t, c.score]])),
            );
         }
      }
   }
   const sources = toSourceResults(
      rows,
      pkgIndex.sourceContext,
      request.environmentName,
      request.packageName,
      searchTexts,
      request.includeCode,
      pkgIndex.droppedSources,
   );
   stages.push({
      name: "respond",
      status: "ran",
      ms: Math.round(performance.now() - started),
      in: rows.length,
      out: sources.length,
      llm_calls: 0,
      tokens: { input: 0, output: 0 },
   });
   return jsonResource(uri, {
      sources,
      ranking: "relevance" as const,
      total_available: sources.length,
      returned: sources.length,
      retrieval: "semantic",
      engine: "fcj",
      ...(args.warnings.length > 0 ? { warnings: args.warnings } : {}),
      ...(traceMode !== "none"
         ? { retrieval_trace: { engine: "fcj", stages } }
         : {}),
      ...(traceMode === "full"
         ? {
              retrieval_trace_full: {
                 fcj: {
                    find: findTrace(index, found),
                    connect: connectTrace(index, connected),
                 },
              },
           }
         : {}),
   });
}

/** Candidates a cluster returns per covered target when no judge picks. */
export function unjudgedPicks(type: FindTarget["type"] | undefined): number {
   if (type === "measure") return 2;
   if (type === "dimension") return 5;
   return 3;
}

/** The connect stage's part of the full trace. */
export function connectTrace(index: FcjIndex, connected: ConnectResult) {
   const describe = (key: string) => {
      const node = index.catalog.nodes.get(key)!;
      return { id: node.id, key, routes: routesOf(index.catalog, node) };
   };
   return {
      considered: connected.considered.map((s) => ({
         source: s.source,
         score: Math.round(s.score * 1e6) / 1e6,
         covers: s.covers,
         best: Object.fromEntries(
            [...s.best].map(([t, b]) => [
               t,
               { id: index.catalog.nodes.get(b.key)?.id, hops: b.hops },
            ]),
         ),
      })),
      unresolved: connected.unresolved,
      clusters: connected.clusters.map((c) => ({
         home: c.home,
         home_source: index.catalog.sources.get(c.home)?.name,
         score: Math.round(c.score * 1e6) / 1e6,
         covers: c.covers,
         newly_covers: c.newlyCovers,
         twins: c.twins,
         reach: [...c.reach.values()].map((r) => ({
            source: r.source,
            hops: r.hops,
         })),
         candidates: [
            ...[...c.candidates].flatMap(([t, list]) =>
               list.map((x) => ({
                  target_index: t,
                  rank: x.rank,
                  score: Math.round(x.score * 1e6) / 1e6,
                  hops: x.hops,
                  ...describe(x.key),
               })),
            ),
            ...c.extras.map((e) => ({ extra: e.reason, ...describe(e.key) })),
         ],
      })),
   };
}
