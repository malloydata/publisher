// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The shape of a get_context query after it has been resolved: stages that run
 * before retrieval, retrievers that rank, and stages that run after. This file
 * holds only these types and the two loops that run stages; the stages
 * themselves live in their own files, and runContextQuery in
 * get_context_tool.ts decides the order.
 *
 * Expected to plug in later: query rephrase as a QueryStage; refine/prune,
 * rerank and value attach as RankStages.
 *
 * Every list is empty or fixed today, so a loop here is a no-op until a stage
 * is registered.
 */

import type { EnvironmentStore } from "../../service/environment_store";
import type {
   PackageIndex,
   ResolvedRequest,
   ResultEntity,
   RetrievalReason,
} from "./get_context_tool";

/** What every stage and retriever sees. */
export interface PipelineContext {
   /** The request being answered. Query stages may replace it. */
   request: ResolvedRequest;
   environmentStore: EnvironmentStore;
   pkgIndex: PackageIndex;
   /**
    * Warnings that open the payload's `warnings`, in order: staleness, the
    * caller's extras, then anything a stage appends. Path-specific cut
    * warnings are added after these when the payload is built.
    */
   warnings: string[];
   /** Whether an embedding provider is configured; read once per request. */
   embeddingConfigured: boolean;
}

/** Runs before retrieval and may rewrite the request (e.g. its search texts). */
export interface QueryStage {
   name: string;
   enabled(ctx: PipelineContext): boolean;
   run(
      request: ResolvedRequest,
      ctx: PipelineContext,
   ): Promise<ResolvedRequest>;
}

/** What a retriever hands back when it ranked the package. */
export interface RetrievalResult {
   /** Ranked but not yet collapsed or windowed; finishRanked does both. */
   rows: ResultEntity[];
   /** Entities under the relevance floor. Meaningful on semantic only. */
   belowCutoffCount: number;
   /** The denominator belowCutoffCount is read against. Semantic only. */
   totalEntities?: number;
}

/**
 * A retriever that cannot answer says why. `unconfigured` means "no embedding
 * provider": the caller falls back silently, with no `retrieval_reason`.
 */
export interface Unavailable {
   unavailable: RetrievalReason | "unconfigured";
}

export interface Retriever {
   name: "semantic" | "lexical";
   retrieve(ctx: PipelineContext): Promise<RetrievalResult | Unavailable>;
}

/** The ranked rows plus how they were found, passed through RankStages. */
export interface RankedState extends RetrievalResult {
   retrieval: Retriever["name"];
   /** Why a configured server answered lexically, when it did. */
   retrievalReason?: RetrievalReason;
}

/** Runs after retrieval and before the rows are windowed and serialized. */
export interface RankStage {
   name: string;
   enabled(ctx: PipelineContext): boolean;
   run(state: RankedState, ctx: PipelineContext): Promise<RankedState>;
}

export async function runQueryStages(
   stages: readonly QueryStage[],
   request: ResolvedRequest,
   ctx: PipelineContext,
): Promise<ResolvedRequest> {
   let current = request;
   for (const stage of stages) {
      if (stage.enabled(ctx)) current = await stage.run(current, ctx);
   }
   return current;
}

export async function runRankStages(
   stages: readonly RankStage[],
   state: RankedState,
   ctx: PipelineContext,
): Promise<RankedState> {
   let current = state;
   for (const stage of stages) {
      if (stage.enabled(ctx)) current = await stage.run(current, ctx);
   }
   return current;
}
