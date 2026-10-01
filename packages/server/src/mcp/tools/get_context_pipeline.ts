// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The shape of a get_context query after it has been resolved: stages that run
 * before retrieval, retrievers that rank, and stages that run after. This file
 * holds only these types and the two loops that run stages; the stages
 * themselves live in their own files, and runContextQuery in
 * get_context_tool.ts decides the order.
 *
 * Expected to plug in later: query rephrase as a QueryStage; refine/prune
 * and value attach as RankStages; rerank and prune as CardStages, which run on
 * assembled source cards before paging.
 *
 * Every list is empty or fixed today, so a loop here is a no-op until a stage
 * is registered.
 */

import type { EnvironmentStore } from "../../service/environment_store";
import type { EmbeddingIndexStatus } from "./embedding_index";
import type {
   PackageIndex,
   ResolvedRequest,
   ResultEntity,
   RetrievalReason,
} from "./get_context_tool";

/**
 * Every switch a later stage or hosted mode will read, in one place. The
 * values runContextQuery passes are today's behaviour; fields marked
 * "unused" are not read by anything yet.
 */
export interface PipelineSettings {
   /** "index": joined copies are index rows (today). "assembly": made after refine. Unused. */
   joins: "index" | "assembly";
   /** Where the per-source cap applies and how many rows it admits. */
   entityWindow: {
      /** Unused: the cap always runs in assembly, after the rank stages. */
      where: "post-rank" | "retrieve";
      perSourcePerTarget: number;
   };
   /** Deepest join chain the index follows. Unused here; the index reads its own constant. */
   joinMaxDepth: number;
   /** Per-hop score multiplier for assembled join copies; null means none. Unused. */
   joinDamping: number | null;
   /** How scores are published. Unused. */
   scoring: "cosine" | "knots";
   /** Response size budget in characters; null means no budget. */
   maxChars: number | null;
   /** Characters held back from maxChars for the envelope. */
   reserveChars: number;
}

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
   settings: PipelineSettings;
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
 * provider": the caller ranks lexically, which is the server's mode and not a
 * fallback. Any other reason is a configured server that cannot answer a
 * search right now, and the caller reports it instead of answering lexically.
 */
export interface Unavailable {
   unavailable: RetrievalReason | "unconfigured";
   /** The package's index state when the reason was found, for the message. */
   status?: EmbeddingIndexStatus;
   /** Reason-specific text the status cannot carry (an invalid configuration). */
   detail?: string;
}

export interface Retriever {
   name: "semantic" | "lexical";
   retrieve(ctx: PipelineContext): Promise<RetrievalResult | Unavailable>;
}

/** The ranked rows plus how they were found, passed through RankStages. */
export interface RankedState extends RetrievalResult {
   retrieval: Retriever["name"];
}

/**
 * One source card before it is turned into wire JSON. Rows keep the full
 * ranked entity, so a later stage can read what the wire form drops.
 */
export interface CardDraft {
   /** sourceContextKey(modelPath, source). */
   key: string;
   modelPath: string;
   source: string;
   /** Best score among the rows, as the wire card's `relevance`. */
   relevance?: number;
   /** In rank order. Includes the `kind: "source"` row when the source matched. */
   rows: ResultEntity[];
   /** Rows the per-source, per-target cap left out of this card. */
   entitiesDropped: number;
}

/** What the card stages and shapeCards see: every card, before paging. */
export interface CardState extends Omit<RankedState, "rows"> {
   /** Best-first: the order sources first appear in the ranked rows. */
   cards: CardDraft[];
}

/** Runs after assembly and before paging. Rerank and prune live here. */
export interface CardStage {
   name: string;
   enabled(ctx: PipelineContext): boolean;
   run(state: CardState, ctx: PipelineContext): Promise<CardState>;
}

/** Runs after retrieval and before the rows are assembled into cards. */
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

export async function runCardStages(
   stages: readonly CardStage[],
   state: CardState,
   ctx: PipelineContext,
): Promise<CardState> {
   let current = state;
   for (const stage of stages) {
      if (stage.enabled(ctx)) current = await stage.run(current, ctx);
   }
   return current;
}
