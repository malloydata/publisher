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
 * The loops here are also the one place that times a stage and records what it
 * did (see StageTrace), so a stage never has to.
 */

import type { EnvironmentStore } from "../../service/environment_store";
import type { EmbeddingIndexStatus } from "./embedding_index";
import type { LlmMeter } from "./get_context_llm";
import type {
   PackageIndex,
   ResolvedRequest,
   ResultEntity,
   RetrievalReason,
} from "./get_context_tool";

/** Every switch the pipeline reads, in one place. */
export interface PipelineSettings {
   /**
    * "index": joined copies are index rows, searched like any field.
    * "assembly": the semantic path searches direct fields only and assembly
    * makes the joined copies from the join topology, damped.
    */
   joins: "index" | "assembly";
   entityWindow: {
      /**
       * Rows kept per source, per search target: the semantic scan's window
       * (best rows by distance in each source) and, in assembly, the most
       * entities a card carries for one target.
       */
      perSourcePerTarget: number;
   };
   /**
    * Deepest join chain assembly follows. The lexical index has its own,
    * lower limit (it makes one entity per path); this never reaches it.
    */
   joinMaxDepth: number;
   /** Base of the score multiplier for assembled join copies, applied as `base ** (hops + 1)`; null means none. */
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
   /**
    * What the stages ran and how long each took, in order. Always collected;
    * it reaches the caller only when the request carries the trace header.
    * Optional so a test that builds a bare context needs neither.
    */
   trace?: StageTrace[];
   /** Counts the chat calls and tokens of this request; see LlmMeter. */
   meter?: LlmMeter;
}

/** One row of the stage trace. `in` and `out` count what the stage's phase works on. */
export interface StageTrace {
   name: string;
   /** `skipped`: the stage was off or not applicable. `failed`: it threw. */
   status: "ran" | "skipped" | "failed";
   ms: number;
   /** Search texts (query stage), ranked rows (rank stage) or cards (card stage). */
   in: number;
   out: number;
   llmCalls: number;
   tokens: { input: number; output: number };
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
   /**
    * Semantic only: per source (`""` for none), the entities that cleared the
    * floor and the scan's per-source window left out. Assembly adds them to
    * each card of that source, because it never sees the rows.
    */
   entitiesCutBySource?: Map<string, number>;
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
   /** `state` is passed so a stage can tell, for example, which retriever ranked. */
   enabled(ctx: PipelineContext, state: CardState): boolean;
   run(state: CardState, ctx: PipelineContext): Promise<CardState>;
}

/** Runs after retrieval and before the rows are assembled into cards. */
export interface RankStage {
   name: string;
   /** `state` is passed so a stage can tell, for example, which retriever ranked. */
   enabled(ctx: PipelineContext, state: RankedState): boolean;
   run(state: RankedState, ctx: PipelineContext): Promise<RankedState>;
}

/**
 * Run one stage if it is enabled, and record a trace row either way. A stage
 * that throws is recorded as failed and the error continues to the caller,
 * which decides what the response says.
 */
async function runStage<S>(
   stage: { name: string },
   isEnabled: boolean,
   ctx: PipelineContext,
   state: S,
   count: (state: S) => number,
   run: () => Promise<S>,
): Promise<S> {
   const before = count(state);
   if (!isEnabled) {
      ctx.trace?.push({
         name: stage.name,
         status: "skipped",
         ms: 0,
         in: before,
         out: before,
         llmCalls: 0,
         tokens: { input: 0, output: 0 },
      });
      return state;
   }
   const started = performance.now();
   const used = ctx.meter?.snapshot();
   const row = (status: StageTrace["status"], out: number): StageTrace => {
      const now = ctx.meter?.snapshot();
      return {
         name: stage.name,
         status,
         ms: Math.round(performance.now() - started),
         in: before,
         out,
         llmCalls: now && used ? now.calls - used.calls : 0,
         tokens: {
            input: now && used ? now.inputTokens - used.inputTokens : 0,
            output: now && used ? now.outputTokens - used.outputTokens : 0,
         },
      };
   };
   try {
      const next = await run();
      ctx.trace?.push(row("ran", count(next)));
      return next;
   } catch (error) {
      ctx.trace?.push(row("failed", before));
      throw error;
   }
}

export async function runQueryStages(
   stages: readonly QueryStage[],
   request: ResolvedRequest,
   ctx: PipelineContext,
): Promise<ResolvedRequest> {
   let current = request;
   for (const stage of stages) {
      current = await runStage(
         stage,
         stage.enabled(ctx),
         ctx,
         current,
         (r) => r.searches.length,
         () => stage.run(current, ctx),
      );
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
      current = await runStage(
         stage,
         stage.enabled(ctx, current),
         ctx,
         current,
         (s) => s.rows.length,
         () => stage.run(current, ctx),
      );
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
      current = await runStage(
         stage,
         stage.enabled(ctx, current),
         ctx,
         current,
         (s) => s.cards.length,
         () => stage.run(current, ctx),
      );
   }
   return current;
}
