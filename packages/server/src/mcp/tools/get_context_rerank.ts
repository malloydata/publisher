// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Rerank: one LLM call scores whole sources against the user's question, so a
 * source that merely holds one well-matching field does not outrank the source
 * that can answer.
 *
 * Runs on the assembled cards, before paging:
 *
 *   1. with 0 or 1 cards there is nothing to rank and the stage is skipped;
 *   2. cards are sorted by their raw relevance and the best `topSources`
 *      (default 8) go to the model; the rest are discarded, but they still
 *      count in `total_available`;
 *   3. the model returns {index, score: 0..3} per source, best first;
 *   4. a card's new relevance is `score + tiebreak`, where the tiebreak is
 *      `(n_in_level - 1 - rank_in_level) * 0.1` in the order the model listed
 *      the cards of that level (the step shrinks past 10 cards in a level, so
 *      the tiebreak stays under 1), and a card the model left out gets 0;
 *   5. a card whose score from the model is below 2 is pruned, unless the
 *      request pins a source (then nothing is pruned: the caller already
 *      chose). The cut is on that score, never on the tiebreak.
 *
 * The entities' own relevances do not change. A failed call fails the stage
 * (StageError); there is no fallback.
 *
 * What is sent: source and model path names, the package name, `#(doc)` text,
 * and up to 20 entities per source with their names, kinds and `#(doc)` text.
 * Values would be listed too, but none are indexed yet. The text goes through
 * scrubForEgress, so no access predicate can ride along.
 */

import {
   renderRerankUserPrompt,
   type RerankSource,
} from "../../prompts/rerank";
import { StageError, replyArray } from "./get_context_llm";
import type {
   CardDraft,
   CardStage,
   CardState,
   PipelineContext,
} from "./get_context_pipeline";
import { mapRawScore } from "./get_context_scoring";
import { sourceContextKey } from "./get_context_tool";
import { scrubForEgress } from "./keyphrases";

/** Entities listed under each source in the prompt. */
export const RERANK_ENTITIES_PER_SOURCE = 20;
/** Lowest score a card may keep, unless the request pins a source. */
export const RERANK_MIN_KEPT_LEVEL = 2;

/** The score a card is sorted on: the raw one when refine ran, else its cosine. */
const sortKey = (card: CardDraft) => card.raw ?? card.relevance ?? 0;

/** The model's scores, in the order it listed them. Index is 1-based into the batch. */
function validateScores(size: number) {
   return (value: unknown): Array<{ index: number; score: number }> => {
      const items = replyArray(value);
      const seen = new Set<number>();
      const out: Array<{ index: number; score: number }> = [];
      const bad: string[] = [];
      items.forEach((item, at) => {
         const entry = item as { index?: unknown; score?: unknown } | null;
         if (
            typeof entry !== "object" ||
            entry === null ||
            typeof entry.index !== "number" ||
            !Number.isInteger(entry.index)
         ) {
            bad.push(`element ${at} needs an integer "index"`);
            return;
         }
         if (
            typeof entry.score !== "number" ||
            !Number.isInteger(entry.score) ||
            entry.score < 0 ||
            entry.score > 3
         ) {
            bad.push(`element ${at} needs "score" to be 0, 1, 2 or 3`);
            return;
         }
         if (entry.index < 1 || entry.index > size || seen.has(entry.index)) {
            return;
         }
         seen.add(entry.index);
         out.push({ index: entry.index, score: entry.score });
      });
      if (bad.length > 0) throw new Error(bad.join("; "));
      return out;
   };
}

/** The largest tiebreak a card can get. Under 1, so it never reaches the next score. */
const MAX_TIEBREAK = 0.9;

/**
 * Relevance per index from the model's list: `score + tiebreak`. Within one
 * score the first listed gets the largest tiebreak, `(n - 1) * 0.1`, and the
 * last gets 0, so the model's order inside a level survives the sort. With more
 * than 10 cards in a level the step shrinks to `0.9 / (n - 1)`: a fixed 0.1
 * let the first of 12 cards rated 1 reach 2.1, past a card the model rated 2.
 */
export function relevanceFromReply(
   scores: ReadonlyArray<{ index: number; score: number }>,
): Map<number, number> {
   const byLevel = new Map<number, number[]>();
   for (const { index, score } of scores) {
      const list = byLevel.get(score) ?? [];
      list.push(index);
      byLevel.set(score, list);
   }
   const out = new Map<number, number>();
   for (const [level, indexes] of byLevel) {
      const step =
         indexes.length > 1
            ? Math.min(0.1, MAX_TIEBREAK / (indexes.length - 1))
            : 0;
      indexes.forEach((index, rank) => {
         out.set(index, level + (indexes.length - 1 - rank) * step);
      });
   }
   return out;
}

function promptSource(
   card: CardDraft,
   sourceDocs: Map<string, string>,
   fallbackPackage: string,
): RerankSource {
   const own = card.rows.find((r) => r.kind === "source");
   const entities = card.rows
      .filter((r) => r.kind !== "source")
      .sort((a, b) => (b.score ?? 0) - (a.score ?? 0))
      .slice(0, RERANK_ENTITIES_PER_SOURCE)
      .map((r) => ({
         name: r.name,
         kind: r.kind,
         description: scrubForEgress(r.embedDoc ?? ""),
         ...(r.values && r.values.length > 0
            ? { values: r.values.slice(0, 5).map((v) => v.value) }
            : {}),
      }));
   return {
      source: card.source,
      modelPath: card.modelPath,
      packageName: card.rows[0]?.packageName ?? fallbackPackage,
      description: scrubForEgress(
         own?.embedDoc ?? sourceDocs.get(card.key) ?? "",
      ),
      entities,
   };
}

function sourceDocsOf(ctx: PipelineContext): Map<string, string> {
   const docs = new Map<string, string>();
   for (const e of ctx.pkgIndex.directEntities) {
      if (e.kind === "source") {
         docs.set(sourceContextKey(e.modelPath, e.name), e.embedDoc);
      }
   }
   return docs;
}

export const rerankStage: CardStage = {
   name: "rerank",
   // Semantic only, and only with something to order.
   enabled: (ctx, state) =>
      ctx.llmStages?.rerank !== undefined &&
      state.retrieval === "semantic" &&
      state.cards.length > 1,

   async run(state: CardState, ctx): Promise<CardState> {
      const cfg = ctx.llmStages?.rerank;
      if (!cfg) return state;
      const { request } = ctx;
      const question = request.searches.map((s) => s.text).join(". ");

      // Stable: equal keys keep the order assembly gave them.
      const sorted = state.cards
         .map((card, at) => ({ card, at }))
         .sort((a, b) => sortKey(b.card) - sortKey(a.card) || a.at - b.at)
         .map(({ card }) => card);
      const top = sorted.slice(0, cfg.topSources);
      const discarded = sorted.length - top.length;

      const sourceDocs = sourceDocsOf(ctx);
      let scores: ReadonlyArray<{ index: number; score: number }>;
      try {
         const reply = await cfg.chat.completeJson({
            system: cfg.instructions,
            prompt: renderRerankUserPrompt({
               question,
               sources: top.map((c) =>
                  promptSource(c, sourceDocs, request.packageName),
               ),
            }),
            maxTokens: 30 * top.length + 100,
            validate: validateScores(top.length),
         });
         scores = reply.value;
      } catch (error) {
         throw new StageError(
            "rerank",
            error instanceof Error ? error.message : String(error),
            error,
         );
      }

      const relevance = relevanceFromReply(scores);
      // The model's own score per card, which is what the cut is made on.
      const scored = new Map(scores.map((s) => [s.index, s.score]));
      const rescored = top.map((card, i) => {
         // A card the model left out gets 0.
         const raw = relevance.get(i + 1) ?? 0;
         return {
            card: { ...card, raw, relevance: mapRawScore(raw) },
            score: scored.get(i + 1) ?? 0,
         };
      });
      // The caller pinned a source: it chose, so no card is pruned.
      const pinned = Boolean(request.sourceName);
      const kept = rescored
         .filter((c) => pinned || c.score >= RERANK_MIN_KEPT_LEVEL)
         .map(({ card }, at) => ({ card, at }))
         .sort((a, b) => b.card.raw - a.card.raw || a.at - b.at)
         .map(({ card }) => card);
      return {
         ...state,
         cards: kept,
         discarded: (state.discarded ?? 0) + discarded,
         reranked: true,
      };
   },
};
