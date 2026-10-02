// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Refine: an LLM rates the candidates the semantic scan found, so a field that
 * is only near the phrase in embedding space is dropped and the ones that fit
 * are ordered by how well.
 *
 * Runs once per entity-search target on the DIRECT ranked rows, before
 * assembly makes joined copies:
 *
 *   1. candidates are unique by (name, source), the best 10 per source by
 *      cosine, then the best 120 overall;
 *   2. they go to the model in batches of 15, `retrieval.llm.concurrency`
 *      batches at a time, each asking for {index, score: LOW|MEDIUM|HIGH};
 *   3. a candidate rated below the package's `minLevel`, or not returned, is
 *      dropped from that target;
 *   4. a survivor's raw score is `level + cosine` (LOW 1, MEDIUM 2, HIGH 3),
 *      published through the knots in get_context_scoring.
 *
 * A failed batch fails the stage (StageError); there is no unrefined fallback.
 *
 * What is sent: name, kind, data type, source and `#(doc)` text. The text goes
 * through scrubForEgress, so no access predicate can ride along.
 */

import {
   renderRefineUserPrompt,
   type RefineCandidate,
} from "../../prompts/refine";
import { REFINE_LEVEL_NAMES } from "../../service/package_retrieval";
import { compareRanked } from "./get_context_assembly";
import { StageError, replyArray, runPooled } from "./get_context_llm";
import type { RankStage, RankedState } from "./get_context_pipeline";
import { LEVEL_VALUE, mapRawScore, type Level } from "./get_context_scoring";
import {
   bestTargetOf,
   type ResolvedRequest,
   type ResultEntity,
} from "./get_context_tool";
import { scrubForEgress } from "./keyphrases";

/** Most candidates one source sends to the model, per target. */
export const REFINE_PER_SOURCE = 10;
/** Most candidates one target sends to the model, after the per-source cut. */
export const REFINE_TOTAL = 120;
/** Candidates per model call. */
export const REFINE_BATCH_SIZE = 15;

interface Candidate {
   key: string;
   row: ResultEntity;
   cosine: number;
}

interface Rating {
   level: number;
   cosine: number;
}

/** (name, source): one rating covers every model path that reaches the entity. */
const candidateKey = (row: { name: string; source?: string }) =>
   JSON.stringify([row.name, row.source ?? ""]);

/** Entity-search targets: everything except `source`, which is not refined. */
function refinedSearches(request: ResolvedRequest) {
   return request.searches.filter((s) => s.targetType !== "source");
}

/** Plain code-unit order, so the cut does not depend on the host's locale. */
const compareText = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

/** Cosine descending; ties by source then name, so the cut does not depend on scan order. */
function byCosine(a: Candidate, b: Candidate): number {
   return (
      b.cosine - a.cosine ||
      compareText(a.row.source ?? "", b.row.source ?? "") ||
      compareText(a.row.name, b.row.name)
   );
}

/**
 * The candidates one target sends: unique by (name, source) at their best
 * cosine, the best REFINE_PER_SOURCE per source, then the best REFINE_TOTAL.
 */
export function selectCandidates(
   rows: readonly ResultEntity[],
   targetIndex: number,
): Candidate[] {
   const unique = new Map<string, Candidate>();
   for (const row of rows) {
      if (row.kind === "source") continue;
      const cosine = row.targetScores?.get(targetIndex);
      if (cosine === undefined) continue;
      const key = candidateKey(row);
      const seen = unique.get(key);
      if (!seen || cosine > seen.cosine) unique.set(key, { key, row, cosine });
   }
   const bySource = new Map<string, Candidate[]>();
   for (const c of unique.values()) {
      const source = c.row.source ?? "";
      const list = bySource.get(source) ?? [];
      list.push(c);
      bySource.set(source, list);
   }
   const kept = [...bySource.values()].flatMap((list) =>
      list.sort(byCosine).slice(0, REFINE_PER_SOURCE),
   );
   return kept.sort(byCosine).slice(0, REFINE_TOTAL);
}

/**
 * Validate a batch reply: an array of {index, score}. A wrong shape or a score
 * outside LOW, MEDIUM, HIGH is an error (the provider layer re-asks once with
 * this message); an index outside the batch or one already seen is ignored.
 */
function validateRatings(size: number) {
   return (value: unknown): Map<number, Level> => {
      const items = replyArray(value);
      const out = new Map<number, Level>();
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
            typeof entry.score !== "string" ||
            !(REFINE_LEVEL_NAMES as readonly string[]).includes(entry.score)
         ) {
            bad.push(
               `element ${at} needs "score" to be one of ${REFINE_LEVEL_NAMES.join(", ")}`,
            );
            return;
         }
         if (entry.index < 1 || entry.index > size || out.has(entry.index)) {
            return;
         }
         out.set(entry.index, entry.score as Level);
      });
      if (bad.length > 0) throw new Error(bad.join("; "));
      return out;
   };
}

/**
 * What the model may see of a row's description: its `#(doc)` text only (never
 * `doc`, which can fall back to raw annotation lines), scrubbed again so an
 * access predicate cannot ride along.
 */
const describe = (row: ResultEntity) => scrubForEgress(row.embedDoc ?? "");

export const refineStage: RankStage = {
   name: "refine",
   // Semantic only: the lexical ranking has no cosine to add to a level.
   enabled: (ctx, state) =>
      ctx.llmStages?.refine !== undefined &&
      state.retrieval === "semantic" &&
      refinedSearches(ctx.request).length > 0,

   async run(state: RankedState, ctx): Promise<RankedState> {
      const cfg = ctx.llmStages?.refine;
      if (!cfg) return state;
      const { request } = ctx;
      const question = request.searches.map((s) => s.text).join(". ");

      const jobs = refinedSearches(request).flatMap((search) => {
         const candidates = selectCandidates(state.rows, search.targetIndex);
         const batches: Candidate[][] = [];
         for (let i = 0; i < candidates.length; i += REFINE_BATCH_SIZE) {
            batches.push(candidates.slice(i, i + REFINE_BATCH_SIZE));
         }
         return batches.map((batch) => ({ search, batch }));
      });

      const ratings = new Map<number, Map<string, Rating>>(
         refinedSearches(request).map((s) => [s.targetIndex, new Map()]),
      );
      const minLevel = LEVEL_VALUE[cfg.minLevel];
      try {
         await runPooled(jobs, ctx.llmStages?.concurrency ?? 1, async (job) => {
            const prompts: RefineCandidate[] = job.batch.map((c) => ({
               name: c.row.name,
               kind: c.row.kind,
               ...(c.row.dataType ? { dataType: c.row.dataType } : {}),
               source: c.row.source ?? "",
               description: describe(c.row),
            }));
            const reply = await cfg.chat.completeJson({
               system: cfg.instructions,
               prompt: renderRefineUserPrompt({
                  question,
                  phrase: job.search.text,
                  candidates: prompts,
               }),
               maxTokens: 40 * job.batch.length + 100,
               validate: validateRatings(job.batch.length),
            });
            const target = ratings.get(job.search.targetIndex);
            for (const [index, level] of reply.value) {
               const c = job.batch[index - 1];
               const value = LEVEL_VALUE[level];
               if (value >= minLevel) {
                  target?.set(c.key, { level: value, cosine: c.cosine });
               }
            }
         });
      } catch (error) {
         throw new StageError(
            "refine",
            error instanceof Error ? error.message : String(error),
            error,
         );
      }

      const rows = state.rows.flatMap((row): ResultEntity[] => {
         if (row.kind === "source") return [sourceRowOnKnots(row)];
         const key = candidateKey(row);
         const targetRaw = new Map<number, number>();
         let level = 0;
         for (const [target, cosine] of row.targetScores ?? []) {
            const rating = ratings.get(target)?.get(key);
            if (!rating) continue;
            targetRaw.set(target, rating.level + cosine);
            level = Math.max(level, rating.level);
         }
         if (targetRaw.size === 0) return [];
         const raw = Math.max(...targetRaw.values());
         return [
            {
               ...row,
               level,
               raw,
               targetRaw,
               score: mapRawScore(raw),
               targetScores: mapTargets(targetRaw),
               bestTarget: bestTargetOf(targetRaw),
            },
         ];
      });
      return { ...state, rows: rows.sort(compareRanked) };
   },
};

function mapTargets(raw: Map<number, number>): Map<number, number> {
   return new Map(
      [...raw].map(([target, value]) => [target, mapRawScore(value)]),
   );
}

/**
 * A source-target row is not rated, but its score is published on the same
 * scale as everything else once refine ran: its cosine is the raw score.
 */
function sourceRowOnKnots(row: ResultEntity): ResultEntity {
   if (row.score === undefined) return row;
   const targetRaw = row.targetScores ? new Map(row.targetScores) : undefined;
   return {
      ...row,
      raw: row.score,
      score: mapRawScore(row.score),
      ...(targetRaw ? { targetRaw, targetScores: mapTargets(targetRaw) } : {}),
   };
}
