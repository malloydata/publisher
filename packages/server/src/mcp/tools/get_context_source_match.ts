// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Source match: when an LLM is configured, a `source` search target with
 * search text is answered by asking the model which sources it means, instead
 * of ranking sources by embedding or keyword.
 *
 *   1. the candidates are every source in scope: a source the discovery
 *      surface hides is not an entity at all, and a source with an
 *      unconditional deny-all gate was dropped when the index was built, so
 *      neither can appear here (they are checked again below anyway);
 *   2. they go to the model in batches of 10, `retrieval.llm.concurrency`
 *      batches at a time, each asking for {index, score: HIGH|MEDIUM} and
 *      leaving out the sources that are not relevant;
 *   3. when more than 8 sources rate HIGH for a target, that target's MEDIUM
 *      ones are dropped;
 *   4. a rated source becomes a source row with raw score 3 (HIGH) or 2
 *      (MEDIUM), published through the knots as 0.9 and 0.7. Assembly, rerank,
 *      paging and the size budget treat it like any other source row.
 *
 * The number of sources a target sends is capped so this stage cannot spend a
 * request's whole call budget (see sourceCapPerTarget): past the cap, the
 * sources are ranked by how many of the target's words their name, doc and
 * summary share, the best are sent, and the response says how many were not.
 *
 * Entity targets are not touched: the retrievers never see the source targets
 * while this stage is on (see sourceMatchActive), and this stage only adds the
 * source rows. A failed batch fails the stage (StageError); there is no
 * embedding or keyword fallback.
 *
 * What is sent: the package, model path and source name, and the source's
 * `#(doc)` text (scrubbed by scrubForEgress, so no access predicate can ride
 * along), or a line built from the source's joins when it has no doc. A source
 * with a stored LLM summary also sends it, in full, after the documentation.
 */

import {
   SOURCE_MATCH_DOC_MAX_CHARS,
   renderSourceMatchUserPrompt,
   type SourceMatchCandidate,
} from "../../prompts/source_match";
import { compareRanked } from "./get_context_assembly";
import { StageError, replyArray, runPooled } from "./get_context_llm";
import { REFINE_BATCH_SIZE, REFINE_TOTAL } from "./get_context_refine";
import type {
   PipelineContext,
   RankStage,
   RankedState,
} from "./get_context_pipeline";
import { LEVEL_VALUE, mapRawScore } from "./get_context_scoring";
import {
   matchesScope,
   projectEntity,
   sourceContextKey,
   type Entity,
   type ResolvedRequest,
   type ResultEntity,
} from "./get_context_tool";
import { scrubForEgress } from "./keyphrases";

/** Candidates per model call. */
export const SOURCE_MATCH_BATCH_SIZE = 10;
/** More HIGH sources than this and the target's MEDIUM ones are dropped. */
export const SOURCE_MATCH_MAX_HIGH = 8;

type MatchLevel = "HIGH" | "MEDIUM";
const MATCH_LEVELS: readonly MatchLevel[] = ["HIGH", "MEDIUM"];

/** Targets this stage answers: `source` targets that carry search text. */
export function sourceSearches(
   request: ResolvedRequest,
): ResolvedRequest["searches"] {
   return request.searches.filter((s) => s.targetType === "source");
}

/** Whether source targets go to the model for this request. */
export function sourceMatchActive(ctx: PipelineContext): boolean {
   return (
      ctx.llmStages?.sourceMatch !== undefined &&
      sourceSearches(ctx.request).length > 0
   );
}

/**
 * The candidates: every source entity in scope, each once per model path.
 * Sources that are not queryable never reach the index (the discovery surface
 * removes hidden ones, and an unconditional deny-all gate drops the source);
 * `droppedSources` is checked here too so that a future path that indexes one
 * cannot send it to the model.
 */
export function selectSourceCandidates(ctx: PipelineContext): Entity[] {
   const { request, pkgIndex } = ctx;
   return pkgIndex.directEntities.filter(
      (e) =>
         e.kind === "source" &&
         !pkgIndex.droppedSources?.has(sourceContextKey(e.modelPath, e.name)) &&
         matchesScope(e, request),
   );
}

/**
 * Names of the sources `e` joins, from the topology, else from its join
 * entities. Only the names of sources in the package's index are kept: a
 * target that is not there (a deny-all source, which is dropped, or one the
 * package does not export) must not be named to the model by a source that
 * joins it. The topology is built before either is removed, so it can still
 * name one.
 */
function joinedSourceNames(ctx: PipelineContext, e: Entity): string[] {
   const key = sourceContextKey(e.modelPath, e.name);
   const indexed = new Set(
      ctx.pkgIndex.directEntities
         .filter((c) => c.kind === "source")
         .map((c) => sourceContextKey(c.modelPath, c.name)),
   );
   const reached = ctx.pkgIndex.topology?.get(key) ?? [];
   const names = [
      ...new Set(
         reached
            .filter((r) =>
               indexed.has(sourceContextKey(r.targetModelPath, r.targetSource)),
            )
            .map((r) => r.targetSource),
      ),
   ];
   if (names.length > 0) return names;
   return (ctx.pkgIndex.sourceContext?.get(key)?.joins ?? []).map(
      (j) => j.name,
   );
}

/**
 * How many sources one source target may send to the model, or null for no cap.
 *
 * Every source target costs ceil(sent / SOURCE_MATCH_BATCH_SIZE) calls, and the
 * request shares `maxCallsPerRequest` with the stages that run after this one.
 * Sending every source made the default settings fail on a package with more
 * than about 70 sources (3 targets, 20 calls). So the cap is sized from the
 * calls still available: what is left, minus one call for rerank and the
 * worst case for refine on each entity target, split evenly between the source
 * targets, at least one batch each. With no ceiling there is no cap.
 */
export function sourceCapPerTarget(ctx: PipelineContext): number | null {
   const remaining = ctx.meter?.remaining() ?? null;
   if (remaining === null) return null;
   const { request } = ctx;
   const sourceTargets = sourceSearches(request).length;
   if (sourceTargets === 0) return null;
   const entityTargets = request.searches.length - sourceTargets;
   const reserved =
      (ctx.llmStages?.rerank ? 1 : 0) +
      (ctx.llmStages?.refine
         ? entityTargets * Math.ceil(REFINE_TOTAL / REFINE_BATCH_SIZE)
         : 0);
   const batches = Math.max(
      1,
      Math.floor(Math.max(0, remaining - reserved) / sourceTargets),
   );
   return batches * SOURCE_MATCH_BATCH_SIZE;
}

const wordsOf = (text: string): string[] =>
   text
      .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
      .toLowerCase()
      .split(/[^a-z0-9]+/)
      .filter((w) => w.length > 1);

/**
 * The positions of the `cap` candidates that best match `phrase`, in their
 * original order. A candidate scores two points for each distinct word of the
 * phrase found in its name and one for each found only in its doc or summary.
 * Ties keep the package's own order, so the cut is the same on every request.
 */
function topCandidates(
   phrase: string,
   texts: ReadonlyArray<{ name: string; text: string }>,
   cap: number,
): number[] {
   const wanted = [...new Set(wordsOf(phrase))];
   const scored = texts.map(({ name, text }, at) => {
      const inName = new Set(wordsOf(name));
      const inText = new Set(wordsOf(text));
      let score = 0;
      for (const w of wanted) {
         if (inName.has(w)) score += 2;
         else if (inText.has(w)) score += 1;
      }
      return { at, score };
   });
   return scored
      .sort((a, b) => b.score - a.score || a.at - b.at)
      .slice(0, cap)
      .map((c) => c.at)
      .sort((a, b) => a - b);
}

/** The one-line doc the model sees for a source. */
export function sourceDescription(ctx: PipelineContext, e: Entity): string {
   const doc = scrubForEgress(e.embedDoc ?? "");
   if (doc !== "") {
      return doc.length > SOURCE_MATCH_DOC_MAX_CHARS
         ? `${doc.slice(0, SOURCE_MATCH_DOC_MAX_CHARS)}...`
         : doc;
   }
   const joined = joinedSourceNames(ctx, e);
   return joined.length > 0
      ? `Source ${e.name} with joined sources: ${joined.join(", ")}`
      : `Source ${e.name} (no joined sources)`;
}

/**
 * The source's stored summary as the prompt shows it: whole, on one line (a
 * blank line would split the candidate), or nothing when none is stored. The
 * documentation above it is cut to SOURCE_MATCH_DOC_MAX_CHARS; this is not.
 */
function summaryOf(ctx: PipelineContext, e: Entity): { summary?: string } {
   const stored = ctx.sourceSummaries?.get(
      sourceContextKey(e.modelPath, e.name),
   );
   const text = scrubForEgress(stored?.summary ?? "");
   return text === "" ? {} : { summary: text };
}

/**
 * Validate a batch reply: an array of {index, score}. A wrong shape or a score
 * other than HIGH or MEDIUM is an error (the provider layer re-asks once with
 * this message); an index outside the batch or one already seen is ignored.
 */
function validateMatches(size: number) {
   return (value: unknown): Map<number, MatchLevel> => {
      const items = replyArray(value);
      const out = new Map<number, MatchLevel>();
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
            !(MATCH_LEVELS as readonly string[]).includes(entry.score)
         ) {
            bad.push(
               `element ${at} needs "score" to be one of ${MATCH_LEVELS.join(", ")}`,
            );
            return;
         }
         if (entry.index < 1 || entry.index > size || out.has(entry.index)) {
            return;
         }
         out.set(entry.index, entry.score as MatchLevel);
      });
      if (bad.length > 0) throw new Error(bad.join("; "));
      return out;
   };
}

export const sourceMatchStage: RankStage = {
   name: "source_match",
   enabled: (ctx) => sourceMatchActive(ctx),

   async run(state: RankedState, ctx: PipelineContext): Promise<RankedState> {
      const cfg = ctx.llmStages?.sourceMatch;
      if (!cfg) return state;
      const { request } = ctx;
      const searches = sourceSearches(request);
      const question = request.searches.map((s) => s.text).join(". ");

      const candidates = selectSourceCandidates(ctx);
      const lines: SourceMatchCandidate[] = candidates.map((e) => ({
         packageName: request.packageName,
         modelPath: e.modelPath,
         source: e.name,
         description: sourceDescription(ctx, e),
         ...summaryOf(ctx, e),
      }));
      const cap = sourceCapPerTarget(ctx);
      const capped = cap !== null && candidates.length > cap;
      const texts = capped
         ? candidates.map((e, at) => ({
              name: e.name,
              text: `${lines[at].description} ${lines[at].summary ?? ""}`,
           }))
         : [];
      const jobs = searches.flatMap((search) => {
         const positions = capped
            ? topCandidates(search.text, texts, cap)
            : candidates.map((_, at) => at);
         const batches: number[][] = [];
         for (let i = 0; i < positions.length; i += SOURCE_MATCH_BATCH_SIZE) {
            batches.push(positions.slice(i, i + SOURCE_MATCH_BATCH_SIZE));
         }
         return batches.map((batch) => ({ search, batch }));
      });
      if (capped) {
         ctx.warnings.push(
            `Source match sent the best ${cap} of ${candidates.length} sources to the model for each source target, ` +
               `so ${candidates.length - cap} were not shown to it and cannot appear in this answer. ` +
               `Narrow the search with model_path or source_name, or raise retrieval.llm.maxCallsPerRequest to let it read more.`,
         );
      }

      // target index -> candidate position -> level
      const rated = new Map<number, Map<number, MatchLevel>>(
         searches.map((s) => [s.targetIndex, new Map()]),
      );
      try {
         await runPooled(jobs, ctx.llmStages?.concurrency ?? 1, async (job) => {
            const reply = await cfg.chat.completeJson({
               system: cfg.instructions,
               prompt: renderSourceMatchUserPrompt({
                  question,
                  phrase: job.search.text,
                  candidates: job.batch.map((at) => lines[at]),
               }),
               maxTokens: 30 * job.batch.length + 100,
               validate: validateMatches(job.batch.length),
            });
            const target = rated.get(job.search.targetIndex);
            for (const [index, level] of reply.value) {
               target?.set(job.batch[index - 1], level);
            }
         });
      } catch (error) {
         throw new StageError(
            "source_match",
            error instanceof Error ? error.message : String(error),
            error,
         );
      }

      // One row per matched source, carrying every source target that rated it.
      const byCandidate = new Map<number, ResultEntity>();
      for (const search of searches) {
         const levels = rated.get(search.targetIndex) as Map<
            number,
            MatchLevel
         >;
         const highs = [...levels.values()].filter((l) => l === "HIGH").length;
         for (const [at, level] of levels) {
            if (level === "MEDIUM" && highs > SOURCE_MATCH_MAX_HIGH) continue;
            const raw = LEVEL_VALUE[level];
            const existing = byCandidate.get(at);
            const targetRaw = new Map(existing?.targetRaw ?? []);
            targetRaw.set(search.targetIndex, raw);
            const targetScores = new Map(
               [...targetRaw].map(([t, v]) => [t, mapRawScore(v)]),
            );
            const best = Math.max(...targetRaw.values());
            const bestTarget = [...targetRaw].find(([, v]) => v === best)?.[0];
            byCandidate.set(at, {
               ...projectEntity(
                  candidates[at],
                  request.environmentName,
                  request.packageName,
               ),
               level: best,
               raw: best,
               score: mapRawScore(best),
               targetRaw,
               targetScores,
               ...(bestTarget !== undefined ? { bestTarget } : {}),
            });
         }
      }
      const rows = [...state.rows, ...byCandidate.values()].sort(compareRanked);
      return { ...state, rows };
   },
};
