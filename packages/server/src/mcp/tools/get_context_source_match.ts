// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Source match: when an LLM is configured, a `source` search target with
 * search text is answered by asking the model which sources it means, instead
 * of ranking sources by embedding or keyword. Candidates (every source in
 * scope) go in batches of SOURCE_MATCH_BATCH_SIZE; each rated source becomes a
 * source row with raw score HIGH=3 / MEDIUM=2. A failed batch fails the stage;
 * there is no fallback. See docs/get-context-pipeline.md.
 *
 * What is sent: package, model path, source name, and the source's `#(doc)`
 * scrubbed by scrubForEgress (so no access predicate rides along), or a line
 * built from its joins; plus its stored LLM summary when there is one.
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
import { matchesScope } from "./get_context_request";
import { projectEntity } from "./get_context_response";
import type {
   Entity,
   ResolvedRequest,
   ResultEntity,
} from "./get_context_types";
import { sourceContextKey } from "./package_entities";
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
 * Unqueryable sources never reach the index; `droppedSources` is checked here
 * too so a future path that indexes one cannot send it to the model.
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

/** The (model path, source) keys of the sources in the package's index, cached per index. */
const indexedSourceKeysCache = new WeakMap<object, ReadonlySet<string>>();
function indexedSourceKeys(pkgIndex: {
   directEntities: readonly Entity[];
}): ReadonlySet<string> {
   let keys = indexedSourceKeysCache.get(pkgIndex);
   if (!keys) {
      keys = new Set(
         pkgIndex.directEntities
            .filter((c) => c.kind === "source")
            .map((c) => sourceContextKey(c.modelPath, c.name)),
      );
      indexedSourceKeysCache.set(pkgIndex, keys);
   }
   return keys;
}

/**
 * Names of the sources `e` joins, from the topology, else from its join
 * entities. Only sources in the package's index are named: the topology is
 * built before deny-all and unexported sources are removed, and those must not
 * be named to the model.
 */
function joinedSourceNames(ctx: PipelineContext, e: Entity): string[] {
   const key = sourceContextKey(e.modelPath, e.name);
   const indexed = indexedSourceKeys(ctx.pkgIndex);
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
 * The request shares `maxCallsPerRequest` with later stages, so the cap is the
 * calls left, minus one for rerank and refine's worst case per entity target,
 * split evenly across source targets, at least one batch each. Past the cap the
 * best word-overlap candidates are sent and the response says how many were not.
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
function summaryOf(
   ctx: PipelineContext,
   e: Entity,
   maxChars?: number,
): { summary?: string } {
   const stored = ctx.sourceSummaries?.get(
      sourceContextKey(e.modelPath, e.name),
   );
   const text = scrubForEgress(stored?.summary ?? "");
   if (text === "") return {};
   return {
      summary:
         maxChars !== undefined && text.length > maxChars
            ? `${text.slice(0, maxChars)}...`
            : text,
   };
}

/**
 * Summary cap for Ollama: its default context (2–4K tokens) silently truncates
 * a longer prompt, dropping later candidates with no error. At this size a
 * full batch, with docs and instructions, fits.
 */
export const SOURCE_MATCH_SMALL_CONTEXT_SUMMARY_CHARS = 300;

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
         ...summaryOf(
            ctx,
            e,
            cfg.chat.provider === "ollama"
               ? SOURCE_MATCH_SMALL_CONTEXT_SUMMARY_CHARS
               : undefined,
         ),
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
