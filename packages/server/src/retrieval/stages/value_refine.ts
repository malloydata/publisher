// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createHash } from "node:crypto";
import { LlmError } from "../../service/llm_provider";
import type { ValueHit } from "../dim_values";
import { parseValueRefineReply } from "../llm_json";
import { mapWithLimit } from "../pool";
import { REPAIR_NOTE } from "../prompts/refine";
import {
   buildValueRefinePrompt,
   valueRefineLine,
} from "../prompts/value_refine";
import type {
   EgressClasses,
   RelevanceLevel,
   RetrievalConfig,
} from "../retrieval_config";
import type { RunLlm } from "../run";
import { finalizeRelevance, levelBelow, levelValue, round4 } from "../scoring";
import type { SearchTargetText } from "../stage_types";

/**
 * Value refine: an LLM rates each matched dimension value against the phrase
 * that found it, and the ones it leaves out, or rates under `minLevel`, are
 * dropped. This is the step that turns "everything that looks a bit like the
 * phrase" into "the values the user means", and it is what Credible's hosted
 * retrieval does after its own value match.
 *
 * Follows the service step for step: candidates are grouped by source and
 * capped per source by score, then capped overall, split into batches, and
 * rated in parallel; the model sees the phrase and one
 * `- [i] value: source.dimension` line per candidate, and answers with a level
 * only. A kept value scores `level + its match score`, published through the
 * same knots as entity refine.
 *
 * Fail-soft, like every LLM stage: a batch that fails leaves its values as they
 * were, and a stage where every batch fails leaves all of them. The values
 * are customer data, so the stage runs only with `egress.dimensionalValues`.
 */
export interface ValueRefineOutcome {
   hits: ValueHit[];
   status: string;
   warnings: string[];
   dropped: Record<string, number>;
   rowsIn: number;
}

const sha = (parts: string[]): string =>
   createHash("sha256").update(parts.join("\u0000")).digest("hex");

const keyOf = (h: ValueHit): string =>
   [h.targetIndex, h.source, h.dimension, h.value].join("\u0000");

export async function runValueRefine(args: {
   hits: ValueHit[];
   searches: SearchTargetText[];
   config: RetrievalConfig;
   llm: RunLlm | null;
   egress: EgressClasses;
}): Promise<ValueRefineOutcome> {
   const { hits, searches, config, llm, egress } = args;
   const cfg = config.dimensionalValues.refine;
   const skip = (why: string): ValueRefineOutcome => ({
      hits,
      status: `skipped:${why}`,
      warnings: [],
      dropped: {},
      rowsIn: hits.length,
   });

   if (!llm) return skip("no_llm");
   // The candidates are customer data and their names go in every line.
   if (!egress.dimensionalValues || !egress.names) return skip("egress");
   if (hits.length === 0) return skip("no_candidates");
   if (llm.runner.breakerOpen()) return skip("cooldown");
   const model = config.llm.models.valueRefine ?? config.llm.model ?? llm.model;
   if (!model) return skip("no_model");

   const wrap = config.llm.jsonMode === "json_object";
   const textOf = new Map(searches.map((s) => [s.targetIndex, s.text]));

   // -- candidates: per target, per source cap, then an overall cap ----------
   type Batch = { target: number; phrase: string; items: ValueHit[] };
   const batches: Batch[] = [];
   const sent = new Set<string>();
   const byTarget = new Map<number, ValueHit[]>();
   for (const h of hits) {
      const at = byTarget.get(h.targetIndex);
      if (at) at.push(h);
      else byTarget.set(h.targetIndex, [h]);
   }
   for (const [target, group] of byTarget) {
      const phrase = textOf.get(target);
      if (!phrase) continue;
      const bySource = new Map<string, ValueHit[]>();
      for (const h of group) {
         const at = bySource.get(h.source);
         if (at) at.push(h);
         else bySource.set(h.source, [h]);
      }
      const capped: ValueHit[] = [];
      for (const list of bySource.values()) {
         list.sort((a, b) => b.score - a.score || (a.value < b.value ? -1 : 1));
         capped.push(...list.slice(0, cfg.maxPerSource));
      }
      capped.sort((a, b) => b.score - a.score || (a.value < b.value ? -1 : 1));
      const picked = capped.slice(0, cfg.maxCandidates);
      for (const h of picked) sent.add(keyOf(h));
      for (let i = 0; i < picked.length; i += cfg.batchSize) {
         batches.push({
            target,
            phrase,
            items: picked.slice(i, i + cfg.batchSize),
         });
      }
   }
   if (batches.length === 0) return skip("no_candidates");

   let failed = 0;
   let firstFailure: LlmError | undefined;

   const rate = async (
      b: Batch,
   ): Promise<Map<number, RelevanceLevel> | null> => {
      const lines = b.items.map((h, i) =>
         valueRefineLine(i, {
            value: h.value,
            source: h.source,
            dimension: h.dimension,
         }),
      );
      const prompt = buildValueRefinePrompt({
         phrase: b.phrase,
         lines,
         wrapForJsonMode: wrap,
      });
      const cacheKey = sha([
         "valueRefine",
         model,
         String(wrap),
         b.phrase,
         lines.join("\n"),
      ]);
      const attempt = async (note: string, key: string | undefined) => {
         const res = await llm.runner.complete(llm.budget, {
            stage: "valueRefine",
            model,
            system: prompt.system,
            user: prompt.user + note,
            cacheKey: key,
            useCache: config.llm.cache.enabled,
         });
         return parseValueRefineReply(res.text, b.items.length);
      };
      try {
         let parsed = await attempt("", cacheKey);
         if (!parsed.usable) parsed = await attempt(REPAIR_NOTE, undefined);
         if (!parsed.usable) {
            throw new LlmError(
               "the model's reply could not be read as a list of ratings",
               "malformed",
               false,
            );
         }
         return new Map(parsed.items.map((it) => [it.index, it.level]));
      } catch (error) {
         failed++;
         firstFailure ??=
            error instanceof LlmError
               ? error
               : new LlmError(String(error), "network", false);
         return null;
      }
   };

   const results = await mapWithLimit(batches, config.llm.concurrency, rate);

   if (failed === batches.length) {
      const kind = firstFailure?.kind ?? "network";
      return {
         hits,
         status: `failed:${kind}`,
         warnings: [
            `LLM value refine unavailable (${kind}); matched values are ranked by similarity only.`,
         ],
         dropped: {},
         rowsIn: hits.length,
      };
   }

   // -- apply the ratings ----------------------------------------------------
   const level = new Map<string, RelevanceLevel | "unrated">();
   batches.forEach((b, i) => {
      const rated = results[i];
      b.items.forEach((h, idx) => {
         if (rated === null) level.set(keyOf(h), "unrated");
         else level.set(keyOf(h), rated.get(idx) ?? ("omitted" as never));
      });
   });

   const dropped: Record<string, number> = {};
   const bump = (why: string) => {
      dropped[why] = (dropped[why] ?? 0) + 1;
   };
   const kept: ValueHit[] = [];
   for (const h of hits) {
      const k = keyOf(h);
      if (!sent.has(k)) {
         bump("refine_cap");
         continue;
      }
      const lv = level.get(k);
      if (lv === "unrated") {
         kept.push(h); // its batch failed: leave the value as it was
         continue;
      }
      if (lv === undefined || (lv as string) === "omitted") {
         bump("llm_omitted");
         continue;
      }
      if (levelBelow(lv, cfg.minLevel)) {
         bump("below_min_level");
         continue;
      }
      kept.push({
         ...h,
         score: round4(
            finalizeRelevance(levelValue(lv) + h.score, config.scoring.knots),
         ),
      });
   }
   kept.sort(
      (a, b) =>
         b.score - a.score ||
         b.weight - a.weight ||
         (a.value < b.value ? -1 : 1),
   );

   const warnings: string[] = [];
   let status = "ok";
   if (failed > 0) {
      status = `partial:${failed}/${batches.length}`;
      warnings.push(
         `LLM value refine failed for ${failed} of ${batches.length} batches (${firstFailure?.kind ?? "error"}); those values were kept at their similarity order.`,
      );
   }
   return { hits: kept, status, warnings, dropped, rowsIn: hits.length };
}
