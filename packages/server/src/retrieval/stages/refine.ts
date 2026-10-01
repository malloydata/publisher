// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createHash } from "node:crypto";
import { entityRowKey } from "../../mcp/tools/embedding_index";
import { LlmError } from "../../service/llm_provider";
import { parseRefineReply, type RefineItem } from "../llm_json";
import { REPAIR_NOTE, buildRefinePrompt, refineLine } from "../prompts/refine";
import type {
   EgressClasses,
   RelevanceLevel,
   RetrievalConfig,
} from "../retrieval_config";
import { mapWithLimit } from "../pool";
import type { RunLlm } from "../run";
import { finalizeRelevance, levelBelow, levelValue, round4 } from "../scoring";
import type {
   SearchTargetText,
   StageOutcome,
   StageRow,
   StageVerdict,
} from "../stage_types";

/**
 * Entity refine: an LLM rates how well each candidate matches each search
 * phrase, and the candidates it rates below the configured level, or does not
 * mention, are dropped. It is the step that turns "close in embedding space"
 * into "actually answers the question", and the one that most shortens a
 * response.
 *
 * Per target, the candidates are the rows that target scored, best first,
 * capped per source and overall. They are rated in batches, in parallel under
 * the runner's concurrency cap. A row's score becomes `level + cosine` (level
 * 1-3), mapped to the wire's [0, 1] through `scoring.knots`, so the LLM's
 * verdict dominates and similarity breaks ties within a level.
 *
 * Fail-soft by construction. A batch that fails keeps its candidates at their
 * prior scores under `refine.unscoredLevel`; a stage where every batch fails
 * leaves the rows exactly as they came in. The tool call never fails because
 * a model server did.
 */

type Outcome =
   | { kind: "scored"; level: RelevanceLevel; reason: string }
   | { kind: "omitted" }
   | { kind: "unscored" }
   | { kind: "capped" };

interface Candidate<T extends StageRow> {
   key: string;
   rep: T;
   cosine: number;
}

const keyOf = (r: StageRow) => entityRowKey(r.kind, r.source ?? "", r.name);

function describe<T extends StageRow>(
   rep: T,
   cfg: RetrievalConfig["refine"],
   egress: EgressClasses,
): string {
   if (!egress.docs) return "";
   const text = rep.embedDoc || rep.keyphrase || "";
   return text.length > cfg.descChars
      ? `${text.slice(0, cfg.descChars)}…`
      : text;
}

function sha(parts: string[]): string {
   return createHash("sha256").update(parts.join("\u0000")).digest("hex");
}

export async function runRefine<T extends StageRow>(args: {
   rows: T[];
   searches: SearchTargetText[];
   config: RetrievalConfig;
   llm: RunLlm | null;
   lexical: boolean;
   egress: EgressClasses;
}): Promise<StageOutcome<T>> {
   const { rows, searches, config, llm, egress } = args;
   const cfg = config.refine;
   const skip = (why: string): StageOutcome<T> => ({
      rows,
      status: `skipped:${why}`,
      warnings: [],
      dropped: {},
      rowsIn: rows.length,
   });

   if (!llm) return skip("no_llm");
   if (args.lexical && !cfg.onLexical) return skip("lexical");
   if (!egress.names) return skip("egress");
   if (rows.length === 0) return skip("no_candidates");
   if (llm.runner.breakerOpen()) return skip("cooldown");
   const model = config.llm.models.refine ?? config.llm.model ?? llm.model;
   if (!model) return skip("no_model");

   const distinct = new Set(rows.map(keyOf));
   if (distinct.size <= cfg.skipIfAtMost) return skip("few_candidates");

   const queryText = searches.map((s) => s.text).join(". ");
   const wrap = config.llm.jsonMode === "json_object";

   // -- candidates per target ------------------------------------------------
   const perTarget = new Map<number, Map<string, Outcome>>();
   const candidatesByTarget = new Map<number, Candidate<T>[]>();
   for (const s of searches) {
      const byKey = new Map<string, Candidate<T>>();
      for (const r of rows) {
         const cosine = r.candidateScores?.get(s.targetIndex);
         if (cosine === undefined) continue;
         const key = keyOf(r);
         const existing = byKey.get(key);
         // The same entity at several model paths reads identically to the
         // model, so it is rated once and the verdict applies to every path.
         if (!existing) byKey.set(key, { key, rep: r, cosine });
      }
      const sorted = [...byKey.values()].sort(
         (a, b) => b.cosine - a.cosine || (a.key < b.key ? -1 : 1),
      );
      const perSource = new Map<string, number>();
      const picked: Candidate<T>[] = [];
      const outcomes = new Map<string, Outcome>();
      for (const c of sorted) {
         const src = c.rep.source ?? c.rep.name;
         const used = perSource.get(src) ?? 0;
         if (used >= cfg.maxPerSource || picked.length >= cfg.maxCandidates) {
            outcomes.set(c.key, { kind: "capped" });
            continue;
         }
         perSource.set(src, used + 1);
         picked.push(c);
      }
      candidatesByTarget.set(s.targetIndex, picked);
      perTarget.set(s.targetIndex, outcomes);
   }

   // -- rate the candidates --------------------------------------------------
   interface Batch {
      target: SearchTargetText;
      items: Candidate<T>[];
   }
   const batches: Batch[] = [];
   for (const s of searches) {
      const picked = candidatesByTarget.get(s.targetIndex) ?? [];
      for (let i = 0; i < picked.length; i += cfg.batchSize) {
         batches.push({ target: s, items: picked.slice(i, i + cfg.batchSize) });
      }
   }

   let failedBatches = 0;
   let firstFailure: LlmError | undefined;

   const rateBatch = async (batch: Batch): Promise<RefineItem[] | null> => {
      const lines = batch.items.map((c, i) =>
         refineLine(i, {
            name: c.rep.name,
            entityType: c.rep.kind,
            dataType: c.rep.dataType,
            source: c.rep.source ?? c.rep.name,
            description: describe(c.rep, cfg, egress),
         }),
      );
      const prompt = buildRefinePrompt({
         query: queryText,
         phrase: batch.target.text,
         lines,
         wrapForJsonMode: wrap,
      });
      const cacheKey = sha([
         "refine",
         model,
         cfg.promptVersion,
         String(wrap),
         queryText,
         batch.target.text,
         lines.join("\n"),
      ]);
      const attempt = async (note: string, key: string | undefined) => {
         const res = await llm.runner.complete(llm.budget, {
            stage: "refine",
            model,
            system: prompt.system,
            user: prompt.user + note,
            cacheKey: key,
            useCache: config.llm.cache.enabled,
         });
         return parseRefineReply(res.text, batch.items.length);
      };
      try {
         let parsed = await attempt("", cacheKey);
         if (!parsed.usable) {
            // One repair attempt: small models often answer correctly in prose
            // the second time they are told not to. Not cached, so a bad reply
            // is never pinned.
            parsed = await attempt(REPAIR_NOTE, undefined);
         }
         if (!parsed.usable) {
            throw new LlmError(
               "the model's reply could not be read as a list of ratings",
               "malformed",
               false,
            );
         }
         return parsed.items;
      } catch (error) {
         failedBatches++;
         if (!firstFailure) {
            firstFailure =
               error instanceof LlmError
                  ? error
                  : new LlmError(String(error), "network", false);
         }
         return null;
      }
   };

   // `refine.concurrency` caps how many of this request's batches are in flight
   // at once, below the process-wide `llm.concurrency` that every stage shares.
   const results = await mapWithLimit(
      batches,
      cfg.concurrency ?? batches.length,
      rateBatch,
   );

   // Everything failed: leave the rows exactly as they came, and say so.
   if (batches.length > 0 && failedBatches === batches.length) {
      const kind = firstFailure?.kind ?? "network";
      return {
         rows,
         status: `failed:${kind}`,
         warnings: [
            `LLM refine unavailable (${kind}); results are ranked by ${args.lexical ? "keyword match" : "embedding similarity"} only.`,
         ],
         dropped: {},
         rowsIn: rows.length,
      };
   }

   batches.forEach((batch, i) => {
      const outcomes = perTarget.get(batch.target.targetIndex)!;
      const rated = results[i];
      if (rated === null) {
         for (const c of batch.items) outcomes.set(c.key, { kind: "unscored" });
         return;
      }
      const byIndex = new Map(rated.map((r) => [r.index, r]));
      batch.items.forEach((c, idx) => {
         const item = byIndex.get(idx);
         outcomes.set(
            c.key,
            item
               ? { kind: "scored", level: item.level, reason: item.reason }
               : { kind: "omitted" },
         );
      });
   });

   // -- apply the verdicts to the rows --------------------------------------
   const dropped: Record<string, number> = {};
   const bump = (reason: string) => {
      dropped[reason] = (dropped[reason] ?? 0) + 1;
   };
   const kept: T[] = [];
   for (const r of rows) {
      // A row no entity target scored (a dimension found only by a value it
      // holds) has nothing here to rate; it keeps its place.
      if (!r.candidateScores || r.candidateScores.size === 0) {
         kept.push(r);
         continue;
      }
      const key = keyOf(r);
      const targetScores = new Map<number, number>();
      const reasons = new Map<number, string>();
      let bestRaw = -Infinity;
      let bestTarget: number | undefined;
      let dropReason: string | undefined;
      for (const [t, cosine] of r.candidateScores ?? []) {
         const outcomes = perTarget.get(t);
         if (!outcomes) continue; // not a text target (defensive)
         const o = outcomes.get(key);
         if (!o) continue;
         let level: RelevanceLevel;
         let reason = "";
         if (o.kind === "capped") {
            dropReason ??= "refine_cap";
            continue;
         } else if (o.kind === "omitted") {
            if (cfg.dropOmitted) {
               dropReason ??= "llm_omitted";
               continue;
            }
            level = "LOW";
         } else if (o.kind === "unscored") {
            level = cfg.unscoredLevel;
         } else {
            level = o.level;
            reason = o.reason;
            if (levelBelow(level, cfg.minLevel)) {
               dropReason ??= "below_min_level";
               continue;
            }
         }
         // A field reached through a join is a step further from the question.
         // By default only the similarity part is discounted (`fraction`): the
         // service's code damps the whole score (`whole`), so a HIGH one join
         // away scores like a MEDIUM at home, which undoes the rating the LLM
         // just gave. Both are here so the two can be compared.
         const hops = r.joinPath ? r.joinPath.split(".").length : 0;
         const damping = config.scoring.joinDepthDamping;
         const damp = hops > 0 && damping < 1 ? damping ** hops : 1;
         const raw =
            config.scoring.joinDampingMode === "whole"
               ? (levelValue(level) + cosine) * damp
               : levelValue(level) + cosine * damp;
         targetScores.set(
            t,
            round4(finalizeRelevance(raw, config.scoring.knots)),
         );
         if (reason) reasons.set(t, reason);
         if (raw > bestRaw) {
            bestRaw = raw;
            bestTarget = t;
         }
      }
      if (targetScores.size === 0) {
         bump(dropReason ?? "llm_omitted");
         continue;
      }
      kept.push({
         ...r,
         score: Math.max(...targetScores.values()),
         targetScores,
         matchReasons: reasons,
         bestTarget,
         rankScore: bestRaw,
      });
   }
   // Best first, by the score the LLM stage produced. Array#sort is stable, so
   // rows that tie (one entity at several model paths) keep their order.
   kept.sort((a, b) => (b.rankScore ?? 0) - (a.rankScore ?? 0));

   const warnings: string[] = [];
   let status = "ok";
   if (failedBatches > 0) {
      status = `partial:${failedBatches}/${batches.length}`;
      warnings.push(
         `LLM refine failed for ${failedBatches} of ${batches.length} batches (${firstFailure?.kind ?? "error"}); those candidates were kept at their similarity order.`,
      );
   }
   const verdicts = new Map<string, Map<number, StageVerdict>>();
   for (const [target, outcomes] of perTarget) {
      for (const [key, o] of outcomes) {
         const v: StageVerdict =
            o.kind === "scored"
               ? { outcome: "scored", level: o.level, reason: o.reason }
               : { outcome: o.kind };
         const byTarget = verdicts.get(key) ?? new Map<number, StageVerdict>();
         byTarget.set(target, v);
         verdicts.set(key, byTarget);
      }
   }
   return {
      rows: kept,
      status,
      warnings,
      dropped,
      rowsIn: rows.length,
      verdicts,
   };
}
