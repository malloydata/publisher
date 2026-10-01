// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createHash } from "node:crypto";
import { LlmError } from "../../service/llm_provider";
import {
   cardScore,
   groupByCard,
   orderCards,
   publishCardRelevance,
   reorderByCards,
} from "../cards";
import { parseRerankReply } from "../llm_json";
import { REPAIR_NOTE } from "../prompts/refine";
import { buildRerankPrompt, type RerankSource } from "../prompts/rerank";
import type { EgressClasses, RetrievalConfig } from "../retrieval_config";
import type { RunLlm } from "../run";
import { round4 } from "../scoring";
import type { SearchTargetText, StageOutcome, StageRow } from "../stage_types";

/**
 * Source rerank: one LLM call ranks the best few source cards against the
 * whole question, because the question is usually about a source ("which of
 * these can answer this?") rather than about any single field in it.
 *
 * What differs from the service's version, each on purpose:
 *
 * - Sources past `topSources` are NOT discarded. They keep their place below
 *   every reranked source (`beyondTop: "keep"`), because being outside the top
 *   few by similarity is not evidence of irrelevance. `"drop"` reproduces the
 *   service.
 * - Entity relevance is left alone. Only the card's own relevance changes, so
 *   a field's score still means what its own stage said.
 * - A card's published relevance never exceeds that of a card ranked above it,
 *   so the numbers agree with the order. Cards the LLM did not rank carry the
 *   lowest score, not their old high one.
 *
 * Fail-soft like refine: on any failure the rows come back as they went in.
 */

const LEVEL_STEP = 0.1;

export interface RerankOutcome<T> extends StageOutcome<T> {
   /** Published relevance per card, keyed by cardKeyOf. Absent when nothing changed. */
   sourceRelevance?: Map<string, number>;
}

function sha(parts: string[]): string {
   return createHash("sha256").update(parts.join("\u0000")).digest("hex");
}

export async function runRerank<T extends StageRow>(args: {
   rows: T[];
   searches: SearchTargetText[];
   config: RetrievalConfig;
   llm: RunLlm | null;
   lexical: boolean;
   egress: EgressClasses;
   packageName: string;
   /** The source doc for a card, already capped. */
   docsFor: (cardKey: string) => string | undefined;
   /** The generated summary of a source, by source name, when one exists. */
   summaryFor?: (source: string) => string | undefined;
   /** Rows already carry LLM-scored ranks (refine applied), so they publish through the knots. */
   scoredByLlm: boolean;
}): Promise<RerankOutcome<T>> {
   const { rows, searches, config, llm, egress } = args;
   const cfg = config.rerank;
   const skip = (why: string): RerankOutcome<T> => ({
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
   const model = config.llm.models.rerank ?? config.llm.model ?? llm.model;
   if (!model) return skip("no_model");

   const cards = groupByCard(rows);
   if (cards.size <= cfg.skipIfAtMost) return skip("few_candidates");

   const mode = config.scoring.sourceRelevance;
   const raw = new Map<string, number>();
   for (const [key, group] of cards) {
      raw.set(key, cardScore(group, mode, searches.length));
   }
   const order = orderCards(raw);
   const top = order.slice(0, cfg.topSources);
   const beyond = order.slice(cfg.topSources);

   const nlText = searches.map((s) => s.text).join(". ");
   const wrap = config.llm.jsonMode === "json_object";
   const sources: RerankSource[] = top.map((key) => {
      const group = cards.get(key)!;
      const head = group[0];
      const entities = group
         .filter((r) => r.kind !== "source")
         .sort((a, b) => (b.rankScore ?? 0) - (a.rankScore ?? 0))
         .slice(0, cfg.maxEntityLines)
         .map((r) => {
            const text = egress.docs ? r.embedDoc || r.keyphrase || "" : "";
            // Values are customer data, so they go only with their own class.
            const values =
               egress.dimensionalValues && cfg.valuesPerEntity > 0
                  ? (r.values ?? [])
                       .slice(0, cfg.valuesPerEntity)
                       .map((v) => v.value)
                  : [];
            return {
               name: r.name,
               entityType: r.kind,
               description:
                  text.length > config.refine.descChars
                     ? `${text.slice(0, config.refine.descChars)}…`
                     : text,
               ...(values.length > 0 ? { values } : {}),
            };
         });
      const docs = egress.docs ? args.docsFor(key) : undefined;
      // The LLM-written summary is generated from the docs, so it travels with them.
      const summary = egress.docs
         ? args.summaryFor?.(head.source ?? head.name)
         : undefined;
      return {
         source: head.source ?? head.name,
         modelPath: head.modelPath,
         pkg: args.packageName,
         ...(docs ? { docs } : {}),
         ...(summary ? { summary } : {}),
         entities,
      };
   });

   const prompt = buildRerankPrompt({ sources, nlText, wrapForJsonMode: wrap });
   const cacheKey = sha([
      "rerank",
      model,
      cfg.promptVersion,
      String(wrap),
      nlText,
      prompt.user,
   ]);

   let ranked;
   try {
      const attempt = async (note: string, key: string | undefined) => {
         const res = await llm.runner.complete(llm.budget, {
            stage: "rerank",
            model,
            system: prompt.system,
            user: prompt.user + note,
            cacheKey: key,
            useCache: config.llm.cache.enabled,
         });
         return parseRerankReply(res.text, top.length);
      };
      let parsed = await attempt("", cacheKey);
      if (!parsed.usable) parsed = await attempt(REPAIR_NOTE, undefined);
      if (!parsed.usable) {
         throw new LlmError(
            "the model's reply could not be read as a ranking",
            "malformed",
            false,
         );
      }
      ranked = parsed.items;
   } catch (error) {
      const kind = error instanceof LlmError ? error.kind : "network";
      return {
         rows,
         status: `failed:${kind}`,
         warnings: [
            `LLM rerank unavailable (${kind}); sources keep their earlier order.`,
         ],
         dropped: {},
         rowsIn: rows.length,
      };
   }

   // Score within a level by the order the model listed them in, so its
   // preference survives the sort below. The model's SCORE decides the level;
   // its listed order only breaks ties inside one, which also keeps the order
   // and the scores in agreement even when the model sorted them badly.
   const byLevel = new Map<number, number[]>();
   for (const item of ranked) {
      const at = byLevel.get(item.score) ?? [];
      at.push(item.index);
      byLevel.set(item.score, at);
   }
   const level = new Map<number, number>(); // top position -> LLM score
   const rerankRaw = new Map<number, number>(); // top position -> level + tie bump
   for (const [score, indices] of byLevel) {
      indices.forEach((idx, i) => {
         level.set(idx, score);
         rerankRaw.set(idx, score + (indices.length - 1 - i) * LEVEL_STEP);
      });
   }
   // A source the model never mentioned is one it found nothing in.
   top.forEach((_, i) => {
      if (!level.has(i)) {
         level.set(i, 0);
         rerankRaw.set(i, 0);
      }
   });

   const dropped: Record<string, number> = {};
   const dropRows = (key: string, reason: string) => {
      dropped[reason] = (dropped[reason] ?? 0) + (cards.get(key)?.length ?? 0);
   };

   const survivors: string[] = [];
   top.forEach((key, i) => {
      if ((level.get(i) ?? 0) < cfg.minScore) dropRows(key, "rerank_below_min");
      else survivors.push(key);
   });
   survivors.sort(
      (a, b) =>
         (rerankRaw.get(top.indexOf(b)) ?? 0) -
            (rerankRaw.get(top.indexOf(a)) ?? 0) ||
         top.indexOf(a) - top.indexOf(b),
   );
   const kept: string[] = [...survivors];
   if (cfg.beyondTop === "keep") kept.push(...beyond);
   else for (const key of beyond) dropRows(key, "beyond_top");

   // Published relevance: reranked cards from their rerank score; the rest
   // capped at the lowest of those so the numbers never contradict the order.
   const relevance = new Map<string, number>();
   let floor = Infinity;
   for (const key of survivors) {
      const r = round4(
         publishCardRelevance(
            rerankRaw.get(top.indexOf(key)) ?? 0,
            true,
            config.scoring.knots,
         ),
      );
      relevance.set(key, r);
      floor = Math.min(floor, r);
   }
   for (const key of beyond) {
      if (cfg.beyondTop !== "keep") continue;
      const own = publishCardRelevance(
         raw.get(key) ?? 0,
         args.scoredByLlm,
         config.scoring.knots,
      );
      relevance.set(key, Math.min(own, floor));
   }

   return {
      rows: reorderByCards(rows, kept),
      status: "ok",
      warnings: [],
      dropped,
      rowsIn: rows.length,
      sourceRelevance: relevance,
   };
}
