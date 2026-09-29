// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { LlmUsage } from "../service/llm_runner";

/**
 * A per-request account of what each retrieval stage did to the candidate
 * set. It exists so an eval can say WHICH stage cost a miss or added noise,
 * not just that the result was wrong: a missing entity that never cleared the
 * floor is a model or embedding problem; one the refine stage pruned is a
 * prompt or threshold problem; one the response cap dropped is a size problem.
 *
 * Off by default and never part of a default payload. When on, every gate
 * records what went in, what came out, and why each dropped candidate left,
 * with the invariant `sum(dropped_by_reason) == in - out`.
 */

export type TraceLevel = "off" | "summary" | "full";

/** Canonical gate names, in pipeline order. */
export type GateName =
   | "index"
   | "candidate"
   | "value_attach"
   | "refine"
   | "rerank"
   | "gap_cut"
   | "page"
   | "budget"
   | "delivered";

export interface GateRecord {
   gate: GateName;
   in: number;
   out: number;
   dropped_by_reason: Record<string, number>;
   ms?: number;
   /** "ok", "skipped:<why>" or "failed:<kind>" for an LLM stage. */
   status?: string;
   llm_calls?: number;
   prompt_tokens?: number;
   completion_tokens?: number;
   cache_hits?: number;
}

/** One candidate as a stage saw it, for offline replay of level sweeps. */
export interface TraceCandidate {
   entity_id: string;
   source: string | undefined;
   model_path: string;
   target: number | undefined;
   /** Cosine (semantic) or normalised lunr score (lexical). */
   score: number | undefined;
   /** LLM level for the row's best target, when refine scored it. */
   level?: string;
   /**
    * Refine's verdict per search target index: a level (LOW/MEDIUM/HIGH), or
    * `omitted` (the LLM left it out), `capped` (never sent, over the per-source
    * or total cap) or `unscored` (its batch failed). Present for dropped rows
    * too, which is what makes a `refine.minLevel` sweep replayable offline.
    */
   levels?: Record<string, string>;
   reason?: string;
   kept: boolean;
   dropped_by?: string;
}

export interface TraceOutput {
   config_fingerprint: string;
   level: Exclude<TraceLevel, "off">;
   retrieval: "semantic" | "lexical" | "listing";
   gates: GateRecord[];
   response_chars: number;
   llm?: {
      calls: number;
      failures: number;
      cache_hits: number;
      prompt_tokens: number;
      completion_tokens: number;
      ms: number;
   };
   candidates?: TraceCandidate[];
}

export class TraceBuilder {
   readonly gates: GateRecord[] = [];
   private candidates: TraceCandidate[] = [];

   constructor(readonly level: Exclude<TraceLevel, "off">) {}

   /**
    * Record one gate. If the stated reasons do not add up to `in - out` the
    * difference is filed under `unattributed` so the invariant always holds in
    * the output and the gap is visible, rather than throwing on a request or
    * hiding it.
    */
   gate(
      gate: GateName,
      inCount: number,
      outCount: number,
      reasons: Record<string, number> = {},
      extra: Partial<Omit<GateRecord, "gate" | "in" | "out" | "dropped_by_reason">> = {},
   ): GateRecord {
      const dropped: Record<string, number> = {};
      let sum = 0;
      for (const [reason, n] of Object.entries(reasons)) {
         if (n > 0) {
            dropped[reason] = n;
            sum += n;
         }
      }
      const gap = inCount - outCount - sum;
      if (gap !== 0) dropped.unattributed = (dropped.unattributed ?? 0) + gap;
      const record: GateRecord = {
         gate,
         in: inCount,
         out: outCount,
         dropped_by_reason: dropped,
         ...extra,
      };
      this.gates.push(record);
      return record;
   }

   /** Whether a stage should spend effort building per-candidate detail. */
   get wantsCandidates(): boolean {
      return this.level === "full";
   }

   setCandidates(candidates: TraceCandidate[]): void {
      if (this.wantsCandidates) this.candidates = candidates;
   }

   /**
    * Patch each candidate from what a stage now knows (a new score, a reason,
    * or that it was dropped). Only does work in full mode.
    */
   annotate(patch: (c: TraceCandidate) => Partial<TraceCandidate>): void {
      if (!this.wantsCandidates) return;
      for (const c of this.candidates) {
         if (c.kept) Object.assign(c, patch(c));
      }
   }

   /** Mark candidates dropped by a later gate. */
   markDropped(
      isDropped: (c: TraceCandidate) => boolean,
      reason: string,
   ): void {
      if (!this.wantsCandidates) return;
      for (const c of this.candidates) {
         if (c.kept && isDropped(c)) {
            c.kept = false;
            c.dropped_by = reason;
         }
      }
   }

   output(args: {
      fingerprint: string;
      retrieval: TraceOutput["retrieval"];
      responseChars: number;
      llm?: LlmUsage;
   }): TraceOutput {
      const out: TraceOutput = {
         config_fingerprint: args.fingerprint,
         level: this.level,
         retrieval: args.retrieval,
         gates: this.gates,
         response_chars: args.responseChars,
      };
      if (args.llm && args.llm.calls + args.llm.cacheHits > 0) {
         out.llm = {
            calls: args.llm.calls,
            failures: args.llm.failures,
            cache_hits: args.llm.cacheHits,
            prompt_tokens: args.llm.promptTokens,
            completion_tokens: args.llm.completionTokens,
            ms: args.llm.ms,
         };
      }
      if (this.wantsCandidates) out.candidates = this.candidates;
      return out;
   }
}
