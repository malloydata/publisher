// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { getLlmConfig } from "../config";
import { getLlmRunner, LlmBudget, type LlmRunner } from "../service/llm_runner";
import {
   applyOverride,
   getRetrievalConfig,
   isDefaultRetrievalConfig,
   retrievalConfigFingerprint,
   retrievalOverridesEnabled,
   type RetrievalConfig,
} from "./retrieval_config";
import { TraceBuilder, type TraceLevel, type TraceOutput } from "./trace";

/** Request header carrying a JSON override of the query-time knobs. */
export const OVERRIDE_HEADER = "x-publisher-retrieval";
/** Request header asking for a per-stage trace: `summary` or `full`. */
export const TRACE_HEADER = "x-publisher-retrieval-trace";
/** Overrides are small knob settings; anything bigger is a mistake. */
const MAX_OVERRIDE_CHARS = 4_096;

type Headers = Record<string, string | string[] | undefined>;

/** The LLM handle a request's stages share: one runner, one budget. */
export interface RunLlm {
   runner: LlmRunner;
   budget: LlmBudget;
   /** The environment's LLM_MODEL, the last fallback for a stage's model. */
   model: string | undefined;
}

/**
 * Everything one get_context call needs to know about retrieval tuning: the
 * effective config (defaults, plus the operator's block, plus any request
 * override), the trace it should fill, and the LLM budget its stages spend.
 * Built once at the top of the call so no stage re-reads the environment.
 */
export interface RetrievalRun {
   config: RetrievalConfig;
   fingerprint: string;
   /** True when nothing differs from the defaults: the payload stays as it was. */
   isDefault: boolean;
   overridden: boolean;
   trace: TraceBuilder | null;
   /** Plain-language notes for the response's `warnings` (e.g. override ignored). */
   warnings: string[];
   /** Null when the LLM is unavailable or no LLM stage is enabled. */
   llm: RunLlm | null;
}

export type BeginRun =
   | { ok: true; run: RetrievalRun }
   | { ok: false; errors: string[] };

const overrideCache = new Map<string, ReturnType<typeof applyOverride>>();

/** LLM_MODEL from the environment; a malformed LLM_API_BASE means none here. */
export function envLlmModel(): string | undefined {
   try {
      return getLlmConfig()?.model;
   } catch {
      return undefined;
   }
}

function first(v: string | string[] | undefined): string | undefined {
   return Array.isArray(v) ? v[0] : v;
}

/**
 * Resolve the effective config for one request.
 *
 * The override header is honoured only when the operator has opened the gate
 * (`PUBLISHER_RETRIEVAL_OVERRIDES=1`): it is an eval facility, not something a
 * client of a production server should be able to steer. With the gate closed
 * a header that was sent is ignored WITH a warning, so an eval that forgot to
 * open it notices instead of quietly measuring the defaults. An invalid
 * override is an error, never a partial application, because a silently
 * ignored sweep point corrupts the comparison it belongs to.
 */
export function beginRun(
   headers: Headers | undefined,
   base: RetrievalConfig = getRetrievalConfig(),
   env: Record<string, string | undefined> = process.env,
): BeginRun {
   const warnings: string[] = [];
   let config = base;
   let overridden = false;
   let traceLevel: TraceLevel = base.trace.defaultLevel;

   const overrideRaw = first(headers?.[OVERRIDE_HEADER]);
   const traceRaw = first(headers?.[TRACE_HEADER]);
   const gateOpen = retrievalOverridesEnabled(env);

   if ((overrideRaw !== undefined || traceRaw !== undefined) && !gateOpen) {
      warnings.push(
         "Retrieval override ignored: set PUBLISHER_RETRIEVAL_OVERRIDES=1 on the server to accept X-Publisher-Retrieval and X-Publisher-Retrieval-Trace.",
      );
   } else {
      if (overrideRaw !== undefined) {
         if (overrideRaw.length > MAX_OVERRIDE_CHARS) {
            return {
               ok: false,
               errors: [
                  `Invalid X-Publisher-Retrieval: expected at most ${MAX_OVERRIDE_CHARS} characters, got ${overrideRaw.length}. Fix: send only the knobs you are changing.`,
               ],
            };
         }
         const key = `${retrievalConfigFingerprint(base)}|${overrideRaw}`;
         let result = overrideCache.get(key);
         if (!result) {
            let parsed: unknown;
            try {
               parsed = JSON.parse(overrideRaw);
            } catch {
               return {
                  ok: false,
                  errors: [
                     `Invalid X-Publisher-Retrieval: expected JSON, got ${JSON.stringify(overrideRaw.slice(0, 60))}. Fix: send e.g. {"refine":{"minLevel":"HIGH"}}.`,
                  ],
               };
            }
            result = applyOverride(base, parsed);
            if (overrideCache.size >= 64) overrideCache.clear();
            overrideCache.set(key, result);
         }
         if (result.errors.length > 0)
            return { ok: false, errors: result.errors };
         config = result.config;
         overridden = true;
      }
      if (traceRaw !== undefined) {
         const v = traceRaw.trim().toLowerCase();
         if (v !== "summary" && v !== "full" && v !== "off") {
            return {
               ok: false,
               errors: [
                  `Invalid X-Publisher-Retrieval-Trace: expected one of "summary", "full", "off", got ${JSON.stringify(traceRaw)}. Fix: send X-Publisher-Retrieval-Trace: summary.`,
               ],
            };
         }
         traceLevel = v;
      }
   }

   const fingerprint = retrievalConfigFingerprint(config);
   const llmStagesOn =
      config.refine.enabled ||
      config.rerank.enabled ||
      (config.dimensionalValues.mode !== "off" &&
         config.dimensionalValues.refine.enabled);
   const runner = llmStagesOn ? getLlmRunner(config) : null;
   return {
      ok: true,
      run: {
         config,
         fingerprint,
         isDefault: isDefaultRetrievalConfig(config),
         overridden,
         trace: traceLevel === "off" ? null : new TraceBuilder(traceLevel),
         warnings,
         llm: runner
            ? {
                 runner,
                 budget: new LlmBudget(
                    config.llm.maxCallsPerRequest,
                    config.llm.requestBudgetMs,
                 ),
                 model: envLlmModel(),
              }
            : null,
      },
   };
}

/**
 * Add the retrieval-tuning keys to a response, or return it untouched.
 *
 * Both keys are absent from a default response, so a server with nothing
 * configured answers byte-for-byte as it did before any of this existed:
 * `retrieval_config` (a fingerprint, so an eval can tell two runs used
 * different settings) appears only when something differs from the defaults,
 * and `retrieval_trace` only when one was asked for.
 */
export function attachRetrievalMeta<T extends Record<string, unknown>>(
   payload: T,
   run: RetrievalRun | undefined,
   retrieval: TraceOutput["retrieval"],
): T {
   if (!run || (run.isDefault && !run.trace)) return payload;
   const out: Record<string, unknown> = { ...payload };
   if (!run.isDefault) out.retrieval_config = run.fingerprint;
   if (run.trace) {
      const responseChars = JSON.stringify(payload).length;
      out.retrieval_trace = run.trace.output({
         fingerprint: run.fingerprint,
         retrieval,
         responseChars,
         llm: run.llm?.budget.usage,
      });
   }
   return out as T;
}

export function _clearOverrideCacheForTests(): void {
   overrideCache.clear();
}
