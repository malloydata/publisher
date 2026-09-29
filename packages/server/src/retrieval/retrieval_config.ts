// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createHash } from "node:crypto";

/**
 * Every tunable of the LLM-assisted `get_context` pipeline, in one typed,
 * validated object.
 *
 * Two goals shape it. First, each phase can be switched off and each
 * threshold, cap, batch size and model can be moved, so an eval run can sweep
 * them and read recall and precision off the result. Second, the defaults do
 * nothing: with no `retrieval` block and no LLM endpoint configured the
 * `get_context` response is byte-identical to what it was before this module
 * existed (pinned by get_context_payload_pin.spec.ts).
 *
 * Secrets and endpoints (`LLM_API_BASE`, `LLM_API_KEY`, `LLM_MODEL`) are NOT
 * here: they stay in the environment, like EMBEDDING_API_KEY. This object
 * carries tuning only, so it can be committed beside a package and recorded
 * in an eval run.
 *
 * Resolved once (at boot, or per request for an override) and frozen. Nothing
 * re-parses it on the hot path.
 */

export type RelevanceLevel = "LOW" | "MEDIUM" | "HIGH";

export interface RetrievalConfig {
   llm: {
      /** "auto": on iff an LLM endpoint is configured in the environment. */
      enabled: "auto" | boolean;
      /** Fallback model for every stage; env LLM_MODEL is used when null. */
      model: string | null;
      models: {
         refine: string | null;
         rerank: string | null;
         keyphrase: string | null;
         summary: string | null;
      };
      temperature: number;
      seed: number;
      timeoutMs: number;
      maxAttempts: number;
      backoffMs: number;
      /** Process-wide cap on in-flight LLM calls. Keep at 1-2 for Ollama. */
      concurrency: number;
      /** Wall-clock budget for all LLM work inside one get_context call. */
      requestBudgetMs: number;
      maxCallsPerRequest: number;
      jsonMode: "none" | "json_object";
      /** Extra fields merged into every chat request (provider-specific). */
      extraBody: Record<string, unknown>;
      breaker: { failures: number; cooldownMs: number };
      cache: { enabled: boolean; maxEntries: number };
   };
   /**
    * What may leave the machine. `null` follows the preset. Predicate
    * annotations (#(access_filter), #(authorize)) have no class here and can
    * never be sent; see retrieval/egress.ts.
    */
   egress: {
      preset: "default" | "full";
      names: boolean | null;
      docs: boolean | null;
      schemaContext: boolean | null;
      code: boolean | null;
      dimensionalValues: boolean | null;
   };
   embedding: {
      /** null: use EMBEDDING_MIN_SIMILARITY / the provider's floor. */
      minSimilarity: number | null;
      /** Prepended to a search text before embedding (e.g. "search_query: "). */
      queryPrefix: string;
      /** Prepended to indexed text before embedding. Part of the index spec. */
      documentPrefix: string;
      extraBody: Record<string, unknown>;
      /** Merged over extraBody for query requests only (e.g. a query task type). */
      queryExtraBody: Record<string, unknown>;
      /** Query-time allow-list of facets to score: name, doc, kw, sum. */
      facets: string[] | null;
   };
   candidates: {
      /** null: min(150, limit * 3), as before. */
      perTargetLimit: number | null;
   };
   refine: {
      enabled: boolean;
      minLevel: RelevanceLevel;
      /** Drop a candidate the LLM did not mention. */
      dropOmitted: boolean;
      /** Level given to a candidate whose batch failed. */
      unscoredLevel: RelevanceLevel;
      maxPerSource: number;
      maxCandidates: number;
      batchSize: number;
      /** null: llm.concurrency. */
      concurrency: number | null;
      /** Skip the stage when there are this few candidates or fewer. */
      skipIfAtMost: number;
      descChars: number;
      onLexical: boolean;
      promptVersion: string;
   };
   rerank: {
      enabled: boolean;
      topSources: number;
      /** Drop a reranked source scoring below this (0-3). */
      minScore: number;
      /** What happens to sources past topSources: keep them below, or drop. */
      beyondTop: "keep" | "drop";
      maxEntityLines: number;
      valuesPerEntity: number;
      skipIfAtMost: number;
      onLexical: boolean;
      promptVersion: string;
   };
   scoring: {
      /** Piecewise-linear map from the raw [0,4] score to the wire [0,1]. */
      knots: Array<[number, number]>;
      /** Multiplier per join hop on the fractional part only. 1 = off. */
      joinDepthDamping: number;
      sourceRelevance: "best-hit" | "coverage";
   };
   response: {
      maxEntitiesPerSourceTarget: number;
      /** null: no character budget beyond the tool's own. */
      maxChars: number | null;
      /** Drop entities below this fraction of a target's top score. */
      gapCut: number | null;
      matchReason: boolean;
      /** Also return LLM-generated descriptions, as separate fields. */
      surfaceGenerated: boolean;
   };
   /** Hard per-sync limits so indexing finishes in minutes, not hours. */
   indexing: {
      deadlineMs: number;
      maxItemsPerPackage: number;
      maxLlmCallsPerSync: number;
   };
   enrichment: {
      enabled: boolean;
      keyphrase: {
         mode: "when-sparse" | "always" | "never";
         wordThreshold: number;
         viewWordThreshold: number;
         batchSize: number;
         maxCodeChars: number;
         template: string;
         promptVersion: string;
      };
      sourceSummary: { enabled: boolean; promptVersion: string };
      retryAfterMs: number;
   };
   dimensionalValues: {
      mode: "off" | "annotated" | "auto";
      include: string[];
      exclude: string[];
      maxValuesPerDimension: number;
      maxValuesPerPackage: number;
      maxValueChars: number;
      onOverflow: "top-n" | "skip";
      queryTimeoutMs: number;
      refreshMinutes: number;
      embed: boolean;
      lexical: boolean;
      minSimilarity: number | null;
      maxHitsPerTarget: number;
      template: string;
   };
   hybrid: { mode: "off" | "rerank-only" | "union"; rrfK: number };
   trace: { defaultLevel: "off" | "summary" | "full" };
}

export const DEFAULT_RETRIEVAL_CONFIG: RetrievalConfig = {
   llm: {
      enabled: "auto",
      model: null,
      models: {
         refine: null,
         rerank: null,
         keyphrase: null,
         summary: null,
      },
      temperature: 0,
      seed: 7,
      timeoutMs: 30_000,
      maxAttempts: 2,
      backoffMs: 300,
      concurrency: 4,
      requestBudgetMs: 20_000,
      maxCallsPerRequest: 24,
      jsonMode: "none",
      extraBody: {},
      breaker: { failures: 3, cooldownMs: 60_000 },
      cache: { enabled: true, maxEntries: 2_000 },
   },
   egress: {
      preset: "default",
      names: null,
      docs: null,
      schemaContext: null,
      code: null,
      dimensionalValues: null,
   },
   embedding: {
      minSimilarity: null,
      queryPrefix: "",
      documentPrefix: "",
      extraBody: {},
      queryExtraBody: {},
      facets: null,
   },
   candidates: { perTargetLimit: null },
   refine: {
      enabled: false,
      minLevel: "MEDIUM",
      dropOmitted: true,
      unscoredLevel: "MEDIUM",
      maxPerSource: 10,
      maxCandidates: 120,
      batchSize: 15,
      concurrency: null,
      skipIfAtMost: 0,
      descChars: 300,
      onLexical: true,
      promptVersion: "v1",
   },
   rerank: {
      enabled: false,
      topSources: 8,
      minScore: 2,
      beyondTop: "keep",
      maxEntityLines: 20,
      valuesPerEntity: 5,
      skipIfAtMost: 1,
      onLexical: true,
      promptVersion: "v1",
   },
   scoring: {
      knots: [
         [0, 0],
         [1, 0.4],
         [2, 0.7],
         [3, 0.9],
         [4, 1],
      ],
      joinDepthDamping: 1,
      sourceRelevance: "best-hit",
   },
   response: {
      maxEntitiesPerSourceTarget: 10,
      maxChars: null,
      gapCut: null,
      matchReason: true,
      surfaceGenerated: false,
   },
   indexing: {
      deadlineMs: 480_000,
      maxItemsPerPackage: 10_000,
      maxLlmCallsPerSync: 300,
   },
   enrichment: {
      enabled: false,
      keyphrase: {
         mode: "when-sparse",
         wordThreshold: 8,
         viewWordThreshold: 12,
         batchSize: 10,
         maxCodeChars: 1_500,
         template: "{name}: {keyphrase}",
         promptVersion: "v1",
      },
      sourceSummary: { enabled: false, promptVersion: "v1" },
      retryAfterMs: 600_000,
   },
   dimensionalValues: {
      mode: "off",
      include: [],
      exclude: [],
      maxValuesPerDimension: 100,
      maxValuesPerPackage: 2_000,
      maxValueChars: 128,
      onOverflow: "top-n",
      queryTimeoutMs: 30_000,
      refreshMinutes: 1_440,
      embed: true,
      lexical: true,
      minSimilarity: null,
      maxHitsPerTarget: 40,
      template: "{value}",
   },
   hybrid: { mode: "off", rrfK: 60 },
   trace: { defaultLevel: "off" },
};

// ---------------------------------------------------------------------------
// Schema: the constraint for each leaf. Kept beside the defaults' shape so a
// new knob is one line in each place and an unknown key is rejected with a
// suggestion (a typo'd knob silently poisons an eval sweep).
// ---------------------------------------------------------------------------

type Leaf =
   | { t: "bool"; nullable?: boolean }
   | { t: "autoBool" }
   | { t: "int"; min: number; max: number; nullable?: boolean }
   | { t: "num"; min: number; max: number; maxExclusive?: boolean; nullable?: boolean }
   | { t: "enum"; values: readonly string[] }
   | { t: "str"; nullable?: boolean }
   | { t: "strList"; nullable?: boolean }
   | { t: "record" }
   | { t: "knots" };
type Node = Leaf | { t: "group"; fields: Record<string, Node> };

const LEVELS = ["LOW", "MEDIUM", "HIGH"] as const;
const g = (fields: Record<string, Node>): Node => ({ t: "group", fields });
const int = (min: number, max: number, nullable = false): Leaf => ({
   t: "int",
   min,
   max,
   nullable,
});
const bool: Leaf = { t: "bool" };
const nbool: Leaf = { t: "bool", nullable: true };
const nstr: Leaf = { t: "str", nullable: true };

const SCHEMA: Node = g({
   llm: g({
      enabled: { t: "autoBool" },
      model: nstr,
      models: g({
         refine: nstr,
         rerank: nstr,
         keyphrase: nstr,
         summary: nstr,
      }),
      temperature: { t: "num", min: 0, max: 2 },
      seed: int(0, 2_147_483_647),
      timeoutMs: int(100, 600_000),
      maxAttempts: int(1, 5),
      backoffMs: int(0, 60_000),
      concurrency: int(1, 64),
      requestBudgetMs: int(100, 3_600_000),
      maxCallsPerRequest: int(0, 1_000),
      jsonMode: { t: "enum", values: ["none", "json_object"] },
      extraBody: { t: "record" },
      breaker: g({ failures: int(1, 100), cooldownMs: int(0, 3_600_000) }),
      cache: g({ enabled: bool, maxEntries: int(0, 1_000_000) }),
   }),
   egress: g({
      preset: { t: "enum", values: ["default", "full"] },
      names: nbool,
      docs: nbool,
      schemaContext: nbool,
      code: nbool,
      dimensionalValues: nbool,
   }),
   embedding: g({
      minSimilarity: { t: "num", min: 0, max: 1, maxExclusive: true, nullable: true },
      queryPrefix: { t: "str" },
      documentPrefix: { t: "str" },
      extraBody: { t: "record" },
      queryExtraBody: { t: "record" },
      facets: { t: "strList", nullable: true },
   }),
   candidates: g({ perTargetLimit: int(1, 1_000, true) }),
   refine: g({
      enabled: bool,
      minLevel: { t: "enum", values: LEVELS },
      dropOmitted: bool,
      unscoredLevel: { t: "enum", values: LEVELS },
      maxPerSource: int(1, 200),
      maxCandidates: int(1, 2_000),
      batchSize: int(1, 100),
      concurrency: int(1, 64, true),
      skipIfAtMost: int(0, 1_000),
      descChars: int(0, 5_000),
      onLexical: bool,
      promptVersion: { t: "str" },
   }),
   rerank: g({
      enabled: bool,
      topSources: int(1, 100),
      minScore: { t: "num", min: 0, max: 3 },
      beyondTop: { t: "enum", values: ["keep", "drop"] },
      maxEntityLines: int(0, 200),
      valuesPerEntity: int(0, 50),
      skipIfAtMost: int(0, 100),
      onLexical: bool,
      promptVersion: { t: "str" },
   }),
   scoring: g({
      knots: { t: "knots" },
      joinDepthDamping: { t: "num", min: 0, max: 1 },
      sourceRelevance: { t: "enum", values: ["best-hit", "coverage"] },
   }),
   response: g({
      maxEntitiesPerSourceTarget: int(1, 200),
      maxChars: int(1_000, 10_000_000, true),
      gapCut: { t: "num", min: 0, max: 1, maxExclusive: true, nullable: true },
      matchReason: bool,
      surfaceGenerated: bool,
   }),
   indexing: g({
      deadlineMs: int(1_000, 86_400_000),
      maxItemsPerPackage: int(1, 5_000_000),
      maxLlmCallsPerSync: int(0, 1_000_000),
   }),
   enrichment: g({
      enabled: bool,
      keyphrase: g({
         mode: { t: "enum", values: ["when-sparse", "always", "never"] },
         wordThreshold: int(0, 1_000),
         viewWordThreshold: int(0, 1_000),
         batchSize: int(1, 100),
         maxCodeChars: int(0, 20_000),
         template: { t: "str" },
         promptVersion: { t: "str" },
      }),
      sourceSummary: g({ enabled: bool, promptVersion: { t: "str" } }),
      retryAfterMs: int(0, 86_400_000),
   }),
   dimensionalValues: g({
      mode: { t: "enum", values: ["off", "annotated", "auto"] },
      include: { t: "strList" },
      exclude: { t: "strList" },
      maxValuesPerDimension: int(1, 100_000),
      maxValuesPerPackage: int(1, 5_000_000),
      maxValueChars: int(1, 1_024),
      onOverflow: { t: "enum", values: ["top-n", "skip"] },
      queryTimeoutMs: int(100, 600_000),
      refreshMinutes: int(1, 525_600),
      embed: bool,
      lexical: bool,
      minSimilarity: { t: "num", min: 0, max: 1, maxExclusive: true, nullable: true },
      maxHitsPerTarget: int(1, 1_000),
      template: { t: "str" },
   }),
   hybrid: g({
      mode: { t: "enum", values: ["off", "rerank-only", "union"] },
      rrfK: int(1, 1_000),
   }),
   trace: g({ defaultLevel: { t: "enum", values: ["off", "summary", "full"] } }),
});

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

function isPlainObject(v: unknown): v is Record<string, unknown> {
   return typeof v === "object" && v !== null && !Array.isArray(v);
}

function show(v: unknown): string {
   const s = JSON.stringify(v);
   if (s === undefined) return String(v);
   return s.length > 80 ? `${s.slice(0, 77)}...` : s;
}

function editDistance(a: string, b: string): number {
   const dp: number[] = Array.from({ length: b.length + 1 }, (_, i) => i);
   for (let i = 1; i <= a.length; i++) {
      let prev = dp[0];
      dp[0] = i;
      for (let j = 1; j <= b.length; j++) {
         const tmp = dp[j];
         dp[j] =
            a[i - 1] === b[j - 1]
               ? prev
               : 1 + Math.min(prev, dp[j], dp[j - 1]);
         prev = tmp;
      }
   }
   return dp[b.length];
}

function suggest(key: string, known: string[]): string | null {
   let best: string | null = null;
   let bestD = Infinity;
   for (const k of known) {
      const d = editDistance(key.toLowerCase(), k.toLowerCase());
      if (d < bestD) {
         bestD = d;
         best = k;
      }
   }
   return best !== null && bestD <= Math.max(2, Math.floor(best.length / 3))
      ? best
      : null;
}

function describeLeaf(leaf: Leaf): string {
   switch (leaf.t) {
      case "bool":
         return leaf.nullable ? "true, false or null" : "true or false";
      case "autoBool":
         return 'true, false or "auto"';
      case "int":
         return `an integer in [${leaf.min}, ${leaf.max}]${leaf.nullable ? " or null" : ""}`;
      case "num":
         return `a number in [${leaf.min}, ${leaf.max}${leaf.maxExclusive ? ")" : "]"}${leaf.nullable ? " or null" : ""}`;
      case "enum":
         return `one of ${leaf.values.map((v) => `"${v}"`).join(", ")}`;
      case "str":
         return leaf.nullable ? "a string or null" : "a string";
      case "strList":
         return leaf.nullable ? "a list of strings or null" : "a list of strings";
      case "record":
         return "an object";
      case "knots":
         return "a list of [raw, wire] pairs with strictly increasing raw and non-decreasing wire values in [0, 1]";
   }
}

function checkLeaf(leaf: Leaf, value: unknown): boolean {
   switch (leaf.t) {
      case "bool":
         return typeof value === "boolean" || (leaf.nullable === true && value === null);
      case "autoBool":
         return typeof value === "boolean" || value === "auto";
      case "int":
         if (value === null) return leaf.nullable === true;
         return (
            typeof value === "number" &&
            Number.isInteger(value) &&
            value >= leaf.min &&
            value <= leaf.max
         );
      case "num":
         if (value === null) return leaf.nullable === true;
         return (
            typeof value === "number" &&
            Number.isFinite(value) &&
            value >= leaf.min &&
            (leaf.maxExclusive ? value < leaf.max : value <= leaf.max)
         );
      case "enum":
         return typeof value === "string" && leaf.values.includes(value);
      case "str":
         return typeof value === "string" || (leaf.nullable === true && value === null);
      case "strList":
         if (value === null) return leaf.nullable === true;
         return Array.isArray(value) && value.every((v) => typeof v === "string");
      case "record":
         return isPlainObject(value);
      case "knots": {
         if (!Array.isArray(value) || value.length < 2) return false;
         let px = -Infinity;
         let py = -Infinity;
         for (const pair of value) {
            if (
               !Array.isArray(pair) ||
               pair.length !== 2 ||
               typeof pair[0] !== "number" ||
               typeof pair[1] !== "number" ||
               !Number.isFinite(pair[0]) ||
               pair[1] < 0 ||
               pair[1] > 1 ||
               pair[0] <= px ||
               pair[1] < py
            ) {
               return false;
            }
            px = pair[0];
            py = pair[1];
         }
         return true;
      }
   }
}

function clone<T>(v: T): T {
   return structuredClone(v);
}

/**
 * Overlay `raw` onto `defaults` under `node`, appending one message per
 * problem to `errors`. Returns the merged value; on error the default stands
 * for that field, but callers must not use a result that reported errors.
 */
function overlay(
   node: Node,
   defaults: unknown,
   raw: unknown,
   path: string,
   errors: string[],
): unknown {
   if (node.t !== "group") {
      if (raw === undefined) return clone(defaults);
      if (!checkLeaf(node, raw)) {
         errors.push(
            `Invalid ${path}: expected ${describeLeaf(node)}, got ${show(raw)}. Fix: use the default ${show(defaults)}, or a value that fits.`,
         );
         return clone(defaults);
      }
      return clone(raw);
   }
   const out: Record<string, unknown> = {};
   const known = Object.keys(node.fields);
   if (raw !== undefined && !isPlainObject(raw)) {
      errors.push(
         `Invalid ${path}: expected an object, got ${show(raw)}. Fix: use { ... } with keys ${known.join(", ")}.`,
      );
      raw = undefined;
   }
   const rawObj = (raw ?? {}) as Record<string, unknown>;
   for (const key of Object.keys(rawObj)) {
      if (!known.includes(key)) {
         const near = suggest(key, known);
         errors.push(
            `Unknown ${path}.${key}${near ? `. Did you mean "${near}"?` : ""} Fix: remove it, or use one of ${known.join(", ")}.`,
         );
      }
   }
   for (const key of known) {
      out[key] = overlay(
         node.fields[key],
         (defaults as Record<string, unknown>)[key],
         rawObj[key],
         `${path}.${key}`,
         errors,
      );
   }
   return out;
}

function deepFreeze<T>(v: T): T {
   if (typeof v === "object" && v !== null && !Object.isFrozen(v)) {
      Object.freeze(v);
      for (const child of Object.values(v as object)) deepFreeze(child);
   }
   return v;
}

export class RetrievalConfigError extends Error {
   constructor(public readonly problems: string[]) {
      super(problems.join("\n"));
      this.name = "RetrievalConfigError";
   }
}

/**
 * Validate a raw `retrieval` block against the defaults. Throws
 * {@link RetrievalConfigError} listing every problem, so an operator fixes
 * the file in one pass instead of one boot per typo. A missing or `null`
 * block yields the defaults.
 */
export function resolveRetrievalConfig(raw: unknown): RetrievalConfig {
   const errors: string[] = [];
   const merged = overlay(
      SCHEMA,
      DEFAULT_RETRIEVAL_CONFIG,
      raw === null ? undefined : raw,
      "retrieval",
      errors,
   ) as RetrievalConfig;
   if (errors.length > 0) throw new RetrievalConfigError(errors);
   return deepFreeze(merged);
}

// ---------------------------------------------------------------------------
// Per-request override
// ---------------------------------------------------------------------------

/**
 * Keys a request-scoped override may set. Query-time tuning only: an override
 * arrives over the wire, so it can never widen what leaves the machine
 * (`egress.*`), redirect it (endpoints and keys are env, not here), change
 * what an index was built from (`indexing`, `enrichment`,
 * `dimensionalValues` except its query-time knobs, `embedding` prefixes and
 * body), or switch the LLM on where the operator left it off (that is decided
 * by the environment). It may turn query-time stages on or off, because a
 * stage only sends what the egress classes already allow to the endpoint the
 * operator configured.
 */
const OVERRIDABLE_PREFIXES = [
   "refine.",
   "rerank.",
   "scoring.",
   "response.",
   "candidates.",
   "hybrid.",
   "trace.",
   "embedding.minSimilarity",
   "embedding.facets",
   "llm.models.",
   "llm.cache.",
   "dimensionalValues.minSimilarity",
   "dimensionalValues.maxHitsPerTarget",
];

function leafPaths(v: unknown, prefix: string, out: string[]): void {
   if (isPlainObject(v)) {
      // Groups recurse, and an empty group sets nothing. A record such as
      // `llm.extraBody` is only ever forbidden here, so treating any nested
      // object as a group is safe: its children carry the same prefix and are
      // rejected the same way.
      for (const [k, child] of Object.entries(v)) {
         leafPaths(child, prefix === "" ? k : `${prefix}.${k}`, out);
      }
      return;
   }
   out.push(prefix);
}

export interface OverrideResult {
   config: RetrievalConfig;
   errors: string[];
}

/**
 * Apply a request override on top of a resolved config. Every problem is
 * reported (an unknown key, a forbidden key, a bad value); a caller that gets
 * errors must fail the request rather than run with a partly applied override,
 * because a silently ignored sweep point corrupts an eval.
 */
export function applyOverride(
   base: RetrievalConfig,
   override: unknown,
): OverrideResult {
   const errors: string[] = [];
   if (!isPlainObject(override)) {
      return {
         config: base,
         errors: [
            `Invalid retrieval override: expected a JSON object, got ${show(override)}. Fix: send e.g. {"refine":{"minLevel":"HIGH"}}.`,
         ],
      };
   }
   const paths: string[] = [];
   leafPaths(override, "", paths);
   for (const p of paths) {
      if (!OVERRIDABLE_PREFIXES.some((pre) => p === pre || p.startsWith(pre))) {
         errors.push(
            `Invalid retrieval override ${p}: not overridable per request. Fix: set it in publisher.config.json (or the environment) and restart. Overridable: ${OVERRIDABLE_PREFIXES.join(", ")}.`,
         );
      }
   }
   if (errors.length > 0) return { config: base, errors };

   // Merge onto the resolved base by treating it as the raw input, then
   // validate the whole thing again: the same rules apply to an override.
   const merged = deepMerge(clone(base) as unknown, override);
   const validationErrors: string[] = [];
   const result = overlay(
      SCHEMA,
      DEFAULT_RETRIEVAL_CONFIG,
      merged,
      "retrieval",
      validationErrors,
   ) as RetrievalConfig;
   if (validationErrors.length > 0) {
      return { config: base, errors: validationErrors };
   }
   return { config: deepFreeze(result), errors: [] };
}

function deepMerge(base: unknown, patch: unknown): unknown {
   if (!isPlainObject(patch)) return patch;
   const out: Record<string, unknown> = isPlainObject(base)
      ? { ...base }
      : {};
   for (const [k, v] of Object.entries(patch)) {
      out[k] = isPlainObject(v) && isPlainObject(out[k]) ? deepMerge(out[k], v) : v;
   }
   return out;
}

// ---------------------------------------------------------------------------
// Fingerprint, egress and singleton
// ---------------------------------------------------------------------------

function canonical(v: unknown): string {
   if (Array.isArray(v)) return `[${v.map(canonical).join(",")}]`;
   if (isPlainObject(v)) {
      return `{${Object.keys(v)
         .sort()
         .map((k) => `${JSON.stringify(k)}:${canonical(v[k])}`)
         .join(",")}}`;
   }
   return JSON.stringify(v) ?? "null";
}

/** Stable short hash of a config, for run records and mixed-config checks. */
export function retrievalConfigFingerprint(config: RetrievalConfig): string {
   return createHash("sha256").update(canonical(config)).digest("hex").slice(0, 12);
}

const DEFAULT_FINGERPRINT = retrievalConfigFingerprint(DEFAULT_RETRIEVAL_CONFIG);

/** True when nothing about retrieval was changed from the defaults. */
export function isDefaultRetrievalConfig(config: RetrievalConfig): boolean {
   return retrievalConfigFingerprint(config) === DEFAULT_FINGERPRINT;
}

export interface EgressClasses {
   names: boolean;
   docs: boolean;
   schemaContext: boolean;
   code: boolean;
   dimensionalValues: boolean;
}

/**
 * The concrete data classes that may be sent to a provider. The `default`
 * preset is today's boundary (entity names and `#(doc)` text). `full` turns
 * every class on. An explicit boolean beats the preset either way.
 * Predicate annotations are not a class: nothing here can enable them.
 */
export function resolveEgress(config: RetrievalConfig): EgressClasses {
   const full = config.egress.preset === "full";
   const pick = (v: boolean | null, presetValue: boolean): boolean =>
      v === null ? presetValue : v;
   return {
      names: pick(config.egress.names, true),
      docs: pick(config.egress.docs, true),
      schemaContext: pick(config.egress.schemaContext, full),
      code: pick(config.egress.code, full),
      dimensionalValues: pick(config.egress.dimensionalValues, full),
   };
}

let current: RetrievalConfig = resolveRetrievalConfig(undefined);
let testOverride: RetrievalConfig | null = null;

/** The process-wide config, resolved at boot by {@link setRetrievalConfig}. */
export function getRetrievalConfig(): RetrievalConfig {
   return testOverride ?? current;
}

/** Install the config parsed from `publisher.config.json` (boot). */
export function setRetrievalConfig(raw: unknown): RetrievalConfig {
   current = resolveRetrievalConfig(raw);
   return current;
}

/** Test seam: force a config (partial values overlay the defaults). */
export function _setRetrievalConfigForTests(raw: unknown): RetrievalConfig {
   testOverride = resolveRetrievalConfig(raw);
   return testOverride;
}

export function _clearRetrievalConfigForTests(): void {
   testOverride = null;
}

/** Whether `X-Publisher-Retrieval*` headers are honoured at all. */
export function retrievalOverridesEnabled(
   env: Record<string, string | undefined> = process.env,
): boolean {
   const raw = env.PUBLISHER_RETRIEVAL_OVERRIDES?.trim().toLowerCase();
   return raw === "1" || raw === "true" || raw === "yes" || raw === "on";
}
