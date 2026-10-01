// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   DEFAULT_RETRIEVAL_CONFIG,
   RetrievalConfigError,
   applyOverride,
   isDefaultRetrievalConfig,
   resolveEgress,
   resolveRetrievalConfig,
   retrievalConfigFingerprint,
   retrievalOverridesEnabled,
} from "./retrieval_config";

const problems = (raw: unknown): string[] => {
   try {
      resolveRetrievalConfig(raw);
   } catch (e) {
      if (e instanceof RetrievalConfigError) return e.problems;
      throw e;
   }
   return [];
};

describe("resolveRetrievalConfig", () => {
   it("yields the defaults for a missing or null block", () => {
      expect(resolveRetrievalConfig(undefined)).toEqual(
         DEFAULT_RETRIEVAL_CONFIG,
      );
      expect(resolveRetrievalConfig(null)).toEqual(DEFAULT_RETRIEVAL_CONFIG);
      expect(resolveRetrievalConfig({})).toEqual(DEFAULT_RETRIEVAL_CONFIG);
   });

   it("has every LLM stage off by default", () => {
      const c = resolveRetrievalConfig(undefined);
      expect(c.refine.enabled).toBe(false);
      expect(c.rerank.enabled).toBe(false);
      expect(c.enrichment.enabled).toBe(false);
      expect(c.dimensionalValues.mode).toBe("off");
      expect(c.hybrid.mode).toBe("off");
      expect(isDefaultRetrievalConfig(c)).toBe(true);
   });

   it("overlays a partial block onto the defaults", () => {
      const c = resolveRetrievalConfig({
         refine: { enabled: true, batchSize: 5 },
      });
      expect(c.refine.enabled).toBe(true);
      expect(c.refine.batchSize).toBe(5);
      expect(c.refine.maxCandidates).toBe(120);
      expect(c.rerank.topSources).toBe(8);
      expect(isDefaultRetrievalConfig(c)).toBe(false);
   });

   it("returns a frozen object", () => {
      const c = resolveRetrievalConfig({ refine: { enabled: true } });
      expect(Object.isFrozen(c)).toBe(true);
      expect(Object.isFrozen(c.refine)).toBe(true);
   });

   it("does not mutate the shared defaults", () => {
      resolveRetrievalConfig({ refine: { batchSize: 3 } });
      expect(DEFAULT_RETRIEVAL_CONFIG.refine.batchSize).toBe(15);
   });

   it("rejects an unknown key and suggests the near miss", () => {
      const p = problems({ refine: { batchSiz: 5 } });
      expect(p).toHaveLength(1);
      expect(p[0]).toContain("Unknown retrieval.refine.batchSiz");
      expect(p[0]).toContain('Did you mean "batchSize"?');
   });

   it("rejects an unknown top-level group", () => {
      const p = problems({ refin: {} });
      expect(p[0]).toContain('Did you mean "refine"?');
   });

   it("names the field, the constraint, the value, and a fix", () => {
      const p = problems({ refine: { batchSize: 0 } });
      expect(p).toHaveLength(1);
      expect(p[0]).toBe(
         "Invalid retrieval.refine.batchSize: expected an integer in [1, 100], got 0. Fix: use the default 15, or a value that fits.",
      );
   });

   it("reports every problem at once", () => {
      const p = problems({
         refine: { minLevel: "MAYBE", batchSize: 1.5 },
         rerank: { topSources: "8" },
         hybrid: { mode: "sum" },
      });
      expect(p).toHaveLength(4);
   });

   it("rejects non-integers, NaN and out-of-range numbers", () => {
      expect(problems({ refine: { maxCandidates: 1.5 } })).toHaveLength(1);
      expect(problems({ llm: { temperature: 3 } })).toHaveLength(1);
      expect(problems({ llm: { temperature: Number.NaN } })).toHaveLength(1);
      expect(problems({ embedding: { minSimilarity: 1 } })).toHaveLength(1);
      expect(problems({ embedding: { minSimilarity: 0.999 } })).toHaveLength(0);
   });

   it("accepts null only where the field is nullable", () => {
      expect(problems({ embedding: { minSimilarity: null } })).toEqual([]);
      expect(problems({ response: { maxChars: null } })).toEqual([]);
      expect(problems({ refine: { batchSize: null } })).toHaveLength(1);
   });

   it("validates enum values", () => {
      expect(problems({ refine: { minLevel: "HIGH" } })).toEqual([]);
      const p = problems({ refine: { minLevel: "high" } });
      expect(p[0]).toContain('one of "LOW", "MEDIUM", "HIGH"');
   });

   it("accepts llm.enabled as auto or boolean only", () => {
      expect(problems({ llm: { enabled: "auto" } })).toEqual([]);
      expect(problems({ llm: { enabled: false } })).toEqual([]);
      expect(problems({ llm: { enabled: "yes" } })).toHaveLength(1);
   });

   it("validates the score knots", () => {
      const ok = [
         [0, 0],
         [2, 0.5],
         [4, 1],
      ];
      expect(problems({ scoring: { knots: ok } })).toEqual([]);
      // x must strictly increase
      expect(
         problems({
            scoring: {
               knots: [
                  [0, 0],
                  [0, 1],
               ],
            },
         }),
      ).toHaveLength(1);
      // y must not decrease
      expect(
         problems({
            scoring: {
               knots: [
                  [0, 1],
                  [1, 0],
               ],
            },
         }),
      ).toHaveLength(1);
      // y within [0, 1]
      expect(
         problems({
            scoring: {
               knots: [
                  [0, 0],
                  [1, 2],
               ],
            },
         }),
      ).toHaveLength(1);
      expect(problems({ scoring: { knots: [[0, 0]] } })).toHaveLength(1);
   });

   it("requires objects where groups are expected", () => {
      expect(problems({ refine: true })[0]).toContain(
         "Invalid retrieval.refine: expected an object",
      );
   });

   it("requires a string list for facets and include/exclude", () => {
      expect(problems({ embedding: { facets: ["name", "kw"] } })).toEqual([]);
      expect(problems({ embedding: { facets: [1] } })).toHaveLength(1);
      expect(
         problems({ dimensionalValues: { include: "orders.status" } }),
      ).toHaveLength(1);
   });

   it("does not let a bad field drive the result", () => {
      let error: RetrievalConfigError | undefined;
      try {
         resolveRetrievalConfig({ refine: { batchSize: -5 } });
      } catch (e) {
         error = e as RetrievalConfigError;
      }
      expect(error).toBeInstanceOf(RetrievalConfigError);
   });
});

describe("egress", () => {
   it("defaults to names and docs only, today's boundary", () => {
      const e = resolveEgress(resolveRetrievalConfig(undefined));
      expect(e).toEqual({
         names: true,
         docs: true,
         schemaContext: false,
         code: false,
         dimensionalValues: false,
      });
   });

   it("turns every class on with the full preset", () => {
      const e = resolveEgress(
         resolveRetrievalConfig({ egress: { preset: "full" } }),
      );
      expect(Object.values(e).every(Boolean)).toBe(true);
   });

   it("lets an explicit boolean beat the preset in both directions", () => {
      const e = resolveEgress(
         resolveRetrievalConfig({
            egress: { preset: "full", code: false, docs: false },
         }),
      );
      expect(e.code).toBe(false);
      expect(e.docs).toBe(false);
      expect(e.names).toBe(true);
      const d = resolveEgress(
         resolveRetrievalConfig({ egress: { code: true } }),
      );
      expect(d.code).toBe(true);
      expect(d.dimensionalValues).toBe(false);
   });

   it("has no class that could carry a predicate annotation", () => {
      const classes = Object.keys(
         resolveEgress(resolveRetrievalConfig(undefined)),
      );
      expect(classes.join(",")).not.toMatch(
         /predicate|access|authorize|filter/i,
      );
   });
});

describe("applyOverride", () => {
   const base = resolveRetrievalConfig({ refine: { enabled: true } });

   it("applies a query-time knob", () => {
      const { config, errors } = applyOverride(base, {
         refine: { minLevel: "HIGH" },
         response: { gapCut: 0.5 },
      });
      expect(errors).toEqual([]);
      expect(config.refine.minLevel).toBe("HIGH");
      expect(config.response.gapCut).toBe(0.5);
      // untouched fields survive
      expect(config.refine.enabled).toBe(true);
      expect(config.refine.batchSize).toBe(15);
   });

   it("lets a request switch a stage off or on", () => {
      expect(
         applyOverride(base, { refine: { enabled: false } }).config.refine
            .enabled,
      ).toBe(false);
      expect(
         applyOverride(base, { rerank: { enabled: true } }).config.rerank
            .enabled,
      ).toBe(true);
   });

   it("does not change the base", () => {
      applyOverride(base, { refine: { minLevel: "HIGH" } });
      expect(base.refine.minLevel).toBe("MEDIUM");
   });

   it("treats an empty group as a no-op", () => {
      const r = applyOverride(base, { refine: {} });
      expect(r.errors).toEqual([]);
      expect(retrievalConfigFingerprint(r.config)).toBe(
         retrievalConfigFingerprint(base),
      );
   });

   it("refuses to widen egress", () => {
      const r = applyOverride(base, { egress: { preset: "full" } });
      expect(r.errors).toHaveLength(1);
      expect(r.errors[0]).toContain("egress.preset");
      expect(r.errors[0]).toContain("not overridable per request");
      expect(r.config).toBe(base);
   });

   it("refuses index-time and infrastructure settings", () => {
      for (const o of [
         { enrichment: { enabled: true } },
         { indexing: { deadlineMs: 1000 } },
         { llm: { enabled: true } },
         { llm: { concurrency: 32 } },
         { llm: { extraBody: { x: 1 } } },
         { dimensionalValues: { mode: "auto" } },
         { embedding: { documentPrefix: "x" } },
      ]) {
         expect(applyOverride(base, o).errors.length).toBeGreaterThan(0);
      }
   });

   it("rejects an invalid value with the same rules as the file", () => {
      const r = applyOverride(base, { refine: { batchSize: 0 } });
      expect(r.errors[0]).toContain("Invalid retrieval.refine.batchSize");
   });

   it("rejects an unknown key rather than ignoring it", () => {
      const r = applyOverride(base, { refine: { minLevle: "HIGH" } });
      expect(r.errors.length).toBeGreaterThan(0);
   });

   it("rejects a non-object override", () => {
      expect(applyOverride(base, "refine").errors).toHaveLength(1);
      expect(applyOverride(base, [1]).errors).toHaveLength(1);
      expect(applyOverride(base, null).errors).toHaveLength(1);
   });

   it("reports every forbidden key, not the first", () => {
      const r = applyOverride(base, {
         egress: { code: true },
         indexing: { deadlineMs: 5000 },
      });
      expect(r.errors).toHaveLength(2);
   });
});

describe("fingerprint", () => {
   it("is stable and sensitive to every change", () => {
      const a = resolveRetrievalConfig(undefined);
      const b = resolveRetrievalConfig({});
      expect(retrievalConfigFingerprint(a)).toBe(retrievalConfigFingerprint(b));
      const c = resolveRetrievalConfig({ refine: { minLevel: "HIGH" } });
      expect(retrievalConfigFingerprint(c)).not.toBe(
         retrievalConfigFingerprint(a),
      );
   });

   it("ignores key order in a record", () => {
      const a = resolveRetrievalConfig({
         llm: { extraBody: { a: 1, b: 2 } },
      });
      const b = resolveRetrievalConfig({
         llm: { extraBody: { b: 2, a: 1 } },
      });
      expect(retrievalConfigFingerprint(a)).toBe(retrievalConfigFingerprint(b));
   });
});

describe("the Credible-parity settings", () => {
   it("default to Publisher's own behavior, so an unconfigured server is unchanged", () => {
      const c = resolveRetrievalConfig(undefined);
      expect(c.embedding.representation).toBe("facets");
      expect(c.candidates).toEqual({
         perTargetLimit: null,
         window: "global",
         perSourceLimit: 10,
      });
      expect(c.scoring.joinDampingMode).toBe("fraction");
      expect(c.dimensionalValues.refine).toEqual({
         enabled: false,
         minLevel: "MEDIUM",
         batchSize: 15,
         maxPerSource: 10,
         maxCandidates: 120,
      });
   });

   it("accept the values that match Credible", () => {
      const c = resolveRetrievalConfig({
         embedding: { representation: "single" },
         candidates: { window: "per-source", perSourceLimit: 10 },
         scoring: { joinDepthDamping: 0.9, joinDampingMode: "whole" },
         dimensionalValues: { mode: "annotated", refine: { enabled: true } },
         llm: { models: { valueRefine: "small-model" } },
      });
      expect(c.embedding.representation).toBe("single");
      expect(c.dimensionalValues.refine.enabled).toBe(true);
      expect(c.llm.models.valueRefine).toBe("small-model");
   });

   it("reject a bad value with the setting's name", () => {
      expect(
         problems({ embedding: { representation: "double" } })[0],
      ).toContain("retrieval.embedding.representation");
      expect(problems({ candidates: { window: "local" } })[0]).toContain(
         "retrieval.candidates.window",
      );
      expect(problems({ candidates: { perSourceLimit: 0 } })[0]).toContain(
         "retrieval.candidates.perSourceLimit",
      );
      expect(problems({ scoring: { joinDampingMode: "all" } })[0]).toContain(
         "retrieval.scoring.joinDampingMode",
      );
      expect(
         problems({ dimensionalValues: { refine: { batchSize: 0 } } })[0],
      ).toContain("retrieval.dimensionalValues.refine.batchSize");
   });

   it("let a request change the window and value refine, but not the index's representation", () => {
      const base = resolveRetrievalConfig({});
      expect(
         applyOverride(base, { candidates: { window: "per-source" } }).errors,
      ).toEqual([]);
      expect(
         applyOverride(base, {
            dimensionalValues: { refine: { enabled: true } },
         }).errors,
      ).toEqual([]);
      const bad = applyOverride(base, {
         embedding: { representation: "single" },
      });
      expect(bad.errors[0]).toContain("embedding.representation");
   });
});

describe("retrievalOverridesEnabled", () => {
   it("is off unless the gate is set", () => {
      expect(retrievalOverridesEnabled({})).toBe(false);
      expect(
         retrievalOverridesEnabled({ PUBLISHER_RETRIEVAL_OVERRIDES: "0" }),
      ).toBe(false);
      expect(
         retrievalOverridesEnabled({ PUBLISHER_RETRIEVAL_OVERRIDES: "1" }),
      ).toBe(true);
      expect(
         retrievalOverridesEnabled({ PUBLISHER_RETRIEVAL_OVERRIDES: "true" }),
      ).toBe(true);
   });
});
