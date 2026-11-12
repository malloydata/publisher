// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { QueryMaterializer } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { resetMaterializationTelemetryForTesting } from "../materialization_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../test_helpers/metrics_harness";
import { routeQuery, type Variant } from "./query_router";

const runnable = (name: string) => ({ name }) as unknown as QueryMaterializer;

function variant(
   kind: Variant["kind"],
   outcome: "answers" | "misses" | "inapplicable",
   calls: string[],
): Variant<string> {
   return {
      kind,
      applicable: outcome !== "inapplicable",
      attempt: async () => {
         calls.push(`attempt:${kind}`);
         if (outcome === "misses") throw new Error(`${kind} cannot answer`);
         return { runnable: runnable(kind), extra: `${kind}-extra` };
      },
      onSkip: () => {
         calls.push(`skip:${kind}`);
      },
   };
}

describe("routeQuery", () => {
   it("takes the first candidate that answers, and tries nothing after it", async () => {
      const calls: string[] = [];
      const decision = await routeQuery(
         [
            variant("storage", "answers", calls),
            variant("rollup", "answers", calls),
         ],
         async () => false,
      );
      expect(decision).toMatchObject({
         served: "variant",
         kind: "storage",
         extra: "storage-extra",
      });
      expect(calls).toEqual(["attempt:storage"]);
      expect(decision.considered).toEqual([
         { kind: "storage", outcome: "used" },
      ]);
   });

   it("falls through a miss to the next candidate, recording why", async () => {
      const calls: string[] = [];
      const decision = await routeQuery(
         [
            variant("storage", "misses", calls),
            variant("rollup", "answers", calls),
         ],
         async () => false,
      );
      expect(decision).toMatchObject({ served: "variant", kind: "rollup" });
      expect(calls).toEqual([
         "attempt:storage",
         "skip:storage",
         "attempt:rollup",
      ]);
      expect(decision.considered).toEqual([
         {
            kind: "storage",
            outcome: "skipped",
            reason: "does_not_answer",
            detail: "storage cannot answer",
         },
         { kind: "rollup", outcome: "used" },
      ]);
   });

   it("answers live when no candidate answers", async () => {
      const calls: string[] = [];
      const decision = await routeQuery(
         [
            variant("storage", "misses", calls),
            variant("rollup", "misses", calls),
         ],
         async () => false,
      );
      expect(decision.served).toBe("live");
      expect(decision.considered.map((c) => c.reason)).toEqual([
         "does_not_answer",
         "does_not_answer",
      ]);
   });

   it("skips every candidate behind a row-level gate without compiling any", async () => {
      const calls: string[] = [];
      const decision = await routeQuery(
         [
            variant("storage", "answers", calls),
            variant("rollup", "answers", calls),
         ],
         async () => true,
      );
      expect(decision.served).toBe("live");
      expect(calls).toEqual([]);
      expect(decision.considered.map((c) => c.reason)).toEqual([
         "row_level_gate",
         "row_level_gate",
      ]);
   });

   it("asks about the gate once when a candidate applies, and never when none does", async () => {
      let asked = 0;
      const gate = async () => {
         asked++;
         return false;
      };
      await routeQuery(
         [variant("storage", "misses", []), variant("rollup", "answers", [])],
         gate,
      );
      expect(asked).toBe(1);
      // An inapplicable candidate cannot route, so the gate's answer is moot.
      await routeQuery([variant("rollup", "inapplicable", [])], gate);
      expect(asked).toBe(1);
      const none = await routeQuery<string>([], gate);
      expect(asked).toBe(1);
      expect(none).toEqual({ served: "live", considered: [] });
   });

   it("skips an inapplicable candidate without attempting it", async () => {
      const calls: string[] = [];
      const decision = await routeQuery(
         [variant("rollup", "inapplicable", calls)],
         async () => false,
      );
      expect(calls).toEqual([]);
      expect(decision.considered).toEqual([
         { kind: "rollup", outcome: "skipped", reason: "not_applicable" },
      ]);
   });

   it("records a candidate's own explanation of its skip, and survives one that throws", async () => {
      const explained: Variant<string> = {
         ...variant("storage", "misses", []),
         explain: () => ({ code: "rollup_missing_field", callerDetail: "x" }),
      };
      const broken: Variant<string> = {
         ...variant("rollup", "misses", []),
         explain: () => {
            throw new Error("boom");
         },
      };
      const decision = await routeQuery([explained, broken], async () => false);
      expect(decision.considered).toEqual([
         {
            kind: "storage",
            outcome: "skipped",
            reason: "does_not_answer",
            detail: "storage cannot answer",
            code: "rollup_missing_field",
            callerDetail: "x",
         },
         {
            kind: "rollup",
            outcome: "skipped",
            reason: "does_not_answer",
            detail: "rollup cannot answer",
         },
      ]);
   });
});

describe("routeQuery metrics", () => {
   let harness: MetricsHarness;
   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetMaterializationTelemetryForTesting();
   });
   afterEach(async () => {
      resetMaterializationTelemetryForTesting();
      await harness.shutdown();
   });

   const count = (labels: Record<string, string>) =>
      harness.collectCounter("publisher_query_variant_routing_total", labels);

   it("counts each candidate by variant, outcome and reason", async () => {
      await routeQuery(
         [variant("storage", "misses", []), variant("rollup", "answers", [])],
         async () => false,
      );
      expect(
         await count({
            variant: "storage",
            outcome: "skipped",
            reason: "does_not_answer",
         }),
      ).toBe(1);
      expect(
         await count({ variant: "rollup", outcome: "used", reason: "none" }),
      ).toBe(1);
   });

   it("counts a gated skip, and nothing for a hypothetical route", async () => {
      await routeQuery([variant("rollup", "answers", [])], async () => true);
      await routeQuery([variant("rollup", "answers", [])], async () => false, {
         recordMetrics: false,
      });
      expect(
         await count({
            variant: "rollup",
            outcome: "skipped",
            reason: "row_level_gate",
         }),
      ).toBe(1);
      expect(
         await count({ variant: "rollup", outcome: "used", reason: "none" }),
      ).toBe(0);
   });
});
