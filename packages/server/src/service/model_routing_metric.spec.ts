// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { QueryMaterializer } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
   type MetricsHarness,
   startMetricsHarness,
} from "../test_helpers/metrics_harness";
import { resetInstrumentsForTesting } from "../telemetry";
import type { LegAnswer } from "./materialization/serve/leg";
import { recordRoutingDecision } from "./query_serving";
import type { RouteDecision } from "./query_router";

const METRIC = "publisher_storage_serve_routing_total";

describe("recordRoutingDecision", () => {
   let harness: MetricsHarness;
   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetInstrumentsForTesting();
   });
   afterEach(async () => {
      await harness.shutdown();
      resetInstrumentsForTesting();
   });

   const count = (outcome: string) =>
      harness.collectCounter(METRIC, { outcome });

   it("records a gate block once per query, whichever tiers it vetoed", async () => {
      // The label's contract covers both tiers: a model whose only candidate
      // is a rollup still records the block, or a blocked query records nothing.
      const rollupOnly: RouteDecision<LegAnswer> = {
         served: "live",
         considered: [
            { kind: "rollup", outcome: "skipped", reason: "row_level_gate" },
         ],
      };
      recordRoutingDecision(rollupOnly);
      expect(await count("blocked_by_row_level_gate")).toBe(1);

      const both: RouteDecision<LegAnswer> = {
         served: "live",
         considered: [
            { kind: "served", outcome: "skipped", reason: "row_level_gate" },
            { kind: "rollup", outcome: "skipped", reason: "row_level_gate" },
         ],
      };
      recordRoutingDecision(both);
      // Once for the query, not once per tier.
      expect(await count("blocked_by_row_level_gate")).toBe(2);
      expect(await count("live_fallback")).toBe(0);
   });

   it("records nothing when neither tier was a candidate", async () => {
      recordRoutingDecision({ served: "live", considered: [] });
      recordRoutingDecision({
         served: "live",
         considered: [
            { kind: "rollup", outcome: "skipped", reason: "does_not_answer" },
         ],
      });
      expect(await harness.collectCounter(METRIC)).toBe(0);
   });

   it("records a served-copy answer as a storage hit, and nothing for its miss", async () => {
      recordRoutingDecision({
         served: "variant",
         kind: "served",
         runnable: {} as QueryMaterializer,
         extra: {
            runnable: {} as QueryMaterializer,
            virtualMap: new Map(),
            shapeBindings: [],
            tablesRead: [],
            graftScope: undefined,
         },
         considered: [{ kind: "served", outcome: "used" }],
      });
      recordRoutingDecision({
         served: "live",
         considered: [
            { kind: "served", outcome: "skipped", reason: "does_not_answer" },
         ],
      });
      expect(
         await harness.collectCounter(METRIC, {
            outcome: "storage",
            origin: "persist",
         }),
      ).toBe(1);
      // A served copy that declined counts nothing: only the retired serve
      // shape's candidate reported a miss, and no candidate has that kind.
      expect(await count("live_fallback")).toBe(0);
   });
});
