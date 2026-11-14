// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { QueryMaterializer } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
   type MetricsHarness,
   startMetricsHarness,
} from "../test_helpers/metrics_harness";
import { resetInstrumentsForTesting } from "../telemetry";
import { recordRoutingDecision, type RoutedExtra } from "./model";
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
      const rollupOnly: RouteDecision<RoutedExtra> = {
         served: "live",
         considered: [
            { kind: "rollup", outcome: "skipped", reason: "row_level_gate" },
         ],
      };
      recordRoutingDecision(rollupOnly);
      expect(await count("blocked_by_row_level_gate")).toBe(1);

      const both: RouteDecision<RoutedExtra> = {
         served: "live",
         considered: [
            { kind: "storage", outcome: "skipped", reason: "row_level_gate" },
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

   it("records a storage hit with its origin, and a miss as live_fallback", async () => {
      recordRoutingDecision({
         served: "variant",
         kind: "storage",
         runnable: {} as QueryMaterializer,
         extra: {
            kind: "storage",
            virtualMap: new Map(),
            bindings: [],
            origin: "preaggregate",
            tablesRead: [],
         },
         considered: [{ kind: "storage", outcome: "used" }],
      });
      recordRoutingDecision({
         served: "live",
         considered: [
            { kind: "storage", outcome: "skipped", reason: "does_not_answer" },
         ],
      });
      expect(
         await harness.collectCounter(METRIC, {
            outcome: "storage",
            origin: "preaggregate",
         }),
      ).toBe(1);
      expect(await count("live_fallback")).toBe(1);
   });
});
