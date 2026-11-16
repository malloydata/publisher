// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import type * as Malloy from "@malloydata/malloy-interfaces";
import { buildQueryEnvelope } from "./mcp/query_envelope";
import { resetInstrumentsForTesting } from "./telemetry";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "./test_helpers/metrics_harness";

/** Signals an operator cannot otherwise see: a truncated agent answer. */
describe("silent-degradation counters", () => {
   let harness: MetricsHarness;
   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetInstrumentsForTesting();
   });
   afterEach(async () => {
      await harness.shutdown();
      resetInstrumentsForTesting();
   });

   it("counts an execute_query answer cut off at the server's row limit", async () => {
      const rows = (n: number) =>
         Array.from({ length: n }, (_, id) => ({ id }));
      const result = { schema: { fields: [] } } as unknown as Malloy.Result;
      buildQueryEnvelope(rows(5), 5, result);
      buildQueryEnvelope(rows(4), 5, result);
      // An author's own limit reached is the answer, not a truncation.
      buildQueryEnvelope(rows(5), 5, result, [], undefined, "query");
      expect(
         await harness.collectCounter("publisher_mcp_row_limit_hits_total"),
      ).toBe(1);
   });
});
