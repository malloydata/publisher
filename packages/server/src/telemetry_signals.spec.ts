// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import type * as Malloy from "@malloydata/malloy-interfaces";
import { buildQueryEnvelope } from "./mcp/query_envelope";
import type { ServeBinding } from "./service/materialization_serve_transform";
import {
   StorageServeShape,
   type ServeShapeHost,
} from "./service/model_serve_shape";
import { resetInstrumentsForTesting } from "./telemetry";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "./test_helpers/metrics_harness";

/** Signals an operator cannot otherwise see: a truncated agent answer, a withheld stale table. */
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

   it("counts each stale binding the freshness gate withholds", async () => {
      const shape = new StorageServeShape({} as ServeShapeHost);
      const stale = (sourceName: string): ServeBinding =>
         ({
            sourceName,
            tablePath: `lake.${sourceName}`,
            freshAsOf: "2000-01-01T00:00:00Z",
            freshnessWindowSeconds: 60,
            freshnessFallback: "live",
         }) as ServeBinding;
      shape.setServeBindings([stale("a"), stale("b")]);
      await expect(shape.loadServeShapeQuery("run: a -> x")).rejects.toThrow(
         "no fresh storage serve bindings",
      );
      expect(
         await harness.collectCounter(
            "publisher_storage_bindings_withheld_total",
         ),
      ).toBe(2);
   });
});
