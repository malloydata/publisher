import { afterEach, beforeEach, describe, expect, it } from "bun:test";

import {
   recordQueryCapExceeded,
   resetQueryCapTelemetryForTesting,
} from "./query_cap_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "./test_helpers/metrics_harness";

describe("query_cap_metrics", () => {
   let harness: MetricsHarness;

   beforeEach(async () => {
      harness = await startMetricsHarness();
      // Drop cached instruments so they re-init against the new
      // provider; otherwise this test's writes go to a counter
      // bound to the previous provider's reader.
      resetQueryCapTelemetryForTesting();
   });

   afterEach(async () => {
      delete process.env.PUBLISHER_MAX_QUERY_ROWS;
      delete process.env.PUBLISHER_MAX_RESPONSE_BYTES;
      resetQueryCapTelemetryForTesting();
      await harness.shutdown();
   });

   it("publisher_query_cap_exceeded_total ticks per call, labeled by cap_type and source", async () => {
      recordQueryCapExceeded("rows", "connection_sql");
      recordQueryCapExceeded("rows", "connection_sql");
      recordQueryCapExceeded("bytes", "model_query");
      recordQueryCapExceeded("rows", "notebook_cell");

      expect(
         await harness.collectCounter("publisher_query_cap_exceeded_total", {
            cap_type: "rows",
            source: "connection_sql",
         }),
      ).toBe(2);
      expect(
         await harness.collectCounter("publisher_query_cap_exceeded_total", {
            cap_type: "bytes",
            source: "model_query",
         }),
      ).toBe(1);
      expect(
         await harness.collectCounter("publisher_query_cap_exceeded_total", {
            cap_type: "rows",
            source: "notebook_cell",
         }),
      ).toBe(1);
   });

   it("counts an unserializable response under its own cap_type, not as bytes", async () => {
      // The distinction is the point of the label: `bytes` means a response
      // measured over the configured cap, `unserializable` means it could not be
      // turned into JSON at all, which may not have exceeded any cap. Folding the
      // second into the first would mix two failures under one alert. Emitted by
      // stringifyQueryResponse; not emitted with source connection_sql, which has
      // no such guard on its own path.
      recordQueryCapExceeded("unserializable", "model_query");
      recordQueryCapExceeded("unserializable", "notebook_cell");

      expect(
         await harness.collectCounter("publisher_query_cap_exceeded_total", {
            cap_type: "unserializable",
            source: "model_query",
         }),
      ).toBe(1);
      expect(
         await harness.collectCounter("publisher_query_cap_exceeded_total", {
            cap_type: "unserializable",
            source: "notebook_cell",
         }),
      ).toBe(1);
      // And it must not have landed in the bytes series.
      expect(
         await harness.collectCounter("publisher_query_cap_exceeded_total", {
            cap_type: "bytes",
            source: "model_query",
         }),
      ).toBe(0);
   });

   it("publisher_max_query_rows gauge reports the live env-var value", async () => {
      process.env.PUBLISHER_MAX_QUERY_ROWS = "12345";
      // Prime telemetry — the gauges install on the first
      // counter-emitting call (`recordQueryCapExceeded`); in
      // production that's the first 413, in tests we trigger it
      // explicitly so the gauge is observable without a 413.
      recordQueryCapExceeded("rows", "connection_sql");
      expect(await harness.collectGauge("publisher_max_query_rows")).toBe(
         12345,
      );
   });

   it("publisher_max_response_bytes gauge reports the live env-var value", async () => {
      process.env.PUBLISHER_MAX_RESPONSE_BYTES = "9876543";
      recordQueryCapExceeded("bytes", "connection_sql");
      expect(await harness.collectGauge("publisher_max_response_bytes")).toBe(
         9876543,
      );
   });

   it("publisher_max_query_rows gauge reports 0 when the cap is opted out", async () => {
      process.env.PUBLISHER_MAX_QUERY_ROWS = "0";
      recordQueryCapExceeded("rows", "connection_sql");
      expect(await harness.collectGauge("publisher_max_query_rows")).toBe(0);
   });

   it("publisher_max_query_rows gauge reports -1 on misconfig so dashboards reveal the bad value", async () => {
      process.env.PUBLISHER_MAX_QUERY_ROWS = "not-a-number";
      // Misconfig must not crash the scrape; -1 is the agreed
      // signal mirroring `publisher_query_timeout_ms`.
      recordQueryCapExceeded("rows", "connection_sql");
      expect(await harness.collectGauge("publisher_max_query_rows")).toBe(-1);
   });
});
