import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";

import {
   checkHeapConfiguration,
   resetHeapTelemetryForTesting,
} from "./heap_check";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "./test_helpers/metrics_harness";

function makeLogStub(): {
   warn: ReturnType<typeof mock>;
   info: ReturnType<typeof mock>;
} {
   return {
      warn: mock(() => undefined),
      info: mock(() => undefined),
   };
}

describe("checkHeapConfiguration", () => {
   it("warns + reports warned=true when heap limit is below the 2 GiB threshold", () => {
      const log = makeLogStub();
      // 1.5 GiB — below the recommended floor.
      const { warned } = checkHeapConfiguration({
         getHeapStatistics: () => ({
            heap_size_limit: 1.5 * 1024 * 1024 * 1024,
         }),
         log,
      });
      expect(warned).toBe(true);
      expect(log.warn).toHaveBeenCalledTimes(1);
      expect(log.info).not.toHaveBeenCalled();
      const args = log.warn.mock.calls[0] as unknown as [
         string,
         Record<string, unknown>,
      ];
      // Operator must be able to grep for the offending env-var
      // hint without guessing the surrounding sentence.
      expect(args[0]).toContain("--max-old-space-size");
      expect(args[0]).toContain("MiB");
      expect(args[1].heapSizeLimitBytes).toBe(1.5 * 1024 * 1024 * 1024);
   });

   it("logs at info + reports warned=false when heap limit meets the recommendation", () => {
      const log = makeLogStub();
      const { warned, heapSizeLimitBytes } = checkHeapConfiguration({
         getHeapStatistics: () => ({
            heap_size_limit: 4 * 1024 * 1024 * 1024,
         }),
         log,
      });
      expect(warned).toBe(false);
      expect(heapSizeLimitBytes).toBe(4 * 1024 * 1024 * 1024);
      expect(log.warn).not.toHaveBeenCalled();
      expect(log.info).toHaveBeenCalledTimes(1);
   });

   it("treats exactly-the-recommended-value as 'meets threshold' (no warn)", () => {
      // Boundary: the comparison is `<`, so equality should pass.
      // Lock the boundary so a future tightening to `<=` requires
      // an explicit test change rather than silently regressing.
      const log = makeLogStub();
      const { warned } = checkHeapConfiguration({
         getHeapStatistics: () => ({
            heap_size_limit: 2 * 1024 * 1024 * 1024,
         }),
         log,
      });
      expect(warned).toBe(false);
      expect(log.warn).not.toHaveBeenCalled();
   });

   it("does not throw or warn under tiny heaps; the cap helpers still work", () => {
      // Pathological "this pod has 128 MiB" case: we want a noisy
      // warning, not a process crash, so the publisher still boots
      // and the operator sees the message in pod logs.
      const log = makeLogStub();
      expect(() =>
         checkHeapConfiguration({
            getHeapStatistics: () => ({ heap_size_limit: 128 * 1024 * 1024 }),
            log,
         }),
      ).not.toThrow();
      expect(log.warn).toHaveBeenCalledTimes(1);
   });

   it("uses live v8.getHeapStatistics when no override is provided (smoke check)", () => {
      // No assertion on warn/info — the result depends on how Node
      // was started — just that the call resolves and returns a
      // sensible structure. Locks the production code path against
      // accidental coupling to the injected getter.
      const result = checkHeapConfiguration();
      expect(result.heapSizeLimitBytes).toBeGreaterThan(0);
      expect(typeof result.warned).toBe("boolean");
   });

   describe("telemetry", () => {
      let harness: MetricsHarness;

      beforeEach(async () => {
         harness = await startMetricsHarness();
         resetHeapTelemetryForTesting();
      });

      afterEach(async () => {
         resetHeapTelemetryForTesting();
         await harness.shutdown();
      });

      it("publisher_heap_size_limit_bytes reports the live V8 heap_size_limit", async () => {
         const log = makeLogStub();
         checkHeapConfiguration({ log });
         const value = await harness.collectGauge(
            "publisher_heap_size_limit_bytes",
         );
         // Live value — we just assert it's a sensible positive
         // number so the test isn't sensitive to how this Bun
         // process was launched.
         expect(typeof value).toBe("number");
         expect(value).toBeGreaterThan(0);
      });

      it("publisher_heap_used_bytes reports the live V8 used_heap_size", async () => {
         const log = makeLogStub();
         checkHeapConfiguration({ log });
         const value = await harness.collectGauge("publisher_heap_used_bytes");
         expect(typeof value).toBe("number");
         expect(value).toBeGreaterThan(0);
      });

      it("does not register the gauges before checkHeapConfiguration is called (lazy install)", async () => {
         // The gauges are wired up via `installHeapGauges`, which
         // is intentionally called from `checkHeapConfiguration`
         // so the OTel SDK is fully up before instruments are
         // registered. Without that, instruments would bind to
         // NoOp during module load and never emit data.
         expect(
            await harness.collectGauge("publisher_heap_size_limit_bytes"),
         ).toBeUndefined();
      });
   });
});
