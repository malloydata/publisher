import { afterEach, beforeEach, describe, expect, it } from "bun:test";

import { QueryTimeoutError } from "./errors";
import {
   PUBLISHER_QUERY_TIMEOUT_REASON,
   resetQueryTimeoutTelemetryForTesting,
   runWithQueryTimeout,
} from "./query_timeout";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "./test_helpers/metrics_harness";

describe("runWithQueryTimeout", () => {
   it("returns the inner result when fn finishes before the timeout", async () => {
      const result = await runWithQueryTimeout(async (signal) => {
         expect(signal).toBeInstanceOf(AbortSignal);
         expect(signal.aborted).toBe(false);
         return "ok";
      }, 1000);
      expect(result).toBe("ok");
   });

   it("hands fn a signal even when the timeout is disabled (uniform contract)", async () => {
      // timeoutMs=0 is the "operator opted out" path. We still want
      // callers to be able to forward `signal` unconditionally; the
      // signal must exist, be an AbortSignal, and never fire.
      let observed: AbortSignal | undefined;
      const result = await runWithQueryTimeout(async (signal) => {
         observed = signal;
         await new Promise((r) => setTimeout(r, 10));
         return "still-here";
      }, 0);
      expect(result).toBe("still-here");
      expect(observed).toBeInstanceOf(AbortSignal);
      expect(observed?.aborted).toBe(false);
   });

   it("aborts the signal and throws QueryTimeoutError when fn exceeds the budget", async () => {
      let observedSignal: AbortSignal | undefined;
      await expect(
         runWithQueryTimeout(async (signal) => {
            observedSignal = signal;
            // Mimic a slow driver: resolve only when the signal aborts
            // (so the test doesn't hang). The thrown error from the
            // driver is irrelevant — runWithQueryTimeout owns the
            // verdict once the timer has fired.
            await new Promise<void>((_resolve, reject) => {
               signal.addEventListener("abort", () =>
                  reject(new Error("driver: aborted by AbortSignal")),
               );
            });
         }, 25),
      ).rejects.toBeInstanceOf(QueryTimeoutError);
      expect(observedSignal?.aborted).toBe(true);
      // The reason sentinel lets composed helpers (e.g. streaming
      // cap-abort) distinguish "publisher timeout" from "their own
      // abort" without coupling to message strings.
      expect(observedSignal?.reason).toBe(PUBLISHER_QUERY_TIMEOUT_REASON);
   });

   it("includes the configured timeout in the QueryTimeoutError message (operator can grep logs)", async () => {
      let caught: unknown;
      try {
         await runWithQueryTimeout(async (signal) => {
            await new Promise<void>((_resolve, reject) => {
               signal.addEventListener("abort", () =>
                  reject(new Error("driver aborted")),
               );
            });
         }, 17);
      } catch (err) {
         caught = err;
      }
      expect(caught).toBeInstanceOf(QueryTimeoutError);
      expect((caught as Error).message).toContain("17ms");
      expect((caught as Error).message).toContain("PUBLISHER_QUERY_TIMEOUT_MS");
   });

   it("re-throws non-timeout errors verbatim (does not mask driver failures)", async () => {
      // Driver fails *before* the timeout; we must surface its error
      // unchanged so the controller's normal error mapping kicks in
      // (502 ConnectionError, 400 BadRequestError, etc.) — wrapping
      // every failure in QueryTimeoutError would lie to clients.
      const driverError = new Error("upstream connection refused");
      await expect(
         runWithQueryTimeout(async () => {
            throw driverError;
         }, 1000),
      ).rejects.toBe(driverError);
   });

   it("does not wrap an error that happens to mention 'abort' if the timer never fired", async () => {
      // Edge case: a driver might surface its own AbortError for an
      // unrelated reason (e.g. caller canceled, transport reset). If
      // the publisher's timer never fired, the error is not ours and
      // must not be re-cast as QueryTimeoutError.
      const fakeAbort = Object.assign(new Error("aborted by something else"), {
         name: "AbortError",
      });
      await expect(
         runWithQueryTimeout(async () => {
            throw fakeAbort;
         }, 1000),
      ).rejects.toBe(fakeAbort);
   });

   describe("telemetry", () => {
      let harness: MetricsHarness;
      beforeEach(async () => {
         harness = await startMetricsHarness();
         // Drop cached instruments so they re-init against the new
         // provider; otherwise this test's writes go to a counter
         // bound to the *previous* provider's reader.
         resetQueryTimeoutTelemetryForTesting();
      });
      afterEach(async () => {
         delete process.env.PUBLISHER_QUERY_TIMEOUT_MS;
         await harness.shutdown();
         resetQueryTimeoutTelemetryForTesting();
      });

      /**
       * Install telemetry without firing a timeout. After the
       * `ensureTimeoutTelemetry` fix any successful call is enough
       * — both the counter and the gauge register on every entry,
       * not just the timeout branch.
       */
      async function primeTelemetry(): Promise<void> {
         await runWithQueryTimeout(async () => 0, 10_000);
      }

      it("publisher_query_timeout_total ticks each time the timer fires", async () => {
         // Establish baseline (0) before the trigger so this test
         // isn't sensitive to whatever else has happened earlier.
         expect(
            await harness.collectCounter("publisher_query_timeout_total"),
         ).toBe(0);
         await expect(
            runWithQueryTimeout(async (signal) => {
               await new Promise<void>((_resolve, reject) => {
                  signal.addEventListener("abort", () =>
                     reject(new Error("driver aborted")),
                  );
               });
            }, 15),
         ).rejects.toBeInstanceOf(QueryTimeoutError);
         expect(
            await harness.collectCounter("publisher_query_timeout_total"),
         ).toBe(1);
      });

      it("does NOT tick the counter on non-timeout errors (driver failed before deadline)", async () => {
         await expect(
            runWithQueryTimeout(async () => {
               throw new Error("upstream broken");
            }, 1000),
         ).rejects.toThrow("upstream broken");
         // A driver failure is not a timeout — the counter must
         // stay at zero or operators will chase phantom timeouts.
         expect(
            await harness.collectCounter("publisher_query_timeout_total"),
         ).toBe(0);
      });

      it("publisher_query_timeout_ms gauge is registered after the FIRST call, not just after a timeout fires", async () => {
         // Regression test for the lazy-init bug where the gauge
         // installed only inside the timeout branch — leaving
         // `publisher_query_timeout_ms` absent from `/metrics`
         // until the first 504. Operators tuning the timeout
         // BEFORE getting paged need this visible.
         process.env.PUBLISHER_QUERY_TIMEOUT_MS = "30000";
         await runWithQueryTimeout(async () => "ok", 60_000);
         expect(await harness.collectGauge("publisher_query_timeout_ms")).toBe(
            30000,
         );
      });

      it("publisher_query_timeout_ms gauge reports the current config", async () => {
         process.env.PUBLISHER_QUERY_TIMEOUT_MS = "42000";
         await primeTelemetry();
         expect(await harness.collectGauge("publisher_query_timeout_ms")).toBe(
            42000,
         );
      });

      it("publisher_query_timeout_ms gauge reports 0 when the timeout is opted out", async () => {
         process.env.PUBLISHER_QUERY_TIMEOUT_MS = "0";
         await primeTelemetry();
         expect(await harness.collectGauge("publisher_query_timeout_ms")).toBe(
            0,
         );
      });

      it("publisher_query_timeout_ms gauge surfaces -1 on misconfig instead of crashing the scrape", async () => {
         process.env.PUBLISHER_QUERY_TIMEOUT_MS = "garbage";
         await primeTelemetry();
         // Operators must be able to *see* misconfig in dashboards
         // — silently dropping the data point would hide the
         // problem until a query timed out.
         expect(await harness.collectGauge("publisher_query_timeout_ms")).toBe(
            -1,
         );
      });
   });

   it("clears the timer on success so the event loop can exit promptly", async () => {
      // Hard to assert directly without leaking implementation
      // internals. Proxy assertion: the call returns quickly and a
      // subsequent timer doesn't race us. If the timer leaked we'd
      // also see a QueryTimeoutError on the *next* call below — the
      // signal would already be aborted.
      const result = await runWithQueryTimeout(async () => "fast", 5000);
      expect(result).toBe("fast");
      // Run a second call: a leaked timer would have aborted the
      // first signal, but the second call gets its own signal so
      // this is really a smoke check.
      const result2 = await runWithQueryTimeout(async (signal) => {
         expect(signal.aborted).toBe(false);
         return "also-fast";
      }, 5000);
      expect(result2).toBe("also-fast");
   });
});
