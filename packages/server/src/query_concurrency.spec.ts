// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { EventEmitter } from "events";
import type { NextFunction, Request, Response } from "express";

import { ServiceUnavailableError } from "./errors";
import {
   getActiveQueryCount,
   queryConcurrencyMiddleware,
   resetActiveQueryCountForTesting,
   resetQueryConcurrencyTelemetryForTesting,
} from "./query_concurrency";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "./test_helpers/metrics_harness";
import { installSettings } from "./test_helpers/settings_env";

/** Puts back the settings the current case installed. */
let restoreSettings: (() => void) | undefined;

/**
 * Make `cap` the PUBLISHER_MAX_CONCURRENT_QUERIES of the current settings (or
 * the default, given none), replacing whatever this case installed before.
 */
function setCap(cap?: string): void {
   restoreSettings?.();
   restoreSettings = installSettings({
      env: cap === undefined ? {} : { PUBLISHER_MAX_CONCURRENT_QUERIES: cap },
   }).restore;
}

function restoreCap(): void {
   restoreSettings?.();
   restoreSettings = undefined;
}

function makeReq(path = "/api/v0/test"): Request {
   return { path } as unknown as Request;
}

/**
 * Minimal Response stub: just needs `on` to capture the release
 * listeners, `emit` for tests to fire them, and an `end` for the
 * middleware to wrap. Wraps an EventEmitter so the on/emit semantics
 * match real Express responses (multiple listeners, listener order,
 * etc.). `end` emits nothing, as on a runtime that never reports the
 * response finishing.
 */
function makeRes(): Response & {
   fireFinish: () => void;
   fireClose: () => void;
} {
   const ee = new EventEmitter();
   const res = ee as unknown as Response & {
      fireFinish: () => void;
      fireClose: () => void;
   };
   res.fireFinish = (): void => {
      ee.emit("finish");
   };
   res.fireClose = (): void => {
      ee.emit("close");
   };
   res.end = (() => res) as unknown as typeof res.end;
   return res;
}

function callMiddleware(
   req: Request,
   res: Response,
): { next: NextFunction; error: { value: unknown } } {
   const errorBox: { value: unknown } = { value: undefined };
   const next: NextFunction = (err) => {
      errorBox.value = err;
   };
   queryConcurrencyMiddleware(req, res, next);
   return { next, error: errorBox };
}

describe("queryConcurrencyMiddleware", () => {
   beforeEach(() => {
      // Belt-and-suspenders: every test starts from a clean gauge.
      resetActiveQueryCountForTesting();
      setCap();
   });
   afterEach(() => {
      restoreCap();
      resetActiveQueryCountForTesting();
   });

   it("passes through when the limit is 0 (opt-out)", () => {
      setCap("0");
      const res = makeRes();
      const { error } = callMiddleware(makeReq(), res);
      expect(error.value).toBeUndefined();
      // Crucially: the counter stays at zero so opt-out really is
      // opt-out — not "still tracks, just never rejects".
      expect(getActiveQueryCount()).toBe(0);
   });

   it("admits the first request under the cap and increments the gauge", () => {
      setCap("2");
      const { error } = callMiddleware(makeReq(), makeRes());
      expect(error.value).toBeUndefined();
      expect(getActiveQueryCount()).toBe(1);
   });

   it("rejects the (cap+1)-th in-flight request with ServiceUnavailableError", () => {
      setCap("2");
      callMiddleware(makeReq(), makeRes());
      callMiddleware(makeReq(), makeRes());
      // Two in flight; the third must be turned away.
      const { error } = callMiddleware(makeReq(), makeRes());
      expect(error.value).toBeInstanceOf(ServiceUnavailableError);
      expect((error.value as Error).message).toContain(
         "PUBLISHER_MAX_CONCURRENT_QUERIES",
      );
      // Gauge is unchanged by the rejection (we never claimed the
      // slot), so subsequent legitimate completions don't go
      // negative.
      expect(getActiveQueryCount()).toBe(2);
   });

   it("decrements on response 'finish' (normal completion)", () => {
      setCap("2");
      const res = makeRes();
      callMiddleware(makeReq(), res);
      expect(getActiveQueryCount()).toBe(1);
      res.fireFinish();
      expect(getActiveQueryCount()).toBe(0);
   });

   it("decrements on response 'close' (client disconnect)", () => {
      // A client tearing down the socket before the response
      // finishes is the failure case that, without 'close'
      // handling, would leak slots until the process restart.
      setCap("2");
      const res = makeRes();
      callMiddleware(makeReq(), res);
      res.fireClose();
      expect(getActiveQueryCount()).toBe(0);
   });

   it("decrements only once even when both 'finish' and 'close' fire", () => {
      // Express + Node fire both events in some versions when a
      // long-poll response wraps up just as the client disconnects.
      // The release must be idempotent or the counter goes negative
      // and we hand out one extra slot than the operator configured.
      setCap("1");
      const res = makeRes();
      callMiddleware(makeReq(), res);
      res.fireFinish();
      res.fireClose();
      expect(getActiveQueryCount()).toBe(0);

      // A second request after the double-fire must still be
      // admitted (proving the counter didn't underflow into a
      // permanently-rejecting state).
      const second = makeRes();
      const { error } = callMiddleware(makeReq(), second);
      expect(error.value).toBeUndefined();
      expect(getActiveQueryCount()).toBe(1);
   });

   it("decrements when the handler ends the response, even if neither 'finish' nor 'close' follows", () => {
      // Bun 1.3.13 emits neither event on a response whose client went
      // away mid-request -- not at the disconnect, and not when the
      // handler later writes its answer to the dead socket. `end()` is
      // the call every handler makes on every runtime once its work is
      // done, so it is what must give the slot back.
      setCap("1");
      const res = makeRes();
      callMiddleware(makeReq(), res);
      res.end();
      expect(getActiveQueryCount()).toBe(0);

      // On a runtime that does report the finish, the events that follow
      // the `end()` must not release a second time.
      res.fireFinish();
      res.fireClose();
      const second = callMiddleware(makeReq(), makeRes());
      expect(second.error.value).toBeUndefined();
      expect(getActiveQueryCount()).toBe(1);
   });

   it("re-admits after an in-flight request completes (gauge rolls forward)", () => {
      setCap("1");
      const firstRes = makeRes();
      callMiddleware(makeReq(), firstRes);
      // Second request is over the cap and rejected.
      const second = callMiddleware(makeReq(), makeRes());
      expect(second.error.value).toBeInstanceOf(ServiceUnavailableError);

      // First request finishes; the next call should now be
      // admitted — proving the cap is not a one-shot fuse.
      firstRes.fireFinish();
      const third = callMiddleware(makeReq(), makeRes());
      expect(third.error.value).toBeUndefined();
      expect(getActiveQueryCount()).toBe(1);
   });

   it("reads the cap from the current settings on every call, not once at module load", () => {
      // The middleware must respect the cap the current settings hold
      // on the next request, not cache the value it first saw: a
      // settings change (here, a re-install) applies without
      // rebuilding the middleware.
      setCap("1");
      callMiddleware(makeReq(), makeRes());
      // At the cap.
      const denied = callMiddleware(makeReq(), makeRes());
      expect(denied.error.value).toBeInstanceOf(ServiceUnavailableError);

      // The cap is raised; the next request is admitted.
      setCap("3");
      const admitted = callMiddleware(makeReq(), makeRes());
      expect(admitted.error.value).toBeUndefined();
      expect(getActiveQueryCount()).toBe(2);
   });

   describe("telemetry", () => {
      let harness: MetricsHarness;
      beforeEach(async () => {
         harness = await startMetricsHarness();
         resetActiveQueryCountForTesting();
         resetQueryConcurrencyTelemetryForTesting();
      });
      afterEach(async () => {
         restoreCap();
         resetActiveQueryCountForTesting();
         resetQueryConcurrencyTelemetryForTesting();
         await harness.shutdown();
      });

      it("publisher_query_concurrency_rejections_total ticks on each 503", async () => {
         setCap("1");
         callMiddleware(makeReq("/api/v0/test"), makeRes());
         // At the cap; this one must be rejected.
         const denied = callMiddleware(makeReq("/api/v0/test"), makeRes());
         expect(denied.error.value).toBeInstanceOf(ServiceUnavailableError);
         expect(
            await harness.collectCounter(
               "publisher_query_concurrency_rejections_total",
            ),
         ).toBe(1);
      });

      it("publisher_query_active_slots gauge reflects the live in-flight count", async () => {
         setCap("5");
         callMiddleware(makeReq(), makeRes());
         callMiddleware(makeReq(), makeRes());
         // The middleware's lazy telemetry init runs on every
         // request, so by now the gauge callback should be
         // attached and the next scrape reads 2.
         expect(
            await harness.collectGauge("publisher_query_active_slots"),
         ).toBe(2);
      });

      it("publisher_query_active_slots gauge follows releases (decrements on res.finish)", async () => {
         setCap("5");
         const res = makeRes();
         callMiddleware(makeReq(), res);
         expect(
            await harness.collectGauge("publisher_query_active_slots"),
         ).toBe(1);
         res.fireFinish();
         // A scrape after release must reflect the new value;
         // otherwise an operator can't tell "leaking slot" from
         // "real load".
         expect(
            await harness.collectGauge("publisher_query_active_slots"),
         ).toBe(0);
      });

      it("publisher_query_max_slots gauge reports the current cap", async () => {
         setCap("17");
         callMiddleware(makeReq(), makeRes());
         expect(await harness.collectGauge("publisher_query_max_slots")).toBe(
            17,
         );
      });

      it("publisher_query_max_slots gauge reports 0 when concurrency is opted out", async () => {
         setCap("0");
         callMiddleware(makeReq(), makeRes());
         expect(await harness.collectGauge("publisher_query_max_slots")).toBe(
            0,
         );
      });
   });
});
