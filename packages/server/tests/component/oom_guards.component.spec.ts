// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * End-to-end integration test for the OOM-mitigation guardrails
 * (Steps 1-6). Mounts a real Express app with the same middleware
 * + controller wiring `server.ts` uses, then exercises each layer
 * of the defense via HTTP requests.
 *
 * What's covered, in firing order:
 *   1. Admission gate — memory governor flags back-pressure →
 *      `ConnectionController` short-circuits with 503 BEFORE the
 *      connector is touched.
 *   2. Concurrency middleware — when the pod is at
 *      `PUBLISHER_MAX_CONCURRENT_QUERIES`, new requests get 503
 *      BEFORE the controller is invoked.
 *   3. Timeout — when the underlying driver takes longer than
 *      `PUBLISHER_QUERY_TIMEOUT_MS`, the request gets 504 and the
 *      abort signal fires.
 *   4. Row cap — when the driver returns more rows than
 *      `PUBLISHER_MAX_QUERY_ROWS`, the request gets 413.
 *
 * Mocked: only the Malloy `Connection.runSQL`. Everything else
 * (admission gate, concurrency middleware, timeout helper, cap
 * detection, error-to-HTTP mapping) is real production code.
 */

import type { Connection, RunSQLOptions } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import express from "express";
import sinon from "sinon";
import request from "supertest";

import { ConnectionController } from "../../src/controller/connection.controller";
import {
   internalErrorToHttpError,
   ServiceUnavailableError,
} from "../../src/errors";
import {
   queryConcurrency,
   resetActiveQueryCountForTesting,
   resetQueryConcurrencyTelemetryForTesting,
} from "../../src/query_concurrency";
import type { EnvironmentStore } from "../../src/service/environment_store";
import { installSettings } from "../../src/test_helpers/settings_env";

function buildApp(
   runSQL: sinon.SinonStub,
   opts: { assertCanAdmitQuery?: sinon.SinonStub } = {},
): {
   app: express.Express;
   assertCanAdmitQuery: sinon.SinonStub;
   runSQL: sinon.SinonStub;
} {
   const fakeConnection = { runSQL } as unknown as Connection;
   const assertCanAdmitQuery =
      opts.assertCanAdmitQuery ?? sinon.stub().returns(undefined);
   const fakeEnv = { assertCanAdmitQuery };
   const fakeStore = {
      getEnvironment: sinon.stub().resolves(fakeEnv),
   } as unknown as EnvironmentStore;
   const controller = new ConnectionController(fakeStore);
   // Bypass the connector lookup; the test only cares about the
   // guardrail chain on top of a stub.
   sinon
      .stub(
         controller as unknown as {
            getMalloyConnection: (...args: unknown[]) => Promise<Connection>;
         },
         "getMalloyConnection",
      )
      .resolves(fakeConnection);

   const app = express();
   app.use(express.json());

   // Same shape as `server.ts` registers for `/sqlQuery` — middleware
   // first, then a thin route handler that delegates to the controller.
   app.post(
      "/api/v0/environments/:env/connections/:conn/sqlQuery",
      queryConcurrency(),
      async (req, res) => {
         try {
            const result = await controller.getConnectionQueryData(
               req.params.env,
               req.params.conn,
               req.body.sqlStatement as string,
               (req.body.options as string | undefined) ?? "",
            );
            res.status(200).json(result);
         } catch (error) {
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   // Mirror the global error handler from `server.ts` — required
   // for the concurrency middleware's `next(err)` path to map
   // `ServiceUnavailableError` to a 503 instead of Express's
   // default 500. Without this the integration test would be
   // exercising a different error path than production.
   app.use(
      (
         err: Error,
         _req: express.Request,
         res: express.Response,
         _next: express.NextFunction,
      ): void => {
         const { json, status } = internalErrorToHttpError(err);
         res.status(status).json(json);
      },
   );

   return { app, assertCanAdmitQuery, runSQL };
}

describe("OOM guardrails: end-to-end chain", () => {
   /** Puts back this suite's settings, so none bleed into later tests. */
   let restoreSettings: (() => void) | undefined;

   /**
    * Install this case's settings: query metadata on (it ships dark; the happy
    * path below asserts the correlation id the response carries, which needs
    * it on) plus `env`. Replaces the case's earlier install.
    */
   function installEnv(env: Record<string, string> = {}): void {
      restoreSettings?.();
      restoreSettings = installSettings({
         env: { PUBLISHER_QUERY_METADATA: "on", ...env },
      }).restore;
   }

   beforeEach(() => {
      resetActiveQueryCountForTesting();
      resetQueryConcurrencyTelemetryForTesting();
      installEnv();
   });

   afterEach(() => {
      sinon.restore();
      resetActiveQueryCountForTesting();
      resetQueryConcurrencyTelemetryForTesting();
      restoreSettings?.();
      restoreSettings = undefined;
   });

   it("admission gate fires FIRST: 503 before the connector is touched", async () => {
      const runSQL = sinon.stub().resolves({ rows: [], totalRows: 0 });
      const assertCanAdmitQuery = sinon
         .stub()
         .throws(
            new ServiceUnavailableError(
               "Publisher is under memory pressure and cannot accept new queries.",
            ),
         );
      const { app } = buildApp(runSQL, { assertCanAdmitQuery });

      const res = await request(app)
         .post("/api/v0/environments/e/connections/c/sqlQuery")
         .send({ sqlStatement: "SELECT 1" });

      expect(res.status).toBe(503);
      // Critical load-shedding invariant: connector must NOT have
      // been called when the gate rejects.
      expect(runSQL.called).toBe(false);
   });

   it("concurrency cap fires SECOND: 503 before the controller is touched, once the slot pool is full", async () => {
      installEnv({ PUBLISHER_MAX_CONCURRENT_QUERIES: "1" });
      // The first query holds its slot until the second request has been
      // answered. Ordered by events, not timers: a fixed head start for the
      // first request and a fixed hold time for its query lost the race on a
      // loaded runner, where the second request arrived after the slot was
      // released and got a 200.
      let entered!: () => void;
      const firstHoldsSlot = new Promise<void>(
         (resolve) => (entered = resolve),
      );
      let release!: () => void;
      const released = new Promise<void>((resolve) => (release = resolve));
      const runSQL = sinon.stub().callsFake(async (sql: string) => {
         // Only the first request's query waits, so a second request that
         // wrongly gets a slot fails the assertions below rather than hanging.
         if (sql === "SELECT 1") {
            entered();
            await released;
         }
         return { rows: [], totalRows: 0 };
      });
      const { app } = buildApp(runSQL);

      // `.then` starts the request; supertest sends nothing until then.
      const firstResponse = request(app)
         .post("/api/v0/environments/e/connections/c/sqlQuery")
         .send({ sqlStatement: "SELECT 1" })
         .then((res) => res);
      // The middleware takes the slot before the controller runs, and gives it
      // back when the response finishes, so the slot is held from here on. A
      // first request answered without reaching the query ends the wait too,
      // so that regression fails the status checks instead of timing out.
      await Promise.race([firstHoldsSlot, firstResponse]);
      const second = await request(app)
         .post("/api/v0/environments/e/connections/c/sqlQuery")
         .send({ sqlStatement: "SELECT 2" });
      release();
      const first = await firstResponse;

      expect(first.status).toBe(200);
      expect(second.status).toBe(503);
      // Connector should still have been called only once — the
      // middleware rejected the second request before it reached
      // the controller.
      expect(runSQL.callCount).toBe(1);
   });

   it("timeout fires THIRD: 504 when the driver outlasts PUBLISHER_QUERY_TIMEOUT_MS, and the abort signal is delivered to the driver", async () => {
      installEnv({ PUBLISHER_QUERY_TIMEOUT_MS: "20" });
      let observedSignal: AbortSignal | undefined;
      const runSQL = sinon.stub().callsFake(
         (_sql: string, opts: RunSQLOptions) =>
            new Promise((_resolve, reject) => {
               observedSignal = opts.abortSignal;
               opts.abortSignal?.addEventListener("abort", () =>
                  // The driver MUST reject when its abort signal
                  // fires; otherwise the timeout helper has nothing
                  // to convert to 504 (the success-after-timeout
                  // race is explicitly allowed).
                  reject(new Error("driver: aborted")),
               );
            }),
      );
      const { app } = buildApp(runSQL);

      const res = await request(app)
         .post("/api/v0/environments/e/connections/c/sqlQuery")
         .send({ sqlStatement: "SELECT pg_sleep(1)" });

      expect(res.status).toBe(504);
      expect(observedSignal).toBeDefined();
      expect(observedSignal?.aborted).toBe(true);
   });

   it("row cap fires LAST: 413 when the driver overshoots PUBLISHER_MAX_QUERY_ROWS", async () => {
      installEnv({ PUBLISHER_MAX_QUERY_ROWS: "2" });
      // Driver returns 3 rows even though we asked for cap+1 = 3
      // — the sentinel pattern catches this and 413s.
      const rows = [{ a: 1 }, { a: 2 }, { a: 3 }];
      const runSQL = sinon.stub().resolves({ rows, totalRows: rows.length });
      const { app } = buildApp(runSQL);

      const res = await request(app)
         .post("/api/v0/environments/e/connections/c/sqlQuery")
         .send({ sqlStatement: "SELECT * FROM big_table" });

      expect(res.status).toBe(413);
      expect(JSON.stringify(res.body)).toContain("more than 2 rows");
   });

   it("happy path: all gates pass-through, 200 with the result body", async () => {
      const rows = [{ a: 1 }];
      const runSQL = sinon.stub().resolves({ rows, totalRows: rows.length });
      const { app } = buildApp(runSQL);

      const res = await request(app)
         .post("/api/v0/environments/e/connections/c/sqlQuery")
         .send({ sqlStatement: "SELECT 1" });

      expect(res.status).toBe(200);
      expect(res.body.data).toEqual(
         JSON.stringify({ rows, totalRows: rows.length }),
      );
      // The response carries the id the statement was tagged with, so a caller
      // can find this query in the backend's own history.
      expect(res.body.queryCorrelationId).toMatch(/^[0-9a-f-]{36}$/);
   });
});
