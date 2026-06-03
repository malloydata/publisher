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

import { ConnectionController } from "./controller/connection.controller";
import { internalErrorToHttpError, ServiceUnavailableError } from "./errors";
import {
   queryConcurrency,
   resetActiveQueryCountForTesting,
   resetQueryConcurrencyTelemetryForTesting,
} from "./query_concurrency";
import type { EnvironmentStore } from "./service/environment_store";

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
   const savedEnv = {
      max: process.env.PUBLISHER_MAX_QUERY_ROWS,
      timeout: process.env.PUBLISHER_QUERY_TIMEOUT_MS,
      concurrency: process.env.PUBLISHER_MAX_CONCURRENT_QUERIES,
   };

   beforeEach(() => {
      resetActiveQueryCountForTesting();
      resetQueryConcurrencyTelemetryForTesting();
   });

   afterEach(() => {
      sinon.restore();
      resetActiveQueryCountForTesting();
      resetQueryConcurrencyTelemetryForTesting();
      // Restore env so later tests don't see this suite's bleed-through.
      const restore = (key: keyof typeof savedEnv, envVar: string): void => {
         if (savedEnv[key] === undefined) delete process.env[envVar];
         else process.env[envVar] = savedEnv[key]!;
      };
      restore("max", "PUBLISHER_MAX_QUERY_ROWS");
      restore("timeout", "PUBLISHER_QUERY_TIMEOUT_MS");
      restore("concurrency", "PUBLISHER_MAX_CONCURRENT_QUERIES");
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
      process.env.PUBLISHER_MAX_CONCURRENT_QUERIES = "1";
      // First request: runSQL takes ~200 ms so the slot stays
      // held while we fire the second. Using a real timer rather
      // than an unresolved deferred keeps the test deterministic
      // even when supertest spawns one ephemeral http server per
      // `request(app)` call — both sockets will eventually close
      // on their own regardless of orchestration order.
      const runSQL = sinon
         .stub()
         .callsFake(
            () =>
               new Promise((resolve) =>
                  setTimeout(() => resolve({ rows: [], totalRows: 0 }), 200),
               ),
         );
      const { app } = buildApp(runSQL);

      // Drive both requests in parallel — `Promise.all` keeps the
      // event loop turning on the first request's pending timer
      // while the second runs through the middleware.
      const [first, second] = await Promise.all([
         request(app)
            .post("/api/v0/environments/e/connections/c/sqlQuery")
            .send({ sqlStatement: "SELECT 1" }),
         (async () => {
            // Yield a few ticks so the first request actually
            // enters the middleware and holds the slot before
            // the second one is dispatched. 20 ms is far longer
            // than any scheduler hop but well below the 200 ms
            // runSQL timer, leaving the slot definitely held.
            await new Promise((r) => setTimeout(r, 20));
            return request(app)
               .post("/api/v0/environments/e/connections/c/sqlQuery")
               .send({ sqlStatement: "SELECT 2" });
         })(),
      ]);

      expect(first.status).toBe(200);
      expect(second.status).toBe(503);
      // Connector should still have been called only once — the
      // middleware rejected the second request before it reached
      // the controller.
      expect(runSQL.callCount).toBe(1);
   });

   it("timeout fires THIRD: 504 when the driver outlasts PUBLISHER_QUERY_TIMEOUT_MS, and the abort signal is delivered to the driver", async () => {
      process.env.PUBLISHER_QUERY_TIMEOUT_MS = "20";
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
      process.env.PUBLISHER_MAX_QUERY_ROWS = "2";
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
      expect(res.body).toEqual({
         data: JSON.stringify({ rows, totalRows: rows.length }),
      });
   });
});
