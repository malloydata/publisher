// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Makes the compile / sqlSource admission claim behavioural instead of textual.
 *
 * `/query`, `/sqlQuery` and `/sqlTemporaryTable` pass through
 * `queryConcurrency()`, which caps concurrent work at
 * PUBLISHER_MAX_CONCURRENT_QUERIES so a flood cannot saturate the pod. `/compile`
 * and both `/sqlSource` routes reach the same controllers -- compile resolves
 * source schemas against the connection, sqlSource runs a live introspection --
 * but were registered without it, so a flood of either bypassed the cap its
 * sibling query routes enforce.
 *
 * Asserting that the characters `queryConcurrency()` appear between two other
 * strings in `server.ts` would prove the token is present, not that a request is
 * admission-controlled: a refactor that keeps the token and breaks the gate --
 * the middleware registered but short-circuited, the cap read from the wrong
 * place, the handler responding before the chain runs -- would stay green. So
 * this stands up a real Express app with the same wiring `server.ts` registers,
 * with only the controller stubbed, and asserts the behaviour: with the cap at 1,
 * a second request that arrives while the first is still in flight is refused
 * with 503.
 *
 * The harness follows `authorize_bypass_wiring.integration.spec.ts`, which made
 * the same call for this same middleware.
 */

import { afterEach, describe, expect, it } from "bun:test";
import express from "express";
import request from "supertest";

import type { CompileController } from "./controller/compile.controller";
import type { ConnectionController } from "./controller/connection.controller";
import { internalErrorToHttpError } from "./errors";
import { queryConcurrency } from "./query_concurrency";

/** Restored after each case so a cap set here cannot leak into another file. */
const ORIGINAL_CAP = process.env.PUBLISHER_MAX_CONCURRENT_QUERIES;

afterEach(() => {
   if (ORIGINAL_CAP === undefined) {
      delete process.env.PUBLISHER_MAX_CONCURRENT_QUERIES;
   } else {
      process.env.PUBLISHER_MAX_CONCURRENT_QUERIES = ORIGINAL_CAP;
   }
});

/** A promise plus the handle that settles it, so a request can be held open. */
function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void } {
   let resolve!: (value: T) => void;
   const promise = new Promise<T>((r) => {
      resolve = r;
   });
   return { promise, resolve };
}

/**
 * The error-handling middleware `server.ts` installs. `queryConcurrency()`
 * surfaces its refusal through `next(error)` rather than throwing into the route
 * handler, so without this an app returns Express's default 500 and the 503 the
 * gate actually produced never appears.
 */
function installErrorHandler(app: express.Express): void {
   app.use(
      (
         err: Error,
         _req: express.Request,
         res: express.Response,
         _next: express.NextFunction,
      ) => {
         const { json, status } = internalErrorToHttpError(err);
         res.status(status).json(json);
      },
   );
}

/**
 * The compile route as `server.ts` registers it: the gate, then the handler.
 * `hold` lets a request park inside the controller so a second one arrives while
 * the first still occupies its slot.
 */
function compileApp(hold: Promise<unknown>): express.Express {
   const compileController = {
      compile: async () => {
         await hold;
         return { problems: [] };
      },
   } as unknown as CompileController;

   const app = express();
   app.use(express.json());
   app.post(
      "/api/v0/environments/:environmentName/packages/:packageName/models/*?/compile",
      queryConcurrency(),
      async (req, res) => {
         try {
            res.status(200).json(await compileController.compile());
         } catch (error) {
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );
   installErrorHandler(app);
   return app;
}

/** The sqlSource route as `server.ts` registers it, same shape. */
function sqlSourceApp(hold: Promise<unknown>): express.Express {
   const connectionController = {
      getConnectionSqlSource: async () => {
         await hold;
         return { source: "{}" };
      },
   } as unknown as ConnectionController;

   const app = express();
   app.use(express.json());
   app.post(
      "/api/v0/environments/:environmentName/connections/:connectionName/sqlSource",
      queryConcurrency(),
      async (req, res) => {
         try {
            res.status(200).json(
               await connectionController.getConnectionSqlSource(),
            );
         } catch (error) {
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );
   installErrorHandler(app);
   return app;
}

const COMPILE_PATH =
   "/api/v0/environments/analytics/packages/ecommerce/models/ecommerce.malloy/compile";
const SQL_SOURCE_PATH =
   "/api/v0/environments/analytics/connections/warehouse/sqlSource";

/**
 * Drive one route with the cap at 1: hold the first request inside the
 * controller, send a second, and report what the second got.
 */
async function secondRequestStatusWhileFirstIsInFlight(
   buildApp: (hold: Promise<unknown>) => express.Express,
   path: string,
   body: Record<string, unknown>,
): Promise<{ second: number; first: number }> {
   process.env.PUBLISHER_MAX_CONCURRENT_QUERIES = "1";
   const gate = deferred<void>();
   const app = buildApp(gate.promise);

   // `.end()` starts the request without awaiting it, so the first can sit inside
   // the controller holding its slot while the second is sent.
   const first = new Promise<number>((resolve, reject) => {
      request(app)
         .post(path)
         .send(body)
         .end((err, res) => (err ? reject(err) : resolve(res.status)));
   });

   // Yield until the first request is actually in the handler; without this the
   // second could win the race and pass for the wrong reason.
   await new Promise((r) => setTimeout(r, 100));

   const second = await request(app).post(path).send(body);
   gate.resolve();

   return { second: second.status, first: await first };
}

describe("compile and sqlSource are admission-controlled", () => {
   it("refuses a second concurrent compile with 503 when the cap is 1", async () => {
      const { second, first } = await secondRequestStatusWhileFirstIsInFlight(
         compileApp,
         COMPILE_PATH,
         { source: "run: 1 -> { select: one is 1 }" },
      );
      expect(first, "the held request should still succeed once released").toBe(200);
      expect(
         second,
         "a second compile arriving while the cap is full must be refused, not queued or served",
      ).toBe(503);
   });

   it("refuses a second concurrent sqlSource with 503 when the cap is 1", async () => {
      const { second, first } = await secondRequestStatusWhileFirstIsInFlight(
         sqlSourceApp,
         SQL_SOURCE_PATH,
         { sqlStatement: "SELECT 1" },
      );
      expect(first, "the held request should still succeed once released").toBe(200);
      expect(
         second,
         "a second sqlSource arriving while the cap is full must be refused, not queued or served",
      ).toBe(503);
   });

   it("serves both when the cap is disabled, so the 503 above is the gate and not the harness", async () => {
      // limit === 0 makes the middleware a pass-through. If this also returned 503
      // the cases above would prove nothing about admission control.
      process.env.PUBLISHER_MAX_CONCURRENT_QUERIES = "0";
      const gate = deferred<void>();
      const app = compileApp(gate.promise);

      const first = request(app).post(COMPILE_PATH).send({ source: "run: 1" });
      await new Promise((r) => setTimeout(r, 50));
      const second = request(app).post(COMPILE_PATH).send({ source: "run: 1" });
      gate.resolve();

      expect((await first).status).toBe(200);
      expect((await second).status).toBe(200);
   });
});
