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
import { readFileSync } from "fs";
import { join } from "path";
import express from "express";
import request from "supertest";

import type { CompileController } from "./controller/compile.controller";
import type { ConnectionController } from "./controller/connection.controller";
import { internalErrorToHttpError } from "./errors";
import {
   queryConcurrency,
   resetActiveQueryCountForTesting,
} from "./query_concurrency";

/** Restored after each case so a cap set here cannot leak into another file. */
const ORIGINAL_CAP = process.env.PUBLISHER_MAX_CONCURRENT_QUERIES;

afterEach(() => {
   if (ORIGINAL_CAP === undefined) {
      delete process.env.PUBLISHER_MAX_CONCURRENT_QUERIES;
   } else {
      process.env.PUBLISHER_MAX_CONCURRENT_QUERIES = ORIGINAL_CAP;
   }
   // The slot counter is module-global and this file runs in a process with 170
   // other spec files. A case that leaked a slot would not fail here; it would
   // make a later cap-of-1 case fail on its FIRST request rather than on the
   // admission it is testing, which reads as the gate being broken.
   resetActiveQueryCountForTesting();
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
            // Real arguments, the way server.ts passes them: the stub ignores
            // them, but the cast keeps the call typed against the controller,
            // so a signature change surfaces here rather than at runtime.
            res.status(200).json(
               await compileController.compile(
                  req.params.environmentName,
                  req.params.packageName,
                  (req.params as Record<string, string>)["0"] ?? "",
                  undefined,
               ),
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
               await connectionController.getConnectionSqlSource(
                  req.params.environmentName,
                  req.params.connectionName,
                  "SELECT 1",
               ),
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
      expect(first, "the held request should still succeed once released").toBe(
         200,
      );
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
      expect(first, "the held request should still succeed once released").toBe(
         200,
      );
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

/**
 * The behavioural cases above build their own apps, so they pin the middleware
 * rather than the routes: reverting `queryConcurrency()` out of `server.ts`
 * leaves every one of them green. This block closes that gap by reading the
 * registrations themselves, the way `data_apps_route_parity.spec.ts` pins a
 * route -- one targeted match per route literal rather than the window
 * extraction that made the source-scan spec this replaced a maintenance
 * liability. Line comments ARE stripped from the matched span: a comment
 * naming `queryConcurrency()` in place of the call is the shape someone
 * removing a gate would plausibly leave behind, and it would otherwise satisfy
 * the assertion on its own.
 */
/**
 * Reads a source file with line endings normalised to LF.
 *
 * The assertions below match a verb and a route literal as an adjacent pair, so
 * they are sensitive to what separates them. A Windows checkout has no
 * `.gitattributes` forcing LF here, so the working tree carries CRLF and a
 * pattern written with `\n` finds nothing -- green on the platforms that
 * develop this file and red only on Windows CI.
 */
function readSourceLf(filePath: string): string {
   return readFileSync(filePath, "utf8").replace(/\r\n/gu, "\n");
}

/**
 * Line comments removed, so a comment naming `queryConcurrency()` cannot stand
 * in for the call.
 *
 * Someone removing a gate would plausibly document why, naming the thing they
 * removed -- which is exactly the text these assertions search for, so without
 * this the scan passes on the comment alone. Line comments only: the block form
 * `/\*[\s\S]*?\*\//` is unsound on `server.ts`, where the `/*` inside a route
 * literal such as `public/*` opens a comment that swallows most of the file.
 */
function withoutLineComments(source: string): string {
   return source.replace(/^\s*\/\/.*$/gmu, "");
}

describe("every compile and sqlSource route registers the concurrency gate", () => {
   const gatedRoutes: Array<{
      file: string;
      literal: string;
      /** Anchor for a path registered on more than one verb. */
      verb?: string;
   }> = [
      {
         file: "server.ts",
         literal:
            "${API_PREFIX}/environments/:environmentName/connections/:connectionName/sqlSource",
      },
      {
         file: "server.ts",
         literal:
            "${API_PREFIX}/environments/:environmentName/packages/:packageName/connections/:connectionName/sqlSource",
      },
      {
         file: "server.ts",
         literal:
            "${API_PREFIX}/environments/:environmentName/packages/:packageName/models/*?/compile",
      },
      {
         // The dashboard save: compiles the submitted text, then writes it under
         // the package lock across a full reload. The same path is also
         // registered as a GET, which reads and is correctly ungated, so this
         // entry anchors on the verb.
         file: "server.ts",
         verb: "app.put(",
         literal:
            "${API_PREFIX}/environments/:environmentName/packages/:packageName/models/*?`",
      },
      {
         file: "server-old.ts",
         literal:
            "${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName/sqlSource",
      },
      {
         file: "server-old.ts",
         literal:
            "${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/connections/:connectionName/sqlSource",
      },
      {
         file: "server-old.ts",
         literal:
            "${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/models/:modelName/compile",
      },
   ];

   it("gates the MCP compile tool", () => {
      // The MCP surface takes a slot directly rather than through Express
      // middleware, so it needs its own assertion: the HTTP twin being gated
      // while malloy_compile is not would leave the same flood open one surface
      // over, which is the argument this change makes about the legacy routes.
      const source = readSourceLf(
         join(import.meta.dir, "mcp/tools/compile_tool.ts"),
      );
      expect(
         source,
         "compile_tool.ts acquires no query slot, so MCP compile bypasses the cap",
      ).toContain('tryAcquireQuerySlot("mcp:compile")');
   });

   for (const { file, literal, verb } of gatedRoutes) {
      it(`gates ${verb ?? ""}${literal} in ${file}`, () => {
         const source = readSourceLf(join(import.meta.dir, file));
         // A path registered on more than one verb needs the pair matched, not
         // the verb or the path alone: the first app.put( in the file is a
         // different route, and the path also appears on a GET that is correctly
         // ungated.
         // Assert the ANCHOR resolved, not just that some index came back: a
         // missed verb anchor returns -1, and `indexOf(literal, -1)` restarts at
         // 0 and silently matches a different registration of the same path --
         // reporting the wrong route as ungated rather than reporting a broken
         // matcher.
         const anchorAt = verb
            ? source.indexOf(`${verb}\n   \`${literal}`)
            : source.indexOf(literal);
         expect(
            anchorAt,
            `route literal not found in ${file}; if it was renamed, update this list`,
         ).toBeGreaterThan(-1);
         const start = verb ? source.indexOf(literal, anchorAt) : anchorAt;
         // From the literal to the handler that follows it. The gate is an
         // argument between the two, so a registration that drops it fails here.
         const handlerAt = source.indexOf("async (req, res)", start);
         expect(handlerAt).toBeGreaterThan(start);
         expect(
            withoutLineComments(source.slice(start, handlerAt)),
            `${literal} in ${file} registers no queryConcurrency() before its handler`,
         ).toContain("queryConcurrency()");
      });
   }
});
