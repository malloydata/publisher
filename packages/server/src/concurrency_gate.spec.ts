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
 * This mounts the real route table with only the controllers stubbed and
 * asserts the behaviour: with the cap at 1, a second request that arrives while
 * the first is still in flight is refused with 503. It then pins exactly which
 * routes carry the gate, read from the registered route table.
 */

import { afterEach, describe, expect, it } from "bun:test";
import { readFileSync } from "fs";
import { join } from "path";
import express from "express";
import request from "supertest";

import { resetActiveQueryCountForTesting } from "./query_concurrency";
import { buildRouteApp, registeredRoutes } from "./test_helpers/route_app";
import { installSettings } from "./test_helpers/settings_env";

/**
 * Puts back the settings a case installed, so a cap set here cannot leak into
 * another file.
 */
let restoreSettings: (() => void) | undefined;

/** Install the concurrency cap for this case, before its app is built. */
function installCap(cap: string): void {
   restoreSettings?.();
   restoreSettings = installSettings({
      env: { PUBLISHER_MAX_CONCURRENT_QUERIES: cap },
   }).restore;
}

afterEach(() => {
   restoreSettings?.();
   restoreSettings = undefined;
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

/** The real routes, with the compile controller parked until `hold` settles. */
function compileApp(hold: Promise<unknown>): express.Express {
   return buildRouteApp({
      compileController: {
         compile: async () => {
            await hold;
            return { problems: [] } as never;
         },
      },
   });
}

/** The real routes, with sqlSource parked until `hold` settles. */
function sqlSourceApp(hold: Promise<unknown>): express.Express {
   return buildRouteApp({
      connectionController: {
         getConnectionSqlSource: async () => {
            await hold;
            return { source: "{}" } as never;
         },
      },
   });
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
   installCap("1");
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
      installCap("0");
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
 * Which routes carry the gate, read from the route table itself: every route
 * that compiles, runs SQL, or runs a query, on both the current and the legacy
 * prefix, and nothing else.
 */
describe("the concurrency gate guards exactly the routes that do database work", () => {
   it("gates every query, compile, and SQL route and no other", () => {
      const gated = registeredRoutes(buildRouteApp())
         .filter((route) => route.gated)
         .map((route) => `${route.method} ${route.path}`)
         .sort();
      const env = "/api/v0/environments/:environmentName";
      const pkg = `${env}/packages/:packageName`;
      const project = "/api/v0/projects/:projectName";
      const projectPkg = `${project}/packages/:packageName`;
      const sql = ["sqlSource", "sqlQuery", "sqlTemporaryTable"];
      expect(gated).toEqual(
         [
            ...sql.flatMap((op) => [
               `POST ${env}/connections/:connectionName/${op}`,
               `POST ${pkg}/connections/:connectionName/${op}`,
               `POST ${project}/connections/:connectionName/${op}`,
               `POST ${projectPkg}/connections/:connectionName/${op}`,
            ]),
            // The report save compiles, then writes under the package lock
            // across a full reload. Its GET twin reads and is ungated.
            `PUT ${pkg}/models/*?`,
            `POST ${pkg}/models/*?/query`,
            `POST ${pkg}/models/*?/compile`,
            `GET ${pkg}/notebooks/*/cells/:cellIndex`,
            `GET ${project}/connections/:connectionName/queryData`,
            `GET ${projectPkg}/connections/:connectionName/queryData`,
            `POST ${projectPkg}/models/*?/query`,
            `POST ${projectPkg}/models/:modelName/compile`,
            `GET ${projectPkg}/notebooks/*/cells/:cellIndex`,
         ].sort(),
      );
   });

   it("gates the MCP compile tool", () => {
      // The MCP surface takes a slot directly rather than through Express
      // middleware, so the HTTP twin being gated proves nothing about it.
      const source = readFileSync(
         join(import.meta.dir, "mcp/tools/compile_tool.ts"),
         "utf8",
      );
      expect(
         source,
         "compile_tool.ts acquires no query slot, so MCP compile bypasses the cap",
      ).toContain('tryAcquireQuerySlot("mcp:compile")');
   });
});
