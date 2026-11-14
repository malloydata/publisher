// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { PostgresConnection } from "@malloydata/db-postgres";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
   colocatedNameViewSite,
   nameViewKey,
   pointNameView,
   probeNameView,
   publishNameViews,
   type NameViewContext,
   type NameViewIntent,
} from "./name_view";
import { backupSuffix } from "./materialization_run_support";
import { nameViewComment } from "./name_view_sql";

/**
 * After Publisher writes or repoints a `name=` view it reads the view's mark
 * back. Whatever that read-back finds other than this package's view of this
 * run's table (another package's view, an author's unmarked view, nothing),
 * or when it cannot be read at all, the name is left as it is: nothing is
 * dropped that a fresh probe has not just shown is this package's.
 *
 * The race is injected deterministically: the site's connection is wrapped so
 * that, right after the statement that writes the view and before the
 * read-back, another writer's statements run on the same database.
 *
 * DuckDB always; Postgres when PUBLISHER_TEST_POSTGRES_URL names a database
 * the test may create and drop the `nv_read_back` schema in.
 */

const OWNER = "env/pkg";
const OTHER = "env/other";
const pgUrl = process.env.PUBLISHER_TEST_POSTGRES_URL;

type Conn = Pick<MalloyConnection, "dialectName" | "runSQL">;

/**
 * `conn`, but once a statement matching `after` has run, `act` runs before
 * the next statement (or, given `fail`, the next statement throws instead of
 * running). Statements are recorded.
 */
function seam(
   conn: Conn,
   after: RegExp,
   hook: { act?: () => Promise<void>; fail?: boolean } = {},
) {
   const statements: string[] = [];
   let armed = false;
   const wrapped = {
      name: "wh",
      dialectName: conn.dialectName,
      runSQL: async (sql: string, options?: unknown) => {
         if (armed) {
            armed = false;
            if (hook.fail) {
               statements.push(`(refused) ${sql}`);
               throw new Error("the warehouse went away");
            }
            await hook.act?.();
         }
         statements.push(sql);
         const result = await conn.runSQL(sql, options as never);
         if (after.test(sql)) armed = true;
         return result;
      },
   };
   return { wrapped, statements };
}

interface Engine {
   name: string;
   dialect: string;
   skip: boolean;
   open(): Promise<{ conn: Conn; close(): Promise<void> }>;
}

const engines: Engine[] = [
   {
      name: "DuckDB",
      dialect: "duckdb",
      skip: false,
      async open() {
         const dir = mkdtempSync(join(tmpdir(), "name-view-read-back-"));
         const conn = new DuckDBConnection("wh", join(dir, "wh.db"));
         await conn.runSQL("CREATE SCHEMA nv_read_back");
         return {
            conn,
            async close() {
               await conn.close().catch(() => undefined);
               rmSync(dir, { recursive: true, force: true });
            },
         };
      },
   },
   {
      name: "Postgres",
      dialect: "postgres",
      skip: !pgUrl,
      async open() {
         const conn = new PostgresConnection({
            name: "wh",
            connectionString: pgUrl!,
         });
         await conn.runSQL("DROP SCHEMA IF EXISTS nv_read_back CASCADE");
         await conn.runSQL("CREATE SCHEMA nv_read_back");
         return {
            conn,
            async close() {
               await conn.runSQL("DROP SCHEMA IF EXISTS nv_read_back CASCADE");
               await conn.close();
            },
         };
      },
   },
];

const VIEW = "nv_read_back.flights_all";
const G1 = "nv_read_back.flights_g1";
const G2 = "nv_read_back.flights_g2";

const intentFor = (physicalTableName: string): NameViewIntent => ({
   sourceEntityId: "addr",
   sourceName: "flights",
   owner: OWNER,
   viewName: VIEW,
   physicalTableName,
   connectionName: "wh",
});

const contextFor = (wrapped: unknown): NameViewContext => ({
   connectionFor: () => wrapped as never,
   destinationFor: () => undefined,
   environmentPath: "",
});

/** A string literal, the single quote doubled (the mark carries none). */
const lit = (value: string) => `'${value.replace(/'/g, "''")}'`;

for (const engine of engines) {
   describe.skipIf(engine.skip)(
      `a name= view's read-back on ${engine.name}`,
      () => {
         let conn: Conn;
         let close: () => Promise<void>;
         const run = (sql: string) => conn.runSQL(sql);
         const probe = () => probeNameView(colocatedNameViewSite(conn), VIEW);
         /** Column `x` of a relation (Malloy's Postgres wraps each row). */
         const xs = async (relation: string) => {
            const sql =
               engine.dialect === "postgres"
                  ? `SELECT row_to_json(r) AS "row" FROM (SELECT x FROM ${relation}) r`
                  : `SELECT x FROM ${relation}`;
            const { rows } = await conn.runSQL(sql);
            return rows.map((r) => r.x);
         };
         /** Another writer's view at the name, marked (`owner`) or not. */
         const replaceView = async (owner?: string) => {
            await run(`DROP VIEW IF EXISTS ${VIEW}`);
            await run(`CREATE VIEW ${VIEW} AS SELECT 42 AS x`);
            if (owner) {
               await run(
                  `COMMENT ON VIEW ${VIEW} IS ${lit(
                     nameViewComment({ target: "elsewhere.t", owner }),
                  )}`,
               );
            }
         };

         beforeEach(async () => {
            ({ conn, close } = await engine.open());
            await run(`CREATE TABLE ${G1} AS SELECT 1 AS x`);
            await run(`CREATE TABLE ${G2} AS SELECT 2 AS x`);
         });
         afterEach(async () => {
            await close();
         });

         it("the happy path: created, then repointed, and reads back as ours", async () => {
            const created = await publishNameViews(
               [intentFor(G1)],
               contextFor(conn),
            );
            expect(created.map((r) => r.outcome)).toEqual(["created"]);
            const repointed = await publishNameViews(
               [intentFor(G2)],
               contextFor(conn),
            );
            expect(repointed.map((r) => r.outcome)).toEqual(["repointed"]);
            expect(repointed[0].previousTarget).toBe(G1);
            expect(await probe()).toEqual({
               state: "ours",
               target: G2,
               owner: OWNER,
            });
         });

         it("another package's view that replaces the new view before its read-back is never dropped", async () => {
            const { wrapped, statements } = seam(
               conn,
               /CREATE OR REPLACE VIEW/,
               {
                  act: () => replaceView(OTHER),
               },
            );
            const records = await publishNameViews(
               [intentFor(G1)],
               contextFor(wrapped),
            );
            expect(records[0].outcome).toBe("failed");
            expect(records[0].message).toMatch(/did not read back/);
            expect(records[0].message).toContain(`'${OTHER}'`);
            expect(await probe()).toEqual({
               state: "ours",
               target: "elsewhere.t",
               owner: OTHER,
            });
            expect(statements.some((s) => /DROP VIEW/.test(s))).toBe(false);
         });

         it("another package's view that replaces a repointed view before its read-back is never dropped", async () => {
            await publishNameViews([intentFor(G1)], contextFor(conn));
            const { wrapped, statements } = seam(
               conn,
               /CREATE OR REPLACE VIEW/,
               {
                  act: () => replaceView(OTHER),
               },
            );
            const records = await publishNameViews(
               [intentFor(G2)],
               contextFor(wrapped),
            );
            expect(records[0].outcome).toBe("failed");
            expect((await probe()).state).toBe("ours");
            expect(await probe()).toMatchObject({ owner: OTHER });
            expect(statements.some((s) => /DROP VIEW/.test(s))).toBe(false);
         });

         it("an author's unmarked view that replaces the new view before its read-back is left alone", async () => {
            const { wrapped } = seam(conn, /CREATE OR REPLACE VIEW/, {
               act: () => replaceView(),
            });
            const records = await publishNameViews(
               [intentFor(G1)],
               contextFor(wrapped),
            );
            expect(records[0].outcome).toBe("failed");
            expect(records[0].message).toMatch(/Publisher did not create/);
            expect(await probe()).toEqual({ state: "taken", kind: "VIEW" });
            expect(await xs(VIEW)).toEqual([42]);
         });

         it("a read-back that throws drops nothing and records a failure", async () => {
            const { wrapped, statements } = seam(
               conn,
               /CREATE OR REPLACE VIEW/,
               {
                  fail: true,
               },
            );
            const records = await publishNameViews(
               [intentFor(G1)],
               contextFor(wrapped),
            );
            expect(records[0].outcome).toBe("failed");
            expect(records[0].message).toMatch(/could not be read back/);
            // Names only: neither the probe's SQL nor the driver's error.
            expect(records[0].message).not.toMatch(/SELECT|went away/);
            expect(
               statements.filter((s) => !s.startsWith("(refused)")),
            ).not.toContainEqual(expect.stringMatching(/DROP VIEW/));
            // The view the call wrote is still there, as it was written.
            expect(await probe()).toEqual({
               state: "ours",
               target: G1,
               owner: OWNER,
            });
         });

         it("a table this package's run wrote at the name is not put back over a view that took the name since", async () => {
            // The package's own table at the name, which the view replaces.
            await run(`CREATE TABLE ${VIEW} AS SELECT 0 AS x`);
            const own = { materializationId: "m1", sourceEntityId: "old" };
            const { wrapped, statements } = seam(conn, /RENAME TO/, {
               act: () => replaceView(OTHER),
            });
            const records = await publishNameViews(
               [intentFor(G1)],
               contextFor(wrapped),
               new Map([[nameViewKey(intentFor(G1)), own]]),
            );
            expect(records[0].outcome).toBe("failed");
            expect(records[0].message).toMatch(/did not read back/);
            expect(await probe()).toMatchObject({ owner: OTHER });
            expect(statements.some((s) => /DROP VIEW/.test(s))).toBe(false);
            // The replaced table stays under its aside name, intact.
            const aside = `${VIEW}${backupSuffix(own.sourceEntityId)}`;
            expect(await xs(aside)).toEqual([0]);
         });

         if (engine.dialect === "postgres") {
            it("a repoint that must drop and remake the view never drops another package's", async () => {
               const site = colocatedNameViewSite(conn);
               // This package's view of G1 was probed; another package's view,
               // of a shape CREATE OR REPLACE refuses, holds the name since.
               await run(`DROP TABLE ${G2}`);
               await run(`CREATE TABLE ${G2} AS SELECT 'two' AS x, 2 AS y`);
               await run(`CREATE VIEW ${VIEW} AS SELECT 42 AS x, 'z' AS z`);
               await run(
                  `COMMENT ON VIEW ${VIEW} IS ${lit(
                     nameViewComment({ target: "elsewhere.t", owner: OTHER }),
                  )}`,
               );
               await expect(
                  pointNameView(
                     site,
                     VIEW,
                     { target: G2, owner: OWNER },
                     { target: G1, owner: OWNER },
                  ),
               ).rejects.toThrow(/not as this package left it/);
               expect(await probe()).toMatchObject({ owner: OTHER });
            });
         }
      },
   );
}
