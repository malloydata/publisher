// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { PostgresConnection } from "@malloydata/db-postgres";
import {
   afterAll,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import { swapIntoPlace } from "./database_build_strategy";
import {
   dropLeftoverNameViews,
   provenOwnView,
   replaceOwnViewStatements,
} from "./leftover_name_views";
import { databaseTableSite, type TableSiteContext } from "./table_site";

/**
 * The leftover `name=` view cleanup on a real Postgres, where a view binds to
 * its table and `CREATE OR REPLACE VIEW` keeps the view's comment: a view
 * repointed in place still carries the mark of the table it no longer reads,
 * so only the dependency catalog says what it reads. Opt-in: set
 * PUBLISHER_TEST_POSTGRES_URL to a server the spec may create a schema on.
 */

const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
const SCHEMA = "leftover_views_spec";
const OWNER = "env1/pkg_a";
const OTHER = "env1/pkg_b";
const markOf = (target: string, owner = OWNER) =>
   `malloy-publisher:name-view owner=${encodeURIComponent(owner)} ` +
   `target=${encodeURIComponent(target)}`;
const q = (name: string) => `${SCHEMA}.${name}`;

describe.skipIf(!POSTGRES_URL)(
   "leftover name= views on Postgres (PUBLISHER_TEST_POSTGRES_URL)",
   () => {
      let pg: PostgresConnection;
      let context: TableSiteContext;
      const run = (sql: string) => pg.runSQL(sql);
      const rows = async (sql: string) =>
         (await pg.runSQL(`SELECT row_to_json(t) AS "row" FROM (${sql}) t`))
            .rows as Record<string, unknown>[];
      const views = async () =>
         (
            await rows(
               `SELECT viewname FROM pg_views WHERE schemaname = '${SCHEMA}' ORDER BY 1`,
            )
         ).map((r) => String(r.viewname));
      const view = async (name: string, reads: string, mark?: string) => {
         await run(`CREATE VIEW ${q(name)} AS SELECT * FROM ${q(reads)}`);
         if (mark) await run(`COMMENT ON VIEW ${q(name)} IS '${mark}'`);
      };

      beforeAll(() => {
         pg = new PostgresConnection({
            name: "pg",
            connectionString: POSTGRES_URL,
         });
         context = {
            connectionFor: () => pg,
            destinationFor: () => undefined,
            environmentPath: "/",
         };
      });
      beforeEach(async () => {
         await run(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`);
         await run(`CREATE SCHEMA ${SCHEMA}`);
         await run(`CREATE TABLE ${q("g1")} AS SELECT 1 AS x`);
         await run(`CREATE TABLE ${q("g2")} AS SELECT 2 AS x`);
      });
      afterAll(async () => {
         await run(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`).catch(
            () => undefined,
         );
         await pg.close();
      });

      it("drops with its table only this package's own view the catalog shows still reads it", async () => {
         await view("mine", "g1", markOf(q("g1")));
         await view("theirs", "g1", markOf(q("g1"), OTHER));
         await view("plain", "g1");
         // Repointed to read another table beside ours, by a join or a
         // subquery: it reads ours, but not only ours, so it is not proven.
         await run(`CREATE TABLE ${q("other")} AS SELECT 1 AS x, 9 AS y`);
         await run(
            `CREATE VIEW ${q("joined")} AS SELECT g.x, o.y FROM ${q("g1")} g ` +
               `JOIN ${q("other")} o ON o.x = g.x`,
         );
         await run(`COMMENT ON VIEW ${q("joined")} IS '${markOf(q("g1"))}'`);
         await run(
            `CREATE VIEW ${q("subquery")} AS SELECT * FROM ${q("other")} ` +
               `WHERE x IN (SELECT x FROM ${q("g1")})`,
         );
         await run(`COMMENT ON VIEW ${q("subquery")} IS '${markOf(q("g1"))}'`);
         // Repointed in place: the comment survives CREATE OR REPLACE VIEW.
         await view("repointed", "g1", markOf(q("g1")));
         await run(
            `CREATE OR REPLACE VIEW ${q("repointed")} AS SELECT * FROM ${q("g2")}`,
         );
         expect(
            await rows(
               `SELECT obj_description('${q("repointed")}'::regclass, 'pg_class') AS c`,
            ),
         ).toEqual([{ c: markOf(q("g1")) }]);
         const record = (viewName: string) => ({
            viewName: q(viewName),
            owner: OWNER,
            physicalTableName: q("g1"),
            connectionName: "pg",
         });
         await dropLeftoverNameViews(
            { physicalTableName: q("g1"), connectionName: "pg" },
            ["mine", "theirs", "plain", "repointed", "joined", "subquery"].map(
               record,
            ),
            OWNER,
            context,
         );
         expect(await views()).toEqual([
            "joined",
            "plain",
            "repointed",
            "subquery",
            "theirs",
         ]);
         // With the view gone, the table it read can be dropped.
         await run(`DROP TABLE ${q("g1")} CASCADE`);
         expect(await views()).toEqual(["repointed"]);
      });

      it("replaces this package's own view with the built table in the swap's transaction", async () => {
         await view("named_t", "g1", markOf(q("g1")));
         await run(`CREATE TABLE ${q("named_t_stage")} AS SELECT 5 AS x`);
         const site = databaseTableSite(pg);
         const proven = await provenOwnView(site, q("named_t"), OWNER);
         expect(proven?.target).toBe(q("g1"));
         const statements = replaceOwnViewStatements(proven!, site);
         expect(statements).toHaveLength(2);
         await swapIntoPlace({
            connection: pg,
            runOptions: {},
            dialect: "postgres",
            physicalTableName: q("named_t"),
            stagingTableName: q("named_t_stage"),
            backupTableName: q("named_t_aside"),
            connectionName: "pg",
            replacesView: statements,
         });
         expect(await views()).toEqual([]);
         expect(await rows(`SELECT x FROM ${q("named_t")}`)).toEqual([
            { x: 5 },
         ]);
      });

      it("fails the build with a clear error, and leaves both views, when a reader's own view reads it", async () => {
         await view("named_t", "g1", markOf(q("g1")));
         await view("readers_view", "named_t");
         await run(`CREATE TABLE ${q("named_t_stage")} AS SELECT 5 AS x`);
         const site = databaseTableSite(pg);
         const proven = await provenOwnView(site, q("named_t"), OWNER);
         const failed = await swapIntoPlace({
            connection: pg,
            runOptions: {},
            dialect: "postgres",
            physicalTableName: q("named_t"),
            stagingTableName: q("named_t_stage"),
            backupTableName: q("named_t_aside"),
            connectionName: "pg",
            replacesView: replaceOwnViewStatements(proven!, site),
         }).catch((e: Error) => e);
         expect(String(failed?.message)).toContain(
            "has views of its own over it",
         );
         expect(await views()).toEqual(["named_t", "readers_view"]);
         expect(await rows(`SELECT x FROM ${q("named_t")}`)).toEqual([
            { x: 1 },
         ]);
      });

      it("leaves the view, and fails the build, when it was repointed after the proof", async () => {
         await view("named_t", "g1", markOf(q("g1")));
         await run(`CREATE TABLE ${q("named_t_stage")} AS SELECT 5 AS x`);
         const site = databaseTableSite(pg);
         const proven = await provenOwnView(site, q("named_t"), OWNER);
         await run(
            `CREATE OR REPLACE VIEW ${q("named_t")} AS SELECT * FROM ${q("g2")}`,
         );
         const failed = await swapIntoPlace({
            connection: pg,
            runOptions: {},
            dialect: "postgres",
            physicalTableName: q("named_t"),
            stagingTableName: q("named_t_stage"),
            backupTableName: q("named_t_aside"),
            connectionName: "pg",
            replacesView: replaceOwnViewStatements(proven!, site),
         }).catch((e: Error) => e);
         expect(String(failed?.message)).toContain(
            "changed while the build checked it",
         );
         expect(await views()).toEqual(["named_t"]);
         expect(await rows(`SELECT x FROM ${q("named_t")}`)).toEqual([
            { x: 2 },
         ]);
         // And a repointed view is never proven as this package's.
         expect(await provenOwnView(site, q("named_t"), OWNER)).toBeUndefined();
      });
   },
);
