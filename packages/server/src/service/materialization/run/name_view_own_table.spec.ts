// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBInstance } from "@duckdb/node-api";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { components } from "../../../api";
import type { Materialization } from "../../../storage/DatabaseInterface";
import { storageDestinationRoot } from "../../connection_config";
import { swapIntoPlace } from "./colocated_build_strategy";
import { createTableAndDescribe } from "./materialization_build_session";
import { backupSuffix } from "./materialization_run_support";
import {
   colocatedNameViewSite,
   heldByOwnNameView,
   nameViewKey,
   ownNameTableIn,
   pointNameView,
   preflightNameView,
   replaceOwnTableWithView,
   probeNameView,
   publishNameViews,
   storageNameViewSite,
   viewToTableSwap,
   NameViewTakenError,
   type NameViewContext,
   type NameViewIntent,
   type NameViewRecord,
   type OwnNameTable,
} from "./name_view";
import { nameViewComment, nameViewReplaceTableDdl } from "./name_view_sql";

type ApiConnection = components["schemas"]["Connection"];

/**
 * A `name=` view taking the place of a table this package's own run wrote
 * at the name (a self-instructed build names its table `name=` itself, and a
 * later build names its table by generation), and the other way round: which
 * records prove the table is the package's own, the DDL, and the replacement
 * against a DuckDB file, a DuckLake catalog and a colocated DuckDB
 * connection. The build lifecycle is in name_view_lifecycle.spec.ts.
 */

const OWNER = "env/pkg";
const mark = (target: string, owner = OWNER) =>
   nameViewComment({ target, owner });
const OWN: OwnNameTable = { materializationId: "m1", sourceEntityId: "addr" };

describe("which tables at a name= are the package's own", () => {
   const intent = {
      viewName: "summary",
      sourceName: "summary",
      connectionName: "wh",
   };
   type Run = Parameters<typeof ownNameTableIn>[0][number];
   const run = (
      id: string,
      at: number,
      entry: Record<string, unknown> = {},
      over: Partial<Run> = {},
   ): Run => ({
      id,
      status: "MANIFEST_FILE_READY",
      manifest: {
         entries: {
            addr: {
               sourceEntityId: `addr-${id}`,
               sourceName: "summary",
               physicalTableName: "summary",
               connectionName: "wh",
               ...entry,
            },
         },
      } as never,
      metadata: null,
      completedAt: new Date(at),
      createdAt: new Date(at),
      ...over,
   });
   const viewRun = (id: string, at: number, outcome = "created"): Run =>
      run(
         id,
         at,
         { physicalTableName: `summary_${id}` },
         {
            metadata: {
               nameViews: [
                  {
                     sourceEntityId: "a",
                     sourceName: "summary",
                     owner: OWNER,
                     viewName: "summary",
                     physicalTableName: `summary_${id}`,
                     connectionName: "wh",
                     outcome,
                  } satisfies NameViewRecord,
               ],
            },
         },
      );

   it("the newest committed run that wrote exactly that table, for that source, there", () => {
      expect(ownNameTableIn([run("a", 1), run("b", 2)], intent)).toEqual({
         materializationId: "b",
         sourceEntityId: "addr-b",
      });
   });

   it("never a table the records place anywhere else, or another source's", () => {
      for (const entry of [
         { physicalTableName: "other.summary" },
         { physicalTableName: "SUMMARY" },
         { connectionName: "wh2" },
         { storageDestinationName: "lake" },
         { sourceName: "other" },
         // A failed source in the deprecated shape names a table never built.
         { error: "boom" },
      ]) {
         expect(ownNameTableIn([run("a", 1, entry)], intent)).toBeUndefined();
      }
      expect(
         ownNameTableIn([run("a", 1)], {
            ...intent,
            storageDestinationName: "lake",
         }),
      ).toBeUndefined();
   });

   it("never one a run that did not commit names", () => {
      for (const status of [
         "PENDING",
         "MANIFEST_ROWS_READY",
         "FAILED",
         "CANCELLED",
      ] as Materialization["status"][]) {
         expect(
            ownNameTableIn([run("a", 1, {}, { status })], intent),
         ).toBeUndefined();
      }
   });

   it("not once a later run made a view at the name; again once a later run wrote the table", () => {
      expect(
         ownNameTableIn([run("a", 1), viewRun("g1", 2)], intent),
      ).toBeUndefined();
      expect(
         ownNameTableIn(
            [run("a", 1), viewRun("g1", 2, "replaced_own_table")],
            intent,
         ),
      ).toBeUndefined();
      // A view the run never made does not count.
      expect(
         ownNameTableIn([run("a", 1), viewRun("g1", 2, "name_taken")], intent),
      ).toEqual({ materializationId: "a", sourceEntityId: "addr-a" });
      expect(
         ownNameTableIn([run("a", 1), viewRun("g1", 2), run("c", 3)], intent),
      ).toEqual({ materializationId: "c", sourceEntityId: "addr-c" });
   });
});

describe("the DDL that puts a view in place of a table", () => {
   const ddl = (dialect: string) =>
      nameViewReplaceTableDdl(
         ["mz", "summary"],
         ["mz", "summary_o1"],
         '"mz"."summary_g1"',
         { target: "mz.summary_g1", owner: OWNER },
         dialect,
      );

   it("is one transaction on DuckDB and Postgres", () => {
      expect(ddl("duckdb")).toEqual({
         transactional: true,
         primary: [
            "BEGIN TRANSACTION;",
            'DROP TABLE "mz"."summary";',
            'CREATE VIEW "mz"."summary" AS SELECT * FROM "mz"."summary_g1";',
            `COMMENT ON VIEW "mz"."summary" IS '${mark("mz.summary_g1")}';`,
            "COMMIT;",
         ].join("\n"),
         rollback: "ROLLBACK",
      });
      expect(ddl("postgres")).toEqual({
         transactional: true,
         primary: [
            'DROP TABLE "mz"."summary";',
            'CREATE VIEW "mz"."summary" AS SELECT * FROM "mz"."summary_g1";',
            `COMMENT ON VIEW "mz"."summary" IS '${mark("mz.summary_g1")}';`,
         ].join("\n"),
      });
   });

   it("elsewhere keeps the table aside until the view stands", () => {
      const snowflake = ddl("snowflake");
      expect(snowflake.transactional).toBe(false);
      if (snowflake.transactional) return;
      expect(snowflake.moveAside).toBe(
         'ALTER TABLE "mz"."summary" RENAME TO "mz"."summary_o1"',
      );
      expect(snowflake.create.primary).toContain(
         'CREATE OR REPLACE VIEW "mz"."summary"',
      );
      expect(snowflake.restore).toBe(
         'ALTER TABLE "mz"."summary_o1" RENAME TO "mz"."summary"',
      );
      expect(snowflake.dropAside).toBe(
         'DROP TABLE IF EXISTS "mz"."summary_o1"',
      );
   });

   it("not in a dialect that cannot mark a view", () => {
      expect(() => ddl("mysql")).toThrow(/cannot be marked/);
   });
});

describe("in a plain-DuckDB storage= destination", () => {
   let dir: string;
   const destination = { name: "lake", type: "duckdb" } as ApiConnection;
   let context: NameViewContext;
   const intent: NameViewIntent = {
      sourceEntityId: "addr2",
      sourceName: "summary",
      owner: OWNER,
      viewName: "summary",
      physicalTableName: "summary_g1",
      connectionName: "wh",
      storageDestinationName: "lake",
   };

   async function onLake<T>(
      fn: (
         run: (sql: string) => Promise<Record<string, unknown>[]>,
      ) => Promise<T>,
   ): Promise<T> {
      const root = storageDestinationRoot(dir);
      mkdirSync(root, { recursive: true });
      const instance = await DuckDBInstance.create(join(root, "lake.duckdb"));
      const connection = await instance.connect();
      try {
         return await fn(async (sql) =>
            (await connection.runAndReadAll(sql)).getRowObjectsJson(),
         );
      } finally {
         connection.closeSync();
         instance.closeSync();
      }
   }
   const relations = () =>
      onLake(async (run) =>
         (
            await run(
               "SELECT table_name AS name, 'TABLE' AS kind FROM duckdb_tables() " +
                  "UNION ALL SELECT view_name, 'VIEW' FROM duckdb_views() WHERE NOT internal",
            )
         )
            .map((r) => `${r.kind}:${r.name}`)
            .sort(),
      );
   const answer = () =>
      onLake(async (run) => Number((await run("SELECT x FROM summary"))[0].x));

   beforeEach(async () => {
      dir = mkdtempSync(join(tmpdir(), "name-view-own-"));
      context = {
         connectionFor: () => undefined,
         destinationFor: (name) => (name === "lake" ? destination : undefined),
         environmentPath: dir,
      };
      await onLake(async (run) => {
         // The table a self-instructed build wrote at `name=`, and the
         // generation a later build wrote.
         await run("CREATE TABLE summary AS SELECT 1 AS x");
         await run("CREATE TABLE summary_g1 AS SELECT 2 AS x");
      });
   });
   afterEach(() => rmSync(dir, { recursive: true, force: true }));

   it("preflight: the package's own table goes ahead; anyone else's is refused", async () => {
      expect(
         await preflightNameView(intent, { ...context, ownTableAt: () => OWN }),
      ).toEqual({
         slot: { state: "own_table", table: OWN },
         dialect: "duckdb",
      });
      await expect(
         preflightNameView(intent, {
            ...context,
            ownTableAt: () => undefined,
         }),
      ).rejects.toThrow(NameViewTakenError);
      await expect(preflightNameView(intent, context)).rejects.toThrow(
         "held by a table Publisher did not create",
      );
   });

   it("preflight: never for a view, whatever the records say", async () => {
      await onLake(async (run) => {
         await run("DROP TABLE summary");
         await run("CREATE VIEW summary AS SELECT 7 AS x");
      });
      await expect(
         preflightNameView(intent, { ...context, ownTableAt: () => OWN }),
      ).rejects.toThrow("held by a view Publisher did not create");
      await onLake((run) =>
         run(`COMMENT ON VIEW summary IS '${mark("theirs", "env/other")}'`),
      );
      await expect(
         preflightNameView(intent, { ...context, ownTableAt: () => OWN }),
      ).rejects.toThrow("a view package 'env/other' made");
   });

   it("the view replaces the package's own table and reads the new generation", async () => {
      const records = await publishNameViews(
         [intent],
         context,
         new Map([[nameViewKey(intent), OWN]]),
      );
      expect(records).toEqual([
         expect.objectContaining({
            viewName: "summary",
            physicalTableName: "summary_g1",
            outcome: "replaced_own_table",
         }),
      ]);
      expect(await relations()).toEqual(["TABLE:summary_g1", "VIEW:summary"]);
      expect(await answer()).toBe(2);
      expect(
         await onLake(async (run) =>
            run(
               "SELECT comment FROM duckdb_views() WHERE view_name = 'summary'",
            ),
         ),
      ).toEqual([{ comment: mark("summary_g1") }]);
      // Its name now holds this package's view: a drop of the old table's
      // entry is retired.
      expect(
         await heldByOwnNameView({ ...intent, viewName: "summary" }, context),
      ).toBe(true);
      expect(
         await heldByOwnNameView(
            { ...intent, viewName: "summary", owner: "env/other" },
            context,
         ),
      ).toBe(false);
   });

   it("without the preflight's finding the table is never touched", async () => {
      const records = await publishNameViews([intent], context);
      expect(records[0].outcome).toBe("name_taken");
      expect(await relations()).toEqual(["TABLE:summary", "TABLE:summary_g1"]);
      expect(await answer()).toBe(1);
      expect(await heldByOwnNameView(intent, context)).toBe(false);
   });

   it("a failed replacement leaves the table as it was", async () => {
      const records = await publishNameViews(
         [{ ...intent, physicalTableName: "missing_g9" }],
         context,
         new Map([[nameViewKey(intent), OWN]]),
      );
      expect(records[0].outcome).toBe("failed");
      expect(await relations()).toEqual(["TABLE:summary", "TABLE:summary_g1"]);
      expect(await answer()).toBe(1);
   });
});

describe("in a DuckLake catalog", () => {
   let dir: string;
   let session: DuckDBConnection;
   const lake = { name: "lake", type: "ducklake" } as ApiConnection;

   beforeEach(async () => {
      dir = mkdtempSync(join(tmpdir(), "name-view-own-ducklake-"));
      session = new DuckDBConnection("name_view_own_ducklake", ":memory:", dir);
      await session.runSQL("INSTALL ducklake");
      await session.runSQL("LOAD ducklake");
      await session.runSQL(
         `ATTACH 'ducklake:${join(dir, "catalog.ducklake")}' AS lake ` +
            `(DATA_PATH '${join(dir, "data")}')`,
      );
      await session.runSQL("CREATE TABLE lake.main.summary AS SELECT 1 AS x");
      await session.runSQL(
         "CREATE TABLE lake.main.summary_g1 AS SELECT 2 AS x",
      );
   });
   afterEach(async () => {
      await session.close().catch(() => undefined);
      rmSync(dir, { recursive: true, force: true });
   });

   const rows = async (sql: string) => (await session.runSQL(sql)).rows;

   it("the view replaces the package's own table, and a table replaces the view again", async () => {
      const site = storageNameViewSite("lake", lake, session);
      const intent: NameViewIntent = {
         sourceEntityId: "addr2",
         sourceName: "summary",
         owner: OWNER,
         viewName: "summary",
         physicalTableName: "summary_g1",
         connectionName: "wh",
         storageDestinationName: "lake",
      };
      await replaceOwnTableWithView(site, intent, OWN);
      expect(await probeNameView(site, "summary")).toEqual({
         state: "ours",
         target: "summary_g1",
         owner: OWNER,
      });
      expect(await rows("SELECT x FROM lake.main.summary")).toEqual([{ x: 2 }]);
      expect(
         await rows(
            "SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake' ORDER BY 1",
         ),
      ).toEqual([{ table_name: "summary_g1" }]);

      // The other way: a storage build of a table called `name=` drops the
      // package's view in the transaction that creates its table.
      await createTableAndDescribe(
         session,
         '"lake"."main"."summary"',
         "SELECT 3 AS x",
         [],
         { replacesView: true },
      );
      expect(await probeNameView(site, "summary")).toEqual({
         state: "taken",
         kind: "TABLE",
      });
      expect(await rows("SELECT x FROM lake.main.summary")).toEqual([{ x: 3 }]);
   });

   it("a storage build that fails leaves the view", async () => {
      const site = storageNameViewSite("lake", lake, session);
      await session.runSQL("DROP TABLE lake.main.summary");
      await pointNameView(site, "summary", {
         target: "summary_g1",
         owner: OWNER,
      });
      await expect(
         createTableAndDescribe(
            session,
            '"lake"."main"."summary"',
            "SELECT * FROM lake.main.no_such_table",
            [],
            { replacesView: true },
         ),
      ).rejects.toThrow();
      expect(await probeNameView(site, "summary")).toEqual({
         state: "ours",
         target: "summary_g1",
         owner: OWNER,
      });
      // Partitioned: the same, through the create-empty / insert path.
      await createTableAndDescribe(
         session,
         '"lake"."main"."summary"',
         "SELECT 4 AS x",
         ["x"],
         { replacesView: true },
      );
      expect(await rows("SELECT x FROM lake.main.summary")).toEqual([{ x: 4 }]);
   });
});

describe("on a colocated DuckDB connection", () => {
   let dir: string;
   let connection: DuckDBConnection;
   const intent: NameViewIntent = {
      sourceEntityId: "addr2",
      sourceName: "summary",
      owner: OWNER,
      viewName: "mz.summary",
      physicalTableName: "mz.summary_g1",
      connectionName: "wh",
   };
   const context = (): NameViewContext => ({
      connectionFor: () => connection,
      destinationFor: () => undefined,
      environmentPath: dir,
   });
   const rows = async (sql: string) => (await connection.runSQL(sql)).rows;

   beforeEach(async () => {
      dir = mkdtempSync(join(tmpdir(), "name-view-own-colocated-"));
      connection = new DuckDBConnection(
         "name_view_own_colocated",
         join(dir, "wh.db"),
      );
      await connection.runSQL("CREATE SCHEMA mz");
      await connection.runSQL("CREATE TABLE mz.summary AS SELECT 1 AS x");
      await connection.runSQL("CREATE TABLE mz.summary_g1 AS SELECT 2 AS x");
   });
   afterEach(async () => {
      await connection.close().catch(() => undefined);
      rmSync(dir, { recursive: true, force: true });
   });

   it("the view replaces the table, and a swap puts a table back in the view's place", async () => {
      const records = await publishNameViews(
         [intent],
         context(),
         new Map([[nameViewKey(intent), OWN]]),
      );
      expect(records[0].outcome).toBe("replaced_own_table");
      expect(await rows("SELECT x FROM mz.summary")).toEqual([{ x: 2 }]);

      // Self-instructed again: the swap drops the view in its transaction.
      const site = colocatedNameViewSite(connection);
      const swap = viewToTableSwap(
         { ...intent, physicalTableName: "mz.summary" },
         "mz.summary_g1",
         "duckdb",
      );
      await connection.runSQL("CREATE TABLE mz.summary_s AS SELECT 3 AS x");
      await swapIntoPlace({
         connection,
         runOptions: {},
         dialect: "duckdb",
         physicalTableName: "mz.summary",
         stagingTableName: "mz.summary_s",
         backupTableName: "mz.summary_o",
         connectionName: "wh",
         swapDependents: swap,
      });
      expect(await probeNameView(site, "mz.summary")).toEqual({
         state: "taken",
         kind: "TABLE",
      });
      expect(await rows("SELECT x FROM mz.summary")).toEqual([{ x: 3 }]);
   });

   it("a swap without the view carried fails and leaves the view", async () => {
      const site = colocatedNameViewSite(connection);
      await connection.runSQL("DROP TABLE mz.summary");
      await pointNameView(site, "mz.summary", {
         target: "mz.summary_g1",
         owner: OWNER,
      });
      await connection.runSQL("CREATE TABLE mz.summary_s AS SELECT 3 AS x");
      await expect(
         swapIntoPlace({
            connection,
            runOptions: {},
            dialect: "duckdb",
            physicalTableName: "mz.summary",
            stagingTableName: "mz.summary_s",
            backupTableName: "mz.summary_o",
            connectionName: "wh",
         }),
      ).rejects.toThrow(/View/);
      expect((await probeNameView(site, "mz.summary")).state).toBe("ours");
      expect(await rows("SELECT x FROM mz.summary")).toEqual([{ x: 2 }]);
   });
});

describe("where DDL is not transactional", () => {
   /**
    * A warehouse holding named relations, enough of Snowflake's DDL to move
    * one aside, make a view, and answer the probe.
    */
   function warehouse(failOn?: RegExp) {
      const statements: string[] = [];
      const held = new Map<string, { kind: string; comment?: string }>([
         ["mz.summary", { kind: "BASE TABLE" }],
         ["mz.summary_g1", { kind: "BASE TABLE" }],
      ]);
      const name = (quoted: string) => quoted.replace(/"/g, "");
      const connection = {
         name: "wh",
         dialectName: "snowflake",
         runSQL: async (sql: string) => {
            statements.push(sql);
            if (failOn?.test(sql)) throw new Error(`refused: ${sql}`);
            let m: RegExpMatchArray | null;
            if (sql.startsWith('SELECT TABLE_TYPE AS "kind"')) {
               const table = /TABLE_NAME = '([^']+)'/.exec(sql)![1];
               const held_ = held.get(`mz.${table}`);
               return {
                  rows: held_
                     ? [{ kind: held_.kind, comment: held_.comment ?? null }]
                     : [],
               };
            }
            if ((m = /^ALTER TABLE (\S+) RENAME TO (\S+)$/.exec(sql))) {
               const from = held.get(name(m[1]));
               if (!from) throw new Error("does not exist");
               held.delete(name(m[1]));
               held.set(name(m[2]), from);
            } else if (
               (m = /^CREATE OR REPLACE VIEW (\S+) COMMENT = '([^']+)'/.exec(
                  sql,
               ))
            ) {
               held.set(name(m[1]), { kind: "VIEW", comment: m[2] });
            } else if (
               (m = /^DROP (?:TABLE|VIEW) IF EXISTS (\S+)$/.exec(sql))
            ) {
               held.delete(name(m[1]));
            }
            return { rows: [] };
         },
      };
      return { connection, statements, held };
   }
   const intent: NameViewIntent = {
      sourceEntityId: "addr2",
      sourceName: "summary",
      owner: OWNER,
      viewName: "mz.summary",
      physicalTableName: "mz.summary_g1",
      connectionName: "wh",
   };
   const aside = `mz.summary${backupSuffix(OWN.sourceEntityId)}`;

   it("moves the table aside, makes the view, and drops the old table only then", async () => {
      const { connection, held } = warehouse();
      const records = await publishNameViews(
         [intent],
         {
            connectionFor: () => connection as never,
            destinationFor: () => undefined,
            environmentPath: "",
         },
         new Map([[nameViewKey(intent), OWN]]),
      );
      expect(records[0].outcome).toBe("replaced_own_table");
      expect([...held.keys()].sort()).toEqual(["mz.summary", "mz.summary_g1"]);
      expect(held.get("mz.summary")).toEqual({
         kind: "VIEW",
         comment: mark("mz.summary_g1"),
      });
   });

   it("a view that cannot be made puts the table back", async () => {
      const { connection, held, statements } = warehouse(
         /CREATE OR REPLACE VIEW/,
      );
      const records = await publishNameViews(
         [intent],
         {
            connectionFor: () => connection as never,
            destinationFor: () => undefined,
            environmentPath: "",
         },
         new Map([[nameViewKey(intent), OWN]]),
      );
      expect(records[0].outcome).toBe("failed");
      expect(held.get("mz.summary")).toEqual({ kind: "BASE TABLE" });
      expect(held.has(aside)).toBe(false);
      expect(statements).not.toContain(
         `DROP TABLE IF EXISTS "${aside.replace(".", '"."')}"`,
      );
   });

   it("a table that cannot be moved aside is left as it was", async () => {
      const { connection, held } = warehouse(/RENAME TO/);
      const records = await publishNameViews(
         [intent],
         {
            connectionFor: () => connection as never,
            destinationFor: () => undefined,
            environmentPath: "",
         },
         new Map([[nameViewKey(intent), OWN]]),
      );
      expect(records[0].outcome).toBe("failed");
      expect(held.get("mz.summary")).toEqual({ kind: "BASE TABLE" });
   });

   it("a table's swap over the package's view drops it first, and remakes it if the swap fails", async () => {
      const { connection, held } = warehouse(
         /^ALTER TABLE "mz"."summary_s" RENAME/,
      );
      held.set("mz.summary", {
         kind: "VIEW",
         comment: mark("mz.summary_g1"),
      });
      held.set("mz.summary_s", { kind: "BASE TABLE" });
      const swap = viewToTableSwap(
         { ...intent, physicalTableName: "mz.summary" },
         "mz.summary_g1",
         "snowflake",
      );
      await expect(
         swapIntoPlace({
            connection: connection as never,
            runOptions: {},
            dialect: "snowflake",
            physicalTableName: "mz.summary",
            stagingTableName: "mz.summary_s",
            backupTableName: "mz.summary_o",
            connectionName: "wh",
            swapDependents: swap,
         }),
      ).rejects.toThrow(/refused/);
      expect(held.get("mz.summary")).toEqual({
         kind: "VIEW",
         comment: mark("mz.summary_g1"),
      });

      // And when the swap succeeds, the table holds the name.
      const ok = warehouse();
      ok.held.set("mz.summary", {
         kind: "VIEW",
         comment: mark("mz.summary_g1"),
      });
      ok.held.set("mz.summary_s", { kind: "BASE TABLE" });
      await swapIntoPlace({
         connection: ok.connection as never,
         runOptions: {},
         dialect: "snowflake",
         physicalTableName: "mz.summary",
         stagingTableName: "mz.summary_s",
         backupTableName: "mz.summary_o",
         connectionName: "wh",
         swapDependents: swap,
      });
      expect(ok.held.get("mz.summary")).toEqual({ kind: "BASE TABLE" });
      expect(ok.held.has("mz.summary_s")).toBe(false);
   });
});
