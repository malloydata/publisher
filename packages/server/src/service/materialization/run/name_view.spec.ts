// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBInstance } from "@duckdb/node-api";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { components } from "../../../api";
import { installSettings } from "../../../test_helpers/settings_env";
import { storageDestinationRoot } from "../../connection_config";
import {
   dropStorageTable,
   listStorageTables,
} from "./materialization_build_session";
import {
   colocatedNameViewSite,
   dropNameView,
   nameViewIntentFor,
   nameViewName,
   nameViewSuccessors,
   pointNameView,
   preflightNameView,
   probeNameView,
   publishNameViews,
   restoreNameViews,
   settleNameViewsBeforeDrop,
   storageNameViewSite,
   swapDependentsFor,
   NameViewTakenError,
   type NameViewContext,
   type NameViewIntent,
   type NameViewRecord,
} from "./name_view";
import { nameViewComment } from "./name_view_sql";

type ApiConnection = components["schemas"]["Connection"];

/** The package every view here is made by. */
const OWNER = "env/pkg";
const mark = (target: string, owner = OWNER) =>
   nameViewComment({ target, owner });

/**
 * The `name=` view's mechanics against real engines: a plain-DuckDB `storage=`
 * destination (made, repointed, never listed or dropped as a table by a
 * sweep, and settled before its table's drop), a DuckLake catalog, and a
 * colocated DuckDB connection. The full build lifecycle is in
 * name_view_lifecycle.spec.ts.
 */

describe("which builds call for a name= view", () => {
   let restore: () => void;
   afterEach(() => restore?.());
   const on = (value: string) => {
      restore?.();
      restore = installSettings({
         env: { PUBLISHER_MATERIALIZE: value },
      }).restore;
   };
   const compiled = {
      materializePlans: {
         "flights@m": { persist: { name: "flights_all" } },
         "bare@m": { persist: {} },
      },
   } as never;
   const source = (sourceID: string) => ({
      sourceID,
      name: "flights",
      connectionName: "wh",
   });

   it("a #(materialize) source whose table is named by its generation", () => {
      on("true");
      expect(
         nameViewIntentFor(
            compiled,
            source("flights@m"),
            { physicalTableName: "flights_all_g7", destination: "lake" },
            "addr",
            OWNER,
         ),
      ).toEqual({
         sourceEntityId: "addr",
         sourceName: "flights",
         owner: OWNER,
         viewName: "flights_all",
         physicalTableName: "flights_all_g7",
         connectionName: "wh",
         storageDestinationName: "lake",
      });
   });

   it("not when the table already has the name, nor for a v1 persist source, nor with the switch off", () => {
      on("true");
      expect(
         nameViewIntentFor(
            compiled,
            source("flights@m"),
            { physicalTableName: "flights_all" },
            "addr",
            OWNER,
         ),
      ).toBeUndefined();
      expect(
         nameViewIntentFor(
            compiled,
            source("v1@m"),
            { physicalTableName: "flights_all_g7" },
            "addr",
            OWNER,
         ),
      ).toBeUndefined();
      expect(
         nameViewIntentFor(
            compiled,
            source("bare@m"),
            { physicalTableName: "flights_all_g7" },
            "addr",
            OWNER,
         ),
      ).toBeUndefined();
      on("false");
      expect(
         nameViewIntentFor(
            compiled,
            source("flights@m"),
            { physicalTableName: "flights_all_g7" },
            "addr",
            OWNER,
         ),
      ).toBeUndefined();
   });

   it("puts a bare name= beside its table, and keeps a qualified one as written", () => {
      expect(nameViewName("flights_all", "mz.flights_all_g7")).toBe(
         "mz.flights_all",
      );
      expect(nameViewName("flights_all", "p.mz.flights_all_g7")).toBe(
         "p.mz.flights_all",
      );
      expect(nameViewName("analytics.flights_all", "mz.flights_all_g7")).toBe(
         "analytics.flights_all",
      );
      expect(nameViewName("flights_all", "flights_all_g7")).toBe("flights_all");
   });
});

describe("a Postgres swap of the table the view reads", () => {
   const intent: NameViewIntent = {
      sourceEntityId: "a",
      sourceName: "flights",
      owner: OWNER,
      viewName: "mz.flights_all",
      physicalTableName: "mz.flights_all_v1",
      connectionName: "pg",
   };

   it("repoints the view at staging first, and drops and remakes it only when that is refused", () => {
      const comment = `COMMENT ON VIEW "mz"."flights_all" IS '${mark("mz.flights_all_v1")}'`;
      expect(
         swapDependentsFor(
            intent,
            { state: "ours", target: "mz.flights_all_v1" },
            "postgres",
         )?.attempts("mz.flights_all_v1_s"),
      ).toEqual([
         {
            before: [
               `CREATE OR REPLACE VIEW "mz"."flights_all" AS SELECT * FROM "mz"."flights_all_v1_s"`,
               comment,
            ],
            after: [],
         },
         {
            before: [`DROP VIEW "mz"."flights_all"`],
            after: [
               `CREATE VIEW "mz"."flights_all" AS SELECT * FROM "mz"."flights_all_v1"`,
               comment,
            ],
         },
      ]);
   });

   it("not when the view reads another table, is absent, or the dialect does not bind views", () => {
      expect(
         swapDependentsFor(
            intent,
            { state: "ours", target: "mz.other" },
            "postgres",
         ),
      ).toBeUndefined();
      expect(
         swapDependentsFor(intent, { state: "empty" }, "postgres"),
      ).toBeUndefined();
      expect(
         swapDependentsFor(
            intent,
            { state: "ours", target: "mz.flights_all_v1" },
            "duckdb",
         ),
      ).toBeUndefined();
   });
});

describe("where a view could point instead", () => {
   const record = (
      table: string,
      viewName = "v",
      outcome = "created",
      owner = OWNER,
   ): NameViewRecord => ({
      sourceEntityId: "a",
      sourceName: "s",
      owner,
      viewName,
      physicalTableName: table,
      connectionName: "wh",
      outcome,
   });
   const run = (table: string, at: number, recorded = [record(table)]) => ({
      metadata: { nameViews: recorded },
      manifest: {
         entries: {
            a: { sourceEntityId: "a", physicalTableName: table },
         } as never,
      },
      completedAt: new Date(at),
   });

   it("the newest other run's table for the same view", () => {
      const successorFor = nameViewSuccessors([
         run("t1", 1),
         run("t3", 3),
         run("t2", 2),
      ]);
      expect(successorFor(record("t3"))?.physicalTableName).toBe("t2");
      expect(successorFor(record("t9"))?.physicalTableName).toBe("t3");
   });

   it("never another view's, nor a table the run no longer names", () => {
      const successorFor = nameViewSuccessors([
         run("t1", 1, [record("t1", "other")]),
         {
            ...run("t2", 2),
            manifest: { entries: {} },
         },
      ]);
      expect(successorFor(record("t3"))).toBeUndefined();
   });

   it("never a record whose view was not made, nor another package's", () => {
      const successorFor = nameViewSuccessors([
         run("t1", 1),
         run("t2", 2, [record("t2", "v", "conflict")]),
         run("t3", 3, [record("t3", "v", "name_taken")]),
         run("t4", 4, [record("t4", "v", "created", "env/other")]),
      ]);
      expect(successorFor(record("t9"))?.physicalTableName).toBe("t1");
   });
});

describe("in a plain-DuckDB storage= destination", () => {
   let dir: string;
   const destination = { name: "lake", type: "duckdb" } as ApiConnection;
   let context: NameViewContext;

   /** Write the destination file directly, closing both handles at once. */
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

   const intent = (table: string): NameViewIntent => ({
      sourceEntityId: "addr",
      sourceName: "flights",
      owner: OWNER,
      viewName: "flights_all",
      physicalTableName: table,
      connectionName: "wh",
      storageDestinationName: "lake",
   });
   const recordFor = (table: string): NameViewRecord => ({
      ...intent(table),
      outcome: "created",
   });

   /** The view's answer and mark, or undefined when there is none. */
   const view = () =>
      onLake(async (run) => {
         const rows = await run(
            "SELECT comment FROM duckdb_views() WHERE view_name = 'flights_all'",
         );
         if (rows.length === 0) return undefined;
         const [answer] = await run("SELECT x FROM flights_all");
         return { x: Number(answer.x), comment: rows[0].comment };
      });

   beforeEach(async () => {
      dir = mkdtempSync(join(tmpdir(), "name-view-lake-"));
      context = {
         connectionFor: () => undefined,
         destinationFor: (name) => (name === "lake" ? destination : undefined),
         environmentPath: dir,
      };
      await onLake(async (run) => {
         await run("CREATE TABLE flights_all_g1 AS SELECT 1 AS x");
         await run("CREATE TABLE flights_all_g2 AS SELECT 2 AS x, 'b' AS y");
      });
   });
   afterEach(() => rmSync(dir, { recursive: true, force: true }));

   it("is made, then repointed to a table of another shape", async () => {
      expect(
         await publishNameViews([intent("flights_all_g1")], context),
      ).toEqual([{ ...intent("flights_all_g1"), outcome: "created" }]);
      expect(await view()).toEqual({
         x: 1,
         comment: mark("flights_all_g1"),
      });
      expect(
         (await publishNameViews([intent("flights_all_g2")], context))[0]
            .outcome,
      ).toBe("repointed");
      expect(await view()).toEqual({
         x: 2,
         comment: mark("flights_all_g2"),
      });
   });

   it("another package's view at the name is refused and never touched", async () => {
      await publishNameViews(
         [{ ...intent("flights_all_g1"), owner: "env/other" }],
         context,
      );
      await expect(
         preflightNameView(intent("flights_all_g2"), context),
      ).rejects.toThrow(/package 'env\/other'/);
      expect(
         (await publishNameViews([intent("flights_all_g2")], context))[0]
            .outcome,
      ).toBe("name_taken");
      await settleNameViewsBeforeDrop(
         { ...intent("flights_all_g1") },
         [recordFor("flights_all_g1")],
         () => undefined,
         context,
      );
      expect(await view()).toEqual({
         x: 1,
         comment: mark("flights_all_g1", "env/other"),
      });
   });

   it("a run that does not commit puts each view back: repointed back, or dropped", async () => {
      await publishNameViews([intent("flights_all_g1")], context);
      const repointed = await publishNameViews(
         [intent("flights_all_g2")],
         context,
      );
      expect(repointed[0]).toMatchObject({
         outcome: "repointed",
         previousTarget: "flights_all_g1",
      });
      await restoreNameViews(repointed, context);
      expect(await view()).toEqual({ x: 1, comment: mark("flights_all_g1") });

      await onLake((run) => run("DROP VIEW flights_all"));
      const created = await publishNameViews(
         [intent("flights_all_g2")],
         context,
      );
      await restoreNameViews(created, context);
      expect(await view()).toBeUndefined();
   });

   it("is never listed as a table, and a sweep's table drop never removes it", async () => {
      await publishNameViews([intent("flights_all_g1")], context);
      expect(
         await listStorageTables({
            destinationName: "lake",
            destinationConnection: destination,
            schemaName: "main",
            environmentPath: dir,
         }),
      ).toEqual(["flights_all_g1", "flights_all_g2"]);
      await expect(
         dropStorageTable({
            destinationName: "lake",
            destinationConnection: destination,
            physicalTableName: "flights_all",
            environmentPath: dir,
         }),
      ).rejects.toThrow(/View/);
      expect((await view())?.x).toBe(1);
   });

   it("before its table's drop: repointed to a standing generation, or dropped with the last", async () => {
      await publishNameViews([intent("flights_all_g2")], context);
      await settleNameViewsBeforeDrop(
         { ...intent("flights_all_g2") },
         [recordFor("flights_all_g2")],
         () => recordFor("flights_all_g1"),
         context,
      );
      expect((await view())?.comment).toBe(mark("flights_all_g1"));
      await settleNameViewsBeforeDrop(
         { ...intent("flights_all_g1") },
         [recordFor("flights_all_g1")],
         () => undefined,
         context,
      );
      expect(await view()).toBeUndefined();
      // Tables are the dropper's: settling never touches them.
      expect(
         await onLake((run) =>
            run("SELECT table_name FROM duckdb_tables() ORDER BY 1"),
         ),
      ).toEqual([
         { table_name: "flights_all_g1" },
         { table_name: "flights_all_g2" },
      ]);
   });

   it("before a drop of a table the view no longer reads: left alone", async () => {
      await publishNameViews([intent("flights_all_g2")], context);
      await settleNameViewsBeforeDrop(
         { ...intent("flights_all_g1") },
         [recordFor("flights_all_g1")],
         () => undefined,
         context,
      );
      expect((await view())?.comment).toBe(mark("flights_all_g2"));
   });

   it("a name held by a table or an unmarked view is refused, reported, and never touched", async () => {
      await onLake((run) => run("CREATE VIEW flights_all AS SELECT 99 AS x"));
      await expect(
         preflightNameView(intent("flights_all_g1"), context),
      ).rejects.toBeInstanceOf(NameViewTakenError);
      expect(
         (await publishNameViews([intent("flights_all_g1")], context))[0]
            .outcome,
      ).toBe("name_taken");
      await settleNameViewsBeforeDrop(
         { ...intent("flights_all_g1") },
         [recordFor("flights_all_g1")],
         () => undefined,
         context,
      );
      expect((await view())?.x).toBe(99);
   });

   it("two sources asking for one view for different tables get neither", async () => {
      const records = await publishNameViews(
         [intent("flights_all_g1"), intent("flights_all_g2")],
         context,
      );
      expect(records.map((r) => r.outcome)).toEqual(["conflict", "conflict"]);
      expect(await view()).toBeUndefined();
   });

   it("an unreachable destination fails the view, never the run", async () => {
      const records = await publishNameViews(
         [{ ...intent("flights_all_g1"), storageDestinationName: "gone" }],
         context,
      );
      expect(records[0].outcome).toBe("failed");
   });
});

describe("in a DuckLake catalog", () => {
   let dir: string;
   let session: DuckDBConnection;
   const lake = { name: "lake", type: "ducklake" } as ApiConnection;

   async function attach(alias: string): Promise<void> {
      await session.runSQL(
         `ATTACH 'ducklake:${join(dir, "catalog.ducklake")}' AS ${alias} ` +
            `(DATA_PATH '${join(dir, "data")}')`,
      );
   }

   beforeEach(async () => {
      dir = mkdtempSync(join(tmpdir(), "name-view-ducklake-"));
      session = new DuckDBConnection("name_view_ducklake", ":memory:", dir);
      await session.runSQL("INSTALL ducklake");
      await session.runSQL("LOAD ducklake");
      await attach("lake");
      await session.runSQL(
         "CREATE TABLE lake.main.flights_all_g1 AS SELECT 1 AS x",
      );
      await session.runSQL(
         "CREATE TABLE lake.main.flights_all_g2 AS SELECT 2 AS x",
      );
   });
   afterEach(async () => {
      await session.close().catch(() => undefined);
      rmSync(dir, { recursive: true, force: true });
   });

   it("is made, repointed, read under another attach name, and dropped", async () => {
      const site = storageNameViewSite("lake", lake, session);
      expect(await probeNameView(site, "flights_all")).toEqual({
         state: "empty",
      });
      await pointNameView(site, "flights_all", {
         target: "flights_all_g1",
         owner: OWNER,
      });
      expect(await probeNameView(site, "flights_all")).toEqual({
         state: "ours",
         target: "flights_all_g1",
         owner: OWNER,
      });
      await pointNameView(site, "flights_all", {
         target: "flights_all_g2",
         owner: OWNER,
      });
      expect(await probeNameView(site, "flights_all")).toEqual({
         state: "ours",
         target: "flights_all_g2",
         owner: OWNER,
      });

      // A reader attaching the catalog under its own name still resolves it.
      await session.runSQL("DETACH lake");
      await attach("theirs");
      expect(
         (await session.runSQL("SELECT x FROM theirs.main.flights_all")).rows,
      ).toEqual([{ x: 2 }]);
      await session.runSQL("DETACH theirs");
      await attach("lake");

      await dropNameView(site, "flights_all");
      expect(await probeNameView(site, "flights_all")).toEqual({
         state: "empty",
      });
   });
});

describe("on a colocated DuckDB connection", () => {
   let dir: string;
   let connection: DuckDBConnection;
   beforeEach(async () => {
      dir = mkdtempSync(join(tmpdir(), "name-view-colocated-"));
      connection = new DuckDBConnection(
         "name_view_colocated",
         join(dir, "wh.db"),
      );
      await connection.runSQL("CREATE SCHEMA mz");
      await connection.runSQL("CREATE TABLE mz.t_g1 AS SELECT 1 AS x");
   });
   afterEach(async () => {
      await connection.close().catch(() => undefined);
      rmSync(dir, { recursive: true, force: true });
   });

   it("lives beside its table in the table's schema, and reads it", async () => {
      const site = colocatedNameViewSite(connection);
      const viewName = nameViewName("flights_all", "mz.t_g1");
      await pointNameView(site, viewName, { target: "mz.t_g1", owner: OWNER });
      expect(await probeNameView(site, viewName)).toEqual({
         state: "ours",
         target: "mz.t_g1",
         owner: OWNER,
      });
      expect(
         (await connection.runSQL("SELECT x FROM mz.flights_all")).rows,
      ).toEqual([{ x: 1 }]);
      // A table under the name is someone else's.
      expect(await probeNameView(site, "mz.t_g1")).toEqual({
         state: "taken",
         kind: "TABLE",
      });
   });
});

describe("a view whose mark does not read back", () => {
   it("is dropped again and reported, so the next build does not find its own view taken", async () => {
      const statements: string[] = [];
      let made = false;
      // A warehouse that keeps the view but loses its comment.
      const connection = {
         name: "wh",
         dialectName: "duckdb",
         runSQL: async (sql: string) => {
            statements.push(sql);
            if (sql.startsWith("SELECT 'VIEW'")) {
               return { rows: made ? [{ kind: "VIEW", comment: null }] : [] };
            }
            if (sql.includes("CREATE OR REPLACE VIEW")) made = true;
            if (sql.startsWith("DROP VIEW")) made = false;
            return { rows: [] };
         },
      };
      const records = await publishNameViews(
         [
            {
               sourceEntityId: "addr",
               sourceName: "flights",
               owner: OWNER,
               viewName: "mz.flights_all",
               physicalTableName: "mz.flights_all_g1",
               connectionName: "wh",
            },
         ],
         {
            connectionFor: () => connection as never,
            destinationFor: () => undefined,
            environmentPath: "",
         },
      );
      expect(records[0].outcome).toBe("failed");
      expect(records[0].message).toMatch(/did not read back/);
      expect(made).toBe(false);
      expect(statements.at(-1)).toBe(`DROP VIEW IF EXISTS "mz"."flights_all"`);
   });
});
