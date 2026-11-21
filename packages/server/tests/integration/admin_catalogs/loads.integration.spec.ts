// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The admin `loads` endpoint (CatalogAdmin.loadTable) against a real Postgres
 * catalog and Parquet objects on a local data path: what a keyed load writes,
 * what it leaves alone, and what it refuses. Same environment as the admin
 * integration spec; skipped without it.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import {
   copyFileSync,
   mkdirSync,
   mkdtempSync,
   rmSync,
   writeFileSync,
} from "fs";
import { tmpdir } from "os";
import { dirname, join } from "path";
import { logger } from "../../../src/logger";
import { loadSlots } from "../../../src/query_concurrency";
import {
   CatalogAdmin,
   CatalogAdminError,
} from "../../../src/service/catalog_admin";
import {
   attachDuckLakeReadOnly,
   attachDuckLakeReadWrite,
} from "../../../src/service/connection";
import { createIsolatedBuildSession } from "../../../src/service/materialization/run/materialization_build_session";
import { mergeStatements } from "../../../src/service/materialization/run/storage_table_load";
import { pollFor } from "../../../src/test_helpers/poll";
import { SUPERUSER, superuser } from "./superuser";

const DB = `loads_it_${process.pid}`;
const CATALOG = "lake";

async function refusal(p: Promise<unknown>): Promise<CatalogAdminError> {
   const error = await p.then(
      () => undefined,
      (e) => e,
   );
   expect(error).toBeInstanceOf(CatalogAdminError);
   return error as CatalogAdminError;
}

/** A type the load must round-trip without rewriting its rows on a retry. */
const TYPES: [string, string][] = [
   ["bigint_beyond_2_53", "9007199254740993::BIGINT + i"],
   ["integer", "i::INTEGER"],
   ["smallint", "i::SMALLINT"],
   ["utinyint", "i::UTINYINT"],
   ["ubigint", "18446744073709551612::UBIGINT + i::UBIGINT"],
   ["float", "i::FLOAT / 3"],
   ["double", "i::DOUBLE / 3"],
   ["nan", "CASE WHEN i = 1 THEN 'NaN'::DOUBLE ELSE i::DOUBLE END"],
   ["decimal", "(i / 7.0)::DECIMAL(18, 3)"],
   ["varchar", "'é' || i"],
   ["boolean", "i % 2 = 0"],
   ["date", "DATE '2026-01-01' + i::INTEGER"],
   ["timestamp", "TIMESTAMP '2026-01-01 00:00:00.123456' + to_hours(i)"],
   ["timestamptz", "TIMESTAMPTZ '2026-01-01 00:00:00+00' + to_hours(i)"],
   ["time", "TIME '01:02:03.456' + to_minutes(i)"],
   ["blob", "('\\x01\\xAB' || i)::BLOB"],
   ["uuid", "uuid()"],
   ["list", "[i, i + 1]"],
   ["struct", "{'a': i, 'b': 'x' || i}"],
   ["map", "MAP {'k': i}"],
   ["nullable", "CASE WHEN i % 2 = 0 THEN NULL ELSE i END"],
];

describe.skipIf(!SUPERUSER)("loads", () => {
   const root = mkdtempSync(join(tmpdir(), "loads-it-"));
   const dataPath = join(root, "data");
   const conninfo = `${SUPERUSER} dbname=${DB}`;
   const disposers: (() => Promise<void>)[] = [];
   let admin: CatalogAdmin;
   let fixtures: DuckDBConnection;
   let reader: DuckDBConnection;
   let writer: DuckDBConnection;
   let counter = 0;

   /** Writes a Parquet object under `base` and returns its URI. */
   async function parquet(
      select: string,
      base = dataPath,
      name = `o${counter++}`,
   ): Promise<string> {
      const file = join(base, "staging", `${name}.parquet`);
      mkdirSync(dirname(file), { recursive: true });
      await fixtures.runSQL(`COPY (${select}) TO '${file}' (FORMAT parquet)`);
      return file;
   }

   /** Copies a fixture Parquet object under the data path and returns its URI. */
   function fixture(name: string): string {
      const file = join(dataPath, "staging", `f${counter++}_${name}`);
      mkdirSync(dirname(file), { recursive: true });
      copyFileSync(join(__dirname, "fixtures", name), file);
      return file;
   }

   async function sql(
      session: DuckDBConnection,
      text: string,
   ): Promise<Record<string, unknown>[]> {
      return (
         (await session.runSQL(text)) as unknown as {
            rows: Record<string, unknown>[];
         }
      ).rows;
   }

   /** A DuckLake session on the catalog database, as a replica's would be. */
   async function attached(
      name: string,
      mode: "read" | "write",
      catalog = CATALOG,
   ): Promise<DuckDBConnection> {
      const { session, dispose } = createIsolatedBuildSession(name);
      disposers.push(dispose);
      const config = {
         catalog: {
            postgresConnection: { connectionString: conninfo },
            metadataSchema: catalog,
         },
      } as Parameters<typeof attachDuckLakeReadOnly>[2];
      await (
         mode === "read" ? attachDuckLakeReadOnly : attachDuckLakeReadWrite
      )(session, catalog, config);
      return session;
   }

   const load = (
      table: string,
      body: Record<string, unknown>,
      catalog = CATALOG,
   ) =>
      admin.loadTable(catalog, "s", table, {
         loadId: `run:${counter++}`,
         ...body,
      });

   const merge = (
      table: string,
      objects: string[],
      extra: Record<string, unknown> = {},
   ) => load(table, { mode: "merge", key: ["id"], objects, ...extra });

   const rowsOf = (table: string, columns = "id::INTEGER AS id, v") =>
      sql(reader, `SELECT ${columns} FROM ${CATALOG}.s.${table} ORDER BY id`);

   const snapshotId = async () => (await admin.listSnapshots(CATALOG, 1))[0].id;

   async function dataFiles(table: string): Promise<number> {
      const [row] = await superuser(
         `select count(*)::int as n from ${CATALOG}.ducklake_data_file f join ${CATALOG}.ducklake_table t ` +
            `on f.table_id = t.table_id where t.table_name = '${table}' and t.end_snapshot is null and f.end_snapshot is null`,
         DB,
      );
      return Number(row.n);
   }

   async function inlinedRows(catalog: string, table: string): Promise<number> {
      let rows = 0;
      for (const { table_name } of await superuser(
         `select i.table_name from ${catalog}.ducklake_inlined_data_tables i join ${catalog}.ducklake_table t ` +
            `on i.table_id = t.table_id where t.table_name = '${table}' and t.end_snapshot is null`,
         DB,
      )) {
         const [row] = await superuser(
            `select count(*)::int as n from ${catalog}."${String(table_name)}" where end_snapshot is null`,
            DB,
         );
         rows += Number(row.n);
      }
      return rows;
   }

   /** The statement_usage records the callback's loads emit. */
   async function metered(run: () => Promise<unknown>) {
      const spy = spyOn(logger, "info");
      try {
         await run().catch(() => undefined);
         return (
            spy.mock.calls as unknown as [unknown, Record<string, unknown>][]
         )
            .filter((c) => c[0] === "statement_usage")
            .map((c) => c[1]);
      } finally {
         spy.mockRestore();
      }
   }

   beforeAll(async () => {
      await superuser(`DROP DATABASE IF EXISTS ${DB} WITH (FORCE)`);
      await superuser(`CREATE DATABASE ${DB}`);
      admin = new CatalogAdmin({ catalogDatabase: conninfo });
      await admin.createCatalog({
         name: CATALOG,
         dataPath,
         encrypted: false,
         schemas: ["s"],
      });
      fixtures = new DuckDBConnection(
         "fixtures",
         ":memory:",
         mkdtempSync(join(tmpdir(), "fx-")),
      );
      reader = await attached("loads_reader", "read");
      writer = await attached("loads_writer", "write");
   });

   afterAll(async () => {
      for (const dispose of disposers) await dispose();
      await fixtures?.close();
      rmSync(root, { recursive: true, force: true });
      await superuser(`DROP DATABASE IF EXISTS ${DB} WITH (FORCE)`);
   });

   describe("merge", () => {
      it("creates the table, then inserts new keys, replaces changed ones and leaves the rest", async () => {
         const first = await parquet(
            "SELECT i AS id, 'v' || i AS v FROM range(1, 6) t(i)",
         );
         expect(await merge("basic", [first])).toEqual({
            rowsInserted: 5,
            rowsReplaced: 0,
            rowsUnchanged: 0,
            rowsDeleted: 0,
            rowCount: 5,
            cursorMax: null,
         });
         const second = await parquet(
            "SELECT * FROM (VALUES (2, 'changed'), (5, 'v5'), (6, 'new')) t(id, v)",
         );
         expect(await merge("basic", [second])).toEqual({
            rowsInserted: 1,
            rowsReplaced: 1,
            rowsUnchanged: 1,
            rowsDeleted: 0,
            rowCount: 6,
            cursorMax: null,
         });
         expect(await rowsOf("basic")).toEqual([
            { id: 1, v: "v1" },
            { id: 2, v: "changed" },
            { id: 3, v: "v3" },
            { id: 4, v: "v4" },
            { id: 5, v: "v5" },
            { id: 6, v: "new" },
         ]);
      });

      it("keeps the latest row of a key, across objects by their order and within one by its row", async () => {
         const a = await parquet(
            "SELECT * FROM (VALUES (1, 'a1'), (1, 'a1-later'), (2, 'a2')) t(id, v)",
         );
         const b = await parquet("SELECT * FROM (VALUES (2, 'b2')) t(id, v)");
         const result = await merge("dedupe", [a, b]);
         expect([result.rowsInserted, result.rowCount]).toEqual([2, 2]);
         expect(await rowsOf("dedupe")).toEqual([
            { id: 1, v: "a1-later" },
            { id: 2, v: "b2" },
         ]);
         await merge("dedupe", [b, a]);
         expect(await rowsOf("dedupe")).toEqual([
            { id: 1, v: "a1-later" },
            { id: 2, v: "a2" },
         ]);
      });

      it("refuses a URI listed twice, before anything is written", async () => {
         const a = await parquet("SELECT 1 AS id, 'x' AS v");
         const error = await refusal(merge("dupe_uri", [a, a]));
         expect([error.status, error.reason]).toEqual([400, "INVALID_REQUEST"]);
         expect(await admin.listTables(CATALOG, "s")).not.toContainEqual(
            expect.objectContaining({ name: "dupe_uri" }),
         );
      });

      it("heals a key stored twice by rewriting it once", async () => {
         const a = await parquet("SELECT 1 AS id, 'x' AS v");
         await merge("heal", [a]);
         await sql(writer, `INSERT INTO ${CATALOG}.s.heal VALUES (1, 'x')`);
         expect(await rowsOf("heal")).toHaveLength(2);
         expect(await merge("heal", [a])).toEqual({
            rowsInserted: 0,
            rowsReplaced: 1,
            rowsUnchanged: 0,
            rowsDeleted: 1,
            rowCount: 1,
            cursorMax: null,
         });
         expect(await rowsOf("heal")).toEqual([{ id: 1, v: "x" }]);
      });

      it("matches a composite key", async () => {
         const a = await parquet(
            "SELECT * FROM (VALUES (1, 'a', 'x'), (1, 'b', 'y')) t(id, region, v)",
         );
         await load("composite", {
            mode: "merge",
            key: ["id", "region"],
            objects: [a],
         });
         const b = await parquet(
            "SELECT * FROM (VALUES (1, 'b', 'z'), (2, 'a', 'w')) t(id, region, v)",
         );
         const result = await load("composite", {
            mode: "merge",
            key: ["id", "region"],
            objects: [b],
         });
         expect([
            result.rowsInserted,
            result.rowsReplaced,
            result.rowsUnchanged,
            result.rowCount,
         ]).toEqual([1, 1, 0, 3]);
      });

      it("reads nested columns that sit before a plain one, and objects whose schemas differ", async () => {
         const a = await parquet(
            "SELECT 1 AS id, {'x': 1, 'y': {'z': 2}} AS st, [1, 2] AS l, MAP {'k': 1} AS m, 'tail' AS last",
         );
         const b = await parquet(
            "SELECT 2 AS id, {'x': 3, 'y': {'z': 4}} AS st, [3] AS l, MAP {'k': 2} AS m, 'tail2' AS last, 'extra' AS only_b",
         );
         expect(await merge("nested", [a, b])).toMatchObject({
            rowsInserted: 2,
            rowCount: 2,
         });
         const table = await admin.getTable(CATALOG, "s", "nested");
         expect(table.columns.map((c) => c.name)).toEqual([
            "id",
            "st",
            "l",
            "m",
            "last",
            "only_b",
         ]);
         expect(await merge("nested", [a, b])).toMatchObject({
            rowsInserted: 0,
            rowsReplaced: 0,
            rowsUnchanged: 2,
         });
      });

      it("writes an object with no row into a table that exists, and creates an empty table", async () => {
         const empty = await parquet("SELECT 1 AS id, 'x' AS v WHERE false");
         expect(await merge("empty_new", [empty])).toMatchObject({
            rowsInserted: 0,
            rowCount: 0,
         });
         const filled = await parquet("SELECT 1 AS id, 'x' AS v");
         await merge("empty_new", [filled]);
         expect(await merge("empty_new", [empty])).toMatchObject({
            rowsInserted: 0,
            rowCount: 1,
         });
      });
   });

   describe("idempotency", () => {
      for (const [type, expr] of TYPES) {
         it(`an identical second call changes nothing and takes no snapshot (${type})`, async () => {
            const object = await parquet(
               `SELECT i AS id, ${expr} AS v FROM range(1, 4) t(i)`,
            );
            const table = `idem_${type}`;
            const first = await merge(table, [object]);
            expect(first.rowsInserted).toBe(3);
            const before = await snapshotId();
            expect(await merge(table, [object])).toEqual({
               rowsInserted: 0,
               rowsReplaced: 0,
               rowsUnchanged: 3,
               rowsDeleted: 0,
               rowCount: 3,
               cursorMax: null,
            });
            expect(await snapshotId()).toBe(before);
         });
      }

      it("adds no data file when nothing changed, and one when a row did", async () => {
         const object = await parquet(
            "SELECT i AS id, 'v' || i AS v FROM range(1, 100) t(i)",
         );
         await merge("files", [object]);
         const files = await dataFiles("files");
         await merge("files", [object]);
         expect(await dataFiles("files")).toBe(files);
         const changed = await parquet("SELECT 7 AS id, 'edited' AS v");
         await merge("files", [changed]);
         expect(await dataFiles("files")).toBe(files + 1);
      });

      it("flushes rows a catalog's own inlining limit keeps in the catalog database, even when nothing changed", async () => {
         await admin.createCatalog({
            name: "inl",
            dataPath: join(root, "inl"),
            encrypted: false,
            dataInliningRowLimit: 100,
            schemas: ["s"],
         });
         const object = await parquet(
            "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(id, v)",
            join(root, "inl"),
         );
         const body = { mode: "merge", key: ["id"], objects: [object] };
         await load("flush", body, "inl");
         expect(await inlinedRows("inl", "flush")).toBe(0);
         const inl = await attached("loads_inl", "write", "inl");
         await sql(inl, "INSERT INTO inl.s.flush VALUES (3, 'c')");
         expect(await inlinedRows("inl", "flush")).toBe(1);
         const third = await parquet(
            "SELECT * FROM (VALUES (3, 'c')) t(id, v)",
            join(root, "inl"),
         );
         expect(
            await load(
               "flush",
               { mode: "merge", key: ["id"], objects: [third] },
               "inl",
            ),
         ).toMatchObject({
            rowsUnchanged: 1,
            rowsInserted: 0,
            rowsReplaced: 0,
         });
         expect(await inlinedRows("inl", "flush")).toBe(0);
      });
   });

   describe("replace", () => {
      async function seeded(table: string): Promise<void> {
         await merge(table, [
            await parquet(
               "SELECT i AS id, 'v' || i AS v FROM range(1, 6) t(i)",
            ),
         ]);
      }

      it("deletes the keys the objects lack, and counts them", async () => {
         await seeded("rep_keyed");
         const object = await parquet(
            "SELECT * FROM (VALUES (2, 'v2'), (3, 'v3'), (6, 'v6')) t(id, v)",
         );
         expect(
            await load("rep_keyed", {
               mode: "replace",
               key: ["id"],
               maxDeletedRows: 3,
               objects: [object],
            }),
         ).toEqual({
            rowsInserted: 1,
            rowsReplaced: 0,
            rowsUnchanged: 2,
            rowsDeleted: 3,
            rowCount: 3,
            cursorMax: null,
         });
         expect(await rowsOf("rep_keyed")).toEqual([
            { id: 2, v: "v2" },
            { id: 3, v: "v3" },
            { id: 6, v: "v6" },
         ]);
      });

      it("refuses a delete past maxDeletedRows and writes nothing", async () => {
         await seeded("rep_guard");
         const object = await parquet("SELECT 2 AS id, 'v2' AS v");
         const before = await snapshotId();
         const error = await refusal(
            load("rep_guard", {
               mode: "replace",
               key: ["id"],
               maxDeletedRows: 3,
               objects: [object],
            }),
         );
         expect([error.status, error.reason]).toEqual([
            422,
            "LOAD_ROW_DROP_GUARD",
         ]);
         expect(error.detail).toEqual({
            wouldDelete: 4,
            rowsBefore: 5,
            maxDeletedRows: 3,
         });
         expect(await snapshotId()).toBe(before);
         expect(await rowsOf("rep_guard")).toHaveLength(5);
      });

      it("deletes the stored rows of a NULL key when the objects lack them", async () => {
         await seeded("rep_null");
         await sql(
            writer,
            `INSERT INTO ${CATALOG}.s.rep_null VALUES (NULL, 'orphan'), (NULL, 'orphan2')`,
         );
         const object = await parquet(
            "SELECT i AS id, 'v' || i AS v FROM range(1, 6) t(i)",
         );
         const result = await load("rep_null", {
            mode: "replace",
            key: ["id"],
            maxDeletedRows: 2,
            objects: [object],
         });
         expect([result.rowsDeleted, result.rowCount]).toEqual([2, 5]);
      });

      it("keyless: writes nothing when the table already equals the objects", async () => {
         await seeded("rep_keyless");
         const same = await parquet(
            "SELECT i AS id, 'v' || i AS v FROM range(1, 6) t(i)",
         );
         const before = await snapshotId();
         expect(
            await load("rep_keyless", {
               mode: "replace",
               maxDeletedRows: 0,
               objects: [same],
            }),
         ).toEqual({
            rowsInserted: 0,
            rowsReplaced: 0,
            rowsUnchanged: 5,
            rowsDeleted: 0,
            rowCount: 5,
            cursorMax: null,
         });
         expect(await snapshotId()).toBe(before);
      });

      it("keyless: replaces every row when anything differs, grows, and shrinks within the guard", async () => {
         await seeded("rep_keyless2");
         const edited = await parquet(
            "SELECT i AS id, CASE WHEN i = 3 THEN 'edited' ELSE 'v' || i END AS v FROM range(1, 6) t(i)",
         );
         expect(
            await load("rep_keyless2", {
               mode: "replace",
               maxDeletedRows: 0,
               objects: [edited],
            }),
         ).toMatchObject({
            rowsReplaced: 5,
            rowsInserted: 0,
            rowsDeleted: 0,
            rowCount: 5,
         });
         const bigger = await parquet(
            "SELECT i AS id, 'v' || i AS v FROM range(1, 8) t(i)",
         );
         expect(
            await load("rep_keyless2", {
               mode: "replace",
               maxDeletedRows: 0,
               objects: [bigger],
            }),
         ).toMatchObject({
            rowsReplaced: 5,
            rowsInserted: 2,
            rowsDeleted: 0,
            rowCount: 7,
         });
         const smaller = await parquet("SELECT 1 AS id, 'v1' AS v");
         expect(
            await load("rep_keyless2", {
               mode: "replace",
               maxDeletedRows: 6,
               objects: [smaller],
            }),
         ).toMatchObject({
            rowsReplaced: 1,
            rowsInserted: 0,
            rowsDeleted: 6,
            rowCount: 1,
         });
         expect(await rowsOf("rep_keyless2")).toEqual([{ id: 1, v: "v1" }]);
      });

      it("keyless: an object with no rows cannot empty the table past the guard", async () => {
         await seeded("rep_empty");
         const empty = await parquet("SELECT 1 AS id, 'x' AS v WHERE false");
         const error = await refusal(
            load("rep_empty", {
               mode: "replace",
               maxDeletedRows: 0,
               objects: [empty],
            }),
         );
         expect(error.reason).toBe("LOAD_ROW_DROP_GUARD");
         expect(await rowsOf("rep_empty")).toHaveLength(5);
      });
   });

   describe("the schema contract", () => {
      async function seed(table: string, select: string): Promise<void> {
         await merge(table, [await parquet(select)]);
      }

      async function contract(
         promise: Promise<unknown>,
      ): Promise<Record<string, unknown>> {
         const error = await refusal(promise);
         expect([error.status, error.reason]).toEqual([
            422,
            "LOAD_SCHEMA_CONTRACT",
         ]);
         return error.detail!;
      }

      it("adds a column the table lacks, and writes NULL for one the objects lack", async () => {
         await seed("c_add", "SELECT 1 AS id, 'a' AS v, 10 AS n");
         const wider = await parquet(
            "SELECT * FROM (VALUES (1, 'a', 10, 'new'), (2, 'b', 20, 'x')) t(id, v, n, w)",
         );
         expect(await merge("c_add", [wider])).toMatchObject({
            rowsInserted: 1,
            rowsReplaced: 1,
         });
         expect(
            await rowsOf("c_add", "id::INTEGER AS id, v, n::INTEGER AS n, w"),
         ).toEqual([
            { id: 1, v: "a", n: 10, w: "new" },
            { id: 2, v: "b", n: 20, w: "x" },
         ]);
         const narrower = await parquet("SELECT 2 AS id, 'b' AS v");
         expect(await merge("c_add", [narrower])).toMatchObject({
            rowsReplaced: 1,
         });
         expect(
            await rowsOf("c_add", "id::INTEGER AS id, n::INTEGER AS n, w"),
         ).toEqual([
            { id: 1, n: 10, w: "new" },
            { id: 2, n: null, w: null },
         ]);
      });

      it("accepts a narrower integer and FLOAT into DOUBLE, and refuses the reverse", async () => {
         await seed(
            "c_widen",
            "SELECT 1 AS id, 5::BIGINT AS big, 1.5::DOUBLE AS d",
         );
         const narrow = await parquet(
            "SELECT 1 AS id, 5::INTEGER AS big, 1.5::FLOAT AS d",
         );
         expect(await merge("c_widen", [narrow])).toMatchObject({
            rowsUnchanged: 1,
         });
         await seed(
            "c_narrow",
            "SELECT 1 AS id, 5::INTEGER AS n, 1.5::FLOAT AS f",
         );
         expect(
            await contract(
               merge("c_narrow", [
                  await parquet(
                     "SELECT 1 AS id, 5::BIGINT AS n, 1.5::FLOAT AS f",
                  ),
               ]),
            ),
         ).toEqual({
            column: "n",
            tableType: "INTEGER",
            loadType: "BIGINT",
            detail: "type_change",
         });
         expect(
            (
               await contract(
                  merge("c_narrow", [
                     await parquet(
                        "SELECT 1 AS id, 5::INTEGER AS n, 1.5::DOUBLE AS f",
                     ),
                  ]),
               )
            ).detail,
         ).toBe("type_change");
         expect(
            (
               await contract(
                  merge("c_narrow", [
                     await parquet("SELECT 1 AS id, 'x' AS n, 1.5::FLOAT AS f"),
                  ]),
               )
            ).detail,
         ).toBe("type_change");
      });

      it("accepts a load that widens without loss, and refuses one that narrows", async () => {
         await seed(
            "c_lossless",
            "SELECT 1 AS id, 1.50::DECIMAL(7,2) AS d, 5::INTEGER AS i, 5::INTEGER AS u",
         );
         const widened = await parquet(
            "SELECT 1 AS id, 1.50::DECIMAL(3,2) AS d, 5::TINYINT AS i, 5::UTINYINT AS u",
         );
         expect(await merge("c_lossless", [widened])).toMatchObject({
            rowsUnchanged: 1,
            rowsReplaced: 0,
         });
         await seed("c_float", "SELECT 1 AS id, 5::DOUBLE AS x");
         expect(
            await merge("c_float", [
               await parquet("SELECT 1 AS id, 5::INTEGER AS x"),
            ]),
         ).toMatchObject({ rowsUnchanged: 1 });
         expect(
            await contract(
               merge("c_lossless", [
                  await parquet("SELECT 1 AS id, 1.50::DECIMAL(10,2) AS d"),
               ]),
            ),
         ).toMatchObject({ column: "d", detail: "type_change" });
         expect(
            await contract(
               merge("c_float", [
                  await parquet("SELECT 1 AS id, 5::BIGINT AS x"),
               ]),
            ),
         ).toMatchObject({ column: "x", detail: "type_change" });
      });

      it("refuses a table that has a column named like the load's own helper", async () => {
         await sql(
            writer,
            `CREATE TABLE ${CATALOG}.s.c_shadow (id INTEGER, v VARCHAR, __publisher_present INTEGER)`,
         );
         expect(
            await contract(
               merge("c_shadow", [await parquet("SELECT 1 AS id, 'a' AS v")]),
            ),
         ).toMatchObject({
            column: "__publisher_present",
            detail: "reserved_name",
         });
      });

      it("refuses a column the objects type differently when they do not widen into one, and takes one that does", async () => {
         const mixed = async (a: string, b: string) => [
            await parquet(`SELECT 1 AS id, 1::${a} AS k`),
            await parquet(`SELECT 2 AS id, 2::${b} AS k`),
         ];
         // Read together BIGINT and DOUBLE would be DOUBLE: past 2^53 distinct keys collapse.
         expect(
            await contract(merge("c_mixed", await mixed("BIGINT", "DOUBLE"))),
         ).toEqual({
            column: "k",
            tableType: null,
            loadType: "BIGINT",
            detail: "type_change",
         });
         expect(
            await refusal(
               merge("c_mixed", await mixed("INTEGER", "VARCHAR")),
            ).then((error) => error.detail),
         ).toMatchObject({ column: "k", detail: "type_change" });
         expect(
            (await admin.listTables(CATALOG, "s")).map((t) => t.name),
         ).not.toContain("c_mixed");
         expect(
            await merge("c_widened", await mixed("INTEGER", "BIGINT")),
         ).toMatchObject({ rowsInserted: 2 });
         expect(
            (await admin.getTable(CATALOG, "s", "c_widened")).columns[1].type,
         ).toBe("BIGINT");
      });

      it("refuses a key the objects lack, and a NULL key", async () => {
         await seed("c_key", "SELECT 1 AS id, 'a' AS v");
         expect(
            await contract(merge("c_key", [await parquet("SELECT 'a' AS v")])),
         ).toMatchObject({ column: "id", detail: "missing_key" });
         expect(
            await contract(
               merge("c_key", [
                  await parquet("SELECT NULL::INTEGER AS id, 'a' AS v"),
               ]),
            ),
         ).toMatchObject({ detail: "null_key" });
         expect(await rowsOf("c_key")).toEqual([{ id: 1, v: "a" }]);
      });

      it("refuses names that differ only by case in two objects", async () => {
         expect(
            await contract(
               merge("c_case", [
                  await parquet("SELECT 1 AS id, 'x' AS v"),
                  await parquet("SELECT 2 AS id, 'y' AS V"),
               ]),
            ),
         ).toMatchObject({ detail: "case_collision" });
      });

      it("refuses a column name the load reserves", async () => {
         for (const name of ["__publisher_x", "file_row_number", "rowid"]) {
            expect(
               await contract(
                  merge("c_reserved", [
                     await parquet(`SELECT 1 AS id, 1 AS ${name}`),
                  ]),
               ),
            ).toMatchObject({ column: name, detail: "reserved_name" });
         }
      });

      it("never splices a hostile struct field name into SQL", async () => {
         await seed("c_hostile", "SELECT 1 AS id, 'a' AS v");
         const hostile = `x"), "evil`;
         const object = await parquet(
            `SELECT 1 AS id, 'a' AS v, {'${hostile}': 1, 'it''s': 2} AS st`,
         );
         expect(await merge("c_hostile", [object])).toMatchObject({
            rowsReplaced: 1,
         });
         const table = await admin.getTable(CATALOG, "s", "c_hostile");
         expect(table.columns.map((c) => c.name)).toEqual(["id", "v", "st"]);
         expect(table.columns[2].type).toContain(`"x""), ""evil"`);
         expect(await merge("c_hostile", [object])).toMatchObject({
            rowsUnchanged: 1,
         });
      });

      it("refuses a cursor that is missing or not an integer, date or timestamp", async () => {
         await seed("c_cursor", "SELECT 1 AS id, 'a' AS v");
         const object = await parquet("SELECT 1 AS id, 'a' AS v");
         expect(
            await contract(merge("c_cursor", [object], { cursor: "v" })),
         ).toEqual({
            column: "v",
            tableType: "VARCHAR",
            loadType: "VARCHAR",
            detail: "cursor_type",
         });
         expect(
            await contract(merge("c_cursor", [object], { cursor: "nope" })),
         ).toMatchObject({ detail: "missing_cursor" });
      });
   });

   describe("a column its writer typed NULL", () => {
      // fixtures/*_null_column.parquet: pyarrow 26.0.1, pq.write_table(pa.table({"id": [1, 2, 3], "v": ["a", "b", "c"], "junk": pa.nulls(3)}))
      // and, for only_null_column, pa.table({"junk": pa.nulls(3)}). DuckDB reads `junk` as INTEGER; parquet_schema calls it NullType().
      const columnsOf = async (table: string) =>
         (await admin.getTable(CATALOG, "s", table)).columns.map(
            (c) => `${c.name} ${c.type}`,
         );

      it("is ignored against a table that has the column typed otherwise", async () => {
         await merge("nulls_existing", [
            await parquet(
               "SELECT * FROM (VALUES (1::BIGINT, 'a', NULL::VARCHAR), (2::BIGINT, 'b', NULL::VARCHAR), (3::BIGINT, 'c', NULL::VARCHAR)) t(id, v, junk)",
            ),
         ]);
         expect(
            await merge("nulls_existing", [fixture("all_null_column.parquet")]),
         ).toMatchObject({
            rowsInserted: 0,
            rowsReplaced: 0,
            rowsUnchanged: 3,
         });
         expect(await columnsOf("nulls_existing")).toEqual([
            "id BIGINT",
            "v VARCHAR",
            "junk VARCHAR",
         ]);
      });

      it("is not created as INTEGER on a first load, and a later batch with values for it adds it", async () => {
         expect(
            await merge("nulls_first", [fixture("all_null_column.parquet")]),
         ).toMatchObject({ rowsInserted: 3, rowCount: 3 });
         expect(await columnsOf("nulls_first")).toEqual([
            "id BIGINT",
            "v VARCHAR",
         ]);
         const later = await parquet(
            "SELECT * FROM (VALUES (1::BIGINT, 'a', 'real'), (4::BIGINT, 'd', 'x')) t(id, v, junk)",
         );
         expect(await merge("nulls_first", [later])).toMatchObject({
            rowsInserted: 1,
            rowsReplaced: 1,
            rowsUnchanged: 0,
         });
         expect(await columnsOf("nulls_first")).toEqual([
            "id BIGINT",
            "v VARCHAR",
            "junk VARCHAR",
         ]);
         expect(
            await rowsOf("nulls_first", "id::INTEGER AS id, junk AS v"),
         ).toEqual([
            { id: 1, v: "real" },
            { id: 2, v: null },
            { id: 3, v: null },
            { id: 4, v: "x" },
         ]);
      });

      it("takes the type of another object that has values for it", async () => {
         const typed = await parquet(
            "SELECT * FROM (VALUES (4::BIGINT, 'd', 7::BIGINT)) t(id, v, junk)",
         );
         expect(
            await merge("nulls_mixed", [
               fixture("all_null_column.parquet"),
               typed,
            ]),
         ).toMatchObject({ rowsInserted: 4 });
         expect(await columnsOf("nulls_mixed")).toEqual([
            "id BIGINT",
            "v VARCHAR",
            "junk BIGINT",
         ]);
      });

      it("refuses objects whose every column is typed NULL, as having no column", async () => {
         const error = await refusal(
            merge("nulls_only", [fixture("only_null_column.parquet")]),
         );
         expect([error.status, error.reason, error.detail]).toEqual([
            422,
            "LOAD_SCHEMA_CONTRACT",
            {
               column: "",
               tableType: null,
               loadType: null,
               detail: "no_columns",
            },
         ]);
      });

      it("adds a typed column whose every value is NULL, though no row changed", async () => {
         await merge("nulls_added", [
            await parquet("SELECT 1 AS id, 'a' AS v"),
         ]);
         const before = await snapshotId();
         expect(
            await merge("nulls_added", [
               await parquet(
                  "SELECT 1 AS id, 'a' AS v, NULL::VARCHAR AS extra",
               ),
            ]),
         ).toMatchObject({
            rowsInserted: 0,
            rowsReplaced: 0,
            rowsUnchanged: 1,
         });
         expect(await columnsOf("nulls_added")).toEqual([
            "id INTEGER",
            "v VARCHAR",
            "extra VARCHAR",
         ]);
         expect(await snapshotId()).toBeGreaterThan(before);
         const settled = await snapshotId();
         await merge("nulls_added", [
            await parquet("SELECT 1 AS id, 'a' AS v, NULL::VARCHAR AS extra"),
         ]);
         expect(await snapshotId()).toBe(settled);
      });
   });

   describe("the cursor", () => {
      it.each([
         [
            "BIGINT beyond 2^53",
            "9007199254740993::BIGINT + i",
            "9007199254740996",
            "BIGINT",
         ],
         [
            "UBIGINT beyond 2^63",
            "18446744073709551612::UBIGINT + i::UBIGINT",
            "18446744073709551615",
            "UBIGINT",
         ],
         ["DATE", "DATE '2026-01-01' + i::INTEGER", "2026-01-04", "DATE"],
         [
            "TIMESTAMP",
            "TIMESTAMP '2026-01-01 00:00:00.5' + to_hours(i)",
            "2026-01-01T03:00:00.500000",
            "TIMESTAMP",
         ],
         [
            "TIMESTAMPTZ",
            "TIMESTAMPTZ '2026-01-01 00:00:00+02' + to_hours(i)",
            "2026-01-01T01:00:00.000000Z",
            "TIMESTAMP WITH TIME ZONE",
         ],
         ["only NULL", "NULL::BIGINT", null, "BIGINT"],
      ])("reports the greatest %s as text", async (name, expr, max, type) => {
         const object = await parquet(
            `SELECT i AS id, ${expr} AS c FROM range(1, 4) t(i)`,
         );
         const table = `cur_${name.replace(/\W+/g, "_")}`;
         const result = await merge(table, [object], { cursor: "c" });
         expect(result.cursorMax).toBe(max);
         expect(result.cursorType).toBe(type);
         // The same on a call that changes nothing.
         expect(await merge(table, [object], { cursor: "c" })).toMatchObject({
            cursorMax: max,
            rowsUnchanged: 3,
         });
      });

      it("takes the greatest over the rows a load keeps, not the ones a later row replaces", async () => {
         const object = await parquet(
            "SELECT * FROM (VALUES (1, 100), (1, 5), (2, 7)) t(id, c)",
         );
         expect(
            (await merge("cur_dedupe", [object], { cursor: "c" })).cursorMax,
         ).toBe("7");
      });
   });

   describe("objects", () => {
      it("refuses a missing object as unreadable, naming no path", async () => {
         const good = await parquet("SELECT 1 AS id, 'x' AS v");
         const missing = join(dataPath, "staging", "missing.parquet");
         const error = await refusal(merge("unread", [good, missing]));
         expect([error.status, error.reason]).toEqual([
            422,
            "LOAD_OBJECT_UNREADABLE",
         ]);
         expect(error.message).not.toContain("missing.parquet");
         expect(error.message).not.toContain(root);
         expect(error.detail).toEqual({ object: 1 });
      });

      it("refuses a file that is not Parquet as unreadable", async () => {
         const file = join(dataPath, "staging", "not_parquet.parquet");
         mkdirSync(dirname(file), { recursive: true });
         writeFileSync(file, "this is not parquet");
         const error = await refusal(merge("unread2", [file]));
         expect([error.status, error.reason]).toEqual([
            422,
            "LOAD_OBJECT_UNREADABLE",
         ]);
         expect(error.message).not.toContain(root);
      });

      it("refuses an object outside the data path", async () => {
         const outside = join(root, "elsewhere.parquet");
         const error = await refusal(merge("outside", [outside]));
         expect([error.status, error.reason]).toEqual([
            400,
            "OBJECT_OUTSIDE_DATA_PATH",
         ]);
      });

      it("answers 404 for a catalog or a schema that does not exist", async () => {
         const object = await parquet("SELECT 1 AS id");
         expect(
            (
               await refusal(
                  admin.loadTable("nowhere", "s", "t", {
                     loadId: "x",
                     mode: "merge",
                     key: ["id"],
                     objects: [object],
                  }),
               )
            ).reason,
         ).toBe("CATALOG_NOT_FOUND");
         expect(
            (
               await refusal(
                  admin.loadTable(CATALOG, "nowhere", "t", {
                     loadId: "x",
                     mode: "merge",
                     key: ["id"],
                     objects: [object],
                  }),
               )
            ).reason,
         ).toBe("SCHEMA_NOT_FOUND");
      });
   });

   describe("admission", () => {
      it("refuses an overlapping load of the same table from another replica, and loads it once the first finishes", async () => {
         const object = await parquet(
            "SELECT i AS id, 'v' || i AS v FROM range(1, 200001) t(i)",
         );
         const other = new CatalogAdmin({ catalogDatabase: conninfo });
         const first = merge("overlap", [object]);
         await pollFor(
            async () =>
               Number(
                  (
                     await superuser(
                        "select count(*)::int as n from pg_locks where locktype = 'advisory' and granted and database = (select oid from pg_database where datname = current_database())",
                        DB,
                     )
                  )[0].n,
               ),
            (n) => n > 0,
            15_000,
            10,
         );
         const busy = await refusal(
            other.loadTable(CATALOG, "s", "overlap", {
               loadId: "second",
               mode: "merge",
               key: ["id"],
               objects: [object],
            }),
         );
         expect([busy.status, busy.reason]).toEqual([503, "LOAD_BUSY"]);
         expect((await first).rowsInserted).toBe(200000);
         expect(
            await other.loadTable(CATALOG, "s", "overlap", {
               loadId: "third",
               mode: "merge",
               key: ["id"],
               objects: [object],
            }),
         ).toMatchObject({ rowsUnchanged: 200000, rowsInserted: 0 });
      });

      it("rolls back and answers 503 when the lock's connection dies before the commit", async () => {
         const object = await parquet(
            "SELECT i AS id, 'v' || i AS v FROM range(1, 200001) t(i)",
         );
         const before = await snapshotId();
         const doomed = merge("killed", [object]);
         const pid = await pollFor(
            async () => {
               const [row] = await superuser(
                  "select pid from pg_locks where locktype = 'advisory' and granted and database = (select oid from pg_database where datname = current_database()) limit 1",
                  DB,
               );
               return row ? Number(row.pid) : undefined;
            },
            (found) => found !== undefined,
            15_000,
            10,
         );
         await superuser(`select pg_terminate_backend(${pid})`);
         const error = await refusal(doomed);
         expect([error.status, error.reason]).toEqual([503, "LOAD_BUSY"]);
         expect(await snapshotId()).toBe(before);
         expect(await admin.listTables(CATALOG, "s")).not.toContainEqual(
            expect.objectContaining({ name: "killed" }),
         );
         // The lock went with its connection: the retry loads.
         expect((await merge("killed", [object])).rowsInserted).toBe(200000);
      });

      it("rolls back and answers 409 when another replica replaces the table before the commit", async () => {
         const first = await parquet(
            "SELECT i AS id, 'v' || i AS v FROM range(1, 4) t(i)",
         );
         await merge("raced", [first]);
         const replacer = await attached("loads_replacer", "write");
         const lock = spyOn(admin, "withTableLock");
         lock.mockImplementation(
            (catalog, schema, table, fn) =>
               CatalogAdmin.prototype.withTableLock.call(
                  admin,
                  catalog,
                  schema,
                  table,
                  (held) =>
                     fn({
                        ...held,
                        // The table is replaced after the load planned against it and before it commits.
                        assertLockHeld: async () => {
                           await sql(
                              replacer,
                              `CREATE OR REPLACE TABLE ${CATALOG}.s.raced AS SELECT 99 AS id, 'replaced' AS v`,
                           );
                           await held.assertLockHeld();
                        },
                     }),
               ) as ReturnType<typeof fn>,
         );
         try {
            const changed = await parquet(
               "SELECT * FROM (VALUES (1, 'changed'), (7, 'new')) t(id, v)",
            );
            const error = await refusal(merge("raced", [changed]));
            expect([error.status, error.reason]).toEqual([
               409,
               "LOAD_CONFLICT",
            ]);
         } finally {
            lock.mockRestore();
         }
         // Only the replace landed; the load's rows did not.
         expect(await rowsOf("raced")).toEqual([{ id: 99, v: "replaced" }]);
         // The retry loads against the new table.
         expect(
            await merge("raced", [
               await parquet("SELECT 99 AS id, 'replaced' AS v"),
            ]),
         ).toMatchObject({ rowsUnchanged: 1 });
      });

      it("refuses a load while this process's load slot is held, and releases the table's lock", async () => {
         const object = await parquet("SELECT 1 AS id, 'x' AS v");
         const slot = loadSlots.tryAcquire()!;
         try {
            const error = await refusal(merge("slot", [object]));
            expect([error.status, error.reason]).toEqual([503, "LOAD_BUSY"]);
         } finally {
            slot.release();
         }
         expect((await merge("slot", [object])).rowsInserted).toBe(1);
         expect(loadSlots.active).toBe(0);
      });
   });

   describe("metering", () => {
      it("meters the rows a load wrote, none for a no-op, and an error for a refusal", async () => {
         const object = await parquet(
            "SELECT i AS id, 'v' || i AS v FROM range(1, 4) t(i)",
         );
         const wrote = await metered(() => merge("meter", [object]));
         expect(wrote).toHaveLength(1);
         expect(wrote[0]).toMatchObject({
            surface: "load",
            connection: CATALOG,
            catalog: CATALOG,
            rows: 3,
            status: "ok",
         });
         const changed = await parquet(
            "SELECT * FROM (VALUES (1, 'x'), (9, 'y')) t(id, v)",
         );
         expect((await metered(() => merge("meter", [changed])))[0].rows).toBe(
            2,
         );
         expect(
            (await metered(() => merge("meter", [changed])))[0],
         ).toMatchObject({
            rows: 0,
            status: "ok",
         });
         const refused = await metered(() =>
            load("meter", {
               mode: "replace",
               key: ["id"],
               maxDeletedRows: 0,
               objects: [changed],
            }),
         );
         expect(refused).toHaveLength(1);
         expect(refused[0]).toMatchObject({ status: "error" });
         expect(refused[0].rows).toBeUndefined();
         // A request refused before it ran is not metered.
         expect(
            await metered(() =>
               load("meter", { mode: "merge", objects: [object] }),
            ),
         ).toEqual([]);
      });
   });

   describe("the plan", () => {
      it.each(["merge", "replace"] as const)(
         "plans every %s statement as hash joins, never a nested loop",
         async (mode) => {
            const table = `plan_${mode}`;
            await merge(table, [
               await parquet(
                  "SELECT i AS id, 'v' || i AS v FROM range(1, 5001) t(i)",
               ),
            ]);
            const target = `"${CATALOG}"."s"."${table}"`;
            await sql(
               writer,
               "CREATE OR REPLACE TEMP TABLE \"__publisher_stage\" AS SELECT i AS id, 'w' || i AS v, 0 AS __publisher_pos, 0 AS __publisher_row FROM range(1, 100) t(i)",
            );
            await sql(writer, 'DROP TABLE IF EXISTS "__publisher_changes"');
            const statements = mergeStatements({
               target,
               mode,
               keys: ["id"],
               columns: [{ name: "v", inStage: true, inTable: true }],
            });
            const plan = async (statement: string) =>
               (await sql(writer, `EXPLAIN ${statement}`))
                  .map((r) => String(r.explain_value))
                  .join("\n");
            const planned = [await plan(statements.changes)];
            await sql(writer, statements.changes);
            planned.push(
               await plan(statements.delete),
               await plan(statements.insert),
            );
            for (const text of planned) {
               expect(text).toContain("HASH_JOIN");
               expect(text).not.toContain("NL_JOIN");
               expect(text).not.toContain("CROSS_PRODUCT");
            }
         },
      );
   });
});
