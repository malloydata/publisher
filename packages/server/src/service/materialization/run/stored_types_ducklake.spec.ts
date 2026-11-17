// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// ms2data/service#6642 on a real DuckLake, the production destination: a build
// casts to the declared types, and the deltas an incremental source applies
// afterwards land in the cast columns. Each delta
// is computed by the warehouse, uncast, so it relies on DuckDB converting the
// warehouse's types on INSERT, which is what these tests pin.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { deltaStatements } from "./incremental_apply";
import { createTableWithDeclaredTypes } from "./materialization_build_session";

const DECLARED = [
   { name: "id", type: "number" },
   { name: "d", type: "date" },
   { name: "flag", type: "boolean" },
];

/** What a warehouse returns for a date it computed as a timestamp, and a 0/1 flag. */
const SEED = `SELECT * FROM (VALUES
   (1, TIMESTAMP '2026-01-15 00:00:00', 1),
   (2, TIMESTAMP '2026-02-03 00:00:00', 0),
   (3, TIMESTAMP '2026-02-20 00:00:00', 1)
) AS t(id, d, flag)`;

let open: { conn: DuckDBConnection; dir: string } | undefined;

afterEach(async () => {
   if (!open) return;
   await open.conn.runSQL("DETACH lake");
   await open.conn.close();
   rmSync(open.dir, { recursive: true, force: true });
   open = undefined;
});

async function lake(): Promise<DuckDBConnection> {
   const dir = mkdtempSync(join(tmpdir(), "ducklake-types-"));
   // File-backed host: db-duckdb shares one in-memory instance between
   // connections, where another file's `lake` may still be attached.
   const conn = new DuckDBConnection(
      "ducklake_types",
      join(dir, "host.duckdb"),
   );
   open = { conn, dir };
   await conn.runSQL("INSTALL ducklake");
   await conn.runSQL("LOAD ducklake");
   await conn.runSQL(
      `ATTACH 'ducklake:${join(dir, "catalog.ducklake")}' AS lake ` +
         `(DATA_PATH '${join(dir, "data")}/')`,
   );
   // Files rather than inlined catalog rows, so there is a layout to check.
   await conn.runSQL("SET ducklake_default_data_inlining_row_limit=0");
   return conn;
}

async function rows(conn: DuckDBConnection): Promise<unknown[]> {
   const result = await conn.runSQL(
      "SELECT id, CAST(d AS VARCHAR) AS d, flag FROM lake.t ORDER BY id",
   );
   return result.rows.map((r) => ({ ...r, id: Number(r.id) }));
}

describe("a declared-type cast on a DuckLake build", () => {
   it("stores the declared types", async () => {
      const conn = await lake();
      const schema = await createTableWithDeclaredTypes(conn, "lake.t", SEED, {
         columns: DECLARED,
      });
      expect(schema).toEqual([
         { name: "id", type: "INTEGER" },
         { name: "d", type: "DATE" },
         { name: "flag", type: "BOOLEAN" },
      ]);
      expect(await rows(conn)).toEqual([
         { id: 1, d: "2026-01-15", flag: true },
         { id: 2, d: "2026-02-03", flag: false },
         { id: 3, d: "2026-02-20", flag: true },
      ]);
   }, 120000);

   it("falls back to the warehouse's types when a cast cannot convert a value", async () => {
      const conn = await lake();
      const schema = await createTableWithDeclaredTypes(
         conn,
         "lake.t",
         `SELECT 1 AS id, DATE '2026-01-15' AS d, 'open' AS flag`,
         { columns: DECLARED },
      );
      expect(schema.find((c) => c.name === "flag")?.type).toBe("VARCHAR");
   }, 120000);
});

// P4-01: DuckLake writes a 128-bit integer to Parquet as a DOUBLE. A build
// that computes one in DuckDB (no passthrough) stores it as DECIMAL(38,0),
// which keeps every digit; past 38 digits the cast fails and the build stores
// the 128-bit type, which the serve side answers live.
describe("a DuckDB-computed 128-bit integer on a DuckLake build (P4-01)", () => {
   it("is stored as DECIMAL(38,0), every digit kept", async () => {
      const conn = await lake();
      const schema = await createTableWithDeclaredTypes(
         conn,
         "lake.t",
         `SELECT k, SUM(v) AS total FROM (VALUES
            ('a', 4530000000000225186::BIGINT), ('a', 1::BIGINT)
          ) AS w(k, v) GROUP BY k`,
         {
            columns: [
               { name: "k", type: "string" },
               { name: "total", type: "number" },
            ],
         },
      );
      expect(schema).toEqual([
         { name: "k", type: "VARCHAR" },
         { name: "total", type: "DECIMAL(38,0)" },
      ]);
      const stored = await conn.runSQL(
         "SELECT total::VARCHAR AS total FROM lake.t",
      );
      expect(stored.rows).toEqual([{ total: "4530000000000225187" }]);
   }, 120000);

   it("past 38 digits, stores the 128-bit type it computed", async () => {
      const conn = await lake();
      const schema = await createTableWithDeclaredTypes(
         conn,
         "lake.t",
         "SELECT 150000000000000000000000000000000000001::HUGEINT AS h",
         { columns: [{ name: "h", type: "number" }] },
      );
      expect(schema).toEqual([{ name: "h", type: "HUGEINT" }]);
   }, 120000);

   it("leaves a passthrough's columns as they arrive", async () => {
      const conn = await lake();
      const schema = await createTableWithDeclaredTypes(
         conn,
         "lake.t",
         "SELECT 1::HUGEINT AS h",
         { columns: [{ name: "h", type: "number" }] },
         { sourceType: "bigquery" },
      );
      expect(schema).toEqual([{ name: "h", type: "HUGEINT" }]);
   }, 120000);
});

// P3-06..09: a warehouse that folds Malloy's quoted aliases (Snowflake with
// QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE) returns them upper-cased; the build
// stores each under the name its build source declares.
describe("a column the warehouse returned case-folded (P3-06..09)", () => {
   it("is stored under its declared name", async () => {
      const conn = await lake();
      const schema = await createTableWithDeclaredTypes(
         conn,
         "lake.t",
         `SELECT 'east' AS "REGION", 2 AS "VIEWS__PARTIAL", 3 AS "Other"`,
         {
            columns: [
               { name: "region", type: "string" },
               { name: "views__partial", type: "number" },
            ],
         },
         { sourceType: "snowflake" },
      );
      expect(schema.map((c) => c.name)).toEqual([
         "region",
         "views__partial",
         "Other",
      ]);
   }, 120000);
});

describe("a delta into a table the build cast", () => {
   const range = {
      start: { malloyType: "date", value: "2026-02-01" },
      end: { malloyType: "date", value: "2026-03-01" },
   };
   /** The warehouse's delta: February again, uncast, one row restated. */
   const DELTA = `SELECT * FROM (VALUES
      (2, TIMESTAMP '2026-02-03 00:00:00', 1),
      (4, TIMESTAMP '2026-02-25 00:00:00', 0)
   ) AS w(id, d, flag)`;

   async function seeded(): Promise<DuckDBConnection> {
      const conn = await lake();
      await createTableWithDeclaredTypes(conn, "lake.t", SEED, {
         columns: DECLARED,
      });
      return conn;
   }

   it("range-replaces into the table, converting the warehouse's types", async () => {
      const conn = await seeded();
      for (const sql of deltaStatements({
         dialect: "duckdb",
         quotedTablePath: "lake.t",
         deltaSQL: DELTA,
         columns: ["id", "d", "flag"],
         mergeKeys: [],
         watermarkName: "d",
         ...range,
      })) {
         await conn.runSQL(sql);
      }
      // February's rows replaced (id 3 gone, id 2 restated, id 4 added),
      // January's kept, every value in the declared type.
      expect(await rows(conn)).toEqual([
         { id: 1, d: "2026-01-15", flag: true },
         { id: 2, d: "2026-02-03", flag: true },
         { id: 4, d: "2026-02-25", flag: false },
      ]);
      const types = await conn.runSQL("DESCRIBE lake.t");
      expect(types.rows.map((r) => [r.column_name, r.column_type])).toEqual([
         ["id", "INTEGER"],
         ["d", "DATE"],
         ["flag", "BOOLEAN"],
      ]);
   }, 120000);

   it("merges by key into the cast columns", async () => {
      const conn = await seeded();
      for (const sql of deltaStatements({
         dialect: "duckdb",
         quotedTablePath: "lake.t",
         deltaSQL: DELTA,
         columns: ["id", "d", "flag"],
         mergeKeys: ["id"],
         watermarkName: "d",
         ...range,
      })) {
         await conn.runSQL(sql);
      }
      expect(await rows(conn)).toEqual([
         { id: 1, d: "2026-01-15", flag: true },
         { id: 2, d: "2026-02-03", flag: true },
         { id: 3, d: "2026-02-20", flag: true },
         { id: 4, d: "2026-02-25", flag: false },
      ]);
   }, 120000);

   it("deletes a range bounded by timestamps from the date column", async () => {
      const conn = await seeded();
      await conn.runSQL(
         "DELETE FROM lake.t WHERE d >= TIMESTAMP '2026-02-01 00:00:00' AND d < TIMESTAMP '2026-02-10 12:00:00'",
      );
      expect((await rows(conn)).map((r) => (r as { id: number }).id)).toEqual([
         1, 3,
      ]);
   }, 120000);
});

// A Postgres `bytea` or `interval`: Postgres's dialect declares both `string`,
// and live answers them in Postgres's own text, which DuckDB's cast to VARCHAR
// does not write. The build keeps the BLOB and INTERVAL the session reads them
// as, so the served copy leaves them out and a query reading one answers live.
describe("a Postgres build keeps a binary or interval column's warehouse type", () => {
   const declared = [
      { name: "id", type: "number" },
      { name: "name", type: "string" },
      { name: "code", type: "string" },
      { name: "bin", type: "string" },
      { name: "iv", type: "string" },
   ];
   const seed = `SELECT * FROM (VALUES
      (1, 'a', 7, '\\x0102'::BLOB, INTERVAL 3 MONTH)
   ) AS t(id, name, code, bin, iv)`;
   const typesOf = (schema: { name?: string; type?: string }[]) =>
      Object.fromEntries(schema.map((c) => [c.name, c.type]));

   it("stores them uncast, and still casts every other declared string", async () => {
      const conn = await lake();
      const schema = await createTableWithDeclaredTypes(
         conn,
         "lake.t",
         seed,
         { columns: declared },
         { sourceType: "postgres" },
      );
      expect(typesOf(schema)).toEqual({
         id: "INTEGER",
         name: "VARCHAR",
         code: "VARCHAR",
         bin: "BLOB",
         iv: "INTERVAL",
      });
   });

   // Only a Postgres passthrough is described: describing another engine's
   // may run its query.
   it.each([["bigquery" as const], [undefined]])(
      "casts every declared string for a %s source, as before",
      async (sourceType) => {
         const conn = await lake();
         const schema = await createTableWithDeclaredTypes(
            conn,
            "lake.t",
            seed,
            { columns: declared },
            sourceType ? { sourceType } : {},
         );
         expect(typesOf(schema)).toMatchObject({
            bin: "VARCHAR",
            iv: "VARCHAR",
         });
      },
   );
});

// `partition=` is retired, and with it the bounds (one thread, a low
// partitioned_write_flush_threshold) a delta into a laid-out table ran under.
// They are not needed: a table an earlier `partition=` build laid out is never
// the target of a delta. The ledger is keyed by table, and its entry records the
// address the boundary was measured under; that address folded the layout in,
// so it no longer matches (incremental_build.spec): a standalone run seeds, and
// a caller-supplied entry fails the run until it is reseeded. Either way the
// next build is full, and a full build replaces the table. This pins that the
// replacement drops the layout, so the delta that follows writes unpartitioned
// files.
describe("a table an earlier partition= build laid out", () => {
   async function laidOutFiles(conn: DuckDBConnection): Promise<string[]> {
      const files = await conn.runSQL(
         "SELECT data_file FROM ducklake_list_files('lake', 't')",
      );
      return (files.rows as { data_file: string }[])
         .map((r) => r.data_file.replaceAll("\\", "/"))
         .filter((f) => /\/d=[^/]+\//.test(f));
   }

   it("is rebuilt unpartitioned, and a delta after it writes no partition files", async () => {
      const conn = await lake();
      // What a `partition="d"` build wrote before the upgrade.
      await conn.runSQL(`CREATE TABLE lake.t AS (${SEED}) WITH NO DATA`);
      await conn.runSQL("ALTER TABLE lake.t SET PARTITIONED BY (d)");
      await conn.runSQL(`INSERT INTO lake.t (${SEED})`);
      expect((await laidOutFiles(conn)).length).toBeGreaterThan(0);

      // The full build the new address forces.
      await createTableWithDeclaredTypes(conn, "lake.t", SEED, {
         columns: DECLARED,
      });
      expect(await laidOutFiles(conn)).toEqual([]);

      // The next refresh, a delta into the rebuilt table.
      for (const sql of deltaStatements({
         dialect: "duckdb",
         quotedTablePath: "lake.t",
         deltaSQL: `SELECT * FROM (VALUES
            (4, TIMESTAMP '2026-02-25 00:00:00', 0)
         ) AS w(id, d, flag)`,
         columns: ["id", "d", "flag"],
         mergeKeys: [],
         watermarkName: "d",
         start: { malloyType: "date", value: "2026-02-21" },
         end: { malloyType: "date", value: "2026-03-01" },
      })) {
         await conn.runSQL(sql);
      }
      expect(await laidOutFiles(conn)).toEqual([]);
      expect(await rows(conn)).toEqual([
         { id: 1, d: "2026-01-15", flag: true },
         { id: 2, d: "2026-02-03", flag: false },
         { id: 3, d: "2026-02-20", flag: true },
         { id: 4, d: "2026-02-25", flag: false },
      ]);
   }, 120000);
});
