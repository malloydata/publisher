// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// ms2data/service#6642 on a real DuckLake, the production destination: a build
// that casts to the declared types keeps its partition layout, and the deltas
// an incremental source applies afterwards land in the cast columns. Each delta
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

async function partitionDirs(conn: DuckDBConnection): Promise<string[]> {
   const files = await conn.runSQL(
      "SELECT data_file FROM ducklake_list_files('lake', 't')",
   );
   return [
      ...new Set(
         (files.rows as { data_file: string }[]).map(
            (r) => /\/(d=[^/]+)\//.exec(r.data_file.replaceAll("\\", "/"))?.[1],
         ),
      ),
   ].sort() as string[];
}

describe("a declared-type cast on a partitioned DuckLake build", () => {
   it("stores the declared types and lays the table out on the cast column", async () => {
      const conn = await lake();
      const schema = await createTableWithDeclaredTypes(
         conn,
         "lake.t",
         SEED,
         { columns: DECLARED },
         ["d"],
      );
      expect(schema).toEqual([
         { name: "id", type: "INTEGER" },
         { name: "d", type: "DATE" },
         { name: "flag", type: "BOOLEAN" },
      ]);
      // One directory per DATE, not per timestamp rendering.
      expect(await partitionDirs(conn)).toEqual([
         "d=2026-01-15",
         "d=2026-02-03",
         "d=2026-02-20",
      ]);
   }, 120000);

   it("falls back to the warehouse's types, still laid out, when a cast cannot convert a value", async () => {
      const conn = await lake();
      const schema = await createTableWithDeclaredTypes(
         conn,
         "lake.t",
         `SELECT 1 AS id, DATE '2026-01-15' AS d, 'open' AS flag`,
         { columns: DECLARED },
         ["d"],
      );
      expect(schema.find((c) => c.name === "flag")?.type).toBe("VARCHAR");
      expect(await partitionDirs(conn)).toEqual(["d=2026-01-15"]);
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
      await createTableWithDeclaredTypes(
         conn,
         "lake.t",
         SEED,
         { columns: DECLARED },
         ["d"],
      );
      return conn;
   }

   it("range-replaces into the laid-out table, converting the warehouse's types", async () => {
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
      expect(await partitionDirs(conn)).toContain("d=2026-02-25");
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
         [],
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
            [],
            sourceType ? { sourceType } : {},
         );
         expect(typesOf(schema)).toMatchObject({
            bin: "VARCHAR",
            iv: "VARCHAR",
         });
      },
   );
});
