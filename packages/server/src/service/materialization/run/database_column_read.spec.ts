// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { capturedDatabaseSchema } from "./database_build_strategy";
import {
   movedSinceBuild,
   observedDatabaseColumns,
} from "./database_column_read";
import { recordConnectionType } from "../../connection_type";

/**
 * The bind's read of each database table's columns from its warehouse:
 * batched catalog queries, never per query, each type spelled as the build's
 * capture spells it, bounded by a timeout, and fail safe (`unread`) when a
 * table cannot be read.
 */
describe("observedDatabaseColumns on DuckDB", () => {
   let connection: DuckDBConnection;
   /** Every statement DuckDB ran, counted. */
   let statements: string[];
   beforeAll(async () => {
      connection = new DuckDBConnection(
         "observed_columns",
         path.join(
            fs.mkdtempSync(path.join(os.tmpdir(), "observed-")),
            "db.duckdb",
         ),
      );
      await connection.runSQL(
         `CREATE TABLE a AS SELECT 'east' AS region, 1.5::DECIMAL(10,1) AS amt, DATE '2026-01-01' AS d`,
      );
      await connection.runSQL(`CREATE TABLE b AS SELECT 1 AS n`);
      await connection.runSQL(`CREATE TABLE c AS SELECT true AS flag`);
      await connection.runSQL(`CREATE SCHEMA "my.schema"`);
      await connection.runSQL(
         `CREATE TABLE "my.schema"."t""q" AS SELECT 1 AS n`,
      );
      // The connection's one statement runner, counted.
      const spied = connection as unknown as {
         runDuckDBQuery: (sql: string) => Promise<unknown>;
      };
      const run = spied.runDuckDBQuery.bind(connection);
      statements = [];
      spied.runDuckDBQuery = (sql: string) => {
         statements.push(sql);
         return run(sql);
      };
   });
   afterAll(async () => {
      await connection.close();
   });

   it("reads one in-process DESCRIBE per table, spelled as the build's capture", async () => {
      const captured = await capturedDatabaseSchema({
         connection,
         physicalTableName: "a",
         dialect: "duckdb",
         sourceName: "a",
      });
      statements.length = 0;
      const observed = await observedDatabaseColumns({
         entries: {
            ida: { tableName: "a" },
            idb: { tableName: '"b"' },
            idc: { tableName: "c" },
         },
         connectionNameOf: () => "duckdb",
         connection: async () => connection,
      });
      // One DESCRIBE per table, and one read of which catalogs are DuckLake.
      expect(
         statements.filter((sql) => !sql.includes("duckdb_databases()")),
      ).toHaveLength(3);
      expect(
         statements.filter((sql) => sql.includes("duckdb_databases()")),
      ).toHaveLength(1);
      const a = observed.get("ida") as {
         columns: { name: string; type?: string }[];
      };
      expect(a.columns.map(({ name, type }) => ({ name, type }))).toEqual(
         captured!,
      );
      expect(observed.get("idb")).toEqual({
         dialect: "duckdb",
         columns: [{ name: "n", type: "BIGINT", nativeType: "INTEGER" }],
      });
   });

   it("reads a quoted name holding a dot and a doubled quote", async () => {
      const observed = await observedDatabaseColumns({
         entries: { q: { tableName: '"my.schema"."t""q"' } },
         connectionNameOf: () => "duckdb",
         connection: async () => connection,
      });
      expect(observed.get("q")).toEqual({
         dialect: "duckdb",
         columns: [{ name: "n", type: "BIGINT", nativeType: "INTEGER" }],
      });
   });

   it("reads the table as it is now", async () => {
      const read = () =>
         observedDatabaseColumns({
            entries: { idc: { tableName: "c" } },
            connectionNameOf: () => "duckdb",
            connection: async () => connection,
         });
      expect((await read()).get("idc")).toEqual({
         dialect: "duckdb",
         columns: [{ name: "flag", type: "BOOLEAN", nativeType: "BOOLEAN" }],
      });
      await connection.runSQL(`ALTER TABLE c ALTER flag TYPE INTEGER`);
      expect((await read()).get("idc")).toEqual({
         dialect: "duckdb",
         columns: [{ name: "flag", type: "BIGINT", nativeType: "INTEGER" }],
      });
   });

   it("a table that cannot be read, or whose connection or name is unusable, is unread", async () => {
      const observed = await observedDatabaseColumns({
         entries: {
            gone: { tableName: "no_such_table" },
            nowhere: { tableName: "a" },
            broken: { tableName: "a", connectionName: "broken" },
            garbled: { tableName: '"a' },
         },
         connectionNameOf: (id) =>
            id === "nowhere"
               ? undefined
               : id === "broken"
                 ? "broken"
                 : "duckdb",
         connection: async (name) => {
            if (name === "broken") throw new Error("no such connection");
            return connection;
         },
      });
      // A fixed class for the API; the driver's text only as the log's detail.
      expect(observed.get("gone")).toMatchObject({
         unread: "catalog_query_failed",
      });
      expect(observed.get("nowhere")).toEqual({
         unread: "connection_failed",
         detail: "the table's connection is not known",
      });
      expect(observed.get("broken")).toEqual({
         unread: "connection_failed",
         detail: "no such connection",
      });
      expect(observed.get("garbled")).toEqual({ unread: "unparseable" });
   });
});

/** A connection that records each statement and answers with `answer`'s rows. */
function recording(
   dialectName: string,
   answer: (sql: string) => Record<string, unknown>[] = () => [],
   delayMs = 0,
) {
   const statements: string[] = [];
   let inFlight = 0;
   let maxInFlight = 0;
   return {
      statements,
      maxInFlight: () => maxInFlight,
      connection: {
         dialectName,
         runSQL: async (sql: string) => {
            statements.push(sql);
            inFlight++;
            maxInFlight = Math.max(maxInFlight, inFlight);
            await new Promise((r) => setTimeout(r, delayMs));
            inFlight--;
            return { rows: answer(sql), totalRows: 0 };
         },
      } as never,
   };
}

const TEN = Object.fromEntries(
   Array.from({ length: 10 }, (_, i) => [`id${i}`, { tableName: `sch.t${i}` }]),
);

describe("observedDatabaseColumns: batched, bounded, timed out", () => {
   it("reads ten Postgres tables in one query, each answered by its key", async () => {
      const r = recording("postgres", () => [
         { key: "id3", name: "n", type: "bigint", attnum: 1 },
      ]);
      const observed = await observedDatabaseColumns({
         entries: TEN,
         connectionNameOf: () => "pg",
         connection: async () => r.connection,
      });
      expect(r.statements).toHaveLength(1);
      expect(r.statements[0]).toContain(`to_regclass('"sch"."t9"')`);
      expect(observed.get("id3")).toEqual({
         dialect: "postgres",
         columns: [{ name: "n", type: "BIGINT", nativeType: "bigint" }],
      });
      expect(observed.get("id4")).toEqual({ unread: "not_found" });
   });

   it("reads ten tables of one schema in one information_schema query, and one per other schema", async () => {
      for (const dialect of ["snowflake", "trino", "mysql", "databricks"]) {
         const r = recording(dialect);
         await observedDatabaseColumns({
            entries: {
               ...TEN,
               other: { tableName: "cat.other.t" },
            },
            connectionNameOf: () => "w",
            connection: async () => r.connection,
         });
         expect(r.statements).toHaveLength(2);
      }
      const bq = recording("standardsql");
      await observedDatabaseColumns({
         entries: { ...TEN, other: { tableName: "ds2.t" } },
         connectionNameOf: () => "w",
         connection: async () => bq.connection,
      });
      expect(bq.statements).toHaveLength(2);
      expect(bq.statements[0]).toContain("`sch`.INFORMATION_SCHEMA.COLUMNS");
   });

   it("reads a BigQuery path in one pair of backticks as its parts", async () => {
      const bq = recording("standardsql");
      await observedDatabaseColumns({
         entries: { p: { tableName: "`proj.ds.t`" } },
         connectionNameOf: () => "w",
         connection: async () => bq.connection,
      });
      expect(bq.statements).toEqual([
         "SELECT table_name, column_name, data_type FROM `proj`.`ds`.INFORMATION_SCHEMA.COLUMNS WHERE table_name IN ('t') ORDER BY table_name, ordinal_position",
      ]);
   });

   it("marks a name its dialect cannot quote unread, alone, and reads the rest", async () => {
      const bq = recording("standardsql");
      const observed = await observedDatabaseColumns({
         entries: {
            newline: { tableName: "`ds`.`a\nb`" },
            doubled: { tableName: "`ds`.`a``b`" },
            escaped: { tableName: "`ds`.`a\\`b`" },
            plain: { tableName: "ds.t" },
         },
         connectionNameOf: () => "w",
         connection: async () => bq.connection,
      });
      expect(observed.get("newline")).toEqual({ unread: "unparseable" });
      expect(observed.get("doubled")).toEqual({ unread: "unparseable" });
      expect(bq.statements).toEqual([
         "SELECT table_name, column_name, data_type FROM `ds`.INFORMATION_SCHEMA.COLUMNS WHERE table_name IN ('a`b', 't') ORDER BY table_name, ordinal_position",
      ]);
   });

   it("runs DuckDB's per-table reads at most four at a time", async () => {
      const r = recording("duckdb", () => [], 20);
      await observedDatabaseColumns({
         entries: TEN,
         connectionNameOf: () => "d",
         connection: async () => r.connection,
      });
      expect(
         r.statements.filter((sql) => !sql.includes("duckdb_databases()")),
      ).toHaveLength(10);
      expect(r.maxInFlight()).toBe(4);
   });

   it("redacts a connection's secret from the detail the log reads", async () => {
      const failing = {
         dialectName: "postgres",
         password: "hunter2-secret",
         runSQL: async () => {
            throw new Error(
               "password authentication failed: postgres://u:hunter2-secret@h/db",
            );
         },
      } as never;
      const observed = await observedDatabaseColumns({
         entries: { t: { tableName: "sch.t" } },
         connectionNameOf: () => "pg",
         connection: async () => failing,
      });
      const read = observed.get("t") as { unread: string; detail?: string };
      expect(read.unread).toBe("catalog_query_failed");
      expect(read.detail).not.toContain("hunter2-secret");
   });

   it("reads a table whose query outlives the timeout as unread", async () => {
      const r = recording("postgres", () => [], 200);
      const started = Date.now();
      const observed = await observedDatabaseColumns({
         entries: { slow: { tableName: "sch.t" } },
         connectionNameOf: () => "pg",
         connection: async () => r.connection,
         timeoutMs: 20,
      });
      expect(Date.now() - started).toBeLessThan(150);
      expect(observed.get("slow")).toEqual({
         unread: "timed_out",
         detail: "the catalog read timed out after 20ms",
      });
   });

   it("reads a name matched in two schemas as unread", async () => {
      const r = recording("snowflake", () => [
         {
            TABLE_CATALOG: "D",
            TABLE_SCHEMA: "A",
            TABLE_NAME: "T",
            COLUMN_NAME: "n",
            DATA_TYPE: "TEXT",
         },
         {
            TABLE_CATALOG: "D",
            TABLE_SCHEMA: "B",
            TABLE_NAME: "T",
            COLUMN_NAME: "n",
            DATA_TYPE: "TEXT",
         },
      ]);
      const observed = await observedDatabaseColumns({
         entries: { t: { tableName: "t" } },
         connectionNameOf: () => "w",
         connection: async () => r.connection,
      });
      expect(observed.get("t")).toEqual({ unread: "ambiguous" });
   });
});

/**
 * P4-01: DuckLake writes a 128-bit integer to Parquet as a DOUBLE while its
 * catalog still says HUGEINT. A table on a `ducklake` connection reads such a
 * column as the DOUBLE it holds; the same column on a plain DuckDB connection,
 * which keeps every digit, reads as its own type.
 */
describe("observedDatabaseColumns: a DuckLake 128-bit integer (P4-01)", () => {
   const open = async (name: string) => {
      const c = new DuckDBConnection(
         name,
         path.join(
            fs.mkdtempSync(path.join(os.tmpdir(), "observed-")),
            "db.duckdb",
         ),
      );
      await c.runSQL(
         `CREATE TABLE t AS SELECT 1::HUGEINT AS h, 1::UHUGEINT AS u, 1::BIGINT AS b, 1::DECIMAL(38,0) AS d`,
      );
      return c;
   };
   const read = async (connection: DuckDBConnection) =>
      (
         (
            await observedDatabaseColumns({
               entries: { id: { tableName: "t" } },
               connectionNameOf: () => "w",
               connection: async () => connection,
            })
         ).get("id") as { columns: unknown[] }
      ).columns;

   it("reads it as a DOUBLE on a ducklake connection", async () => {
      const c = recordConnectionType(await open("lake_like"), "ducklake");
      try {
         expect(await read(c)).toEqual([
            {
               name: "h",
               type: "DOUBLE",
               nativeType: "DOUBLE",
               storedAsDouble: true,
            },
            {
               name: "u",
               type: "DOUBLE",
               nativeType: "DOUBLE",
               storedAsDouble: true,
            },
            { name: "b", type: "BIGINT", nativeType: "BIGINT" },
            { name: "d", type: "DOUBLE", nativeType: "DECIMAL(38,0)" },
         ]);
      } finally {
         await c.close();
      }
   });

   it("reads it as its own type on a plain duckdb connection", async () => {
      const c = recordConnectionType(await open("plain"), "duckdb");
      try {
         const columns = (await read(c)) as {
            name: string;
            storedAsDouble?: true;
         }[];
         expect(columns.some((col) => col.storedAsDouble)).toBe(false);
         expect(columns[0]).toMatchObject({ name: "h", nativeType: "HUGEINT" });
      } finally {
         await c.close();
      }
   });
});

/**
 * P4-01: the guard is decided per table, from its catalog: a DuckDB
 * connection whose setup attached a DuckLake writes lake tables while it
 * reports `duckdb`.
 */
describe("observedDatabaseColumns: a DuckLake catalog on a duckdb connection (P4-01)", () => {
   it("reads a lake table's 128-bit integer as a DOUBLE, and a native table's as itself", async () => {
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "observed-lake-"));
      const c = recordConnectionType(
         new DuckDBConnection("attached_lake", path.join(dir, "db.duckdb")),
         "duckdb",
      );
      try {
         await c.runSQL("INSTALL ducklake");
         await c.runSQL("LOAD ducklake");
         await c.runSQL(
            `ATTACH 'ducklake:${path.join(dir, "m.ducklake")}' AS lake (DATA_PATH '${path.join(dir, "data")}/')`,
         );
         await c.runSQL("CREATE TABLE lake.main.lt AS SELECT 1::HUGEINT AS h");
         await c.runSQL("CREATE TABLE nt AS SELECT 1::HUGEINT AS h");
         const observed = await observedDatabaseColumns({
            entries: {
               lake: { tableName: "lake.main.lt" },
               native: { tableName: "nt" },
            },
            connectionNameOf: () => "w",
            connection: async () => c,
         });
         expect(
            (observed.get("lake") as { columns: unknown[] }).columns,
         ).toEqual([
            {
               name: "h",
               type: "DOUBLE",
               nativeType: "DOUBLE",
               storedAsDouble: true,
            },
         ]);
         expect(
            (observed.get("native") as { columns: unknown[] }).columns,
         ).toEqual([{ name: "h", type: "BIGINT", nativeType: "HUGEINT" }]);
         // A USE of the lake makes an unqualified name a lake table.
         await c.runSQL("USE lake");
         await c.runSQL("CREATE TABLE ut AS SELECT 1::HUGEINT AS h");
         const used = await observedDatabaseColumns({
            entries: { used: { tableName: "ut" } },
            connectionNameOf: () => "w",
            connection: async () => c,
         });
         expect(
            (used.get("used") as { columns: { storedAsDouble?: true }[] })
               .columns[0].storedAsDouble,
         ).toBe(true);
      } finally {
         await c.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});

describe("movedSinceBuild on DuckDB", () => {
   let connection: DuckDBConnection;
   beforeAll(async () => {
      connection = new DuckDBConnection(
         "moved_since_build",
         path.join(
            fs.mkdtempSync(path.join(os.tmpdir(), "moved-")),
            "db.duckdb",
         ),
      );
      await connection.runSQL(
         `CREATE TABLE t AS SELECT 'east' AS cat, 2.4::DECIMAL(10,1) AS amt`,
      );
   });
   afterAll(async () => {
      await connection.close();
   });

   it("tells a table as built from one moved, unreadable, or with no recorded types", async () => {
      const built = [
         { name: "cat", type: "VARCHAR" },
         { name: "amt", type: "DECIMAL(10,1)" },
      ];
      const moved = await movedSinceBuild(
         new Map([
            ["as_built", { physicalTableName: "t", nativeSchema: built }],
            [
               "retyped",
               {
                  physicalTableName: "t",
                  nativeSchema: [built[0], { name: "amt", type: "DOUBLE" }],
               },
            ],
            [
               "gone",
               {
                  physicalTableName: "t",
                  nativeSchema: [...built, { name: "x", type: "INTEGER" }],
               },
            ],
            ["missing", { physicalTableName: "nope", nativeSchema: built }],
            ["unrecorded", { physicalTableName: "t" }],
         ]),
         connection,
      );
      expect(Object.fromEntries(moved)).toEqual({
         retyped: {
            cause: "moved",
            reason:
               "its column 'amt' was built as DOUBLE and is now DECIMAL(10,1)",
         },
         gone: { cause: "moved", reason: "its column 'x' is gone" },
         missing: {
            cause: "unreadable",
            reason: "its columns could not be read (catalog_query_failed)",
         },
         unrecorded: {
            cause: "types_unrecorded",
            reason: "its build recorded no column types to check it against",
         },
      });
   });
});
