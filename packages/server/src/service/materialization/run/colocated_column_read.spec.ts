// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { capturedColocatedSchema } from "./colocated_build_strategy";
import { observedColocatedColumns } from "./colocated_column_read";

/**
 * The bind's read of each colocated table's columns from its warehouse:
 * batched catalog queries, never per query, each type spelled as the build's
 * capture spells it, bounded by a timeout, and fail safe (`unread`) when a
 * table cannot be read.
 */
describe("observedColocatedColumns on DuckDB", () => {
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
      const captured = await capturedColocatedSchema({
         connection,
         physicalTableName: "a",
         dialect: "duckdb",
         sourceName: "a",
      });
      statements.length = 0;
      const observed = await observedColocatedColumns({
         entries: {
            ida: { tableName: "a" },
            idb: { tableName: '"b"' },
            idc: { tableName: "c" },
         },
         connectionNameOf: () => "duckdb",
         connection: async () => connection,
      });
      expect(statements).toHaveLength(3);
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
      const observed = await observedColocatedColumns({
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
         observedColocatedColumns({
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
      const observed = await observedColocatedColumns({
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
      expect(Object.keys(observed.get("gone")!)).toEqual(["unread"]);
      expect(observed.get("nowhere")).toEqual({
         unread: "the table's connection is not known",
      });
      expect(observed.get("broken")).toEqual({ unread: "no such connection" });
      expect(observed.get("garbled")).toEqual({
         unread: "the table's name does not parse",
      });
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

describe("observedColocatedColumns: batched, bounded, timed out", () => {
   it("reads ten Postgres tables in one query, each answered by its key", async () => {
      const r = recording("postgres", () => [
         { key: "id3", name: "n", type: "bigint", attnum: 1 },
      ]);
      const observed = await observedColocatedColumns({
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
      expect(observed.get("id4")).toEqual({
         unread: "the table was not found in its catalog",
      });
   });

   it("reads ten tables of one schema in one information_schema query, and one per other schema", async () => {
      for (const dialect of ["snowflake", "trino", "mysql", "databricks"]) {
         const r = recording(dialect);
         await observedColocatedColumns({
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
      await observedColocatedColumns({
         entries: { ...TEN, other: { tableName: "ds2.t" } },
         connectionNameOf: () => "w",
         connection: async () => bq.connection,
      });
      expect(bq.statements).toHaveLength(2);
      expect(bq.statements[0]).toContain("`sch`.INFORMATION_SCHEMA.COLUMNS");
   });

   it("reads a BigQuery path in one pair of backticks as its parts", async () => {
      const bq = recording("standardsql");
      await observedColocatedColumns({
         entries: { p: { tableName: "`proj.ds.t`" } },
         connectionNameOf: () => "w",
         connection: async () => bq.connection,
      });
      expect(bq.statements).toEqual([
         "SELECT table_name, column_name, data_type FROM `proj`.`ds`.INFORMATION_SCHEMA.COLUMNS WHERE table_name IN ('t') ORDER BY table_name, ordinal_position",
      ]);
   });

   it("runs DuckDB's per-table reads at most four at a time", async () => {
      const r = recording("duckdb", () => [], 20);
      await observedColocatedColumns({
         entries: TEN,
         connectionNameOf: () => "d",
         connection: async () => r.connection,
      });
      expect(r.statements).toHaveLength(10);
      expect(r.maxInFlight()).toBe(4);
   });

   it("reads a table whose query outlives the timeout as unread", async () => {
      const r = recording("postgres", () => [], 200);
      const started = Date.now();
      const observed = await observedColocatedColumns({
         entries: { slow: { tableName: "sch.t" } },
         connectionNameOf: () => "pg",
         connection: async () => r.connection,
         timeoutMs: 20,
      });
      expect(Date.now() - started).toBeLessThan(150);
      expect(observed.get("slow")).toEqual({
         unread: "the catalog read timed out after 20ms",
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
      const observed = await observedColocatedColumns({
         entries: { t: { tableName: "t" } },
         connectionNameOf: () => "w",
         connection: async () => r.connection,
      });
      expect(observed.get("t")).toEqual({
         unread: "the table's name matched more than one table",
      });
   });
});
