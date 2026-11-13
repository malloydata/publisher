// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { capturedNativeSchema } from "./colocated_native_types";

describe("capturedNativeSchema: a colocated table's warehouse column types", () => {
   let duckdb: DuckDBConnection;
   beforeAll(async () => {
      duckdb = new DuckDBConnection("duckdb", ":memory:");
      await duckdb.runSQL(`CREATE SCHEMA s`);
      await duckdb.runSQL(
         `CREATE TABLE s."Wide" AS SELECT 1::BIGINT AS b, 1::UHUGEINT AS u, 1::DECIMAL(38,0) AS d, 'x' AS t`,
      );
   });
   afterAll(async () => {
      await duckdb.close();
   });

   it("reads DuckDB's own type names, in column order", async () => {
      expect(
         await capturedNativeSchema({
            run: (sql) => duckdb.runSQL(sql, { rowLimit: 1000 }),
            physicalTableName: "s.Wide",
            dialect: "duckdb",
            sourceName: "wide",
         }),
      ).toEqual([
         { name: "b", type: "BIGINT" },
         { name: "u", type: "UHUGEINT" },
         { name: "d", type: "DECIMAL(38,0)" },
         { name: "t", type: "VARCHAR" },
      ]);
   });

   it("captures nothing when the read fails, so the build stands", async () => {
      expect(
         await capturedNativeSchema({
            run: (sql) => duckdb.runSQL(sql, { rowLimit: 1000 }),
            physicalTableName: "s.missing",
            dialect: "duckdb",
            sourceName: "missing",
         }),
      ).toBeUndefined();
   });

   it("composes an information_schema decimal's precision and scale", async () => {
      // The read every other dialect uses, its rows as Snowflake returns them
      // (upper-case names, `NUMBER` without its arguments).
      let ran = "";
      const columns = await capturedNativeSchema({
         run: async (sql) => {
            ran = sql;
            return {
               rows: [
                  {
                     COLUMN_NAME: "TOT",
                     DATA_TYPE: "NUMBER",
                     NUMERIC_PRECISION: 38,
                     NUMERIC_SCALE: 0,
                  },
                  {
                     COLUMN_NAME: "G",
                     DATA_TYPE: "TEXT",
                     NUMERIC_PRECISION: null,
                     NUMERIC_SCALE: null,
                  },
               ],
            };
         },
         physicalTableName: "DB.SCH.T",
         dialect: "snowflake",
         sourceName: "t",
      });
      expect(ran).toContain("information_schema.columns");
      expect(ran).toContain("lower(table_schema) = lower('SCH')");
      expect(columns).toEqual([
         { name: "TOT", type: "NUMBER(38,0)" },
         { name: "G", type: "TEXT" },
      ]);
   });
});

const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;

describe.skipIf(!POSTGRES_URL)(
   "capturedNativeSchema on Postgres (PUBLISHER_TEST_POSTGRES_URL)",
   () => {
      let pg: PostgresConnection;
      beforeAll(async () => {
         pg = new PostgresConnection({
            name: "pg",
            connectionString: POSTGRES_URL,
         });
         await pg.runSQL(`DROP SCHEMA IF EXISTS native_types CASCADE`);
         await pg.runSQL(`CREATE SCHEMA native_types`);
         await pg.runSQL(
            `CREATE TABLE native_types.t AS SELECT sum(v) AS tot, max(v) AS top, 'x'::text AS g FROM (VALUES (1::bigint)) AS x(v)`,
         );
      });
      afterAll(async () => {
         await pg.runSQL(`DROP SCHEMA IF EXISTS native_types CASCADE`);
         await pg.close();
      });

      it("reads a sum of BIGINTs as the numeric Postgres stores it", async () => {
         expect(
            await capturedNativeSchema({
               run: (sql) => pg.runSQL(sql, { rowLimit: 1000 }),
               physicalTableName: "native_types.t",
               dialect: "postgres",
               sourceName: "t",
            }),
         ).toEqual([
            { name: "tot", type: "numeric" },
            { name: "top", type: "bigint" },
            { name: "g", type: "text" },
         ]);
      });
   },
);
