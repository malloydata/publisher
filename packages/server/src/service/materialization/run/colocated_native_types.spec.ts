// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { MySQLConnection } from "@malloydata/db-mysql";
import { PostgresConnection } from "@malloydata/db-postgres";
import { TrinoConnection } from "@malloydata/db-trino";
import { SingleConnectionRuntime } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { bigIntegerReadOf, nativeTypeLookup } from "../stored_column_types";
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

describe("capturedNativeSchema: reading the right table's catalog", () => {
   it("reads a three-part name's own catalog, not the session's", async () => {
      // Unqualified, `information_schema` is the session's current database
      // (Snowflake, SQL Server) or catalog (Trino, Databricks): a table of
      // the same schema and name there would be read instead.
      const ran: string[] = [];
      for (const dialect of ["snowflake", "sqlserver", "trino", "databricks"]) {
         await capturedNativeSchema({
            run: async (sql) => {
               ran.push(sql);
               return { rows: [] };
            },
            physicalTableName: "DB.SCH.T",
            dialect,
            sourceName: "t",
         });
      }
      expect(ran[0]).toContain(`FROM "DB".information_schema.columns`);
      expect(ran[1]).toContain(`FROM "DB".information_schema.columns`);
      expect(ran[2]).toContain(`FROM "DB".information_schema.columns`);
      expect(ran[3]).toContain("FROM `DB`.information_schema.columns");
   });

   it("captures nothing when the catalog finds the name in more than one schema", async () => {
      // An unqualified name, read through `information_schema`, matches a
      // table of that name in every schema the connection can see; which
      // one the build wrote cannot be told, so none is trusted.
      const rows = (schema: string, type: string) => ({
         table_catalog: "def",
         table_schema: schema,
         table_name: "t",
         column_name: "big",
         data_type: "decimal",
         numeric_precision: type === "wide" ? 38 : 20,
         numeric_scale: 0,
      });
      expect(
         await capturedNativeSchema({
            run: async () => ({
               rows: [rows("native_b", "narrow"), rows("native_a", "wide")],
            }),
            physicalTableName: "t",
            dialect: "snowflake",
            sourceName: "t",
         }),
      ).toBeUndefined();
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

const MYSQL_URL = process.env.PUBLISHER_TEST_MYSQL_URL;

/**
 * Served against live on MySQL, as the served copy reads a colocated
 * big-integer column: the type the build captures, the cast
 * `bigIntegerReadOf` picks from it, and that cast written as the served
 * shape writes it (`v__stored::"<type>"`), run beside the live read.
 */
describe.skipIf(!MYSQL_URL)(
   "capturedNativeSchema on MySQL (PUBLISHER_TEST_MYSQL_URL)",
   () => {
      let mysql: MySQLConnection;
      let runtime: SingleConnectionRuntime;
      beforeAll(async () => {
         const url = new URL(MYSQL_URL as string);
         const config = {
            host: url.hostname,
            port: Number(url.port || 3306),
            user: decodeURIComponent(url.username),
            password: decodeURIComponent(url.password),
         };
         const admin = new MySQLConnection("admin", config);
         for (const db of ["native_a", "native_b"]) {
            await admin.runSQL(`DROP DATABASE IF EXISTS ${db}`);
            await admin.runSQL(`CREATE DATABASE ${db}`);
         }
         // The table the build wrote, in the connection's database.
         await admin.runSQL(
            `CREATE TABLE native_a.t AS SELECT CAST(9007199254740993 AS SIGNED) AS id, CAST(1e30 AS DECIMAL(38,0)) AS big`,
         );
         // A table of the same name in another database on the same server
         // (another environment's build), its columns in another order and
         // narrower.
         await admin.runSQL(
            `CREATE TABLE native_b.t AS SELECT CAST(1 AS DECIMAL(20,0)) AS big, CAST(1 AS SIGNED) AS id`,
         );
         await admin.close();
         mysql = new MySQLConnection("mysql", {
            ...config,
            database: "native_a",
         });
         runtime = new SingleConnectionRuntime({ connection: mysql });
      });
      afterAll(async () => {
         for (const db of ["native_a", "native_b"]) {
            await mysql.runSQL(`DROP DATABASE IF EXISTS ${db}`);
         }
         await mysql.close();
      });

      const capture = () =>
         capturedNativeSchema({
            run: (sql, options) => mysql.runSQL(sql, options),
            physicalTableName: "t",
            dialect: "mysql",
            sourceName: "t",
         });

      it("reads the table the build wrote, not one of its name in another database", async () => {
         const captured = await capture();
         expect(captured).toEqual([
            { name: "id", type: "bigint" },
            { name: "big", type: "decimal(38,0)" },
         ]);
      });

      it("serves each big-integer column exactly as live answers it", async () => {
         const lookup = nativeTypeLookup(await capture());
         for (const column of ["id", "big"]) {
            const field = { type: "number", numberType: "bigint" };
            const read = bigIntegerReadOf(
               field,
               { name: column, type: "DOUBLE" },
               { dialect: "mysql", colocated: true, nativeTypeOf: lookup },
            );
            const live = await runtime
               .loadQuery(`run: mysql.table('t') -> { select: ${column} }`)
               .run();
            if (!read?.castTo) continue; // answered live: nothing to compare
            const served = await runtime
               .loadQuery(
                  `run: mysql.table('t') extend { dimension: v is ${column}::"${read.castTo}" } -> { select: v }`,
               )
               .run();
            expect(String(served.data.value[0].v)).toBe(
               String(live.data.value[0][column]),
            );
         }
      });
   },
);

const TRINO_URL = process.env.PUBLISHER_TEST_TRINO_URL;

/** Served against live on Trino, as the MySQL block above. */
describe.skipIf(!TRINO_URL)(
   "capturedNativeSchema on Trino (PUBLISHER_TEST_TRINO_URL, a memory catalog)",
   () => {
      let trino: TrinoConnection;
      let runtime: SingleConnectionRuntime;
      beforeAll(async () => {
         trino = new TrinoConnection("trino", undefined, {
            server: TRINO_URL,
            catalog: "memory",
            schema: "default",
            user: "test",
         });
         await trino.runSQL(`DROP TABLE IF EXISTS memory.native_types.t`);
         await trino.runSQL(`DROP SCHEMA IF EXISTS memory.native_types`);
         await trino.runSQL(`CREATE SCHEMA memory.native_types`);
         await trino.runSQL(
            `CREATE TABLE memory.native_types.t AS SELECT CAST(9007199254740993 AS BIGINT) AS id, 'x' AS g`,
         );
         runtime = new SingleConnectionRuntime({ connection: trino });
      });
      afterAll(async () => {
         await trino.runSQL(`DROP TABLE IF EXISTS memory.native_types.t`);
         await trino.runSQL(`DROP SCHEMA IF EXISTS memory.native_types`);
         await trino.close();
      });

      it("reads Trino's catalog, so a big integer is served, exactly as live", async () => {
         const captured = await capturedNativeSchema({
            run: (sql, options) => trino.runSQL(sql, options),
            physicalTableName: "memory.native_types.t",
            dialect: "trino",
            sourceName: "t",
         });
         expect(captured).toEqual([
            { name: "id", type: "bigint" },
            { name: "g", type: "varchar(1)" },
         ]);
         const read = bigIntegerReadOf(
            { type: "number", numberType: "bigint" },
            { name: "id", type: "DOUBLE" },
            {
               dialect: "trino",
               colocated: true,
               nativeTypeOf: nativeTypeLookup(captured),
            },
         );
         expect(read?.castTo).toBe("bigint");
         const table = `trino.table('memory.native_types.t')`;
         const live = await runtime
            .loadQuery(`run: ${table} -> { select: id }`)
            .run();
         const served = await runtime
            .loadQuery(
               `run: ${table} extend { dimension: v is id::"${read?.castTo}" } -> { select: v }`,
            )
            .run();
         expect(String(served.data.value[0].v)).toBe(
            String(live.data.value[0].id),
         );
      });
   },
);
