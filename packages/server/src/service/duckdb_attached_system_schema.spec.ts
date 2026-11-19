// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * An attached database's Postgres system schemas (`pg_catalog`,
 * `information_schema`, `pg_toast`, the `pg_temp_N` schemas) are refused in
 * caller and author SQL at all three enforcement points: the raw SQL route
 * (assertSingleSelect), the load-time author SQL check (authorSqlRefusals),
 * and the statement gate on a locked instance. DuckDB's own
 * `information_schema` and `pg_catalog`, read unqualified or through its
 * `system` catalog, and an attached database's own tables still read.
 *
 * The gate cases run against an attached in-memory DuckDB database: the gate
 * refuses by name before DuckDB resolves anything, so no Postgres is needed
 * here (the Postgres-gated integration spec covers a real one).
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { BadRequestError } from "../errors";
import {
   attachedSystemSchemaIn,
   introspectionCallInParse,
   introspectionNameIn,
   isAttachedSystemSchema,
} from "./duckdb_introspection";
import {
   authorSqlRefusals,
   installDuckDBStatementLockdown,
   lockDownDuckDBSession,
   networkedDuckDBOptions,
} from "./duckdb_lockdown";
import { assertSingleSelect, serializeSQL } from "./single_select";

/** Each refused statement, and the `catalog.schema` it is refused for. */
const REFUSED: [string, string][] = [
   ["SELECT * FROM pg.pg_catalog.pg_authid", "pg.pg_catalog"],
   ["SELECT * FROM pg.pg_catalog.pg_stat_activity", "pg.pg_catalog"],
   ["SELECT name, setting FROM pg.pg_catalog.pg_settings", "pg.pg_catalog"],
   ["SELECT * FROM pg.information_schema.tables", "pg.information_schema"],
   ["SELECT * FROM pg.pg_toast.pg_toast_2619", "pg.pg_toast"],
   ["SELECT * FROM pg.pg_temp_3.t", "pg.pg_temp_3"],
   // Quoted and case-varied.
   ['SELECT * FROM "pg"."pg_catalog"."pg_authid"', "pg.pg_catalog"],
   ['SELECT * FROM "PG"."Pg_Catalog".PG_AUTHID', "pg.pg_catalog"],
   ["SELECT * FROM Pg.INFORMATION_SCHEMA.Columns", "pg.information_schema"],
   // Whitespace and comments around the dots.
   ["SELECT * FROM pg . pg_catalog . pg_authid", "pg.pg_catalog"],
   ["SELECT * FROM pg/* c */./* c */pg_catalog.pg_authid", "pg.pg_catalog"],
   ["SELECT * FROM pg--c\n.pg_catalog.pg_authid", "pg.pg_catalog"],
   // FROM-first, a CTE, a subquery, a join, a scalar subquery.
   ["FROM pg.pg_catalog.pg_authid", "pg.pg_catalog"],
   [
      "WITH x AS (SELECT * FROM pg.information_schema.tables) SELECT * FROM x",
      "pg.information_schema",
   ],
   [
      "SELECT * FROM (SELECT * FROM app.pg_catalog.pg_settings) AS s",
      "app.pg_catalog",
   ],
   [
      "SELECT * FROM app.public.orders o JOIN app.pg_catalog.pg_user u ON true",
      "app.pg_catalog",
   ],
   [
      "SELECT (SELECT count(*) FROM app.pg_catalog.pg_stat_activity) AS n",
      "app.pg_catalog",
   ],
   // SUMMARIZE and DESCRIBE, which DuckDB serializes as a SELECT.
   ["SUMMARIZE pg.pg_catalog.pg_authid", "pg.pg_catalog"],
   ["DESCRIBE pg.pg_catalog.pg_authid", "pg.pg_catalog"],
   // A DuckLake catalog's metadata alias, already refused by its prefix.
   [
      "SELECT * FROM __ducklake_metadata_lake.pg_catalog.pg_authid",
      "__ducklake_metadata_lake",
   ],
];

/** Reads that must still pass every check. */
const ALLOWED = [
   // An attached database's own tables.
   "SELECT * FROM app.public.orders",
   "SELECT * FROM app.main.t",
   'SELECT * FROM "app"."public"."orders"',
   // A user table whose name merely starts with pg_.
   "SELECT * FROM app.public.pg_lookalike",
   // DuckDB's own information_schema and pg_catalog, unqualified or through
   // its system catalog, as Publisher's connection browsing reads them.
   "SELECT DISTINCT schema_name, catalog_name FROM information_schema.schemata ORDER BY catalog_name, schema_name",
   "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_catalog = 'app'",
   "SELECT count(*) AS n FROM system.information_schema.tables",
   "SELECT count(*) AS n FROM pg_catalog.pg_class",
   "SELECT count(*) AS n FROM system.pg_catalog.pg_namespace",
   // A string or column that merely holds the words.
   "SELECT 'pg_catalog' AS s",
   "SELECT t.pg_catalog_rows FROM app.main.t AS t",
];

describe("the rule", () => {
   it("names an attached catalog's Postgres system schema, never DuckDB's own", () => {
      expect(isAttachedSystemSchema("pg", "pg_catalog")).toBe(true);
      expect(isAttachedSystemSchema("pg", "information_schema")).toBe(true);
      expect(isAttachedSystemSchema("pg", "pg_toast")).toBe(true);
      expect(isAttachedSystemSchema("pg", "pg_toast_temp_1")).toBe(true);
      expect(isAttachedSystemSchema("system", "information_schema")).toBe(
         false,
      );
      expect(isAttachedSystemSchema("system", "pg_catalog")).toBe(false);
      expect(isAttachedSystemSchema("", "pg_catalog")).toBe(false);
      expect(isAttachedSystemSchema("pg", "public")).toBe(false);
      expect(isAttachedSystemSchema("pg", "pg_lookalike")).toBe(false);
   });

   it.each(REFUSED)("finds %p lexically or in DuckDB's parse", async (sql) => {
      const parsed = (await serializeSQL(sql)).statements;
      expect(
         introspectionNameIn(sql) ?? introspectionCallInParse(parsed),
      ).toBeDefined();
   });

   it.each(ALLOWED)("finds nothing in %p", async (sql) => {
      expect(attachedSystemSchemaIn(sql)).toBeUndefined();
      expect(introspectionNameIn(sql)).toBeUndefined();
      expect(
         introspectionCallInParse((await serializeSQL(sql)).statements),
      ).toBeUndefined();
   });
});

describe("raw SQL (the route's single-SELECT guard)", () => {
   it.each(REFUSED)(
      "refuses %p with a 400 naming the schema",
      async (sql, name) => {
         const refused = assertSingleSelect(
            "duck",
            sql,
            "locked-down DuckDB connection",
         );
         await expect(refused).rejects.toBeInstanceOf(BadRequestError);
         // The lexical read names what it sees: a comment between the parts
         // can stand in for the catalog, so only the schema is certain.
         await expect(refused).rejects.toThrow(
            new RegExp(`may not name '[^']*${name.split(".").pop()}'`),
         );
      },
   );

   it.each(ALLOWED)("lets %p through", async (sql) => {
      await assertSingleSelect("duck", sql, "locked-down DuckDB connection");
   });
});

describe("author SQL (the load check)", () => {
   const model = (sources: unknown[]) => ({ dialect: "duckdb", sources });
   const sqlSource = (selectStr: string) => ({
      type: "sql_select",
      selectStr,
   });
   const table = (tablePath: string) => ({ type: "table", tablePath });
   const expression = (src: string[]) => ({
      type: "table",
      tablePath: "app.main.t",
      fields: [{ e: { node: "genericSQLExpr", src, kids: { args: [] } } }],
   });

   it.each(REFUSED)("refuses a .sql() source reading %p", async (sql) => {
      const refusals = await authorSqlRefusals(
         "m.malloy",
         model([sqlSource(sql)]),
      );
      expect(refusals).toHaveLength(1);
      expect(refusals[0]).toContain("author SQL may not use");
   });

   it("refuses a .table() path and a raw SQL expression, however spelled", async () => {
      for (const source of [
         table("pg.pg_catalog.pg_authid"),
         table('"PG"."Information_Schema"."tables"'),
         table("pg /* c */ . pg_catalog . pg_settings"),
         expression(["(SELECT max(rolname) FROM pg.pg_catalog.pg_authid)"]),
         expression([
            '(SELECT count(*) FROM "pg"."information_schema"."columns")',
         ]),
      ]) {
         const refusals = await authorSqlRefusals("m.malloy", model([source]));
         expect(refusals).toHaveLength(1);
         expect(refusals[0]).toContain("author SQL may not use");
      }
   });

   it("loads an attached database's own tables and DuckDB's own catalog", async () => {
      expect(
         await authorSqlRefusals(
            "m.malloy",
            model([
               table("app.public.orders"),
               table("app.public.pg_lookalike"),
               ...ALLOWED.map(sqlSource),
            ]),
         ),
      ).toEqual([]);
   });
});

describe("the gate on a locked instance with an attached database", () => {
   let connection: DuckDBConnection;

   const refused = async (sql: string): Promise<string> => {
      try {
         await connection.runSQL(sql);
      } catch (error) {
         return error instanceof Error ? error.message : String(error);
      }
      throw new Error(`expected the gate to refuse: ${sql}`);
   };

   beforeAll(async () => {
      installDuckDBStatementLockdown();
      connection = new DuckDBConnection({
         name: "attached_conn",
         databasePath: ":memory:",
         ...networkedDuckDBOptions("attached", "system-schema-spec"),
      });
      await connection.runSQL("ATTACH ':memory:' AS app");
      await connection.runSQL("CREATE SCHEMA app.public");
      await connection.runSQL(
         "CREATE TABLE app.public.orders AS SELECT range AS id FROM range(3)",
      );
      await connection.runSQL(
         "CREATE TABLE app.public.pg_lookalike AS SELECT 1 AS v",
      );
      await connection.runSQL(
         "CREATE TABLE app.main.t AS SELECT 1 AS v, 2 AS pg_catalog_rows",
      );
      await lockDownDuckDBSession(connection);
   });

   afterAll(async () => {
      await connection.close();
   });

   it("refuses each form before DuckDB resolves it, by query and by stream", async () => {
      for (const [sql, name] of [
         ...REFUSED,
         // Statements DuckDB's parse does not serialize, read lexically.
         ["SHOW TABLES FROM pg.pg_catalog", "pg.pg_catalog"],
         [
            "PIVOT pg.pg_catalog.pg_authid ON rolname USING count(*)",
            "pg.pg_catalog",
         ],
         [
            "CREATE TEMP TABLE x AS SELECT * FROM pg/**/.pg_catalog.pg_authid",
            "pg.pg_catalog",
         ],
      ]) {
         const message = await refused(sql);
         expect(message).toContain("locked down");
         expect(message).toContain(`${name.split(".").pop()}'`);
         let streamed: string | undefined;
         try {
            for await (const _row of connection.runSQLStream(sql)) {
               // Drained: a refusal throws before the first row.
            }
         } catch (error) {
            streamed = error instanceof Error ? error.message : String(error);
         }
         expect(streamed).toContain("locked down");
      }
   });

   it("reads the attached database's own tables and DuckDB's own catalog", async () => {
      for (const sql of ALLOWED) await connection.runSQL(sql);
      const { rows } = await connection.runSQL(
         'SELECT count(*) AS n FROM "app"."public"."orders"',
      );
      expect(Number(rows[0]?.n)).toBe(3);
   });

   it("fetches an attached table's schema the way Malloy does", async () => {
      const { schemas, errors } = await connection.fetchSchemaForTables(
         { "app.public.orders": "app.public.orders" },
         {},
      );
      expect(errors).toEqual({});
      expect(
         schemas["app.public.orders"].fields.map((field) => field.name),
      ).toEqual(["id"]);
      const refusedFetch = await connection.fetchSchemaForTables(
         { "pg.pg_catalog.pg_authid": "pg.pg_catalog.pg_authid" },
         {},
      );
      expect(refusedFetch.errors["pg.pg_catalog.pg_authid"]).toContain(
         "locked down",
      );
   });
});
