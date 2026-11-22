// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Connection } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import type { components } from "../../api";
import { installSettings } from "../../test_helpers/settings_env";
import { getSchemasForConnection, listTablesForSchema } from "../db_utils";

/**
 * Each connection type's schema and table listing, as the registry
 * dispatches it, on a recording Malloy connection: the refusal of a missing
 * config, a configured connection's schema listing under a connection that
 * answers, and the type's own error wrap under one whose every statement
 * fails. The SQL each listing sends is pinned per type in db_utils.spec.ts,
 * so it is not repeated here.
 *
 * No case reaches a network: BigQuery is refused before its client is made,
 * and a `publisher` connection dials a closed local port.
 */

type ApiConnection = components["schemas"]["Connection"];

/**
 * Per type: a configured connection, the schemas it lists from ROW, and the
 * name its error wrap gives the type.
 */
const CONFIGURED: Record<
   string,
   [
      Partial<ApiConnection>,
      { name: string; isHidden: boolean; isDefault: boolean },
      string,
   ]
> = {
   postgres: [
      { postgresConnection: { databaseName: "d" } },
      { name: "s", isHidden: false, isDefault: false },
      "Postgres",
   ],
   snowflake: [
      { snowflakeConnection: { database: "DB", schema: "S" } },
      { name: "cat.s", isHidden: true, isDefault: false },
      "Snowflake",
   ],
   trino: [
      { trinoConnection: { catalog: "cat", schema: "s" } },
      { name: "s", isHidden: false, isDefault: true },
      "Trino",
   ],
   databricks: [
      { databricksConnection: { defaultCatalog: "cat", defaultSchema: "s" } },
      { name: "s", isHidden: false, isDefault: true },
      "Databricks",
   ],
   sqlserver: [
      { sqlserverConnection: { database: "d" } },
      { name: "s", isHidden: false, isDefault: false },
      "SQL Server",
   ],
   duckdb: [
      { duckdbConnection: { attachedDatabases: [] } },
      { name: "cat.s", isHidden: false, isDefault: false },
      "DuckDB",
   ],
   motherduck: [
      { motherduckConnection: { database: "md" } },
      { name: "s", isHidden: false, isDefault: false },
      "MotherDuck",
   ],
   ducklake: [
      {
         ducklakeConnection: {
            catalog: { postgresConnection: {} },
            destinationSchema: "ds",
         },
      },
      { name: "s", isHidden: true, isDefault: false },
      "DuckLake",
   ],
};

/** Per type, the refusal of a connection with no config. */
const MISSING: Record<string, string> = {
   postgres: "Postgres connection is required",
   bigquery: "BigQuery connection is required",
   snowflake: "Snowflake connection is required",
   trino: "Trino connection is required",
   databricks: "Databricks connection is required",
   mysql: "Mysql connection is required",
   sqlserver: "SQL Server connection is required",
   duckdb: "DuckDB connection is required",
   motherduck: "MotherDuck connection is required",
   publisher: 'Publisher connection "probe" is missing connectionUri',
};

/** A row every listing can read: each column under both spellings. */
const ROW = {
   schema_name: "s",
   SCHEMA_NAME: "s",
   catalog_name: "cat",
   CATALOG_NAME: "cat",
   database_name: "db",
   DATABASE_NAME: "db",
   table_name: "t",
   TABLE_NAME: "t",
   table_schema: "s",
   TABLE_SCHEMA: "s",
   column_name: "c",
   COLUMN_NAME: "c",
   data_type: "INTEGER",
   DATA_TYPE: "INTEGER",
   name: "n",
   is_default: false,
   comment: null,
};

function recorder(fail: boolean): { conn: Connection; sql: string[] } {
   const sql: string[] = [];
   const conn = {
      dialectName: "duckdb",
      name: "probe",
      runSQL: async (q: string, options?: unknown) => {
         sql.push(
            `${q.replace(/\s+/g, " ").trim()} ${JSON.stringify(options)}`,
         );
         if (fail) throw new Error("statement refused");
         return { rows: [ROW], totalRows: 1 };
      },
   } as unknown as Connection;
   return { conn, sql };
}

/**
 * The runtime's socket error text differs by platform (Linux prefixes an
 * `ECONNREFUSED: ` code that macOS leaves out), so the golden records it
 * without the code.
 */
function stableMessage(message: string): string {
   return message.replace(/\bE[A-Z]+: (?=Unable to connect)/g, "");
}

async function listing(
   run: (conn: Connection) => Promise<unknown>,
   fail: boolean,
): Promise<unknown> {
   const r = recorder(fail);
   try {
      return { value: await run(r.conn), sql: r.sql };
   } catch (error) {
      return {
         error: stableMessage(
            error instanceof Error ? error.message : String(error),
         ),
         name: error instanceof Error ? error.constructor.name : typeof error,
         sql: r.sql,
      };
   }
}

const probe = (type: string, extra: Partial<ApiConnection> = {}) =>
   ({ name: "probe", type, ...extra }) as ApiConnection;

let restoreSettings: () => void;
beforeAll(() => {
   restoreSettings = installSettings().restore;
});
afterAll(() => restoreSettings());

describe("the introspectors are dispatched by type", () => {
   it("refuses each type's missing config in its own words", async () => {
      for (const [type, message] of Object.entries(MISSING)) {
         expect(
            await listing(
               (conn) => getSchemasForConnection(probe(type), conn),
               false,
            ),
         ).toEqual({ error: message, name: "Error", sql: [] });
      }
   });

   it("lists each configured type's schemas, and wraps its failure in its name", async () => {
      for (const [type, [extra, schema, label]] of Object.entries(CONFIGURED)) {
         const connection = probe(type, extra);
         const answered = (await listing(
            (conn) => getSchemasForConnection(connection, conn),
            false,
         )) as { value: unknown; sql: string[] };
         expect([type, answered.value]).toEqual([type, [schema]]);
         expect(answered.sql).toHaveLength(1);
         const failed = await listing(
            (conn) => getSchemasForConnection(connection, conn),
            true,
         );
         expect([type, failed]).toMatchObject([
            type,
            {
               error: `Failed to get schemas for ${label} connection probe: statement refused`,
            },
         ]);
         const tables = await listing(
            (conn) => listTablesForSchema(connection, "cat.s", conn),
            true,
         );
         expect([type, tables]).toMatchObject([
            type,
            {
               error: `Failed to get tables for ${label} schema cat.s in connection probe: statement refused`,
            },
         ]);
      }
   });

   it("reads MySQL's one schema from its config, sending nothing", async () => {
      expect(
         await listing(
            (conn) =>
               getSchemasForConnection(
                  probe("mysql", { mysqlConnection: { database: "d" } }),
                  conn,
               ),
            true,
         ),
      ).toEqual({
         value: [{ name: "d", isHidden: false, isDefault: true }],
         sql: [],
      });
   });

   it("refuses BigQuery before its client is made", async () => {
      expect(
         await listing(
            (conn) =>
               getSchemasForConnection(
                  probe("bigquery", { bigqueryConnection: {} }),
                  conn,
               ),
            false,
         ),
      ).toEqual({
         error: "Failed to get schemas for BigQuery connection probe: BigQuery connection is required, either set the bigqueryConnection in the connection configuration or set the GOOGLE_APPLICATION_CREDENTIALS environment variable.",
         name: "Error",
         sql: [],
      });
   });

   it("refuses an invalid Trino catalog before any statement", async () => {
      expect(
         await listing(
            (conn) =>
               getSchemasForConnection(
                  probe("trino", { trinoConnection: { catalog: "bad-cat" } }),
                  conn,
               ),
            false,
         ),
      ).toEqual({
         error: 'Invalid catalog name "bad-cat": expected a plain identifier (letters, digits, underscore or dollar, not starting with a digit).',
         name: "InvalidArgumentError",
         sql: [],
      });
   });

   it("asks a publisher connection's dataplane, not the session", async () => {
      const connection = probe("publisher", {
         publisherConnection: {
            connectionUri: "http://user:pw@127.0.0.1:1/connections/c",
            accessToken: "tok",
         },
      });
      expect(
         await listing(
            (conn) => listTablesForSchema(connection, "cat.s", conn),
            false,
         ),
      ).toEqual({
         error: 'Publisher dataplane request to http://127.0.0.1:1/connections/c/schemas/cat.s/tables failed for connection "probe": Unable to connect. Is the computer able to access the url?',
         name: "Error",
         sql: [],
      });
   });

   it("refuses a prototype name and an unknown type", async () => {
      for (const type of ["constructor", "nope"]) {
         expect(
            await listing(
               (conn) => listTablesForSchema(probe(type), "s", conn),
               false,
            ),
         ).toEqual({
            error: `Unsupported connection type: ${type}`,
            name: "Error",
            sql: [],
         });
      }
   });
});
