// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Connection } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import type { components } from "../../api";
import { installSettings } from "../../test_helpers/settings_env";
import { getSchemasForConnection, listTablesForSchema } from "../db_utils";

/**
 * The schema and table listing the registry took over from db_utils.ts, run
 * for every connection type (and for types no entry names) on a recording
 * Malloy connection, and compared with what the code produced before the
 * move, recorded in introspector.golden.json: each listing's result or
 * refusal, and every statement it sent, under a connection that answers and
 * one whose every statement fails.
 *
 * No case reaches a network: BigQuery is refused before its client is made,
 * and a `publisher` connection dials a closed local port.
 *
 * Re-record only for a reviewed behaviour change:
 * `RECORD_INTROSPECTOR=1 bun test <this file>`.
 */

type ApiConnection = components["schemas"]["Connection"];

const GOLDEN = path.join(__dirname, "introspector.golden.json");

const TYPES = [
   "postgres",
   "bigquery",
   "snowflake",
   "trino",
   "databricks",
   "mysql",
   "sqlserver",
   "duckdb",
   "motherduck",
   "ducklake",
   "publisher",
   "constructor",
   "toString",
   "nope",
];

/** Per type, the configured connections beyond a bare one. */
const CONFIGURED: Record<string, Partial<ApiConnection>[]> = {
   postgres: [{ postgresConnection: { databaseName: "d" } }],
   bigquery: [{ bigqueryConnection: {} }],
   snowflake: [
      { snowflakeConnection: { database: "DB", schema: "S" } },
      { snowflakeConnection: {} },
   ],
   trino: [
      { trinoConnection: { catalog: "cat", schema: "s" } },
      { trinoConnection: { catalog: "bad-cat" } },
   ],
   databricks: [
      { databricksConnection: { defaultCatalog: "cat", defaultSchema: "s" } },
   ],
   mysql: [{ mysqlConnection: { database: "d" } }],
   sqlserver: [{ sqlserverConnection: { database: "d" } }],
   duckdb: [{ duckdbConnection: { attachedDatabases: [] } }],
   motherduck: [{ motherduckConnection: { database: "md" } }],
   ducklake: [
      {
         ducklakeConnection: {
            catalog: { postgresConnection: {} },
            destinationSchema: "ds",
         },
      },
   ],
   publisher: [
      {
         publisherConnection: {
            connectionUri: "http://user:pw@127.0.0.1:1/connections/c",
            accessToken: "tok",
         },
      },
   ],
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

async function listing(
   run: (conn: Connection) => Promise<unknown>,
   fail: boolean,
): Promise<unknown> {
   const r = recorder(fail);
   try {
      return { value: await run(r.conn), sql: r.sql };
   } catch (error) {
      return {
         error: error instanceof Error ? error.message : String(error),
         name: error instanceof Error ? error.constructor.name : typeof error,
         sql: r.sql,
      };
   }
}

function cases(): [string, ApiConnection][] {
   const out: [string, ApiConnection][] = [];
   for (const type of TYPES) {
      const base = { name: "probe", type } as ApiConnection;
      out.push([`${type} bare`, base]);
      (Object.hasOwn(CONFIGURED, type) ? CONFIGURED[type] : []).forEach(
         (extra, i) =>
            out.push([`${type} #${i}`, { ...base, ...extra } as ApiConnection]),
      );
   }
   return out;
}

let restoreSettings: () => void;
beforeAll(() => {
   restoreSettings = installSettings().restore;
});
afterAll(() => restoreSettings());

describe("the introspectors are unchanged by the registry", () => {
   it("lists every type's schemas and tables as before the move", async () => {
      const actual: Record<string, unknown> = {};
      for (const [label, connection] of cases()) {
         for (const fail of [false, true]) {
            const session = fail ? "failing" : "answering";
            actual[`${label} | ${session} | schemas`] = await listing(
               (conn) => getSchemasForConnection(connection, conn),
               fail,
            );
            actual[`${label} | ${session} | tables`] = await listing(
               (conn) => listTablesForSchema(connection, "cat.s", conn),
               fail,
            );
            actual[`${label} | ${session} | named tables`] = await listing(
               (conn) =>
                  listTablesForSchema(connection, "s", conn, ["t", "o'x"]),
               fail,
            );
         }
      }
      if (process.env.RECORD_INTROSPECTOR === "1") {
         fs.writeFileSync(GOLDEN, JSON.stringify(actual, null, 2) + "\n");
      }
      expect(actual).toEqual(JSON.parse(fs.readFileSync(GOLDEN, "utf8")));
   }, 120_000);
});
