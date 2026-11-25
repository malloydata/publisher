// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Against a real Postgres: every way Publisher attaches a Postgres database to
// DuckDB (a DuckLake catalog read-only and read-write, an attached database
// given as fields or as a URI, a federated build source) attaches with no
// credential in the attach path, still reads, and refuses catalog
// introspection to caller SQL once locked.
//
// Opt-in: set PUBLISHER_TEST_POSTGRES_URL to a Postgres the test may create
// and drop schemas in.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { Client } from "pg";
import type { components } from "../api";
import {
   attachDuckLakeReadWrite,
   buildEnvironmentMalloyConfig,
} from "./connection";
import { federateSourceForPassthrough } from "./dialects";
import { isInstanceLocked, runInternalSQL } from "./duckdb_lockdown";
import { wrapPassthrough } from "./materialization/run/materialization_build_session";

type ApiConnection = components["schemas"]["Connection"];
type PostgresConnection = components["schemas"]["PostgresConnection"];

const PG_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;

function rowsOf(result: unknown): Record<string, unknown>[] {
   const rows = (result as { rows?: unknown }).rows;
   return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

describe.skipIf(!PG_URL)(
   "Postgres attaches carry no credential in their attach path",
   () => {
      let pg: Client;
      let fields: PostgresConnection;
      let password: string;
      let schema: string;
      let root: string;

      /**
       * What the instance itself says about its attachments, read the way only
       * Publisher may: every attached path, the settings, and the secrets.
       */
      async function instanceText(connection: DuckDBConnection) {
         const read = async (sql: string) =>
            JSON.stringify(rowsOf(await runInternalSQL(connection, sql)));
         return {
            paths: await read(
               "SELECT database_name, path FROM duckdb_databases()",
            ),
            list: await read("PRAGMA database_list"),
            settings: await read("SELECT name, value FROM duckdb_settings()"),
            secrets: await read("SELECT * FROM duckdb_secrets()"),
         };
      }

      async function expectNoCredential(connection: DuckDBConnection) {
         const text = await instanceText(connection);
         for (const value of Object.values(text)) {
            expect(value).not.toContain(password);
         }
         expect(text.paths).not.toContain("user=");
         expect(text.list).not.toContain("user=");
      }

      async function refused(
         connection: DuckDBConnection,
         sql: string,
      ): Promise<string> {
         try {
            await connection.runSQL(sql);
         } catch (error) {
            return error instanceof Error ? error.message : String(error);
         }
         throw new Error(`expected a refusal: ${sql}`);
      }

      beforeAll(async () => {
         const u = new URL(PG_URL!);
         password = decodeURIComponent(u.password);
         fields = {
            host: u.hostname,
            port: Number(u.port || 5432),
            databaseName: u.pathname.slice(1),
            userName: decodeURIComponent(u.username),
            password,
         };
         if (!process.env.PGSSLMODE) fields.sslmode = "disable";
         pg = new Client({ connectionString: PG_URL });
         await pg.connect();
         schema = `attach_secret_${process.pid}`;
         await pg.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
         await pg.query(`CREATE SCHEMA "${schema}"`);
         await pg.query(
            `CREATE TABLE "${schema}".orders AS SELECT g AS id FROM generate_series(1, 3) g`,
         );
         root = mkdtempSync(join(tmpdir(), "pg-attach-secret-"));
         // The catalog, created by a read-write attach as a build makes one.
         const creator = new DuckDBConnection("creator", ":memory:", root);
         try {
            await attachDuckLakeReadWrite(creator, "lake", {
               catalog: { postgresConnection: fields, metadataSchema: schema },
               storage: { bucketUrl: join(root, "data") + "/" },
            });
            await creator.runSQL(
               "CREATE TABLE lake.main.sales AS SELECT range AS id FROM range(4)",
            );
            await expectNoCredential(creator);
         } finally {
            await creator.close();
         }
      });

      afterAll(async () => {
         await pg?.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
         await pg?.end();
         if (root) rmSync(root, { recursive: true, force: true });
      });

      it("a DuckLake catalog connection reads, and its instance shows no credential", async () => {
         const env = buildEnvironmentMalloyConfig(
            [
               {
                  name: "lake",
                  type: "ducklake",
                  ducklakeConnection: {
                     catalog: {
                        postgresConnection: fields,
                        metadataSchema: schema,
                     },
                     storage: { bucketUrl: join(root, "data") + "/" },
                  },
               } as ApiConnection,
            ],
            mkdtempSync(join(root, "env-lake-")),
         );
         try {
            const connection =
               (await env.malloyConfig.connections.lookupConnection(
                  "lake",
               )) as DuckDBConnection;
            expect(isInstanceLocked(connection)).toBe(true);
            expect(
               rowsOf(
                  await connection.runSQL(
                     "SELECT count(*) AS n FROM lake.main.sales",
                  ),
               ),
            ).toEqual([{ n: "4" }]);
            await expectNoCredential(connection);
            for (const sql of [
               "SELECT path FROM duckdb_databases()",
               "SELECT * FROM postgres_query('__ducklake_metadata_lake', 'SELECT 1')",
               `SELECT * FROM __ducklake_metadata_lake."${schema}".ducklake_metadata`,
            ]) {
               expect(await refused(connection, sql)).toContain("locked down");
            }
         } finally {
            await env.releaseConnections();
         }
      });

      it("an attached Postgres database, as fields or as a URI, reads and shows no credential", async () => {
         const u = new URL(PG_URL!);
         u.searchParams.set("sslmode", process.env.PGSSLMODE ?? "disable");
         const env = buildEnvironmentMalloyConfig(
            [
               {
                  name: "duck",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [
                        {
                           name: "app",
                           type: "postgres",
                           postgresConnection: fields,
                        },
                        {
                           name: "appu",
                           type: "postgres",
                           postgresConnection: { connectionString: u.href },
                        },
                     ],
                  },
               } as ApiConnection,
            ],
            mkdtempSync(join(root, "env-duck-")),
         );
         try {
            const connection =
               (await env.malloyConfig.connections.lookupConnection(
                  "duck",
               )) as DuckDBConnection;
            for (const alias of ["app", "appu"]) {
               expect(
                  rowsOf(
                     await connection.runSQL(
                        `SELECT count(*) AS n FROM ${alias}."${schema}".orders`,
                     ),
                  ),
               ).toEqual([{ n: "3" }]);
            }
            await expectNoCredential(connection);
            for (const sql of [
               "SELECT * FROM duckdb_settings()",
               "SELECT * FROM postgres_query('app', 'SELECT 1')",
               "SELECT * FROM postgres_scan('dbname=x', 'public', 't')",
            ]) {
               expect(await refused(connection, sql)).toContain("locked down");
            }
         } finally {
            await env.releaseConnections();
         }
      });

      it("a federated Postgres build source passes through and shows no credential", async () => {
         const session = new DuckDBConnection("federate", ":memory:", root);
         try {
            const { handle } = await federateSourceForPassthrough(
               session,
               "postgres",
               { name: "src", postgresConnection: fields },
            );
            expect(
               rowsOf(
                  await session.runSQL(
                     wrapPassthrough(
                        "postgres",
                        handle,
                        `SELECT count(*) AS n FROM "${schema}".orders`,
                     ),
                  ),
               ).map((row) => Number(row.n)),
            ).toEqual([3]);
            await expectNoCredential(session);
         } finally {
            await session.close();
         }
      });

      it("a refused attach names no credential in its error", async () => {
         const wrong = "not-the-password-xyz";
         const env = buildEnvironmentMalloyConfig(
            [
               {
                  name: "bad",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [
                        {
                           name: "app",
                           type: "postgres",
                           postgresConnection: { ...fields, password: wrong },
                        },
                     ],
                  },
               } as ApiConnection,
            ],
            mkdtempSync(join(root, "env-bad-")),
         );
         try {
            const message = await env.malloyConfig.connections
               .lookupConnection("bad")
               .then(
                  () => "",
                  (error: unknown) =>
                     error instanceof Error ? error.message : String(error),
               );
            expect(message).not.toBe("");
            expect(message).not.toContain(wrong);
         } finally {
            await env.releaseConnections();
         }
      });
   },
);
