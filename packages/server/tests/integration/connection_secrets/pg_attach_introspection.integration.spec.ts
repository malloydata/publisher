/// <reference types="bun-types" />

// A server with a DuckLake catalog connection and a DuckDB connection with an
// attached Postgres database, both over a real Postgres: every SQL surface
// that reaches their DuckDB instances (raw SQL, its schema and temporary-table
// variants, a model's author SQL, and the MCP query tool) refuses catalog
// introspection with an answer that carries no credential, and reads data as
// before.
//
// Opt-in: set PUBLISHER_TEST_POSTGRES_URL to a Postgres the test may create
// and drop schemas in.

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { Client } from "pg";
import { attachDuckLakeReadWrite } from "../../../src/service/connection";
import {
   spawnServer,
   type SpawnedServer,
   writeSourcePackage,
} from "../../harness/spawn_server";

const PG_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
const ENV = "attach-env";

describe.skipIf(!PG_URL)(
   "catalog introspection is refused on every SQL surface",
   () => {
      let server: SpawnedServer;
      let pg: Client;
      let password: string;
      let schema: string;
      let root: string;
      const sources: { cleanup: () => void }[] = [];

      const api = (suffix: string) =>
         `${server.baseUrl}/api/v0/environments/${ENV}${suffix}`;
      const post = async (suffix: string, body: unknown) => {
         const res = await fetch(api(suffix), {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(body),
         });
         return { status: res.status, text: await res.text() };
      };

      beforeAll(async () => {
         const u = new URL(PG_URL!);
         password = decodeURIComponent(u.password);
         const fields = {
            host: u.hostname,
            port: Number(u.port || 5432),
            databaseName: u.pathname.slice(1),
            userName: decodeURIComponent(u.username),
            password,
            ...(process.env.PGSSLMODE ? {} : { sslmode: "disable" as const }),
         };
         pg = new Client({ connectionString: PG_URL });
         await pg.connect();
         schema = `attach_surface_${process.pid}`;
         await pg.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
         await pg.query(`CREATE SCHEMA "${schema}"`);
         await pg.query(
            `CREATE TABLE "${schema}".orders AS SELECT g AS id FROM generate_series(1, 3) g`,
         );
         root = mkdtempSync(join(tmpdir(), "attach-surface-"));
         const dataPath = join(root, "data") + "/";
         const creator = new DuckDBConnection("creator", ":memory:", root);
         try {
            await attachDuckLakeReadWrite(creator, "lake", {
               catalog: { postgresConnection: fields, metadataSchema: schema },
               storage: { bucketUrl: dataPath },
            });
            await creator.runSQL(
               "CREATE TABLE lake.main.sales AS SELECT range AS id FROM range(4)",
            );
         } finally {
            await creator.close();
         }

         const good = writeSourcePackage("good", {
            "m.malloy": [
               "source: sales is lake.table('main.sales')",
               `source: orders is duck.table('app.${schema}.orders')`,
            ].join("\n"),
         });
         const authored = writeSourcePackage("authored", {
            "m.malloy":
               'source: s is lake.sql("SELECT database_name, path FROM duckdb_databases()")\n',
         });
         sources.push(good, authored);
         server = await spawnServer({
            config: {
               frozenConfig: false,
               environments: [
                  {
                     name: ENV,
                     packages: [
                        { name: "good", location: good.dir },
                        { name: "authored", location: authored.dir },
                     ],
                     connections: [
                        {
                           name: "lake",
                           type: "ducklake",
                           ducklakeConnection: {
                              catalog: {
                                 postgresConnection: fields,
                                 metadataSchema: schema,
                              },
                              storage: { bucketUrl: dataPath },
                           },
                        },
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
                              ],
                           },
                        },
                     ],
                  },
               ],
            },
         });
         await server.waitUntilServing();
      }, 180_000);

      afterAll(async () => {
         await server?.stop();
         for (const source of sources) source.cleanup();
         await pg?.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
         await pg?.end();
         if (root) rmSync(root, { recursive: true, force: true });
      });

      it("reads data through both connections and a model, so the refusals are not vacuous", async () => {
         const lake = await post("/connections/lake/sqlQuery", {
            sqlStatement: "SELECT count(*) AS n FROM lake.main.sales",
         });
         expect(lake.status).toBe(200);
         expect(lake.text).toContain("4");
         const duck = await post("/connections/duck/sqlQuery", {
            sqlStatement: `SELECT count(*) AS n FROM app."${schema}".orders`,
         });
         expect(duck.status).toBe(200);
         expect(duck.text).toContain("3");
         const model = await post("/packages/good/models/m.malloy/query", {
            query: "run: orders -> { aggregate: n is count() }",
            compactJson: true,
         });
         expect(model.status).toBe(200);
         expect(model.text).toContain("3");
      });

      it("refuses introspection in raw SQL, its schema and its temporary table, with no credential in the answer", async () => {
         for (const conn of ["lake", "duck"]) {
            for (const route of [
               "sqlQuery",
               "sqlSource",
               "sqlTemporaryTable",
            ]) {
               for (const sqlStatement of [
                  "SELECT database_name, path FROM duckdb_databases()",
                  "SELECT name, value FROM duckdb_settings()",
                  "SELECT * FROM duckdb_secrets()",
                  "SELECT * FROM postgres_query('app', 'SELECT 1')",
                  "SELECT * FROM __ducklake_metadata_lake.main.ducklake_metadata",
               ]) {
                  const answer = await post(`/connections/${conn}/${route}`, {
                     sqlStatement,
                  });
                  expect(answer.status).toBe(400);
                  expect(answer.text).toContain("may not name");
                  expect(answer.text).not.toContain(password);
               }
            }
         }
      });

      it("refuses a model whose author SQL names introspection, with no credential in the status", async () => {
         const status = await (
            await fetch(`${server.baseUrl}/api/v0/status`)
         ).text();
         expect(status).not.toContain(password);
         const loadErrors = (
            JSON.parse(status) as {
               loadErrors?: { package?: string; message?: string }[];
            }
         ).loadErrors;
         const authored = loadErrors?.find((e) => e.package === "authored");
         expect(authored?.message).toContain("introspection");
      });

      it("answers the MCP query tool from the model and carries no credential", async () => {
         const res = await fetch(`http://127.0.0.1:${server.mcpPort}/mcp`, {
            method: "POST",
            headers: {
               "content-type": "application/json",
               accept: "application/json, text/event-stream",
            },
            body: JSON.stringify({
               jsonrpc: "2.0",
               id: 1,
               method: "tools/call",
               params: {
                  name: "execute_query",
                  arguments: {
                     environmentName: ENV,
                     packageName: "good",
                     modelPath: "m.malloy",
                     query: "run: sales -> { aggregate: n is count() }",
                  },
               },
            }),
         });
         const text = await res.text();
         expect(text).toContain("4");
         expect(text).not.toContain(password);
      });

      it("writes no credential to the server log", () => {
         expect(server.output()).not.toContain(password);
      });
   },
);
