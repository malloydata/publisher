/// <reference types="bun-types" />

// A server with a DuckDB connection holding an attached Postgres database and
// a DuckLake catalog over the same Postgres: caller and author SQL may not read
// the attached database's Postgres system schemas (`pg_catalog`,
// `information_schema`), through the attached alias or the DuckLake catalog's
// metadata alias, while the attached database's own tables, the connection's
// schema and table listings, and a model over an attached table work as
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
const ENV = "sysschema-env";

describe.skipIf(!PG_URL)(
   "an attached database's Postgres system schemas are refused",
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
      const get = async (suffix: string) => {
         const res = await fetch(api(suffix));
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
         schema = `sys_schemas_${process.pid}`;
         await pg.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
         await pg.query(`CREATE SCHEMA "${schema}"`);
         await pg.query(
            `CREATE TABLE "${schema}".orders AS SELECT g AS id FROM generate_series(1, 3) g`,
         );
         root = mkdtempSync(join(tmpdir(), "sys-schemas-"));
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

         const packages: Record<string, string> = {
            good: `source: orders is duck.table('app.${schema}.orders')\n`,
            by_sql:
               'source: s is duck.sql("SELECT name, setting FROM app.pg_catalog.pg_settings")\n',
            by_table:
               'source: s is duck.table(\'"app"."PG_CATALOG"."pg_stat_activity"\')\n',
            by_expression: [
               "##! experimental.sql_functions",
               `source: s is duck.table('app.${schema}.orders') extend {`,
               '  dimension: d is sql_number("(SELECT count(*) FROM app.information_schema.tables)")',
               "}",
               "",
            ].join("\n"),
            by_query_table:
               "source: s is duck.sql(\"SELECT * FROM query_table('app.pg_' || 'catalog.pg_roles')\")\n",
            by_metadata_alias:
               'source: s is lake.sql("SELECT * FROM __ducklake_metadata_lake.pg_catalog.pg_settings")\n',
         };
         const entries = Object.entries(packages).map(([name, model]) => {
            const source = writeSourcePackage(name, { "m.malloy": model });
            sources.push(source);
            return { name, location: source.dir };
         });
         server = await spawnServer({
            config: {
               frozenConfig: false,
               environments: [
                  {
                     name: ENV,
                     packages: entries,
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

      it("reads the attached database's own tables, raw and through a model", async () => {
         const raw = await post("/connections/duck/sqlQuery", {
            sqlStatement: `SELECT count(*) AS n FROM app."${schema}".orders`,
         });
         expect(raw.status).toBe(200);
         expect(raw.text).toContain("3");
         const lake = await post("/connections/lake/sqlQuery", {
            sqlStatement: "SELECT count(*) AS n FROM lake.main.sales",
         });
         expect(lake.status).toBe(200);
         expect(lake.text).toContain("4");
         const model = await post("/packages/good/models/m.malloy/query", {
            query: "run: orders -> { aggregate: n is count() }",
            compactJson: true,
         });
         expect(model.status).toBe(200);
         expect(model.text).toContain("3");
      });

      it("lists the attached database's schemas and tables, and describes its table", async () => {
         const schemas = await get("/connections/duck/schemas");
         expect(schemas.status).toBe(200);
         const listed = JSON.parse(schemas.text) as {
            name: string;
            isHidden: boolean;
         }[];
         expect(listed.find((s) => s.name === `app.${schema}`)?.isHidden).toBe(
            false,
         );
         // The system schemas are listed, hidden, as before.
         expect(listed.find((s) => s.name === "app.pg_catalog")?.isHidden).toBe(
            true,
         );
         const tables = await get(
            `/connections/duck/schemas/app.${schema}/tables`,
         );
         expect(tables.status).toBe(200);
         expect(tables.text).toContain(`app.${schema}.orders`);
         const table = await get(
            `/connections/duck/schemas/app.${schema}/tables/app.${schema}.orders`,
         );
         expect(table.status).toBe(200);
         expect(table.text).toContain('"id"');
         const lakeSchemas = await get("/connections/lake/schemas");
         expect(lakeSchemas.status).toBe(200);
         expect(lakeSchemas.text).toContain("main");
      });

      it("refuses the system schemas in raw SQL, its schema and its temporary table", async () => {
         const statements: [string, string[]][] = [
            [
               "duck",
               [
                  "SELECT name, setting FROM app.pg_catalog.pg_settings",
                  "SELECT count(*) AS n FROM app.pg_catalog.pg_stat_activity",
                  'SELECT * FROM "app"."PG_CATALOG"."pg_user"',
                  "SELECT * FROM app.information_schema.tables",
                  "WITH x AS (SELECT * FROM app.pg_catalog.pg_roles) SELECT * FROM x",
                  "SELECT * FROM (SELECT * FROM app . information_schema . columns) AS c",
                  "SELECT (SELECT count(*) FROM app.pg_catalog.pg_settings) AS n",
               ],
            ],
            [
               "lake",
               [
                  "SELECT * FROM __ducklake_metadata_lake.pg_catalog.pg_settings",
                  "SELECT * FROM __ducklake_metadata_lake.information_schema.tables",
                  "SELECT name, setting FROM pg_settings",
                  "SELECT * FROM system.pg_catalog.pg_database",
                  "SELECT * FROM query_table('lake.main.' || 'sales')",
               ],
            ],
         ];
         for (const [conn, sqls] of statements) {
            for (const route of [
               "sqlQuery",
               "sqlSource",
               "sqlTemporaryTable",
            ]) {
               for (const sqlStatement of sqls) {
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

      it("fails the load of a model whose author SQL reads them", async () => {
         const status = await (
            await fetch(`${server.baseUrl}/api/v0/status`)
         ).text();
         expect(status).not.toContain(password);
         const loadErrors =
            (
               JSON.parse(status) as {
                  loadErrors?: { package?: string; message?: string }[];
               }
            ).loadErrors ?? [];
         for (const name of [
            "by_sql",
            "by_table",
            "by_expression",
            "by_query_table",
            "by_metadata_alias",
         ]) {
            const failed = loadErrors.find((e) => e.package === name);
            expect(failed?.message).toMatch(/introspection|locked down/);
         }
         expect(loadErrors.find((e) => e.package === "good")).toBeUndefined();
      });

      it("writes no credential to the server log", () => {
         expect(server.output()).not.toContain(password);
      });
   },
);
