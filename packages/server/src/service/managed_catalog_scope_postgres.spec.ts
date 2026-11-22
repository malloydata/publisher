// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A managed DuckLake connection whose catalog is also a storage destination,
// over a real Postgres catalog. The managed view's storage secret reaches only
// `main`'s files, but DuckLake answers a table's columns, row count, min and
// max, and an inlined table's rows from the catalog alone. Before the fix a
// model on the managed connection read all of that about the destination's
// `materialized` schema; now each route is refused at compile or load, while
// `main` reads, a schema the view lists in `readableSchemas`, and the
// destination's own reads still answer.
//
// Opt-in: set PUBLISHER_TEST_POSTGRES_URL to a Postgres the test may create
// and drop schemas in.
import { DuckDBInstance } from "@duckdb/node-api";
import { InMemoryURLReader, Runtime } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { Client } from "pg";
import type { components } from "../api";
import { buildEnvironmentMalloyConfig } from "./connection";
import { managedSchemasOf } from "./connection_type";
import { getSchemasForConnection, listTablesForSchema } from "./db_utils";
import { authorSqlRefusals } from "./duckdb_lockdown";

type ApiConnection = components["schemas"]["Connection"];
type Env = ReturnType<typeof buildEnvironmentMalloyConfig>;

const PG_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
const ROOT = "file:///managed-scope/";
const GATED_ROWS = 1713;

describe.skipIf(!PG_URL)(
   "a managed DuckLake connection sharing its catalog with a storage destination",
   () => {
      const schema = `managed_scope_${process.pid}`;
      const root = mkdtempSync(join(tmpdir(), "managed-scope-"));
      const dataPath = join(root, "data") + "/";
      let pg: Client;
      let managed: Env;
      let readable: Env;
      let destination: Env;
      let orders: { n: string; total: string };
      let charges: string;

      beforeAll(async () => {
         const u = new URL(PG_URL!);
         const pgConn = {
            host: u.hostname,
            port: Number(u.port || 5432),
            databaseName: u.pathname.slice(1),
            userName: decodeURIComponent(u.username),
            password: decodeURIComponent(u.password),
         };
         pg = new Client({ connectionString: PG_URL });
         await pg.connect();
         await pg.query(`CREATE SCHEMA "${schema}"`);
         mkdirSync(dataPath, { recursive: true });

         // The host's writer: people's table in main; stored copies in
         // `materialized`, one in files and one small enough to sit inline
         // (the catalog records an inlining limit, as DuckLake defaults to).
         const writer = await DuckDBInstance.create(":memory:");
         const w = await writer.connect();
         await w.run(
            "INSTALL ducklake; LOAD ducklake; INSTALL postgres; LOAD postgres;",
         );
         await w.run(
            `ATTACH 'ducklake:postgres:host=${pgConn.host} port=${pgConn.port} dbname=${pgConn.databaseName} user=${pgConn.userName} password=${pgConn.password}' AS c (DATA_PATH '${dataPath}', METADATA_SCHEMA '${schema}')`,
         );
         await w.run("SET ducklake_default_data_inlining_row_limit = 0");
         await w.run(
            "CREATE TABLE c.main.orders AS SELECT i AS id, i * 25 AS amount FROM range(1, 41) t(i)",
         );
         await w.run("CREATE SCHEMA c.materialized");
         await w.run(
            `CREATE TABLE c.materialized.gated AS SELECT CASE WHEN i % 3 = 0 THEN 'Customer' ELSE 'Subscriber' END AS subscriber_type, i AS trip_count FROM range(${GATED_ROWS}) t(i)`,
         );
         await w.run("CREATE SCHEMA c.stripe");
         await w.run(
            "CREATE TABLE c.stripe.charges AS SELECT i AS id, i * 100 AS cents FROM range(7) t(i)",
         );
         await w.run("CALL c.set_option('data_inlining_row_limit', 10)");
         await w.run(
            "CREATE TABLE c.materialized.tiny AS SELECT 'acct_' || i AS account, i AS balance FROM range(3) t(i)",
         );
         const reader = await w.runAndReadAll(
            "SELECT count(*)::VARCHAR AS n, sum(amount)::VARCHAR AS total FROM c.main.orders",
         );
         orders = reader.getRowObjectsJson()[0] as typeof orders;
         charges = String(
            (
               await w.runAndReadAll(
                  "SELECT count(*)::VARCHAR AS n FROM c.stripe.charges",
               )
            ).getRowObjectsJson()[0].n,
         );
         w.closeSync();
         writer.closeSync();

         const ducklakeConnection = {
            catalog: { postgresConnection: pgConn, metadataSchema: schema },
            storage: { bucketUrl: dataPath },
         };
         const managedRoot = join(root, "env");
         const readableRoot = join(root, "env-readable");
         const destinationRoot = join(root, ".storage-destinations");
         mkdirSync(managedRoot, { recursive: true });
         mkdirSync(readableRoot, { recursive: true });
         mkdirSync(destinationRoot, { recursive: true });
         readable = buildEnvironmentMalloyConfig(
            [
               {
                  name: "lake2",
                  type: "ducklake",
                  managed: true,
                  ducklakeConnection: {
                     ...ducklakeConnection,
                     readableSchemas: ["stripe"],
                  },
               } as ApiConnection,
            ],
            readableRoot,
         );
         managed = buildEnvironmentMalloyConfig(
            [
               {
                  name: "lake",
                  type: "ducklake",
                  managed: true,
                  ducklakeConnection,
               } as ApiConnection,
            ],
            managedRoot,
         );
         destination = buildEnvironmentMalloyConfig(
            [
               {
                  name: "dest",
                  type: "ducklake",
                  ducklakeConnection: {
                     ...ducklakeConnection,
                     destinationSchema: "materialized",
                  },
               } as ApiConnection,
            ],
            destinationRoot,
            false,
            { origin: "storage-destination" },
         );
      });

      afterAll(async () => {
         await managed?.releaseConnections().catch(() => {});
         await readable?.releaseConnections().catch(() => {});
         await destination?.releaseConnections().catch(() => {});
         await pg?.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
         await pg?.end();
         rmSync(root, { recursive: true, force: true });
      });

      function runtime(model: string, env: Env = managed): Runtime {
         return new Runtime({
            urlReader: new InMemoryURLReader(
               new Map([
                  [
                     `${ROOT}m.malloy`,
                     `##! experimental.sql_functions\n${model}`,
                  ],
               ]),
            ),
            connections: env.malloyConfig.connections,
         });
      }

      async function compile(model: string, env: Env = managed) {
         return runtime(model, env)
            .loadModel(new URL(`${ROOT}m.malloy`))
            .getModel();
      }

      /** The load-time check, as a package load asks it. */
      async function loadRefusals(
         model: string,
         env: Env = managed,
      ): Promise<string[]> {
         const compiled = await compile(model, env);
         return authorSqlRefusals("m.malloy", compiled._modelDef, {
            managedSchemas: async (name) =>
               managedSchemasOf(
                  await env.malloyConfig.connections.lookupConnection(name),
               ),
         });
      }

      it("serves main's tables to a model, answering as the catalog does", async () => {
         for (const source of ["o", "q"]) {
            const rows = await runtime(
               `source: o is lake.table('orders')
                source: q is lake.sql("SELECT * FROM main.orders")
                run: ${source} -> { aggregate: n is count(), total is sum(amount) }`,
            )
               .loadQuery(new URL(`${ROOT}m.malloy`))
               .run();
            const row = rows.data.toObject()[0] as Record<string, unknown>;
            expect({ n: String(row.n), total: String(row.total) }).toEqual(
               orders,
            );
         }
         expect(
            await loadRefusals(
               `source: o is lake.table('orders') extend {
                  dimension: doubled is sql_number("\${amount} * 2")
                }`,
            ),
         ).toEqual([]);
      });

      it.each([
         ["another schema's table", "lake.table('materialized.gated')"],
         ["it, qualified", "lake.table('lake.materialized.gated')"],
         ["an inlined table", "lake.table('materialized.tiny')"],
         [
            "the metadata catalog",
            `lake.table('__ducklake_metadata_lake.ducklake_table_column_stats')`,
         ],
         ["information_schema", "lake.table('information_schema.columns')"],
         [
            "a .sql() row count",
            `lake.sql("SELECT count(*) AS n FROM materialized.gated")`,
         ],
         [
            "a .sql() min and max",
            `lake.sql("SELECT min(trip_count) AS lo, max(trip_count) AS hi FROM materialized.gated")`,
         ],
         [
            "a .sql() inlined read",
            `lake.sql("SELECT * FROM lake.materialized.tiny")`,
         ],
         ["a .sql() DESCRIBE", `lake.sql("DESCRIBE materialized.gated")`],
         [
            "a .sql() catalog listing",
            `lake.sql("SELECT * FROM duckdb_columns()")`,
         ],
         [
            "a .sql() row estimate",
            `lake.sql("SELECT table_name, estimated_size FROM duckdb_tables()")`,
         ],
         [
            "a .sql() pg_class",
            `lake.sql("SELECT relname, reltuples FROM pg_class")`,
         ],
         [
            "a .sql() main.duckdb_tables",
            `lake.sql("SELECT table_name, estimated_size FROM main.duckdb_tables")`,
         ],
         [
            "a .sql() quoted, mixed-case catalog view",
            `lake.sql("""SELECT * FROM "Main"."DuckDB_Columns" """)`,
         ],
         [
            "a .sql() main.sqlite_master",
            `lake.sql("SELECT * FROM main.sqlite_master")`,
         ],
         [
            "a .table() main.duckdb_columns",
            "lake.table('main.duckdb_columns')",
         ],
         [
            "a .sql() replacement scan of another schema's files",
            `lake.sql("SELECT count(*) AS n FROM '${dataPath}materialized/**/*.parquet'")`,
         ],
      ])("refuses %s when the model compiles", async (_label, expr) => {
         const failure = await compile(`source: s is ${expr}`).then(
            () => undefined,
            (error: Error) => error.message,
         );
         expect(failure).toContain("reads only its own catalog's tables");
         // Names what the author wrote, never the catalog's internals.
         expect(failure).not.toContain(schema);
         if (!expr.includes(dataPath)) {
            expect(failure).not.toContain(dataPath);
         }
         expect(failure).not.toContain("acct_");
      });

      it.each([
         [
            "a row count",
            `sql_number("(SELECT count(*) FROM materialized.gated)")`,
         ],
         [
            "a max",
            `sql_number("(SELECT max(trip_count) FROM lake.materialized.gated)")`,
         ],
         [
            "an inlined value",
            `sql_string("(SELECT max(account) FROM materialized.tiny)")`,
         ],
         [
            "a catalog listing",
            `sql_number("(SELECT max(estimated_size) FROM duckdb_tables())")`,
         ],
         [
            "main.duckdb_tables",
            `sql_number("(SELECT max(estimated_size) FROM main.duckdb_tables)")`,
         ],
         [
            "another schema's files",
            `sql_number("(SELECT count(*) FROM '${dataPath}materialized/**/*.parquet')")`,
         ],
      ])(
         "refuses a raw SQL expression reading %s when the package loads",
         async (_label, expr) => {
            const refusals = await loadRefusals(
               `source: o is lake.table('orders') extend { dimension: leak is ${expr} }`,
            );
            expect(refusals).toHaveLength(1);
            expect(refusals[0]).toContain(
               "A raw SQL expression on managed connection 'lake'",
            );
         },
      );

      it("lists only main's schema and tables to the managed connection", async () => {
         const api = {
            name: "lake",
            type: "ducklake",
            managed: true,
         } as ApiConnection;
         const conn =
            await managed.malloyConfig.connections.lookupConnection("lake");
         const schemas = await getSchemasForConnection(api, conn);
         expect(schemas.map((s) => s.name)).toEqual(["main"]);
         const tables = await listTablesForSchema(api, "main", conn);
         expect(tables.map((t) => t.resource)).toEqual(["lake.main.orders"]);
         for (const other of ["materialized", "lake.materialized"]) {
            await expect(listTablesForSchema(api, other, conn)).rejects.toThrow(
               "reads only its own catalog's tables",
            );
         }
      });

      it("lets the managed connection's lock open only main's files on a local data path", async () => {
         // The check below the model's: SQL the scope check never saw still
         // cannot open another schema's files.
         const conn = (await managed.malloyConfig.connections.lookupConnection(
            "lake",
         )) as unknown as {
            runSQL(sql: string): Promise<{ rows: Record<string, unknown>[] }>;
         };
         const main = await conn.runSQL(
            `SELECT count(*) AS n FROM read_parquet('${dataPath}main/**/*.parquet')`,
         );
         expect(String(main.rows[0].n)).toBe(orders.n);
         await expect(
            conn.runSQL(
               `SELECT count(*) AS n FROM read_parquet('${dataPath}materialized/**/*.parquet')`,
            ),
         ).rejects.toThrow();
      });

      it.each([
         ["a .table() source", "lake.table('gatd')"],
         ["a .table() source in main.", "lake.table('main.gatd')"],
         ["a .sql() source", `lake.sql("SELECT * FROM gatd")`],
      ])(
         "names no other schema's table when %s misses, at compile",
         async (_label, expr) => {
            const failure = await compile(`source: s is ${expr}`).then(
               () => undefined,
               (error: Error) => error.message,
            );
            expect(failure).toBeString();
            expect(failure).not.toMatch(/did you mean/i);
            expect(failure).not.toContain("materialized");
            expect(failure).not.toContain("gated");
         },
      );

      it("names no other schema's table when a query misses, at run time", async () => {
         const conn = (await managed.malloyConfig.connections.lookupConnection(
            "lake",
         )) as unknown as {
            runSQL(sql: string): Promise<unknown>;
            runSQLStream(sql: string): AsyncIterableIterator<unknown>;
         };
         const failures: string[] = [];
         await conn.runSQL("SELECT * FROM gatd").catch((error: Error) => {
            failures.push(`${error.message}\n${error.stack ?? ""}`);
         });
         try {
            for await (const row of conn.runSQLStream("SELECT * FROM gatd")) {
               void row;
            }
         } catch (error) {
            failures.push(
               `${(error as Error).message}\n${(error as Error).stack ?? ""}`,
            );
         }
         expect(failures).toHaveLength(2);
         for (const failure of failures) {
            expect(failure).toContain("gatd");
            expect(failure).not.toMatch(/did you mean/i);
            expect(failure).not.toContain("materialized");
         }
      });

      it("returns no result schema for another schema's table, though the catalog could describe it", async () => {
         const conn =
            await managed.malloyConfig.connections.lookupConnection("lake");
         const failure = await conn
            .fetchSchemaForSQLStruct(
               {
                  connection: "lake",
                  selectStr: "SELECT * FROM materialized.gated",
               },
               {},
            )
            .then(
               (result) => JSON.stringify(result),
               (error: Error) => error.message,
            );
         expect(failure).toContain("reads only its own catalog's tables");
         expect(failure).not.toContain("trip_count");
         expect(failure).not.toContain("subscriber_type");
      });

      describe("with readableSchemas listing stripe", () => {
         it.each([
            ["bare-qualified", "lake2.table('stripe.charges')"],
            ["alias-qualified", "lake2.table('lake2.stripe.charges')"],
            ["a .sql() read", `lake2.sql("SELECT * FROM stripe.charges")`],
         ])("serves a listed schema's table, %s", async (_label, expr) => {
            const rows = await runtime(
               `source: s is ${expr}
                run: s -> { aggregate: n is count() }`,
               readable,
            )
               .loadQuery(new URL(`${ROOT}m.malloy`))
               .run();
            const row = rows.data.toObject()[0] as Record<string, unknown>;
            expect(String(row.n)).toBe(charges);
         });

         it("passes a raw SQL expression reading a listed schema at load", async () => {
            expect(
               await loadRefusals(
                  `source: o is lake2.table('orders') extend {
                     dimension: c is sql_number("(SELECT count(*) FROM stripe.charges)")
                   }`,
                  readable,
               ),
            ).toEqual([]);
         });

         it.each([
            ["an unlisted schema", "lake2.table('materialized.gated')"],
            ["it, alias-qualified", "lake2.table('lake2.materialized.gated')"],
            [
               "a catalog view in the listed schema",
               `lake2.sql("SELECT * FROM stripe.duckdb_tables")`,
            ],
            [
               "a function qualified with the listed schema",
               `lake2.sql("SELECT stripe.current_schemas(true) AS s")`,
            ],
            [
               "a listed schema's files by path",
               `lake2.sql("SELECT count(*) AS n FROM '${dataPath}stripe/**/*.parquet'")`,
            ],
            ["a DESCRIBE of it", `lake2.sql("DESCRIBE stripe.charges")`],
         ])(
            "still refuses %s when the model compiles",
            async (_label, expr) => {
               const failure = await compile(
                  `source: s is ${expr}`,
                  readable,
               ).then(
                  () => undefined,
                  (error: Error) => error.message,
               );
               expect(failure).toContain("reads only its own catalog's tables");
            },
         );

         it("lists main and the listed schema only", async () => {
            const api = {
               name: "lake2",
               type: "ducklake",
               managed: true,
               ducklakeConnection: { readableSchemas: ["stripe"] },
            } as ApiConnection;
            const conn =
               await readable.malloyConfig.connections.lookupConnection(
                  "lake2",
               );
            const schemas = await getSchemasForConnection(api, conn);
            expect(schemas.map((s) => [s.name, s.isHidden])).toEqual([
               ["main", false],
               ["stripe", false],
            ]);
            const tables = await listTablesForSchema(api, "stripe", conn);
            expect(tables.map((t) => t.resource)).toEqual([
               "lake2.stripe.charges",
            ]);
            for (const other of ["materialized", "lake2.materialized"]) {
               await expect(
                  listTablesForSchema(api, other, conn),
               ).rejects.toThrow("reads only its own catalog's tables");
            }
         });

         it("lets the lock open the listed schema's files and not materialized's", async () => {
            const conn =
               (await readable.malloyConfig.connections.lookupConnection(
                  "lake2",
               )) as unknown as {
                  runSQL(
                     sql: string,
                  ): Promise<{ rows: Record<string, unknown>[] }>;
               };
            const listed = await conn.runSQL(
               `SELECT count(*) AS n FROM read_parquet('${dataPath}stripe/**/*.parquet')`,
            );
            expect(String(listed.rows[0].n)).toBe(charges);
            await expect(
               conn.runSQL(
                  `SELECT count(*) AS n FROM read_parquet('${dataPath}materialized/**/*.parquet')`,
               ),
            ).rejects.toThrow();
         });
      });

      it("leaves the destination's own reads of its schema as they were", async () => {
         const conn =
            (await destination.malloyConfig.connections.lookupConnection(
               "dest",
            )) as unknown as {
               runSQL(
                  sql: string,
               ): Promise<{ rows: Record<string, unknown>[] }>;
            };
         const counted = await conn.runSQL(
            "SELECT count(*) AS n FROM dest.materialized.gated",
         );
         expect(String(counted.rows[0].n)).toBe(String(GATED_ROWS));
         const tiny = await conn.runSQL(
            "SELECT account FROM dest.materialized.tiny ORDER BY account",
         );
         expect(tiny.rows.map((row) => String(row.account))).toEqual([
            "acct_0",
            "acct_1",
            "acct_2",
         ]);
      });
   },
);
