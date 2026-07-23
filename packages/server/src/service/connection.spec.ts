import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import sinon from "sinon";
import { components } from "../api";
import {
   buildProxiedSslQuery,
   createEnvironmentConnections,
   resolveProxiedTls,
   testConnectionConfig,
} from "./connection";
import { assembleEnvironmentConnections } from "./connection_config";
import { EnvironmentStore } from "./environment_store";

type ApiConnection = components["schemas"]["Connection"];
type AttachedDatabase = components["schemas"]["AttachedDatabase"];

const hasPostgresCredentials = () =>
   !!(
      process.env.POSTGRES_TEST_HOST &&
      process.env.POSTGRES_TEST_USER &&
      process.env.POSTGRES_TEST_PASSWORD
   );

const hasBigQueryCredentials = () =>
   !!(
      process.env.GOOGLE_APPLICATION_CREDENTIALS &&
      process.env.BIGQUERY_TEST_PROJECT_ID
   );

const hasSnowflakeCredentials = () =>
   !!(
      process.env.SNOWFLAKE_TEST_ACCOUNT &&
      process.env.SNOWFLAKE_TEST_USER &&
      process.env.SNOWFLAKE_TEST_PASSWORD &&
      process.env.SNOWFLAKE_TEST_WAREHOUSE
   );

const hasS3Credentials = () =>
   !!(
      process.env.S3_TEST_ACCESS_KEY_ID && process.env.S3_TEST_SECRET_ACCESS_KEY
   );

const hasGCSCredentials = () =>
   !!(process.env.GCS_TEST_KEY_ID && process.env.GCS_TEST_SECRET);

const readBigQueryServiceAccountJson = async (): Promise<string> =>
   fs.readFile(process.env.GOOGLE_APPLICATION_CREDENTIALS!, "utf-8");

// `BIGQUERY_PUBLIC_DATA_*` env vars are populated by the
// `Setup BigQuery public-data credentials` step in
// `.github/workflows/connection-integration-tests.yml`. Kept separate
// from the existing `BIGQUERY_TEST_*` vars (which point at the
// org-scoped BQ_PRESTO_TRINO_KEY service account) so the two SAs can
// coexist without one overwriting the other.
const hasPublicDataBigQueryCredentials = () =>
   !!(
      process.env.BIGQUERY_PUBLIC_DATA_CREDENTIALS &&
      process.env.BIGQUERY_PUBLIC_DATA_PROJECT_ID
   );

const readPublicDataBigQueryServiceAccountJson = async (): Promise<string> =>
   fs.readFile(process.env.BIGQUERY_PUBLIC_DATA_CREDENTIALS!, "utf-8");

describe("connection integration tests", () => {
   const testEnvironmentPath = path.join(
      process.cwd(),
      "test-environment-connections",
   );
   let createdConnections: DuckDBConnection[] = [];

   beforeEach(async () => {
      await fs.mkdir(testEnvironmentPath, { recursive: true });
   });

   afterEach(async () => {
      sinon.restore();

      for (const conn of createdConnections) {
         try {
            await conn.close();
         } catch (error) {
            console.warn("Error closing connection:", error);
         }
      }
      createdConnections = [];

      const maxRetries = 5;
      const delay = 100;
      let lastError: unknown;

      for (let attempt = 0; attempt < maxRetries; attempt++) {
         try {
            await fs.rm(testEnvironmentPath, { recursive: true, force: true });
            return;
         } catch (error) {
            lastError = error;
            const errnoError = error as NodeJS.ErrnoException;
            if (errnoError.code !== "EBUSY") throw error;
            if (attempt < maxRetries - 1) {
               await new Promise((resolve) =>
                  setTimeout(resolve, delay * Math.pow(2, attempt)),
               );
            }
         }
      }

      if ((lastError as NodeJS.ErrnoException).code !== "EBUSY") {
         throw lastError;
      }
   });

   describe("createEnvironmentConnections", () => {
      describe("DuckDB with PostgreSQL attachment", () => {
         it(
            "should create DuckDB connection with attached PostgreSQL database",
            async () => {
               if (!hasPostgresCredentials()) {
                  console.log(
                     "Skipping: PostgreSQL credentials not configured",
                  );
                  return;
               }

               const postgresAttachment: AttachedDatabase = {
                  name: "pg_test",
                  type: "postgres",
                  postgresConnection: {
                     host: process.env.POSTGRES_TEST_HOST,
                     port: parseInt(process.env.POSTGRES_TEST_PORT || "5432"),
                     userName: process.env.POSTGRES_TEST_USER!,
                     password: process.env.POSTGRES_TEST_PASSWORD!,
                     databaseName: process.env.POSTGRES_TEST_DATABASE,
                  },
               };

               const duckdbConnection: ApiConnection = {
                  name: "duckdb_with_postgres",
                  type: "duckdb",
                  duckdbConnection: { attachedDatabases: [postgresAttachment] },
               };

               const { malloyConnections, apiConnections } =
                  await createEnvironmentConnections(
                     [duckdbConnection],
                     testEnvironmentPath,
                  );

               expect(malloyConnections.size).toBe(1);
               expect(apiConnections.length).toBe(1);

               const connection = malloyConnections.get(
                  "duckdb_with_postgres",
               ) as DuckDBConnection;
               expect(connection).toBeDefined();
               createdConnections.push(connection);

               const databases = await connection.runSQL("SHOW DATABASES");
               const dbNames = databases.rows.map(
                  (row) => Object.values(row)[0],
               );
               expect(dbNames).toContain("pg_test");

               const result = await connection.runSQL(
                  "SELECT 1 as test_value FROM pg_test.information_schema.tables LIMIT 1",
               );
               expect(result.rows).toBeDefined();
            },
            { timeout: 30000 },
         );

         it(
            "should list schemas from attached PostgreSQL database",
            async () => {
               if (!hasPostgresCredentials()) {
                  console.log(
                     "Skipping: PostgreSQL credentials not configured",
                  );
                  return;
               }

               const duckdbConnection: ApiConnection = {
                  name: "duckdb_pg_schemas",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [
                        {
                           name: "pg_schemas",
                           type: "postgres",
                           postgresConnection: {
                              host: process.env.POSTGRES_TEST_HOST,
                              port: parseInt(
                                 process.env.POSTGRES_TEST_PORT || "5432",
                              ),
                              userName: process.env.POSTGRES_TEST_USER!,
                              password: process.env.POSTGRES_TEST_PASSWORD!,
                              databaseName: process.env.POSTGRES_TEST_DATABASE,
                           },
                        },
                     ],
                  },
               };

               const { malloyConnections } = await createEnvironmentConnections(
                  [duckdbConnection],
                  testEnvironmentPath,
               );

               const connection = malloyConnections.get(
                  "duckdb_pg_schemas",
               ) as DuckDBConnection;
               createdConnections.push(connection);

               // listSchemas equivalent
               const result = await connection.runSQL(
                  "SELECT schema_name FROM pg_schemas.information_schema.schemata ORDER BY schema_name",
               );
               expect(result.rows.length).toBeGreaterThan(0);
               const schemaNames = result.rows.map(
                  (row) => Object.values(row)[0] as string,
               );
               expect(schemaNames).toContain("public");
            },
            { timeout: 30000 },
         );

         it(
            "should list tables from attached PostgreSQL database",
            async () => {
               if (!hasPostgresCredentials()) {
                  console.log(
                     "Skipping: PostgreSQL credentials not configured",
                  );
                  return;
               }

               const duckdbConnection: ApiConnection = {
                  name: "duckdb_pg_tables",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [
                        {
                           name: "pg_tables_db",
                           type: "postgres",
                           postgresConnection: {
                              host: process.env.POSTGRES_TEST_HOST,
                              port: parseInt(
                                 process.env.POSTGRES_TEST_PORT || "5432",
                              ),
                              userName: process.env.POSTGRES_TEST_USER!,
                              password: process.env.POSTGRES_TEST_PASSWORD!,
                              databaseName: process.env.POSTGRES_TEST_DATABASE,
                           },
                        },
                     ],
                  },
               };

               const { malloyConnections } = await createEnvironmentConnections(
                  [duckdbConnection],
                  testEnvironmentPath,
               );

               const connection = malloyConnections.get(
                  "duckdb_pg_tables",
               ) as DuckDBConnection;
               createdConnections.push(connection);

               // listTables equivalent
               const result = await connection.runSQL(`
                  SELECT table_schema, table_name, table_type
                  FROM pg_tables_db.information_schema.tables
                  WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                  ORDER BY table_schema, table_name
               `);
               expect(result.rows).toBeDefined();
            },
            { timeout: 30000 },
         );

         it(
            "should get table columns from attached PostgreSQL database",
            async () => {
               if (!hasPostgresCredentials()) {
                  console.log(
                     "Skipping: PostgreSQL credentials not configured",
                  );
                  return;
               }

               const duckdbConnection: ApiConnection = {
                  name: "duckdb_pg_cols",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [
                        {
                           name: "pg_cols",
                           type: "postgres",
                           postgresConnection: {
                              host: process.env.POSTGRES_TEST_HOST,
                              port: parseInt(
                                 process.env.POSTGRES_TEST_PORT || "5432",
                              ),
                              userName: process.env.POSTGRES_TEST_USER!,
                              password: process.env.POSTGRES_TEST_PASSWORD!,
                              databaseName: process.env.POSTGRES_TEST_DATABASE,
                           },
                        },
                     ],
                  },
               };

               const { malloyConnections } = await createEnvironmentConnections(
                  [duckdbConnection],
                  testEnvironmentPath,
               );

               const connection = malloyConnections.get(
                  "duckdb_pg_cols",
               ) as DuckDBConnection;
               createdConnections.push(connection);

               const result = await connection.runSQL(`
                  SELECT column_name, data_type, is_nullable
                  FROM pg_cols.information_schema.columns
                  WHERE table_schema = 'information_schema'
                    AND table_name = 'tables'
                  ORDER BY ordinal_position
               `);
               expect(result.rows.length).toBeGreaterThan(0);
               const columnNames = result.rows.map(
                  (row) => (row as Record<string, unknown>)["column_name"],
               );
               expect(columnNames).toContain("table_name");
               expect(columnNames).toContain("table_schema");
            },
            { timeout: 30000 },
         );

         it(
            "should handle PostgreSQL connection string format",
            async () => {
               if (!hasPostgresCredentials()) {
                  console.log(
                     "Skipping: PostgreSQL credentials not configured",
                  );
                  return;
               }

               const connectionString = `host=${process.env.POSTGRES_TEST_HOST} port=${process.env.POSTGRES_TEST_PORT || "5432"} dbname=${process.env.POSTGRES_TEST_DATABASE} user=${process.env.POSTGRES_TEST_USER} password=${process.env.POSTGRES_TEST_PASSWORD}`;

               const duckdbConnection: ApiConnection = {
                  name: "duckdb_pg_string",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [
                        {
                           name: "pg_conn_string",
                           type: "postgres",
                           postgresConnection: { connectionString },
                        },
                     ],
                  },
               };

               const { malloyConnections } = await createEnvironmentConnections(
                  [duckdbConnection],
                  testEnvironmentPath,
               );

               const connection = malloyConnections.get(
                  "duckdb_pg_string",
               ) as DuckDBConnection;
               createdConnections.push(connection);

               const databases = await connection.runSQL("SHOW DATABASES");
               const dbNames = databases.rows.map(
                  (row) => Object.values(row)[0],
               );
               expect(dbNames).toContain("pg_conn_string");
            },
            { timeout: 30000 },
         );
      });

      describe("DuckDB with BigQuery attachment", () => {
         it(
            "should create DuckDB connection with attached BigQuery database",
            async () => {
               if (!hasBigQueryCredentials()) {
                  console.log("Skipping: BigQuery credentials not configured");
                  return;
               }

               const serviceAccountJson =
                  await readBigQueryServiceAccountJson();

               const duckdbConnection: ApiConnection = {
                  name: "duckdb_with_bigquery",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [
                        {
                           name: "bq_test",
                           type: "bigquery",
                           bigqueryConnection: {
                              defaultProjectId:
                                 process.env.BIGQUERY_TEST_PROJECT_ID!,
                              serviceAccountKeyJson: serviceAccountJson,
                           },
                        },
                     ],
                  },
               };

               const { malloyConnections } = await createEnvironmentConnections(
                  [duckdbConnection],
                  testEnvironmentPath,
               );

               const connection = malloyConnections.get(
                  "duckdb_with_bigquery",
               ) as DuckDBConnection;
               expect(connection).toBeDefined();
               createdConnections.push(connection);

               const databases = await connection.runSQL("SHOW DATABASES");
               const dbNames = databases.rows.map(
                  (row) => Object.values(row)[0],
               );
               expect(dbNames).toContain("bq_test");
            },
            { timeout: 60000 },
         );

         it(
            "should list datasets (schemas) from attached BigQuery database",
            async () => {
               if (
                  !hasBigQueryCredentials() ||
                  !process.env.BIGQUERY_TEST_DATASET
               ) {
                  console.log(
                     "Skipping: GOOGLE_APPLICATION_CREDENTIALS, BIGQUERY_TEST_PROJECT_ID, or BIGQUERY_TEST_DATASET not configured",
                  );
                  return;
               }

               const serviceAccountJson =
                  await readBigQueryServiceAccountJson();

               const duckdbConnection: ApiConnection = {
                  name: "duckdb_bq_schemas",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [
                        {
                           name: "bq_schemas",
                           type: "bigquery",
                           bigqueryConnection: {
                              defaultProjectId:
                                 process.env.BIGQUERY_TEST_PROJECT_ID!,
                              serviceAccountKeyJson: serviceAccountJson,
                           },
                        },
                     ],
                  },
               };

               const { malloyConnections } = await createEnvironmentConnections(
                  [duckdbConnection],
                  testEnvironmentPath,
               );

               const connection = malloyConnections.get(
                  "duckdb_bq_schemas",
               ) as DuckDBConnection;
               createdConnections.push(connection);

               const result = await connection.runSQL(
                  "SELECT schema_name FROM bq_schemas.information_schema.schemata",
               );
               expect(result.rows.length).toBeGreaterThan(0);
               const datasetNames = result.rows.map(
                  (row) => Object.values(row)[0] as string,
               );
               expect(datasetNames).toContain(
                  process.env.BIGQUERY_TEST_DATASET!,
               );
            },
            { timeout: 60000 },
         );

         it(
            "should list tables from attached BigQuery dataset",
            async () => {
               if (
                  !hasBigQueryCredentials() ||
                  !process.env.BIGQUERY_TEST_DATASET
               ) {
                  console.log(
                     "Skipping: GOOGLE_APPLICATION_CREDENTIALS, BIGQUERY_TEST_PROJECT_ID, or BIGQUERY_TEST_DATASET not configured",
                  );
                  return;
               }

               const serviceAccountJson =
                  await readBigQueryServiceAccountJson();

               const duckdbConnection: ApiConnection = {
                  name: "duckdb_bq_tables",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [
                        {
                           name: "bq_tables",
                           type: "bigquery",
                           bigqueryConnection: {
                              defaultProjectId:
                                 process.env.BIGQUERY_TEST_PROJECT_ID!,
                              serviceAccountKeyJson: serviceAccountJson,
                           },
                        },
                     ],
                  },
               };

               const { malloyConnections } = await createEnvironmentConnections(
                  [duckdbConnection],
                  testEnvironmentPath,
               );

               const connection = malloyConnections.get(
                  "duckdb_bq_tables",
               ) as DuckDBConnection;
               createdConnections.push(connection);

               const result = await connection.runSQL(`
                  SELECT table_name, table_type
                  FROM bq_tables.${process.env.BIGQUERY_TEST_DATASET!}.INFORMATION_SCHEMA.TABLES
                  ORDER BY table_name
               `);
               expect(result.rows.length).toBeGreaterThan(0);
            },
            { timeout: 60000 },
         );

         it(
            "should get table columns from attached BigQuery table",
            async () => {
               if (
                  !hasBigQueryCredentials() ||
                  !process.env.BIGQUERY_TEST_TABLE
               ) {
                  console.log(
                     "Skipping: GOOGLE_APPLICATION_CREDENTIALS, BIGQUERY_TEST_PROJECT_ID, or BIGQUERY_TEST_TABLE not configured",
                  );
                  return;
               }

               const serviceAccountJson =
                  await readBigQueryServiceAccountJson();
               const [dataset, table] =
                  process.env.BIGQUERY_TEST_TABLE!.split(".");

               const duckdbConnection: ApiConnection = {
                  name: "duckdb_bq_cols",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [
                        {
                           name: "bq_cols",
                           type: "bigquery",
                           bigqueryConnection: {
                              defaultProjectId:
                                 process.env.BIGQUERY_TEST_PROJECT_ID!,
                              serviceAccountKeyJson: serviceAccountJson,
                           },
                        },
                     ],
                  },
               };

               const { malloyConnections } = await createEnvironmentConnections(
                  [duckdbConnection],
                  testEnvironmentPath,
               );

               const connection = malloyConnections.get(
                  "duckdb_bq_cols",
               ) as DuckDBConnection;
               createdConnections.push(connection);

               // getTable equivalent — fetch column metadata
               const result = await connection.runSQL(`
                  SELECT column_name, data_type, is_nullable
                  FROM bq_cols.${dataset}.INFORMATION_SCHEMA.COLUMNS
                  WHERE table_name = '${table}'
                  ORDER BY ordinal_position
               `);
               expect(result.rows.length).toBeGreaterThan(0);
            },
            { timeout: 60000 },
         );

         it(
            "should query data from attached BigQuery table",
            async () => {
               if (
                  !hasBigQueryCredentials() ||
                  !process.env.BIGQUERY_TEST_TABLE
               ) {
                  console.log(
                     "Skipping: GOOGLE_APPLICATION_CREDENTIALS, BIGQUERY_TEST_PROJECT_ID, or BIGQUERY_TEST_TABLE not configured",
                  );
                  return;
               }

               const serviceAccountJson =
                  await readBigQueryServiceAccountJson();

               const duckdbConnection: ApiConnection = {
                  name: "duckdb_bq_query",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [
                        {
                           name: "bq_query",
                           type: "bigquery",
                           bigqueryConnection: {
                              defaultProjectId:
                                 process.env.BIGQUERY_TEST_PROJECT_ID!,
                              serviceAccountKeyJson: serviceAccountJson,
                           },
                        },
                     ],
                  },
               };

               const { malloyConnections } = await createEnvironmentConnections(
                  [duckdbConnection],
                  testEnvironmentPath,
               );

               const connection = malloyConnections.get(
                  "duckdb_bq_query",
               ) as DuckDBConnection;
               createdConnections.push(connection);

               const result = await connection.runSQL(
                  `SELECT * FROM bq_query.${process.env.BIGQUERY_TEST_TABLE!} LIMIT 1`,
               );
               expect(result.rows).toBeDefined();
               expect(result.rows.length).toBeGreaterThan(0);
            },
            { timeout: 60000 },
         );

         it(
            "should validate BigQuery service account key format",
            async () => {
               await expect(
                  createEnvironmentConnections(
                     [
                        {
                           name: "duckdb_bq_invalid",
                           type: "duckdb",
                           duckdbConnection: {
                              attachedDatabases: [
                                 {
                                    name: "bq_invalid",
                                    type: "bigquery",
                                    bigqueryConnection: {
                                       defaultProjectId: "test-environment",
                                       serviceAccountKeyJson: JSON.stringify({
                                          invalid: "key",
                                       }),
                                    },
                                 },
                              ],
                           },
                        },
                     ],
                     testEnvironmentPath,
                  ),
               ).rejects.toThrow(/Invalid service account key/);
            },
            { timeout: 30000 },
         );
      });

      describe("BigQuery direct connection (public-data)", () => {
         // Single end-to-end check that a real `bigquery`-typed
         // connection authenticated by the BIGQUERY_PUBLIC_DATA_SA
         // service account can actually round-trip a query against
         // bigquery-public-data. Skips locally if the creds aren't
         // set; runs in CI under .github/workflows/connection-integration-tests.yml.
         //
         // Picked `samples.shakespeare` because (a) it's tiny (~6 MB
         // / ~165k rows) so the BigQuery byte-scanned cost is well
         // under the 1 TB/month free tier even across many PR runs,
         // (b) it's a long-stable public table so the assertion stays
         // valid across years of reruns. If this assertion ever fails
         // the cause is almost certainly auth, not data drift.
         it(
            "should query bigquery-public-data via real BigQuery connection",
            async () => {
               if (!hasPublicDataBigQueryCredentials()) {
                  console.log(
                     "Skipping: BIGQUERY_PUBLIC_DATA_CREDENTIALS or BIGQUERY_PUBLIC_DATA_PROJECT_ID not configured",
                  );
                  return;
               }

               const serviceAccountJson =
                  await readPublicDataBigQueryServiceAccountJson();

               const bqConnection: ApiConnection = {
                  name: "bq_public_data",
                  type: "bigquery",
                  bigqueryConnection: {
                     // Billing/auth project (the SA's own project_id);
                     // the queried tables live in bigquery-public-data
                     // and are referenced explicitly in the SQL below.
                     defaultProjectId:
                        process.env.BIGQUERY_PUBLIC_DATA_PROJECT_ID!,
                     serviceAccountKeyJson: serviceAccountJson,
                  },
               };

               const { malloyConnections } = await createEnvironmentConnections(
                  [bqConnection],
                  testEnvironmentPath,
               );

               const connection = malloyConnections.get("bq_public_data");
               expect(connection).toBeDefined();

               try {
                  const result = await connection!.runSQL(
                     "SELECT COUNT(*) AS row_count FROM `bigquery-public-data.samples.shakespeare`",
                  );
                  expect(result.rows.length).toBe(1);
                  // Shakespeare has ~165k rows; bound on both sides so
                  // we catch both "auth succeeded but query returned
                  // nothing" and "got a confusingly large value" failure
                  // modes, while staying tolerant of any minor row-count
                  // jitter Google might introduce.
                  const row = result.rows[0] as Record<string, unknown>;
                  const rowCount = Number(row.row_count);
                  expect(rowCount).toBeGreaterThan(100_000);
                  expect(rowCount).toBeLessThan(200_000);
               } finally {
                  // BigQuery driver holds an HTTP/2 client + auth refresh
                  // state; close it explicitly so we don't leak across
                  // the rest of the test run. (createdConnections is
                  // typed for DuckDBConnection, so we can't use the
                  // shared cleanup array here.)
                  await connection?.close();
               }
            },
            { timeout: 60000 },
         );
      });

      describe("BigQuery package end-to-end (bq-hackernews)", () => {
         // Step 2 of Sagar's bun-setup-fixes ask: not just "does the
         // BigQuery driver round-trip a SQL query" (the previous
         // describe block covers that), but "does the publisher's
         // package-loading path successfully use the BigQuery
         // connection on behalf of a Malloy package."
         //
         // Mechanism: stand up a real EnvironmentStore against a
         // temp publisher.config.json that declares both the BQ
         // connection (with the BIGQUERY_PUBLIC_DATA_SA injected via
         // serviceAccountKeyJson) AND the bq-hackernews package
         // (loaded from credibledata/malloy-samples on GitHub).
         // EnvironmentStore.initialize then: clones the malloy-samples
         // repo, extracts the bigquery-hackernews subdirectory, parses
         // the package's Malloy models, and — crucially — introspects
         // their BigQuery table schemas during model compilation. A
         // successful Package.listModels() call therefore proves the
         // entire package-uses-connection path, not just the bare
         // driver auth.
         //
         // Cost: ~50 MB git clone (one time per test run since
         // publisher_data lives under testEnvironmentPath which the
         // afterEach cleans up) + ~6 MB BQ schema scan. Both well
         // under any meaningful free-tier limit. Test budget: 3 min
         // (clone is ~30-60s on GH runners; compile is ~10s).
         it(
            "should load bq-hackernews package and compile its models via real BQ",
            async () => {
               if (!hasPublicDataBigQueryCredentials()) {
                  console.log(
                     "Skipping: BIGQUERY_PUBLIC_DATA_CREDENTIALS or BIGQUERY_PUBLIC_DATA_PROJECT_ID not configured",
                  );
                  return;
               }

               const serviceAccountJson =
                  await readPublicDataBigQueryServiceAccountJson();

               // Each test gets its own serverRoot so publisher_data
               // doesn't leak across tests; afterEach removes
               // testEnvironmentPath which carries the whole tree
               // away.
               const tempServerRoot = path.join(
                  testEnvironmentPath,
                  "bq-hackernews-pkg-test",
               );
               await fs.mkdir(tempServerRoot, { recursive: true });

               const config = {
                  frozenConfig: false,
                  environments: [
                     {
                        name: "malloy-samples",
                        packages: [
                           {
                              name: "bigquery-hackernews",
                              location:
                                 "https://github.com/credibledata/malloy-samples/tree/main/bigquery-hackernews",
                           },
                        ],
                        connections: [
                           {
                              name: "bigquery",
                              type: "bigquery",
                              bigqueryConnection: {
                                 defaultProjectId:
                                    process.env
                                       .BIGQUERY_PUBLIC_DATA_PROJECT_ID!,
                                 serviceAccountKeyJson: serviceAccountJson,
                              },
                           },
                        ],
                     },
                  ],
               };
               await fs.writeFile(
                  path.join(tempServerRoot, "publisher.config.json"),
                  JSON.stringify(config),
               );

               // Force the load-from-config init path. Without this,
               // EnvironmentStore.initialize() takes the load-from-DB
               // branch and only falls back to config when the DB is
               // empty (which it is here, by construction, but relying
               // on that is brittle if the test pattern is reused).
               //
               // SAFETY PRECONDITION: this flag is the SAME one users
               // opt into via `--init` / `start:init` to wipe persisted
               // storage and re-initialize from config (see CLAUDE.md
               // and `environment_store.ts:158`). It is destructive on
               // a real serverRoot. We only set it here because
               // `tempServerRoot` is a freshly-created empty directory
               // (path.join(testEnvironmentPath, "bq-hackernews-pkg-test")
               // mkdir'd ~20 lines above), so there is nothing to wipe.
               // DO NOT copy this pattern into a test that points
               // EnvironmentStore at a non-empty or shared serverRoot
               // — it will delete state.
               const previousInitializeStorage = process.env.INITIALIZE_STORAGE;
               process.env.INITIALIZE_STORAGE = "true";

               try {
                  const envStore = new EnvironmentStore(tempServerRoot);
                  await envStore.finishedInitialization;

                  // initialize() swallows top-level errors and just calls
                  // markNotReady(), so finishedInitialization always resolves
                  // regardless of success; operationalState is what says it
                  // succeeded. It does NOT imply every configured environment
                  // and package loaded: a load failure is skipped and still
                  // reports "serving", which is why loadErrors exists. Assert
                  // on both, and on the package list below.
                  const status = await envStore.getStatus();
                  expect(status.operationalState).toBe("serving");
                  expect(status.loadErrors).toBeUndefined();

                  const env = await envStore.getEnvironment("malloy-samples");
                  const apiPackages = await env.listPackages();
                  expect(apiPackages.map((p) => p.name)).toContain(
                     "bigquery-hackernews",
                  );

                  const apiConnections = env.listApiConnections();
                  expect(apiConnections.map((c) => c.name)).toContain(
                     "bigquery",
                  );

                  // The actual integration assertion. Package.listModels()
                  // collects compile errors per-model and returns ALL
                  // models in the result (with `error` populated on
                  // failures) — so models.length>0 alone would pass even
                  // if every BQ schema introspection failed. Filter for
                  // models that compiled cleanly: at least one is the
                  // real proof that BQ-on-behalf-of-a-package works.
                  const pkg = await env.getPackage("bigquery-hackernews");
                  const models = await pkg.listModels();
                  const okModels = models.filter((m) => !m.error);
                  if (okModels.length === 0) {
                     console.error(
                        "All bq-hackernews model compilations failed:",
                        models.map((m) => `${m.path}: ${m.error}`),
                     );
                  }
                  expect(okModels.length).toBeGreaterThan(0);
               } finally {
                  if (previousInitializeStorage === undefined) {
                     delete process.env.INITIALIZE_STORAGE;
                  } else {
                     process.env.INITIALIZE_STORAGE = previousInitializeStorage;
                  }
               }
            },
            { timeout: 180000 },
         );
      });

      describe("DuckDB with Snowflake attachment", () => {
         it(
            "should create DuckDB connection with attached Snowflake database",
            async () => {
               if (!hasSnowflakeCredentials()) {
                  console.log("Skipping: Snowflake credentials not configured");
                  return;
               }

               const duckdbConnection: ApiConnection = {
                  name: "duckdb_with_snowflake",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [
                        {
                           name: "sf_test",
                           type: "snowflake",
                           snowflakeConnection: {
                              account: process.env.SNOWFLAKE_TEST_ACCOUNT!,
                              username: process.env.SNOWFLAKE_TEST_USER!,
                              password: process.env.SNOWFLAKE_TEST_PASSWORD!,
                              warehouse: process.env.SNOWFLAKE_TEST_WAREHOUSE!,
                              database: process.env.SNOWFLAKE_TEST_DATABASE,
                              schema: process.env.SNOWFLAKE_TEST_SCHEMA,
                           },
                        },
                     ],
                  },
               };

               const { malloyConnections } = await createEnvironmentConnections(
                  [duckdbConnection],
                  testEnvironmentPath,
               );

               const connection = malloyConnections.get(
                  "duckdb_with_snowflake",
               ) as DuckDBConnection;
               expect(connection).toBeDefined();
               createdConnections.push(connection);

               const databases = await connection.runSQL("SHOW DATABASES");
               const dbNames = databases.rows.map(
                  (row) => Object.values(row)[0],
               );
               expect(dbNames).toContain("sf_test");

               const result = await connection.runSQL(
                  "SELECT * FROM snowflake_query('SELECT 1 as test', 'sf_test_secret')",
               );
               expect(result.rows).toBeDefined();
            },
            { timeout: 30000 },
         );

         it("should validate required Snowflake fields", async () => {
            await expect(
               createEnvironmentConnections(
                  [
                     {
                        name: "duckdb_sf_incomplete",
                        type: "duckdb",
                        duckdbConnection: {
                           attachedDatabases: [
                              {
                                 name: "sf_incomplete",
                                 type: "snowflake",
                                 snowflakeConnection: {
                                    account: "test-account",
                                    username: "test-user",
                                 },
                              },
                           ],
                        },
                     },
                  ],
                  testEnvironmentPath,
               ),
            ).rejects.toThrow(/required/);
         });
      });

      describe("DuckDB with S3 attachment", () => {
         it(
            "should create DuckDB connection with S3 configuration",
            async () => {
               if (!hasS3Credentials()) {
                  console.log("Skipping: S3 credentials not configured");
                  return;
               }

               const duckdbConnection: ApiConnection = {
                  name: "duckdb_with_s3",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [
                        {
                           name: "s3_test",
                           type: "s3",
                           s3Connection: {
                              accessKeyId: process.env.S3_TEST_ACCESS_KEY_ID!,
                              secretAccessKey:
                                 process.env.S3_TEST_SECRET_ACCESS_KEY!,
                              region: process.env.S3_TEST_REGION || "us-east-1",
                           },
                        },
                     ],
                  },
               };

               const { malloyConnections } = await createEnvironmentConnections(
                  [duckdbConnection],
                  testEnvironmentPath,
               );

               const connection = malloyConnections.get(
                  "duckdb_with_s3",
               ) as DuckDBConnection;
               expect(connection).toBeDefined();
               createdConnections.push(connection);

               const secrets = await connection.runSQL(
                  "SELECT name FROM duckdb_secrets()",
               );
               const secretNames = secrets.rows.map(
                  (row) => Object.values(row)[0],
               );
               expect(
                  secretNames.some((name) => String(name).includes("s3")),
               ).toBe(true);
            },
            { timeout: 30000 },
         );

         it(
            "should handle S3 with custom endpoint",
            async () => {
               if (!hasS3Credentials()) {
                  console.log("Skipping: S3 credentials not configured");
                  return;
               }

               const duckdbConnection: ApiConnection = {
                  name: "duckdb_s3_custom",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [
                        {
                           name: "s3_custom",
                           type: "s3",
                           s3Connection: {
                              accessKeyId: process.env.S3_TEST_ACCESS_KEY_ID!,
                              secretAccessKey:
                                 process.env.S3_TEST_SECRET_ACCESS_KEY!,
                              region: "us-east-1",
                              endpoint: "https://s3.custom-endpoint.com",
                           },
                        },
                     ],
                  },
               };

               const { malloyConnections } = await createEnvironmentConnections(
                  [duckdbConnection],
                  testEnvironmentPath,
               );

               const connection = malloyConnections.get(
                  "duckdb_s3_custom",
               ) as DuckDBConnection;
               createdConnections.push(connection);
               expect(connection).toBeDefined();
            },
            { timeout: 30000 },
         );
      });

      describe("DuckDB with GCS attachment", () => {
         it(
            "should create DuckDB connection with GCS configuration",
            async () => {
               if (!hasGCSCredentials()) {
                  console.log("Skipping: GCS credentials not configured");
                  return;
               }

               const duckdbConnection: ApiConnection = {
                  name: "duckdb_with_gcs",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [
                        {
                           name: "gcs_test",
                           type: "gcs",
                           gcsConnection: {
                              keyId: process.env.GCS_TEST_KEY_ID!,
                              secret: process.env.GCS_TEST_SECRET!,
                           },
                        },
                     ],
                  },
               };

               const { malloyConnections } = await createEnvironmentConnections(
                  [duckdbConnection],
                  testEnvironmentPath,
               );

               const connection = malloyConnections.get(
                  "duckdb_with_gcs",
               ) as DuckDBConnection;
               expect(connection).toBeDefined();
               createdConnections.push(connection);

               const secrets = await connection.runSQL(
                  "SELECT name FROM duckdb_secrets()",
               );
               const secretNames = secrets.rows.map(
                  (row) => Object.values(row)[0],
               );
               expect(
                  secretNames.some((name) => String(name).includes("gcs")),
               ).toBe(true);
            },
            { timeout: 30000 },
         );
      });

      describe("DuckDB with multiple attachments", () => {
         it(
            "should attach multiple databases to single DuckDB connection",
            async () => {
               const attachments: AttachedDatabase[] = [];

               if (hasPostgresCredentials()) {
                  attachments.push({
                     name: "pg_multi",
                     type: "postgres",
                     postgresConnection: {
                        host: process.env.POSTGRES_TEST_HOST,
                        port: parseInt(
                           process.env.POSTGRES_TEST_PORT || "5432",
                        ),
                        userName: process.env.POSTGRES_TEST_USER!,
                        password: process.env.POSTGRES_TEST_PASSWORD!,
                        databaseName: process.env.POSTGRES_TEST_DATABASE,
                     },
                  });
               }

               if (hasBigQueryCredentials()) {
                  const serviceAccountJson =
                     await readBigQueryServiceAccountJson();
                  attachments.push({
                     name: "bq_multi",
                     type: "bigquery",
                     bigqueryConnection: {
                        defaultProjectId: process.env.BIGQUERY_TEST_PROJECT_ID!,
                        serviceAccountKeyJson: serviceAccountJson,
                     },
                  });
               }

               if (hasS3Credentials()) {
                  attachments.push({
                     name: "s3_multi",
                     type: "s3",
                     s3Connection: {
                        accessKeyId: process.env.S3_TEST_ACCESS_KEY_ID!,
                        secretAccessKey: process.env.S3_TEST_SECRET_ACCESS_KEY!,
                        region: process.env.S3_TEST_REGION || "us-east-1",
                     },
                  });
               }

               if (attachments.length === 0) {
                  console.log("Skipping: No database credentials configured");
                  return;
               }

               const { malloyConnections } = await createEnvironmentConnections(
                  [
                     {
                        name: "duckdb_multi",
                        type: "duckdb",
                        duckdbConnection: { attachedDatabases: attachments },
                     },
                  ],
                  testEnvironmentPath,
               );

               const connection = malloyConnections.get(
                  "duckdb_multi",
               ) as DuckDBConnection;
               expect(connection).toBeDefined();
               createdConnections.push(connection);

               const databases = await connection.runSQL("SHOW DATABASES");
               const dbNames = databases.rows.map(
                  (row) => Object.values(row)[0],
               );
               attachments.forEach((attachment) => {
                  expect(dbNames).toContain(attachment.name!);
               });
            },
            { timeout: 60000 },
         );
      });

      describe("error handling", () => {
         describe("DuckLake connection type", () => {
            it(
               "should create DuckLake connection",
               async () => {
                  if (!hasPostgresCredentials() || !hasS3Credentials()) {
                     console.log(
                        "Skipping: PostgreSQL and S3 credentials not configured",
                     );
                     return;
                  }

                  const { malloyConnections } =
                     await createEnvironmentConnections(
                        [
                           {
                              name: "ducklake_test",
                              type: "ducklake",
                              ducklakeConnection: {
                                 catalog: {
                                    postgresConnection: {
                                       host: process.env.POSTGRES_TEST_HOST,
                                       port: parseInt(
                                          process.env.POSTGRES_TEST_PORT ||
                                             "5432",
                                       ),
                                       userName:
                                          process.env.POSTGRES_TEST_USER!,
                                       password:
                                          process.env.POSTGRES_TEST_PASSWORD!,
                                       databaseName:
                                          process.env.POSTGRES_TEST_DATABASE,
                                    },
                                 },
                                 storage: {
                                    bucketUrl:
                                       process.env.S3_TEST_BUCKET_URL ||
                                       "s3://test-bucket",
                                    s3Connection: {
                                       accessKeyId:
                                          process.env.S3_TEST_ACCESS_KEY_ID!,
                                       secretAccessKey:
                                          process.env
                                             .S3_TEST_SECRET_ACCESS_KEY!,
                                    },
                                 },
                              },
                           },
                        ],
                        testEnvironmentPath,
                     );

                  const connection = malloyConnections.get(
                     "ducklake_test",
                  ) as DuckDBConnection;
                  createdConnections.push(connection);
                  expect(connection).toBeDefined();

                  // Verify DuckLake database is attached
                  const databases = await connection.runSQL("SHOW DATABASES");
                  const dbNames = databases.rows.map(
                     (row) => Object.values(row)[0],
                  );
                  expect(dbNames).toContain("ducklake_test");
               },
               { timeout: 30000 },
            );

            it("should throw error if DuckLake catalog connection is missing", async () => {
               await expect(
                  createEnvironmentConnections(
                     [
                        {
                           name: "ducklake_no_catalog",
                           type: "ducklake",
                           ducklakeConnection: {
                              storage: {
                                 bucketUrl: "s3://test-bucket",
                                 s3Connection: {
                                    accessKeyId: "test",
                                    secretAccessKey: "test",
                                 },
                              },
                           },
                        } as ApiConnection,
                     ],
                     testEnvironmentPath,
                  ),
               ).rejects.toThrow(
                  /PostgreSQL connection configuration is required/,
               );
            });

            it("should throw error if DuckLake connection config is missing", async () => {
               await expect(
                  createEnvironmentConnections(
                     [
                        {
                           name: "ducklake_missing_config",
                           type: "ducklake",
                        },
                     ],
                     testEnvironmentPath,
                  ),
               ).rejects.toThrow(
                  /DuckLake connection configuration is missing/,
               );
            });
         });

         it("should throw error if DuckDB connection name conflicts with attached database", async () => {
            await expect(
               createEnvironmentConnections(
                  [
                     {
                        name: "conflict_db",
                        type: "duckdb",
                        duckdbConnection: {
                           attachedDatabases: [
                              {
                                 name: "conflict_db",
                                 type: "postgres",
                                 postgresConnection: {
                                    connectionString:
                                       "postgresql://localhost/test",
                                 },
                              },
                           ],
                        },
                     },
                  ],
                  testEnvironmentPath,
               ),
            ).rejects.toThrow(/cannot conflict/);
         });

         it("should throw error if connection name is 'duckdb'", async () => {
            await expect(
               createEnvironmentConnections(
                  [
                     {
                        name: "duckdb",
                        type: "duckdb",
                        duckdbConnection: { attachedDatabases: [] },
                     },
                  ],
                  testEnvironmentPath,
               ),
            ).rejects.toThrow(/'duckdb' is reserved/);
         });

         it("should reject DuckDB connections with no attachments", async () => {
            // Env-level DuckDB connections must declare at least one
            // attached foreign database; the empty-array case is operator
            // confusion (the per-package "duckdb" sandbox already covers
            // the plain-in-memory use case).
            await expect(
               createEnvironmentConnections(
                  [
                     {
                        name: "empty_duckdb",
                        type: "duckdb",
                        duckdbConnection: { attachedDatabases: [] },
                     },
                  ],
                  testEnvironmentPath,
               ),
            ).rejects.toThrow(/has no attached databases/);
         });

         it("should reject unsupported DuckDB connector fields", async () => {
            await expect(
               createEnvironmentConnections(
                  [
                     {
                        name: "duckdb_with_setup_sql",
                        type: "duckdb",
                        duckdbConnection: {
                           attachedDatabases: [],
                           setupSQL: "INSTALL httpfs",
                        },
                     } as unknown as ApiConnection,
                  ],
                  testEnvironmentPath,
               ),
            ).rejects.toThrow(/Unsupported DuckDB connection field/);
         });

         it("should reject environment-authored DuckDB policy fields", async () => {
            await expect(
               createEnvironmentConnections(
                  [
                     {
                        name: "duckdb_with_policy",
                        type: "duckdb",
                        duckdbConnection: {
                           attachedDatabases: [],
                           securityPolicy: "sandboxed",
                        },
                     } as unknown as ApiConnection,
                  ],
                  testEnvironmentPath,
               ),
            ).rejects.toThrow(/Unsupported DuckDB connection field/);
         });

         it("should preserve Snowflake private-key auth options", async () => {
            const { malloyConnections, releaseConnections } =
               await createEnvironmentConnections(
                  [
                     {
                        name: "sf_private_key",
                        type: "snowflake",
                        snowflakeConnection: {
                           account: "test-account",
                           username: "test-user",
                           privateKey:
                              "-----BEGIN PRIVATE KEY-----MIIB-----END PRIVATE KEY-----",
                           warehouse: "test-warehouse",
                        },
                     },
                  ],
                  testEnvironmentPath,
               );

            try {
               const connection = malloyConnections.get(
                  "sf_private_key",
               ) as unknown as { connOptions: Record<string, unknown> };
               expect(connection.connOptions.authenticator).toBe(
                  "SNOWFLAKE_JWT",
               );
               expect(connection.connOptions.privateKey).toContain(
                  "BEGIN PRIVATE KEY",
               );
            } finally {
               await releaseConnections();
            }
         });

         it("should translate Trino Peaka credentials to core extraCredential", () => {
            const assembled = assembleEnvironmentConnections(
               [
                  {
                     name: "trino_peaka",
                     type: "trino",
                     trinoConnection: {
                        server: "https://example.com",
                        port: 443,
                        catalog: "catalog",
                        schema: "schema",
                        user: "user",
                        peakaKey: "peaka-secret",
                     },
                  },
               ],
               testEnvironmentPath,
            );

            expect(
               assembled.pojo.connections.trino_peaka.extraCredential,
            ).toEqual({ peakaKey: "peaka-secret" });
            expect(
               assembled.pojo.connections.trino_peaka.extraConfig,
            ).toBeUndefined();
         });

         it("should validate environment-level BigQuery service account keys", () => {
            expect(() =>
               assembleEnvironmentConnections(
                  [
                     {
                        name: "bq_invalid",
                        type: "bigquery",
                        bigqueryConnection: {
                           defaultProjectId: "test-environment",
                           serviceAccountKeyJson: '{"invalid":"key"}',
                        },
                     },
                  ],
                  testEnvironmentPath,
               ),
            ).toThrow(/missing "type" field/);
         });

         it("should preserve PGSSLMODE for environment-level Postgres", () => {
            const previousPgSslMode = process.env.PGSSLMODE;
            process.env.PGSSLMODE = "require";
            try {
               const assembled = assembleEnvironmentConnections(
                  [
                     {
                        name: "pg_ssl",
                        type: "postgres",
                        postgresConnection: {
                           host: "localhost",
                           port: 5432,
                           userName: "user",
                           password: "pass",
                           databaseName: "db",
                        },
                     },
                  ],
                  testEnvironmentPath,
               );

               expect(
                  assembled.pojo.connections.pg_ssl.connectionString,
               ).toContain("sslmode=require");
            } finally {
               if (previousPgSslMode === undefined) {
                  delete process.env.PGSSLMODE;
               } else {
                  process.env.PGSSLMODE = previousPgSslMode;
               }
            }
         });

         it("should use environment-root-relative file paths for environment-level DuckDB", async () => {
            const insideCsvPath = path.join(testEnvironmentPath, "inside.csv");
            await fs.writeFile(insideCsvPath, "id\n1\n");

            const minimalGcsAttachment = {
               name: "test_gcs",
               type: "gcs" as const,
               gcsConnection: {
                  keyId: "test-key-id",
                  secret: "test-secret",
               },
            };

            const { malloyConnections } = await createEnvironmentConnections(
               [
                  {
                     name: "environment_scoped_duckdb",
                     type: "duckdb",
                     duckdbConnection: {
                        attachedDatabases: [minimalGcsAttachment],
                     },
                  },
               ],
               testEnvironmentPath,
            );

            const connection = malloyConnections.get(
               "environment_scoped_duckdb",
            ) as DuckDBConnection;
            createdConnections.push(connection);

            const assembled = assembleEnvironmentConnections(
               [
                  {
                     name: "environment_scoped_duckdb",
                     type: "duckdb",
                     duckdbConnection: {
                        attachedDatabases: [minimalGcsAttachment],
                     },
                  },
               ],
               testEnvironmentPath,
            );
            expect(
               assembled.pojo.connections.environment_scoped_duckdb
                  .workingDirectory,
            ).toBeUndefined();
            expect(
               assembled.pojo.connections.environment_scoped_duckdb
                  .securityPolicy,
            ).toBeUndefined();

            await expect(
               connection.runSQL("SELECT * FROM read_csv_auto('inside.csv')"),
            ).resolves.toBeDefined();
         });

         it("should keep external access available for federated DuckDB entries", () => {
            const assembled = assembleEnvironmentConnections(
               [
                  {
                     name: "federated_duckdb",
                     type: "duckdb",
                     duckdbConnection: {
                        attachedDatabases: [
                           {
                              name: "pg",
                              type: "postgres",
                              postgresConnection: {
                                 host: "localhost",
                                 port: 5432,
                                 userName: "user",
                                 password: "pass",
                                 databaseName: "db",
                              },
                           },
                        ],
                     },
                  },
               ],
               testEnvironmentPath,
            );

            const entry = assembled.pojo.connections.federated_duckdb;
            expect(entry.securityPolicy).toBeUndefined();
            expect(entry.enableExternalAccess).toBeUndefined();
            expect(entry.allowedDirectories).toBeUndefined();
         });

         it("should keep external access available for MotherDuck entries", () => {
            const assembled = assembleEnvironmentConnections(
               [
                  {
                     name: "md",
                     type: "motherduck",
                     motherduckConnection: {
                        accessToken: "token",
                        database: "db",
                     },
                  },
               ],
               testEnvironmentPath,
            );

            const entry = assembled.pojo.connections.md;
            expect(entry.securityPolicy).toBeUndefined();
            expect(entry.enableExternalAccess).toBeUndefined();
            expect(entry.allowedDirectories).toBeUndefined();
         });

         it("should handle already attached database gracefully", async () => {
            if (!hasPostgresCredentials()) {
               console.log("Skipping: PostgreSQL credentials not configured");
               return;
            }

            const postgresAttachment: AttachedDatabase = {
               name: "pg_duplicate",
               type: "postgres",
               postgresConnection: {
                  host: process.env.POSTGRES_TEST_HOST,
                  port: parseInt(process.env.POSTGRES_TEST_PORT || "5432"),
                  userName: process.env.POSTGRES_TEST_USER!,
                  password: process.env.POSTGRES_TEST_PASSWORD!,
                  databaseName: process.env.POSTGRES_TEST_DATABASE,
               },
            };

            const { malloyConnections } = await createEnvironmentConnections(
               [
                  {
                     name: "duckdb_duplicate_test",
                     type: "duckdb",
                     duckdbConnection: {
                        attachedDatabases: [
                           postgresAttachment,
                           postgresAttachment,
                        ],
                     },
                  },
               ],
               testEnvironmentPath,
            );

            const connection = malloyConnections.get(
               "duckdb_duplicate_test",
            ) as DuckDBConnection;
            createdConnections.push(connection);
            expect(connection).toBeDefined();
         });
      });
   });

   describe("testConnectionConfig", () => {
      it(
         "should successfully test valid PostgreSQL connection",
         async () => {
            if (!hasPostgresCredentials()) {
               console.log("Skipping: PostgreSQL credentials not configured");
               return;
            }

            const result = await testConnectionConfig({
               name: "test_postgres",
               type: "postgres",
               postgresConnection: {
                  host: process.env.POSTGRES_TEST_HOST,
                  port: parseInt(process.env.POSTGRES_TEST_PORT || "5432"),
                  userName: process.env.POSTGRES_TEST_USER!,
                  password: process.env.POSTGRES_TEST_PASSWORD!,
                  databaseName: process.env.POSTGRES_TEST_DATABASE,
               },
            });

            expect(result.status).toBe("ok");
            expect(result.errorMessage).toBe("");
         },
         { timeout: 30000 },
      );

      it(
         "should fail for invalid PostgreSQL credentials",
         async () => {
            const result = await testConnectionConfig({
               name: "test_postgres_invalid",
               type: "postgres",
               postgresConnection: {
                  host: process.env.POSTGRES_TEST_HOST,
                  port: parseInt(process.env.POSTGRES_TEST_PORT || "5432"),
                  userName: "invalid_user",
                  password: "invalid_password",
                  databaseName: "nonexistent",
               },
            });

            expect(result.status).toBe("failed");
            expect(result.errorMessage).toBeDefined();
         },
         { timeout: 30000 },
      );

      it("should fail for missing connection name", async () => {
         const result = await testConnectionConfig({
            name: "",
            type: "postgres",
            postgresConnection: {},
         });

         expect(result.status).toBe("failed");
         expect(result.errorMessage).toContain("name is required");
      });

      // These specs drive real attach failures offline: nothing can listen on
      // localhost port 1 without root, so DuckDB's postgres extension fails
      // with "Connection refused" and echoes the full connection string into
      // the error message. The assertions prove that string reaches the
      // caller redacted (`***`) and never in cleartext.
      describe("errorMessage redaction", () => {
         const leakedPassword = "supersecretpw";

         // The security invariant is unconditional: the cleartext password
         // must never appear (this is also what fails if the fix regresses).
         // The positive redaction marker is asserted only once the DSN has
         // actually reached the message (the attach got as far as the connect
         // refusal, so the host is present). On a runner where a DuckDB
         // extension can't load, the attach fails earlier with a message that
         // carries no DSN to redact, and the marker check would then be
         // meaningless rather than a real failure.
         const expectRedacted = (
            errorMessage: string | undefined,
            marker: string,
         ) => {
            expect(errorMessage).not.toContain(leakedPassword);
            if (
               typeof errorMessage === "string" &&
               errorMessage.includes("127.0.0.1")
            ) {
               expect(errorMessage).toContain(marker);
            }
         };

         afterEach(async () => {
            // testConnectionConfig now removes its own `<name>.duckdb` from
            // process.cwd() in its finally; this is a belt-and-suspenders net
            // in case that cleanup is ever skipped (e.g. a file-lock quirk).
            for (const name of ["redact_pg_url", "redact_pg_kw"]) {
               await fs.rm(path.join(process.cwd(), `${name}.duckdb`), {
                  force: true,
               });
            }
         });

         it(
            "redacts a URL-form DSN in a failed duckdb attached-postgres test",
            async () => {
               const result = await testConnectionConfig({
                  name: "redact_pg_url",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [
                        {
                           name: "redact_pg_url_db",
                           type: "postgres",
                           postgresConnection: {
                              connectionString: `postgres://alice:${leakedPassword}@127.0.0.1:1/mydb`,
                           },
                        },
                     ],
                  },
               });

               expect(result.status).toBe("failed");
               expectRedacted(result.errorMessage, ":***@");
            },
            { timeout: 30000 },
         );

         it(
            "redacts a keyword-form connection string in a failed duckdb attached-postgres test",
            async () => {
               const result = await testConnectionConfig({
                  name: "redact_pg_kw",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [
                        {
                           name: "redact_pg_kw_db",
                           type: "postgres",
                           postgresConnection: {
                              host: "127.0.0.1",
                              port: 1,
                              userName: "alice",
                              password: leakedPassword,
                              databaseName: "mydb",
                           },
                        },
                     ],
                  },
               });

               expect(result.status).toBe("failed");
               expectRedacted(result.errorMessage, "password=***");
            },
            { timeout: 30000 },
         );

         it(
            "redacts the catalog DSN in a failed ducklake connection test",
            async () => {
               const result = await testConnectionConfig({
                  name: "redact_ducklake",
                  type: "ducklake",
                  ducklakeConnection: {
                     catalog: {
                        postgresConnection: {
                           connectionString: `postgres://alice:${leakedPassword}@127.0.0.1:1/mydb`,
                        },
                     },
                     storage: {
                        bucketUrl: "s3://redact-test-bucket",
                        s3Connection: {
                           accessKeyId: "testkey",
                           secretAccessKey: "testsecret",
                        },
                     },
                  },
               });

               expect(result.status).toBe("failed");
               expectRedacted(result.errorMessage, ":***@");
            },
            { timeout: 30000 },
         );
      });

      // testConnectionConfig roots the throwaway config at process.cwd() and
      // the connection name becomes `<name>.duckdb` under it, so a caller
      // supplying a path-traversal name must be rejected before any file is
      // created outside the working directory.
      describe("connection-name path safety", () => {
         it("rejects a path-traversal connection name without touching the filesystem", async () => {
            const traversalName = "../redact_traversal_probe";
            const escapedPath = path.join(
               process.cwd(),
               `${traversalName}.duckdb`,
            );
            try {
               const result = await testConnectionConfig({
                  name: traversalName,
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [
                        {
                           name: "probe_db",
                           type: "postgres",
                           postgresConnection: {
                              connectionString:
                                 "postgres://u:p@127.0.0.1:1/mydb",
                           },
                        },
                     ],
                  },
               });

               expect(result.status).toBe("failed");
               expect(result.errorMessage).toContain("Invalid package name");

               const created = await fs
                  .access(escapedPath)
                  .then(() => true)
                  .catch(() => false);
               expect(created).toBe(false);
            } finally {
               await fs.rm(escapedPath, { force: true });
            }
         });

         it("removes the <name>.duckdb file it created in cwd after a duckdb test", async () => {
            const name = "cleanup_probe_duckdb";
            const dbPath = path.join(process.cwd(), `${name}.duckdb`);
            try {
               const result = await testConnectionConfig({
                  name,
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [
                        {
                           name: "probe_db",
                           type: "postgres",
                           postgresConnection: {
                              connectionString:
                                 "postgres://u:p@127.0.0.1:1/mydb",
                           },
                        },
                     ],
                  },
               });

               // The attach fails (closed port), but the base DuckDB file is
               // opened before that, so without the finally cleanup it would
               // linger. Assert the service removed it.
               expect(result.status).toBe("failed");
               const left = await fs
                  .access(dbPath)
                  .then(() => true)
                  .catch(() => false);
               expect(left).toBe(false);
            } finally {
               await fs.rm(dbPath, { force: true });
            }
         });
      });
   });

   describe("SQL injection prevention", () => {
      it("should properly escape special characters in credentials", async () => {
         if (!hasPostgresCredentials()) {
            console.log("Skipping: PostgreSQL credentials not configured");
            return;
         }

         const { malloyConnections } = await createEnvironmentConnections(
            [
               {
                  name: "duckdb_special_chars",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [
                        {
                           name: "pg_special",
                           type: "postgres",
                           postgresConnection: {
                              host: process.env.POSTGRES_TEST_HOST,
                              port: parseInt(
                                 process.env.POSTGRES_TEST_PORT || "5432",
                              ),
                              userName: process.env.POSTGRES_TEST_USER!,
                              password: process.env.POSTGRES_TEST_PASSWORD!,
                              databaseName: process.env.POSTGRES_TEST_DATABASE,
                           },
                        },
                     ],
                  },
               },
            ],
            testEnvironmentPath,
         );

         const connection = malloyConnections.get(
            "duckdb_special_chars",
         ) as DuckDBConnection;
         createdConnections.push(connection);
         expect(connection).toBeDefined();
      });
   });

   describe("connection attributes", () => {
      it(
         "should return correct attributes for DuckDB connection",
         async () => {
            if (!hasPostgresCredentials()) {
               console.log("Skipping: PostgreSQL credentials not configured");
               return;
            }

            const { apiConnections } = await createEnvironmentConnections(
               [
                  {
                     name: "duckdb_attrs",
                     type: "duckdb",
                     duckdbConnection: {
                        attachedDatabases: [
                           {
                              name: "pg_attrs",
                              type: "postgres",
                              postgresConnection: {
                                 host: process.env.POSTGRES_TEST_HOST,
                                 port: parseInt(
                                    process.env.POSTGRES_TEST_PORT || "5432",
                                 ),
                                 userName: process.env.POSTGRES_TEST_USER!,
                                 password: process.env.POSTGRES_TEST_PASSWORD!,
                                 databaseName:
                                    process.env.POSTGRES_TEST_DATABASE,
                              },
                           },
                        ],
                     },
                  },
               ],
               testEnvironmentPath,
            );

            const connection = apiConnections[0];
            expect(connection.attributes).toBeDefined();
            expect(connection.attributes?.dialectName).toBe("duckdb");
            expect(typeof connection.attributes?.canPersist).toBe("boolean");
            expect(typeof connection.attributes?.canStream).toBe("boolean");
            expect(typeof connection.attributes?.isPool).toBe("boolean");
         },
         { timeout: 30000 },
      );
   });
});

describe("buildProxiedSslQuery", () => {
   const ORIG_CA = process.env.NODE_EXTRA_CA_CERTS;
   afterEach(() => {
      if (ORIG_CA === undefined) delete process.env.NODE_EXTRA_CA_CERTS;
      else process.env.NODE_EXTRA_CA_CERTS = ORIG_CA;
   });

   it("defaults to no-verify when sslmode is unset (force-SSL target isn't rejected for plaintext)", () => {
      expect(buildProxiedSslQuery("conn")).toBe("?sslmode=no-verify");
   });

   it("returns no-verify for explicit no-verify", () => {
      expect(buildProxiedSslQuery("conn", "no-verify")).toBe(
         "?sslmode=no-verify",
      );
   });

   it("emits an explicit sslmode=disable, never an empty query", () => {
      // Regression: an empty query lets pg fall back to the deployment's PGSSLMODE
      // env, which would verify and fail the 127.0.0.1 hostname check.
      expect(buildProxiedSslQuery("conn", "disable")).toBe("?sslmode=disable");
   });

   it("builds a chain-verifying, hostname-skipped query for verify-ca", () => {
      process.env.NODE_EXTRA_CA_CERTS = "/etc/ssl/certs/rds bundle.pem";
      const q = buildProxiedSslQuery("conn", "verify-ca");
      expect(q).toContain("uselibpqcompat=true");
      expect(q).toContain("sslmode=verify-ca");
      expect(q).toContain(
         `sslrootcert=${encodeURIComponent("/etc/ssl/certs/rds bundle.pem")}`,
      );
   });

   it("throws for verify-ca when no CA bundle is available", () => {
      delete process.env.NODE_EXTRA_CA_CERTS;
      expect(() => buildProxiedSslQuery("conn", "verify-ca")).toThrow(
         /no trusted CA bundle/i,
      );
   });

   it("throws for an unsupported sslmode", () => {
      expect(() => buildProxiedSslQuery("conn", "require")).toThrow(
         /unsupported sslmode/i,
      );
   });

   it("verify-ca connectionString parses to an ssl config (pg-connection-string version guard)", async () => {
      // uselibpqcompat=true (pg-connection-string >= 2.7) is what turns verify-ca
      // into "verify chain, skip hostname". If pg is ever downgraded below that
      // floor, sslmode would be ignored — this guard fails loudly instead.
      const caPath = path.join(os.tmpdir(), `proxy-ca-${process.pid}.pem`);
      await fs.writeFile(
         caPath,
         "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n",
      );
      process.env.NODE_EXTRA_CA_CERTS = caPath;
      try {
         const q = buildProxiedSslQuery("conn", "verify-ca");
         const { parse } = await import("pg-connection-string");
         const parsed = parse(`postgresql://127.0.0.1:5432/db${q}`);
         expect(parsed.ssl).toBeTruthy();
         expect(typeof parsed.ssl).toBe("object");
      } finally {
         await fs.rm(caPath, { force: true });
      }
   });
});

describe("resolveProxiedTls", () => {
   it("routes verify-full to a raw ssl object with servername = the real host", () => {
      // verify-full (chain + hostname) can't ride the connectionString through a
      // tunnel — it needs ssl.servername set to the real DB host so pg checks the
      // cert's SAN against it, not against the 127.0.0.1 socket address.
      const { query, ssl } = resolveProxiedTls(
         "conn",
         "db.internal.example.com",
         "verify-full",
      );
      expect(query).toBe("");
      expect(ssl).toEqual({
         servername: "db.internal.example.com",
         rejectUnauthorized: true,
      });
   });

   it("throws for verify-full with no host (servername would be undefined)", () => {
      expect(() => resolveProxiedTls("conn", undefined, "verify-full")).toThrow(
         /verify-full.*no host/i,
      );
   });

   it("routes the connectionString-param modes through buildProxiedSslQuery with no ssl object", () => {
      // verify-ca needs a CA bundle to build its query, so set one for the loop.
      const prior = process.env.NODE_EXTRA_CA_CERTS;
      process.env.NODE_EXTRA_CA_CERTS = "/etc/ssl/certs/bundle.pem";
      try {
         for (const mode of ["disable", "no-verify", "verify-ca", undefined]) {
            const { query, ssl } = resolveProxiedTls("conn", "127.0.0.1", mode);
            expect(query).toBe(buildProxiedSslQuery("conn", mode));
            expect(ssl).toBeUndefined();
         }
      } finally {
         if (prior === undefined) delete process.env.NODE_EXTRA_CA_CERTS;
         else process.env.NODE_EXTRA_CA_CERTS = prior;
      }
   });

   it("verify-full: a bare connectionString does not clobber the explicit ssl object (pg merge guard)", async () => {
      // Load-bearing invariant: pg's ConnectionParameters does
      // Object.assign({}, config, parse(connectionString)). A verify-full
      // connectionString carries no sslmode, so pg-connection-string adds no
      // `ssl` key and our explicit { servername, rejectUnauthorized } survives
      // the merge. If a future pg bump changes this — or we accidentally emit an
      // sslmode into the query — this guard fails loudly instead of silently
      // downgrading verify-full.
      const { query, ssl } = resolveProxiedTls(
         "conn",
         "db.internal.example.com",
         "verify-full",
      );
      const { default: ConnectionParameters } = await import(
         "pg/lib/connection-parameters.js"
      );
      const params = new (ConnectionParameters as unknown as new (
         c: unknown,
      ) => {
         ssl: unknown;
      })({
         connectionString: `postgresql://u:p@127.0.0.1:5432/db${query}`,
         ssl,
      });
      expect(params.ssl).toEqual({
         servername: "db.internal.example.com",
         rejectUnauthorized: true,
      });
   });
});
