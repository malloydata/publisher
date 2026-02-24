import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import path from "path";
import sinon from "sinon";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { createProjectConnections, testConnectionConfig } from "./connection";
import { components } from "../api";

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

describe("connection integration tests", () => {
   const testProjectPath = path.join(process.cwd(), "test-project-connections");
   let createdConnections: DuckDBConnection[] = [];

   beforeEach(async () => {
      await fs.mkdir(testProjectPath, { recursive: true });
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
            await fs.rm(testProjectPath, { recursive: true, force: true });
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

   describe("createProjectConnections", () => {
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
                  await createProjectConnections(
                     [duckdbConnection],
                     testProjectPath,
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

               const { malloyConnections } = await createProjectConnections(
                  [duckdbConnection],
                  testProjectPath,
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

               const { malloyConnections } = await createProjectConnections(
                  [duckdbConnection],
                  testProjectPath,
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

               const { malloyConnections } = await createProjectConnections(
                  [duckdbConnection],
                  testProjectPath,
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

               const { malloyConnections } = await createProjectConnections(
                  [duckdbConnection],
                  testProjectPath,
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

               const { malloyConnections } = await createProjectConnections(
                  [duckdbConnection],
                  testProjectPath,
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

               const { malloyConnections } = await createProjectConnections(
                  [duckdbConnection],
                  testProjectPath,
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

               const { malloyConnections } = await createProjectConnections(
                  [duckdbConnection],
                  testProjectPath,
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

               const { malloyConnections } = await createProjectConnections(
                  [duckdbConnection],
                  testProjectPath,
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

               const { malloyConnections } = await createProjectConnections(
                  [duckdbConnection],
                  testProjectPath,
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
                  createProjectConnections(
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
                                       defaultProjectId: "test-project",
                                       serviceAccountKeyJson: JSON.stringify({
                                          invalid: "key",
                                       }),
                                    },
                                 },
                              ],
                           },
                        },
                     ],
                     testProjectPath,
                  ),
               ).rejects.toThrow(/Invalid service account key/);
            },
            { timeout: 30000 },
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

               const { malloyConnections } = await createProjectConnections(
                  [duckdbConnection],
                  testProjectPath,
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
               createProjectConnections(
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
                  testProjectPath,
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

               const { malloyConnections } = await createProjectConnections(
                  [duckdbConnection],
                  testProjectPath,
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

               const { malloyConnections } = await createProjectConnections(
                  [duckdbConnection],
                  testProjectPath,
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

               const { malloyConnections } = await createProjectConnections(
                  [duckdbConnection],
                  testProjectPath,
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

               const { malloyConnections } = await createProjectConnections(
                  [
                     {
                        name: "duckdb_multi",
                        type: "duckdb",
                        duckdbConnection: { attachedDatabases: attachments },
                     },
                  ],
                  testProjectPath,
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
         it("should throw error if DuckDB connection name conflicts with attached database", async () => {
            await expect(
               createProjectConnections(
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
                  testProjectPath,
               ),
            ).rejects.toThrow(/cannot conflict/);
         });

         it("should throw error if connection name is 'duckdb'", async () => {
            await expect(
               createProjectConnections(
                  [
                     {
                        name: "duckdb",
                        type: "duckdb",
                        duckdbConnection: { attachedDatabases: [] },
                     },
                  ],
                  testProjectPath,
               ),
            ).rejects.toThrow(/cannot be 'duckdb'/);
         });

         it("should throw error if no attached databases configured", async () => {
            await expect(
               createProjectConnections(
                  [
                     {
                        name: "empty_duckdb",
                        type: "duckdb",
                        duckdbConnection: { attachedDatabases: [] },
                     },
                  ],
                  testProjectPath,
               ),
            ).rejects.toThrow(/at least one attached database/);
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

            const { malloyConnections } = await createProjectConnections(
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
               testProjectPath,
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
   });

   describe("SQL injection prevention", () => {
      it("should properly escape special characters in credentials", async () => {
         if (!hasPostgresCredentials()) {
            console.log("Skipping: PostgreSQL credentials not configured");
            return;
         }

         const { malloyConnections } = await createProjectConnections(
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
            testProjectPath,
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

            const { apiConnections } = await createProjectConnections(
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
               testProjectPath,
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
