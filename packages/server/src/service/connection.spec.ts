import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import path from "path";
import sinon from "sinon";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { createProjectConnections, testConnectionConfig } from "./connection";
import { components } from "../api";

type ApiConnection = components["schemas"]["Connection"];
type AttachedDatabase = components["schemas"]["AttachedDatabase"];

/**
 * Integration tests for DuckDB connections with attached databases.
 *
 * SETUP REQUIREMENTS:
 * These tests require real database credentials set via environment variables:
 *
 * PostgreSQL:
 *   - POSTGRES_TEST_HOST (default: localhost)
 *   - POSTGRES_TEST_PORT (default: 5432)
 *   - POSTGRES_TEST_USER
 *   - POSTGRES_TEST_PASSWORD
 *   - POSTGRES_TEST_DATABASE
 *
 * BigQuery:
 *   - BIGQUERY_TEST_PROJECT_ID
 *   - BIGQUERY_TEST_SERVICE_ACCOUNT_JSON (full JSON string)
 *
 * Snowflake:
 *   - SNOWFLAKE_TEST_ACCOUNT
 *   - SNOWFLAKE_TEST_USER
 *   - SNOWFLAKE_TEST_PASSWORD
 *   - SNOWFLAKE_TEST_WAREHOUSE
 *   - SNOWFLAKE_TEST_DATABASE (optional)
 *   - SNOWFLAKE_TEST_SCHEMA (optional)
 *
 * S3:
 *   - S3_TEST_ACCESS_KEY_ID
 *   - S3_TEST_SECRET_ACCESS_KEY
 *   - S3_TEST_REGION (default: us-east-1)
 *
 * GCS:
 *   - GCS_TEST_KEY_ID
 *   - GCS_TEST_SECRET
 *
 * Individual tests will be skipped if their required credentials are not available.
 */

describe("connection integration tests", () => {
   const testProjectPath = path.join(process.cwd(), "test-project-connections");
   let createdConnections: DuckDBConnection[] = [];

   beforeEach(async () => {
      await fs.mkdir(testProjectPath, { recursive: true });
   });

   afterEach(async () => {
      sinon.restore();

      // Close all DuckDB connections
      for (const conn of createdConnections) {
         try {
            await conn.close();
         } catch (error) {
            console.warn("Error closing connection:", error);
         }
      }
      createdConnections = [];

      // Clean up test directory with retry for Windows file locking
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
            if (errnoError.code !== "EBUSY") {
               throw error;
            }
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
         const hasPostgresCredentials = () =>
            process.env.POSTGRES_TEST_USER &&
            process.env.POSTGRES_TEST_PASSWORD;

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
                     host: "localhost",
                     port: 5432,
                     userName: process.env.POSTGRES_TEST_USER!,
                     password: process.env.POSTGRES_TEST_PASSWORD!,
                     databaseName: "postgres",
                  },
               };

               const duckdbConnection: ApiConnection = {
                  name: "duckdb_with_postgres",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [postgresAttachment],
                  },
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

               // Verify PostgreSQL database is attached
               const databases = await connection.runSQL("SHOW DATABASES");
               const dbNames = databases.rows.map(
                  (row) => Object.values(row)[0],
               );
               expect(dbNames).toContain("pg_test");

               // Test querying from attached database
               const result = await connection.runSQL(
                  "SELECT 1 as test_value FROM pg_test.information_schema.tables LIMIT 1",
               );
               expect(result.rows).toBeDefined();
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

               const connectionString = `host=${"localhost"} port=${"5432"} dbname=${"postgres"} user=${process.env.POSTGRES_TEST_USER} password=${process.env.POSTGRES_TEST_PASSWORD}`;

               const postgresAttachment: AttachedDatabase = {
                  name: "pg_conn_string",
                  type: "postgres",
                  postgresConnection: {
                     connectionString,
                  },
               };

               const duckdbConnection: ApiConnection = {
                  name: "duckdb_pg_string",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [postgresAttachment],
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
         const hasBigQueryCredentials = () =>
            process.env.BIGQUERY_TEST_PROJECT_ID &&
            process.env.BIGQUERY_TEST_SERVICE_ACCOUNT_JSON;

         it(
            "should create DuckDB connection with attached BigQuery database",
            async () => {
               if (!hasBigQueryCredentials()) {
                  console.log("Skipping: BigQuery credentials not configured");
                  return;
               }

               const bigqueryAttachment: AttachedDatabase = {
                  name: "bq_test",
                  type: "bigquery",
                  bigqueryConnection: {
                     defaultProjectId: process.env.BIGQUERY_TEST_PROJECT_ID!,
                     serviceAccountKeyJson:
                        process.env.BIGQUERY_TEST_SERVICE_ACCOUNT_JSON!,
                  },
               };

               const duckdbConnection: ApiConnection = {
                  name: "duckdb_with_bigquery",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [bigqueryAttachment],
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

               // Verify BigQuery database is attached
               const databases = await connection.runSQL("SHOW DATABASES");
               const dbNames = databases.rows.map(
                  (row) => Object.values(row)[0],
               );
               expect(dbNames).toContain("bq_test");
            },
            { timeout: 30000 },
         );

         it(
            "should validate BigQuery service account key format",
            async () => {
               const invalidKeyAttachment: AttachedDatabase = {
                  name: "bq_invalid",
                  type: "bigquery",
                  bigqueryConnection: {
                     defaultProjectId: "test-project",
                     serviceAccountKeyJson: JSON.stringify({ invalid: "key" }),
                  },
               };

               const duckdbConnection: ApiConnection = {
                  name: "duckdb_bq_invalid",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [invalidKeyAttachment],
                  },
               };

               await expect(
                  createProjectConnections([duckdbConnection], testProjectPath),
               ).rejects.toThrow(/Invalid service account key/);
            },
            { timeout: 30000 },
         );
      });

      describe("DuckDB with Snowflake attachment", () => {
         const hasSnowflakeCredentials = () =>
            process.env.SNOWFLAKE_TEST_ACCOUNT &&
            process.env.SNOWFLAKE_TEST_USER &&
            process.env.SNOWFLAKE_TEST_PASSWORD &&
            process.env.SNOWFLAKE_TEST_WAREHOUSE;

         it(
            "should create DuckDB connection with attached Snowflake database",
            async () => {
               if (!hasSnowflakeCredentials()) {
                  console.log("Skipping: Snowflake credentials not configured");
                  return;
               }

               const snowflakeAttachment: AttachedDatabase = {
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
               };

               const duckdbConnection: ApiConnection = {
                  name: "duckdb_with_snowflake",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [snowflakeAttachment],
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

               // Verify Snowflake database is attached
               const databases = await connection.runSQL("SHOW DATABASES");
               const dbNames = databases.rows.map(
                  (row) => Object.values(row)[0],
               );
               expect(dbNames).toContain("sf_test");

               // Test simple query
               const result = await connection.runSQL(
                  "SELECT * FROM snowflake_query('SELECT 1 as test', 'sf_test_secret')",
               );
               expect(result.rows).toBeDefined();
            },
            { timeout: 30000 },
         );

         it("should validate required Snowflake fields", async () => {
            const incompleteAttachment: AttachedDatabase = {
               name: "sf_incomplete",
               type: "snowflake",
               snowflakeConnection: {
                  account: "test-account",
                  username: "test-user",
                  // Missing password and warehouse
               },
            };

            const duckdbConnection: ApiConnection = {
               name: "duckdb_sf_incomplete",
               type: "duckdb",
               duckdbConnection: {
                  attachedDatabases: [incompleteAttachment],
               },
            };

            await expect(
               createProjectConnections([duckdbConnection], testProjectPath),
            ).rejects.toThrow(/required/);
         });
      });

      describe("DuckDB with S3 attachment", () => {
         const hasS3Credentials = () =>
            process.env.S3_TEST_ACCESS_KEY_ID &&
            process.env.S3_TEST_SECRET_ACCESS_KEY;

         it(
            "should create DuckDB connection with S3 configuration",
            async () => {
               if (!hasS3Credentials()) {
                  console.log("Skipping: S3 credentials not configured");
                  return;
               }

               const s3Attachment: AttachedDatabase = {
                  name: "s3_test",
                  type: "s3",
                  s3Connection: {
                     accessKeyId: process.env.S3_TEST_ACCESS_KEY_ID!,
                     secretAccessKey: process.env.S3_TEST_SECRET_ACCESS_KEY!,
                     region: "us-east-1",
                  },
               };

               const duckdbConnection: ApiConnection = {
                  name: "duckdb_with_s3",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [s3Attachment],
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

               // Verify secret was created
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

               const s3Attachment: AttachedDatabase = {
                  name: "s3_custom",
                  type: "s3",
                  s3Connection: {
                     accessKeyId: process.env.S3_TEST_ACCESS_KEY_ID!,
                     secretAccessKey: process.env.S3_TEST_SECRET_ACCESS_KEY!,
                     region: "us-east-1",
                     endpoint: "https://s3.custom-endpoint.com",
                  },
               };

               const duckdbConnection: ApiConnection = {
                  name: "duckdb_s3_custom",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [s3Attachment],
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
         const hasGCSCredentials = () =>
            process.env.GCS_TEST_KEY_ID && process.env.GCS_TEST_SECRET;

         it(
            "should create DuckDB connection with GCS configuration",
            async () => {
               if (!hasGCSCredentials()) {
                  console.log("Skipping: GCS credentials not configured");
                  return;
               }

               const gcsAttachment: AttachedDatabase = {
                  name: "gcs_test",
                  type: "gcs",
                  gcsConnection: {
                     keyId: process.env.GCS_TEST_KEY_ID!,
                     secret: process.env.GCS_TEST_SECRET!,
                  },
               };

               const duckdbConnection: ApiConnection = {
                  name: "duckdb_with_gcs",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: [gcsAttachment],
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

               // Verify secret was created
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

               // Add PostgreSQL if available
               if (
                  process.env.POSTGRES_TEST_USER &&
                  process.env.POSTGRES_TEST_PASSWORD
               ) {
                  attachments.push({
                     name: "pg_multi",
                     type: "postgres",
                     postgresConnection: {
                        host: "localhost",
                        port: 5432,
                        userName: process.env.POSTGRES_TEST_USER!,
                        password: process.env.POSTGRES_TEST_PASSWORD!,
                        databaseName: "postgres",
                     },
                  });
               }

               // Add S3 if available
               if (
                  process.env.S3_TEST_ACCESS_KEY_ID &&
                  process.env.S3_TEST_SECRET_ACCESS_KEY
               ) {
                  attachments.push({
                     name: "s3_multi",
                     type: "s3",
                     s3Connection: {
                        accessKeyId: process.env.S3_TEST_ACCESS_KEY_ID!,
                        secretAccessKey: process.env.S3_TEST_SECRET_ACCESS_KEY!,
                        region: "us-east-1",
                     },
                  });
               }

               if (attachments.length === 0) {
                  console.log("Skipping: No database credentials configured");
                  return;
               }

               const duckdbConnection: ApiConnection = {
                  name: "duckdb_multi",
                  type: "duckdb",
                  duckdbConnection: {
                     attachedDatabases: attachments,
                  },
               };

               const { malloyConnections } = await createProjectConnections(
                  [duckdbConnection],
                  testProjectPath,
               );

               const connection = malloyConnections.get(
                  "duckdb_multi",
               ) as DuckDBConnection;
               expect(connection).toBeDefined();
               createdConnections.push(connection);

               // Verify all databases are attached
               const databases = await connection.runSQL("SHOW DATABASES");
               const dbNames = databases.rows.map(
                  (row) => Object.values(row)[0],
               );

               attachments.forEach((attachment) => {
                  expect(dbNames).toContain(attachment.name!);
               });
            },
            { timeout: 45000 },
         );
      });

      describe("error handling", () => {
         it("should throw error if DuckDB connection name conflicts with attached database", async () => {
            const postgresAttachment: AttachedDatabase = {
               name: "conflict_db",
               type: "postgres",
               postgresConnection: {
                  connectionString: "postgresql://localhost/test",
               },
            };

            const duckdbConnection: ApiConnection = {
               name: "conflict_db",
               type: "duckdb",
               duckdbConnection: {
                  attachedDatabases: [postgresAttachment],
               },
            };

            await expect(
               createProjectConnections([duckdbConnection], testProjectPath),
            ).rejects.toThrow(/cannot conflict/);
         });

         it("should throw error if connection name is 'duckdb'", async () => {
            const duckdbConnection: ApiConnection = {
               name: "duckdb",
               type: "duckdb",
               duckdbConnection: {
                  attachedDatabases: [],
               },
            };

            await expect(
               createProjectConnections([duckdbConnection], testProjectPath),
            ).rejects.toThrow(/cannot be 'duckdb'/);
         });

         it("should throw error if no attached databases configured", async () => {
            const duckdbConnection: ApiConnection = {
               name: "empty_duckdb",
               type: "duckdb",
               duckdbConnection: {
                  attachedDatabases: [],
               },
            };

            await expect(
               createProjectConnections([duckdbConnection], testProjectPath),
            ).rejects.toThrow(/at least one attached database/);
         });

         it("should handle already attached database gracefully", async () => {
            if (!process.env.POSTGRES_TEST_USER) {
               console.log("Skipping: PostgreSQL credentials not configured");
               return;
            }

            const postgresAttachment: AttachedDatabase = {
               name: "pg_duplicate",
               type: "postgres",
               postgresConnection: {
                  host: "localhost",
                  port: 5432,
                  userName: process.env.POSTGRES_TEST_USER!,
                  password: process.env.POSTGRES_TEST_PASSWORD!,
                  databaseName: "postgres",
               },
            };

            const duckdbConnection: ApiConnection = {
               name: "duckdb_duplicate_test",
               type: "duckdb",
               duckdbConnection: {
                  attachedDatabases: [postgresAttachment, postgresAttachment],
               },
            };

            // Should not throw - should handle duplicate gracefully
            const { malloyConnections } = await createProjectConnections(
               [duckdbConnection],
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
            if (!process.env.POSTGRES_TEST_USER) {
               console.log("Skipping: PostgreSQL credentials not configured");
               return;
            }

            const postgresConnection: ApiConnection = {
               name: "test_postgres",
               type: "postgres",
               postgresConnection: {
                  host: "localhost",
                  port: 5432,
                  userName: process.env.POSTGRES_TEST_USER!,
                  password: process.env.POSTGRES_TEST_PASSWORD!,
                  databaseName: "postgres",
               },
            };

            const result = await testConnectionConfig(postgresConnection);

            expect(result.status).toBe("ok");
            expect(result.errorMessage).toBe("");
         },
         { timeout: 30000 },
      );

      it(
         "should fail for invalid PostgreSQL credentials",
         async () => {
            const postgresConnection: ApiConnection = {
               name: "test_postgres_invalid",
               type: "postgres",
               postgresConnection: {
                  host: "localhost",
                  port: 5432,
                  userName: "invalid_user",
                  password: "invalid_password",
                  databaseName: "nonexistent",
               },
            };

            const result = await testConnectionConfig(postgresConnection);

            expect(result.status).toBe("failed");
            expect(result.errorMessage).toBeDefined();
         },
         { timeout: 30000 },
      );

      it("should fail for missing connection name", async () => {
         const invalidConnection: ApiConnection = {
            name: "",
            type: "postgres",
            postgresConnection: {},
         };

         const result = await testConnectionConfig(invalidConnection);

         expect(result.status).toBe("failed");
         expect(result.errorMessage).toContain("name is required");
      });
   });

   describe("SQL injection prevention", () => {
      it("should properly escape special characters in credentials", async () => {
         if (!process.env.POSTGRES_TEST_USER) {
            console.log("Skipping: PostgreSQL credentials not configured");
            return;
         }

         // Test with credentials containing special SQL characters
         const specialCharsAttachment: AttachedDatabase = {
            name: "pg_special",
            type: "postgres",
            postgresConnection: {
               host: "localhost",
               port: 5432,
               userName: process.env.POSTGRES_TEST_USER!,
               password: process.env.POSTGRES_TEST_PASSWORD!,
               databaseName: "postgres",
            },
         };

         const duckdbConnection: ApiConnection = {
            name: "duckdb_special_chars",
            type: "duckdb",
            duckdbConnection: {
               attachedDatabases: [specialCharsAttachment],
            },
         };

         // Should not throw SQL syntax errors
         const { malloyConnections } = await createProjectConnections(
            [duckdbConnection],
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
            if (!process.env.POSTGRES_TEST_USER) {
               console.log("Skipping: PostgreSQL credentials not configured");
               return;
            }

            const postgresAttachment: AttachedDatabase = {
               name: "pg_attrs",
               type: "postgres",
               postgresConnection: {
                  host: "localhost",
                  port: 5432,
                  userName: process.env.POSTGRES_TEST_USER!,
                  password: process.env.POSTGRES_TEST_PASSWORD!,
                  databaseName: "postgres",
               },
            };

            const duckdbConnection: ApiConnection = {
               name: "duckdb_attrs",
               type: "duckdb",
               duckdbConnection: {
                  attachedDatabases: [postgresAttachment],
               },
            };

            const { apiConnections } = await createProjectConnections(
               [duckdbConnection],
               testProjectPath,
            );

            const connection = apiConnections[0];
            expect(connection.attributes).toBeDefined();
            expect(connection.attributes?.dialectName).toBe("duckdb");
            expect(connection.attributes?.canPersist).toBeDefined();
            expect(connection.attributes?.canStream).toBeDefined();
            expect(connection.attributes?.isPool).toBeDefined();

            const duckdb = createdConnections[createdConnections.length - 1];
            if (duckdb) createdConnections.push(duckdb);
         },
         { timeout: 30000 },
      );
   });
});
