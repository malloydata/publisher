// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import fs from "fs/promises";
import path from "path";
import { Client } from "pg";
import { components } from "../../../src/api";
import {
   createEnvironmentConnections,
   testConnectionConfig,
} from "../../../src/service/connection";
import {
   getSchemasForConnection,
   listTablesForSchema,
} from "../../../src/service/db_utils";
import { runInternalSQL } from "../../../src/service/duckdb_lockdown";
import {
   startClosingListener,
   type ClosingListener,
} from "../../../src/test_helpers/closing_listener";

type ApiConnection = components["schemas"]["Connection"];

const hasPostgresCredentials = () =>
   !!(
      process.env.POSTGRES_TEST_HOST &&
      process.env.POSTGRES_TEST_USER &&
      process.env.POSTGRES_TEST_PASSWORD
   );

const hasS3Credentials = () =>
   !!(
      process.env.S3_TEST_ACCESS_KEY_ID && process.env.S3_TEST_SECRET_ACCESS_KEY
   );

const hasGCSCredentials = () =>
   !!(process.env.GCS_TEST_KEY_ID && process.env.GCS_TEST_SECRET);

const duckdbFilesIn = async (directory: string) =>
   (await fs.readdir(directory)).filter((file) => file.endsWith(".duckdb"));

describe("DuckLake Connection Tests", () => {
   const testProjectPath = path.join(process.cwd(), "test-project-ducklake");
   let createdConnections: DuckDBConnection[] = [];

   beforeEach(async () => {
      await fs.mkdir(testProjectPath, { recursive: true });
   });

   afterEach(async () => {
      // Close all connections
      for (const conn of createdConnections) {
         try {
            await conn.close();
         } catch (error) {
            console.warn("Error closing connection:", error);
         }
      }
      createdConnections = [];

      // Clean up test directory
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

   describe("Connection Creation", () => {
      it.skipIf(!hasPostgresCredentials() || !hasS3Credentials())(
         "should create DuckLake connection with S3 storage",
         async () => {
            const ducklakeConnection: ApiConnection = {
               name: "ducklake_s3_test",
               type: "ducklake",
               ducklakeConnection: {
                  catalog: {
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
                  storage: {
                     bucketUrl:
                        process.env.S3_TEST_BUCKET_URL || "s3://test-bucket",
                     s3Connection: {
                        accessKeyId: process.env.S3_TEST_ACCESS_KEY_ID!,
                        secretAccessKey: process.env.S3_TEST_SECRET_ACCESS_KEY!,
                        region: process.env.S3_TEST_REGION || "us-east-1",
                     },
                  },
               },
            };

            const { malloyConnections, apiConnections } =
               await createEnvironmentConnections(
                  [ducklakeConnection],
                  testProjectPath,
               );

            expect(malloyConnections.size).toBe(1);
            expect(apiConnections.length).toBe(1);

            const connection = malloyConnections.get(
               "ducklake_s3_test",
            ) as DuckDBConnection;
            expect(connection).toBeDefined();
            createdConnections.push(connection);

            // Verify DuckLake database is attached
            const databases = await connection.runSQL("SHOW DATABASES");
            const dbNames = databases.rows.map((row) => Object.values(row)[0]);
            expect(dbNames).toContain("ducklake_s3_test");

            // The primary is in memory: no database file is kept
            expect(await duckdbFilesIn(testProjectPath)).toEqual([]);
         },
         { timeout: 30000 },
      );

      it(
         "should create DuckLake connection with GCS storage",
         async () => {
            if (!hasPostgresCredentials() || !hasGCSCredentials()) {
               console.log(
                  "Skipping: PostgreSQL and GCS credentials not configured",
               );
               return;
            }

            const ducklakeConnection: ApiConnection = {
               name: "ducklake_gcs_test",
               type: "ducklake",
               ducklakeConnection: {
                  catalog: {
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
                  storage: {
                     bucketUrl:
                        process.env.GCS_TEST_BUCKET_URL || "gs://test-bucket",
                     gcsConnection: {
                        keyId: process.env.GCS_TEST_KEY_ID!,
                        secret: process.env.GCS_TEST_SECRET!,
                     },
                  },
               },
            };

            const { malloyConnections } = await createEnvironmentConnections(
               [ducklakeConnection],
               testProjectPath,
            );

            const connection = malloyConnections.get(
               "ducklake_gcs_test",
            ) as DuckDBConnection;
            expect(connection).toBeDefined();
            createdConnections.push(connection);

            // Verify DuckLake database is attached
            const databases = await connection.runSQL("SHOW DATABASES");
            const dbNames = databases.rows.map((row) => Object.values(row)[0]);
            expect(dbNames).toContain("ducklake_gcs_test");
         },
         { timeout: 30000 },
      );

      it(
         "should load required extensions for DuckLake",
         async () => {
            if (!hasPostgresCredentials() || !hasS3Credentials()) {
               console.log(
                  "Skipping: PostgreSQL and S3 credentials not configured",
               );
               return;
            }

            const ducklakeConnection: ApiConnection = {
               name: "ducklake_extensions_test",
               type: "ducklake",
               ducklakeConnection: {
                  catalog: {
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
                  storage: {
                     bucketUrl:
                        process.env.S3_TEST_BUCKET_URL || "s3://test-bucket",
                     s3Connection: {
                        accessKeyId: process.env.S3_TEST_ACCESS_KEY_ID!,
                        secretAccessKey: process.env.S3_TEST_SECRET_ACCESS_KEY!,
                        region: process.env.S3_TEST_REGION || "us-east-1",
                     },
                  },
               },
            };

            const { malloyConnections } = await createEnvironmentConnections(
               [ducklakeConnection],
               testProjectPath,
            );

            const connection = malloyConnections.get(
               "ducklake_extensions_test",
            ) as DuckDBConnection;
            createdConnections.push(connection);

            // Verify required extensions are loaded
            // duckdb_extensions() is refused to caller SQL on a locked
            // instance; read it as Publisher's own statement.
            const extensions = await runInternalSQL(
               connection,
               "SELECT extension_name FROM duckdb_extensions() WHERE loaded = true",
            );
            const extensionNames = extensions.rows.map(
               (row) => Object.values(row)[0] as string,
            );

            // DuckLake requires these extensions
            expect(extensionNames).toContain("ducklake");
            expect(extensionNames).toContain("postgres_scanner");
            expect(extensionNames).toContain("httpfs");
         },
         { timeout: 30000 },
      );
   });

   describe("Schema Operations", () => {
      it(
         "should list schemas from DuckLake connection",
         async () => {
            if (!hasPostgresCredentials() || !hasS3Credentials()) {
               console.log(
                  "Skipping: PostgreSQL and S3 credentials not configured",
               );
               return;
            }

            const ducklakeConnection: ApiConnection = {
               name: "ducklake_schemas_test",
               type: "ducklake",
               ducklakeConnection: {
                  catalog: {
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
                  storage: {
                     bucketUrl:
                        process.env.S3_TEST_BUCKET_URL || "s3://test-bucket",
                     s3Connection: {
                        accessKeyId: process.env.S3_TEST_ACCESS_KEY_ID!,
                        secretAccessKey: process.env.S3_TEST_SECRET_ACCESS_KEY!,
                        region: process.env.S3_TEST_REGION || "us-east-1",
                     },
                  },
               },
            };

            const { malloyConnections } = await createEnvironmentConnections(
               [ducklakeConnection],
               testProjectPath,
            );

            const connection = malloyConnections.get(
               "ducklake_schemas_test",
            ) as DuckDBConnection;
            createdConnections.push(connection);

            // Test schema listing using db_utils
            const schemas = await getSchemasForConnection(
               ducklakeConnection,
               connection,
            );
            expect(schemas).toBeDefined();
            expect(Array.isArray(schemas)).toBe(true);

            // Also test direct SQL query
            const result = await connection.runSQL(
               `SELECT DISTINCT schema_name FROM information_schema.schemata WHERE catalog_name = 'ducklake_schemas_test' ORDER BY schema_name`,
            );
            expect(result.rows).toBeDefined();
            expect(result.rows.length).toBeGreaterThanOrEqual(0);
         },
         { timeout: 30000 },
      );
   });

   describe("Table Operations", () => {
      it(
         "should list tables from DuckLake schema",
         async () => {
            if (!hasPostgresCredentials() || !hasS3Credentials()) {
               console.log(
                  "Skipping: PostgreSQL and S3 credentials not configured",
               );
               return;
            }

            const ducklakeConnection: ApiConnection = {
               name: "ducklake_tables_test",
               type: "ducklake",
               ducklakeConnection: {
                  catalog: {
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
                  storage: {
                     bucketUrl:
                        process.env.S3_TEST_BUCKET_URL || "s3://test-bucket",
                     s3Connection: {
                        accessKeyId: process.env.S3_TEST_ACCESS_KEY_ID!,
                        secretAccessKey: process.env.S3_TEST_SECRET_ACCESS_KEY!,
                        region: process.env.S3_TEST_REGION || "us-east-1",
                     },
                  },
               },
            };

            const { malloyConnections } = await createEnvironmentConnections(
               [ducklakeConnection],
               testProjectPath,
            );

            const connection = malloyConnections.get(
               "ducklake_tables_test",
            ) as DuckDBConnection;
            createdConnections.push(connection);

            // Get schemas first
            const schemas = await getSchemasForConnection(
               ducklakeConnection,
               connection,
            );
            if (schemas.length === 0) {
               console.log("No schemas found, skipping table listing test");
               return;
            }

            // Test table listing for first schema
            const schemaName = schemas[0].name!;
            const tables = await listTablesForSchema(
               ducklakeConnection,
               schemaName,
               connection,
            );
            expect(tables).toBeDefined();
            expect(Array.isArray(tables)).toBe(true);
         },
         { timeout: 30000 },
      );

      it(
         "should handle table path prefixing correctly",
         async () => {
            if (!hasPostgresCredentials() || !hasS3Credentials()) {
               console.log(
                  "Skipping: PostgreSQL and S3 credentials not configured",
               );
               return;
            }

            const ducklakeConnection: ApiConnection = {
               name: "ducklake_prefix_test",
               type: "ducklake",
               ducklakeConnection: {
                  catalog: {
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
                  storage: {
                     bucketUrl:
                        process.env.S3_TEST_BUCKET_URL || "s3://test-bucket",
                     s3Connection: {
                        accessKeyId: process.env.S3_TEST_ACCESS_KEY_ID!,
                        secretAccessKey: process.env.S3_TEST_SECRET_ACCESS_KEY!,
                        region: process.env.S3_TEST_REGION || "us-east-1",
                     },
                  },
               },
            };

            const { malloyConnections } = await createEnvironmentConnections(
               [ducklakeConnection],
               testProjectPath,
            );

            const connection = malloyConnections.get(
               "ducklake_prefix_test",
            ) as DuckDBConnection;
            createdConnections.push(connection);

            // Test that connection name is used as catalog prefix
            // DuckLake tables should be accessible as connectionName.schemaName.tableName
            const result = await connection.runSQL(
               `SELECT catalog_name, schema_name FROM information_schema.schemata WHERE catalog_name = 'ducklake_prefix_test' LIMIT 1`,
            );
            expect(result.rows).toBeDefined();
         },
         { timeout: 30000 },
      );
   });

   describe("Query Operations", () => {
      it(
         "should execute queries against DuckLake database",
         async () => {
            if (!hasPostgresCredentials() || !hasS3Credentials()) {
               console.log(
                  "Skipping: PostgreSQL and S3 credentials not configured",
               );
               return;
            }

            const ducklakeConnection: ApiConnection = {
               name: "ducklake_query_test",
               type: "ducklake",
               ducklakeConnection: {
                  catalog: {
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
                  storage: {
                     bucketUrl:
                        process.env.S3_TEST_BUCKET_URL || "s3://test-bucket",
                     s3Connection: {
                        accessKeyId: process.env.S3_TEST_ACCESS_KEY_ID!,
                        secretAccessKey: process.env.S3_TEST_SECRET_ACCESS_KEY!,
                        region: process.env.S3_TEST_REGION || "us-east-1",
                     },
                  },
               },
            };

            const { malloyConnections } = await createEnvironmentConnections(
               [ducklakeConnection],
               testProjectPath,
            );

            const connection = malloyConnections.get(
               "ducklake_query_test",
            ) as DuckDBConnection;
            createdConnections.push(connection);

            // Test basic query
            const result = await connection.runSQL(
               `SELECT schema_name FROM information_schema.schemata WHERE catalog_name = 'ducklake_query_test' LIMIT 1`,
            );
            expect(result.rows).toBeDefined();
            expect(Array.isArray(result.rows)).toBe(true);
         },
         { timeout: 30000 },
      );
   });

   describe("Error Handling", () => {
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
               testProjectPath,
            ),
         ).rejects.toThrow(/PostgreSQL connection configuration is required/);
      });

      it("passes a DuckLake connection with no bucketUrl through to its attach: the catalog's recorded data path is used", async () => {
         // No catalog is reachable here, so the attach itself fails; what this
         // pins is that the configuration is no longer refused before it.
         const outcome = await createEnvironmentConnections(
            [
               {
                  name: "ducklake_no_bucket",
                  type: "ducklake",
                  ducklakeConnection: {
                     catalog: {
                        postgresConnection: {
                           host: "localhost",
                           port: 5432,
                           userName: "test",
                           password: "test",
                           databaseName: "test",
                        },
                     },
                     storage: {
                        s3Connection: {
                           accessKeyId: "test",
                           secretAccessKey: "test",
                        },
                     },
                  },
               } as ApiConnection,
            ],
            testProjectPath,
         ).then(
            () => "attached",
            (error: Error) => error.message,
         );
         expect(outcome).not.toMatch(/bucketUrl/i);
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
               testProjectPath,
            ),
         ).rejects.toThrow(/DuckLake connection configuration is missing/);
      });

      it("should handle already attached database gracefully", async () => {
         if (!hasPostgresCredentials() || !hasS3Credentials()) {
            console.log(
               "Skipping: PostgreSQL and S3 credentials not configured",
            );
            return;
         }

         const ducklakeConnection: ApiConnection = {
            name: "ducklake_duplicate_test",
            type: "ducklake",
            ducklakeConnection: {
               catalog: {
                  postgresConnection: {
                     host: process.env.POSTGRES_TEST_HOST,
                     port: parseInt(process.env.POSTGRES_TEST_PORT || "5432"),
                     userName: process.env.POSTGRES_TEST_USER!,
                     password: process.env.POSTGRES_TEST_PASSWORD!,
                     databaseName: process.env.POSTGRES_TEST_DATABASE,
                  },
               },
               storage: {
                  bucketUrl:
                     process.env.S3_TEST_BUCKET_URL || "s3://test-bucket",
                  s3Connection: {
                     accessKeyId: process.env.S3_TEST_ACCESS_KEY_ID!,
                     secretAccessKey: process.env.S3_TEST_SECRET_ACCESS_KEY!,
                     region: process.env.S3_TEST_REGION || "us-east-1",
                  },
               },
            },
         };

         // Create connection twice - second should handle already attached gracefully
         const { malloyConnections: conn1 } =
            await createEnvironmentConnections(
               [ducklakeConnection],
               testProjectPath,
            );
         const connection1 = conn1.get(
            "ducklake_duplicate_test",
         ) as DuckDBConnection;
         createdConnections.push(connection1);

         const { malloyConnections: conn2 } =
            await createEnvironmentConnections(
               [ducklakeConnection],
               testProjectPath,
            );
         const connection2 = conn2.get(
            "ducklake_duplicate_test",
         ) as DuckDBConnection;
         createdConnections.push(connection2);

         expect(connection1).toBeDefined();
         expect(connection2).toBeDefined();
      });
   });

   describe("Database File Management", () => {
      // A local data path: the primary's file, or its absence, needs no cloud storage.
      it.skipIf(!hasPostgresCredentials())(
         "should keep no database file",
         async () => {
            const dataPath = path.join(testProjectPath, "lake-data");
            await fs.mkdir(dataPath, { recursive: true });
            const metadataSchema = `ducklake_file_test_${process.pid}`;
            const env = process.env;
            try {
               // The connection attaches an existing catalog only, so one is created first.
               const creator = new DuckDBConnection(
                  "ducklake_file_test_creator",
                  ":memory:",
                  testProjectPath,
               );
               try {
                  await creator.runSQL("INSTALL ducklake; LOAD ducklake;");
                  await creator.runSQL(
                     `ATTACH 'ducklake:postgres:host=${env.POSTGRES_TEST_HOST} port=${env.POSTGRES_TEST_PORT || "5432"} ` +
                        `user=${env.POSTGRES_TEST_USER} password=${env.POSTGRES_TEST_PASSWORD}` +
                        `${env.POSTGRES_TEST_DATABASE ? ` dbname=${env.POSTGRES_TEST_DATABASE}` : ""}' ` +
                        `AS fresh (DATA_PATH '${dataPath}/', METADATA_SCHEMA '${metadataSchema}')`,
                  );
                  await creator.runSQL("DETACH fresh");
               } finally {
                  await creator.close();
               }
               const ducklakeConnection: ApiConnection = {
                  name: "ducklake_file_test",
                  type: "ducklake",
                  ducklakeConnection: {
                     catalog: {
                        postgresConnection: {
                           host: env.POSTGRES_TEST_HOST,
                           port: parseInt(env.POSTGRES_TEST_PORT || "5432"),
                           userName: env.POSTGRES_TEST_USER!,
                           password: env.POSTGRES_TEST_PASSWORD!,
                           databaseName: env.POSTGRES_TEST_DATABASE,
                        },
                        metadataSchema,
                     },
                     storage: { bucketUrl: dataPath },
                  },
               };

               const { malloyConnections } = await createEnvironmentConnections(
                  [ducklakeConnection],
                  testProjectPath,
               );
               const connection = malloyConnections.get(
                  "ducklake_file_test",
               ) as DuckDBConnection;
               createdConnections.push(connection);
               const databases = await connection.runSQL("SHOW DATABASES");
               expect(
                  databases.rows.map((row) => Object.values(row)[0]),
               ).toContain("ducklake_file_test");

               expect(await duckdbFilesIn(testProjectPath)).toEqual([]);
            } finally {
               const pg = new Client({
                  host: env.POSTGRES_TEST_HOST,
                  port: parseInt(env.POSTGRES_TEST_PORT || "5432"),
                  user: env.POSTGRES_TEST_USER,
                  password: env.POSTGRES_TEST_PASSWORD,
                  database: env.POSTGRES_TEST_DATABASE,
               });
               await pg.connect();
               try {
                  await pg.query(
                     `DROP SCHEMA IF EXISTS ${metadataSchema} CASCADE`,
                  );
               } finally {
                  await pg.end();
               }
            }
         },
         { timeout: 30000 },
      );
   });

   describe("Connection Testing", () => {
      // Accepts and drops each connection, so a catalog behind it fails at once.
      let unreachableCatalog: ClosingListener;
      beforeAll(async () => {
         unreachableCatalog = await startClosingListener();
      });
      afterAll(async () => {
         await unreachableCatalog.close();
      });

      it(
         "should test DuckLake connection configuration",
         async () => {
            if (!hasPostgresCredentials() || !hasS3Credentials()) {
               console.log(
                  "Skipping: PostgreSQL and S3 credentials not configured",
               );
               return;
            }

            const result = await testConnectionConfig({
               name: "ducklake_test_config",
               type: "ducklake",
               ducklakeConnection: {
                  catalog: {
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
                  storage: {
                     bucketUrl:
                        process.env.S3_TEST_BUCKET_URL || "s3://test-bucket",
                     s3Connection: {
                        accessKeyId: process.env.S3_TEST_ACCESS_KEY_ID!,
                        secretAccessKey: process.env.S3_TEST_SECRET_ACCESS_KEY!,
                        region: process.env.S3_TEST_REGION || "us-east-1",
                     },
                  },
               },
            });

            expect(result.status).toBe("ok");
            expect(result.errorMessage).toBe("");
         },
         { timeout: 30000 },
      );

      it(
         "should fail test for invalid DuckLake configuration",
         async () => {
            const result = await testConnectionConfig({
               name: "ducklake_invalid_test",
               type: "ducklake",
               ducklakeConnection: {
                  catalog: {
                     postgresConnection: {
                        host: "127.0.0.1",
                        port: unreachableCatalog.port,
                        userName: "invalid",
                        password: "invalid",
                        databaseName: "invalid",
                     },
                  },
                  storage: {
                     bucketUrl: "s3://invalid-bucket",
                     s3Connection: {
                        accessKeyId: "invalid",
                        secretAccessKey: "invalid",
                     },
                  },
               },
            });

            expect(result.status).toBe("failed");
            expect(result.errorMessage).toBeDefined();
            expect(result.errorMessage?.length).toBeGreaterThan(0);
         },
         { timeout: 30000 },
      );
   });

   describe("Connection Attributes", () => {
      it(
         "should return correct attributes for DuckLake connection",
         async () => {
            if (!hasPostgresCredentials() || !hasS3Credentials()) {
               console.log(
                  "Skipping: PostgreSQL and S3 credentials not configured",
               );
               return;
            }

            const { apiConnections } = await createEnvironmentConnections(
               [
                  {
                     name: "ducklake_attrs_test",
                     type: "ducklake",
                     ducklakeConnection: {
                        catalog: {
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
                        storage: {
                           bucketUrl:
                              process.env.S3_TEST_BUCKET_URL ||
                              "s3://test-bucket",
                           s3Connection: {
                              accessKeyId: process.env.S3_TEST_ACCESS_KEY_ID!,
                              secretAccessKey:
                                 process.env.S3_TEST_SECRET_ACCESS_KEY!,
                              region: process.env.S3_TEST_REGION || "us-east-1",
                           },
                        },
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
