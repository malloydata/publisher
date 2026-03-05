import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import path from "path";
import { components } from "../../../src/api";
import {
   createProjectConnections,
   deleteDuckLakeConnectionFile,
   testConnectionConfig,
} from "../../../src/service/connection";
import {
   getSchemasForConnection,
   getTablesForSchema,
} from "../../../src/service/db_utils";

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

      // Clean up DuckLake database files from testProjectPath
      try {
         const files = await fs.readdir(testProjectPath);
         for (const file of files) {
            if (file.endsWith("_ducklake.duckdb")) {
               await fs.rm(path.join(testProjectPath, file), { force: true });
            }
         }
      } catch (error) {
         // Ignore cleanup errors
      }

      // Clean up DuckLake database files from process.cwd() (created by testConnectionConfig)
      // testConnectionConfig creates files in process.cwd() instead of testProjectPath
      try {
         const cwdFiles = await fs.readdir(process.cwd());
         for (const file of cwdFiles) {
            if (file.endsWith("_ducklake.duckdb")) {
               const filePath = path.join(process.cwd(), file);
               // Only delete test files, not production files
               if (file.includes("_test") || file.includes("invalid")) {
                  await fs.rm(filePath, { force: true });
               }
            }
         }
      } catch (_error) {
         // Ignore cleanup errors (file might not exist or already deleted)
      }

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

      if ((lastError as NodeJS.ErrnoError).code !== "EBUSY") {
         throw lastError;
      }
   });

   describe("Connection Creation", () => {
      it(
         "should create DuckLake connection with S3 storage",
         async () => {
            if (!hasPostgresCredentials() || !hasS3Credentials()) {
               console.log(
                  "Skipping: PostgreSQL and S3 credentials not configured",
               );
               return;
            }

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
               await createProjectConnections(
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

            // Verify database file was created
            const dbPath = path.join(
               testProjectPath,
               "ducklake_s3_test_ducklake.duckdb",
            );
            const exists = await fs
               .access(dbPath)
               .then(() => true)
               .catch(() => false);
            expect(exists).toBe(true);
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

            const { malloyConnections } = await createProjectConnections(
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

            const { malloyConnections } = await createProjectConnections(
               [ducklakeConnection],
               testProjectPath,
            );

            const connection = malloyConnections.get(
               "ducklake_extensions_test",
            ) as DuckDBConnection;
            createdConnections.push(connection);

            // Verify required extensions are loaded
            const extensions = await connection.runSQL(
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

            const { malloyConnections } = await createProjectConnections(
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

            const { malloyConnections } = await createProjectConnections(
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
            const schemaName = schemas[0].name;
            const tables = await getTablesForSchema(
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

            const { malloyConnections } = await createProjectConnections(
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

            const { malloyConnections } = await createProjectConnections(
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
            createProjectConnections(
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

      it("should throw error if DuckLake storage bucketUrl is missing", async () => {
         await expect(
            createProjectConnections(
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
            ),
         ).rejects.toThrow(/Storage bucketUrl is required/);
      });

      it("should throw error if DuckLake connection config is missing", async () => {
         await expect(
            createProjectConnections(
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
         const { malloyConnections: conn1 } = await createProjectConnections(
            [ducklakeConnection],
            testProjectPath,
         );
         const connection1 = conn1.get(
            "ducklake_duplicate_test",
         ) as DuckDBConnection;
         createdConnections.push(connection1);

         const { malloyConnections: conn2 } = await createProjectConnections(
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
      it(
         "should create database file with correct naming pattern",
         async () => {
            if (!hasPostgresCredentials() || !hasS3Credentials()) {
               console.log(
                  "Skipping: PostgreSQL and S3 credentials not configured",
               );
               return;
            }

            const ducklakeConnection: ApiConnection = {
               name: "ducklake_file_test",
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

            const { malloyConnections } = await createProjectConnections(
               [ducklakeConnection],
               testProjectPath,
            );

            const connection = malloyConnections.get(
               "ducklake_file_test",
            ) as DuckDBConnection;
            createdConnections.push(connection);

            // Verify database file follows naming pattern: {connectionName}_ducklake.duckdb
            const dbPath = path.join(
               testProjectPath,
               "ducklake_file_test_ducklake.duckdb",
            );
            const exists = await fs
               .access(dbPath)
               .then(() => true)
               .catch(() => false);
            expect(exists).toBe(true);
         },
         { timeout: 30000 },
      );

      it("should delete DuckLake connection file", async () => {
         if (!hasPostgresCredentials() || !hasS3Credentials()) {
            console.log(
               "Skipping: PostgreSQL and S3 credentials not configured",
            );
            return;
         }

         const ducklakeConnection: ApiConnection = {
            name: "ducklake_delete_test",
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

         const { malloyConnections } = await createProjectConnections(
            [ducklakeConnection],
            testProjectPath,
         );

         const connection = malloyConnections.get(
            "ducklake_delete_test",
         ) as DuckDBConnection;
         await connection.close();
         createdConnections = createdConnections.filter(
            (c) => c !== connection,
         );

         // Delete the file
         await deleteDuckLakeConnectionFile(
            "ducklake_delete_test",
            testProjectPath,
         );

         // Verify file is deleted
         const dbPath = path.join(
            testProjectPath,
            "ducklake_delete_test_ducklake.duckdb",
         );
         const exists = await fs
            .access(dbPath)
            .then(() => true)
            .catch(() => false);
         expect(exists).toBe(false);
      });

      it("should handle deletion of non-existent file gracefully", async () => {
         // Should not throw error if file doesn't exist
         // The function catches ENOENT errors internally, so this should complete without throwing
         await expect(
            deleteDuckLakeConnectionFile(
               "nonexistent_connection",
               testProjectPath,
            ),
         ).resolves.toBeUndefined();
      });
   });

   describe("Connection Testing", () => {
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
                        host: "invalid-host",
                        port: 5432,
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
            expect(result.errorMessage.length).toBeGreaterThan(0);
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

            const { apiConnections } = await createProjectConnections(
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
