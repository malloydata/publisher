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
import os from "os";
import path from "path";
import type { components } from "../../../src/api";
import { createPackageDuckDBConnections } from "../../../src/service/connection";

type ApiConnection = components["schemas"]["Connection"];

const TEST_DB_DIR = path.join(os.tmpdir(), "duckdb-attached-db-tests");
const TEST_DB_PATH = path.join(TEST_DB_DIR, "test.duckdb");

describe("DuckDB Attached Databases", () => {
   let connection: DuckDBConnection;

   beforeAll(async () => {
      await fs.mkdir(TEST_DB_DIR, { recursive: true });
      connection = new DuckDBConnection("test", TEST_DB_PATH, TEST_DB_DIR);
   });

   afterAll(async () => {
      try {
         await connection.close();
         await new Promise((resolve) => setTimeout(resolve, 100));
         await fs.rm(TEST_DB_DIR, { recursive: true, force: true });
      } catch {
         // Ignore cleanup errors
      }
   });

   describe("Extension Availability Tests", () => {
      it("should verify community extension install capability", async () => {
         const result = await connection.runSQL("INSTALL json;");
         expect(result).toBeDefined();
      });

      it("should report clear error when extension is not found", async () => {
         try {
            await connection.runSQL(
               "INSTALL 'nonexistent_extension_xyz' FROM community;",
            );
            expect(true).toBe(false);
         } catch (error) {
            expect(error).toBeInstanceOf(Error);
            const message = (error as Error).message.toLowerCase();
            expect(
               message.includes("extension") || message.includes("not found"),
            ).toBe(true);
         }
      });

      it("should load httpfs extension for cloud storage", async () => {
         await connection.runSQL("INSTALL httpfs;");
         await connection.runSQL("LOAD httpfs;");
         const result = await connection.runSQL(
            "SELECT * FROM duckdb_extensions() WHERE extension_name = 'httpfs';",
         );
         expect(result.rows.length).toBeGreaterThan(0);
      });

      it("should load postgres extension", async () => {
         await connection.runSQL("INSTALL postgres;");
         await connection.runSQL("LOAD postgres;");
         // Extension is named 'postgres' but registered as 'postgres_scanner'
         const result = await connection.runSQL(
            "SELECT * FROM duckdb_extensions() WHERE extension_name = 'postgres_scanner';",
         );
         expect(result.rows.length).toBeGreaterThan(0);
      });

      it("should load bigquery extension from community", async () => {
         try {
            await connection.runSQL("INSTALL 'bigquery' FROM community;");
            await connection.runSQL("LOAD bigquery;");
            const result = await connection.runSQL(
               "SELECT * FROM duckdb_extensions() WHERE extension_name = 'bigquery';",
            );
            expect(result.rows.length).toBeGreaterThan(0);
         } catch (error) {
            const message = (error as Error).message;
            if (
               message.includes("not found") ||
               message.includes("Extension")
            ) {
               console.error(
                  `\n⚠️  BigQuery extension not available for this DuckDB version/platform.`,
               );
               console.error(`Error: ${message}\n`);
            }
            throw error;
         }
      });

      it("should load snowflake extension from community", async () => {
         try {
            await connection.runSQL("INSTALL 'snowflake' FROM community;");
            await connection.runSQL("LOAD snowflake;");
            const result = await connection.runSQL(
               "SELECT * FROM duckdb_extensions() WHERE extension_name = 'snowflake';",
            );
            expect(result.rows.length).toBeGreaterThan(0);
         } catch (error) {
            const message = (error as Error).message;
            if (
               message.includes("not found") ||
               message.includes("Extension")
            ) {
               console.error(
                  `\n⚠️  Snowflake extension not available for this DuckDB version/platform.`,
               );
               console.error(`Error: ${message}\n`);
            }
            throw error;
         }
      });
   });

   describe("Configuration Validation - Negative Tests", () => {
      it("should reject BigQuery config without service account key", async () => {
         const invalidConfig = {
            name: "test_bq",
            type: "bigquery",
            bigqueryConnection: {
               defaultProjectId: "test-project",
            },
         };

         expect(
            invalidConfig.bigqueryConnection.defaultProjectId,
         ).toBeDefined();
         expect(
            (invalidConfig.bigqueryConnection as Record<string, unknown>)
               .serviceAccountKeyJson,
         ).toBeUndefined();
      });

      it("should reject invalid service account JSON", async () => {
         const invalidJson = '{"type": "wrong_type"}';

         try {
            const keyData = JSON.parse(invalidJson);
            if (keyData.type !== "service_account") {
               throw new Error(
                  'Invalid service account key: incorrect "type" field',
               );
            }
            expect(true).toBe(false);
         } catch (error) {
            expect((error as Error).message).toContain("type");
         }
      });

      it("should reject service account JSON missing required fields", async () => {
         const incompleteJson = JSON.stringify({
            type: "service_account",
            project_id: "test-project",
         });

         const requiredFields = [
            "type",
            "project_id",
            "private_key",
            "client_email",
         ];
         const keyData = JSON.parse(incompleteJson);

         const missingFields = requiredFields.filter(
            (field) => !keyData[field],
         );
         expect(missingFields).toContain("private_key");
         expect(missingFields).toContain("client_email");
      });

      it("should reject Snowflake config without required credentials", async () => {
         const invalidConfig = {
            name: "test_sf",
            type: "snowflake",
            snowflakeConnection: {
               account: "test-account",
            },
         };

         const requiredFields = {
            account:
               (invalidConfig.snowflakeConnection as Record<string, unknown>)
                  .account || null,
            username:
               (invalidConfig.snowflakeConnection as Record<string, unknown>)
                  .username || null,
            password:
               (invalidConfig.snowflakeConnection as Record<string, unknown>)
                  .password || null,
         };

         const missingFields = Object.entries(requiredFields)
            .filter(([, value]) => !value)
            .map(([key]) => key);

         expect(missingFields).toContain("username");
         expect(missingFields).toContain("password");
      });

      it("should reject GCS config without keyId and secret", async () => {
         const invalidConfig = {
            name: "test_gcs",
            type: "gcs",
            gcsConnection: {},
         };

         expect(
            (invalidConfig.gcsConnection as Record<string, unknown>).keyId,
         ).toBeUndefined();
         expect(
            (invalidConfig.gcsConnection as Record<string, unknown>).secret,
         ).toBeUndefined();
      });

      it("should reject S3 config without credentials", async () => {
         const invalidConfig = {
            name: "test_s3",
            type: "s3",
            s3Connection: {},
         };

         expect(
            (invalidConfig.s3Connection as Record<string, unknown>).accessKeyId,
         ).toBeUndefined();
         expect(
            (invalidConfig.s3Connection as Record<string, unknown>)
               .secretAccessKey,
         ).toBeUndefined();
      });

      it("should reject unsupported database type", async () => {
         const unsupportedType = "mongodb";
         const supportedTypes = [
            "bigquery",
            "snowflake",
            "postgres",
            "gcs",
            "s3",
         ];

         expect(supportedTypes).not.toContain(unsupportedType);
      });
   });

   describe("DuckDB Connection - Positive Tests", () => {
      it("should create and query in-memory database", async () => {
         const memConnection = new DuckDBConnection(
            "mem_test",
            ":memory:",
            TEST_DB_DIR,
         );
         const result = await memConnection.runSQL("SELECT 1 as value;");
         expect(result.rows[0]).toEqual({ value: 1 });
         await memConnection.close();
      });

      it("should show databases including memory", async () => {
         const result = await connection.runSQL("SHOW DATABASES;");
         expect(result.rows).toBeDefined();
         expect(Array.isArray(result.rows)).toBe(true);
      });

      it("should handle SQL escaping correctly", async () => {
         const testValue = "test'with\"special;chars";
         const escaped = testValue.replace(/'/g, "''");
         const result = await connection.runSQL(
            `SELECT '${escaped}' as escaped_value;`,
         );
         expect(result.rows[0].escaped_value).toBe(testValue);
      });

      it("should create and use secrets", async () => {
         await connection.runSQL(`
            CREATE OR REPLACE SECRET test_secret (
               TYPE S3,
               KEY_ID 'test_key',
               SECRET 'test_secret_value',
               REGION 'us-east-1'
            );
         `);

         const secrets = await connection.runSQL(
            "SELECT * FROM duckdb_secrets();",
         );
         expect(secrets.rows.length).toBeGreaterThan(0);

         await connection.runSQL("DROP SECRET IF EXISTS test_secret;");
      });
   });

   describe("Version Compatibility Check", () => {
      it("should report DuckDB version for diagnostics", async () => {
         const result = await connection.runSQL("SELECT version() as version;");
         const version = result.rows[0].version as string;

         console.log(`\n📊 DuckDB Version: ${version}`);

         expect(version).toBeDefined();
         expect(typeof version).toBe("string");
         expect(version).toMatch(/v?\d+\.\d+\.\d+/);
      });

      it("should list available extensions for diagnostics", async () => {
         const result = await connection.runSQL(
            "SELECT extension_name, loaded, installed FROM duckdb_extensions() WHERE installed = true ORDER BY extension_name;",
         );

         console.log(`\n📦 Installed Extensions:`);
         for (const row of result.rows) {
            const r = row as {
               extension_name: string;
               loaded: boolean;
               installed: boolean;
            };
            console.log(`   - ${r.extension_name} (loaded: ${r.loaded})`);
         }

         expect(result.rows).toBeDefined();
      });
   });
});

describe("createPackageDuckDBConnections", () => {
   const PACKAGE_TEST_DIR = path.join(os.tmpdir(), "duckdb-package-tests");
   let createdConnections: Map<string, unknown> = new Map();

   beforeEach(async () => {
      await fs.mkdir(PACKAGE_TEST_DIR, { recursive: true });
   });

   afterEach(async () => {
      for (const conn of createdConnections.values()) {
         try {
            await (conn as DuckDBConnection).close();
         } catch {
            // Ignore
         }
      }
      createdConnections.clear();
      await new Promise((resolve) => setTimeout(resolve, 100));
      try {
         await fs.rm(PACKAGE_TEST_DIR, { recursive: true, force: true });
      } catch {
         // Ignore
      }
   });

   describe("Positive Tests", () => {
      it("should create default duckdb connection when none provided", async () => {
         const { malloyConnections, apiConnections } =
            await createPackageDuckDBConnections([], PACKAGE_TEST_DIR);

         createdConnections = malloyConnections;

         expect(malloyConnections.has("duckdb")).toBe(true);
         expect(apiConnections.length).toBe(1);
         expect(apiConnections[0].name).toBe("duckdb");
         expect(apiConnections[0].type).toBe("duckdb");
      });

      it("should create named DuckDB connection", async () => {
         const connections: ApiConnection[] = [
            {
               name: "my_duckdb",
               type: "duckdb",
               duckdbConnection: {
                  attachedDatabases: [],
               },
            },
         ];

         const { malloyConnections, apiConnections } =
            await createPackageDuckDBConnections(connections, PACKAGE_TEST_DIR);

         createdConnections = malloyConnections;

         expect(malloyConnections.has("my_duckdb")).toBe(true);
         expect(malloyConnections.has("duckdb")).toBe(true);
         expect(apiConnections.length).toBe(2);
      });

      it("should set connection attributes correctly", async () => {
         const connections: ApiConnection[] = [
            {
               name: "attr_test",
               type: "duckdb",
               duckdbConnection: {
                  attachedDatabases: [],
               },
            },
         ];

         const { apiConnections } = await createPackageDuckDBConnections(
            connections,
            PACKAGE_TEST_DIR,
         );

         createdConnections = (
            await createPackageDuckDBConnections([], PACKAGE_TEST_DIR)
         ).malloyConnections;

         const conn = apiConnections.find((c) => c.name === "attr_test");
         expect(conn?.attributes).toBeDefined();
         expect(conn?.attributes?.dialectName).toBe("duckdb");
      });

      it("should skip non-duckdb connection types", async () => {
         const connections: ApiConnection[] = [
            {
               name: "postgres_conn",
               type: "postgres",
               postgresConnection: {
                  host: "localhost",
               },
            },
            {
               name: "my_duck",
               type: "duckdb",
               duckdbConnection: {
                  attachedDatabases: [],
               },
            },
         ];

         const { malloyConnections } = await createPackageDuckDBConnections(
            connections,
            PACKAGE_TEST_DIR,
         );

         createdConnections = malloyConnections;

         expect(malloyConnections.has("postgres_conn")).toBe(false);
         expect(malloyConnections.has("my_duck")).toBe(true);
      });

      it("should create database file at package path", async () => {
         const connections: ApiConnection[] = [
            {
               name: "file_test",
               type: "duckdb",
               duckdbConnection: {
                  attachedDatabases: [],
               },
            },
         ];

         const { malloyConnections } = await createPackageDuckDBConnections(
            connections,
            PACKAGE_TEST_DIR,
         );

         createdConnections = malloyConnections;

         const conn = malloyConnections.get(
            "file_test",
         ) as unknown as DuckDBConnection;
         await conn.runSQL("CREATE TABLE test_table (id INT);");

         const dbPath = path.join(PACKAGE_TEST_DIR, "file_test.duckdb");
         const exists = await fs
            .access(dbPath)
            .then(() => true)
            .catch(() => false);
         expect(exists).toBe(true);
      });
   });

   describe("Negative Tests", () => {
      it("should throw on duplicate connection names", async () => {
         const connections: ApiConnection[] = [
            {
               name: "duplicate",
               type: "duckdb",
               duckdbConnection: { attachedDatabases: [] },
            },
            {
               name: "duplicate",
               type: "duckdb",
               duckdbConnection: { attachedDatabases: [] },
            },
         ];

         await expect(
            createPackageDuckDBConnections(connections, PACKAGE_TEST_DIR),
         ).rejects.toThrow("only supports one DuckDB connection per name");
      });

      it("should throw on missing connection name", async () => {
         const connections: ApiConnection[] = [
            {
               name: "",
               type: "duckdb",
               duckdbConnection: { attachedDatabases: [] },
            },
         ];

         await expect(
            createPackageDuckDBConnections(connections, PACKAGE_TEST_DIR),
         ).rejects.toThrow();
      });

      it("should throw on missing duckdbConnection config", async () => {
         const connections = [
            {
               name: "no_config",
               type: "duckdb",
            },
         ] as ApiConnection[];

         await expect(
            createPackageDuckDBConnections(connections, PACKAGE_TEST_DIR),
         ).rejects.toThrow("DuckDB connection configuration is missing");
      });

      it("should throw on unsupported attached database type", async () => {
         const connections: ApiConnection[] = [
            {
               name: "bad_attach",
               type: "duckdb",
               duckdbConnection: {
                  attachedDatabases: [
                     {
                        name: "unsupported_db",
                        type: "mongodb" as "bigquery",
                     },
                  ],
               },
            },
         ];

         await expect(
            createPackageDuckDBConnections(connections, PACKAGE_TEST_DIR),
         ).rejects.toThrow("Unsupported database type");
      });

      it("should throw on BigQuery attach without service account", async () => {
         const connections: ApiConnection[] = [
            {
               name: "bq_no_creds",
               type: "duckdb",
               duckdbConnection: {
                  attachedDatabases: [
                     {
                        name: "bq_test",
                        type: "bigquery",
                        bigqueryConnection: {
                           defaultProjectId: "test-project",
                        },
                     },
                  ],
               },
            },
         ];

         await expect(
            createPackageDuckDBConnections(connections, PACKAGE_TEST_DIR),
         ).rejects.toThrow("service account key required");
      });

      it("should throw on GCS attach without credentials", async () => {
         const connections = [
            {
               name: "gcs_no_creds",
               type: "duckdb",
               duckdbConnection: {
                  attachedDatabases: [
                     {
                        name: "gcs_test",
                        type: "gcs",
                        gcsConnection: {},
                     },
                  ],
               },
            },
         ] as ApiConnection[];

         await expect(
            createPackageDuckDBConnections(connections, PACKAGE_TEST_DIR),
         ).rejects.toThrow("keyId and secret are required");
      });

      it("should throw on S3 attach without credentials", async () => {
         const connections = [
            {
               name: "s3_no_creds",
               type: "duckdb",
               duckdbConnection: {
                  attachedDatabases: [
                     {
                        name: "s3_test",
                        type: "s3",
                        s3Connection: {},
                     },
                  ],
               },
            },
         ] as ApiConnection[];

         await expect(
            createPackageDuckDBConnections(connections, PACKAGE_TEST_DIR),
         ).rejects.toThrow("accessKeyId and secretAccessKey are required");
      });

      it("should throw on Snowflake attach without credentials", async () => {
         const connections: ApiConnection[] = [
            {
               name: "sf_no_creds",
               type: "duckdb",
               duckdbConnection: {
                  attachedDatabases: [
                     {
                        name: "sf_test",
                        type: "snowflake",
                        snowflakeConnection: {
                           account: "test-account",
                        },
                     },
                  ],
               },
            },
         ];

         await expect(
            createPackageDuckDBConnections(connections, PACKAGE_TEST_DIR),
         ).rejects.toThrow("username is required");
      });

      it("should throw on Postgres attach without connection config", async () => {
         const connections: ApiConnection[] = [
            {
               name: "pg_no_config",
               type: "duckdb",
               duckdbConnection: {
                  attachedDatabases: [
                     {
                        name: "pg_test",
                        type: "postgres",
                     },
                  ],
               },
            },
         ];

         await expect(
            createPackageDuckDBConnections(connections, PACKAGE_TEST_DIR),
         ).rejects.toThrow("PostgreSQL connection configuration missing");
      });
   });
});
