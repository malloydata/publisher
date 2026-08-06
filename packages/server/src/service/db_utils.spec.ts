import { afterEach, describe, expect, it, mock } from "bun:test";

// Stub the missing optional dependency so db_utils.ts can be imported
mock.module("@azure/identity", () => ({
   ClientSecretCredential: class {},
}));
mock.module("@azure/storage-blob", () => ({
   ContainerClient: class {},
}));
// Records bigquery.dataset() calls so tests can assert how a (possibly
// project-qualified) schema name is split into datasetId + projectId.
const bqDatasetCalls: Array<{ id: string; options?: unknown }> = [];
mock.module("@google-cloud/bigquery", () => ({
   BigQuery: class {
      dataset(id: string, options?: unknown) {
         bqDatasetCalls.push({ id, options });
         return { getTables: async () => [[]] };
      }
   },
}));

import { Connection } from "@malloydata/malloy";
import { normalizeQueryArray } from "../query_param_utils";
import {
   extractErrorDataFromError,
   getSchemasForConnection,
   listTablesForSchema,
   sqlInFilter,
} from "./db_utils";
import { components } from "../api";

type ApiConnection = components["schemas"]["Connection"];

/**
 * Minimal mock Connection whose runSQL captures the SQL string
 * and returns configurable rows.
 */
function mockConnection(rows: unknown[] = []) {
   let lastSQL = "";
   return {
      get lastSQL() {
         return lastSQL;
      },
      conn: {
         runSQL: async (sql: string) => {
            lastSQL = sql;
            return { rows };
         },
      } as unknown as Connection,
   };
}

// ---------------------------------------------------------------------------
// sqlInFilter
// ---------------------------------------------------------------------------
describe("sqlInFilter", () => {
   it("returns empty string for undefined", () => {
      expect(sqlInFilter("col", undefined)).toBe("");
   });

   it("returns empty string for empty array", () => {
      expect(sqlInFilter("col", [])).toBe("");
   });

   it("builds single-value IN clause", () => {
      expect(sqlInFilter("TABLE_NAME", ["orders"])).toBe(
         "AND TABLE_NAME IN ('orders')",
      );
   });

   it("builds multi-value IN clause", () => {
      expect(sqlInFilter("t", ["a", "b", "c"])).toBe(
         "AND t IN ('a', 'b', 'c')",
      );
   });

   it("escapes single quotes in values", () => {
      expect(sqlInFilter("t", ["it's", "a'b"])).toBe(
         "AND t IN ('it''s', 'a''b')",
      );
   });
});

// ---------------------------------------------------------------------------
// normalizeQueryArray
// ---------------------------------------------------------------------------
describe("normalizeQueryArray", () => {
   it("returns undefined for undefined", () => {
      expect(normalizeQueryArray(undefined)).toBeUndefined();
   });

   it("returns undefined for null", () => {
      expect(normalizeQueryArray(null)).toBeUndefined();
   });

   it("wraps a single string in an array", () => {
      expect(normalizeQueryArray("table1")).toEqual(["table1"]);
   });

   it("passes through an array of strings", () => {
      expect(normalizeQueryArray(["a", "b"])).toEqual(["a", "b"]);
   });

   it("converts non-string array elements to strings", () => {
      expect(normalizeQueryArray([1, true])).toEqual(["1", "true"]);
   });

   it("converts a numeric value to a string array", () => {
      expect(normalizeQueryArray(42)).toEqual(["42"]);
   });
});

// ---------------------------------------------------------------------------
// listTablesForSchema – SQL generation & result grouping
// ---------------------------------------------------------------------------
describe("listTablesForSchema", () => {
   const columnRows = [
      { TABLE_NAME: "orders", COLUMN_NAME: "id", DATA_TYPE: "INTEGER" },
      { TABLE_NAME: "orders", COLUMN_NAME: "total", DATA_TYPE: "DECIMAL" },
      { TABLE_NAME: "customers", COLUMN_NAME: "id", DATA_TYPE: "INTEGER" },
      { TABLE_NAME: "customers", COLUMN_NAME: "name", DATA_TYPE: "VARCHAR" },
   ];

   describe("mysql", () => {
      const conn: ApiConnection = {
         name: "test",
         type: "mysql",
         mysqlConnection: {
            host: "localhost",
            port: 3306,
            user: "root",
            password: "",
            database: "testdb",
         },
      };

      it("queries INFORMATION_SCHEMA.COLUMNS and groups into ApiTable[]", async () => {
         const m = mockConnection(columnRows);
         const tables = await listTablesForSchema(conn, "testdb", m.conn);

         expect(m.lastSQL).toContain("information_schema.columns");
         expect(m.lastSQL).toContain("table_schema = 'testdb'");
         expect(tables).toHaveLength(2);
         expect(tables[0].resource).toBe("testdb.orders");
         expect(tables[0].columns).toEqual([
            { name: "id", type: "integer" },
            { name: "total", type: "decimal" },
         ]);
         expect(tables[1].resource).toBe("testdb.customers");
      });

      it("includes IN filter when tableNames provided", async () => {
         const m = mockConnection(columnRows.slice(0, 2));
         await listTablesForSchema(conn, "testdb", m.conn, ["orders"]);
         expect(m.lastSQL).toContain("AND TABLE_NAME IN ('orders')");
      });

      it("omits IN filter when tableNames is undefined", async () => {
         const m = mockConnection(columnRows);
         await listTablesForSchema(conn, "testdb", m.conn);
         expect(m.lastSQL).not.toContain("IN (");
      });
   });

   describe("postgres", () => {
      const conn: ApiConnection = {
         name: "test",
         type: "postgres",
         postgresConnection: {
            host: "localhost",
            port: 5432,
            userName: "postgres",
            password: "",
            databaseName: "testdb",
         },
      };

      it("queries information_schema.columns wrapped in row_to_json", async () => {
         const m = mockConnection(columnRows);
         const tables = await listTablesForSchema(conn, "public", m.conn);

         expect(m.lastSQL).toContain("row_to_json");
         expect(m.lastSQL).toContain("information_schema.columns");
         expect(m.lastSQL).toContain("table_schema = 'public'");
         expect(tables).toHaveLength(2);
         expect(tables[0].resource).toBe("public.orders");
      });

      it("includes IN filter when tableNames provided", async () => {
         const m = mockConnection([]);
         await listTablesForSchema(conn, "public", m.conn, ["orders"]);
         expect(m.lastSQL).toContain("AND table_name IN ('orders')");
      });
   });

   describe("snowflake", () => {
      const conn: ApiConnection = {
         name: "test",
         type: "snowflake",
         snowflakeConnection: {
            account: "test_account",
            username: "user",
            password: "pass",
            database: "MY_DB",
            schema: "PUBLIC",
         },
      };

      it("queries DATABASE.INFORMATION_SCHEMA.COLUMNS", async () => {
         const m = mockConnection(columnRows);
         const tables = await listTablesForSchema(conn, "MY_DB.PUBLIC", m.conn);

         expect(m.lastSQL).toContain("MY_DB.INFORMATION_SCHEMA.COLUMNS");
         expect(m.lastSQL).toContain("TABLE_SCHEMA = 'PUBLIC'");
         expect(tables).toHaveLength(2);
         expect(tables[0].resource).toBe("MY_DB.PUBLIC.orders");
      });

      it("falls back to connection database when schema is unqualified", async () => {
         const m = mockConnection(columnRows);
         const tables = await listTablesForSchema(conn, "PUBLIC", m.conn);

         expect(m.lastSQL).toContain("MY_DB.INFORMATION_SCHEMA.COLUMNS");
         expect(tables[0].resource).toBe("MY_DB.PUBLIC.orders");
      });

      it("includes IN filter when tableNames provided", async () => {
         const m = mockConnection([]);
         await listTablesForSchema(conn, "MY_DB.PUBLIC", m.conn, [
            "orders",
            "customers",
         ]);
         expect(m.lastSQL).toContain(
            "AND TABLE_NAME IN ('orders', 'customers')",
         );
      });
   });

   describe("trino", () => {
      it("uses catalog-prefixed information_schema.columns", async () => {
         const conn: ApiConnection = {
            name: "test",
            type: "trino",
            trinoConnection: {
               server: "localhost",
               port: 8080,
               catalog: "hive",
            },
         };
         const m = mockConnection(columnRows);
         const tables = await listTablesForSchema(conn, "default", m.conn);

         expect(m.lastSQL).toContain("hive.information_schema.columns");
         expect(m.lastSQL).toContain("table_schema = 'default'");
         expect(tables[0].resource).toBe("hive.default.orders");
      });

      it("extracts catalog from schemaName when no explicit catalog", async () => {
         const conn: ApiConnection = {
            name: "test",
            type: "trino",
            trinoConnection: { server: "localhost", port: 8080 },
         };
         const m = mockConnection(columnRows);
         const tables = await listTablesForSchema(conn, "hive.default", m.conn);

         expect(m.lastSQL).toContain("hive.information_schema.columns");
         expect(m.lastSQL).toContain("table_schema = 'default'");
         expect(tables[0].resource).toBe("hive.default.orders");
      });
   });

   describe("databricks", () => {
      it("uses defaultCatalog-prefixed information_schema.columns", async () => {
         const conn: ApiConnection = {
            name: "test",
            type: "databricks",
            databricksConnection: {
               host: "dbc.cloud.databricks.com",
               path: "/sql/1.0/warehouses/abc",
               token: "dapi",
               defaultCatalog: "main",
            },
         };
         const m = mockConnection(columnRows);
         const tables = await listTablesForSchema(conn, "default", m.conn);

         expect(m.lastSQL).toContain("main.information_schema.columns");
         expect(m.lastSQL).toContain("table_schema = 'default'");
         expect(tables[0].resource).toBe("main.default.orders");
      });

      it("extracts catalog from schemaName when no defaultCatalog", async () => {
         const conn: ApiConnection = {
            name: "test",
            type: "databricks",
            databricksConnection: {
               host: "dbc.cloud.databricks.com",
               path: "/sql/1.0/warehouses/abc",
               token: "dapi",
            },
         };
         const m = mockConnection(columnRows);
         const tables = await listTablesForSchema(conn, "main.default", m.conn);

         expect(m.lastSQL).toContain("main.information_schema.columns");
         expect(m.lastSQL).toContain("table_schema = 'default'");
         expect(tables[0].resource).toBe("main.default.orders");
      });

      it("includes IN filter when tableNames provided", async () => {
         const conn: ApiConnection = {
            name: "test",
            type: "databricks",
            databricksConnection: {
               host: "dbc.cloud.databricks.com",
               path: "/sql/1.0/warehouses/abc",
               token: "dapi",
               defaultCatalog: "main",
            },
         };
         const m = mockConnection(columnRows);
         await listTablesForSchema(conn, "default", m.conn, ["orders"]);
         expect(m.lastSQL).toContain("table_name IN ('orders')");
      });
   });

   describe("duckdb", () => {
      const conn: ApiConnection = {
         name: "test",
         type: "duckdb",
         duckdbConnection: {},
      };

      it("queries information_schema.columns with catalog and schema", async () => {
         const rows = columnRows.map((r) => ({
            table_name: r.TABLE_NAME,
            column_name: r.COLUMN_NAME,
            data_type: r.DATA_TYPE,
         }));
         const m = mockConnection(rows);
         const tables = await listTablesForSchema(conn, "memory.main", m.conn);

         expect(m.lastSQL).toContain("information_schema.columns");
         expect(m.lastSQL).toContain("table_schema = 'main'");
         expect(m.lastSQL).toContain("table_catalog = 'memory'");
         expect(tables).toHaveLength(2);
         expect(tables[0].resource).toBe("memory.main.orders");
      });
   });

   describe("motherduck", () => {
      const conn: ApiConnection = {
         name: "test",
         type: "motherduck",
         motherduckConnection: { accessToken: "fake" },
      };

      it("queries information_schema.columns", async () => {
         const m = mockConnection(columnRows);
         const tables = await listTablesForSchema(conn, "main", m.conn);

         expect(m.lastSQL).toContain("information_schema.columns");
         expect(m.lastSQL).toContain("table_schema = 'main'");
         expect(tables).toHaveLength(2);
         expect(tables[0].resource).toBe("main.orders");
      });
   });

   describe("ducklake", () => {
      const conn: ApiConnection = {
         name: "test",
         type: "ducklake",
         ducklakeConnection: {
            catalog: {
               postgresConnection: {
                  host: "localhost",
                  port: 5432,
                  userName: "postgres",
                  password: "",
                  databaseName: "testdb",
               },
            },
            storage: { bucketUrl: "s3://bucket" },
         },
      };

      it("queries information_schema.columns with catalog and schema", async () => {
         const m = mockConnection(columnRows);
         const tables = await listTablesForSchema(
            conn,
            "mycat.myschema",
            m.conn,
         );

         expect(m.lastSQL).toContain("information_schema.columns");
         expect(m.lastSQL).toContain("table_schema = 'myschema'");
         expect(m.lastSQL).toContain("table_catalog = 'mycat'");
         expect(tables[0].resource).toBe("mycat.myschema.orders");
      });
   });

   describe("column grouping", () => {
      it("lowercases data types", async () => {
         const conn: ApiConnection = {
            name: "test",
            type: "mysql",
            mysqlConnection: {
               host: "localhost",
               port: 3306,
               user: "root",
               password: "",
               database: "testdb",
            },
         };
         const m = mockConnection([
            {
               TABLE_NAME: "t",
               COLUMN_NAME: "col",
               DATA_TYPE: "VARCHAR(255)",
            },
         ]);
         const tables = await listTablesForSchema(conn, "testdb", m.conn);
         expect(tables[0]?.columns?.[0]?.type).toBe("varchar(255)");
      });

      it("returns empty array when no rows", async () => {
         const conn: ApiConnection = {
            name: "test",
            type: "postgres",
            postgresConnection: {
               host: "localhost",
               port: 5432,
               userName: "postgres",
               password: "",
               databaseName: "testdb",
            },
         };
         const m = mockConnection([]);
         const tables = await listTablesForSchema(conn, "public", m.conn);
         expect(tables).toEqual([]);
      });
   });

   describe("error handling", () => {
      it("throws for unsupported connection type", async () => {
         const conn = {
            name: "test",
            type: "unsupported",
         } as unknown as ApiConnection;
         const m = mockConnection();
         await expect(
            listTablesForSchema(conn, "schema", m.conn),
         ).rejects.toThrow("Unsupported connection type");
      });

      it("throws when duckdb schema is not qualified", async () => {
         const conn: ApiConnection = {
            name: "test",
            type: "duckdb",
            duckdbConnection: {},
         };
         const m = mockConnection();
         await expect(
            listTablesForSchema(conn, "main", m.conn),
         ).rejects.toThrow('must be qualified as "catalog.schema"');
      });

      it("throws when snowflake schema is unqualified and no database configured", async () => {
         const conn: ApiConnection = {
            name: "test",
            type: "snowflake",
            snowflakeConnection: {
               account: "test_account",
               username: "user",
               password: "pass",
            },
         };
         const m = mockConnection();
         await expect(
            listTablesForSchema(conn, "PUBLIC", m.conn),
         ).rejects.toThrow("Cannot resolve database");
      });
   });

   describe("ducklake schema prefixing", () => {
      const conn: ApiConnection = {
         name: "myconn",
         type: "ducklake",
         ducklakeConnection: {
            catalog: {
               postgresConnection: {
                  host: "localhost",
                  port: 5432,
                  userName: "postgres",
                  password: "",
                  databaseName: "testdb",
               },
            },
            storage: { bucketUrl: "s3://bucket" },
         },
      };

      it("prefixes bare schema name with connection name", async () => {
         const m = mockConnection([]);
         await listTablesForSchema(conn, "main", m.conn);
         expect(m.lastSQL).toContain("table_catalog = 'myconn'");
         expect(m.lastSQL).toContain("table_schema = 'main'");
      });

      it("uses provided catalog when schema is already qualified", async () => {
         const m = mockConnection([]);
         await listTablesForSchema(conn, "othercat.myschema", m.conn);
         expect(m.lastSQL).toContain("table_catalog = 'othercat'");
         expect(m.lastSQL).toContain("table_schema = 'myschema'");
      });
   });
});

// ---------------------------------------------------------------------------
// getSchemasForConnection – schema listing
// ---------------------------------------------------------------------------
describe("getSchemasForConnection", () => {
   describe("postgres", () => {
      const conn: ApiConnection = {
         name: "test",
         type: "postgres",
         postgresConnection: {
            host: "localhost",
            port: 5432,
            userName: "postgres",
            password: "",
            databaseName: "testdb",
         },
      };

      it("queries information_schema.schemata wrapped in row_to_json", async () => {
         const rows = [
            { schema_name: "public" },
            { schema_name: "information_schema" },
            { schema_name: "pg_catalog" },
            { schema_name: "app" },
         ];
         const m = mockConnection(rows);
         const schemas = await getSchemasForConnection(conn, m.conn);

         expect(m.lastSQL).toContain("row_to_json");
         expect(m.lastSQL).toContain("information_schema.schemata");
         expect(schemas).toHaveLength(4);
         expect(schemas.find((s) => s.name === "public")?.isDefault).toBe(true);
         expect(
            schemas.find((s) => s.name === "information_schema")?.isHidden,
         ).toBe(true);
         expect(schemas.find((s) => s.name === "pg_catalog")?.isHidden).toBe(
            true,
         );
         expect(schemas.find((s) => s.name === "app")?.isHidden).toBe(false);
      });
   });

   describe("mysql", () => {
      it("returns a single schema from the connection database", async () => {
         const conn: ApiConnection = {
            name: "test",
            type: "mysql",
            mysqlConnection: {
               host: "localhost",
               port: 3306,
               user: "root",
               password: "",
               database: "mydb",
            },
         };
         const m = mockConnection();
         const schemas = await getSchemasForConnection(conn, m.conn);

         expect(schemas).toHaveLength(1);
         expect(schemas[0].name).toBe("mydb");
         expect(schemas[0].isDefault).toBe(true);
      });
   });

   describe("snowflake", () => {
      it("queries INFORMATION_SCHEMA.SCHEMATA with database filter", async () => {
         const conn: ApiConnection = {
            name: "test",
            type: "snowflake",
            snowflakeConnection: {
               account: "test_account",
               username: "user",
               password: "pass",
               database: "MY_DB",
               schema: "PUBLIC",
            },
         };
         const rows = [
            {
               CATALOG_NAME: "MY_DB",
               SCHEMA_NAME: "PUBLIC",
               SCHEMA_OWNER: "SYSADMIN",
            },
            {
               CATALOG_NAME: "MY_DB",
               SCHEMA_NAME: "INFORMATION_SCHEMA",
               SCHEMA_OWNER: "",
            },
         ];
         const m = mockConnection(rows);
         const schemas = await getSchemasForConnection(conn, m.conn);

         expect(m.lastSQL).toContain("INFORMATION_SCHEMA.SCHEMATA");
         expect(m.lastSQL).toContain("CATALOG_NAME = 'MY_DB'");
         expect(schemas).toHaveLength(2);
         expect(schemas.find((s) => s.name === "MY_DB.PUBLIC")?.isDefault).toBe(
            true,
         );
         expect(
            schemas.find((s) => s.name === "MY_DB.INFORMATION_SCHEMA")
               ?.isHidden,
         ).toBe(true);
      });
   });

   describe("duckdb", () => {
      it("queries information_schema.schemata and hides system schemas", async () => {
         const conn: ApiConnection = {
            name: "test",
            type: "duckdb",
            duckdbConnection: {},
         };
         const rows = [
            { catalog_name: "main", schema_name: "main" },
            { catalog_name: "main", schema_name: "information_schema" },
            { catalog_name: "system", schema_name: "main" },
         ];
         const m = mockConnection(rows);
         const schemas = await getSchemasForConnection(conn, m.conn);

         expect(schemas).toHaveLength(3);
         const mainMain = schemas.find((s) => s.name === "main.main");
         expect(mainMain?.isDefault).toBe(true);
         expect(mainMain?.isHidden).toBe(false);
         expect(
            schemas.find((s) => s.name === "main.information_schema")?.isHidden,
         ).toBe(true);
         expect(schemas.find((s) => s.name === "system.main")?.isHidden).toBe(
            true,
         );
      });
   });

   describe("motherduck", () => {
      it("queries information_schema.schemata with optional database filter", async () => {
         const conn: ApiConnection = {
            name: "test",
            type: "motherduck",
            motherduckConnection: { accessToken: "fake", database: "mydb" },
         };
         const rows = [
            { schema_name: "main" },
            { schema_name: "information_schema" },
         ];
         const m = mockConnection(rows);
         const schemas = await getSchemasForConnection(conn, m.conn);

         expect(m.lastSQL).toContain("catalog_name = 'mydb'");
         expect(schemas).toHaveLength(2);
         expect(schemas.find((s) => s.name === "main")?.isDefault).toBe(true);
         expect(
            schemas.find((s) => s.name === "information_schema")?.isHidden,
         ).toBe(true);
      });
   });

   describe("ducklake", () => {
      it("queries information_schema.schemata filtered by connection name", async () => {
         const conn: ApiConnection = {
            name: "myconn",
            type: "ducklake",
            ducklakeConnection: {
               catalog: {
                  postgresConnection: {
                     host: "localhost",
                     port: 5432,
                     userName: "postgres",
                     password: "",
                     databaseName: "testdb",
                  },
               },
               storage: { bucketUrl: "s3://bucket" },
            },
         };
         const rows = [
            { schema_name: "main" },
            { schema_name: "public" },
            { schema_name: "internal" },
         ];
         const m = mockConnection(rows);
         const schemas = await getSchemasForConnection(conn, m.conn);

         expect(m.lastSQL).toContain("catalog_name = 'myconn'");
         expect(schemas).toHaveLength(3);
         expect(schemas.find((s) => s.name === "main")?.isHidden).toBe(false);
         expect(schemas.find((s) => s.name === "public")?.isHidden).toBe(false);
         expect(schemas.find((s) => s.name === "internal")?.isHidden).toBe(
            true,
         );
      });
   });

   describe("trino", () => {
      it("queries catalog.information_schema.schemata when catalog is set", async () => {
         const conn: ApiConnection = {
            name: "test",
            type: "trino",
            trinoConnection: {
               server: "localhost",
               port: 8080,
               catalog: "hive",
            },
         };
         const rows = [
            { schema_name: "default" },
            { schema_name: "information_schema" },
         ];
         const m = mockConnection(rows);
         const schemas = await getSchemasForConnection(conn, m.conn);

         expect(m.lastSQL).toContain("hive.information_schema.schemata");
         expect(schemas).toHaveLength(2);
         expect(schemas.find((s) => s.name === "default")?.isHidden).toBe(
            false,
         );
         expect(
            schemas.find((s) => s.name === "information_schema")?.isHidden,
         ).toBe(true);
      });
   });

   describe("databricks", () => {
      it("queries catalog.information_schema.schemata when defaultCatalog is set", async () => {
         const conn: ApiConnection = {
            name: "test",
            type: "databricks",
            databricksConnection: {
               host: "dbc.cloud.databricks.com",
               path: "/sql/1.0/warehouses/abc",
               token: "dapi",
               defaultCatalog: "main",
               defaultSchema: "default",
            },
         };
         const rows = [
            { schema_name: "default" },
            { schema_name: "information_schema" },
         ];
         const m = mockConnection(rows);
         const schemas = await getSchemasForConnection(conn, m.conn);

         expect(m.lastSQL).toContain("main.information_schema.schemata");
         expect(schemas).toHaveLength(2);
         expect(schemas.find((s) => s.name === "default")?.isDefault).toBe(
            true,
         );
         expect(
            schemas.find((s) => s.name === "information_schema")?.isHidden,
         ).toBe(true);
      });

      it("falls back to SHOW CATALOGS when defaultCatalog is unset", async () => {
         const conn: ApiConnection = {
            name: "test",
            type: "databricks",
            databricksConnection: {
               host: "dbc.cloud.databricks.com",
               path: "/sql/1.0/warehouses/abc",
               token: "dapi",
            },
         };
         // First runSQL returns catalog list; subsequent runSQL calls (one
         // per catalog) return schema rows. We use a dedicated mock so we
         // can switch behavior across calls.
         let callIndex = 0;
         const calls: string[] = [];
         const fakeConn = {
            runSQL: async (sql: string) => {
               calls.push(sql);
               if (callIndex++ === 0) {
                  return {
                     rows: [{ catalog: "main" }, { catalog: "samples" }],
                  };
               }
               return { rows: [{ schema_name: "default" }] };
            },
         } as unknown as Connection;

         const schemas = await getSchemasForConnection(conn, fakeConn);

         expect(calls[0]).toContain("SHOW CATALOGS");
         expect(
            calls.some((c) => c.includes("main.information_schema.schemata")),
         ).toBe(true);
         expect(
            calls.some((c) =>
               c.includes("samples.information_schema.schemata"),
            ),
         ).toBe(true);
         // Two catalogs each contribute one schema → catalog-qualified names.
         expect(schemas.map((s) => s.name)).toEqual([
            "main.default",
            "samples.default",
         ]);
      });

      it("warns and continues when a catalog rejects information_schema", async () => {
         const conn: ApiConnection = {
            name: "test",
            type: "databricks",
            databricksConnection: {
               host: "dbc.cloud.databricks.com",
               path: "/sql/1.0/warehouses/abc",
               token: "dapi",
            },
         };
         let callIndex = 0;
         const fakeConn = {
            runSQL: async (sql: string) => {
               if (callIndex++ === 0) {
                  return { rows: [{ catalog: "denied" }, { catalog: "ok" }] };
               }
               if (sql.includes("denied")) {
                  throw new Error("USE CATALOG denied");
               }
               return { rows: [{ schema_name: "default" }] };
            },
         } as unknown as Connection;

         const schemas = await getSchemasForConnection(conn, fakeConn);

         // Denied catalog is skipped, ok catalog contributes its schema.
         expect(schemas).toHaveLength(1);
         expect(schemas[0].name).toBe("ok.default");
      });
   });

   it("throws for unsupported connection type", async () => {
      const conn = {
         name: "test",
         type: "unsupported",
      } as unknown as ApiConnection;
      const m = mockConnection();
      await expect(getSchemasForConnection(conn, m.conn)).rejects.toThrow(
         "Unsupported connection type",
      );
   });
});

// ---------------------------------------------------------------------------
// extractErrorDataFromError
// ---------------------------------------------------------------------------
describe("extractErrorDataFromError", () => {
   it("extracts message from Error instance", () => {
      const result = extractErrorDataFromError(new Error("boom"));
      expect(result.error).toBe("boom");
   });

   it("converts string errors", () => {
      const result = extractErrorDataFromError("something went wrong");
      expect(result.error).toBe("something went wrong");
   });

   it("converts non-string non-Error values", () => {
      const result = extractErrorDataFromError(42);
      expect(result.error).toBe("42");
   });

   it("extracts task property when present", () => {
      const err = Object.assign(new Error("fail"), { task: { id: 1 } });
      const result = extractErrorDataFromError(err);
      expect(result.error).toBe("fail");
      expect(result.task).toEqual({ id: 1 });
   });
});

// ---------------------------------------------------------------------------
// publisher proxy introspection
//
// The remote dataplane is the oracle for introspection correctness and is not
// simulated here. These tests cover only this server's own logic: the request
// URL it builds, the auth header, the bare-name table filter, and how it
// surfaces a non-ok / unreachable dataplane (credentials redacted from errors).
// ---------------------------------------------------------------------------
describe("publisher proxy introspection", () => {
   const realFetch = globalThis.fetch;
   let lastRequest:
      | { url: string; headers: Record<string, string> }
      | undefined;

   function stubFetch(
      payload: unknown,
      init: { ok?: boolean; status?: number; body?: string } = {},
   ) {
      lastRequest = undefined;
      const ok = init.ok ?? true;
      globalThis.fetch = (async (url: unknown, opts?: RequestInit) => {
         lastRequest = {
            url: String(url),
            headers: (opts?.headers as Record<string, string>) ?? {},
         };
         return {
            ok,
            status: init.status ?? (ok ? 200 : 500),
            json: async () => payload,
            text: async () => init.body ?? "",
         } as Response;
      }) as typeof fetch;
   }

   afterEach(() => {
      globalThis.fetch = realFetch;
   });

   const conn: ApiConnection = {
      name: "analytics",
      type: "publisher",
      publisherConnection: {
         connectionUri:
            "https://org.data.example.com/api/v0/environments/proj/connections/analytics",
         accessToken: "jwt-token",
      },
   };
   // No malloyConnection is used for the publisher path.
   const noConn = undefined as unknown as Connection;

   it("forwards GET /schemas to the connectionUri with a bearer token", async () => {
      const schemas = [{ name: "main", isHidden: false, isDefault: true }];
      stubFetch(schemas);
      const result = await getSchemasForConnection(conn, noConn);

      expect(lastRequest?.url).toBe(
         "https://org.data.example.com/api/v0/environments/proj/connections/analytics/schemas",
      );
      expect(lastRequest?.headers["Authorization"]).toBe("Bearer jwt-token");
      expect(result).toEqual(schemas);
   });

   it("omits the Authorization header when no accessToken is configured", async () => {
      const tokenless: ApiConnection = {
         name: "analytics",
         type: "publisher",
         publisherConnection: {
            connectionUri:
               "https://org.data.example.com/api/v0/environments/proj/connections/analytics",
         },
      };
      stubFetch([]);
      await getSchemasForConnection(tokenless, noConn);
      expect(lastRequest?.headers["Authorization"]).toBeUndefined();
   });

   it("forwards GET /schemas/<schema>/tables and returns all tables when unfiltered", async () => {
      const tables = [
         { resource: "main.orders" },
         { resource: "main.customers" },
      ];
      stubFetch(tables);
      const result = await listTablesForSchema(conn, "main", noConn);

      expect(lastRequest?.url).toBe(
         "https://org.data.example.com/api/v0/environments/proj/connections/analytics/schemas/main/tables",
      );
      expect(result).toEqual(tables);
   });

   it("URL-encodes the schema name in the tables path", async () => {
      stubFetch([]);
      await listTablesForSchema(conn, "weird schema/name", noConn);
      expect(lastRequest?.url).toContain(
         "/schemas/weird%20schema%2Fname/tables",
      );
   });

   it("filters by bare table name across schema- and catalog-qualified resources", async () => {
      const tables = [
         { resource: "main.orders" }, // "<schema>.<table>"
         { resource: "cat.main.customers" }, // "<catalog>.<schema>.<table>"
         { resource: "main.events" },
      ];
      stubFetch(tables);
      const result = await listTablesForSchema(conn, "main", noConn, [
         "orders",
         "customers",
      ]);
      expect(result.map((t) => t.resource)).toEqual([
         "main.orders",
         "cat.main.customers",
      ]);
   });

   it("returns an empty list when the filter matches nothing (not all rows)", async () => {
      stubFetch([{ resource: "main.orders" }]);
      const result = await listTablesForSchema(conn, "main", noConn, [
         "nonexistent",
      ]);
      expect(result).toEqual([]);
   });

   it("throws an actionable error on a non-ok dataplane response", async () => {
      stubFetch(null, { ok: false, status: 403, body: "forbidden" });
      await expect(getSchemasForConnection(conn, noConn)).rejects.toThrow(
         "failed (403): forbidden",
      );
   });

   it("redacts URL-embedded credentials from error messages", async () => {
      const withCreds: ApiConnection = {
         name: "analytics",
         type: "publisher",
         publisherConnection: {
            connectionUri:
               "https://user:secret@org.data.example.com/api/v0/environments/proj/connections/analytics",
         },
      };
      stubFetch(null, { ok: false, status: 500, body: "boom" });
      await expect(getSchemasForConnection(withCreds, noConn)).rejects.toThrow(
         /Publisher dataplane request to https:\/\/org\.data\.example\.com.*failed \(500\)/,
      );
      await expect(
         getSchemasForConnection(withCreds, noConn),
      ).rejects.not.toThrow(/secret/);
   });

   it("surfaces a network-level failure with the connection name and no token", async () => {
      globalThis.fetch = (async () => {
         throw new Error("ECONNREFUSED");
      }) as unknown as typeof fetch;
      await expect(getSchemasForConnection(conn, noConn)).rejects.toThrow(
         'failed for connection "analytics": ECONNREFUSED',
      );
   });
});

// ---------------------------------------------------------------------------
// listTablesForSchema — bigquery (project-qualified dataset)
// ---------------------------------------------------------------------------
describe("listTablesForSchema bigquery", () => {
   const bqConnection = {
      name: "bq",
      type: "bigquery",
      bigqueryConnection: {
         defaultProjectId: "default-proj",
         serviceAccountKeyJson: JSON.stringify({ project_id: "default-proj" }),
      },
   } as unknown as ApiConnection;
   const malloyConn = {} as unknown as Connection;

   it("splits a project-qualified schema into a bare dataset id + projectId", async () => {
      bqDatasetCalls.length = 0;
      await listTablesForSchema(bqConnection, "myproj.myds", malloyConn);
      expect(bqDatasetCalls).toHaveLength(1);
      expect(bqDatasetCalls[0]?.id).toBe("myds");
      expect(bqDatasetCalls[0]?.options).toEqual({ projectId: "myproj" });
   });

   it("splits on the last dot so domain-scoped project ids survive", async () => {
      bqDatasetCalls.length = 0;
      await listTablesForSchema(
         bqConnection,
         "domain.com:proj.myds",
         malloyConn,
      );
      expect(bqDatasetCalls[0]?.id).toBe("myds");
      expect(bqDatasetCalls[0]?.options).toEqual({
         projectId: "domain.com:proj",
      });
   });

   it("passes a bare dataset name through unqualified", async () => {
      bqDatasetCalls.length = 0;
      await listTablesForSchema(bqConnection, "myds", malloyConn);
      expect(bqDatasetCalls).toHaveLength(1);
      expect(bqDatasetCalls[0]?.id).toBe("myds");
      expect(bqDatasetCalls[0]?.options).toBeUndefined();
   });
});

describe("SQL escaping and identifier validation", () => {
   it("escapes backslashes for exactly the dialects that treat them as escapes", async () => {
      const { sqlLiteral } = await import("./db_utils");
      // From Malloy's own dialect layer (stringLiteralStyle). Getting this set
      // wrong is what left Snowflake and Databricks injectable after the fix
      // that was supposed to close them.
      for (const dialect of ["mysql", "snowflake", "databricks"]) {
         expect(sqlLiteral("x\\' OR 1=1 #", dialect)).toBe("x\\\\'' OR 1=1 #");
      }
      // Doubled dialects: a backslash is an ordinary character, so doubling it
      // would corrupt a legitimate name like a Postgres schema `data\archive`.
      for (const dialect of [
         "postgres",
         "duckdb",
         "motherduck",
         "ducklake",
         "trino",
      ]) {
         expect(sqlLiteral("data\\archive", dialect)).toBe("data\\archive");
      }
      // Quote doubling applies everywhere, including with no dialect given.
      expect(sqlLiteral("o'brien", "snowflake")).toBe("o''brien");
      expect(sqlLiteral("o'brien")).toBe("o''brien");
      expect(sqlLiteral("plain", "postgres")).toBe("plain");
   });

   it("refuses to build a BigQuery literal rather than mis-escaping one", async () => {
      const { sqlLiteral } = await import("./db_utils");
      // GoogleSQL is backslash-only: it does not read '' as an escaped quote,
      // and it does not concatenate adjacent literals either, so the doubling
      // this function always applies would not parse. Nothing reaches it today
      // (BigQuery introspects through its client, building no SQL), so this
      // pins the failure DIRECTION for whoever adds the first BigQuery SQL
      // path: throw, rather than inherit an escape rule that does not hold.
      expect(() => sqlLiteral("o'brien", "bigquery")).toThrow(
         /Cannot build a SQL literal for "bigquery"/,
      );
   });

   it("rejects an identifier that is not a plain name", async () => {
      const { assertSafeSqlIdentifier } = await import("./db_utils");
      // The payload that would otherwise reach a raw identifier position and
      // return real row values from a tool that promises it returns none.
      expect(() =>
         assertSafeSqlIdentifier(
            "(SELECT c1 AS TABLE_NAME FROM customers) x --",
            "catalog name",
         ),
      ).toThrow();
      expect(() => assertSafeSqlIdentifier("a.b", "catalog name")).toThrow();
      expect(() => assertSafeSqlIdentifier("", "catalog name")).toThrow();
      expect(() => assertSafeSqlIdentifier("1abc", "catalog name")).toThrow();
   });

   it("accepts the identifier shapes real catalogs use", async () => {
      const { assertSafeSqlIdentifier } = await import("./db_utils");
      for (const ok of ["memory", "my_catalog", "_x", "c$1"]) {
         expect(assertSafeSqlIdentifier(ok, "catalog name")).toBe(ok);
      }
   });
});

describe("identifier rejection survives the dialect catch blocks", () => {
   // The guard was wired INSIDE a try whose catch rewrapped everything as a
   // plain Error, so on Snowflake, Trino and Databricks a rejected identifier
   // reached the caller as an "unexpected internal error, try again later".
   // Testing the helper alone (above) would not have caught that: all three
   // call sites could be deleted and this file would stay green.
   const dialects = [
      { type: "snowflake", conn: { snowflakeConnection: { database: "db" } } },
      { type: "trino", conn: { trinoConnection: {} } },
      { type: "databricks", conn: { databricksConnection: {} } },
   ] as const;

   for (const { type, conn } of dialects) {
      it(`surfaces an InvalidArgumentError, not a wrapped Error, on ${type}`, async () => {
         const { listTablesForSchema } = await import("./db_utils");
         const { BadRequestError, InvalidArgumentError } = await import(
            "../errors"
         );
         const malloyConnection = {
            runSQL: async () => {
               throw new Error("should not be reached");
            },
         };
         let thrown: unknown;
         try {
            await listTablesForSchema(
               { name: "c", type, ...conn } as never,
               // A catalog segment that is not a plain identifier.
               "evil; DROP TABLE t; --.public",
               malloyConnection as never,
            );
         } catch (error) {
            thrown = error;
         }
         // The subclass is what keeps the MCP layer from answering a bad
         // argument with Malloy syntax advice; the base class is what keeps it
         // an HTTP 400. Both matter, so both are asserted.
         expect(thrown).toBeInstanceOf(InvalidArgumentError);
         expect(thrown).toBeInstanceOf(BadRequestError);
      });
   }
});

describe("catalog names in the schema listings are validated too", () => {
   // The last instance of the raw-interpolation shape, in getSchemasForTrino
   // and getSchemasForDatabricks. Config-derived rather than caller-derived, so
   // not a vulnerability, but an untested fix is how the previous round's
   // extracted-but-never-wired helper got through: assert runSQL is never
   // reached, not merely that something threw.
   const dialects = [
      {
         type: "trino",
         conn: { trinoConnection: { catalog: "evil; DROP TABLE t; --" } },
      },
      {
         type: "databricks",
         conn: {
            databricksConnection: { defaultCatalog: "evil; DROP TABLE t; --" },
         },
      },
   ] as const;

   for (const { type, conn } of dialects) {
      it(`rejects an unsafe configured catalog on ${type} before running SQL`, async () => {
         const { getSchemasForConnection } = await import("./db_utils");
         const { InvalidArgumentError } = await import("../errors");
         let ranSQL = false;
         const malloyConnection = {
            runSQL: async () => {
               ranSQL = true;
               return { rows: [] };
            },
         };
         let thrown: unknown;
         try {
            await getSchemasForConnection(
               { name: "c", type, ...conn } as never,
               malloyConnection as never,
            );
         } catch (error) {
            thrown = error;
         }
         expect(thrown).toBeInstanceOf(InvalidArgumentError);
         expect(ranSQL).toBe(false);
      });
   }
});

describe("an unclassified dialect fails loudly", () => {
   // The classification has been wrong three times in this file's history. A
   // dialect added to the dispatch switch but not to either escape set must not
   // inherit quote-doubling silently, because doubling alone on a
   // backslash-escaping dialect is exploitable rather than merely cosmetic.
   it("throws for a type that is given but unrecognised", async () => {
      const { sqlLiteral } = await import("./db_utils");
      expect(() => sqlLiteral("x", "some_new_warehouse")).toThrow(
         /Unclassified SQL dialect/,
      );
   });

   it("still doubles quotes when no dialect is supplied at all", async () => {
      const { sqlLiteral } = await import("./db_utils");
      expect(sqlLiteral("o'brien")).toBe("o''brien");
   });

   // Kyle verified this by hand in review, against api-doc.yaml, which is the
   // right source but the wrong mechanism: the next dialect added to the enum
   // would inherit quote-doubling silently and nobody would be checking. Read
   // the enum rather than restating it, so the two cannot drift. Behavioural on
   // purpose, so it needs no new exports from the module under test.
   it("classifies every connection type in the api-doc enum", async () => {
      const { sqlLiteral } = await import("./db_utils");
      const { readFileSync } = await import("node:fs");
      const apiDoc = readFileSync(
         new URL("../../../../api-doc.yaml", import.meta.url),
         "utf8",
      );
      const start = apiDoc.indexOf("      description: Database connection");
      expect(start).toBeGreaterThan(-1);
      const enumAt = apiDoc.indexOf("enum:", start);
      const types = [
         ...apiDoc
            .slice(enumAt, apiDoc.indexOf("]", enumAt))
            .matchAll(/^\s+(\w+),$/gm),
      ].map((m) => m[1]);
      // Guard the extraction itself: a regex that silently matched nothing
      // would make this test vacuously pass.
      expect(types).toContain("postgres");
      expect(types.length).toBeGreaterThanOrEqual(10);

      for (const type of types) {
         // Either it produces a literal, or it refuses for a NAMED reason.
         // "Unclassified" means nobody decided, which is the state this test
         // exists to prevent.
         try {
            expect(typeof sqlLiteral("o'brien", type)).toBe("string");
         } catch (error) {
            expect((error as Error).message).toContain(
               "Cannot build a SQL literal",
            );
            expect((error as Error).message).not.toContain("Unclassified");
         }
      }
   });
});

describe("the identifier guard rejects the SQL comment token", () => {
   // A hyphen was in the accepted charset and looked harmless. Two of them are
   // the SQL line-comment token, so `a--b.public` executed as `FROM a` with the
   // rest of the query commented away, which can return row values from a tool
   // documented never to return one.
   it("rejects a doubled hyphen, which comments out the rest of the query", async () => {
      const { assertSafeSqlIdentifier } = await import("./db_utils");
      expect(() => assertSafeSqlIdentifier("a--b", "catalog name")).toThrow();
      expect(() =>
         assertSafeSqlIdentifier("cat--x", "database name"),
      ).toThrow();
   });

   it("rejects a single hyphen too, since no dialect reaching here accepts one bare", async () => {
      const { assertSafeSqlIdentifier } = await import("./db_utils");
      expect(() => assertSafeSqlIdentifier("MY-CAT", "catalog name")).toThrow();
   });
});

describe("the rejection message matches what the guard actually accepts", () => {
   // The charset lost its hyphen but this message kept advertising one, so an
   // agent was told its rejected value matched the stated format and had no
   // repair available: the retry loop, arriving through the error text. Three
   // times in this feature's history the code changed and what it says about
   // itself did not, so the two are pinned together here.
   it("does not offer a character the guard rejects", async () => {
      const { assertSafeSqlIdentifier } = await import("./db_utils");
      let message = "";
      try {
         assertSafeSqlIdentifier("bad value", "catalog name");
      } catch (error) {
         message = (error as Error).message;
      }
      expect(message).toContain("plain identifier");
      // Every character class the message names must genuinely be accepted.
      for (const [named, sample] of [
         ["letters", "abc"],
         ["digits", "a1"],
         ["underscore", "a_b"],
         ["dollar", "a$b"],
      ] as const) {
         expect(message).toContain(named);
         expect(assertSafeSqlIdentifier(sample, "x")).toBe(sample);
      }
      expect(message).not.toContain("hyphen");
   });
});
