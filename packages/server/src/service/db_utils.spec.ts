import { describe, expect, it, mock } from "bun:test";

// Stub the missing optional dependency so db_utils.ts can be imported
mock.module("@azure/identity", () => ({
   ClientSecretCredential: class {},
}));
mock.module("@azure/storage-blob", () => ({
   ContainerClient: class {},
}));
mock.module("@google-cloud/bigquery", () => ({
   BigQuery: class {},
}));

import { Connection } from "@malloydata/malloy";
import { normalizeQueryArray } from "../server";
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
         const tables = await listTablesForSchema(
            conn,
            "main.default",
            m.conn,
         );

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
         expect(schemas.find((s) => s.name === "default")?.isDefault).toBe(true);
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
                  return { rows: [{ catalog: "main" }, { catalog: "samples" }] };
               }
               return { rows: [{ schema_name: "default" }] };
            },
         } as unknown as Connection;

         const schemas = await getSchemasForConnection(conn, fakeConn);

         expect(calls[0]).toContain("SHOW CATALOGS");
         expect(calls.some((c) => c.includes("main.information_schema.schemata")))
            .toBe(true);
         expect(calls.some((c) =>
            c.includes("samples.information_schema.schemata"),
         )).toBe(true);
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
