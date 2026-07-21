import { describe, expect, it } from "bun:test";
import { BadRequestError } from "../errors";
import type { components } from "../api";
import {
   assertStorageServeShapeCompiles,
   buildSourceIntoStorage,
   dropStorageTable,
   dropStorageTableSql,
   logicalViewSql,
   passthroughSourceType,
   wrapPassthrough,
} from "./materialization_build_session";

type ApiConnection = components["schemas"]["Connection"];

describe("assertStorageServeShapeCompiles (build-time servability gate)", () => {
   it("passes for a well-formed captured schema (compiles the serve shape in DuckDB)", async () => {
      await expect(
         assertStorageServeShapeCompiles({
            destinationName: "lake",
            sourceName: "daily_orders",
            virtualHandle: "se_1",
            physicalTableName: "daily_orders__mabc123",
            schema: [
               { name: "order_date", type: "DATE" },
               { name: "order_count", type: "BIGINT" },
               { name: "total_amount", type: "DOUBLE" },
            ],
         }),
      ).resolves.toBeUndefined();
   });
});

describe("dropStorageTableSql", () => {
   it("drops the content-addressed table, catalog-qualified and quoted", () => {
      expect(dropStorageTableSql("lake", "daily__mabc123")).toBe(
         'DROP TABLE IF EXISTS "lake"."daily__mabc123"',
      );
   });
   it("qualifies a dotted schema.table name", () => {
      expect(dropStorageTableSql("lake", "analytics.daily__mabc123")).toBe(
         'DROP TABLE IF EXISTS "lake"."analytics"."daily__mabc123"',
      );
   });
});

describe("dropStorageTable gating (no session I/O before the gate)", () => {
   it("refuses a non-DuckDB-family destination before creating a session", async () => {
      await expect(
         dropStorageTable({
            destinationName: "bq_dest",
            destinationConnection: {
               name: "bq_dest",
               type: "bigquery",
            } as ApiConnection,
            physicalTableName: "t__mabc",
            environmentPath: "/tmp/env",
         }),
      ).rejects.toThrow(/must be one of/i);
   });
});

describe("logicalViewSql", () => {
   it("points the logical name at the physical table, catalog-qualified and quoted", () => {
      expect(logicalViewSql("lake", "daily", "daily__mabc123")).toBe(
         'CREATE OR REPLACE VIEW "lake"."daily" AS SELECT * FROM "lake"."daily__mabc123"',
      );
   });

   it("qualifies a dotted schema.table logical name", () => {
      expect(
         logicalViewSql("lake", "analytics.daily", "analytics.daily__mabc123"),
      ).toBe(
         'CREATE OR REPLACE VIEW "lake"."analytics"."daily" AS SELECT * FROM "lake"."analytics"."daily__mabc123"',
      );
   });
});

describe("wrapPassthrough", () => {
   const SQL = "SELECT a, b FROM t WHERE s = 'x'";

   it("bigquery: bigquery_query(handle, sql)", () => {
      expect(wrapPassthrough("bigquery", "my-project", SQL)).toBe(
         "SELECT * FROM bigquery_query('my-project', 'SELECT a, b FROM t WHERE s = ''x''')",
      );
   });

   it("postgres: postgres_query(handle, sql)", () => {
      expect(wrapPassthrough("postgres", "src_pg", SQL)).toBe(
         "SELECT * FROM postgres_query('src_pg', 'SELECT a, b FROM t WHERE s = ''x''')",
      );
   });

   it("snowflake: snowflake_query(sql, handle) — SQL FIRST", () => {
      expect(wrapPassthrough("snowflake", "secret_snowflake_src", SQL)).toBe(
         "SELECT * FROM snowflake_query('SELECT a, b FROM t WHERE s = ''x''', 'secret_snowflake_src')",
      );
   });

   it("escapes single quotes in both the handle and the inner SQL", () => {
      expect(wrapPassthrough("postgres", "a'b", "x'y")).toBe(
         "SELECT * FROM postgres_query('a''b', 'x''y')",
      );
   });
});

describe("passthroughSourceType", () => {
   it.each(["bigquery", "snowflake", "postgres"])("accepts %s", (type) => {
      expect(
         passthroughSourceType({ name: "s", type } as ApiConnection) as string,
      ).toBe(type);
   });

   it.each(["duckdb", "ducklake", "mysql", "trino"])(
      "refuses %s with a clean 422",
      (type) => {
         expect(() =>
            passthroughSourceType({ name: "s", type } as ApiConnection),
         ).toThrow(BadRequestError);
      },
   );
});

describe("buildSourceIntoStorage gating (no session I/O before the gates)", () => {
   const okSource = { name: "wh", type: "postgres" } as ApiConnection;

   it("refuses a non-DuckDB-family destination before creating a session", async () => {
      await expect(
         buildSourceIntoStorage({
            destinationName: "bq_dest",
            destinationConnection: {
               name: "bq_dest",
               type: "bigquery",
            } as ApiConnection,
            sourceConnection: okSource,
            buildSQL: "SELECT 1",
            physicalTableName: "t",
            environmentPath: "/tmp/env",
         }),
      ).rejects.toThrow(/must be one of/i);
   });

   it("refuses an unsupported source type before creating a session", async () => {
      await expect(
         buildSourceIntoStorage({
            destinationName: "lake",
            destinationConnection: {
               name: "lake",
               type: "duckdb",
            } as ApiConnection,
            sourceConnection: {
               name: "md",
               type: "motherduck",
            } as ApiConnection,
            buildSQL: "SELECT 1",
            physicalTableName: "t",
            environmentPath: "/tmp/env",
         }),
      ).rejects.toThrow(/query-passthrough build supports/i);
   });
});
