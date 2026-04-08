import { describe, expect, it } from "bun:test";
import {
   snowflakeSqlStringLiteral,
   parseSnowflakeDbSchemaTable,
   informationSchemaRowToDialectTypeString,
} from "./publisher_snowflake_connection";

describe("snowflakeSqlStringLiteral", () => {
   it("wraps a plain string in single quotes", () => {
      expect(snowflakeSqlStringLiteral("hello")).toBe("'hello'");
   });

   it("escapes embedded single quotes", () => {
      expect(snowflakeSqlStringLiteral("it's")).toBe("'it''s'");
   });

   it("handles empty string", () => {
      expect(snowflakeSqlStringLiteral("")).toBe("''");
   });

   it("escapes multiple single quotes", () => {
      expect(snowflakeSqlStringLiteral("a'b'c")).toBe("'a''b''c'");
   });
});

describe("parseSnowflakeDbSchemaTable", () => {
   it("parses a fully qualified 3-part path", () => {
      expect(parseSnowflakeDbSchemaTable("MY_DB.MY_SCHEMA.MY_TABLE")).toEqual({
         database: "MY_DB",
         schema: "MY_SCHEMA",
         table: "MY_TABLE",
      });
   });

   it("parses a 2-part path with default database", () => {
      expect(
         parseSnowflakeDbSchemaTable("MY_SCHEMA.MY_TABLE", "DEFAULT_DB"),
      ).toEqual({
         database: "DEFAULT_DB",
         schema: "MY_SCHEMA",
         table: "MY_TABLE",
      });
   });

   it("returns undefined for 2-part path without default database", () => {
      expect(parseSnowflakeDbSchemaTable("MY_SCHEMA.MY_TABLE")).toBeUndefined();
   });

   it("returns undefined for 2-part path with null default database", () => {
      expect(
         parseSnowflakeDbSchemaTable("MY_SCHEMA.MY_TABLE", null),
      ).toBeUndefined();
   });

   it("returns undefined for single-part path", () => {
      expect(parseSnowflakeDbSchemaTable("MY_TABLE")).toBeUndefined();
   });

   it("returns undefined for 4-part path", () => {
      expect(
         parseSnowflakeDbSchemaTable("A.B.C.D"),
      ).toBeUndefined();
   });

   it("returns undefined for quoted identifiers", () => {
      expect(
         parseSnowflakeDbSchemaTable('"MY DB".MY_SCHEMA.MY_TABLE'),
      ).toBeUndefined();
   });

   it("returns undefined for unsafe characters", () => {
      expect(
         parseSnowflakeDbSchemaTable("MY-DB.MY_SCHEMA.MY_TABLE"),
      ).toBeUndefined();
   });

   it("accepts identifiers with $ and digits", () => {
      expect(parseSnowflakeDbSchemaTable("DB1.SCHEMA$2.TABLE_3")).toEqual({
         database: "DB1",
         schema: "SCHEMA$2",
         table: "TABLE_3",
      });
   });

   it("rejects identifiers starting with a digit", () => {
      expect(
         parseSnowflakeDbSchemaTable("1DB.SCHEMA.TABLE"),
      ).toBeUndefined();
   });
});

describe("informationSchemaRowToDialectTypeString", () => {
   it("returns NUMBER with precision and scale", () => {
      expect(
         informationSchemaRowToDialectTypeString({
            DATA_TYPE: "NUMBER",
            NUMERIC_PRECISION: 10,
            NUMERIC_SCALE: 2,
         }),
      ).toBe("number(10,2)");
   });

   it("returns NUMBER with precision only", () => {
      expect(
         informationSchemaRowToDialectTypeString({
            DATA_TYPE: "NUMERIC",
            NUMERIC_PRECISION: 38,
            NUMERIC_SCALE: null,
         }),
      ).toBe("numeric(38)");
   });

   it("returns VARCHAR as-is (lowercased)", () => {
      expect(
         informationSchemaRowToDialectTypeString({
            DATA_TYPE: "VARCHAR",
            NUMERIC_PRECISION: null,
            NUMERIC_SCALE: null,
         }),
      ).toBe("varchar");
   });

   it("returns VARIANT as-is (lowercased)", () => {
      expect(
         informationSchemaRowToDialectTypeString({
            DATA_TYPE: "VARIANT",
            NUMERIC_PRECISION: null,
            NUMERIC_SCALE: null,
         }),
      ).toBe("variant");
   });

   it("defaults to varchar when DATA_TYPE is missing", () => {
      expect(informationSchemaRowToDialectTypeString({})).toBe("varchar");
   });

   it("handles lowercase column names", () => {
      expect(
         informationSchemaRowToDialectTypeString({
            data_type: "BOOLEAN",
            numeric_precision: null,
            numeric_scale: null,
         }),
      ).toBe("boolean");
   });

   it("handles DECIMAL with precision and scale", () => {
      expect(
         informationSchemaRowToDialectTypeString({
            DATA_TYPE: "DECIMAL",
            NUMERIC_PRECISION: 18,
            NUMERIC_SCALE: 4,
         }),
      ).toBe("decimal(18,4)");
   });
});
