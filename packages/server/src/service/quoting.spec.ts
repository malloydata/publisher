import { describe, expect, it } from "bun:test";
import { bareTableName, quoteIdentifier, quoteTablePath } from "./quoting";

describe("bareTableName", () => {
   it("returns the segment after the last dot for a qualified name", () => {
      expect(bareTableName("my_schema.my_table")).toBe("my_table");
   });

   it("returns the name unchanged when it is unqualified", () => {
      expect(bareTableName("my_table")).toBe("my_table");
   });
});

describe("quoteIdentifier", () => {
   it("backticks for backtick dialects (BigQuery / MySQL / Databricks)", () => {
      expect(quoteIdentifier("my-table", "standardsql")).toBe("`my-table`");
      expect(quoteIdentifier("t", "mysql")).toBe("`t`");
      expect(quoteIdentifier("t", "databricks")).toBe("`t`");
   });

   it("double-quotes for everything else (Postgres / DuckDB / Snowflake)", () => {
      expect(quoteIdentifier("my-table", "postgres")).toBe('"my-table"');
      expect(quoteIdentifier("t", "duckdb")).toBe('"t"');
      expect(quoteIdentifier("t", "snowflake")).toBe('"t"');
   });

   it("escapes an embedded quote char by doubling it", () => {
      expect(quoteIdentifier("a`b", "standardsql")).toBe("`a``b`");
      expect(quoteIdentifier('a"b', "postgres")).toBe('"a""b"');
   });
});

describe("quoteTablePath", () => {
   it("quotes each segment of a container path independently", () => {
      expect(
         quoteTablePath(
            "my-proj.mydataset.engaged_events_ab12_v0",
            "standardsql",
         ),
      ).toBe("`my-proj`.`mydataset`.`engaged_events_ab12_v0`");
      expect(quoteTablePath("mydataset.t_ab12_v0", "postgres")).toBe(
         '"mydataset"."t_ab12_v0"',
      );
   });

   it("quotes a bare (unqualified) name", () => {
      expect(quoteTablePath("t_ab12_v0", "standardsql")).toBe("`t_ab12_v0`");
      expect(quoteTablePath("t_ab12_v0", "postgres")).toBe('"t_ab12_v0"');
   });
});
