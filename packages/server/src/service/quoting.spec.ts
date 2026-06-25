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

// Cross-repo conformance (PR ms2data/service#5272 review item 1). The publisher
// keys identifier quoting off the Malloy `dialectName`; the control plane keys
// the SAME fact off the publisher *connection type* in
// `PhysicalTableName.BACKTICK_TYPES` ({bigquery, mysql, databricks}). The two
// must stay byte-compatible — a name the publisher CREATEs with backticks the CP
// must DROP with backticks — but they live in different repos under different key
// vocabularies (`standardsql` the dialect vs `bigquery` the connection type), so
// drift produces malformed DDL with no compile error. This table pins the full
// connection-type → dialect → quote-char correspondence; if it changes, update
// `PhysicalTableName.BACKTICK_TYPES` in the control plane in lockstep.
describe("dialect/connection-type quoting conformance", () => {
   const QUOTING = [
      { connectionType: "bigquery", dialect: "standardsql", quote: "`" },
      { connectionType: "mysql", dialect: "mysql", quote: "`" },
      { connectionType: "databricks", dialect: "databricks", quote: "`" },
      { connectionType: "postgres", dialect: "postgres", quote: '"' },
      { connectionType: "snowflake", dialect: "snowflake", quote: '"' },
      { connectionType: "trino", dialect: "trino", quote: '"' },
      { connectionType: "duckdb", dialect: "duckdb", quote: '"' },
      { connectionType: "motherduck", dialect: "duckdb", quote: '"' },
   ] as const;

   for (const { connectionType, dialect, quote } of QUOTING) {
      it(`${connectionType} (${dialect}) quotes with ${quote}`, () => {
         expect(quoteIdentifier("x", dialect)).toBe(`${quote}x${quote}`);
      });
   }
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
