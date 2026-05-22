import { describe, expect, it } from "bun:test";

import {
   isCappableSelect,
   PUBLISHER_CAP_ALIAS,
   wrapWithRowLimit,
} from "./sql_helpers";

describe("isCappableSelect", () => {
   it.each([
      ["plain SELECT", "SELECT 1"],
      ["lowercase select", "select * from t"],
      ["mixed case", "SeLeCt 1"],
      ["leading whitespace", "   \n\t SELECT 1"],
      ["leading line comment", "-- some comment\nSELECT 1"],
      ["leading block comment", "/* hi */ SELECT 1"],
      ["multiple comment blocks", "-- a\n/* b */-- c\nSELECT 1"],
      ["WITH ... SELECT", "WITH x AS (SELECT 1) SELECT * FROM x"],
      ["lowercase with", "with x as (select 1) select * from x"],
      ["trailing semicolon", "SELECT 1;"],
      ["trailing semicolon + whitespace", "SELECT 1;   \n"],
      ["trailing semicolon + comment", "SELECT 1; -- done\n"],
      ["semicolon inside string literal", "SELECT 'a;b' AS c"],
      ["semicolon inside double-quoted ident", 'SELECT 1 AS "weird;name"'],
   ])("accepts %s", (_label, sql) => {
      expect(isCappableSelect(sql)).toBe(true);
   });

   it.each([
      ["empty", ""],
      ["whitespace only", "   \n\t  "],
      ["only comments", "-- nothing\n/* still nothing */"],
      ["CREATE TABLE", "CREATE TABLE t (a INT)"],
      ["INSERT", "INSERT INTO t VALUES (1)"],
      ["UPDATE", "UPDATE t SET a = 1"],
      ["DELETE", "DELETE FROM t"],
      ["DROP", "DROP TABLE t"],
      ["ALTER", "ALTER TABLE t ADD b INT"],
      ["TRUNCATE", "TRUNCATE TABLE t"],
      ["SHOW", "SHOW TABLES"],
      ["DESCRIBE", "DESCRIBE t"],
      ["EXPLAIN", "EXPLAIN SELECT 1"],
      ["VALUES literal", "VALUES (1), (2)"],
      ["TABLE shorthand", "TABLE t"],
      ["multi statement", "SELECT 1; SELECT 2"],
      ["multi statement with whitespace", "SELECT 1;\n\nSELECT 2"],
      ["duckdb split sentinel", "SELECT 1 -- hack: split on this\nSELECT 2"],
      ["select then DDL", "SELECT 1; CREATE TABLE t (a INT)"],
   ])("rejects %s", (_label, sql) => {
      expect(isCappableSelect(sql)).toBe(false);
   });

   it("rejects non-string input", () => {
      expect(isCappableSelect(undefined as unknown as string)).toBe(false);
      expect(isCappableSelect(null as unknown as string)).toBe(false);
   });
});

describe("wrapWithRowLimit", () => {
   it("wraps a plain SELECT and pushes the LIMIT to the database", () => {
      const wrapped = wrapWithRowLimit("SELECT 1", 100);
      expect(wrapped).toBe(
         `SELECT * FROM (SELECT 1) AS ${PUBLISHER_CAP_ALIAS} LIMIT 100`,
      );
   });

   it("preserves the original SQL body verbatim inside the subquery", () => {
      const sql = "SELECT a, b FROM t WHERE a > 5 ORDER BY b DESC";
      const wrapped = wrapWithRowLimit(sql, 42);
      expect(wrapped).toContain(`(${sql})`);
      expect(wrapped.endsWith("LIMIT 42")).toBe(true);
   });

   it("strips a trailing semicolon so the wrapper parses", () => {
      const wrapped = wrapWithRowLimit("SELECT 1;", 10);
      expect(wrapped).toBe(
         `SELECT * FROM (SELECT 1) AS ${PUBLISHER_CAP_ALIAS} LIMIT 10`,
      );
   });

   it("strips trailing semicolon + whitespace + comment", () => {
      const wrapped = wrapWithRowLimit("SELECT 1; -- bye\n", 10);
      expect(wrapped).toBe(
         `SELECT * FROM (SELECT 1) AS ${PUBLISHER_CAP_ALIAS} LIMIT 10`,
      );
   });

   it("keeps semicolons inside string literals intact", () => {
      const wrapped = wrapWithRowLimit("SELECT 'a;b' AS c", 5);
      expect(wrapped).toBe(
         `SELECT * FROM (SELECT 'a;b' AS c) AS ${PUBLISHER_CAP_ALIAS} LIMIT 5`,
      );
   });

   it("rejects non-integer / negative limits", () => {
      expect(() => wrapWithRowLimit("SELECT 1", -1)).toThrow(RangeError);
      expect(() => wrapWithRowLimit("SELECT 1", 1.5)).toThrow(RangeError);
      expect(() => wrapWithRowLimit("SELECT 1", NaN)).toThrow(RangeError);
   });

   /**
    * Defense-in-depth: callers are expected to validate that `sql` is a
    * string before reaching here (the controller does, and `isCappableSelect`
    * also rejects non-strings), but `wrapWithRowLimit` re-checks so a future
    * caller can't reintroduce the type-confusion vector CodeQL flagged on
    * the original PR.
    */
   it.each([
      ["array", ["SELECT 1", "SELECT 2"]],
      ["object", { sql: "SELECT 1" }],
      ["number", 42],
      ["null", null],
      ["undefined", undefined],
   ])("rejects non-string sql (%s) with TypeError", (_label, sql) => {
      expect(() => wrapWithRowLimit(sql as unknown as string, 10)).toThrow(
         TypeError,
      );
   });

   it("accepts a zero limit (used when the cap is explicitly set to 0)", () => {
      const wrapped = wrapWithRowLimit("SELECT 1", 0);
      expect(wrapped).toBe(
         `SELECT * FROM (SELECT 1) AS ${PUBLISHER_CAP_ALIAS} LIMIT 0`,
      );
   });
});
