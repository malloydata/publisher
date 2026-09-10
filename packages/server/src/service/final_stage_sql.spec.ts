// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Eligibility for the dialect final-stage wrapper on ad hoc `sqlQuery` SQL.
// The upstream facts this rests on -- Postgres is the only dialect with a final
// stage, and its final stage is a `row_to_json` call -- are pinned in
// incremental_compiler_contract.spec.ts, so a compiler bump that moves either
// fails there rather than silently changing what gets wrapped here.
import { PostgresDialect } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import {
   dialectHasFinalStage,
   finalStageContractMessage,
   shouldWrapFinalStage,
   wrapFinalStage,
} from "./final_stage_sql";

/** The shape `@malloydata/malloy` compiles a Postgres query into. */
const COMPILED_QUERY = `WITH __stage0 AS (
  SELECT "category" as "category", COUNT(1) as "n" FROM orders GROUP BY 1
)
SELECT row_to_json(finalStage) as row FROM __stage0 AS finalStage`;

describe("dialectHasFinalStage", () => {
   it("is true for postgres and false for the dialects that do not finalize", () => {
      expect(dialectHasFinalStage("postgres")).toBe(true);
      for (const name of [
         "duckdb",
         "standardsql",
         "snowflake",
         "trino",
         "mysql",
         "databricks",
      ]) {
         expect(dialectHasFinalStage(name)).toBe(false);
      }
   });

   it("is false for a dialect it does not know", () => {
      expect(dialectHasFinalStage("nonesuch")).toBe(false);
      expect(dialectHasFinalStage("")).toBe(false);
   });
});

describe("shouldWrapFinalStage", () => {
   it("wraps a plain SELECT on a finalizing dialect", () => {
      // The reported bug: `SELECT 1` came back as a nullish row because the
      // connector unwrapped a column the statement never projected.
      expect(shouldWrapFinalStage("postgres", "SELECT 1 AS x")).toBe(true);
      expect(
         shouldWrapFinalStage("postgres", "select a, b from t where a > 1"),
      ).toBe(true);
      expect(shouldWrapFinalStage("postgres", "VALUES (1), (2)")).toBe(true);
      expect(
         shouldWrapFinalStage("postgres", "(SELECT 1) UNION (SELECT 2)"),
      ).toBe(true);
      expect(
         shouldWrapFinalStage(
            "postgres",
            "WITH t AS (SELECT 1 AS x) SELECT * FROM t",
         ),
      ).toBe(true);
   });

   it("never wraps on a dialect whose connector does not unwrap a row column", () => {
      for (const name of ["duckdb", "snowflake", "standardsql", "nonesuch"]) {
         expect(shouldWrapFinalStage(name, "SELECT 1 AS x")).toBe(false);
      }
   });

   it("leaves an already-finalized statement alone", () => {
      // The load-bearing case. A `publisher` connection reports the REMOTE's
      // dialect, so the Malloy compiler on the far side has already finalized
      // the statement before it reaches this endpoint. Wrapping it again adds a
      // level the connector does not strip and the caller reads nulls off
      // `{"row": {...}}` -- a wrong answer, not an error.
      expect(shouldWrapFinalStage("postgres", COMPILED_QUERY)).toBe(false);
   });

   it("leaves a statement that cannot sit in a subquery alone", () => {
      for (const sql of [
         "CREATE TABLE t (a int)",
         "SET search_path = public",
         "EXPLAIN SELECT 1",
         "SHOW search_path",
         "INSERT INTO t VALUES (1)",
         "TRUNCATE t",
         "SELECT a INTO snapshot FROM t",
         "WITH moved AS (DELETE FROM t RETURNING *) SELECT * FROM moved",
      ]) {
         expect(shouldWrapFinalStage("postgres", sql)).toBe(false);
      }
   });

   it("reads past leading comments and whitespace to find the statement", () => {
      expect(
         shouldWrapFinalStage("postgres", "-- a note\n  SELECT 1 AS x"),
      ).toBe(true);
      expect(
         shouldWrapFinalStage("postgres", "/* a note */\n/* two */ SELECT 1"),
      ).toBe(true);
      expect(
         shouldWrapFinalStage("postgres", "-- CREATE TABLE t\nSELECT 1"),
      ).toBe(true);
   });
});

describe("wrapFinalStage", () => {
   it("finalizes the statement the way the dialect finalizes a query", () => {
      const wrapped = wrapFinalStage("postgres", "SELECT 1 AS x");
      expect(wrapped).toContain("SELECT 1 AS x");
      expect(wrapped).toContain(
         new PostgresDialect().sqlFinalStage("__publisher_raw_sql", []),
      );
   });

   it("drops a trailing semicolon, which is legal alone and illegal in the CTE", () => {
      expect(wrapFinalStage("postgres", "SELECT 1;  ")).not.toContain(";");
   });

   it("is a no-op on a dialect with no final stage", () => {
      expect(wrapFinalStage("duckdb", "SELECT 1")).toBe("SELECT 1");
   });
});

describe("finalStageContractMessage", () => {
   it("names the wrapper in the dialect's own terms", () => {
      const message = finalStageContractMessage("postgres");
      expect(message).toContain("row_to_json");
      expect(message).toContain("postgres");
   });

   it("omits the example for a dialect that has no final stage", () => {
      expect(finalStageContractMessage("duckdb")).not.toContain("Wrap it as");
   });
});
