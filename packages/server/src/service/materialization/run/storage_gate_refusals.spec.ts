// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `storage=` build no longer passes a per-build DuckDB gate: whether a
 * stored table can be read is decided at bind, per source, by the served
 * copy's stored-type plan (`declarableColumns`, the one declaration rule).
 * A column no Malloy type reads back faithfully is left out of the source's
 * `type:`, so only a query reading it answers live; a table with no column
 * left is still built, and the served copy declines its source at bind
 * (`column_types`), so every query over it answers live.
 *
 * Pinned per stored DuckDB type, beside the outcome the gate gave before it
 * was replaced: the types whose one-column table the build used to refuse
 * with a 422 (`not_duckdb_portable`) now build and answer live.
 */

import { describe, expect, it } from "bun:test";
import { declarableColumns } from "../stored_column_types";

/**
 * A one-column table of each DuckDB type `DESCRIBE` can report: whether the
 * column is declared (served) or left out (its source answers live), and
 * what the replaced gate did with the table.
 */
const BY_TYPE: Record<
   string,
   { column: "declared" | "left out"; gateBefore: "built" | "422" }
> = {
   BOOLEAN: { column: "declared", gateBefore: "built" },
   TINYINT: { column: "declared", gateBefore: "built" },
   SMALLINT: { column: "declared", gateBefore: "built" },
   INTEGER: { column: "declared", gateBefore: "built" },
   BIGINT: { column: "declared", gateBefore: "built" },
   HUGEINT: { column: "declared", gateBefore: "built" },
   UBIGINT: { column: "declared", gateBefore: "built" },
   UHUGEINT: { column: "declared", gateBefore: "built" },
   FLOAT: { column: "declared", gateBefore: "built" },
   DOUBLE: { column: "declared", gateBefore: "built" },
   "DECIMAL(18,2)": { column: "declared", gateBefore: "built" },
   "DECIMAL(38,10)": { column: "declared", gateBefore: "built" },
   VARCHAR: { column: "declared", gateBefore: "built" },
   UUID: { column: "declared", gateBefore: "built" },
   DATE: { column: "declared", gateBefore: "built" },
   TIME: { column: "declared", gateBefore: "built" },
   TIMESTAMP: { column: "declared", gateBefore: "built" },
   TIMESTAMP_NS: { column: "declared", gateBefore: "built" },
   "TIMESTAMP WITH TIME ZONE": { column: "declared", gateBefore: "built" },
   TIMESTAMPTZ: { column: "declared", gateBefore: "built" },
   INTERVAL: { column: "left out", gateBefore: "422" },
   BLOB: { column: "left out", gateBefore: "422" },
   BIT: { column: "left out", gateBefore: "422" },
   JSON: { column: "left out", gateBefore: "422" },
   "INTEGER[]": { column: "declared", gateBefore: "built" },
   "VARCHAR[]": { column: "declared", gateBefore: "built" },
   "STRUCT(a INTEGER, b VARCHAR)": { column: "declared", gateBefore: "built" },
   "MAP(VARCHAR, INTEGER)": { column: "left out", gateBefore: "422" },
   "ENUM('a', 'b')": { column: "left out", gateBefore: "422" },
   "UNION(n INTEGER, s VARCHAR)": { column: "left out", gateBefore: "422" },
   NOT_A_REAL_TYPE: { column: "left out", gateBefore: "422" },
};

describe("a storage= table's columns, decided at bind", () => {
   it("declares or leaves out a column by its type", () => {
      const seen: Record<string, string> = {};
      for (const type of Object.keys(BY_TYPE)) {
         const { declared } = declarableColumns([{ name: "c", type }]);
         seen[type] = declared.length === 1 ? "declared" : "left out";
      }
      expect(seen).toEqual(
         Object.fromEntries(
            Object.entries(BY_TYPE).map(([t, o]) => [t, o.column]),
         ),
      );
   });

   it("refused exactly the tables whose every column is left out, which now build", () => {
      for (const [type, outcome] of Object.entries(BY_TYPE)) {
         expect({ type, refusedBefore: outcome.gateBefore === "422" }).toEqual({
            type,
            refusedBefore: outcome.column === "left out",
         });
      }
   });

   it("leaves a column out rather than the table", () => {
      expect(
         declarableColumns([
            { name: "id", type: "BIGINT" },
            { name: "m", type: "MAP(VARCHAR, INTEGER)" },
         ]),
      ).toEqual({
         declared: [{ name: "id", type: "BIGINT", malloyType: "number" }],
         leftOut: [{ name: "m", type: "MAP(VARCHAR, INTEGER)" }],
      });
   });
});
