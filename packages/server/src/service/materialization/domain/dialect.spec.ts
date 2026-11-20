// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { getDialect } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import {
   CAPABILITIES,
   capabilitiesOf,
   dialectsWith,
   type Capabilities,
   type DialectName,
} from "./dialect";

/** Every capability but the leaf budget, which is a number. */
type Flag = Exclude<keyof Capabilities, "leafBudget">;

/**
 * Which dialects have each capability, as decided. A change here changes what
 * a dialect is allowed to do, so it says why in its commit.
 */
const DECIDED: Record<Flag, DialectName[]> = {
   incrementalSourceRange: ["postgres", "snowflake", "standardsql"],
   incrementalTargetDml: ["duckdb", "postgres", "snowflake", "standardsql"],
   merge: ["duckdb", "postgres", "snowflake", "standardsql"],
   mergeGatedByServerVersion: ["postgres"],
   pairDelta: ["duckdb"],
   passthroughNumericAsDouble: ["postgres"],
   // A kept table a probe cannot see may be hidden by a grant, not gone.
   catalogHidesTables: ["mysql", "snowflake"],
   transactionalSwap: ["duckdb", "postgres"],
   dependencyGuard: ["postgres"],
   relationProbe: [
      "databricks",
      "duckdb",
      "postgres",
      "snowflake",
      "sqlserver",
      "standardsql",
      "trino",
   ],
   tableOwnership: [
      "databricks",
      "duckdb",
      "mysql",
      "postgres",
      "presto",
      "snowflake",
      "sqlserver",
      "standardsql",
      "trino",
   ],
   needsQualifiedTable: ["standardsql"],
   nestedColumns: ["duckdb"],
   driverKeepsBigIntegers: ["duckdb"],
   liveRoundsBigIntegersMeasured: ["postgres"],
   liveWritesFloat4Shortest: ["postgres"],
   castsToIntegerType: [
      "databricks",
      "duckdb",
      "postgres",
      "presto",
      "snowflake",
      "sqlserver",
      "standardsql",
      "trino",
   ],
   onlyBigIntegerType: ["standardsql"],
   foldsTableNameCase: ["databricks", "duckdb", "presto", "sqlserver", "trino"],
   utcSessions: ["duckdb"],
   // A string column's comparison is read only where it can be: BigQuery is
   // byte-exact, DuckDB and Postgres are probed (`serve/gate_collation.ts`).
   stringCollation: ["duckdb", "postgres", "standardsql"],
   // BigQuery and Postgres refuse GROUP BY on some types (F-45); a pair
   // table's build groups.
   ungroupableTypes: ["postgres", "standardsql"],
   // A stored DOUBLE compares as live only where live's double orders NaN
   // as DuckDB does (not BigQuery's FLOAT64).
   binary64Types: ["duckdb", "postgres", "snowflake"],
};

/** Each dialect's leaf budget, as decided (`domain/table_name.ts`). */
const DECIDED_LEAF_BUDGETS: Record<DialectName, number> = {
   duckdb: 120,
   postgres: 50,
   standardsql: 120,
   snowflake: 120,
   databricks: 120,
   trino: 50,
   presto: 50,
   mysql: 50,
   sqlserver: 115,
};

describe("the dialect table", () => {
   it("names only dialects Malloy registers", () => {
      for (const name of Object.keys(CAPABILITIES)) {
         expect(getDialect(name).name).toBe(name);
      }
   });

   for (const [capability, dialects] of Object.entries(DECIDED)) {
      it(`gives ${capability} to exactly the decided dialects`, () => {
         expect([...dialectsWith(capability as Flag)].sort()).toEqual(dialects);
      });
   }

   it("reads a named dialect's row", () => {
      for (const name of Object.keys(CAPABILITIES) as DialectName[]) {
         expect(capabilitiesOf(name)).toBe(CAPABILITIES[name]);
      }
   });

   it("gives each dialect its decided leaf budget", () => {
      for (const name of Object.keys(CAPABILITIES) as DialectName[]) {
         expect(capabilitiesOf(name).leafBudget).toBe(
            DECIDED_LEAF_BUDGETS[name],
         );
      }
   });

   it("names the types BigQuery's GROUP BY refuses", () => {
      expect(capabilitiesOf("standardsql").ungroupableTypes).toEqual({
         malloy: ["json", "array", "record"],
         native: ["geography"],
      });
   });

   it("names the native types Postgres's GROUP BY refuses, and not jsonb", () => {
      const refused = capabilitiesOf("postgres").ungroupableTypes;
      expect(refused?.malloy).toEqual([]);
      expect(refused?.native).toContain("json");
      expect(refused?.native).toContain("xml");
      expect(refused?.native).toContain("point");
      expect(refused?.native).not.toContain("jsonb");
   });

   it("gives an unknown dialect no capability, and the least leaf budget", () => {
      for (const unknown of ["bigquery", "", "toString", undefined, null, 7]) {
         const { leafBudget, ...flags } = capabilitiesOf(unknown);
         expect(Object.values(flags).some(Boolean)).toBe(false);
         expect(Object.keys(flags).sort()).toEqual(Object.keys(DECIDED).sort());
         expect(leafBudget).toBe(50);
      }
   });
});
