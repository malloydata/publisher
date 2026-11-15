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

/**
 * Which dialects have each capability, as decided. A change here changes what
 * a dialect is allowed to do, so it says why in its commit.
 */
const DECIDED: Record<keyof Capabilities, DialectName[]> = {
   incrementalSourceRange: ["postgres", "snowflake", "standardsql"],
   incrementalTargetDml: ["duckdb", "postgres", "snowflake", "standardsql"],
   merge: ["duckdb", "postgres", "snowflake", "standardsql"],
   mergeGatedByServerVersion: ["postgres"],
   pairDelta: ["duckdb"],
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
   foldsTableNameCase: ["databricks", "duckdb", "presto", "sqlserver", "trino"],
};

describe("the dialect table", () => {
   it("names only dialects Malloy registers", () => {
      for (const name of Object.keys(CAPABILITIES)) {
         expect(getDialect(name).name).toBe(name);
      }
   });

   for (const [capability, dialects] of Object.entries(DECIDED)) {
      it(`gives ${capability} to exactly the decided dialects`, () => {
         expect(
            [...dialectsWith(capability as keyof Capabilities)].sort(),
         ).toEqual(dialects);
      });
   }

   it("reads a named dialect's row", () => {
      for (const name of Object.keys(CAPABILITIES) as DialectName[]) {
         expect(capabilitiesOf(name)).toBe(CAPABILITIES[name]);
      }
   });

   it("gives an unknown dialect no capability", () => {
      for (const unknown of ["bigquery", "", "toString", undefined, null, 7]) {
         const flags = capabilitiesOf(unknown);
         expect(Object.values(flags).some(Boolean)).toBe(false);
         expect(Object.keys(flags).sort()).toEqual(Object.keys(DECIDED).sort());
      }
   });
});
