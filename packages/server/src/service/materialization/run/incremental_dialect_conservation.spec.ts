// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { CAPABILITIES, capabilitiesOf } from "../domain/dialect";
import {
   decodeTablePathSegments,
   probeServerVersion,
} from "./incremental_apply";

/**
 * The incremental refresh's per-dialect facts read from the dialect table:
 * which dialects gate MERGE on a server-version read, and which dialects'
 * table paths are decoded for the target probe, and how. The statement text
 * itself is pinned in dialect_statements.golden.json; this pins which names
 * reach it, including connection types that are not dialects, a prototype
 * name and an unknown one.
 */

const DECODED = new Set(["duckdb", "postgres", "standardsql", "snowflake"]);

const NAMES = [
   ...Object.keys(CAPABILITIES),
   "bigquery",
   "motherduck",
   "ducklake",
   "constructor",
   "nope",
];

describe("the incremental refresh's dialect facts", () => {
   it("gates MERGE on the server version for Postgres alone", async () => {
      for (const dialect of NAMES) {
         expect([
            dialect,
            capabilitiesOf(dialect).mergeGatedByServerVersion,
         ]).toEqual([dialect, dialect === "postgres"]);
      }
      expect(
         await probeServerVersion(
            async () => ({ rows: [{ version_num: "160004" }] }),
            "postgres",
         ),
      ).toBe(160004);
   });

   it("decodes a table path by each decoding dialect's identifier rules", () => {
      const decode = (dialect: string, p: string) =>
         decodeTablePathSegments(dialect, p) ?? null;
      // Unquoted parts keep their case in DuckDB and BigQuery, fold to lower
      // in Postgres and to upper in Snowflake; quoted parts keep theirs.
      expect(decode("duckdb", "Lake.Main.T")).toEqual(["Lake", "Main", "T"]);
      expect(decode("standardsql", "Lake.Main.T")).toEqual([
         "Lake",
         "Main",
         "T",
      ]);
      expect(decode("postgres", "Lake.Main.T")).toEqual(["lake", "main", "t"]);
      expect(decode("snowflake", "Lake.Main.T")).toEqual(["LAKE", "MAIN", "T"]);
      expect(decode("snowflake", '"a.b".c')).toEqual(["a.b", "C"]);
      expect(decode("postgres", '"S"."T"')).toEqual(["S", "T"]);
      // A backticked part is BigQuery's quote; a double-quoted one is not.
      expect(decode("standardsql", "`p`.d.t")).toEqual(["p", "d", "t"]);
      expect(decode("standardsql", '"a.b".c')).toBeNull();
      for (const dialect of NAMES.filter((d) => !DECODED.has(d))) {
         expect([dialect, decode(dialect, "s.t")]).toEqual([dialect, null]);
      }
   });
});
