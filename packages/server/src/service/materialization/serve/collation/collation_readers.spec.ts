// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   CAPABILITIES,
   type DialectName,
   STORAGE_DIALECT,
} from "../../domain/dialect";
import { collationReaderOf, COLLATION_READERS } from ".";
import { DUCKDB_COLLATION } from "./duckdb";

describe("the collation readers", () => {
   it("read each dialect as its stringCollation fact says", () => {
      for (const dialect of Object.keys(CAPABILITIES) as DialectName[]) {
         expect({
            dialect,
            scope: collationReaderOf(dialect)?.scope,
         }).toEqual({
            dialect,
            scope: CAPABILITIES[dialect].stringCollation as never,
         });
      }
   });

   it("leave no dialect `binary`: a binary dialect would need no reader, and none is one today", () => {
      expect(
         Object.values(CAPABILITIES).filter(
            (c) => c.stringCollation === "binary",
         ),
      ).toEqual([]);
   });

   it("read every storage destination as DuckDB", () => {
      expect(collationReaderOf(STORAGE_DIALECT)).toBe(DUCKDB_COLLATION);
   });

   it("find no reader for an unregistered dialect, prototype members included", () => {
      for (const dialect of [
         undefined,
         "",
         "bigquery",
         "ducklake",
         "constructor",
         "toString",
         "__proto__",
      ]) {
         expect(collationReaderOf(dialect)).toBeUndefined();
      }
      expect(Object.keys(COLLATION_READERS).sort()).toEqual([
         "duckdb",
         "postgres",
         "standardsql",
      ]);
   });
});
