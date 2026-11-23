// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { widenedColumns } from "./delta_type_widening";

/** Whether a column stored as `stored` is judged unable to hold `delta`. */
const widened = (stored: string, delta: string) =>
   widenedColumns(new Map([["c", stored]]), new Map([["c", delta]])).length > 0;

describe("whether a stored column holds a delta's type", () => {
   it.each([
      // An upstream widened after the build.
      ["integer", "bigint"],
      ["INTEGER", "BIGINT"],
      ["real", "double precision"],
      ["FLOAT", "DOUBLE"],
      ["SMALLINT", "INTEGER"],
      ["UINTEGER", "INTEGER"],
      ["BIGINT", "UBIGINT"],
      ["BIGINT", "HUGEINT"],
      // A rounding: a fraction into an integer, a decimal into a float.
      ["integer", "numeric"],
      ["BIGINT", "DOUBLE"],
      ["DOUBLE", "DECIMAL(38,3)"],
      ["DECIMAL(10,2)", "DECIMAL(12,2)"],
      ["DECIMAL(10,2)", "DECIMAL(10,4)"],
      ["DECIMAL(9,0)", "INTEGER"],
      ["numeric(10,2)", "numeric"],
      // A float holds an integer only within its significand.
      ["FLOAT", "INTEGER"],
      ["DOUBLE", "BIGINT"],
   ])("rebuilds a %s column the delta brings %s", (stored, delta) => {
      expect(widened(stored, delta)).toBe(true);
   });

   it.each([
      ["integer", "integer"],
      ["BIGINT", "INTEGER"],
      ["double precision", "real"],
      ["DOUBLE", "INTEGER"],
      ["FLOAT", "SMALLINT"],
      ["BIGINT", "UINTEGER"],
      ["numeric", "bigint"],
      ["numeric", "numeric"],
      ["DECIMAL(38,3)", "DECIMAL(10,2)"],
      ["DECIMAL(38,0)", "BIGINT"],
      // Not numbers, or not judged: left to the delta.
      ["character varying", "text"],
      ["TIMESTAMP", "TIMESTAMP WITH TIME ZONE"],
      ["USER-DEFINED", "integer"],
   ])("deltas into a %s column the delta brings %s", (stored, delta) => {
      expect(widened(stored, delta)).toBe(false);
   });

   it("names each narrower column, and skips columns either side lacks", () => {
      expect(
         widenedColumns(
            new Map([
               ["id", "INTEGER"],
               ["amt", "FLOAT"],
               ["gone", "INTEGER"],
            ]),
            new Map([
               ["id", "BIGINT"],
               ["amt", "FLOAT"],
               ["new", "BIGINT"],
            ]),
         ),
      ).toEqual(['"id" (INTEGER → BIGINT)']);
   });
});
