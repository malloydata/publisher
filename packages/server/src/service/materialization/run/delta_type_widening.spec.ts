// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { storedTypesAsBuilt } from "../stored_column_types";
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
      ["DECIMAL(18,0)", "BIGINT"],
      ["DECIMAL(19,0)", "UBIGINT"],
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
      // A signed BIGINT is at most 19 digits; an unsigned one needs 20.
      ["DECIMAL(19,0)", "BIGINT"],
      ["DECIMAL(20,0)", "UBIGINT"],
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

describe("a delta type a storage= build casts", () => {
   const judged = (stored: string, delta: string) =>
      widenedColumns(
         new Map([["c", stored]]),
         new Map([["c", delta]]),
         storedTypesAsBuilt,
      );

   it.each(["DECIMAL(38,0)", "HUGEINT", "DOUBLE"])(
      "is no widening of a %s column a rebuild would write from a HUGEINT",
      (stored) => {
         expect(judged(stored, "HUGEINT")).toEqual([]);
      },
   );

   it("still finds a genuine widening", () => {
      expect(judged("INTEGER", "BIGINT")).toEqual(['"c" (INTEGER → BIGINT)']);
      expect(judged("FLOAT", "DOUBLE")).toEqual(['"c" (FLOAT → DOUBLE)']);
      expect(judged("DECIMAL(10,0)", "HUGEINT")).toEqual([
         '"c" (DECIMAL(10,0) → HUGEINT)',
      ]);
   });

   it("is judged on the raw type when no build cast is known", () => {
      expect(
         widenedColumns(
            new Map([["c", "DECIMAL(38,0)"]]),
            new Map([["c", "HUGEINT"]]),
         ),
      ).toEqual(['"c" (DECIMAL(38,0) → HUGEINT)']);
   });
});
