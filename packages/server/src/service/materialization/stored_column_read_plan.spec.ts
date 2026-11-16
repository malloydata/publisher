// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `storedColumnReadPlan` against the composition every serve-side reader of a
 * stored table built for itself before it existed: type mismatches, the
 * faithful-cast plan, then each big integer's native read. Over a matrix of
 * stored types, declared types, time zones, dialects and tiers, the plan must
 * give exactly what that composition gave, in the same order.
 */

import { describe, expect, it } from "bun:test";
import {
   bigIntegerReadOf,
   type BigIntegerReadOptions,
   type DeclaredRead,
   type ReadCast,
   serveTypePlan,
   storedColumnReadPlan,
   storedColumnTypeMismatches,
} from "./stored_column_types";

/** The composition each reader built, as it built it. */
function legacy(
   schema: { name: string; type: string }[],
   declared: DeclaredRead[],
   options: { queryTimezone?: string } & Partial<BigIntegerReadOptions>,
) {
   const mismatches = storedColumnTypeMismatches(schema, declared);
   const plan = serveTypePlan(mismatches, {
      queryTimezone: options.queryTimezone,
   });
   const casts: ReadCast[] = [...plan.casts];
   const refusals: { column: string; storedType: string }[] = [];
   for (const d of declared) {
      if (!("field" in d)) continue;
      const stored = schema.find((s) => s.name === d.name);
      if (!stored || casts.some((c) => c.column === d.name)) continue;
      const big = bigIntegerReadOf(d.field, stored, {
         dialect: options.dialect,
         ...(options.colocated ? { colocated: true } : {}),
         ...(options.nativeTypeOf
            ? { nativeTypeOf: options.nativeTypeOf }
            : {}),
      });
      if (!big) continue;
      if (big.castTo) {
         casts.push({ column: d.name, castTo: big.castTo, native: true });
      } else {
         refusals.push({ column: d.name, storedType: stored.type });
      }
   }
   return { mismatches, casts, leftOut: plan.unservable, refusals };
}

const bigint = { type: "number", numberType: "bigint" };
const integer = { type: "number", numberType: "integer" };

const STORED_TYPES = [
   "VARCHAR",
   "BIGINT",
   "INTEGER",
   "DOUBLE",
   "UHUGEINT",
   "DECIMAL(38,0)",
   "DATE",
   "TIMESTAMP",
   "TIMESTAMP WITH TIME ZONE",
   "BOOLEAN",
   "BLOB",
];
const DECLARED: { type: string; field?: unknown }[] = [
   { type: "string" },
   { type: "number", field: bigint },
   { type: "number", field: integer },
   { type: "number" },
   { type: "boolean" },
   { type: "date" },
   { type: "timestamp" },
   { type: "timestamptz" },
   { type: "sql native" },
   { type: "" },
];
const OPTIONS: ({ queryTimezone?: string } & Partial<BigIntegerReadOptions>)[] =
   [
      { dialect: "duckdb" },
      { dialect: "postgres" },
      { dialect: "bigquery" },
      { dialect: "duckdb", queryTimezone: "Asia/Tokyo" },
      { dialect: "bigquery", colocated: true },
      {
         dialect: "postgres",
         colocated: true,
         nativeTypeOf: (c: string) => (c === "a" ? "bigint" : undefined),
      },
      {},
   ];

describe("storedColumnReadPlan", () => {
   it("gives exactly the composition every reader built, column by column", () => {
      let compared = 0;
      for (const options of OPTIONS) {
         for (const stored of STORED_TYPES) {
            for (const d of DECLARED) {
               const schema = [{ name: "a", type: stored }];
               const declared: DeclaredRead[] = [
                  {
                     name: "a",
                     type: d.type,
                     ...("field" in d ? { field: d.field } : {}),
                  },
               ];
               expect(storedColumnReadPlan(schema, declared, options)).toEqual(
                  legacy(schema, declared, options),
               );
               compared++;
            }
         }
      }
      expect(compared).toBe(
         OPTIONS.length * STORED_TYPES.length * DECLARED.length,
      );
   });

   it("keeps the readers' order: faithful casts in schema order, then native reads in declared order", () => {
      const schema = [
         { name: "big2", type: "BIGINT" },
         { name: "flag", type: "INTEGER" },
         { name: "big1", type: "BIGINT" },
         { name: "day", type: "TIMESTAMP" },
         { name: "lost", type: "DOUBLE" },
         { name: "text", type: "VARCHAR" },
      ];
      const declared: DeclaredRead[] = [
         { name: "day", type: "date" },
         { name: "big1", type: "number", field: bigint },
         { name: "lost", type: "number", field: bigint },
         { name: "flag", type: "boolean" },
         { name: "big2", type: "number", field: bigint },
         { name: "text", type: "number" },
      ];
      const plan = storedColumnReadPlan(schema, declared, {
         dialect: "duckdb",
      });
      expect(plan).toEqual(legacy(schema, declared, { dialect: "duckdb" }));
      expect(plan.casts.map((c) => c.column)).toEqual([
         "flag",
         "day",
         "big1",
         "big2",
      ]);
      expect(plan.refusals).toEqual([{ column: "lost", storedType: "DOUBLE" }]);
      expect(plan.leftOut.map((m) => m.column)).toEqual(["text"]);
   });

   it("matches a declared column by a unique case fold for its type, and a big integer only by exact name", () => {
      const schema = [
         { name: "DAY", type: "TIMESTAMP" },
         { name: "BIG", type: "BIGINT" },
      ];
      const declared: DeclaredRead[] = [
         { name: "day", type: "date" },
         { name: "big", type: "number", field: bigint },
      ];
      const plan = storedColumnReadPlan(schema, declared, {
         dialect: "duckdb",
      });
      expect(plan).toEqual(legacy(schema, declared, { dialect: "duckdb" }));
      expect(plan.casts).toEqual([{ column: "day", castTo: "DATE" }]);
   });
});
