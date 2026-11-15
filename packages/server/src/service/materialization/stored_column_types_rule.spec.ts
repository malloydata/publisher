// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The one declaration rule for a stored column: its Malloy type, fitted to live's field, or left out.

import { describe, expect, it } from "bun:test";

import {
   declarableColumns,
   storedMalloyType,
   storedMalloyTypeFor,
} from "./stored_column_types";

describe("storedMalloyType", () => {
   it.each([
      ["BIGINT", "number"],
      ["INTEGER", "number"],
      ["HUGEINT", "number"],
      ["UBIGINT", "number"],
      ["DOUBLE", "number"],
      ["DECIMAL(18,2)", "number"],
      ["NUMERIC", "number"],
      ["VARCHAR", "string"],
      ["VARCHAR(255)", "string"],
      ["TEXT", "string"],
      ["UUID", "string"],
      ["BOOLEAN", "boolean"],
      ["BOOL", "boolean"],
      ["DATE", "date"],
      ["TIMESTAMP", "timestamp"],
      ["TIMESTAMP WITH TIME ZONE", "timestamp"],
      ["TIMESTAMPTZ", "timestamp"],
      ["timestamp", "timestamp"],
      // A list and a struct, as Malloy's array and record types.
      ["INTEGER[]", "number[]"],
      ["INTEGER[][]", "number[][]"],
      ["STRUCT(a INTEGER)", "{ a::number }"],
      [
         "STRUCT(k VARCHAR, v STRUCT(x DOUBLE)[])[]",
         "{ k::string, v::{ x::number }[] }[]",
      ],
   ])("declares %s as %s", (duck, malloy) => {
      expect(storedMalloyType(duck)).toBe(malloy);
   });

   // No declaration reads these back as live does: Malloy refuses `json` in
   // a `type:`, and a quoted native type changes how values come back.
   it.each([
      "BLOB",
      "INTERVAL",
      "JSON",
      "MAP(VARCHAR, INTEGER)",
      "UNION(n INTEGER, s VARCHAR)",
      "ENUM('a', 'b')",
      "STRUCT(m MAP(VARCHAR, INTEGER))",
      "BLOB[]",
      "NOT_A_REAL_TYPE",
   ])("declares no type for %s", (duck) => {
      expect(storedMalloyType(duck)).toBeUndefined();
   });
});

describe("storedMalloyTypeFor", () => {
   const n = (numberType: string) => ({ type: "number", numberType });
   const list = (element: unknown) => ({
      type: "array",
      elementTypeDef: element,
   });
   const records = (fields: unknown[]) => ({
      type: "array",
      elementTypeDef: { type: "record_element" },
      fields,
   });

   it("declares a list or struct in live's shape", () => {
      expect(storedMalloyTypeFor("INTEGER[]", list(n("integer")), true)).toBe(
         "number[]",
      );
      expect(
         storedMalloyTypeFor(
            'STRUCT(k VARCHAR, "my field" INTEGER[])[]',
            records([
               { name: "k", type: "string" },
               { name: "my field", ...list(n("integer")) },
            ]),
            true,
         ),
      ).toBe("{ k::string, `my field`::number[] }[]");
      // A zoned timestamp live types as `timestamptz` is declared as one.
      expect(
         storedMalloyTypeFor(
            "TIMESTAMP WITH TIME ZONE[]",
            list({ type: "timestamptz" }),
            true,
         ),
      ).toBe("timestamptz[]");
      // Where live rounds a bigint too, a `number` reads it as live does.
      expect(storedMalloyTypeFor("BIGINT[]", list(n("bigint")), false)).toBe(
         "number[]",
      );
   });

   it.each([
      ["a bigint live keeps every digit of", "BIGINT[]", list(n("bigint"))],
      [
         "a bigint in a struct",
         "STRUCT(a BIGINT)",
         { type: "record", fields: [{ name: "a", ...n("bigint") }] },
      ],
      [
         "a leaf live types as sql native",
         "STRUCT(a VARCHAR)",
         { type: "record", fields: [{ name: "a", type: "sql native" }] },
      ],
      [
         "a field live types as sql native",
         "STRUCT(a INTEGER)",
         { type: "sql native" },
      ],
      [
         "a struct whose fields are not live's",
         "STRUCT(a INTEGER)",
         { type: "record", fields: [{ name: "b", ...n("integer") }] },
      ],
      ["a list live types as a scalar", "VARCHAR[]", { type: "string" }],
   ])("declares nothing for %s", (_label, stored, field) => {
      expect(storedMalloyTypeFor(stored, field, true)).toBeUndefined();
   });
});

describe("declarableColumns", () => {
   it("declares what reads back faithfully and leaves the rest out, by name and type", () => {
      expect(
         declarableColumns([
            { name: "id", type: "BIGINT" },
            { name: "tags", type: "VARCHAR[]" },
            { name: "attrs", type: " MAP(VARCHAR, INTEGER) " },
         ]),
      ).toEqual({
         declared: [
            { name: "id", type: "BIGINT", malloyType: "number" },
            { name: "tags", type: "VARCHAR[]", malloyType: "string[]" },
         ],
         leftOut: [{ name: "attrs", type: "MAP(VARCHAR, INTEGER)" }],
      });
   });
});
