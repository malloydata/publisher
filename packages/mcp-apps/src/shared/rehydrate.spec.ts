// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { FieldInfo, Schema } from "@malloydata/malloy-interfaces";
import { rehydrate, type ResultMeta } from "./rehydrate";

/**
 * Why this file exists, in the reviewer's words and they were right: this module
 * is the only one in the widget that can produce a chart that is WRONG rather
 * than a chart that is ABSENT.
 *
 * Everything else fails loudly. A missing payload draws an error card, a broken
 * tag is reported by the server, an unparseable result says so. But
 * `rehydrateValue` silently coerces ten cell kinds, and a coercion that is merely
 * wrong produces a perfectly normal-looking chart of the wrong numbers, which is
 * the failure nobody notices. `tool_result_payload.ts` says in its own header
 * that the piece most likely to be wrong against a given server is the piece that
 * most needs tests; that argument applies here at least as strongly, and this
 * module had none.
 *
 * The cases below are the ones where being wrong is invisible: precision loss on
 * bigint, a null that becomes a zero, a nested `nest:` result that flattens, a
 * timestamp that loses its zone, and a schema field whose key is absent from the
 * row.
 */

function schemaOf(...fields: FieldInfo[]): Schema {
   return { fields };
}

function dim(name: string, type: unknown): FieldInfo {
   return { kind: "dimension", name, type } as unknown as FieldInfo;
}

function metaOf(schema: Schema): ResultMeta {
   return { schema, connection_name: "duckdb" };
}

/** One rehydrated cell, read structurally so a wrong shape fails the compile. */
type TestCell = Record<string, unknown>;

/**
 * The top level is always an array_cell of record_cells, one per row, so this is
 * rows of cells: `cellsOf(...)[row][column]`.
 */
function cellsOf(
   rows: Record<string, unknown>[],
   schema: Schema,
): TestCell[][] {
   const result = rehydrate(rows, metaOf(schema)) as unknown as {
      data: { array_value: { record_value: TestCell[] }[] };
   };
   return result.data.array_value.map((r) => r.record_value);
}

function firstCell(rows: Record<string, unknown>[], schema: Schema): TestCell {
   return cellsOf(rows, schema)[0][0];
}

describe("rehydrate: shape", () => {
   it("wraps rows as an array of record cells, positionally by schema field", () => {
      // Positional, not keyed: the renderer reads record_value by index against
      // schema.fields, so a reordering here would mislabel every column while
      // still drawing a normal chart.
      const schema = schemaOf(
         dim("category", { kind: "string_type" }),
         dim("total", { kind: "number_type" }),
      );
      const cells = cellsOf([{ total: 12, category: "Outerwear" }], schema);
      expect(cells[0]).toEqual([
         { kind: "string_cell", string_value: "Outerwear" },
         { kind: "number_cell", number_value: 12 },
      ]);
   });

   it("carries the metadata the renderer reads for tags and timezone", () => {
      const schema = schemaOf(dim("a", { kind: "string_type" }));
      const result = rehydrate([{ a: "x" }], {
         schema,
         connection_name: "duckdb",
         annotations: [{ value: "# bar_chart\n" }],
         query_timezone: "America/Los_Angeles",
      } as ResultMeta) as unknown as Record<string, unknown>;
      expect(result.connection_name).toBe("duckdb");
      expect(result.annotations).toEqual([{ value: "# bar_chart\n" }]);
      expect(result.query_timezone).toBe("America/Los_Angeles");
   });

   it("tolerates the sql field Publisher does not send", () => {
      // Publisher's envelope has no `sql`; Credible's does. It is optional on
      // Malloy's Result, so its absence must not throw or invent a value.
      const schema = schemaOf(dim("a", { kind: "string_type" }));
      const result = rehydrate([{ a: "x" }], metaOf(schema)) as unknown as {
         sql?: unknown;
      };
      expect(result.sql).toBeUndefined();
   });

   it("returns an empty array cell for an empty result, not a null", () => {
      const schema = schemaOf(dim("a", { kind: "string_type" }));
      const result = rehydrate([], metaOf(schema)) as unknown as {
         data: { kind: string; array_value: unknown[] };
      };
      expect(result.data.kind).toBe("array_cell");
      expect(result.data.array_value).toEqual([]);
   });
});

describe("rehydrate: values that are wrong invisibly", () => {
   it("keeps bigint digits in string_value, where the number cannot hold them", () => {
      // The server sends counts above 2^53 as JSON strings so the digits survive
      // the wire. If they are only put through Number(), the chart shows a
      // rounded figure that looks entirely plausible.
      const big = "9007199254740993"; // 2^53 + 1, not representable as a double
      const cell = firstCell(
         [{ n: big }],
         schemaOf(dim("n", { kind: "number_type", subtype: "bigint" })),
      );
      expect(cell.kind).toBe("number_cell");
      expect(cell.subtype).toBe("bigint");
      expect(cell.string_value).toBe(big);
      // The numeric field is lossy by nature; the point is that it is not the
      // only copy.
      expect(String(cell.number_value)).not.toBe(big);
   });

   it("does not attach string_value to an ordinary number", () => {
      const cell = firstCell(
         [{ n: 42 }],
         schemaOf(dim("n", { kind: "number_type" })),
      );
      expect(cell).toEqual({ kind: "number_cell", number_value: 42 });
   });

   it("carries a non-bigint subtype through", () => {
      const cell = firstCell(
         [{ n: 1.5 }],
         schemaOf(dim("n", { kind: "number_type", subtype: "decimal" })),
      );
      expect(cell.subtype).toBe("decimal");
      expect(cell.string_value).toBeUndefined();
   });

   it("coerces a numeric string, since drivers return numbers as text", () => {
      const cell = firstCell(
         [{ n: "1234.5" }],
         schemaOf(dim("n", { kind: "number_type" })),
      );
      expect(cell).toEqual({ kind: "number_cell", number_value: 1234.5 });
   });

   it("maps null to a null cell, never to zero or empty string", () => {
      // The invisible one. A null coerced to 0 draws a bar at the origin that
      // reads as a real measured zero.
      for (const type of [
         { kind: "number_type" },
         { kind: "string_type" },
         { kind: "boolean_type" },
      ]) {
         expect(firstCell([{ v: null }], schemaOf(dim("v", type)))).toEqual({
            kind: "null_cell",
         });
      }
   });

   it("treats a schema field with no matching key as null, not undefined", () => {
      // The row genuinely lacks the key, which happens when a schema field is
      // renamed. A cell of `undefined` would break the renderer's positional
      // read; a null cell renders as an empty value.
      const cell = firstCell(
         [{ present: "x" }],
         schemaOf(
            dim("present", { kind: "string_type" }),
            dim("absent", { kind: "string_type" }),
         ),
      );
      expect(cell).toEqual({ kind: "string_cell", string_value: "x" });
      const cells = cellsOf(
         [{ present: "x" }],
         schemaOf(
            dim("present", { kind: "string_type" }),
            dim("absent", { kind: "string_type" }),
         ),
      );
      expect(cells[0][1]).toEqual({ kind: "null_cell" });
   });

   it("does not confuse false or zero with null", () => {
      // Both are falsy, so a truthiness check here would erase real data.
      expect(
         firstCell(
            [{ v: false }],
            schemaOf(dim("v", { kind: "boolean_type" })),
         ),
      ).toEqual({ kind: "boolean_cell", boolean_value: false });
      expect(
         firstCell([{ v: 0 }], schemaOf(dim("v", { kind: "number_type" }))),
      ).toEqual({ kind: "number_cell", number_value: 0 });
      expect(
         firstCell([{ v: "" }], schemaOf(dim("v", { kind: "string_type" }))),
      ).toEqual({ kind: "string_cell", string_value: "" });
   });

   it("keeps a timestamptz as a timestamp cell with its offset intact", () => {
      // timestamptz and timestamp share a branch. The zone lives in the string,
      // so dropping or reformatting it would shift every point on a time axis.
      const withOffset = "2026-08-24T09:30:00-07:00";
      expect(
         firstCell(
            [{ t: withOffset }],
            schemaOf(dim("t", { kind: "timestamptz_type" })),
         ),
      ).toEqual({ kind: "timestamp_cell", timestamp_value: withOffset });
      expect(
         firstCell(
            [{ t: "2026-08-24 09:30:00" }],
            schemaOf(dim("t", { kind: "timestamp_type" })),
         ),
      ).toEqual({
         kind: "timestamp_cell",
         timestamp_value: "2026-08-24 09:30:00",
      });
   });

   it("keeps a date as a date cell", () => {
      expect(
         firstCell(
            [{ d: "2026-08-24" }],
            schemaOf(dim("d", { kind: "date_type" })),
         ),
      ).toEqual({ kind: "date_cell", date_value: "2026-08-24" });
   });

   it("serialises a parsed json value and passes a json string through", () => {
      expect(
         firstCell(
            [{ j: { a: 1 } }],
            schemaOf(dim("j", { kind: "json_type" })),
         ),
      ).toEqual({ kind: "json_cell", json_value: '{"a":1}' });
      expect(
         firstCell(
            [{ j: '{"a":1}' }],
            schemaOf(dim("j", { kind: "json_type" })),
         ),
      ).toEqual({ kind: "json_cell", json_value: '{"a":1}' });
   });

   it("stringifies a sql_native value", () => {
      expect(
         firstCell(
            [{ v: 123 }],
            schemaOf(dim("v", { kind: "sql_native_type" })),
         ),
      ).toEqual({ kind: "sql_native_cell", sql_native_value: "123" });
   });

   it("falls back to a null cell for a type it does not know", () => {
      // Forward compatibility: a Malloy release adding a cell kind must not throw
      // in the middle of a render.
      expect(
         firstCell(
            [{ v: "x" }],
            schemaOf(dim("v", { kind: "some_future_type" })),
         ),
      ).toEqual({ kind: "null_cell" });
   });
});

describe("rehydrate: nested results from nest:", () => {
   it("expands an array of records, which is what a nest: produces", () => {
      // The shape behind every nested chart. Flattening it, or reading the
      // sub-rows by the wrong fields, renders a dashboard of empty tiles.
      const schema = schemaOf(
         dim("state", { kind: "string_type" }),
         dim("by_month", {
            kind: "array_type",
            element_type: {
               kind: "record_type",
               fields: [
                  { name: "month", type: { kind: "string_type" } },
                  { name: "sales", type: { kind: "number_type" } },
               ],
            },
         }),
      );
      const cells = cellsOf(
         [
            {
               state: "CA",
               by_month: [
                  { month: "2026-01", sales: 10 },
                  { month: "2026-02", sales: 20 },
               ],
            },
         ],
         schema,
      );
      expect(cells[0][1]).toEqual({
         kind: "array_cell",
         array_value: [
            {
               kind: "record_cell",
               record_value: [
                  { kind: "string_cell", string_value: "2026-01" },
                  { kind: "number_cell", number_value: 10 },
               ],
            },
            {
               kind: "record_cell",
               record_value: [
                  { kind: "string_cell", string_value: "2026-02" },
                  { kind: "number_cell", number_value: 20 },
               ],
            },
         ],
      });
   });

   it("expands an inline record", () => {
      const schema = schemaOf(
         dim("point", {
            kind: "record_type",
            fields: [
               { name: "lat", type: { kind: "number_type" } },
               { name: "lon", type: { kind: "number_type" } },
            ],
         }),
      );
      expect(firstCell([{ point: { lat: 1, lon: 2 } }], schema)).toEqual({
         kind: "record_cell",
         record_value: [
            { kind: "number_cell", number_value: 1 },
            { kind: "number_cell", number_value: 2 },
         ],
      });
   });

   it("expands a scalar array", () => {
      const schema = schemaOf(
         dim("tags", {
            kind: "array_type",
            element_type: { kind: "string_type" },
         }),
      );
      expect(firstCell([{ tags: ["a", "b"] }], schema)).toEqual({
         kind: "array_cell",
         array_value: [
            { kind: "string_cell", string_value: "a" },
            { kind: "string_cell", string_value: "b" },
         ],
      });
   });

   it("renders an empty or absent nest as an empty array, not a crash", () => {
      const schema = schemaOf(
         dim("by_month", {
            kind: "array_type",
            element_type: {
               kind: "record_type",
               fields: [{ name: "m", type: { kind: "string_type" } }],
            },
         }),
      );
      expect(firstCell([{ by_month: [] }], schema)).toEqual({
         kind: "array_cell",
         array_value: [],
      });
      // A nest that returned nothing arrives as null, which is a null cell
      // rather than an empty array: the distinction is the renderer's to make.
      expect(firstCell([{ by_month: null }], schema)).toEqual({
         kind: "null_cell",
      });
   });

   it("treats a non-array where an array is declared as empty rather than throwing", () => {
      const schema = schemaOf(
         dim("tags", {
            kind: "array_type",
            element_type: { kind: "string_type" },
         }),
      );
      expect(firstCell([{ tags: "not-an-array" }], schema)).toEqual({
         kind: "array_cell",
         array_value: [],
      });
   });
});
