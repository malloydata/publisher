// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   carriesRenderTag,
   isMeasureRow,
   promoteMeasureRowToKpis,
} from "./promoteMeasureRow";

/** One row of two measures, in the interfaces format a tile result arrives in. */
const measureRow = (annotations: string[] = []) => ({
   schema: {
      fields: [
         {
            kind: "measure",
            name: "total_sales",
            type: { kind: "number_type" },
         },
         {
            kind: "measure",
            name: "order_count",
            type: { kind: "number_type" },
         },
      ],
   },
   data: {
      kind: "array_cell",
      array_value: [
         {
            kind: "record_cell",
            record_value: [
               { kind: "number_cell", number_value: 1200 },
               { kind: "number_cell", number_value: 42 },
            ],
         },
      ],
   },
   annotations: annotations.map((value) => ({ value })),
});

const annotationsOf = (json: string): string[] =>
   (JSON.parse(json).annotations as Array<{ value: string }>).map(
      (a) => a.value,
   );

describe("promoteMeasureRowToKpis", () => {
   it("prepends # big_value to a bare row of measures", () => {
      const out = promoteMeasureRowToKpis(JSON.stringify(measureRow()));
      expect(annotationsOf(out)).toEqual(["# big_value\n"]);
   });

   it("keeps the author's other annotations behind it", () => {
      const out = promoteMeasureRowToKpis(
         JSON.stringify(measureRow(['# label="Key figures"\n'])),
      );
      expect(annotationsOf(out)).toEqual([
         "# big_value\n",
         '# label="Key figures"\n',
      ]);
   });

   // The author's choice wins, and `# table` is the opt-out: the same result
   // with an explicit rendering is returned byte-identical.
   it.each(["# big_value\n", "# table\n", '# label="x" bar_chart\n'])(
      "leaves a result alone that already carries %j",
      (tag) => {
         const input = JSON.stringify(measureRow([tag]));
         expect(promoteMeasureRowToKpis(input)).toBe(input);
      },
   );

   it("leaves a result with a dimension alone", () => {
      const row = measureRow();
      row.schema.fields.push({
         kind: "dimension",
         name: "state",
         type: { kind: "string_type" },
      });
      const input = JSON.stringify(row);
      expect(promoteMeasureRowToKpis(input)).toBe(input);
   });

   it("leaves several rows of measures alone", () => {
      const row = measureRow();
      row.data.array_value.push(row.data.array_value[0]);
      const input = JSON.stringify(row);
      expect(promoteMeasureRowToKpis(input)).toBe(input);
   });

   it("returns anything it cannot parse untouched, for the renderer to report", () => {
      expect(promoteMeasureRowToKpis("not json")).toBe("not json");
      expect(promoteMeasureRowToKpis("null")).toBe("null");
   });
});

/**
 * The row as the SERVER actually sends it. Every output field is a
 * `dimension`; an aggregate is marked by the `calculation` token in its
 * internal annotation. Captured from `order_items -> key_figures` on the
 * storefront package, trimmed to what the promotion reads.
 */
const serverMeasureRow = () => ({
   schema: {
      fields: [
         {
            kind: "dimension",
            name: "total_sales",
            type: { kind: "number_type", subtype: "decimal" },
            annotations: [
               { value: "#(doc) Total revenue\n" },
               { value: "# currency\n" },
               { value: '# label="Revenue"\n' },
               {
                  value: '#(malloy) reference_id = "276f77be" calculation drill_expression { kind = field_reference name = total_sales }\n',
               },
            ],
         },
         {
            kind: "dimension",
            name: "order_count",
            type: { kind: "number_type", subtype: "integer" },
            annotations: [
               { value: '# label="Orders"\n' },
               {
                  value: '#(malloy) reference_id = "97a73061" calculation drill_expression { kind = field_reference name = order_count }\n',
               },
            ],
         },
      ],
   },
   data: measureRow().data,
   annotations: [
      { value: "#(doc) Revenue and orders\n" },
      { value: "#(malloy) source.name = order_items\n" },
   ],
});

describe("isMeasureRow", () => {
   // The case that matters: real output. Until the aggregate marker was read,
   // this returned false for every result the server ever produced, and the
   // KPI strip drew as a one-row table whenever its view did not carry
   // `# big_value` itself.
   it("recognises the server's own shape, where aggregates are dimensions marked calculation", () => {
      const row = serverMeasureRow();
      expect(isMeasureRow(row)).toBe(true);
      expect(
         annotationsOf(promoteMeasureRowToKpis(JSON.stringify(row)))[0],
      ).toBe("# big_value\n");
   });

   it("refuses a grouped result in the server's shape", () => {
      const row = serverMeasureRow();
      row.schema.fields.unshift({
         kind: "dimension",
         name: "category",
         type: { kind: "string_type", subtype: "text" },
         annotations: [
            { value: '# label="Category"\n' },
            {
               value: '#(malloy) reference_id = "4d02e53c" drill_expression { kind = field_reference name = category }\n',
            },
         ],
      });
      expect(isMeasureRow(row)).toBe(false);
   });

   it("accepts a single record cell as well as a one-element array", () => {
      const row = measureRow();
      expect(isMeasureRow(row)).toBe(true);
      expect(
         isMeasureRow({ ...row, data: row.data.array_value[0] as never }),
      ).toBe(true);
   });

   it("refuses an empty schema", () => {
      expect(
         isMeasureRow({ schema: { fields: [] }, data: measureRow().data }),
      ).toBe(false);
   });
});

describe("carriesRenderTag", () => {
   it("reads a tag anywhere in a property list", () => {
      expect(
         carriesRenderTag(['# label="Sales" bar_chart { x=month }\n']),
      ).toBe(true);
      expect(carriesRenderTag(["# bar_chart.stack\n"])).toBe(true);
   });

   it("ignores documentation lines and formatting-only tags", () => {
      expect(carriesRenderTag(['#" A table of sales\n'])).toBe(false);
      expect(carriesRenderTag(["#(doc) list of orders\n"])).toBe(false);
      expect(carriesRenderTag(["# currency colspan=2\n"])).toBe(false);
   });

   it("matches whole names only", () => {
      expect(carriesRenderTag(["# bar_charts\n"])).toBe(false);
   });

   // A tag name inside a label reads as a tag. The only effect is a withheld
   // promotion, which is the safe direction, and the spec pins that it is so.
   it("errs toward not promoting when a quoted value names a tag", () => {
      expect(carriesRenderTag(['# label="table"\n'])).toBe(false);
      expect(carriesRenderTag(["# label=table\n"])).toBe(true);
   });
});
