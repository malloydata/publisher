// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type * as Malloy from "@malloydata/malloy-interfaces";
import { buildQueryEnvelope } from "./query_envelope";
import { resultColumns } from "./result_columns";

const a = (...values: string[]) => values.map((value) => ({ value }));

/**
 * The schema of a real storefront result (annotations as Malloy writes them,
 * reference ids shortened):
 *
 *    run: order_items -> {
 *       group_by: created_at.month, category
 *       aggregate: total_sales, order_count
 *       nest: top is { group_by: brand; aggregate: total_sales; limit: 2 }
 *    }
 */
const storefront = {
   connection_name: "duckdb",
   schema: {
      fields: [
         {
            kind: "dimension",
            name: "created_at",
            type: { kind: "date_type", timeframe: "month" },
            annotations: a(
               '#(malloy) reference_id = "d9" drill_expression { kind = time_truncation field_reference.name = created_at truncation = month code = "created_at.month" }\n',
            ),
         },
         {
            kind: "dimension",
            name: "category",
            type: { kind: "string_type" },
            annotations: a(
               "#(doc) Product category of the item sold\n",
               '# label="Category"\n',
               "# drill { to=self given=CATEGORY }\n",
               '#(malloy) reference_id = "17" drill_expression { kind = field_reference name = category code = "products.category" }\n',
            ),
         },
         {
            kind: "dimension",
            name: "total_sales",
            type: { kind: "number_type", subtype: "decimal" },
            annotations: a(
               "#(doc) Total revenue\n",
               "# currency\n",
               '# label="Revenue"\n',
               '#(malloy) reference_id = "a4" calculation drill_expression { kind = field_reference name = total_sales }\n',
            ),
         },
         {
            kind: "dimension",
            name: "order_count",
            type: { kind: "number_type", subtype: "integer" },
            annotations: a(
               '# label="Orders"\n',
               '#(malloy) reference_id = "84" calculation drill_expression { kind = field_reference name = order_count }\n',
            ),
         },
         {
            kind: "dimension",
            name: "top",
            type: {
               kind: "array_type",
               element_type: {
                  kind: "record_type",
                  fields: [
                     {
                        name: "brand",
                        type: { kind: "string_type" },
                        annotations: a('# label="Brand"\n'),
                     },
                     {
                        name: "total_sales",
                        type: { kind: "number_type", subtype: "decimal" },
                        annotations: a(
                           '#(malloy) reference_id = "a4" calculation drill_expression { kind = field_reference name = total_sales path = [top] }\n',
                           "# currency=usd0k\n",
                        ),
                     },
                  ],
               },
            },
            annotations: a('#(malloy) reference_id = "n1"\n'),
         },
      ],
   },
} as unknown as Malloy.Result;

describe("resultColumns", () => {
   it("says what each column of a real result is, nest included", () => {
      expect(resultColumns(storefront)).toEqual([
         { name: "created_at", type: "date", timeframe: "month" },
         { name: "category", type: "string", label: "Category" },
         {
            name: "total_sales",
            type: "number",
            subtype: "decimal",
            label: "Revenue",
            measure: true,
            format: "currency",
         },
         {
            name: "order_count",
            type: "number",
            subtype: "integer",
            label: "Orders",
            measure: true,
         },
         {
            name: "top",
            type: "array",
            fields: [
               { name: "brand", type: "string", label: "Brand" },
               {
                  name: "total_sales",
                  type: "number",
                  subtype: "decimal",
                  measure: true,
                  format: "currency = usd0k",
               },
            ],
         },
      ]);
   });

   it("re-spells each format as the renderer reads it, and takes no chart tag for one", () => {
      const columns = resultColumns({
         schema: {
            fields: [
               ["p", '# percent label="Share"\n'],
               ["d", "# duration=minutes\n"],
               ["n", '# number="#,##0.0"\n'],
               ["c", "# currency { scale=m decimals=2 }\n"],
               ["chart", "# bar_chart\n"],
            ].map(([name, tag]) => ({
               kind: "dimension",
               name,
               type: { kind: "number_type" },
               annotations: a(tag),
            })),
         },
      } as unknown as Malloy.Result);
      expect(columns.map((c) => c.format)).toEqual([
         "percent",
         "duration = minutes",
         'number = "#,##0.0"',
         "currency { scale = m decimals = 2 }",
         undefined,
      ]);
      expect(columns[0].label).toBe("Share");
   });

   it("names the zone a timestamp reads in, a nest's own over the query's, and never a date's", () => {
      const columns = resultColumns({
         query_timezone: "America/New_York",
         schema: {
            fields: [
               {
                  kind: "dimension",
                  name: "at",
                  type: { kind: "timestamp_type", timeframe: "hour" },
               },
               { kind: "dimension", name: "day", type: { kind: "date_type" } },
               {
                  kind: "dimension",
                  name: "tokyo",
                  type: {
                     kind: "array_type",
                     element_type: {
                        kind: "record_type",
                        fields: [
                           { name: "at", type: { kind: "timestamp_type" } },
                        ],
                     },
                  },
                  annotations: a('#(malloy) query_timezone = "Asia/Tokyo"\n'),
               },
            ],
         },
      } as unknown as Malloy.Result);
      expect(columns[0]).toEqual({
         name: "at",
         type: "timestamp",
         timeframe: "hour",
         timezone: "America/New_York",
      });
      expect(columns[1]).toEqual({ name: "day", type: "date" });
      expect(columns[2].fields).toEqual([
         { name: "at", type: "timestamp", timezone: "Asia/Tokyo" },
      ]);
   });

   it("leaves out the zone when it is UTC, which absent already means", () => {
      const [column] = resultColumns({
         query_timezone: "UTC",
         schema: {
            fields: [
               {
                  kind: "dimension",
                  name: "at",
                  type: { kind: "timestamp_type" },
               },
            ],
         },
      } as unknown as Malloy.Result);
      expect(column).toEqual({ name: "at", type: "timestamp" });
   });

   it("gives an array of scalars its element type", () => {
      const [column] = resultColumns({
         schema: {
            fields: [
               {
                  kind: "dimension",
                  name: "tags",
                  type: {
                     kind: "array_type",
                     element_type: { kind: "string_type" },
                  },
               },
            ],
         },
      } as unknown as Malloy.Result);
      expect(column).toEqual({
         name: "tags",
         type: "array",
         element: { type: "string" },
      });
   });

   it("never reads an @env reference into a label", () => {
      const [column] = resultColumns({
         schema: {
            fields: [
               {
                  kind: "dimension",
                  name: "x",
                  type: { kind: "string_type" },
                  annotations: a("# label=@env.SECRET\n"),
               },
            ],
         },
      } as unknown as Malloy.Result);
      expect(column).toEqual({ name: "x", type: "string" });
   });

   it("never reads an @env reference into a nest's timezone", () => {
      process.env.RESULT_COLUMNS_SPEC_SECRET = "leaked";
      try {
         const [column] = resultColumns({
            schema: {
               fields: [
                  {
                     kind: "dimension",
                     name: "nested",
                     type: {
                        kind: "array_type",
                        element_type: {
                           kind: "record_type",
                           fields: [
                              { name: "at", type: { kind: "timestamp_type" } },
                           ],
                        },
                     },
                     annotations: a(
                        "#(malloy) query_timezone = @env.RESULT_COLUMNS_SPEC_SECRET\n",
                     ),
                  },
               ],
            },
         } as unknown as Malloy.Result);
         expect(column.fields).toEqual([{ name: "at", type: "timestamp" }]);
      } finally {
         delete process.env.RESULT_COLUMNS_SPEC_SECRET;
      }
   });

   it("gives an array of timestamps the zone its elements read in", () => {
      const [column] = resultColumns({
         query_timezone: "America/New_York",
         schema: {
            fields: [
               {
                  kind: "dimension",
                  name: "times",
                  type: {
                     kind: "array_type",
                     element_type: { kind: "timestamp_type" },
                  },
               },
            ],
         },
      } as unknown as Malloy.Result);
      expect(column).toEqual({
         name: "times",
         type: "array",
         element: { type: "timestamp", timezone: "America/New_York" },
      });
   });

   it("names only the format the renderer applies when a column has two", () => {
      const [column] = resultColumns({
         schema: {
            fields: [
               {
                  kind: "dimension",
                  name: "x",
                  type: { kind: "number_type" },
                  annotations: a("# percent\n", "# currency\n"),
               },
            ],
         },
      } as unknown as Malloy.Result);
      expect(column.format).toBe("currency");
   });

   it("lists a column it cannot read by name rather than failing the answer", () => {
      const columns = resultColumns({
         schema: { fields: [{ kind: "dimension", name: "odd" }] },
      } as unknown as Malloy.Result);
      expect(columns).toEqual([{ name: "odd", type: "unknown" }]);
   });

   it("is on every execute_query envelope, ahead of _meta", () => {
      const envelope = buildQueryEnvelope(
         [{ category: "Outerwear" }],
         1000,
         storefront,
      );
      expect(envelope._columns.map((c) => c.name)).toEqual([
         "created_at",
         "category",
         "total_sales",
         "order_count",
         "top",
      ]);
      expect(Object.keys(envelope).slice(0, 3)).toEqual([
         "rows",
         "_columns",
         "_meta",
      ]);
   });
});
