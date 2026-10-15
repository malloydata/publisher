// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   chartMinWidth,
   humanizeFieldName,
   labelChartFields,
   MIN_CHART_WIDTH_PX,
} from "./chartPresentation";
import { requestsRowChart } from "./chartShapeProblem";

// The shape the server returns: output fields with their annotations, the
// query's own annotations at the root. Trimmed from a real `orders` result.
const field = (name: string, ...lines: string[]) => ({
   kind: "dimension",
   name,
   type: { kind: "number_type" },
   annotations: lines.map((value) => ({ value: `${value}\n` })),
});

const result = (rootLines: string[], fields: unknown[], rows = 2) => ({
   annotations: rootLines.map((value) => ({ value: `${value}\n` })),
   schema: { fields },
   data: { kind: "array_cell", array_value: Array.from({ length: rows }) },
});

const labelsOf = (out: { schema: { fields: unknown[] } }) =>
   (
      out.schema.fields as Array<{
         name: string;
         annotations: Array<{ value: string }>;
      }>
   ).map((f) => [
      f.name,
      f.annotations.map((a) => a.value.trim()).find((v) => /label=/.test(v)),
   ]);

describe("labelChartFields", () => {
   it("titles a chart's unlabelled fields the way a reader says them", () => {
      const out = labelChartFields(
         result(
            ["# bar_chart"],
            [
               field("order_month"),
               field("total_revenue", "# currency"),
               field("carrier", '# label="Airline"'),
            ],
         ),
      );
      expect(labelsOf(out)).toEqual([
         ["order_month", '# label="Order month"'],
         ["total_revenue", '# label="Total revenue"'],
         // The author's own label is never replaced.
         ["carrier", '# label="Airline"'],
      ]);
   });

   it("leaves a table's columns as their author named them", () => {
      const input = result([], [field("total_revenue")]);
      expect(labelChartFields(input)).toBe(input);
   });

   it("follows the builder's chart lines, which negate before they pick", () => {
      const picked = result(
         ["# line_chart", "# -line_chart -viz bar_chart"],
         [field("total_revenue")],
      );
      expect(labelsOf(labelChartFields(picked))[0][1]).toBe(
         '# label="Total revenue"',
      );
      // A table pick on a view that carried a chart: no chart is left.
      const table = result(
         ["# bar_chart", "# -bar_chart -viz"],
         [field("total_revenue")],
      );
      expect(labelChartFields(table)).toBe(table);
   });

   it("labels a nested chart's fields and not its table sibling's", () => {
      const nest = (name: string, lines: string[]) => ({
         kind: "join",
         name,
         annotations: lines.map((value) => ({ value: `${value}\n` })),
         type: {
            kind: "array_type",
            element_type: {
               kind: "record_type",
               fields: [field("flight_count")],
            },
         },
      });
      const out = labelChartFields(
         result(
            ["# dashboard"],
            [nest("by_month", ["# line_chart"]), nest("detail", [])],
         ),
      ) as unknown as {
         schema: {
            fields: Array<{
               type: {
                  element_type: {
                     fields: Array<{ annotations: Array<{ value: string }> }>;
                  };
               };
            }>;
         };
      };
      const childLines = (index: number) =>
         out.schema.fields[index].type.element_type.fields[0].annotations.map(
            (a) => a.value.trim(),
         );
      expect(childLines(0)).toContain('# label="Flight count"');
      expect(childLines(1)).toEqual([]);
   });
});

describe("requestsRowChart", () => {
   it("reads viz= as a chart, and ignores doc and quoted text", () => {
      expect(requestsRowChart(["# viz=bar"])).toBe(true);
      expect(requestsRowChart(['#" a bar_chart of sales'])).toBe(false);
      expect(requestsRowChart(['# label="bar_chart"'])).toBe(false);
      expect(requestsRowChart(["# big_value"])).toBe(false);
   });
});

describe("humanizeFieldName", () => {
   it("writes snake case as a sentence", () => {
      expect(humanizeFieldName("total_revenue")).toBe("Total revenue");
      expect(humanizeFieldName("name")).toBe("Name");
   });
});

describe("chartMinWidth", () => {
   it("gives a bar chart room for every bar, so none is cut", () => {
      // The renderer draws one bar per 8px and drops the rest.
      const width = chartMinWidth("bar", result(["# bar_chart"], [], 80));
      expect(width).toBeGreaterThanOrEqual(80 * 8);
   });

   it("floors a short bar or line chart at a readable width", () => {
      expect(chartMinWidth("bar", result([], [], 3))).toBe(MIN_CHART_WIDTH_PX);
      expect(chartMinWidth("line", result([], [], 300))).toBe(
         MIN_CHART_WIDTH_PX,
      );
   });

   it("leaves anything that is not a plotted chart alone", () => {
      expect(chartMinWidth("table", result([], [], 80))).toBeUndefined();
      expect(chartMinWidth("big_value", result([], []))).toBeUndefined();
      expect(chartMinWidth(undefined, result([], []))).toBeUndefined();
   });
});
