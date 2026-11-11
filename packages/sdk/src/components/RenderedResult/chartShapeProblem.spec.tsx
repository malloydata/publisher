// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "bun:test";
import type { QueryResultState } from "../../hooks/useQueryResult";
import {
   chartShapeGap,
   chartShapeProblem,
   marksCalculation,
   requestedChart,
} from "./chartShapeProblem";
import { ResultPanel } from "./ResultPanel";

// Fields as the server returns them: every output column is a "dimension",
// and a total is marked by `calculation` in its internal note.
const total = (name: string) => ({
   name,
   kind: "dimension",
   type: { kind: "number_type" },
   annotations: [{ value: '#(malloy) reference_id = "x" calculation\n' }],
});
const groupBy = (name: string) => ({
   name,
   kind: "dimension",
   type: { kind: "string_type" },
   annotations: [],
});
const result = (fields: object[], lines: string[]) =>
   JSON.stringify({
      schema: { fields },
      data: { kind: "array_cell", array_value: [] },
      annotations: lines.map((value) => ({ value })),
   });

describe("requestedChart", () => {
   it("reads the lines in order, a later one winning and a negation taking one off", () => {
      expect(requestedChart(["# line_chart\n"])).toBe("line_chart");
      expect(requestedChart(["# bar_chart\n", "# -bar_chart -viz\n"])).toBe(
         undefined,
      );
      expect(requestedChart(["# -bar_chart -viz line_chart\n"])).toBe(
         "line_chart",
      );
      expect(requestedChart(["# viz=bar\n"])).toBe("bar_chart");
      expect(requestedChart(['#(doc) "a line_chart"\n'])).toBe(undefined);
      expect(requestedChart(['# label="bar_chart" line_chart\n'])).toBe(
         "line_chart",
      );
   });

   // Result annotations come from the model. A line of one quote and many
   // escaped quotes made the quoted-value regex this used quadratic.
   it("reads a long unclosed quoted value in linear time", () => {
      const hostile = `# line_chart label="${'\\"'.repeat(200_000)}\n`;
      const started = performance.now();
      expect(requestedChart([hostile])).toBe("line_chart");
      expect(
         chartShapeProblem(result([groupBy("month")], [hostile])),
      ).toStartWith("This chart has no total for the y-axis.");
      expect(performance.now() - started).toBeLessThan(500);
   });
});

describe("marksCalculation", () => {
   it("reads calculation as one of the note's own properties only", () => {
      expect(
         marksCalculation(
            '#(malloy) reference_id = "x" calculation drill_expression { kind = field_reference name = n }\n',
         ),
      ).toBe(true);
      // A group-by literally named `calculation`: the word is in its drill
      // expression and in a quoted value, never a property of the note.
      expect(
         marksCalculation(
            '#(malloy) reference_id = "calculation" drill_expression { kind = field_reference name = calculation }\n',
         ),
      ).toBe(false);
      expect(marksCalculation("#(malloy) label = calculation\n")).toBe(false);
      expect(marksCalculation("# calculation\n")).toBe(false);
   });

   it("counts a group-by named calculation as a group-by, so a line chart over it draws", () => {
      const named = {
         name: "calculation",
         kind: "dimension",
         type: { kind: "string_type" },
         annotations: [
            {
               value: "#(malloy) drill_expression { kind = field_reference name = calculation }\n",
            },
         ],
      };
      expect(
         chartShapeProblem(
            result([named, total("revenue")], ["# line_chart\n"]),
         ),
      ).toBeUndefined();
   });
});

describe("chartShapeGap", () => {
   it("names what a chart that plots rows is missing, and nothing for any other chart", () => {
      expect(chartShapeGap("line_chart", { groupBys: 0, totals: 2 })).toBe(
         "no_x_axis",
      );
      expect(chartShapeGap("bar_chart", { groupBys: 1, totals: 0 })).toBe(
         "no_y_axis",
      );
      expect(
         chartShapeGap("scatter_chart", { groupBys: 2, totals: 0 }),
      ).toBeUndefined();
      expect(chartShapeGap("combo_chart", { groupBys: 1, totals: 3 })).toBe(
         "combo_totals",
      );
      expect(
         chartShapeGap("combo_chart", { groupBys: 1, totals: 2 }),
      ).toBeUndefined();
      expect(
         chartShapeGap("big_value", { groupBys: 0, totals: 0 }),
      ).toBeUndefined();
   });
});

describe("chartShapeProblem", () => {
   const kpis = [total("revenue"), total("order_count")];

   it("names the missing x-axis on a line chart of totals only, in plain words", () => {
      expect(chartShapeProblem(result(kpis, ["# line_chart\n"]))).toBe(
         "This chart has no field for the x-axis. A line chart needs a group by, such as a date or a category. Show it as big numbers or a table instead, or add a group by to its query.",
      );
   });

   it("names the missing y-axis on a bar chart with no total", () => {
      expect(
         chartShapeProblem(result([groupBy("carrier")], ["# bar_chart\n"])),
      ).toBe(
         "This chart has no total for the y-axis. A bar chart needs at least one aggregate, such as a count or a sum.",
      );
   });

   it("counts the totals a bar and line chart was given", () => {
      expect(
         chartShapeProblem(
            result(
               [groupBy("carrier"), ...kpis, total("distance")],
               ["# combo_chart\n"],
            ),
         ),
      ).toBe(
         "A bar and line chart needs exactly two totals, the first drawn as bars and the second as a line. This one has 3.",
      );
   });

   it("says nothing when the chart can draw, when no chart is asked for, or when channels are placed by hand", () => {
      expect(
         chartShapeProblem(
            result([groupBy("carrier"), ...kpis], ["# line_chart\n"]),
         ),
      ).toBeUndefined();
      expect(chartShapeProblem(result(kpis, []))).toBeUndefined();
      expect(
         chartShapeProblem(
            result(
               [{ ...total("revenue"), annotations: [{ value: "# x\n" }] }],
               ["# line_chart\n"],
            ),
         ),
      ).toBeUndefined();
      expect(chartShapeProblem("not json")).toBeUndefined();
   });

   // The renderer reads channels written on the chart tag itself, and counts
   // nothing then: the form its own combo error recommends.
   it("leaves channels placed on the chart tag to the renderer", () => {
      expect(
         chartShapeProblem(
            result(
               [groupBy("carrier"), ...kpis, total("distance")],
               ["# combo_chart { y=revenue y2=order_count }\n"],
            ),
         ),
      ).toBeUndefined();
      expect(
         chartShapeProblem(
            result(
               [groupBy("month"), groupBy("revenue")],
               ["# line_chart { y=revenue }\n"],
            ),
         ),
      ).toBeUndefined();
   });

   // A `viz=` replaces a bar chart in the renderer, so a table drawn over an
   // inherited `# bar_chart` is not a bar chart missing its x-axis.
   it("reads viz=table over an inherited bar chart as a table", () => {
      expect(
         requestedChart(["# bar_chart\n", "# viz=table\n"]),
      ).toBeUndefined();
      expect(
         chartShapeProblem(result(kpis, ["# bar_chart\n", "# viz=table\n"])),
      ).toBeUndefined();
   });

   // The renderer's scatter plots the first two columns, whatever they are.
   it("lets a scatter chart plot two totals against each other", () => {
      expect(
         chartShapeGap("scatter_chart", { groupBys: 0, totals: 2 }),
      ).toBeUndefined();
      expect(chartShapeGap("scatter_chart", { groupBys: 0, totals: 1 })).toBe(
         "no_x_axis",
      );
      expect(
         chartShapeProblem(result(kpis, ["# scatter_chart\n"])),
      ).toBeUndefined();
   });
});

describe("ResultPanel with a chart its result cannot fill", () => {
   it("says what is missing in place of the renderer's error", () => {
      const state = {
         isSuccess: true,
         isError: false,
         fetchStatus: "idle",
         data: {
            data: {
               result: result(
                  [total("revenue"), total("order_count")],
                  ["# line_chart\n"],
               ),
            },
         },
      } as unknown as QueryResultState;
      render(<ResultPanel state={state} context="orders -> kpis" />);
      expect(
         screen
            .getByRole("status")
            .textContent?.startsWith("This chart has no field for the x-axis."),
      ).toBe(true);
   });
});
