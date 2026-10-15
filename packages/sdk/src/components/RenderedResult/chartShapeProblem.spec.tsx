// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "bun:test";
import type { QueryResultState } from "../../hooks/useQueryResult";
import { chartShapeProblem, requestedChart } from "./chartShapeProblem";
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
      ).toStartWith("This tile has no total for the y-axis.");
      expect(performance.now() - started).toBeLessThan(500);
   });
});

describe("chartShapeProblem", () => {
   const kpis = [total("revenue"), total("order_count")];

   it("names the missing x-axis on a line chart of totals only, in plain words", () => {
      expect(chartShapeProblem(result(kpis, ["# line_chart\n"]))).toBe(
         "This tile has no field for the x-axis. A line chart needs a group by, such as a date or a category. Show it as big numbers or a table instead, or add a group by to its view.",
      );
   });

   it("names the missing y-axis on a bar chart with no total", () => {
      expect(
         chartShapeProblem(result([groupBy("carrier")], ["# bar_chart\n"])),
      ).toBe(
         "This tile has no total for the y-axis. A bar chart needs at least one aggregate, such as a count or a sum.",
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
         "A bar and line chart needs exactly two totals, the first drawn as bars and the second as a line. This tile has 3.",
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
            .textContent?.startsWith("This tile has no field for the x-axis."),
      ).toBe(true);
   });
});
