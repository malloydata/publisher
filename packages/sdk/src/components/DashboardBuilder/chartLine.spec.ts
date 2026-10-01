// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   CHART_TAGS,
   chartLineText,
   chartStateOf,
   isChartPick,
   mentionsChartTag,
   parseChartLine,
} from "./chartLine";

describe("parseChartLine", () => {
   it("recognizes a bare pick, negations, and the writer's own lines", () => {
      expect(parseChartLine("# line_chart")).toEqual({
         negated: [],
         pick: "line_chart",
      });
      expect(parseChartLine("# -bar_chart line_chart")).toEqual({
         negated: ["bar_chart"],
         pick: "line_chart",
      });
      expect(parseChartLine("# -bar_chart -viz")).toEqual({
         negated: ["bar_chart", "viz"],
      });
      expect(parseChartLine("  #  -viz  \r")).toEqual({ negated: ["viz"] });
   });

   it("does not recognize a line with properties, other tags, or two picks", () => {
      for (const line of [
         "# bar_chart { size=spark }",
         '# line_chart label="Revenue"',
         "# viz=line",
         "# bar_chart line_chart",
         "# line_chart -bar_chart",
         "# bar_chart colspan=2",
         "# bar_chart label",
         "# -nonsense",
         "# drill",
         "#",
         "#-bar_chart",
         '#" a caption',
         "#(doc) bar_chart",
         "## bar_chart",
         "// # bar_chart",
      ])
         expect(parseChartLine(line)).toBeUndefined();
   });
});

describe("mentionsChartTag", () => {
   it("flags the unmodelled lines a chart edit must not touch", () => {
      expect(mentionsChartTag("# bar_chart { size=spark }")).toBe(true);
      expect(mentionsChartTag('# line_chart label="Revenue"')).toBe(true);
      expect(mentionsChartTag("# viz=line")).toBe(true);
      expect(mentionsChartTag("# line_chart")).toBe(false);
      expect(mentionsChartTag('# label="Sales"')).toBe(false);
      expect(mentionsChartTag('#" about the bar_chart')).toBe(false);
   });
});

describe("chartLineText", () => {
   it("negates every other chart tag and viz, in a fixed order", () => {
      expect(chartLineText("line_chart")).toBe(
         "# -bar_chart -big_value -scatter_chart -shape_map -segment_map -viz line_chart",
      );
      expect(chartLineText("bar_chart")).toBe(
         "# -line_chart -big_value -scatter_chart -shape_map -segment_map -viz bar_chart",
      );
   });

   it("writes -viz on the no-chart line, which keeps a viz= view from rendering a chart", () => {
      expect(chartLineText("none")).toBe(
         "# -line_chart -bar_chart -big_value -scatter_chart -shape_map -segment_map -viz",
      );
   });

   it("emits lines the recognizer reads back as the same state", () => {
      for (const pick of CHART_TAGS.filter(isChartPick))
         expect(chartStateOf([chartLineText(pick)])).toBe(pick);
      expect(chartStateOf([chartLineText("none")])).toBe("none");
   });
});

describe("chartStateOf", () => {
   it("reads no line as default, a partial negation or two lines as custom", () => {
      expect(chartStateOf([])).toBe("default");
      expect(chartStateOf(["# -bar_chart"])).toBe("custom");
      expect(chartStateOf(["# line_chart", "# -viz"])).toBe("custom");
      expect(chartStateOf(["# sparkline"])).toBe("custom");
      expect(chartStateOf(["# bar_chart"])).toBe("bar_chart");
   });
});
