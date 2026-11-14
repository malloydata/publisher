// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { parseAnnotation } from "@malloydata/malloy-tag";
import { describe, expect, it } from "bun:test";
import {
   CHART_TAGS,
   chartLineText,
   chartStateOf,
   isChartPick,
   mentionsChartTag,
   parseChartLine,
   seriesColorFrom,
   seriesColorLine,
   seriesColorOf,
} from "./chartLine";

describe("seriesColorFrom", () => {
   // A host theme's series, as the swatches offer them: only what a colour
   // line can hold and read back.
   it("writes a theme colour as #rrggbb, widening #abc and refusing what it cannot write", () => {
      expect(seriesColorFrom("#1F6AA8")).toBe("#1f6aa8");
      expect(seriesColorFrom("#AbC")).toBe("#aabbcc");
      expect(seriesColorFrom("rebeccapurple")).toBeUndefined();
      expect(seriesColorFrom("rgb(1, 2, 3)")).toBeUndefined();
      for (const color of ["#1F6AA8", "#AbC"])
         expect(
            seriesColorOf(seriesColorLine(seriesColorFrom(color) as string)),
         ).toBe(seriesColorFrom(color));
   });
});

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
         "# -bar_chart -big_value -scatter_chart -shape_map -segment_map -combo_chart -viz line_chart",
      );
      expect(chartLineText("bar_chart")).toBe(
         "# -line_chart -big_value -scatter_chart -shape_map -segment_map -combo_chart -viz bar_chart",
      );
   });

   it("writes a combo chart, bars and a line on two axes, like any other pick", () => {
      expect(chartLineText("combo_chart")).toBe(
         "# -line_chart -bar_chart -big_value -scatter_chart -shape_map -segment_map -viz combo_chart",
      );
      expect(chartLineText("combo_chart", ["bar_chart"])).toBe(
         "# -bar_chart -viz combo_chart",
      );
   });

   it("writes -viz on the no-chart line, which keeps a viz= view from rendering a chart", () => {
      expect(chartLineText("none")).toBe(
         "# -line_chart -bar_chart -big_value -scatter_chart -shape_map -segment_map -combo_chart -viz",
      );
   });

   it("negates only the chart tags the view carries when the catalog knows them", () => {
      expect(chartLineText("line_chart", ["bar_chart"])).toBe(
         "# -bar_chart -viz line_chart",
      );
      expect(chartLineText("bar_chart", ["bar_chart"])).toBe(
         "# -viz bar_chart",
      );
      expect(chartLineText("line_chart", [])).toBe("# -viz line_chart");
   });

   // A view of totals draws as big numbers with no tag (the KPI promotion
   // adds one), so Table takes `big_value` off whatever the view carries.
   it("always takes big_value off for Table", () => {
      expect(chartLineText("none", ["bar_chart"])).toBe(
         "# -bar_chart -big_value -viz",
      );
      expect(chartLineText("none", [])).toBe("# -big_value -viz");
   });

   it("reads a narrowed line back as the same state", () => {
      expect(chartStateOf([chartLineText("line_chart", ["bar_chart"])])).toBe(
         "line_chart",
      );
      expect(chartStateOf([chartLineText("none", ["bar_chart"])])).toBe("none");
      expect(chartStateOf([chartLineText("none", [])])).toBe("none");
   });

   it("emits lines the recognizer reads back as the same state", () => {
      for (const pick of CHART_TAGS.filter(isChartPick))
         expect(chartStateOf([chartLineText(pick)])).toBe(pick);
      expect(chartStateOf([chartLineText("none")])).toBe("none");
   });
});

describe("chartLineText through the tag parser", () => {
   const PLUGINS = [
      "line_chart",
      "bar_chart",
      "big_value",
      "scatter_chart",
      "shape_map",
      "segment_map",
      "combo_chart",
   ] as const;
   const BASES = [
      "# bar_chart",
      "# combo_chart",
      "# line_chart",
      "# viz=line",
      "# big_value",
      "# scatter_chart",
   ];

   it("leaves exactly the picked tag over any base, stacked as the renderer inherits", () => {
      for (const base of BASES)
         for (const pick of PLUGINS) {
            const { tag, log } = parseAnnotation([
               base,
               `${chartLineText(pick)}\n`,
            ]);
            expect(log).toEqual([]);
            for (const name of [...PLUGINS, "viz"])
               expect(tag.has(name)).toBe(name === pick);
         }
   });

   it("a narrowed line over a view carrying its tag leaves the same tags as the full line", () => {
      for (const carried of PLUGINS)
         for (const pick of [...PLUGINS, "none" as const]) {
            const { tag, log } = parseAnnotation([
               `# ${carried}`,
               `${chartLineText(pick, [carried])}\n`,
            ]);
            expect(log).toEqual([]);
            for (const name of [...PLUGINS, "viz"])
               expect(tag.has(name)).toBe(name === pick);
         }
   });

   it("turns every chart and viz off for none", () => {
      for (const base of BASES) {
         const { tag, log } = parseAnnotation([
            base,
            `${chartLineText("none")}\n`,
         ]);
         expect(log).toEqual([]);
         for (const name of [...PLUGINS, "viz"])
            expect(tag.has(name)).toBe(false);
      }
   });
});

describe("chartStateOf", () => {
   it("reads no line as default, a partial negation or two lines as custom", () => {
      expect(chartStateOf([])).toBe("default");
      expect(chartStateOf(["# -bar_chart"])).toBe("custom");
      expect(chartStateOf(["# line_chart", "# -viz"])).toBe("custom");
      expect(chartStateOf(["# sparkline"])).toBe("custom");
      expect(chartStateOf(["# bar_chart"])).toBe("bar_chart");
      expect(chartStateOf(["# combo_chart"])).toBe("combo_chart");
   });
});

describe("mentionsChartTag: quoted values", () => {
   it("ignores words inside a string and still sees real ones", () => {
      expect(mentionsChartTag('# label="Sales viz"')).toBe(false);
      expect(mentionsChartTag('# label="a \\" viz"')).toBe(false);
      expect(mentionsChartTag('# line_chart label="Revenue"')).toBe(true);
      expect(mentionsChartTag("# viz=line")).toBe(true);
   });
});
