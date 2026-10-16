// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { parseAnnotation } from "@malloydata/malloy-tag";
import { describe, expect, it } from "bun:test";
import {
   CHART_TAGS,
   chartLineText,
   chartStateOf,
   drawsAs,
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
      expect(parseChartLine("# viz=line")).toEqual({
         negated: [],
         pick: "line_chart",
      });
      expect(parseChartLine("# viz=table")).toEqual({
         negated: [],
         pick: "table",
      });
   });

   it("does not recognize a line with properties, other tags, or two picks", () => {
      for (const line of [
         "# bar_chart { size=spark }",
         '# line_chart label="Revenue"',
         "# viz=line { stack }",
         "# viz=pie",
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
      expect(mentionsChartTag("# viz=line { stack }")).toBe(true);
      expect(mentionsChartTag("# viz=line")).toBe(false);
      expect(mentionsChartTag("# line_chart")).toBe(false);
      expect(mentionsChartTag('# label="Sales"')).toBe(false);
      expect(mentionsChartTag('#" about the bar_chart')).toBe(false);
   });
});

describe("chartLineText", () => {
   it("writes line, bar, bar and line and a table as a viz, and the rest as their own key", () => {
      expect(chartLineText("line_chart")).toBe("# viz=line");
      expect(chartLineText("bar_chart")).toBe("# viz=bar");
      expect(chartLineText("combo_chart")).toBe("# viz=combo");
      expect(chartLineText("none")).toBe("# viz=table");
      expect(chartLineText("big_value")).toBe("# big_value");
      expect(chartLineText("scatter_chart")).toBe("# scatter_chart");
   });

   it("takes nothing off: no line it writes negates a tag", () => {
      for (const pick of [...CHART_TAGS.filter(isChartPick), "none" as const])
         expect(chartLineText(pick)).not.toContain("-");
   });

   it("emits lines the recognizer reads back as the same state", () => {
      for (const pick of CHART_TAGS.filter(isChartPick))
         expect(chartStateOf([chartLineText(pick)])).toBe(pick);
      expect(chartStateOf([chartLineText("none")])).toBe("none");
   });
});

describe("chartLineText over a view's own chart", () => {
   const PLUGINS = [
      "line_chart",
      "bar_chart",
      "big_value",
      "scatter_chart",
      "shape_map",
      "segment_map",
      "combo_chart",
   ] as const;

   it("parses cleanly over any base, merged as the renderer merges a tile's tags", () => {
      for (const base of ["# bar_chart", "# viz=line", "# big_value"])
         for (const pick of [...PLUGINS, "none" as const]) {
            const { log } = parseAnnotation([base, `${chartLineText(pick)}\n`]);
            expect(log).toEqual([]);
         }
   });

   // What `drawsAs` promises is the renderer's rule (resolvedChart, which a
   // spec checks against the renderer itself).
   it("draws a viz pick over any view without a big value or map key, and says where a pick cannot", () => {
      for (const view of [undefined, "bar_chart", "line_chart", "combo_chart"])
         for (const pick of [
            "line_chart",
            "bar_chart",
            "combo_chart",
            "none",
         ] as const)
            expect(drawsAs(pick, view)).toBe(true);
      expect(drawsAs("none", "big_value")).toBe(false);
      expect(drawsAs("bar_chart", "big_value")).toBe(true);
      expect(drawsAs("scatter_chart", undefined)).toBe(true);
      expect(drawsAs("scatter_chart", "bar_chart")).toBe(false);
      expect(drawsAs("big_value", "shape_map")).toBe(true);
      expect(drawsAs("shape_map", "big_value")).toBe(false);
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
      expect(mentionsChartTag("# viz=line size=spark")).toBe(true);
   });
});
