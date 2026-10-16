// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { resolvedChart } from "./resolvedChart";

describe("resolvedChart", () => {
   it("reads a chart key, and viz= for line, bar and bar and line", () => {
      expect(resolvedChart(["# bar_chart\n"])).toBe("bar_chart");
      expect(resolvedChart(["# viz=line\n"])).toBe("line_chart");
      expect(resolvedChart(["# viz=combo\n"])).toBe("combo_chart");
      expect(resolvedChart(["# viz=table\n"])).toBe("table");
      expect(resolvedChart([])).toBeUndefined();
   });

   // A report tile's query merges its own tags over its view's. The view's
   // chart comes first, the query's after.
   it("lets a viz replace a line, bar or bar and line key, wherever it was written", () => {
      expect(resolvedChart(["# line_chart\n", "# viz=bar\n"])).toBe(
         "bar_chart",
      );
      expect(resolvedChart(["# viz=bar\n", "# line_chart\n"])).toBe(
         "bar_chart",
      );
      expect(
         resolvedChart(["# bar_chart { size=spark }\n", "# viz=line\n"]),
      ).toBe("line_chart");
      expect(resolvedChart(["# combo_chart\n", "# viz=table\n"])).toBe("table");
   });

   it("lets big value, scatter and the maps win whatever viz says, by the renderer's order", () => {
      expect(resolvedChart(["# big_value\n", "# viz=table\n"])).toBe(
         "big_value",
      );
      expect(resolvedChart(["# scatter_chart\n", "# viz=table\n"])).toBe(
         "scatter_chart",
      );
      // Bar is tried before big value, so bar wins while viz selects it.
      expect(resolvedChart(["# big_value\n", "# viz=bar\n"])).toBe("bar_chart");
      // With no viz, a bar key wins over a scatter key whatever their order.
      expect(resolvedChart(["# scatter_chart\n", "# bar_chart\n"])).toBe(
         "bar_chart",
      );
      expect(resolvedChart(["# bar_chart\n", "# line_chart\n"])).toBe(
         "line_chart",
      );
   });

   it("takes a key off with -key, and reads only top-level words", () => {
      expect(resolvedChart(["# bar_chart\n", "# -bar_chart -viz\n"])).toBe(
         undefined,
      );
      expect(resolvedChart(["# viz=bar\n", "# -viz\n"])).toBeUndefined();
      expect(resolvedChart(['# label="bar_chart"\n'])).toBeUndefined();
      expect(resolvedChart(['#(doc) "a line_chart"\n'])).toBeUndefined();
      expect(resolvedChart(["# dashboard { bar_chart }\n"])).toBeUndefined();
      expect(resolvedChart(["# viz=bar { stack }\n"])).toBe("bar_chart");
      // viz=scatter selects no plugin: scatter matches only on its own key.
      expect(resolvedChart(["# viz=scatter\n"])).toBeUndefined();
   });
});
