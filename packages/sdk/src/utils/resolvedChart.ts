// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { blankQuoted } from "./blankQuoted";

/**
 * Which chart a set of tag lines draws, decided the way the renderer decides
 * it. The one copy of that rule: the builder's chart picker, the catalog's
 * "chart a view declares", the result panel's shape check and the report
 * writer all ask this, so none of them can disagree with what is drawn.
 *
 * The rule, read from `@malloydata/render` (0.0.435, `index.mjs`):
 *
 * 1. The renderer offers the result to its chart plugins in a fixed order —
 *    line, bar, bar and line, big value, scatter, shape map, segment map
 *    (`MalloyRenderer`'s registry) — and the FIRST that matches draws it.
 * 2. Line, bar and bar and line match on `viz` when the tag has one
 *    (`viz=line`, `viz=bar`, `viz=combo`), and on their own key
 *    (`line_chart`, …) only when it has none. So a `viz` replaces any of
 *    those keys, wherever they were written.
 * 3. Big value, scatter, shape map and segment map match on their own key
 *    (`big_value`, `scatter_chart`, …) whatever `viz` says.
 * 4. With no plugin, `viz=table` and `viz=dashboard` say how it is drawn.
 *
 * Lines are read in order as the tag parser merges them: a later key
 * overrides, `-key` takes one off. Only top-level words count, so a value
 * (`label="bar_chart"`) or a property inside `{ … }` is never a chart.
 */

/** The renderer's chart plugins, in the order it tries them. */
const PLUGINS = [
   "line_chart",
   "bar_chart",
   "combo_chart",
   "big_value",
   "scatter_chart",
   "shape_map",
   "segment_map",
] as const;

/** The plugins a `viz=` value selects, and which therefore ignore their own key while `viz` is set. */
const VIZ_PLUGIN: Record<string, string> = {
   line: "line_chart",
   bar: "bar_chart",
   combo: "combo_chart",
};
const VIZ_SELECTED = new Set(Object.values(VIZ_PLUGIN));

/** What the lines draw as: a plugin's tag name, `table` or `dashboard`, or undefined for the default. */
export type ResolvedChart =
   | (typeof PLUGINS)[number]
   | "table"
   | "dashboard"
   | undefined;

/** The top-level words of one `#` tag line: `# a b=c { d }` -> `a`, `b=c`. Doc and routed notes have none. */
function topLevelWords(line: string): string[] {
   const text = line.trim();
   if (!text.startsWith("#") || text.startsWith("#(") || text.startsWith('#"'))
      return [];
   const body = blankQuoted(text.slice(1));
   const words: string[] = [];
   let word = "";
   let depth = 0;
   const flush = () => {
      if (word) words.push(word);
      word = "";
   };
   for (const ch of body) {
      if (ch === "{" || ch === "[") {
         if (depth === 0) flush();
         depth++;
      } else if (ch === "}" || ch === "]") {
         depth = Math.max(0, depth - 1);
      } else if (depth > 0) {
         continue;
      } else if (/[\s,]/.test(ch)) {
         flush();
      } else {
         word += ch;
      }
   }
   flush();
   return words;
}

/** The chart `lines` draw, by the renderer's rule above. */
export function resolvedChart(lines: readonly string[]): ResolvedChart {
   const keys = new Set<string>();
   let viz: string | undefined;
   for (const line of lines)
      for (const raw of topLevelWords(line)) {
         // `a = b` splits into `a`, `=`, `b` around spaces; this reads the
         // unspaced spelling, which is how a chart key and `viz=` are written.
         const word = raw.trim();
         if (word.startsWith("-")) {
            const name = word.slice(1);
            if (name === "viz") viz = undefined;
            keys.delete(name);
            continue;
         }
         const eq = word.indexOf("=");
         if (eq >= 0) {
            const name = word.slice(0, eq);
            if (name === "viz") viz = word.slice(eq + 1) || undefined;
            keys.add(name);
            continue;
         }
         keys.add(word);
      }
   for (const plugin of PLUGINS) {
      const matches = VIZ_SELECTED.has(plugin)
         ? viz !== undefined
            ? VIZ_PLUGIN[viz] === plugin
            : keys.has(plugin)
         : keys.has(plugin);
      if (matches) return plugin;
   }
   if (viz === "table" || viz === "dashboard") return viz;
   return undefined;
}
