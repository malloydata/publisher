// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The one `# <chart>` tag line a builder writes above a tile or a query cell.
 * Pure text, with no Malloy import, so both writers and the main entry can reach it.
 */

/** The renderer's chart tags, as they are spelled in a model. */
export const CHART_TAGS = [
   "bar_chart",
   "line_chart",
   "scatter_chart",
   "shape_map",
   "segment_map",
   "big_value",
   "sparkline",
];

/** The tags the renderer has a plugin for, in the order a line negates them. */
const PLUGIN_TAGS = [
   "line_chart",
   "bar_chart",
   "big_value",
   "scatter_chart",
   "shape_map",
   "segment_map",
] as const;

export type ChartPick = (typeof PLUGIN_TAGS)[number];

/** What a picker holds: a chart, no chart (a table), the view's own (no line), or a line the builder does not model. */
export type ChartState = ChartPick | "none" | "default" | "custom";

export const isChartPick = (value: unknown): value is ChartPick =>
   PLUGIN_TAGS.includes(value as ChartPick);

const LINE_NAMES = new Set([...CHART_TAGS, "viz"]);

export interface ChartLineParts {
   negated: string[];
   pick?: string;
}

/** `#`, any `-name`s, at most one `name`, every name a chart tag or `viz`, no `{` and no `=`; anything else is not ours to rewrite. */
export function parseChartLine(line: string): ChartLineParts | undefined {
   const trimmed = line.trim();
   if (!/^#[ \t]/.test(trimmed) || /[{=]/.test(trimmed)) return undefined;
   const words = trimmed
      .slice(1)
      .trim()
      .split(/[ \t]+/);
   const negated: string[] = [];
   let pick: string | undefined;
   for (const word of words) {
      if (word.startsWith("-")) {
         if (pick !== undefined) return undefined;
         const name = word.slice(1);
         if (!LINE_NAMES.has(name)) return undefined;
         negated.push(name);
      } else {
         if (pick !== undefined || !LINE_NAMES.has(word)) return undefined;
         pick = word;
      }
   }
   return { negated, ...(pick !== undefined && { pick }) };
}

/** Some other `#` tag line that names a chart tag (`# bar_chart { size=spark }`), which a chart edit must leave alone. */
export function mentionsChartTag(line: string): boolean {
   const trimmed = line.trim();
   if (!/^#[ \t]/.test(trimmed) || parseChartLine(trimmed)) return false;
   const unquoted = trimmed.replace(/"(?:[^"\\]|\\.)*"/g, '""');
   return (unquoted.match(/[A-Za-z_]+/g) ?? []).some((word) =>
      LINE_NAMES.has(word),
   );
}

/** The line for a pick: it negates every other chart tag and `viz`, so it needs no knowledge of the view underneath. */
export function chartLineText(chart: ChartPick | "none"): string {
   const negated = PLUGIN_TAGS.filter((tag) => tag !== chart).map(
      (tag) => `-${tag}`,
   );
   return ["#", ...negated, "-viz", ...(chart === "none" ? [] : [chart])].join(
      " ",
   );
}

/** What a recognized line says: its pick, a table when it negates everything, otherwise a line the picker cannot show. */
export function chartStateOfParts(parts: ChartLineParts): ChartState {
   if (parts.pick !== undefined)
      return isChartPick(parts.pick) ? parts.pick : "custom";
   const all = [...PLUGIN_TAGS, "viz"];
   return all.every((name) => parts.negated.includes(name)) ? "none" : "custom";
}

/** The picker state of a tile's or cell's recognized chart lines. */
export function chartStateOf(lines: string[]): ChartState {
   if (lines.length === 0) return "default";
   if (lines.length > 1) return "custom";
   const parts = parseChartLine(lines[0]);
   return parts ? chartStateOfParts(parts) : "custom";
}

/** What a tile's `#` lines say about its chart: nothing (undefined), a state, or "custom" when any line names a chart tag the builder does not model. */
export function chartStateOfTagLines(lines: string[]): ChartState | undefined {
   const ours = lines.filter((line) => parseChartLine(line));
   if (lines.some(mentionsChartTag)) return "custom";
   return ours.length === 0 ? undefined : chartStateOf(ours);
}

/** The lines of `lines` that are about the chart, recognized or not: what a "custom" state is made of. */
export const chartLinesOf = (lines: string[]): string[] =>
   lines.filter((line) => parseChartLine(line) || mentionsChartTag(line));
