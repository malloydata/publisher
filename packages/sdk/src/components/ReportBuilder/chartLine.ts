// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The one `# <chart>` tag line a builder writes above a tile or a query cell.
 * Pure text, with no Malloy import, so both writers and the main entry can reach it.
 */

import { blankQuoted } from "../../utils/blankQuoted";

/** The renderer's chart tags, as they are spelled in a model. */
export const CHART_TAGS = [
   "bar_chart",
   "line_chart",
   "scatter_chart",
   "shape_map",
   "segment_map",
   "big_value",
   "sparkline",
   "combo_chart",
];

/** The tags the renderer has a plugin for, in the order a line negates them. */
const PLUGIN_TAGS = [
   "line_chart",
   "bar_chart",
   "big_value",
   "scatter_chart",
   "shape_map",
   "segment_map",
   // Last, so the lines written before the renderer had it keep their order.
   "combo_chart",
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
   const unquoted = blankQuoted(trimmed);
   return (unquoted.match(/[A-Za-z_]+/g) ?? []).some((word) =>
      LINE_NAMES.has(word),
   );
}

/**
 * The line that previews "Default" on a tile whose saved body may carry
 * a chart line of its own: the chart tags taken off, and nothing put on.
 * Unlike Table's line it does not negate `viz`, so it is not a choice of
 * rendering: what the view's own shape gives is what draws.
 *
 * `big_value` is left alone. A one-row view of totals draws as big numbers
 * with no tag at all (see `promoteMeasureRowToKpis`), which is what "From the
 * view" must show; measured, a `# big_value` after a `# -big_value` in the
 * same result does not bring the cards back, and the tile drew a one-row
 * table. Big value can only have been picked on such a view, so keeping it
 * changes nothing that Default would otherwise show.
 */
export function chartResetLine(): string {
   return [
      "#",
      ...PLUGIN_TAGS.filter((tag) => tag !== "big_value").map(
         (tag) => `-${tag}`,
      ),
   ].join(" ");
}

/**
 * The line for a pick. Given the chart tags the view carries it negates only those; without them
 * (the catalog does not know the view) it negates every other chart tag. `-viz` stays either way,
 * since the catalog does not report a `viz=` tag.
 *
 * Table also negates `big_value` always. A one-row view of totals draws as big numbers with no
 * tag of its own (`promoteMeasureRowToKpis` puts `# big_value` ahead of the view's lines), so
 * "the view carries none" is not "nothing to take off": only a `-big_value` after it draws a table.
 */
export function chartLineText(
   chart: ChartPick | "none",
   carried?: readonly string[],
): string {
   const negated = PLUGIN_TAGS.filter(
      (tag) =>
         tag !== chart &&
         (carried === undefined ||
            carried.includes(tag) ||
            (chart === "none" && tag === "big_value")),
   ).map((tag) => `-${tag}`);
   return ["#", ...negated, "-viz", ...(chart === "none" ? [] : [chart])].join(
      " ",
   );
}

/** What a recognized line says: its pick, a table when it negates `viz` and picks nothing, otherwise a line the picker cannot show. */
function chartStateOfParts(parts: ChartLineParts): ChartState {
   if (parts.pick !== undefined)
      return isChartPick(parts.pick) ? parts.pick : "custom";
   return parts.negated.includes("viz") ? "none" : "custom";
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

/**
 * The one colour line a builder writes above a tile: its chart's first series
 * colour, as the per-chart theme annotation the renderer already reads
 * (`# theme.palette.series=["#1f6aa8"]`). One colour only, so a line with a
 * whole palette is the author's and never rewritten.
 */
const SERIES_COLOR_LINE =
   /^#[ \t]*theme\.palette\.series[ \t]*=[ \t]*\[[ \t]*"(#[0-9A-Fa-f]{6})"[ \t]*\][ \t]*$/;

/** The colour a builder-written colour line sets, or undefined for any other line. */
export function seriesColorOf(line: string): string | undefined {
   return SERIES_COLOR_LINE.exec(line.trim())?.[1]?.toLowerCase();
}

/** The colour line for a tile's chart colour. */
export const seriesColorLine = (color: string): string =>
   `# theme.palette.series=["${color.toLowerCase()}"]`;

/**
 * The line that takes a saved colour line off, for a preview of a tile set
 * back to the theme's colours. The tag parser deletes just that path, so any
 * other `theme.*` setting on the view stays.
 */
export const seriesColorResetLine = "# -theme.palette.series";

/** Whether a colour is one the builder writes: `#rrggbb`. */
export const isSeriesColor = (value: string): boolean =>
   /^#[0-9A-Fa-f]{6}$/.test(value);

/**
 * A theme colour as the builder writes it (`#rrggbb`, lower case), or
 * undefined for one it cannot write. A host theme may spell its series `#abc`,
 * which is widened; a named colour or `rgb()` is not offered, since a line
 * holding one would not read back (`seriesColorOf`).
 */
export function seriesColorFrom(value: string): string | undefined {
   const text = value.trim().toLowerCase();
   if (isSeriesColor(text)) return text;
   const short = /^#([0-9a-f])([0-9a-f])([0-9a-f])$/.exec(text);
   return short
      ? `#${short[1]}${short[1]}${short[2]}${short[2]}${short[3]}${short[3]}`
      : undefined;
}

/** The lines of `lines` that are about the chart, recognized or not: what a "custom" state is made of. */
export const chartLinesOf = (lines: string[]): string[] =>
   lines.filter((line) => parseChartLine(line) || mentionsChartTag(line));
