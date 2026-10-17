// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The one `# <chart>` tag line a builder writes above a tile or a query cell.
 * Pure text, with no Malloy import, so both writers and the main entry can reach it.
 */

import { blankQuoted } from "../../utils/blankQuoted";
import { resolvedChart } from "../../utils/resolvedChart";

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

/** The `viz=` value the builder writes for a pick the renderer selects by `viz`. */
const VIZ_OF: Partial<Record<ChartPick | "none", string>> = {
   line_chart: "line",
   bar_chart: "bar",
   combo_chart: "combo",
   none: "table",
};

const VIZ_VALUES = new Set(["line", "bar", "combo", "table"]);

/** The pick a `viz=` value stands for: `table` for a table, else its chart tag. */
const pickOfViz = (viz: string): string =>
   viz === "table"
      ? "table"
      : (Object.keys(VIZ_OF).find(
           (pick) => VIZ_OF[pick as ChartPick] === viz,
        ) as string);

/**
 * A chart line the builder can rewrite: `#`, any `-name`s, then at most one
 * pick, which is a chart tag (`big_value`) or `viz=<line|bar|combo|table>`;
 * every name a chart tag or `viz`, and no `{`. That covers the builder's own
 * lines (`# viz=bar`, `# -big_value viz=table`) and the ones earlier builders
 * wrote (`# -bar_chart -viz line_chart`). Anything else is not ours to
 * rewrite.
 */
export function parseChartLine(line: string): ChartLineParts | undefined {
   const trimmed = line.trim();
   if (!/^#[ \t]/.test(trimmed) || trimmed.includes("{")) return undefined;
   const words = trimmed
      .slice(1)
      .trim()
      .split(/[ \t]+/);
   const negated: string[] = [];
   let pick: string | undefined;
   for (const word of words) {
      if (pick !== undefined) return undefined;
      if (word.startsWith("-")) {
         const name = word.slice(1);
         if (!LINE_NAMES.has(name)) return undefined;
         negated.push(name);
         continue;
      }
      const viz = /^viz=(.*)$/.exec(word)?.[1];
      if (viz !== undefined) {
         if (!VIZ_VALUES.has(viz)) return undefined;
         pick = pickOfViz(viz);
         continue;
      }
      if (!LINE_NAMES.has(word) || word === "viz") return undefined;
      pick = word;
   }
   if (negated.length === 0 && pick === undefined) return undefined;
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
 * with no tag at all (see `promoteMeasureRowToKpis`), which is what Default
 * must show; measured, a `# big_value` after a `# -big_value` in the
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
 * The line for a pick, taking off the view's own chart only where it must.
 *
 * A tile's tags merge over its view's, and the renderer picks the chart from
 * the merged tag ({@link resolvedChart}). Line, bar, bar and line and a table
 * are a `viz=`, which replaces any of those the view sets, so they need
 * nothing taken off. Big value, scatter and the maps are their own key, drawn
 * whatever `viz` says and tried after line, bar and bar and line. So a pick
 * over a view whose chart would still win takes that chart off first: `reset`,
 * from {@link chartResetFor}, as `# -big_value viz=table` or
 * `# -bar_chart -viz scatter_chart`.
 */
export function chartLineText(
   chart: ChartPick | "none",
   reset: readonly string[] = [],
): string {
   const viz = VIZ_OF[chart];
   return [
      "#",
      ...reset.map((key) => `-${key}`),
      viz !== undefined ? `viz=${viz}` : chart,
   ].join(" ");
}

/** What the renderer draws `pick`'s line, with `reset`, over a view drawn as `viewChart`. */
const drawnOver = (
   pick: ChartPick | "none",
   viewChart: string | undefined,
   reset: readonly string[],
) =>
   resolvedChart([
      ...(viewChart ? [`# ${viewChart}`] : []),
      chartLineText(pick, reset),
   ]);

const draws = (pick: ChartPick | "none", drawn: string | undefined) =>
   pick === "none" ? drawn === "table" : drawn === pick;

/**
 * What `pick`'s line has to take off a view drawn as `viewChart` (the
 * catalog's {@link chartOf}) for the pick to draw: nothing where the line alone
 * wins, which is most picks; else the view's chart key, and `viz` too when that
 * chart is one a `viz=` may have set (line, bar, bar and line), since the
 * catalog reports `viz=bar` as `bar_chart`.
 */
export function chartResetFor(
   pick: ChartPick | "none",
   viewChart: string | undefined,
): string[] {
   if (!viewChart || draws(pick, drawnOver(pick, viewChart, []))) return [];
   return ["line_chart", "bar_chart", "combo_chart"].includes(viewChart)
      ? [viewChart, "viz"]
      : [viewChart];
}

/** Whether `pick` draws over a view drawn as `viewChart`, with the reset {@link chartResetFor} writes. */
export function drawsAs(
   pick: ChartPick | "none",
   viewChart: string | undefined,
): boolean {
   return draws(
      pick,
      drawnOver(pick, viewChart, chartResetFor(pick, viewChart)),
   );
}

/** What a recognized line says: its pick, a table when it negates `viz` and picks nothing, otherwise a line the picker cannot show. */
function chartStateOfParts(parts: ChartLineParts): ChartState {
   if (parts.pick === "table") return "none";
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
