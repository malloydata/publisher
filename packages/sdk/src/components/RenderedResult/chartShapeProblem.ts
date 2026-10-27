// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What stops a result's chart from drawing, said in the reader's words, or
 * undefined when nothing does.
 *
 * The renderer refuses a chart its result cannot fill, but says so in its own
 * terms ("Line chart settings error: Malloy Line Chart: No dimensions found.
 * A line chart must have at least 1 dimension for the x axis."), which a
 * reader who never wrote the query cannot act on. This reads the same two
 * facts the renderer checks, which chart the result asks for and how many of
 * its columns are group-bys and totals, so the panel can say what is missing
 * before the renderer is asked.
 *
 * Conservative by design: anything it cannot read (a channel placed by hand,
 * on a field or on the chart tag, a result that does not parse) is left to
 * the renderer.
 */

import { blankQuoted } from "../../utils/blankQuoted";
import { resolvedChart } from "../../utils/resolvedChart";

/** The charts that plot rows along an x axis, by their tag. */
const ROW_CHARTS: Record<string, string> = {
   line_chart: "A line chart",
   bar_chart: "A bar chart",
   scatter_chart: "A scatter chart",
   combo_chart: "A bar and line chart",
};

/** A field tag that names a chart channel: the author placed fields by hand. */
const CHANNEL_TAGS = new Set(["x", "y", "y2", "series"]);

/** An output column as a result's schema, or a view's in the catalog, describes it. */
export interface ResultField {
   kind?: string;
   type?: { kind?: string };
   annotations?: Array<{ value?: string }>;
}

interface ResultShape {
   schema?: { fields?: ResultField[] };
   annotations?: Array<{ value?: string }>;
}

/** The words of a `#` tag line, quoted values blanked; doc and internal notes have none. */
function tagWords(line: string): string[] {
   const text = line.trim();
   if (!text.startsWith("#") || text.startsWith("#(") || text.startsWith('#"'))
      return [];
   return blankQuoted(text.slice(1))
      .split(/[\s{}[\],]+/)
      .filter(Boolean);
}

/** The chart plugin the result's lines are drawn by, as the renderer picks it ({@link resolvedChart}); undefined for a table or no chart. */
export function requestedChart(lines: readonly string[]): string | undefined {
   const chart = resolvedChart(lines);
   return chart === "table" || chart === "dashboard" ? undefined : chart;
}

/** Whether the lines ask for a chart that plots rows (bar, line, scatter, bar and line), not big numbers or a table. */
export function requestsRowChart(lines: readonly string[]): boolean {
   const chart = requestedChart(lines);
   return chart !== undefined && ROW_CHARTS[chart] !== undefined;
}

/** A result's or a view's output columns by role: group-bys for the x axis, totals for the y axis. */
export interface ColumnCounts {
   groupBys: number;
   totals: number;
}

/** What a chart that plots rows is missing: a group-by, a total, or exactly two totals for a bar and line chart. */
export type ChartShapeGap = "no_x_axis" | "no_y_axis" | "combo_totals";

/**
 * What keeps `chart` from drawing columns of this shape, or undefined when
 * nothing does or `chart` does not plot rows. The one rule both the builder's
 * chart picker and the result panel apply. A scatter chart plots its first
 * two columns against each other, whatever they are, so two group-bys draw,
 * and so do two totals with no group-by.
 */
export function chartShapeGap(
   chart: string,
   { groupBys, totals }: ColumnCounts,
): ChartShapeGap | undefined {
   if (ROW_CHARTS[chart] === undefined) return undefined;
   if (chart === "scatter_chart")
      return groupBys === 0 && totals < 2 ? "no_x_axis" : undefined;
   if (groupBys === 0) return "no_x_axis";
   if (chart === "combo_chart")
      return totals === 2 ? undefined : "combo_totals";
   if (totals === 0) return "no_y_axis";
   return undefined;
}

/**
 * Whether a `#(malloy)` note marks its column a total: `calculation` as one of
 * the note's own properties. The note also carries a `drill_expression { … }`
 * whose `name = …` can be a group-by literally named `calculation`, so only
 * words outside any `{…}` or `[…]` and outside quotes count, and never a
 * property's value.
 */
export function marksCalculation(note: string): boolean {
   const text = note.trim();
   if (!text.startsWith("#(malloy)")) return false;
   const body = blankQuoted(text.slice("#(malloy)".length));
   let top = "";
   let depth = 0;
   for (const ch of body) {
      if (ch === "{" || ch === "[") depth++;
      else if (ch === "}" || ch === "]") depth = Math.max(0, depth - 1);
      else if (depth === 0) top += ch === "=" ? " = " : ch;
   }
   const words = top.split(/\s+/);
   return words.some(
      (word, at) => word === "calculation" && words[at - 1] !== "=",
   );
}

/**
 * Whether an output column is a total. The one definition: the result panel,
 * the builder's catalog and the KPI promotion all read a column's role here.
 */
export const isTotalColumn = (field: ResultField | undefined): boolean =>
   field !== undefined &&
   (field.kind === "measure" ||
      (field.annotations ?? []).some((note) =>
         marksCalculation(note?.value ?? ""),
      ));

/** Whether an output column is a nest, which no chart axis takes. */
export const isNestColumn = (field: ResultField): boolean =>
   field.kind === "join" || field.type?.kind === "array_type";

/** Output columns by role: a group-by is neither a total nor a nest. */
export function columnCounts(fields: readonly ResultField[]): ColumnCounts {
   const totals = fields.filter(isTotalColumn).length;
   const groupBys = fields.filter(
      (field) => !isTotalColumn(field) && !isNestColumn(field),
   ).length;
   return { groupBys, totals };
}

/** A tag word that places a field on a chart channel: `y=revenue` on the chart tag, or a bare `y` on a field. */
const placesChannel = (word: string) => CHANNEL_TAGS.has(word.split("=")[0]);

export function chartShapeProblem(
   result: string | undefined,
): string | undefined {
   if (!result) return undefined;
   let parsed: ResultShape;
   try {
      parsed = JSON.parse(result) as ResultShape;
   } catch {
      return undefined;
   }
   const fields = parsed?.schema?.fields;
   if (!Array.isArray(fields) || fields.length === 0) return undefined;
   const lines = (parsed.annotations ?? [])
      .map((note) => note?.value)
      .filter((value): value is string => typeof value === "string");
   const chart = requestedChart(lines);
   const name = chart && ROW_CHARTS[chart];
   if (!chart || !name) return undefined;
   // Placed by hand, on a field (`# y`) or on the chart tag itself
   // (`# combo_chart { y=revenue y2=orders }`): the renderer then reads those
   // channels rather than counting columns, so a count says nothing.
   const handPlaced =
      lines.some((line) => tagWords(line).some(placesChannel)) ||
      fields.some((field) =>
         (field.annotations ?? []).some((note) =>
            tagWords(note.value ?? "").some(placesChannel),
         ),
      );
   if (handPlaced) return undefined;
   const { groupBys, totals } = columnCounts(fields);
   switch (chartShapeGap(chart, { groupBys, totals })) {
      case "no_x_axis":
         return `This chart has no field for the x-axis. ${name} needs a group by, such as a date or a category. Show it as big numbers or a table instead, or add a group by to its query.`;
      case "combo_totals":
         return `${name} needs exactly two totals, the first drawn as bars and the second as a line. This one has ${totals}.`;
      case "no_y_axis":
         return `This chart has no total for the y-axis. ${name} needs at least one aggregate, such as a count or a sum.`;
      default:
         return undefined;
   }
}
