// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * How a chart reads, decided from the result before the renderer sees it.
 *
 * Two things the renderer leaves to the model, which a model rarely says:
 *
 * - An axis is titled with the field's `# label`, or else its name, so a
 *   measure nobody labelled is titled `total_revenue`. A chart's unlabelled
 *   fields are given a label written the way the builder already titles a tile
 *   (`total_revenue` → "Total revenue"). An author's own `# label` always wins:
 *   only a field with none is touched, and only in a chart, so a table keeps
 *   the column names its author chose.
 * - A plotted chart fills whatever width it is handed, and a bar chart draws at
 *   most one bar per 8px of it, cutting the rest under "Showing limited
 *   results". {@link chartMinWidth} is the narrowest a chart is drawn, past
 *   which its panel scrolls sideways rather than squeezing it.
 *
 * Pure, and over the result's JSON shape only, so it is tested without a
 * renderer.
 */

import { blankQuoted } from "../../utils/blankQuoted";

/** The chart tags a `#` line can pick, in the renderer's spelling. */
const CHART_TAGS = new Set([
   "bar_chart",
   "line_chart",
   "scatter_chart",
   "combo_chart",
]);

/** The `viz=` values that name the same charts. */
const CHART_VIZ = new Set(["bar", "line", "scatter", "combo"]);

interface Annotation {
   value?: string;
}

interface ResultField {
   name?: string;
   kind?: string;
   type?: {
      kind?: string;
      fields?: ResultField[];
      element_type?: { kind?: string; fields?: ResultField[] };
   };
   annotations?: Annotation[];
}

interface ResultShape {
   schema?: { fields?: ResultField[] };
   annotations?: Annotation[];
   data?: { kind?: string; array_value?: unknown[] };
}

const linesOf = (annotations: Annotation[] | undefined): string[] =>
   (annotations ?? [])
      .map((a) => a?.value)
      .filter((v): v is string => typeof v === "string");

/**
 * Whether these `#` lines, read in order, leave a chart picked: `# bar_chart`
 * picks one and `# -bar_chart` takes it back, as the builder's chart lines do
 * (`# -line_chart -viz bar_chart`). `#(…)` and `#"` lines are not tags.
 */
export function picksChart(lines: readonly string[]): boolean {
   const active = new Set<string>();
   for (const line of lines) {
      const text = line.trim();
      if (!text.startsWith("#") || /^#[("|]/.test(text)) continue;
      // A quoted value is never a tag name.
      const body = blankQuoted(text.slice(1));
      for (const token of body.split(/[\s{}]+/)) {
         if (token === "") continue;
         const negated = token.startsWith("-");
         const word = negated ? token.slice(1) : token;
         const viz = /^viz=(\w+)$/.exec(word)?.[1];
         if (word === "viz" && negated) {
            for (const name of [...active])
               if (name.startsWith("viz:")) active.delete(name);
            continue;
         }
         const name =
            viz !== undefined
               ? CHART_VIZ.has(viz)
                  ? `viz:${viz}`
                  : undefined
               : CHART_TAGS.has(word)
                 ? word
                 : undefined;
         if (name === undefined) continue;
         if (negated) active.delete(name);
         else active.add(name);
      }
   }
   return active.size > 0;
}

/** `total_revenue` → "Total revenue". */
export function humanizeFieldName(name: string): string {
   const words = name.replace(/[-_]+/g, " ").replace(/\s+/g, " ").trim();
   return words.charAt(0).toUpperCase() + words.slice(1);
}

const hasLabel = (field: ResultField) =>
   linesOf(field.annotations).some((line) => {
      const text = line.trim();
      // A tag line (not `#(…)` or `#"`) that sets `label=`.
      return /^#[^("]/.test(text) && /(?:^#|[\s{])label\s*=/.test(text);
   });

const quoted = (text: string) =>
   `"${text.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;

/** The fields of a nested result, wherever its type keeps them. */
const nestedFields = (field: ResultField): ResultField[] | undefined =>
   field.type?.element_type?.fields ?? field.type?.fields;

function labelFields(
   fields: ResultField[],
   isChart: boolean,
): { fields: ResultField[]; changed: boolean } {
   let changed = false;
   const out = fields.map((field) => {
      const children = nestedFields(field);
      if (children) {
         // A nest is its own chart, or not, by its own tags.
         const inner = labelFields(
            children,
            picksChart(linesOf(field.annotations)),
         );
         if (!inner.changed) return field;
         changed = true;
         const type = field.type?.element_type
            ? {
                 ...field.type,
                 element_type: {
                    ...field.type.element_type,
                    fields: inner.fields,
                 },
              }
            : { ...field.type, fields: inner.fields };
         return { ...field, type };
      }
      if (!isChart || !field.name || hasLabel(field)) return field;
      const label = humanizeFieldName(field.name);
      if (label === field.name) return field;
      changed = true;
      return {
         ...field,
         annotations: [
            { value: `# label=${quoted(label)}\n` },
            ...(field.annotations ?? []),
         ],
      };
   });
   return { fields: out, changed };
}

/**
 * The result with a human label on every unlabelled field of a chart, at the
 * root or in a nest; the same object when there is nothing to label.
 */
export function labelChartFields<T>(result: T): T {
   if (!result || typeof result !== "object") return result;
   const shape = result as ResultShape;
   const fields = shape.schema?.fields;
   if (!Array.isArray(fields)) return result;
   const { fields: labelled, changed } = labelFields(
      fields,
      picksChart(linesOf(shape.annotations)),
   );
   if (!changed) return result;
   return { ...shape, schema: { ...shape.schema, fields: labelled } } as T;
}

/** The narrowest a plotted chart is drawn before its panel scrolls sideways. */
export const MIN_CHART_WIDTH_PX = 320;

/**
 * Room per bar: the renderer draws one bar per 8px and cuts the rest, so this
 * keeps every bar of a result drawn, at a width a bar still reads at.
 */
const BAR_PX = 10;

/** The plot's own chrome beside its bars: the measure axis and its title. */
const AXIS_PX = 96;

/** Past this a chart is wide enough to scroll through, not to fit. */
const MAX_CHART_WIDTH_PX = 4000;

/**
 * The narrowest width a result's chart is drawn at, or undefined for a
 * result that is not a plotted chart (`renderAs` in the renderer's plugin
 * vocabulary: `bar`, `line`, `combo`, or the tag fallback `chart`).
 */
export function chartMinWidth(
   renderAs: string | undefined,
   result: unknown,
): number | undefined {
   if (
      renderAs !== "bar" &&
      renderAs !== "combo" &&
      renderAs !== "line" &&
      renderAs !== "chart" &&
      renderAs !== "scatter_chart"
   )
      return undefined;
   if (renderAs !== "bar" && renderAs !== "combo") return MIN_CHART_WIDTH_PX;
   const rows = (result as ResultShape | undefined)?.data?.array_value?.length;
   const forBars = rows === undefined ? 0 : rows * BAR_PX + AXIS_PX;
   return Math.min(Math.max(MIN_CHART_WIDTH_PX, forBars), MAX_CHART_WIDTH_PX);
}
