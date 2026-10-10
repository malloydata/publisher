// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Promote a composite tile whose result is one row of measures to KPI tiles.
 *
 * Malloyyo does this for every composite tile: a tile that comes back as a
 * single row with nothing but measures is spliced into the grid as big-value
 * cards, the way top-level `aggregate:` items of a `# dashboard` query render.
 * Publisher runs each tile through the renderer on its own, so the same result
 * drew as a one-row table unless the view carried `# big_value` itself. A repo
 * written for Malloyyo therefore looked different here for no reason an author
 * could see in the file.
 *
 * The promotion is the renderer's own tag, prepended to the result's
 * annotations, so the KPI tiles are exactly what `# big_value` on the view would
 * have produced. It steps aside whenever the author already said how the result
 * should draw: any render tag on the result leaves it alone, `# table` included,
 * which is how an author opts out.
 *
 * Textual rather than through `@malloydata/malloy-tag`, which the SDK loads
 * lazily so a host may bundle without it; a synchronous check in the render
 * path cannot wait on that import. The cost is that a tag NAME inside a quoted
 * string reads as a tag, which only ever withholds the promotion.
 */

/**
 * The renderer's top-level render tags: any one of these on the result means
 * the author chose a rendering, and this module has nothing to add.
 */
const RENDER_TAGS = new Set([
   "bar_chart",
   "line_chart",
   "scatter_chart",
   "shape_map",
   "segment_map",
   "point_map",
   "big_value",
   "sparkline",
   "list",
   "list_detail",
   "dashboard",
   "transpose",
   "table",
   "json",
   "image",
   "link",
   "text",
]);

const BIG_VALUE_ANNOTATION = { value: "# big_value\n" };

interface ResultField {
   kind?: string;
   annotations?: Array<{ value?: string }>;
}

interface ResultShape {
   schema?: { fields?: ResultField[] };
   data?: { kind?: string; array_value?: unknown[] };
   annotations?: Array<{ value?: string }>;
}

/**
 * Whether an output field is an aggregate.
 *
 * Measured against the server: every output field of a query comes back
 * `kind: "dimension"` — a result's columns are its dimensions, whatever they
 * were in the source — and what marks an aggregate is the `calculation` token
 * in the field's internal annotation:
 *
 *     #(malloy) reference_id = "…" calculation drill_expression { … }
 *
 * `kind === "measure"` is kept for a renderer that says so directly, but no
 * real result has yet. Until this read the annotation the promotion never
 * fired on real output, and only its own fixture — written with `"measure"` —
 * ever passed it.
 */
const MALLOY_CALCULATION = /^#\(malloy\)[\s\S]*\bcalculation\b/;
function isMeasureField(field: ResultField | undefined): boolean {
   if (!field) return false;
   if (field.kind === "measure") return true;
   return (field.annotations ?? []).some(
      (annotation) =>
         typeof annotation?.value === "string" &&
         MALLOY_CALCULATION.test(annotation.value.trim()),
   );
}

/**
 * True when the result is one row consisting solely of measures: the shape a
 * `view: kpis is { aggregate: … }` tile produces.
 */
export function isMeasureRow(result: ResultShape): boolean {
   const fields = result.schema?.fields;
   if (!Array.isArray(fields) || fields.length === 0) return false;
   if (!fields.every(isMeasureField)) return false;
   const data = result.data;
   if (!data) return false;
   if (data.kind === "record_cell") return true;
   return (
      data.kind === "array_cell" &&
      Array.isArray(data.array_value) &&
      data.array_value.length === 1
   );
}

/**
 * True when any annotation on the result names a render tag.
 *
 * A `# ` line is a MOTLY tag list: space-separated properties, each a bare name
 * or `name=value`, with `{ … }` sub-properties and `.` paths. Quoted values are
 * dropped before tokenizing so a space inside a label does not split it; the
 * remaining tokens are compared whole, so `bar_chart` matches and `bar_charts`
 * does not. `#(…)` and `#"` lines are documentation, never render tags.
 */
export function carriesRenderTag(annotations: readonly string[]): boolean {
   for (const line of annotations) {
      const text = line.trim();
      if (
         !text.startsWith("#") ||
         text.startsWith("#(") ||
         text.startsWith('#"')
      )
         continue;
      const body = text.slice(1).replace(/"(?:[^"\\]|\\.)*"/g, '""');
      for (const token of body.split(/[\s{}=.,[\]]+/)) {
         if (RENDER_TAGS.has(token)) return true;
      }
   }
   return false;
}

/**
 * The result with `# big_value` prepended when {@link isMeasureRow} and no render
 * tag is already present; otherwise the input string untouched. A result that
 * does not parse as JSON is returned as is, for the renderer to report.
 */
export function promoteMeasureRowToKpis(result: string): string {
   let parsed: ResultShape;
   try {
      parsed = JSON.parse(result) as ResultShape;
   } catch {
      return result;
   }
   if (!parsed || typeof parsed !== "object" || !isMeasureRow(parsed)) {
      return result;
   }
   const annotations = Array.isArray(parsed.annotations)
      ? parsed.annotations
      : [];
   const lines = annotations
      .map((a) => a?.value)
      .filter((v): v is string => typeof v === "string");
   if (carriesRenderTag(lines)) return result;
   return JSON.stringify({
      ...parsed,
      annotations: [BIG_VALUE_ANNOTATION, ...annotations],
   });
}
