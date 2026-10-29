// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type * as Malloy from "@malloydata/malloy-interfaces";
import type { DrillClause } from "./drillClauses";
import type { DrillField, DrillTagReader } from "./resolveDrill";

/**
 * What "the rows behind a value" runs, read from the result's own metadata
 * rather than from the tile's text.
 *
 * Malloy annotates every result with what a drill needs: the source the query
 * ran on (`source.name`), each nest level's filters as code (`drill_filters`:
 * the view's own `where:` and any refinement's, givens included), and each
 * dimension's expression as code (`drill_expression.code`: `region`,
 * `d.month`, `customers.name`). So the rows behind a row are
 * `run: <source> -> { where: <every filter>, <every dimension> = <value>; select: * }`,
 * whatever the tile was written as: `source -> view`, a refined view, a
 * named query, an inline query, a nest.
 *
 * `where:` and not `drill:`, measured on Malloy 0.0.435. `drill:` needs the
 * view's own path for every dimension, and a refined or named query reports a
 * plain source field with none (`region`, not `by_month.region`), so its rows
 * could not be named that way. And a `drill:` on a joined field
 * (`by_name.name`, grouped as `customers.name`) does not bring the join in,
 * so its query failed with `Referenced table … not found`; the same filter as
 * `where:` does (malloydata/malloy#3158). Once a Malloy release with that fix
 * reaches Publisher, the join is no longer a reason; the refined query's
 * missing view path still is.
 *
 * A field reads as having rows only when every piece is there: a source with
 * no arguments, every level on the way down marked `drillable` (one stage),
 * a code for every filter on the way down, and a code for every dimension of
 * the row. Anything else is not clickable for rows, never a guess. Malloy's
 * metadata is wrong in places that would name the wrong rows (a pipeline's
 * root marked `drillable`, a joined dimension's code); Publisher's server
 * corrects those before it sends a result (`drill_metadata.ts`), and stamps
 * the root {@link DRILL_METADATA_CORRECTED}. A result without the stamp, from
 * a server older than the correction, offers no rows.
 */

/**
 * The root tag Publisher's server writes on a result whose drill metadata it
 * corrected. Spelled the same in the server's `drill_metadata.ts`.
 */
export const DRILL_METADATA_CORRECTED = "drill_metadata_corrected";

/**
 * A renderer result field, as much as the rows behind a value read: the
 * drill field plus Malloy's result metadata, its place in the result, and its
 * type. Its own type because the renderer declares `metadataTag` protected, so
 * a renderer field does not satisfy a `DrillField` that names it; a click's
 * field is read as this one ({@link asRowsField}).
 */
export interface RowsField extends DrillField {
   metadataTag?: DrillTagReader;
   parent?: RowsField;
   fields?: RowsField[];
   drillStableExpression?: Malloy.Expression;
   getEffectiveQueryTimezone?: () => string | undefined;
   isArray?: () => boolean;
   isRecord?: () => boolean;
   isBasic?: () => boolean;
   isString?: () => boolean;
   isNumber?: () => boolean;
   isBoolean?: () => boolean;
   isTime?: () => boolean;
}

/** A clicked or listed renderer field, read for its rows. */
export const asRowsField = (field: DrillField) => field as RowsField;

/** One dimension of the clicked row: its expression as code, and its value. */
export interface RowsDimension {
   code: string;
   value: Malloy.LiteralValue;
}

/** Where the rows come from, and what narrows them. */
export interface RowsTarget {
   source: string;
   /** The tile's own filters, every nest level's, as code, outermost first. */
   filters: string[];
   /** The view or query the value was grouped in, for telemetry and the title. */
   queryName?: string;
}

const call = <T>(f: (() => T) | undefined): T | undefined => {
   try {
      return f?.();
   } catch {
      return undefined;
   }
};

const isDimension = (field: RowsField) =>
   call(field.isBasic?.bind(field)) === true &&
   call(field.wasDimension?.bind(field)) === true;

/**
 * Whether a cell of this field has a value a filter can be written from. The
 * renderer reports no value for any other type (a uuid, JSON, a database-native
 * type), and a row missing one cannot be named.
 */
const hasLiteral = (field: RowsField) =>
   [field.isString, field.isNumber, field.isBoolean, field.isTime].some(
      (is) => call(is?.bind(field)) === true,
   );

/** A dimension's expression as Malloy code, or undefined when the result has none. */
export function dimensionCode(field: RowsField): string | undefined {
   const code = field.metadataTag?.text("drill_expression", "code");
   return code === undefined || code.trim() === "" ? undefined : code;
}

/** The field's ancestors, nearest first, ending at the root. */
function ancestors(field: RowsField): RowsField[] {
   const out: RowsField[] = [];
   for (let at = field.parent; at; at = at.parent) out.push(at);
   return out;
}

/**
 * The source and filters the rows behind `field`'s values run with, or
 * undefined when the result does not carry them all.
 */
export function rowsTargetOf(field: RowsField): RowsTarget | undefined {
   const chain = ancestors(field);
   const root = chain[chain.length - 1] ?? field;
   const tag = root.metadataTag;
   const source = tag?.text("source", "name");
   // A parameterized source would need its arguments spelled; none are read.
   if (
      !source ||
      !tag?.array ||
      tag.array("source", "parameters") !== undefined ||
      // Without the stamp, `drillable` and the codes are Malloy's as written,
      // which name the wrong rows in places.
      tag.has?.(DRILL_METADATA_CORRECTED) !== true
   )
      return undefined;
   const filters: string[] = [];
   // Outermost first, as Malloy applies them; each level's own `where:` and a
   // refinement's are both here, a given (`region ~ $REGION`) included.
   for (const level of [...chain].reverse()) {
      if (call(level.isArray?.bind(level)) !== true) continue;
      // Malloy marks a level `drillable` only when it runs one stage, and
      // Publisher's server corrects the root of a longer pipeline to match:
      // a later stage's dimensions are an earlier stage's outputs, which the
      // source cannot be filtered by.
      if (level.metadataTag?.has?.("drillable") !== true) return undefined;
      for (const filter of level.metadataTag?.array?.("drill_filters") ?? []) {
         const code = filter.text("code");
         if (!code) return undefined;
         filters.push(code);
      }
   }
   const queryName = tag?.text("query_name");
   return {
      source,
      filters,
      ...(queryName && !queryName.startsWith("__") ? { queryName } : {}),
   };
}

/**
 * The dimensions of every level on the way from `field` to the root: the
 * ones a row's values are given for. A level is whatever holds the fields,
 * which for a table row is its repeated record (the root included).
 */
export function rowDimensionsOf(field: RowsField): RowsField[] {
   const seen = new Set<RowsField>();
   for (const level of ancestors(field))
      for (const child of level.fields ?? [])
         if (isDimension(child)) seen.add(child);
   return [...seen];
}

/**
 * The one zone every time dimension of the row was truncated in: undefined
 * when there are none or they use the default, `null` when they differ. A
 * rows query has one `timezone:`, so a nest whose levels set different zones
 * cannot be filtered by all of its values at once.
 */
export function rowTimezone(field: RowsField): string | undefined | null {
   const zones = new Set<string | undefined>(
      rowDimensionsOf(field)
         .filter((d) => call(d.isTime?.bind(d)) === true)
         .map((d) => call(d.getEffectiveQueryTimezone?.bind(d))),
   );
   if (zones.size > 1) return null;
   return [...zones][0];
}

/**
 * Whether a click on `field` can open its rows: a dimension, with a target,
 * and every dimension of its row having a code, an expression the renderer
 * reports its value under, and a value a filter can spell, all in one zone.
 * What the affordance asks, so a value reads as clickable exactly when the
 * click can build its query: the renderer reports no row at all when any
 * dimension lacks an expression, and leaves out one whose value it cannot
 * write.
 */
export function canShowRows(field: RowsField): boolean {
   if (!isDimension(field) || dimensionCode(field) === undefined) return false;
   if (rowsTargetOf(field) === undefined) return false;
   if (rowTimezone(field) === null) return false;
   return rowDimensionsOf(field).every(
      (d) =>
         dimensionCode(d) !== undefined &&
         d.drillStableExpression !== undefined &&
         hasLiteral(d),
   );
}

/**
 * The clicked row's dimensions, each as code with its value, from the
 * renderer's row (`drillClauses`). Undefined unless every dimension of the row
 * is there with a code: never a subset, since dropping a dimension returns
 * rows that are not the row's.
 */
export function rowsDimensions(
   field: RowsField,
   clauses: readonly DrillClause[],
): RowsDimension[] | undefined {
   const row = rowDimensionsOf(field);
   const out: RowsDimension[] = [];
   const seen = new Set<RowsField>();
   for (const clause of clauses) {
      // A level's own filter, if the renderer reported one: it is in the
      // target's filters already, as code.
      if ((clause as { kind?: string }).kind === "drill") continue;
      const { filter } = clause;
      if (filter.kind !== "literal_equality") return undefined;
      const key = JSON.stringify(filter.expression);
      const match = row.find(
         (d) => JSON.stringify(d.drillStableExpression) === key,
      );
      const code = match && dimensionCode(match);
      if (match === undefined || code === undefined) return undefined;
      out.push({ code, value: filter.value });
      seen.add(match);
   }
   return row.every((d) => seen.has(d)) && out.length > 0 ? out : undefined;
}
