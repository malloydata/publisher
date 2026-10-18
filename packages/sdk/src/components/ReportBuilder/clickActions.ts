// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type * as Malloy from "@malloydata/malloy-interfaces";
import { drillClauseLabels, type DrillClause } from "../drill/drillClauses";
import {
   isNestColumn,
   isTotalColumn,
   type ResultField,
} from "../RenderedResult/chartShapeProblem";
import {
   DRILL_SELF,
   drillValueLabel,
   type EntryDrill,
} from "../drill/resolveDrill";
import { drillDestinationLabel, showRowsLabel } from "../drill/useDrill";

/**
 * What the "What clicking a value does" dialog knows about one dimension of a
 * tile: read off the tile's own result, so it lists exactly the dimensions a
 * reader can click, nested ones included, each with the value the tile's first
 * row shows for it.
 */
export interface ClickDimension {
   /** The field name, which a tile entry's `drill { <name> … }` keys on. */
   name: string;
   /** Its `# label`, else its name made readable: what the dialog calls it. */
   label: string;
   /** The model's `# drill` tag on it, which an entry replaces on this tile. */
   modelDrill?: EntryDrill;
   /** Every dimension of the row a click on it filters the rows by, enclosing rows first. */
   rowClauses: DrillClause[];
   /** The value it shows in the tile's first row, as the renderer hands it to a click. */
   value: unknown;
   /** Whether {@link value} is a value a click acts on (not a null). */
   clickable: boolean;
}

/** A dimension column of a result. */
type GroupedField = Extract<Malloy.FieldInfo, { kind: "dimension" }>;

/** A tag object, as `parseAnnotation` returns it. Only the reads this file makes. */
interface TagLike {
   tag(...at: string[]): TagLike | undefined;
   text(...at: string[]): string | undefined;
   textArray(...at: string[]): string[] | undefined;
   has(...at: string[]): boolean;
}
export type ParseTags = (lines: string[]) => { tag?: TagLike | null };

/** `region_name` → `Region name`: the name a field with no `# label` reads as. */
export function readableName(name: string): string {
   const words = name.replace(/[_-]+/g, " ").trim();
   return words.charAt(0).toUpperCase() + words.slice(1);
}

/** A field's `# label` and `# drill`, read off its annotations. */
function fieldTags(
   field: GroupedField,
   parse: ParseTags,
): { label?: string; drill?: EntryDrill } {
   const lines = (field.annotations ?? []).map((note) => note.value);
   const tag = parse(lines).tag ?? undefined;
   const label = tag?.text("label");
   const drill = tag?.tag("drill");
   if (!drill) return label ? { label } : {};
   const one = drill.text("to");
   const to = drill.textArray("to") ?? (one ? [one] : []);
   const given = drill.text("given");
   return {
      ...(label ? { label } : {}),
      drill: given === undefined ? { to } : { to, given },
   };
}

/** A cell as the renderer hands a click its value: a date or time as a `Date`. */
function cellValue(cell: Malloy.Cell | undefined): unknown {
   switch (cell?.kind) {
      case "string_cell":
         return cell.string_value;
      case "number_cell":
         return cell.number_value;
      case "boolean_cell":
         return cell.boolean_value;
      case "date_cell":
         return new Date(cell.date_value);
      case "timestamp_cell":
         return new Date(cell.timestamp_value);
      default:
         return null;
   }
}

/** A cell as the literal a drill clause carries, truncated as its field is. */
function cellLiteral(
   cell: Malloy.Cell | undefined,
   type: Malloy.AtomicType,
): Malloy.LiteralValue {
   switch (cell?.kind) {
      case "string_cell":
         return { kind: "string_literal", string_value: cell.string_value };
      case "number_cell":
         return { kind: "number_literal", number_value: cell.number_value };
      case "boolean_cell":
         return { kind: "boolean_literal", boolean_value: cell.boolean_value };
      case "date_cell":
         return {
            kind: "date_literal",
            date_value: cell.date_value,
            ...(type.kind === "date_type" && type.timeframe
               ? { granularity: type.timeframe }
               : {}),
         };
      case "timestamp_cell":
         return {
            kind: "timestamp_literal",
            timestamp_value: cell.timestamp_value,
            ...(type.kind === "timestamp_type" && type.timeframe
               ? { granularity: type.timeframe }
               : {}),
         };
      default:
         return { kind: "null_literal" };
   }
}

/** A grouped column: a dimension that is neither a total nor a nest. */
const isGrouped = (field: Malloy.FieldInfo): field is GroupedField =>
   field.kind === "dimension" &&
   !isTotalColumn(field as ResultField) &&
   !isNestColumn(field as ResultField);

/** A nested view's rows, as a result lists them: a join holding its own schema. */
const isNest = (
   field: Malloy.FieldInfo,
): field is Extract<Malloy.FieldInfo, { kind: "join" }> =>
   field.kind === "join";

/**
 * Every dimension a reader can click in `result`, in column order, each with
 * the first row's value: a nested view's own dimensions after its parent's,
 * read from the first row of the first row's nest.
 */
export function clickDimensions(
   result: Malloy.Result,
   parse: ParseTags,
): ClickDimension[] {
   const out: ClickDimension[] = [];
   const walk = (
      fields: readonly Malloy.FieldInfo[],
      row: readonly Malloy.Cell[] | undefined,
      enclosing: DrillClause[],
   ) => {
      const own: DrillClause[] = [];
      const grouped: Array<{
         field: GroupedField;
         cell?: Malloy.Cell;
      }> = [];
      fields.forEach((field, at) => {
         if (!isGrouped(field)) return;
         const cell = row?.[at];
         grouped.push({ field, cell });
         own.push({
            filter: {
               kind: "literal_equality",
               expression: { kind: "field_reference", name: field.name },
               value: cellLiteral(cell, field.type as Malloy.AtomicType),
            },
         });
      });
      const rowClauses = [...enclosing, ...own];
      for (const { field, cell } of grouped) {
         const tags = fieldTags(field, parse);
         const value = cellValue(cell);
         out.push({
            name: field.name,
            label: tags.label ?? readableName(field.name),
            ...(tags.drill ? { modelDrill: tags.drill } : {}),
            rowClauses,
            value,
            clickable: value !== null && value !== undefined,
         });
      }
      fields.forEach((field, at) => {
         if (!isNest(field)) return;
         const nested = row?.[at];
         const first =
            nested?.kind === "array_cell" &&
            nested.array_value[0]?.kind === "record_cell"
               ? nested.array_value[0].record_value
               : undefined;
         walk(field.schema.fields, first, rowClauses);
      });
   };
   const data = result.data;
   const firstRow =
      data?.kind === "array_cell" && data.array_value[0]?.kind === "record_cell"
         ? data.array_value[0].record_value
         : undefined;
   walk(result.schema.fields, firstRow, []);
   return out;
}

/**
 * The click menu a value of `dimension` opens on this tile, as its entries
 * read: the same labels `useDrill` writes (`drillDestinationLabel`,
 * `showRowsLabel`), in its order, `to=self` first. `drill` is what the tile
 * will say for the dimension: the entry's setting, else the model's tag.
 */
export function clickMenuPreview(
   dimension: ClickDimension,
   drill: EntryDrill | undefined,
   options: {
      selfLabel: string;
      /** Whether the tile offers rows: false for a shape that cannot name them. */
      rows: boolean;
      timezone?: string;
   },
): string[] {
   if (!dimension.clickable) return [];
   const value = drillValueLabel(dimension.value);
   const destinations = Array.from(new Set(drill?.to ?? [])).sort(
      (a, b) => Number(b === DRILL_SELF) - Number(a === DRILL_SELF),
   );
   const items = destinations.map((destination) =>
      drillDestinationLabel(destination, value, options.selfLabel),
   );
   if (options.rows) {
      const labels = drillClauseLabels(dimension.rowClauses, options.timezone);
      items.push(
         showRowsLabel(
            labels.length > 0
               ? labels.map((label) => label.label).join(", ")
               : value,
         ),
      );
   }
   return items;
}
