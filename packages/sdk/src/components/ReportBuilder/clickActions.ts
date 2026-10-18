// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type * as Malloy from "@malloydata/malloy-interfaces";
import { drillClauseLabels, type DrillClause } from "../drill/drillClauses";
import {
   columnCounts,
   isNestColumn,
   isTotalColumn,
   type ResultField,
} from "../RenderedResult/chartShapeProblem";
import type { CatalogView } from "./catalog";
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
   /** Its value type (`string`, `number`, `date`, `timestamp`, `boolean`), which decides the filters it can set. */
   type?: string;
}

/** One of this report's filters, as the dialog offers it for "Filter this report by …". */
export interface ReportFilter {
   /** The given's name: what a drill's `given=` names. */
   name: string;
   label?: string;
   /** The declared type, as written: `filter<string>`, `string`, `date`, … Absent when the host did not say. */
   type?: string;
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
         const kind = (field.type as Malloy.AtomicType).kind;
         out.push({
            name: field.name,
            label: tags.label ?? readableName(field.name),
            ...(kind.endsWith("_type")
               ? { type: kind.slice(0, -"_type".length) }
               : {}),
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

/** A report a click can open: its path without `.malloy`, its title, and the filters it shows. */
export interface LinkTarget {
   path: string;
   title: string;
   /** Absent when the host did not say: such a report is offered, its filter unchecked. */
   givenNames?: readonly string[];
}

/** What the dialog's two checkboxes say for one dimension. */
export interface ClickChoice {
   /** "Filter this report by …" is ticked. */
   self: boolean;
   /** Which of this report's filters it sets, when the author picked one or the tile already names one. */
   selfGiven?: string;
   /** "Link to another report:" is ticked, and to which. */
   link?: string;
   /** Destinations the dialog does not show (a second linked report), kept as written. */
   extra: string[];
}

const fold = (name: string) => name.toLowerCase();

/**
 * The filter a click on `dimension` sets, among `names`: the one the tile's
 * entry names (`given`), else its model tag's, else one named like the
 * dimension. Matched without case, as the reader matches a drill's given.
 */
export function filterFor(
   dimension: ClickDimension,
   names: readonly string[],
   given?: string,
): string | undefined {
   const wanted = [given, dimension.modelDrill?.given, dimension.name].filter(
      (name): name is string => name !== undefined,
   );
   for (const want of wanted) {
      const hit = names.find((name) => fold(name) === fold(want));
      if (hit) return hit;
   }
   return undefined;
}

/** `filter<string>` and `string` are both `string`; a `date` value also sets a `timestamp` filter and back. */
function valueType(declared: string): string {
   const inner = /^filter\s*<\s*([a-z]+)\s*>$/i.exec(declared.trim());
   return (inner ? inner[1] : declared.trim()).toLowerCase();
}

/** Whether a value of `dimension` can set `filter`: same type, or a type either side doesn't say. */
export function filterAccepts(
   filter: ReportFilter,
   dimension: ClickDimension,
): boolean {
   if (filter.type === undefined || dimension.type === undefined) return true;
   const want = valueType(filter.type);
   const have = dimension.type;
   const time = (t: string) => t === "date" || t === "timestamp";
   return want === have || (time(want) && time(have));
}

/**
 * The report's filters a click on `dimension` can set, the one it would set
 * by name first: what the dialog's filter picker lists.
 */
export function filtersFor(
   dimension: ClickDimension,
   filters: readonly ReportFilter[],
   given?: string,
): ReportFilter[] {
   const fitting = filters.filter((filter) => filterAccepts(filter, dimension));
   const first = filterFor(
      dimension,
      fitting.map((filter) => filter.name),
      given,
   );
   return first === undefined
      ? fitting
      : [
           ...fitting.filter((filter) => filter.name === first),
           ...fitting.filter((filter) => filter.name !== first),
        ];
}

/** The other reports a click on `dimension` can open with its value set: those with a filter it sets. */
export function linkTargetsFor(
   dimension: ClickDimension,
   reports: readonly LinkTarget[],
): LinkTarget[] {
   return reports.filter(
      (report) =>
         report.givenNames === undefined ||
         filterFor(dimension, report.givenNames) !== undefined,
   );
}

/** What the tile does now for `dimension`: its entry's drill, else the model's tag. */
export function currentChoice(
   dimension: ClickDimension,
   entry: EntryDrill | undefined,
): ClickChoice {
   const drill = entry ?? dimension.modelDrill;
   const to = drill?.to ?? [];
   const reports = to.filter((destination) => destination !== DRILL_SELF);
   return {
      self: to.includes(DRILL_SELF),
      ...(to.includes(DRILL_SELF) && drill?.given !== undefined
         ? { selfGiven: drill.given }
         : {}),
      ...(reports[0] !== undefined ? { link: reports[0] } : {}),
      extra: reports.slice(1),
   };
}

/**
 * The filter "Filter this report by …" sets for `dimension`: the one the
 * author picked, else the one the tile already names or the dimension is
 * named like. Undefined when neither, even if the report has filters of its
 * type: guessing would write a value into an unrelated filter, so the author
 * picks one.
 */
export function selfFilter(
   dimension: ClickDimension,
   choice: ClickChoice,
   filters: readonly ReportFilter[],
): string | undefined {
   const offered = filtersFor(dimension, filters, choice.selfGiven);
   const picked = offered.find(
      (filter) => fold(filter.name) === fold(choice.selfGiven ?? ""),
   );
   if (picked) return picked.name;
   return filterFor(
      dimension,
      offered.map((filter) => filter.name),
   );
}

/**
 * The entry drill that makes `choice` true for `dimension`, or undefined when
 * the model's tag already says it (nothing to write). Turning every choice off
 * over a model tag writes `to=[]`. The given is the filter it sets: this
 * report's when it filters itself, else the linked report's.
 */
export function entryFor(
   dimension: ClickDimension,
   choice: ClickChoice,
   filters: readonly ReportFilter[],
   reports: readonly LinkTarget[],
): EntryDrill | undefined {
   // Filtering this report needs a filter to write the value into; until one
   // is chosen, the choice writes nothing.
   const sets = choice.self
      ? (selfFilter(dimension, choice, filters) ?? choice.selfGiven)
      : undefined;
   const self = choice.self && sets !== undefined;
   const to = [
      ...(self ? [DRILL_SELF] : []),
      ...(choice.link !== undefined ? [choice.link] : []),
      ...choice.extra,
   ];
   const linked = reports.find((report) => report.path === choice.link);
   const given =
      sets ??
      (linked?.givenNames
         ? filterFor(dimension, linked.givenNames)
         : dimension.modelDrill?.given);
   // A drill turned off sets nothing, so it names no filter.
   const next: EntryDrill =
      to.length > 0 && given !== undefined && given !== dimension.name
         ? { to, given }
         : { to };
   const model = dimension.modelDrill;
   // Order does not matter to the reader, which puts `self` first anyway.
   const key = (drill: EntryDrill) =>
      `${[...new Set(drill.to)].sort().join("|")}#${fold(drill.given ?? dimension.name)}`;
   const same = (a: EntryDrill, b: EntryDrill) => key(a) === key(b);
   if (model ? same(next, model) : to.length === 0) return undefined;
   return next;
}

/**
 * A tile's shape as the chart picker reads a model view's, from the result the
 * canvas drew for it: what an inline-stage query, which no model lists, is
 * judged by. Undefined until the tile has run.
 */
export function viewOfResult(
   result: string | undefined,
): Pick<CatalogView, "shape" | "aggregateOnly"> | undefined {
   if (!result) return undefined;
   try {
      const fields = (JSON.parse(result) as Malloy.Result).schema
         .fields as unknown as ResultField[];
      if (fields.length === 0) return undefined;
      const { groupBys, totals } = columnCounts(fields);
      return {
         shape: { groupBys, totals },
         ...(totals === fields.length ? { aggregateOnly: true } : {}),
      };
   } catch {
      return undefined;
   }
}
