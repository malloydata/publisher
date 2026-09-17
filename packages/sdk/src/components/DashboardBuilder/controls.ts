// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { DashboardDocument, DashboardTile, LocalGiven } from "./document";

/**
 * The builder's view of the dashboard's filter controls, and the pure edits it
 * makes to them. Kept out of the component so every rule here has a test that
 * needs no DOM.
 *
 * A control is a GIVEN some tile binds. It comes from one of two places, and
 * the difference decides what the builder may do with it:
 *
 * - `dashboard` — declared in this file ({@link LocalGiven}). The builder owns
 *   it outright: add, retag, remove, and bind on any tile.
 * - `model` — declared in the model and reaching this file through an import.
 *   The builder may bind it on tiles, because a binding is a refinement in this
 *   file, but it cannot change or remove the declaration, because the builder
 *   never edits imports or model files.
 *
 * Declaring in the dashboard is the convention; see {@link LocalGiven}.
 */

export type ControlOrigin = "dashboard" | "model";

/** A given the builder can bind a tile to, from either origin. */
export interface BuilderGiven {
   /** The given's name, as written after `$` in a `where:`. */
   name: string;
   label?: string;
   /** The declared type, which decides how a binding compares; see {@link defaultOperator}. */
   type?: string;
   /**
    * The field to compare against, used as the default when this given is
    * bound to a tile. The dimension its control suggests over is the right
    * guess — a `CATEGORY` control that suggests over `category` is almost
    * always filtering `category` — and it is only a default: the binding is
    * editable afterwards, because the tile's source may spell it differently.
    */
   field?: string;
}

export interface BuilderControl extends BuilderGiven {
   origin: ControlOrigin;
   /** The declaration, for a control this dashboard owns. */
   local?: LocalGiven;
   /** How many tiles bind it. Zero means declared but not yet a control. */
   boundTiles: number;
}

/**
 * Every control the builder can offer: the dashboard's own first, in file
 * order, then the model's that the caller knows about. A model given that
 * shares a name with a local one is the same control as far as any `where:` is
 * concerned, and the local declaration is the one that binds — so it is not
 * listed twice.
 */
export function controlsOf(
   document: DashboardDocument,
   modelGivens: readonly BuilderGiven[] = [],
): BuilderControl[] {
   const bound = new Map<string, number>();
   for (const tile of document.tiles)
      for (const filter of tile.filters ?? [])
         bound.set(filter.given, (bound.get(filter.given) ?? 0) + 1);

   const out: BuilderControl[] = [];
   const seen = new Set<string>();
   for (const local of document.localGivens ?? []) {
      seen.add(local.name);
      out.push({
         name: local.name,
         ...(local.label === undefined ? {} : { label: local.label }),
         type: local.type,
         ...(local.suggest ? { field: local.suggest.dimension } : {}),
         origin: "dashboard",
         local,
         boundTiles: bound.get(local.name) ?? 0,
      });
   }
   for (const given of modelGivens) {
      if (seen.has(given.name)) continue;
      seen.add(given.name);
      out.push({
         ...given,
         origin: "model",
         boundTiles: bound.get(given.name) ?? 0,
      });
   }
   // A binding to a given nobody declared is still a control the file has —
   // it is how a dashboard over a bare `import '../givens.malloy'` looks when
   // the caller passed no model givens. Shown so it can at least be unbound.
   for (const [name, count] of bound) {
      if (seen.has(name)) continue;
      out.push({ name, origin: "model", boundTiles: count });
   }
   return out;
}

/**
 * How a binding to a given of this type compares.
 *
 * A `filter<…>` given is a filter EXPRESSION and binds with `~`, which is the
 * common case and is left implicit on the binding. A plain-typed given is a
 * VALUE: a `date` is "since", a `number` is "at least", a `string` is "equals".
 * Measured: `created_at ~ $SINCE` does not compile against a `date` given.
 */
export function defaultOperator(type: string | undefined): string | undefined {
   if (type === undefined || type.startsWith("filter<")) return undefined;
   if (type === "string" || type === "boolean") return "=";
   return ">=";
}

/**
 * The scalar a given compares: `filter<string>` and `string` both compare
 * strings, one as a filter expression and one as a value.
 */
function givenScalar(type: string | undefined): string | undefined {
   if (!type) return undefined;
   return /^filter<(.+)>$/.exec(type)?.[1] ?? type;
}

/**
 * Whether a given of `givenType` can compare a field of `fieldType` (the
 * catalog's spelling: `string_type`, `number_type`, `date_type`, …).
 *
 * A `filter<string>` over a number field compiles nowhere and a `date` over a
 * string field compares nothing, so the picker offers only the fields a control
 * can take, and a binding to one it cannot is marked. Dates and timestamps
 * compare with each other. Unknown on EITHER side is accepted: the catalog may
 * carry no type, and a given of a type this does not know is not refused on
 * that account.
 */
export function acceptsField(
   givenType: string | undefined,
   fieldType: string | undefined,
): boolean {
   const scalar = givenScalar(givenType);
   const field = fieldType?.replace(/_type$/, "");
   if (!scalar || !field) return true;
   const dateLike = new Set(["date", "timestamp"]);
   if (dateLike.has(scalar)) return dateLike.has(field);
   return scalar === field;
}

/** `number_type` -> "a number", as a message would say it. */
export function typeLabel(type: string | undefined): string {
   switch (type?.replace(/_type$/, "")) {
      case "string":
         return "text";
      case "number":
         return "a number";
      case "date":
         return "a date";
      case "timestamp":
         return "a timestamp";
      case "boolean":
         return "true or false";
      default:
         return type ?? "an unknown type";
   }
}

/**
 * The control a field of this type naturally gets: a number range for a
 * number, a date picker for a date, a pick-list for anything else. What the
 * window switches a new control to when the field picked does not fit the kind
 * it started with.
 */
export function kindForFieldType(fieldType: string | undefined): ControlKind {
   switch (fieldType?.replace(/_type$/, "")) {
      case "number":
         return "number";
      case "date":
      case "timestamp":
         return "date";
      default:
         return "select";
   }
}

/** The comparisons a binding can use, with how a reader would say them. */
export const OPERATORS: ReadonlyArray<{ op: string; label: string }> = [
   { op: "~", label: "matches" },
   { op: "=", label: "equals" },
   { op: ">=", label: "at least" },
   { op: "<=", label: "at most" },
   { op: ">", label: "more than" },
   { op: "<", label: "less than" },
];

/**
 * A given name from a field: `products.category` -> `CATEGORY`, and kept
 * distinct from any name already taken.
 */
export function givenNameFor(field: string, taken: Iterable<string>): string {
   const words = (field.split(".").at(-1) ?? "")
      .split(/[^A-Za-z0-9]+/)
      .filter(Boolean);
   const base = words.join("_").toUpperCase() || "FILTER";
   const stem = /^[A-Z_]/.test(base) ? base : `F_${base}`;
   const used = new Set(taken);
   if (!used.has(stem)) return stem;
   for (let n = 2; ; n++) if (!used.has(`${stem}_${n}`)) return `${stem}_${n}`;
}

/** What kind of control a new filter is, in a reader's terms. */
export type ControlKind = "select" | "multiselect" | "text" | "number" | "date";

export const CONTROL_KINDS: ReadonlyArray<{
   kind: ControlKind;
   label: string;
   hint: string;
}> = [
   {
      kind: "select",
      label: "Pick one",
      hint: "A dropdown of the field's values",
   },
   {
      kind: "multiselect",
      label: "Pick several",
      hint: "The same, taking more than one",
   },
   { kind: "text", label: "Text", hint: "A box taking Malloy filter syntax" },
   {
      kind: "number",
      label: "Number range",
      hint: "A slider over the field's values",
   },
   {
      kind: "date",
      label: "Since a date",
      hint: "A date picker; tiles keep rows on or after it",
   },
];

/**
 * A new control's declaration, from what the dialog asked.
 *
 * The picker's options come from the source the field lives on — the base of
 * the tile's own extension — which the file necessarily already imports, so a
 * `suggest` here always resolves and the builder never has to add an import.
 */
export function newLocalGiven(spec: {
   name: string;
   label: string;
   kind: ControlKind;
   field: string;
   /** The source to suggest over, for `select` and `multiselect`. */
   source?: string;
   /** A date's default, `YYYY-MM-DD`; a number range's bounds. */
   dateDefault?: string;
   range?: { min: number; max: number };
}): LocalGiven {
   const label = spec.label.trim() || spec.name;
   const dimension = spec.field.split(".").at(-1) ?? spec.field;
   switch (spec.kind) {
      case "select":
      case "multiselect":
         return {
            name: spec.name,
            type: "filter<string>",
            default: "f''",
            label,
            control: spec.kind,
            ...(spec.source
               ? { suggest: { source: spec.source, dimension } }
               : {}),
         };
      case "text":
         return {
            name: spec.name,
            type: "filter<string>",
            default: "f''",
            label,
         };
      case "number":
         return {
            name: spec.name,
            type: "filter<number>",
            default: "f''",
            label,
            ...(spec.range
               ? { rangeMin: spec.range.min, rangeMax: spec.range.max }
               : {}),
         };
      case "date":
         return {
            name: spec.name,
            type: "date",
            default: `@${spec.dateDefault ?? new Date().toISOString().slice(0, 10)}`,
            label,
         };
   }
}

/** One tile's row in a control's mapping: whether it binds, and how. */
export interface MappingRow {
   include: boolean;
   field: string;
   /** Absent means `~`; see {@link defaultOperator}. */
   op?: string;
}

/**
 * Which tiles can take a binding, and why the others cannot.
 *
 * A binding is a `where:` the splice writer can locate and own: a `+ {
 * where: … }` refinement on a `reference` tile, or a depth-1 `where:`
 * statement in an `inline` tile's own first stage (see BINDING_CLAUSE and
 * viewBodyStage1 in the splice/read modules). Both forms are declarations this
 * file owns and the reader reads back. An `inherited` tile is declared on the
 * model's source, which the builder never writes, so it is the one kind this
 * excludes. A tile whose body shape the writer cannot locate a single first
 * stage in — a multi-stage `->` pipeline, a `{ … } + { … }` compound body —
 * still shows as bindable here; the splice writer is what refuses that write,
 * with a reason naming the shape, once a save is attempted.
 */
export const canBind = (tile: DashboardTile) =>
   tile.declaration.kind !== "inherited";

/** The mapping a control has NOW, one row per tile, for the dialog to open on. */
export function mappingOf(
   document: DashboardDocument,
   control: { name: string; field?: string; type?: string },
): MappingRow[] {
   return document.tiles.map((tile) => {
      const bound = (tile.filters ?? []).find((f) => f.given === control.name);
      return {
         // Ticked where the tile ALREADY carries this control, so the window
         // opens on what is true rather than on what would be added. That is
         // what lets one window add, change and remove: untick a row to unbind
         // it, untick them all and the control leaves every tile.
         include: bound !== undefined,
         field: bound?.field ?? control.field ?? control.name.toLowerCase(),
         ...(bound?.op !== undefined
            ? { op: bound.op }
            : defaultOperator(control.type) !== undefined
              ? { op: defaultOperator(control.type) as string }
              : {}),
      };
   });
}

/**
 * Apply a mapping: a DIFF against what each tile carries now — added where
 * newly ticked, dropped where unticked, rewritten where only the field or the
 * comparison changed. Mutates the draft; the caller makes it one history entry.
 */
export function applyMapping(
   draft: DashboardDocument,
   given: string,
   rows: readonly MappingRow[],
): void {
   rows.forEach((row, index) => {
      const tile = draft.tiles[index];
      if (!tile || !canBind(tile)) return;
      const rest = (tile.filters ?? []).filter((f) => f.given !== given);
      const next = row.include
         ? [
              ...rest,
              {
                 field: row.field.trim() || given.toLowerCase(),
                 given,
                 ...(row.op && row.op !== "~" ? { op: row.op } : {}),
              },
           ]
         : rest;
      if (next.length > 0) tile.filters = next;
      else delete tile.filters;
   });
}

/** Declare a control in this dashboard, or replace its declaration by name. */
export function declareControl(
   draft: DashboardDocument,
   given: LocalGiven,
): void {
   const rest = (draft.localGivens ?? []).filter((g) => g.name !== given.name);
   const at = (draft.localGivens ?? []).findIndex((g) => g.name === given.name);
   const next = [...rest];
   next.splice(at >= 0 ? at : next.length, 0, given);
   draft.localGivens = next;
}

/**
 * Take a control off the dashboard: every binding to it, and — when this file
 * declares it — the declaration too. A model given keeps its declaration, which
 * is not ours; it simply stops being a control here, because a control is a
 * given some tile binds.
 */
export function removeControl(draft: DashboardDocument, name: string): void {
   for (const tile of draft.tiles) {
      const kept = (tile.filters ?? []).filter((f) => f.given !== name);
      if (kept.length > 0) tile.filters = kept;
      else delete tile.filters;
   }
   const locals = (draft.localGivens ?? []).filter((g) => g.name !== name);
   if (locals.length > 0) draft.localGivens = locals;
   else delete draft.localGivens;
}
