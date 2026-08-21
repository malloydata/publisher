// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Resolving a renderer click into a `# drill` intent.
 *
 * `# drill { to=[…] given=… }` is written on a source *dimension*, and Malloy
 * carries a dimension's annotations through to the query result's output field.
 * So everything a click needs is already on the clicked field, and drill needs
 * no endpoint of its own: the same resolution works for a dashboard tile and a
 * notebook cell, which is the "one drill implementation" the design asks for.
 *
 * That propagation is the premise the whole design rests on, and NOTHING IN THIS
 * PR pins it: the specs here drive a hand-built tag stub, deliberately, because
 * what they test is the grammar. It is pinned against the real compiler by
 * `packages/server/src/service/drill_probe.spec.ts`, which arrives with the
 * dashboard slice of this split rather than here. If a Malloy upgrade stopped
 * propagating dimension annotations, drill would go quiet everywhere at once and
 * this slice's tests would all still pass.
 *
 * This module answers only *what* was asked for, which dashboards, seeding
 * which given to which value. Navigating is the host's job.
 */

import {
   encodeFilterList,
   filterInnerType,
   isFilterType,
} from "../given/filterValue";

/**
 * The parsed-tag surface this module reads, structurally typed.
 *
 * The renderer hands over an already-parsed `Tag` from `@malloydata/malloy-tag`,
 * so naming the shape rather than importing the class keeps that package a
 * render-time detail: SDK consumers can bundle without it, the same allowance
 * `RenderedResult` makes for its theme annotations.
 */
export interface DrillTagReader {
   tag(...at: string[]): DrillTagReader | undefined;
   text(...at: string[]): string | undefined;
   textArray(...at: string[]): string[] | undefined;
}

/** The clicked field, as much of it as drill reads. */
export interface DrillField {
   name: string;
   tag?: DrillTagReader;
   /**
    * Whether the field was grouped rather than aggregated. Only dimensions
    * drill: a measure's cell has no dimension value to seed a filter with even
    * when the tag reaches it, so filtering by one would filter by something
    * other than what was clicked. Optional because the renderer's click payload
    * and its field metadata are the two callers and only one is guaranteed to
    * carry the method; absent, the field is taken at its word.
    */
   wasDimension?: () => boolean;
}

/**
 * A click from `@malloydata/render`'s `onClick`, narrowed to what drill needs.
 * Mirrors its `MalloyClickEventPayload` without importing it, since the renderer
 * is a lazily-imported dependency here.
 */
export interface DrillClickPayload {
   field?: DrillField;
   value?: unknown;
   isHeader?: boolean;
   /**
    * The originating click, which the host needs for two things a resolved
    * intent cannot express, where to anchor a menu of destinations, and whether
    * a modifier was held (cmd/ctrl to open in a new tab).
    */
   event?: MouseEvent;
}

/** The literal `to=self`: filter the current view instead of leaving it. */
export const DRILL_SELF = "self";

/**
 * The `to=` destinations a field's `# drill` declares, empty when it declares
 * none or the field cannot drill.
 *
 * Read separately from {@link resolveDrill} because two callers need it: the
 * click, which also needs the clicked value, and the *affordance*, which runs
 * over the result's field metadata before any click to decide which columns read
 * as clickable. Sharing this is what keeps the two honest: a cell that looks
 * clickable is one whose click resolves, because both answers come from here.
 */
export function drillDestinations(field: DrillField | undefined): string[] {
   if (!field) return [];
   // `wasDimension` is only consulted when the field answers; see DrillField.
   if (field.wasDimension && !field.wasDimension()) return [];
   const drill = field.tag?.tag("drill");
   if (!drill) return [];
   // Malloyyo accepts a lone destination unbracketed (`to=overview`) as well as
   // the list form. The server grows the same leniency in `readDrillTag`
   // (`service/dashboard.ts`) with the dashboard slice of this split; it is not
   // in the tree yet, so keep the two in step when it lands.
   return (
      drill.textArray("to") ?? [drill.text("to")].filter(isNonEmpty)
   ).filter(isNonEmpty);
}

/**
 * A destination slug as a menu label: `category_detail` → `Category detail`.
 *
 * Ported from Malloyyo so the same drill reads the same way in both, rather than
 * naming the file on one and a sentence on the other.
 */
export function humanizeSlug(slug: string): string {
   const words = slug.replace(/[-_]+/g, " ").trim();
   return words.charAt(0).toUpperCase() + words.slice(1);
}

/**
 * The given a `# drill` on this field would set: `given=` when the tag names
 * one, otherwise the dimension name exactly as the model spells it.
 *
 * Shared with the affordance for the same reason `drillDestinations` is: a cell
 * reads as clickable exactly when clicking it would do something, and deciding
 * "would it" needs the given's name before any click happens.
 */
export function drillGivenName(field: DrillField): string {
   // The field name verbatim, NOT upper-cased. Given names are stored as the
   // model spells them, so upper-casing meant a `to=self` on a lowercase-named
   // dimension resolved to a given that does not exist and the click was
   // dropped in silence. An upper-case convention is common enough that it hid
   // this: `given: REGION` worked, `given: region` did not.
   //
   // How the name is then MATCHED is the caller's business, and Notebook folds
   // case before looking it up, so a tag naming `region` still finds a declared
   // `REGION`. That is a courtesy at the lookup, not a licence to re-spell the
   // name here: the value has to be set under the name the model declares.
   return field.tag?.tag("drill")?.text("given") ?? field.name;
}

/** Where a `# drill` click wants to go, for the host to route however it routes. */
export interface DrillNavigation {
   /** The destination dashboard's slug. */
   dashboard: string;
   /**
    * Control values to open it with: the clicked value seeded into the drill's
    * given. Only the seeded given: the source view's other filters are not
    * carried over, since the destination need not bind them.
    */
   givens: Record<string, string>;
}

/** What a click asked for, once the tag on the clicked field is read. */
export interface DrillIntent {
   /** Destinations in declared order: dashboard slugs and/or {@link DRILL_SELF}. */
   to: string[];
   /** The given to seed: the dimension name as spelled, unless `given=` says otherwise. */
   given: string;
   /**
    * The clicked value as Malloy filter syntax, ready to send to a `filter<…>`
    * given. This is the right encoding for a cross-dashboard drill, whose
    * destination declares its own givens and is conventionally `filter<…>`.
    */
   value: string;
   /**
    * The same value before any encoding.
    *
    * Carried because the filter encoding above is only correct for a
    * `filter<…>` target, and a click cannot know what it is aiming at: the
    * destination may be another dashboard entirely. A host that *does* know,
    * `to=self`, where the target given is one of its own: re-encodes from this
    * with {@link encodeDrillValue}. Without it, drilling into a plain `string`
    * given seeded it with backslash-escaped filter syntax and matched nothing.
    */
   rawValue: unknown;
   /** The clicked value as it read on screen, for labelling the destination menu. */
   label: string;
}

/**
 * The clicked cell's value as filter syntax, or undefined when it cannot be
 * expressed as one.
 *
 * Encoded from the value's own type rather than the given's, which a click does
 * not know (a notebook cell has no control row to consult). Refusing the cases
 * it cannot encode is deliberate: a drill that filtered by something other than
 * the cell the user clicked would be worse than one that does nothing.
 */
export function drillValueToFilter(value: unknown): string | undefined {
   if (typeof value === "string") {
      // Refused when it ENCODES to nothing, not when it IS the empty string.
      // `encodeFilterList` drops any value with no non-whitespace character,
      // and the empty filter matches every row, so a drill producing one would
      // silently widen instead of narrowing. Testing the input against `""`
      // duplicated the encoder's rule and then fell out of step with it the
      // moment that rule grew to cover whitespace: clicking a blank-looking
      // cell (a space, a tab, an NBSP, all ordinary in pasted data) handed back
      // the whole dataset dressed up as a drilled view. Asking the encoder is
      // the one form of this that cannot drift again.
      const encoded = encodeFilterList([value]);
      return encoded === "" ? undefined : encoded;
   }
   if (typeof value === "boolean") return String(value);
   if (typeof value === "number") {
      // A bare number reads as equality. NaN/Infinity have no filter spelling.
      return Number.isFinite(value) ? String(value) : undefined;
   }
   if (value instanceof Date) {
      // Malloy's date filter literal. Deliberately day-granularity: seeding a
      // timestamp to the millisecond would filter to a single row nobody asked
      // for, and for a `date` column the clicked cell was a truncated date in
      // the first place.
      //
      // KNOWN LIMITATION for a TIMESTAMP column feeding a `filter<…>` given.
      // This truncates on the UTC day, and the renderer prints a timestamp in
      // the query timezone, so the two disagree whenever that zone crosses
      // midnight away from UTC: an instant of `2024-03-06T04:00:00Z` shows as
      // `2024-03-05 20:00` under US-Pacific and seeds `2024-03-06`, the day
      // AFTER the one on screen. Plain `date`/`timestamp` givens route around
      // this entirely (`encodeDrillValue` hands them the Date and lets
      // `givensToRequest` spell it per type); only the filter-typed targets
      // land here, because a filter's value must be text.
      //
      // Not fixed rather than fixed wrongly: the correct day is the one the
      // CELL displayed, which needs the query timezone, and that is not on the
      // click payload or anywhere else this function can see. Guessing the
      // browser's zone would be a second wrong answer that happens to be right
      // more often, which is harder to diagnose than a consistent one.
      const iso = value.toISOString();
      return iso.slice(0, iso.indexOf("T"));
   }
   // null included: "the rows with no value" is expressible in Malloy, but
   // clicking an empty cell is far more likely a misclick than that request.
   return undefined;
}

/** How the clicked value read on screen, for a menu label. */
function drillValueLabel(value: unknown): string {
   if (value instanceof Date) return drillValueToFilter(value) ?? "";
   return String(value);
}

/**
 * Read the `# drill` intent off a renderer click, or undefined when the click
 * was not on a drillable value.
 *
 * Silent on anything untagged, which is most clicks: this runs on every click
 * the renderer reports, so "not a drill" is the ordinary answer, not a fault.
 */
export function resolveDrill(
   payload: DrillClickPayload | undefined,
): DrillIntent | undefined {
   // A column header names the field rather than carrying a value, so there is
   // nothing to seed even though the tag is on the field the header belongs to.
   if (!payload || payload.isHeader) return undefined;

   const field = payload.field;
   if (!field) return undefined;

   const to = drillDestinations(field);
   if (to.length === 0) return undefined;

   const value = drillValueToFilter(payload.value);
   if (value === undefined) return undefined;

   return {
      to,
      given: drillGivenName(field),
      value,
      rawValue: payload.value,
      label: drillValueLabel(payload.value),
   };
}

function isNonEmpty(value: string | undefined): value is string {
   return typeof value === "string" && value !== "";
}

/**
 * A clicked value encoded for a given whose declared type is known.
 *
 * `filter<…>` takes filter syntax, so the value is escaped into a one-value
 * filter list. Every other type takes the value itself: a plain `string` given
 * seeded with `Ben\ &\ Jerry` would match nothing, because the escaping is
 * meaningful only to the filter parser.
 *
 * Undefined when the value has no spelling for that type, which the caller
 * should treat the way it treats an unresolvable drill: do nothing.
 */
export function encodeDrillValue(
   rawValue: unknown,
   targetType: string | undefined,
): string | number | boolean | Date | undefined {
   if (targetType !== undefined && !isFilterType(targetType)) {
      // Returned in the given's OWN type, not stringified. `givensToRequest`
      // converts only `Date`, so a string sent for a `number` or `boolean` given
      // travels as a string and Malloy refuses the query.
      if (targetType === "number") {
         // Only a number, or a string that spells one. `Number()` accepts far
         // more than that and every extra it accepts is a wrong filter rather
         // than a refused one: a clicked Date became its epoch milliseconds
         // (1709646300000 for 2024-03-05) and a null cell became 0, so the
         // notebook re-ran filtered to a value nobody clicked and returned
         // nothing, with no error to explain it.
         if (typeof rawValue === "number") {
            return Number.isFinite(rawValue) ? rawValue : undefined;
         }
         if (typeof rawValue !== "string" || rawValue.trim() === "") {
            return undefined;
         }
         const parsed = Number(rawValue);
         return Number.isFinite(parsed) ? parsed : undefined;
      }
      if (
         targetType === "date" ||
         targetType === "timestamp" ||
         targetType === "timestamptz"
      ) {
         // The Date itself, so `givensToRequest` can spell it per type. Falling
         // through to `drillValueToFilter` truncated it to `YYYY-MM-DD` first,
         // which is the right spelling for a `date` given and drops the time of
         // day for a `timestamp` one: the drill then filtered to midnight
         // rather than to the instant in the cell.
         return rawValue instanceof Date ? rawValue : undefined;
      }
      if (targetType === "boolean") {
         if (typeof rawValue === "boolean") return rawValue;
         if (rawValue === "true") return true;
         if (rawValue === "false") return false;
         return undefined;
      }
      // A blank cell is refused here too, for the same reason as in
      // `drillValueToFilter`: on a plain given it is a real but almost certainly
      // unintended value, and clicking a blank cell is far more likely a
      // misclick. Whitespace counts as blank, which is what makes that reason
      // and this test agree: refusing only `""` left a space, a tab or an NBSP
      // drilling to a value that matches essentially nothing, so the reader got
      // an empty notebook rather than the click being declined.
      if (typeof rawValue === "string")
         return rawValue.trim() === "" ? undefined : rawValue;
      if (typeof rawValue === "number") {
         return Number.isFinite(rawValue) ? String(rawValue) : undefined;
      }
      if (typeof rawValue === "boolean") return String(rawValue);
   }
   // A `filter<string>` target takes the STRING grammar, so a bare number is
   // not safe there: `-5` parses as `{operator:"=", values:["5"], not:true}`,
   // a NEGATION, so drilling a negative cell filtered to every row EXCEPT the
   // one clicked, silently. `drillValueToFilter`'s number branch is right for a
   // `filter<number>`, whose grammar reads `-5` as the value. The spec already
   // knew the two grammars disagree here and pinned only the number one.
   if (
      filterInnerType(targetType) === "string" &&
      typeof rawValue === "number"
   ) {
      return Number.isFinite(rawValue)
         ? encodeFilterList([String(rawValue)])
         : undefined;
   }
   // A `filter<number>` takes the NUMBER grammar, so a string belongs to it only
   // if it spells a number. Falling through emitted STRING filter syntax for it:
   // a click on a text cell seeded `filter<number>` with `West`, a boolean with
   // `true`, and a Date with a `YYYY-MM-DD` day, each of which that grammar
   // refuses, so the server failed EVERY cell and the reader got an error
   // notebook rather than a declined click. The non-filter `number` branch above
   // already refuses exactly these; this is the same rule for the filter arm,
   // which is where the two copies had drifted.
   if (filterInnerType(targetType) === "number") {
      if (typeof rawValue === "number") {
         return Number.isFinite(rawValue) ? String(rawValue) : undefined;
      }
      if (typeof rawValue !== "string" || rawValue.trim() === "") {
         return undefined;
      }
      return Number.isFinite(Number(rawValue)) ? rawValue.trim() : undefined;
   }
   return drillValueToFilter(rawValue);
}
