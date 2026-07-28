/**
 * Resolving a renderer click into a `# drill` intent.
 *
 * `# drill { to=[…] given=… }` is written on a source *dimension*, and Malloy
 * carries a dimension's annotations through to the query result's output field
 * (pinned by `packages/server/src/service/drill_probe.spec.ts`). So everything a
 * click needs is already on the clicked field, and drill needs no endpoint of
 * its own: the same resolution works for a dashboard tile and a notebook cell,
 * which is the "one drill implementation" the design asks for.
 *
 * This module answers only *what* was asked for — which dashboards, seeding
 * which given to which value. Navigating is the host's job.
 */

import { encodeFilterList } from "../given/filterValue";

/**
 * The parsed-tag surface this module reads, structurally typed.
 *
 * The renderer hands over an already-parsed `Tag` from `@malloydata/malloy-tag`,
 * so naming the shape rather than importing the class keeps that package a
 * render-time detail — SDK consumers can bundle without it, the same allowance
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
    * intent cannot express: where to anchor a menu of destinations, and whether
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
 * as clickable. Sharing this is what keeps the two honest — a cell that looks
 * clickable is one whose click resolves, because both answers come from here.
 */
export function drillDestinations(field: DrillField | undefined): string[] {
   if (!field) return [];
   // `wasDimension` is only consulted when the field answers; see DrillField.
   if (field.wasDimension && !field.wasDimension()) return [];
   const drill = field.tag?.tag("drill");
   if (!drill) return [];
   // Malloyyo accepts a lone destination unbracketed (`to=overview`) as well as
   // the list form, matching the server's `readDrillTag`.
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

/** Where a `# drill` click wants to go, for the host to route however it routes. */
export interface DrillNavigation {
   /** The destination dashboard's slug. */
   dashboard: string;
   /**
    * Control values to open it with — the clicked value seeded into the drill's
    * given. Only the seeded given: the source view's other filters are not
    * carried over, since the destination need not bind them.
    */
   givens: Record<string, string>;
}

/** What a click asked for, once the tag on the clicked field is read. */
export interface DrillIntent {
   /** Destinations in declared order: dashboard slugs and/or {@link DRILL_SELF}. */
   to: string[];
   /** The given to seed — the dimension name upper-cased, unless `given=` says otherwise. */
   given: string;
   /** The clicked value as Malloy filter syntax, ready to send as a given. */
   value: string;
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
   if (typeof value === "string") return encodeFilterList([value]);
   if (typeof value === "boolean") return String(value);
   if (typeof value === "number") {
      // A bare number reads as equality. NaN/Infinity have no filter spelling.
      return Number.isFinite(value) ? String(value) : undefined;
   }
   if (value instanceof Date) {
      // Malloy's date filter literal. Deliberately day-granularity: seeding a
      // timestamp to the millisecond would filter to a single row nobody asked
      // for, and the clicked cell was a truncated date in the first place.
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
 * Silent on anything untagged, which is most clicks — this runs on every click
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
      given: field.tag?.tag("drill")?.text("given") ?? field.name.toUpperCase(),
      value,
      label: drillValueLabel(payload.value),
   };
}

function isNonEmpty(value: string | undefined): value is string {
   return typeof value === "string" && value !== "";
}
