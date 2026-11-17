// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The one reader of materialization's annotations: every `#@` line
 * (`persist`, a v1 `preaggregate`, a persisted view's), `#(materialize)`,
 * `#(preaggregate)` and a model's `##` and `##!` lines are parsed here and
 * nowhere else in materialization (the annotation ratchet,
 * `layer_rules.spec.ts`), and a declaration's place is decided by one rule,
 * {@link placementOf}.
 *
 * Internal to plan: a table plan carries what it resolved.
 *
 * The placement rule, for every route alike:
 *
 *  - `storage=` names the destination, written beside the route's key
 *    (`#@ persist storage="lake"`) or inside it (`#@ persist { storage="lake" }`);
 *    the form inside the key wins when both are written, as it is the more
 *    specific, and it wins even when it is unusable: a nested `storage=` that
 *    names nothing refuses the source, never falling through to a flat one;
 *  - `name=` is read the same two ways;
 *  - a `storage=` that names nothing refuses that source alone: one that is
 *    empty or only whitespace (`storage=""`), or not text at all (a list,
 *    `storage=["lake"]`, or a block, `storage { … }`). A destination left
 *    blank is a mistake to report, never a reason to build in the source's
 *    own warehouse or to fail the package. Any other text is a destination
 *    name, trimmed, and is looked up exactly: one no destination is
 *    configured under is not built, and the package warns that the source
 *    serves live;
 *  - no `storage=` builds colocated, in the source's own warehouse.
 *
 * Pure: reads only the annotations it is handed.
 */

import { Annotations } from "@malloydata/malloy";
import { routeOfNote } from "../../annotation_reader";
import { parseRouteTag, routeHasKey } from "../../motly";

/** A route's parsed tag. */
export type RouteTag = ReturnType<typeof parseRouteTag>["tag"];

/** A route's parse: its tag and the parser's log. */
type RouteParse = ReturnType<typeof parseRouteTag>;

/** Where a declaration asks its table to live. */
type Placement =
   | { tier: "database"; name?: string }
   | { tier: "storage"; destination: string; name?: string };

/** Why a declaration's placement is refused: its `storage=` names nothing. */
const EMPTY_STORAGE_REFUSAL = "empty_storage";

/**
 * What a source refused for {@link EMPTY_STORAGE_REFUSAL} is told, on every
 * route: the refusal and the package warning carry it.
 */
export const EMPTY_STORAGE_REASON =
   "its `storage=` names no destination: name the storage destination to " +
   "build it into, or remove the key to build it in its own warehouse";

/** {@link placementOf}'s answer: a place, or the refusal of that source. */
type PlacementRead =
   | { placement: Placement }
   | { refused: typeof EMPTY_STORAGE_REFUSAL };

/**
 * Parse `route` of compiled annotations (`"@"` for `#@`, `""` for `#` and
 * `##`), dropping a line that reads the environment as the parser's own
 * reader does. Throws as Malloy's parser does.
 */
export function readRoute(annotations: Annotations, route = ""): RouteParse {
   return parseRouteTag(annotations, route);
}

/** {@link readRoute}'s tag, or undefined when the route does not parse. */
export function readRouteIfAny(
   annotations: Annotations,
   route = "",
): RouteTag | undefined {
   try {
      return readRoute(annotations, route).tag;
   } catch {
      return undefined;
   }
}

/** {@link readRoute} over a list of compiled notes. */
export function readNotes(
   notes: readonly unknown[],
   route: string,
): RouteParse {
   return readRoute(
      new Annotations({ notes } as ConstructorParameters<
         typeof Annotations
      >[0]),
      route,
   );
}

/** {@link readNotes}'s tag, or undefined when the notes do not parse. */
export function readNotesIfAny(
   notes: readonly unknown[],
   route: string,
): RouteTag | undefined {
   try {
      return readNotes(notes, route).tag;
   } catch {
      return undefined;
   }
}

/**
 * Whether `route` declares `path`, decided as Malloy decides it: over every
 * line, a line that reads the environment included.
 */
export function declares(
   annotations: Annotations,
   route: string,
   ...path: string[]
): boolean {
   return routeHasKey(annotations, route, ...path);
}

/**
 * Whether a source's own notes (its statement's, not the ones it inherits)
 * declare `#@ persist`: a `#@` line that writes another key (a v1
 * `preaggregate`) does not.
 */
export function notesDeclarePersist(
   notes: readonly { text: string; at?: unknown }[],
): boolean {
   const own = notes.filter((note) => routeOfNote(note.text) === "@");
   if (own.length === 0) return false;
   return declares(
      new Annotations({ notes: own } as ConstructorParameters<
         typeof Annotations
      >[0]),
      "@",
      "persist",
   );
}

/**
 * Where a declaration read as `tag` asks its table to live (the rule in this
 * module's header). `key` is the route's own key when the route is shared
 * (`persist` or `preaggregate` on `#@`), whose `storage=` and `name=` may be
 * written inside it; a route of its own (`#(materialize)`,
 * `#(preaggregate)`) has none.
 */
export function placementOf(tag: RouteTag, key?: string): PlacementRead {
   const inside = (k: string) =>
      key === undefined ? undefined : tag.text(key, k);
   const nested = key !== undefined && tag.has(key, "storage");
   const name = inside("name") ?? tag.text("name");
   const named = name !== undefined ? { name } : {};
   if (!nested && !tag.has("storage")) {
      return { placement: { tier: "database", ...named } };
   }
   // The nested form wins whenever it is written, even when it is unusable:
   // a nested list or block never falls through to the flat value.
   const storage = nested ? inside("storage") : tag.text("storage");
   // Written, but empty, whitespace, or not text (a list, a block): names
   // nothing.
   const destination = storage?.trim() ?? "";
   if (destination === "") return { refused: EMPTY_STORAGE_REFUSAL };
   return { placement: { tier: "storage", destination, ...named } };
}

/** The destination {@link placementOf} names, or undefined for none or a refusal. */
export function destinationOf(tag: RouteTag, key?: string): string | undefined {
   const read = placementOf(tag, key);
   return "placement" in read && read.placement.tier === "storage"
      ? read.placement.destination
      : undefined;
}

/**
 * Every scalar `key=value` of a source's `#@` route, with `storage=` and
 * `name=` as {@link placementOf} reads them for `#@ persist` (written inside
 * the key or beside it); {} when the route does not parse. The control plane
 * reads `name` as the table's name; the rest flows through unread.
 */
export function persistAnnotationFields(
   annotations: Annotations,
): Record<string, string> {
   const out: Record<string, string> = {};
   let tag: RouteTag;
   try {
      tag = readRoute(annotations, "@").tag;
   } catch {
      return out;
   }
   for (const [key, value] of tag.entries()) {
      const text = value.text();
      if (text !== undefined) out[key] = text;
   }
   const read = placementOf(tag, "persist");
   if ("placement" in read) {
      const { placement } = read;
      if (placement.tier === "storage") out.storage = placement.destination;
      if (placement.name !== undefined) out.name = placement.name;
   } else {
      // Refused: no destination, not even a flat one a nested `storage=`
      // overrides.
      delete out.storage;
   }
   return out;
}
