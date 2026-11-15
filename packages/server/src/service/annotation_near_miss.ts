// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The near-miss scan (materialization v2, D5): annotations that read as an
 * attempt at a route Publisher owns, but that Malloy routes somewhere nothing
 * reads, so they silently do nothing. `#(clasification) "phi"`, `# (doc) …`,
 * `#@persist`, `# persist`, `#(persist)`, `#@ persits`.
 *
 * Every finding is a coded publish warning (`annotation-near-miss`), never a
 * refusal: a route that merely looks like one of ours may be another app's,
 * and the outcome rule warns on what it cannot be sure of. The gate spellings
 * the load refuses (`collectAuthorizeNearMisses`), including a misspelled
 * gate such as `#(authorise)` (`misspelledGateRoute`), are left to that
 * refusal, and the matching itself is shared with it (`isRouteAttempt`).
 *
 * A note whose prefix Malloy cannot route at all (`#percent`) is reported too
 * (`annotation-malformed-route`): Malloy warns at compile, but nothing carried
 * that warning to the package until now.
 *
 * Glued prefixes Publisher reads (`#(carries)["x"]`, `#"Orders`) are on their
 * route, so they are never reported as doing nothing.
 */

import type { DocumentLocation, ModelDef, StructDef } from "@malloydata/malloy";
import { isSourceDef } from "@malloydata/malloy";
import { getMaterializeEnabled } from "../config";
import {
   editDistance,
   foldRouteName,
   MATERIALIZE_V2_ROUTES,
   payloadOfNote,
   PUBLISHER_ROUTES,
   publisherRouteOf,
   routeOfNote,
   type AnnotationFinding,
} from "./annotation_reader";
import {
   isReservedRoute,
   notePositionKey,
   ownLevelNotes,
   ownModelNoteObjects,
   type AnnotationNote,
} from "./annotations";
import { collectAuthorizeNearMissesAllRoutes } from "./authorize";

/** The code of a finding for an annotation that was reaching for one of Publisher's routes. */
export const ANNOTATION_NEAR_MISS_CODE = "annotation-near-miss";

/** The code of a finding for an annotation whose prefix Malloy cannot route. */
export const ANNOTATION_MALFORMED_ROUTE_CODE = "annotation-malformed-route";

/**
 * The `code` of a `compile_model` error for a misspelled gate, which the load
 * refuses ("never enforced"): the editor sees the refusal before the save.
 */
export const ANNOTATION_GATE_NEAR_MISS_CODE = "annotation-gate-near-miss";

/** The bracketed app routes Publisher owns, as `#(name)`, v2's included. */
const OWNED_ROUTES: readonly string[] = Object.keys(PUBLISHER_ROUTES)
   .filter((route) => !isReservedRoute(route))
   .sort();

/** {@link OWNED_ROUTES} without the materialization v2 routes. */
const OWNED_V1_ROUTES: readonly string[] = OWNED_ROUTES.filter(
   (route) => !MATERIALIZE_V2_ROUTES.has(route),
);

/**
 * The owned routes in force: the v2 routes only with the materialize switch
 * on. Off, nothing reads `#(materialize)` or `#(preaggregate)`, so the scan
 * reads them as it did before they were registered: `#(preaggregate)` is
 * reaching for `#@ preaggregate`, which works there.
 */
function ownedRoutes(): readonly string[] {
   return getMaterializeEnabled() ? OWNED_ROUTES : OWNED_V1_ROUTES;
}

/** Whether Publisher reads `route` now (a v2 route only with the switch on). */
function routeIsRead(route: string): boolean {
   return (
      route in PUBLISHER_ROUTES &&
      (!MATERIALIZE_V2_ROUTES.has(route) || getMaterializeEnabled())
   );
}

/** The `#@` directives that make a source persist or roll up. */
const PERSIST_DIRECTIVES: readonly string[] = ["persist", "preaggregate"];

/** One annotation as the scan reads it. */
export interface ScannedNote {
   text: string;
   at?: DocumentLocation;
   /** The source, or `source.field`, the note is on, when known. */
   subject?: string;
}

/** What a near miss was reaching for. */
interface Intended {
   /** The spelling to write: `#(classification)`, `#@ persist`. */
   spelling: string;
   /** Its route, for the finding. */
   route: string;
}

/**
 * The owned route `name` is a misspelling of, or `undefined`. A folded match
 * (`#(Doc)`, `#(access-filter)`), or an edit within 1 for a short name and 2
 * for a longer one (`#(clasification)`, `#(docs)`). A name with a `/` or `.`
 * reads as a URL or version, another app's route, and never matches.
 */
function misspelledOwnedRoute(name: string): Intended | undefined {
   if (/[/.:]/.test(name)) return undefined;
   const folded = foldRouteName(name);
   if (folded === "") return undefined;
   let best: { route: string; distance: number } | undefined;
   for (const route of ownedRoutes()) {
      const target = foldRouteName(route);
      const distance = editDistance(folded, target);
      const limit = target.length <= 5 ? 1 : 2;
      if (distance <= limit && (!best || distance < best.distance)) {
         best = { route, distance };
      }
   }
   if (best) return { spelling: `#(${best.route})`, route: best.route };
   const directive = PERSIST_DIRECTIVES.find(
      (d) => foldRouteName(d) === folded,
   );
   return directive ? { spelling: `#@ ${directive}`, route: "@" } : undefined;
}

/**
 * The `#@` directive `word` is a misspelling of (an edit within 2, or a
 * different case: Malloy reads only `persist`, never `Persist`), or
 * `undefined`.
 */
function misspelledDirective(word: string): string | undefined {
   const lowered = word.toLowerCase();
   return PERSIST_DIRECTIVES.find(
      (d) => d !== word && editDistance(lowered, d) <= 2,
   );
}

/** The first word of a payload, or `undefined`. */
function firstWord(payload: string): string | undefined {
   return /^\s*([A-Za-z_][\w-]*)/.exec(payload)?.[1];
}

/** The keys the `#@` route reads, any of which may open a `#@` line. */
const AT_ROUTE_KEYS: ReadonlySet<string> = new Set([
   ...(PUBLISHER_ROUTES["@"]?.keys ?? []),
   ...Object.keys(PUBLISHER_ROUTES["@"]?.retiredKeys ?? {}),
]);

/**
 * What one note was reaching for, or `"malformed"` when Malloy cannot route
 * it and it reads as no route of ours, or `undefined` when it is fine (on a
 * route Publisher reads, or plainly another app's or a render tag).
 */
function intendedRoute(text: string): Intended | "malformed" | undefined {
   const trimmed = text.trimStart();
   const read = publisherRouteOf(trimmed);
   if (read === "@") {
      // `#@ persits name="t"`: a #@ line whose opening word is no key the
      // route reads but is one edit or two from a directive.
      const word = firstWord(payloadOfNote(trimmed));
      if (word === undefined || AT_ROUTE_KEYS.has(word)) return undefined;
      const directive = misspelledDirective(word);
      return directive
         ? { spelling: `#@ ${directive}`, route: "@" }
         : undefined;
   }
   if (read === "") {
      const payload = payloadOfNote(trimmed);
      // `# persist name="t"`: a render tag named for a directive.
      const word = firstWord(payload)?.toLowerCase();
      if (word !== undefined && PERSIST_DIRECTIVES.includes(word)) {
         return { spelling: `#@ ${word}`, route: "@" };
      }
      // `# (doc) …`: a bracketed route word after a space.
      const bracketed = /^\s*[([{<]\s*([^)\]}>\s]+)\s*[)\]}>]/.exec(payload);
      if (bracketed?.[1] !== undefined) {
         const intended = misspelledOwnedRoute(bracketed[1]);
         if (intended) return intended;
      }
      return undefined;
   }
   // On a route Publisher reads, or a sigil route such as a glued `#"…`
   // doc string, which Publisher reads too.
   if (read !== undefined && (routeIsRead(read) || isReservedRoute(read))) {
      return undefined;
   }

   const route = routeOfNote(trimmed);
   if (route === undefined) {
      // `#@persist`: the sigil glued to the directive.
      const glued = /^##?@([A-Za-z_]\w*)/.exec(trimmed);
      if (glued?.[1] !== undefined) {
         const word = glued[1].toLowerCase();
         const directive = PERSIST_DIRECTIVES.includes(word)
            ? word
            : misspelledDirective(word);
         if (directive) return { spelling: `#@ ${directive}`, route: "@" };
      }
      // `#( doc )`, `#(doc)X`, `#doc`: the first word after the prefix.
      const attempt = /^##?\|?\s*[([{<]?\s*([A-Za-z_][\w-]*)/.exec(trimmed);
      if (attempt?.[1] !== undefined) {
         const intended = misspelledOwnedRoute(attempt[1]);
         if (intended) return intended;
      }
      return "malformed";
   }
   // A reserved sigil (`#:`) is Malloy's to warn about; a MOTLY tag was
   // handled above.
   if (isReservedRoute(route)) return undefined;
   return misspelledOwnedRoute(route);
}

/** The first line of a note, shortened for a message. */
function shown(text: string): string {
   const line = text.trimStart().split(/[\r\n]/, 1)[0] ?? "";
   return line.length > 80 ? `${line.slice(0, 77)}...` : line;
}

/**
 * The near-miss and malformed-route findings among `notes`, one per note.
 * The gate spellings the load refuses are skipped: that refusal names them.
 */
export function annotationNearMisses(
   notes: Iterable<ScannedNote>,
): AnnotationFinding[] {
   const findings: AnnotationFinding[] = [];
   for (const note of notes) {
      if (collectAuthorizeNearMissesAllRoutes([note.text]).length > 0) continue;
      const intended = intendedRoute(note.text);
      if (intended === undefined) continue;
      const written = shown(note.text);
      const where = {
         ...(note.subject ? { subject: note.subject } : {}),
         ...(note.at ? { at: note.at } : {}),
      };
      if (intended === "malformed") {
         findings.push({
            route: "",
            tier: "unservable",
            code: ANNOTATION_MALFORMED_ROUTE_CODE,
            message:
               `\`${written}\` does nothing: Malloy cannot route its prefix, ` +
               `so no reader sees it`,
            rewrite:
               "write `# …` (with the space) for a tag, or `#(name)` for an app route",
            ...where,
         });
         continue;
      }
      findings.push({
         route: intended.route,
         tier: "unservable",
         code: ANNOTATION_NEAR_MISS_CODE,
         message:
            `\`${written}\` is not read as \`${intended.spelling}\`, so it does ` +
            `nothing (did you mean \`${intended.spelling}\`?)`,
         rewrite: `write \`${intended.spelling}\``,
         ...where,
      });
   }
   return findings;
}

/**
 * Every annotation a model file wrote itself, once each: its `##` notes, and
 * the notes on each source, field, view, join and named query it declares.
 * A note from an imported file is that file's to report (every `.malloy` file
 * in a package is a model), so it is left out; a notebook's cells compile as
 * `internal://` documents and are kept.
 */
export function authoredNotes(modelDef: ModelDef): ScannedNote[] {
   const own = (note: AnnotationNote): boolean => {
      const url = note.at?.url;
      return (
         url === undefined ||
         url === modelDef.modelID ||
         url.startsWith("internal://")
      );
   };
   const seen = new Set<string>();
   const out: ScannedNote[] = [];
   const add = (notes: readonly AnnotationNote[], subject?: string) => {
      for (const note of notes) {
         if (!own(note)) continue;
         const key = note.at
            ? notePositionKey(note.at)
            : `${subject ?? ""}#${note.text}`;
         if (seen.has(key)) continue;
         seen.add(key);
         out.push({
            text: note.text,
            ...(note.at ? { at: note.at } : {}),
            ...(subject ? { subject } : {}),
         });
      }
   };
   add(ownModelNoteObjects(modelDef));
   const structs: { name: string; struct: StructDef }[] = [];
   for (const [name, obj] of Object.entries(modelDef.contents)) {
      if (isSourceDef(obj)) {
         structs.push({ name, struct: obj as StructDef });
      } else if (obj.type === "query") {
         add(ownLevelNotes(obj.annotations), name);
      }
   }
   for (const value of Object.values(modelDef.sourceRegistry ?? {})) {
      const entry = value.entry;
      if (entry.type === "source_registry_reference") continue;
      if (!isSourceDef(entry)) continue;
      const struct = entry as StructDef;
      structs.push({ name: struct.as ?? struct.name, struct });
   }
   for (const { name, struct } of structs) {
      add(ownLevelNotes(struct.annotations), name);
      for (const field of struct.fields) {
         add(
            ownLevelNotes(field.annotations),
            `${name}.${field.as ?? field.name}`,
         );
      }
   }
   return out;
}
