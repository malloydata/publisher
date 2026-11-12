// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * DEPRECATED: v1's `#@ -persist` and `#@ preaggregate`, found where an author
 * wrote them, so the package can warn with the v2 rewrite
 * (`docs/materialization-migration.md`).
 *
 * Under materialization v2 a materialized source serves every reader, so
 * `#@ -persist` (opt a reader out of a persisted table) has no meaning, and
 * `#@ preaggregate` is spelled `#(preaggregate)`. Both keep working through
 * 0.x, are refused at 1.0.0 and lose their readers in 1.1 (the Publisher v1
 * plan's rule, #1563; materialization v2 plan, question 5). This module only
 * finds them; it changes nothing about how either is built or served.
 */

import { isSourceDef, type ModelDef } from "@malloydata/malloy";
import { payloadOfNote, routeOfNote } from "../annotation_reader";
import {
   notePositionKey,
   ownLevelNotes,
   type NotePosition,
} from "../annotations";

/** One deprecated `#@` annotation, at the source or measure that declares it. */
export type DeprecatedPersistAnnotation =
   | { kind: "persist_opt_out"; source: string }
   | { kind: "preaggregate"; source: string; measure: string };

const OPT_OUT_PAYLOAD = /^\s*-persist\b/;
const PREAGGREGATE_PAYLOAD = /^\s*preaggregate\b/;

interface NoteLike {
   text: string;
   at?: NotePosition;
}

/** Whether a note is a `#@` line whose payload `payload` matches. */
function isPersistRouteNote(note: NoteLike, payload: RegExp): boolean {
   return (
      routeOfNote(note.text) === "@" && payload.test(payloadOfNote(note.text))
   );
}

/**
 * Every deprecated `#@` annotation `modelPath`'s own file declares: one per
 * `#@ -persist` line and one per measure declaring `#@ preaggregate`, however
 * many sources carry them. A source that extends,
 * aliases or imports an annotated one carries the same note (Malloy copies it
 * by reference), and is not reported; the note is attributed to the first
 * source that carries it, which in declaration order is the one it was written
 * on. Notes written in another file are that file's to report.
 */
export function deprecatedPersistAnnotations(
   modelDef: ModelDef,
   modelPath: string,
): DeprecatedPersistAnnotation[] {
   const ownFile = (at: NotePosition | undefined): boolean => {
      if (at === undefined) return false;
      try {
         return decodeURIComponent(new URL(at.url).pathname).endsWith(
            `/${modelPath}`,
         );
      } catch {
         return false;
      }
   };
   const seen = new Set<string>();
   /** Whether `note` is written here and not reported yet; marks it reported. */
   const fresh = (note: NoteLike): boolean => {
      if (!ownFile(note.at)) return false;
      const key = notePositionKey(note.at as NotePosition);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
   };

   const found: DeprecatedPersistAnnotation[] = [];
   for (const entry of Object.values(modelDef.contents)) {
      if (!isSourceDef(entry)) continue;
      const source = entry.as || entry.name;
      for (const note of ownLevelNotes(entry.annotations) as NoteLike[]) {
         if (isPersistRouteNote(note, OPT_OUT_PAYLOAD) && fresh(note)) {
            found.push({ kind: "persist_opt_out", source });
         }
      }
      for (const field of entry.fields) {
         const annotations = (field as { annotations?: unknown }).annotations;
         if (annotations === undefined) continue;
         const notes = ownLevelNotes(
            annotations as Parameters<typeof ownLevelNotes>[0],
         ) as NoteLike[];
         // One finding per measure, however many grains it declares; every
         // line is marked seen, so an extension carrying them reports none.
         const declared = notes
            .filter((note) => isPersistRouteNote(note, PREAGGREGATE_PAYLOAD))
            .map(fresh);
         if (declared.includes(true)) {
            found.push({
               kind: "preaggregate",
               source,
               measure: field.as ?? field.name,
            });
         }
      }
   }
   return found;
}
