// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * DEPRECATED: v1's `#@ preaggregate`, found where an author wrote it, so the
 * package can warn with the v2 rewrite (`docs/materialization-migration.md`).
 *
 * Under materialization v2 `#@ preaggregate` is spelled `#(preaggregate)`. It
 * keeps working through 0.x, is refused at 1.0.0 and loses its reader in 1.1
 * (the Publisher v1 plan's rule, #1563; materialization v2 plan, question 5).
 * `#@ -persist` is not here: it is Malloy core's opt-out, and Publisher keeps
 * honouring it (question 5, decided 2026-11-12). This module only finds
 * `#@ preaggregate`; it changes nothing about how it is built or served.
 */

import { isSourceDef, type ModelDef } from "@malloydata/malloy";
import { payloadOfNote, routeOfNote } from "../annotation_reader";
import { ownLevelNotes, type AnnotationNote } from "../annotations";

/** One deprecated `#@` annotation, at the measure that declares it. */
export type DeprecatedPersistAnnotation = {
   kind: "preaggregate";
   source: string;
   measure: string;
};

const PREAGGREGATE_PAYLOAD = /^\s*preaggregate\b/;

/** Whether a note is a `#@` line whose payload `payload` matches. */
function isPersistRouteNote(note: AnnotationNote, payload: RegExp): boolean {
   return (
      routeOfNote(note.text) === "@" && payload.test(payloadOfNote(note.text))
   );
}

interface Position {
   line: number;
   character: number;
}

function notAfter(a: Position, b: Position): boolean {
   return a.line < b.line || (a.line === b.line && a.character <= b.character);
}

/**
 * Every deprecated `#@` annotation the model file declares: one per measure
 * whose own definition, in a source statement of this file, declares
 * `#@ preaggregate` (however many grains). `declaredHere` says whether a
 * document URL is this model's file: a source an import brought in is its own
 * file's to report.
 *
 * A measure's note is the source's own when it was parsed inside that source's
 * statement, so an extension that carries the base's measures (and the base's
 * notes on them) reports none, while an `include` list item that annotates a
 * measure it lists does.
 */
export function deprecatedPersistAnnotations(
   modelDef: ModelDef,
   declaredHere: (url: string) => boolean,
): DeprecatedPersistAnnotation[] {
   const found: DeprecatedPersistAnnotation[] = [];
   for (const entry of Object.values(modelDef.contents)) {
      if (!isSourceDef(entry)) continue;
      const url = entry.location?.url;
      if (!url || !declaredHere(url)) continue;
      const source = entry.as || entry.name;
      const range = entry.location?.range;
      /** Whether `note` was parsed in this source's own statement. */
      const ownStatement = (note: AnnotationNote): boolean =>
         note.at !== undefined &&
         note.at.url === url &&
         (range === undefined ||
            (notAfter(range.start, note.at.range.start) &&
               notAfter(note.at.range.start, range.end)));
      for (const field of entry.fields) {
         const annotations = (field as { annotations?: unknown }).annotations;
         if (annotations === undefined) continue;
         const notes = ownLevelNotes(
            annotations as Parameters<typeof ownLevelNotes>[0],
         );
         if (
            notes.some(
               (note) =>
                  ownStatement(note) &&
                  isPersistRouteNote(note, PREAGGREGATE_PAYLOAD),
            )
         ) {
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
