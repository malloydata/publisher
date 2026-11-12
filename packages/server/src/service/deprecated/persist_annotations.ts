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
import { ownLevelNotes, type AnnotationNote } from "../annotations";
import { declaredNotes } from "../governance/ir/declared_notes";

/** One deprecated `#@` annotation, at the source or measure that declares it. */
export type DeprecatedPersistAnnotation =
   | { kind: "persist_opt_out"; source: string }
   | { kind: "preaggregate"; source: string; measure: string };

const OPT_OUT_PAYLOAD = /^\s*-persist\b/;
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
 * Every deprecated `#@` annotation the model file declares: one per source
 * whose own statement declares `#@ -persist`, and one per measure whose own
 * definition, in a source statement of this file, declares `#@ preaggregate`
 * (however many grains). `declaredHere` says whether a document URL is this
 * model's file: a source an import brought in is its own file's to report.
 *
 * Which source declares a source-level note is {@link declaredNotes}'s rule,
 * the one the `#(materialize)` planner reads: an extension or rename that
 * declares nothing inherits its base's notes and is not reported, and each
 * definition of a multi-definition statement declares the block's notes. A
 * measure's note is the source's own when it was parsed inside that source's
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
      if (
         declaredNotes(entry, modelDef.contents).some((note) =>
            isPersistRouteNote(note, OPT_OUT_PAYLOAD),
         )
      ) {
         found.push({ kind: "persist_opt_out", source });
      }
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
