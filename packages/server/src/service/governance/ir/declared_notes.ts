// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which source-level notes a source's OWN `source:` statement declared, read
 * off compiler-set IR only.
 *
 * Malloy's `DefineSource` gives a source a FRESH `annotations` object
 * (`{ ...ownAnnotation, inherits: base }`) exactly when its statement carries
 * an annotation, and otherwise hands it the base's `annotations` object as is.
 * So an extension or rename that declares nothing (`source: e is o extend
 * {…}`, `source: r is o`) holds the very object its base holds, and its
 * own-level notes are the base's. The IR does not reliably name that base
 * (`extends` is absent on an extension of a table-backed source, and a rename
 * of a modified source has no `referenceID`), so the sharing itself is the
 * signal:
 *
 *  - a note parsed in another file than the source's statement is not the
 *    source's (an import's annotation reached it by reference);
 *  - a source that shares its `annotations` object with a source of the same
 *    file declared before it declared nothing: that object is the earlier
 *    source's, however many hops away.
 *
 * Every definition of a multi-definition statement (`#(x) source: a is …, b
 * is …`) gets its own object carrying the block's notes, so each declares
 * them, as Malloy reads it. Identity holds within one compile, which is all a
 * plan reads (a source is planned only in its own file's compile). A
 * `contents` that lost identity (a JSON round trip) reads an unannotated
 * extension in the base's file as declaring the base's notes: a stray build,
 * never a missed one. A source with no location keeps every note.
 *
 * Bundled into the package-load worker: keep imports to `@malloydata/malloy`
 * types and `../../annotations`.
 */

import { ownLevelNotes, type AnnotationNote } from "../../annotations";

/** A position in a document, as the IR carries one. */
interface Position {
   line: number;
   character: number;
}

/** The compiled source shape read here: compiler-set IR fields only. */
export interface NotedSourceDef {
   /** Where the source's own statement was parsed. */
   location?: {
      url?: string;
      range?: { start?: Position };
   };
   annotations?: Parameters<typeof ownLevelNotes>[0];
}

function before(a: Position, b: Position): boolean {
   return a.line < b.line || (a.line === b.line && a.character < b.character);
}

/**
 * The source-level notes `def`'s own statement declared (see the module doc).
 * `contents` is the compiled model's `contents`, `def` among them.
 */
export function declaredNotes(
   def: NotedSourceDef,
   contents: Readonly<Record<string, unknown>>,
): AnnotationNote[] {
   const notes = ownLevelNotes(def.annotations);
   const url = def.location?.url;
   const start = def.location?.range?.start;
   if (notes.length === 0 || !url || !start) return notes;
   const inherited = Object.values(contents).some((object) => {
      if (object === def || !object || typeof object !== "object") return false;
      const other = object as NotedSourceDef;
      const otherStart = other.location?.range?.start;
      return (
         other.annotations === def.annotations &&
         other.location?.url === url &&
         !!otherStart &&
         before(otherStart, start)
      );
   });
   if (inherited) return [];
   return notes.filter((note) => !note.at || note.at.url === url);
}
