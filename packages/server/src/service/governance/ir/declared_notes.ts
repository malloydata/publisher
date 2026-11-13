// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which source-level notes a source's OWN `source:` statement declared, and
 * which notes it declared on one of its fields ({@link declaredFieldNotes}),
 * read off compiler-set IR only.
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

/** A field of a compiled source, as {@link declaredFieldNotes} reads it. */
export interface NotedFieldDef {
   annotations?: Parameters<typeof ownLevelNotes>[0];
}

/** The compiled sources a field's holders are looked for among. */
export type SourceScope =
   | readonly unknown[]
   | Readonly<Record<string, unknown>>;

/** Each field's `annotations` object, with the sources of a scope holding it. */
const fieldAnnotationHolders = new WeakMap<
   object,
   Map<object, NotedSourceDef[]>
>();

function holdersOf(scope: SourceScope): Map<object, NotedSourceDef[]> {
   const cached = fieldAnnotationHolders.get(scope);
   if (cached) return cached;
   const holders = new Map<object, NotedSourceDef[]>();
   const sources: readonly unknown[] = Array.isArray(scope)
      ? scope
      : Object.values(scope);
   for (const object of sources) {
      if (!object || typeof object !== "object") continue;
      const fields = (object as { fields?: unknown }).fields;
      if (!Array.isArray(fields)) continue;
      for (const field of fields) {
         const annotations = (field as NotedFieldDef | undefined)?.annotations;
         if (!annotations || typeof annotations !== "object") continue;
         const list = holders.get(annotations) ?? [];
         if (!list.includes(object as NotedSourceDef)) {
            list.push(object as NotedSourceDef);
         }
         holders.set(annotations, list);
      }
   }
   fieldAnnotationHolders.set(scope, holders);
   return holders;
}

/**
 * The notes `def`'s own statement declared on one of its fields (a view's
 * `#@ persist`, say), by the rule of {@link declaredNotes}.
 *
 * Malloy copies a source's fields onto every source built from it (an
 * extension, a rename, a `rename:` of the field, an import), and every copy
 * keeps the field's `annotations` object by reference. A statement that
 * annotates the field (its own `view:`, a redefinition after `except:`, an
 * alias `view: v2 is v`, a re-annotation in `include { #@ … public: v }`)
 * gives the field a FRESH object. So a field whose `annotations` object a
 * source of the same file declared before `def` also holds declared nothing
 * in `def`'s statement, and a note parsed in another file is never `def`'s.
 *
 * The field's own `location` is not the signal: a `rename:` gives the field
 * a new location inside the renaming statement but keeps its notes, and an
 * `include:` re-annotation adds notes but keeps the field's old location.
 * `scope` holds every compiled source to look among, `def` included: a
 * model's `contents`, or every declaration it reaches (its source registry
 * too, which holds a source an imported file declared but did not export).
 * Identity holds within one compile; a scope that lost it reads every
 * same-file copy as declaring the notes (a stray plan, never a missed one).
 */
export function declaredFieldNotes(
   def: NotedSourceDef,
   field: NotedFieldDef,
   scope: SourceScope,
): AnnotationNote[] {
   const notes = ownLevelNotes(field.annotations);
   const url = def.location?.url;
   const start = def.location?.range?.start;
   if (notes.length === 0 || !url || !start || !field.annotations) {
      return notes;
   }
   const inherited = (holdersOf(scope).get(field.annotations) ?? []).some(
      (other) => {
         if (other === def) return false;
         const otherStart = other.location?.range?.start;
         return (
            other.location?.url === url &&
            !!otherStart &&
            before(otherStart, start)
         );
      },
   );
   if (inherited) return [];
   return notes.filter((note) => !note.at || note.at.url === url);
}
