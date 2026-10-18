// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Annotations, type ModelDef } from "@malloydata/malloy";

/**
 * The raw IR annotation bundle. Workaround: `@malloydata/malloy` exports the
 * `Annotations` view but not the underlying `AnnotationsDef` type, so we
 * recover it from the view's constructor parameter. Replace with a direct
 * `import type { AnnotationsDef }` once malloy exports it.
 */
export type AnnotationsDef = NonNullable<
   ConstructorParameters<typeof Annotations>[0]
>;

/**
 * One raw annotation note, as carried on `AnnotationsDef.blockNotes`/`.notes`.
 * Callers hold the OBJECT, not just its text: object identity is the only way
 * to tell a note Malloy copied by reference from a base from an independently
 * authored note with identical text (see {@link ownLevelNotes}).
 */
export type AnnotationNote = NonNullable<AnnotationsDef["blockNotes"]>[number];

/**
 * True if a route belongs to Malloy's own reserved namespace rather than an
 * application. Malloy claims the empty route (`''` — MOTLY tags / render
 * config) and the whole punctuation-sigil namespace (`!` `@` `"` `:`, and
 * any punct-only route). Everything else is a bracketed app route
 * (`#(doc)`, `#<label>`, …). Belongs in `@malloydata/malloy` core beside
 * `parsePrefix`; core does not export an equivalent.
 */
export function isReservedRoute(route: string): boolean {
   return route === "" || !/[\p{L}\p{N}]/u.test(route);
}

/**
 * A model file's OWN `##` annotation bundle, ignoring everything it imports.
 *
 * The parsed-tag counterpart to {@link ownModelNotes}, which returns note TEXTS
 * and so cannot feed `parseAsTag` — that needs each note's source location, and
 * a note without one makes every reader catch and degrade to "no layer". Same
 * document rule as `ownModelNotes`: a node counts only if it is this document or
 * one of malloy's synthetic URL-less compiles, so a notebook's per-cell nodes are
 * in and every real-URL import is out.
 *
 * Use this for a tag that describes one document; use {@link modelAnnotations}
 * only for a policy gate that must survive being imported.
 */
export function ownModelAnnotations(modelDef: ModelDef): AnnotationsDef {
   return foldModelAnnotations(
      modelDef,
      (id) => id === modelDef.modelID || id.startsWith("internal://"),
   );
}

/**
 * The model (`##`) annotation bundle for one model, folded across its
 * import/extend lineage. Replicates malloy's unexported `getModelAnnotations`
 * (`dist/model/annotation_utils.js`): a cycle-safe post-order DFS over
 * `inheritsFrom`, folded so the local model sits at the top of `inherits`.
 *
 * A model with no `##` of its own adds no link, so `.notes` returns the nearest
 * ancestor that has notes. That matters because `source_extraction.ts` refuses
 * a file-level `##(authorize)` from this, and the refusal must fire for one in
 * an imported model too — otherwise the stray tag fails open. Only
 * `notes`/`blockNotes` are copied, so `ownNotes.inherits` cannot leak in.
 * Use this only for a policy gate that must survive being imported.
 */
export function modelAnnotations(modelDef: ModelDef): AnnotationsDef {
   return foldModelAnnotations(modelDef, () => true);
}

/**
 * Shared fold behind {@link modelAnnotations} and {@link ownModelAnnotations} —
 * the two differ only in which nodes they admit.
 */
function foldModelAnnotations(
   modelDef: ModelDef,
   admits: (id: string) => boolean,
): AnnotationsDef {
   const registry = modelDef.modelAnnotations ?? {};
   const visited = new Set<string>();
   const order: string[] = [];
   const visit = (id: string): void => {
      if (!admits(id)) return;
      if (visited.has(id)) return;
      visited.add(id);
      const entry = registry[id];
      if (!entry) return;
      for (const dep of entry.inheritsFrom) visit(dep);
      order.push(id); // post-order: ancestors precede the model itself
   };
   visit(modelDef.modelID);

   // Fold most-ancestral → local so the local model lands at the top of the
   // resulting `inherits` chain. Models with no own notes add no link.
   let folded: AnnotationsDef | undefined;
   for (const id of order) {
      const own = registry[id].ownNotes;
      if (!own.notes?.length && !own.blockNotes?.length) continue;
      folded = {
         notes: own.notes,
         blockNotes: own.blockNotes,
         inherits: folded,
      };
   }
   return folded ?? {};
}

/**
 * The `##` tags a model file declares **itself**, ignoring everything it
 * imports. {@link modelAnnotations} folds the import lineage on purpose, for
 * policy gates; every other model-level tag describes one document, and
 * folding it would let a shared include configure every importer (e.g. an
 * include's `##(filters)` configuring every importing notebook's filter
 * panel). Read through here unless the tag is a policy gate.
 *
 * Precondition: `modelID` names either a URL-loaded file or an all-inline
 * compile chain, which is what the loader builds for a `.malloy` and a
 * `.malloynb` respectively. A URL-loaded model that was then `extendModel`ed
 * would leave its tags on the `file://` node while `modelID` is `internal://`,
 * so this returns nothing for that shape. Nothing builds it today, and the
 * failure drops a document's own tags rather than folding an import's in.
 */
export function ownModelNotes(modelDef: ModelDef): string[] {
   return ownModelNoteObjects(modelDef).map((note) => note.text);
}

/** {@link ownModelNotes} with each note's location, for callers that need where it sits. */
export function ownModelNoteObjects(modelDef: ModelDef): AnnotationNote[] {
   const registry = modelDef.modelAnnotations ?? {};

   // A `.malloynb` is compiled a cell at a time, so its `##` tags land on
   // `internal://` nodes that the `modelID` node inherits from; reading only
   // `modelID` would find nothing on a notebook. This is an allowlist: every
   // import resolves to a real URL and is excluded, and a renamed internal
   // scheme (malloy's `isInternalURL`) fails safe — tags dropped, not folded in.
   const isSameDocument = (id: string) =>
      id === modelDef.modelID || id.startsWith("internal://");

   const seen = new Set<string>();
   const notes: AnnotationNote[] = [];
   const visit = (id: string): void => {
      if (seen.has(id) || !isSameDocument(id)) return;
      seen.add(id);
      const entry = registry[id];
      if (!entry) return;
      // Ancestral-first, matching `Annotations.texts()`, so a later cell's tag
      // wins over an earlier one the way a later line does within a file.
      for (const dep of entry.inheritsFrom) visit(dep);
      notes.push(...ownLevelNotes(entry.ownNotes));
   };
   visit(modelDef.modelID);
   return notes;
}

/**
 * Every annotation text on an entity — its own `notes` and `blockNotes`
 * plus everything inherited from its ancestors. All of an entity's
 * annotations apply; none are dropped by source location. Returns
 * `undefined`, not `[]`, when empty, to match the optional API shape.
 */
export function annotationTexts(
   annote: AnnotationsDef | undefined,
): string[] | undefined {
   const texts = new Annotations(annote).texts();
   return texts.length > 0 ? texts : undefined;
}

/**
 * The annotation texts declared directly on ONE node of an `AnnotationsDef`
 * chain — not its `inherits` ancestors.
 *
 * A source-level annotation lands under one of two keys, decided purely by
 * which syntax the author used:
 *
 *     blockNotes    #(tag)                 statement form
 *                   source: name is ...
 *
 *     notes         source:                multi-definition block form
 *                     #(tag)
 *                     name is ...
 *
 *     notes         source: name is        after `is` (malloy's getIsNotes)
 *                     #(tag)
 *                     base
 *
 * It is the same declaration slot in all three, so a caller walking `inherits`
 * by hand for the nearest declaring level must read both keys at each link,
 * or it silently skips the latter two forms.
 */
export function ownLevelNoteTexts(
   annote: AnnotationsDef | undefined,
): string[] {
   return ownLevelNotes(annote).map((note) => note.text);
}

/**
 * {@link ownLevelNoteTexts}, but the note objects. Malloy copies a base's
 * note objects onto a deriving struct BY REFERENCE when the deriving statement
 * adds no annotation of its own (`extend {}`, an unannotated join, …), so
 * object identity — not text — tells a copy from an identical authored note.
 */
export function ownLevelNotes(
   annote: AnnotationsDef | undefined,
): AnnotationNote[] {
   return [...(annote?.blockNotes ?? []), ...(annote?.notes ?? [])];
}

/** Where a note was parsed: a document URL and the span of the annotation. */
export interface NotePosition {
   url: string;
   range: {
      start: { line: number; character: number };
      end: { line: number; character: number };
   };
}

/**
 * Whether two notes were parsed from the exact same place: identifies one note
 * across two compiles of a file, where object identity cannot. Compared field
 * by field because `JSON.stringify` key order is not guaranteed.
 */
export function sameNotePosition(a: NotePosition, b: NotePosition): boolean {
   return (
      a.url === b.url &&
      a.range.start.line === b.range.start.line &&
      a.range.start.character === b.range.start.character &&
      a.range.end.line === b.range.end.line &&
      a.range.end.character === b.range.end.character
   );
}

/** {@link sameNotePosition} as a string, for use as a `Set`/`Map` key. */
export function notePositionKey(at: NotePosition): string {
   return `${at.url}#${at.range.start.line}:${at.range.start.character}-${at.range.end.line}:${at.range.end.character}`;
}
