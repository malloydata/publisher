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
 * True if a route belongs to Malloy's own reserved namespace rather than an
 * application. Malloy claims the empty route (`''` — MOTLY tags / render
 * config) and the whole punctuation-sigil namespace (`!` `@` `"` `:`, and
 * any punct-only route — Form 2 reserves all of them). Everything else is a
 * bracketed app route (`#(doc)`, `#<label>`, …).
 *
 * TODO: this route classification belongs in `@malloydata/malloy` core,
 * beside `parsePrefix` — otherwise every consumer reinvents it. Remove when
 * core exports an equivalent.
 */
export function isReservedRoute(route: string): boolean {
   return route === "" || !/[\p{L}\p{N}]/u.test(route);
}

/**
 * The model (`##`) annotation bundle for one model, folded across its
 * import/extend lineage.
 *
 * Workaround: malloy 0.0.405 moved model annotations off `ModelDef.annotation`
 * and onto `ModelDef.modelAnnotations` (a `modelID → {ownNotes, inheritsFrom}`
 * registry), folded by the `getModelAnnotations` helper — which malloy does
 * NOT export from its public barrel. We replicate it (matching
 * `@malloydata/malloy/dist/model/annotation_utils.js`): a post-order DFS over
 * `inheritsFrom` (cycle-safe, each model emitted once at its most-ancestral
 * slot) yields ancestral-first / local-last order, folded into an
 * `AnnotationsDef` whose `inherits` chain runs most-ancestral-deepest / local
 * at the top.
 *
 * A model that contributes no `##` of its own adds NO link to the chain (we
 * skip empty `ownNotes`), so `.notes` returns the nearest ancestor that
 * actually has notes — not an empty local node. This matters because `.notes`
 * feeds file-level `##(authorize)` enforcement: an imported model's
 * `##(authorize)` must still flow into an importing file that declares no `##`
 * of its own. We also copy only `notes`/`blockNotes` rather than spreading
 * `ownNotes`, whose own `inherits` would otherwise leak in. Replace with a
 * direct `import { getModelAnnotations }` once malloy exports it.
 */
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
 * imports.
 *
 * This is the counterpart to {@link modelAnnotations}, and the difference is
 * not a detail. That function folds the import lineage on purpose, because
 * file-level `##(authorize)` has to flow into an importing file: a gate an
 * import could shed would be no gate at all. Every *other* model-level tag
 * describes one document, so folding it lets a shared include configure every
 * file that imports it. The one such tag a consumer reads today is
 * `##(filters)`, which the SDK's `parseNotebookFilterAnnotation` matches on the
 * notebook response: an include carrying it configured the filter panel of
 * every notebook that imported the include. Read through here unless the tag is
 * a policy gate.
 *
 * Precondition: `modelID` names either a URL-loaded file or an all-inline
 * compile chain, which is what the loader builds for a `.malloy` and a
 * `.malloynb` respectively. A URL-loaded model that was then `extendModel`ed
 * would leave its tags on the `file://` node while `modelID` is `internal://`,
 * so this returns nothing for that shape. Nothing builds it today, and the
 * failure drops a document's own tags rather than folding an import's in.
 */
export function ownModelNotes(modelDef: ModelDef): string[] {
   const registry = modelDef.modelAnnotations ?? {};

   // One document can span several compilation nodes. A `.malloynb` is compiled
   // a cell at a time, so its `##` tags land on an `internal://loadModel` node
   // that an `internal://extendModel` node (the one `modelID` names) inherits
   // from, with empty notes of its own. Reading only `modelID` would therefore
   // find nothing on every notebook.
   //
   // So this is an allowlist, not a denylist: a node counts as more of this
   // document only if it is the document itself or one of malloy's synthetic
   // URL-less compiles. Every import resolves to a real URL, so excluding
   // everything else keeps a `gs://` or `https://` import out for the same
   // reason a `file://` one is out. `internal://` is malloy's own
   // `isInternalURL` predicate (`api/foundation/readers.ts`), and a scheme it
   // renamed would fall on the safe side here: tags dropped, not folded in.
   const isSameDocument = (id: string) =>
      id === modelDef.modelID || id.startsWith("internal://");

   const seen = new Set<string>();
   const texts: string[] = [];
   const visit = (id: string): void => {
      if (seen.has(id) || !isSameDocument(id)) return;
      seen.add(id);
      const entry = registry[id];
      if (!entry) return;
      // Ancestral-first, matching `Annotations.texts()`, so a later cell's tag
      // wins over an earlier one the way a later line does within a file.
      for (const dep of entry.inheritsFrom) visit(dep);
      texts.push(...ownLevelNoteTexts(entry.ownNotes));
   };
   visit(modelDef.modelID);
   return texts;
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
 * It is the same declaration slot in all three — confirmed by compiling each
 * form and inspecting the resulting `SourceDef.annotations`. So a caller that
 * walks `inherits` by hand for the nearest declaring level, rather than
 * flattening the whole chain via {@link annotationTexts}, has to read both keys
 * at each link, or it silently skips the latter two forms.
 */
export function ownLevelNoteTexts(
   annote: AnnotationsDef | undefined,
): string[] {
   return [...(annote?.blockNotes ?? []), ...(annote?.notes ?? [])].map(
      (note) => note.text,
   );
}
