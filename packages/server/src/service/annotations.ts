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
export function modelAnnotations(modelDef: ModelDef): AnnotationsDef {
   const registry = modelDef.modelAnnotations ?? {};
   const visited = new Set<string>();
   const order: string[] = [];
   const visit = (id: string): void => {
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
 * file-level `##(authorize)` has to flow into an importing file — a gate an
 * import could shed would be no gate at all. Every *other* model-level tag
 * wants the opposite: `## artifact`, `## title=`, `## autorun=`, and
 * `## givens {…}` describe one document, so inheriting them means a shared
 * include carrying an `## artifact` silently turns every file that imports it
 * into a copy of that dashboard, and a notebook picks up an imported model's
 * title. Read through here unless the tag is a policy gate.
 */
export function ownModelNotes(modelDef: ModelDef): string[] {
   const registry = modelDef.modelAnnotations ?? {};

   // One document can span several compilation nodes. A `.malloynb` is compiled
   // a cell at a time, so its `##` tags land on an `internal://loadModel` node
   // that an `internal://extendModel` node — the one `modelID` names — inherits
   // from, with empty notes of its own. Reading only `modelID` would therefore
   // find nothing on every notebook. An imported file, by contrast, is always a
   // `file://` node: that scheme is the line between "more of this document"
   // and "a different document".
   const isSameDocument = (id: string) =>
      id === modelDef.modelID || !id.startsWith("file:");

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
      for (const note of [
         ...(entry.ownNotes.blockNotes ?? []),
         ...(entry.ownNotes.notes ?? []),
      ]) {
         texts.push(note.text);
      }
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
