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
 * NOT export from its public barrel. We replicate it here: a post-order DFS
 * over `inheritsFrom` (cycle-safe, each model emitted once at its most-
 * ancestral slot) yields imports-first / local-last order, which we wrap into
 * an `AnnotationsDef` whose `inherits` chain carries that order with the target
 * model's own notes at the top. The `Annotations` view then reads it unchanged
 * (`.notes` = the target's own notes; `.texts()` = the whole lineage). Replace
 * with a direct `import { getModelAnnotations }` once malloy exports it.
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
   // resulting `inherits` chain.
   let folded: AnnotationsDef | undefined;
   for (const id of order) {
      folded = { ...registry[id].ownNotes, inherits: folded };
   }
   return folded ?? {};
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
