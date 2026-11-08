// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Finding a struct's inherited `#(authorize)` gate, shared by `Model`
 * (request time) and `extractSourcesFromModelDef` (package load), plus
 * {@link resolveQuerySourceBase}, the `query_source` base lookup both use.
 *
 * How Malloy carries a base's annotations onto a derived struct:
 *  - `extend {}` (also with `rename:`/`except:`/`accept:`) carrying NO
 *    annotation of its own: the base's note objects are copied, by reference,
 *    onto the derived struct's OWN `annotations`. Reading the struct's own
 *    notes already finds the gate, so authored-vs-inherited must be decided by
 *    note identity, not source identity (see `validateAuthorizeProbes`).
 *  - a deriving statement that DOES carry an annotation demotes the base's
 *    notes to `annotations.inherits`, which {@link ancestorGateExprs} walks.
 *  - a `query_source` (`Z is X -> {...}`) carries no `annotations` at all and
 *    its registry entry self-references; the only link back to `X` is
 *    `Z.query.structRef` ({@link resolveQuerySourceBase}). That hop is NOT
 *    folded into {@link ancestorGateExprs}: `Model.collectEntryPointGates`
 *    takes it in its own recursion, so folding it in would double the result.
 *    `extractSourcesFromModelDef` uses {@link effectiveAncestorGateExprs}.
 *
 * Bundled into the package-load worker via `source_extraction.ts`: keep
 * imports to `@malloydata/malloy` types and pure annotation helpers.
 */

import type { ModelDef, SourceDef } from "@malloydata/malloy";
import { type AnnotationsDef, ownLevelNotes } from "./annotations";
import { collectAuthorizeExprsForRoute } from "./authorize";
import {
   resolveCompositeResolvedBase,
   resolveDeclaredSource,
   resolveQuerySourceBase,
} from "./governance/ir/source_graph";

/**
 * Link budget for {@link ancestorGateExprs}'s derivation walk. Exceeding it
 * denies/stops (the chain wasn't read to its end), so it only has to be
 * larger than any real chain.
 */
export const ANCESTOR_WALK_MAX_DEPTH = 32;

/** The gate exprs of one level's own notes, for one route. */
function gateAtLevel(
   annotations: AnnotationsDef | undefined,
   route: string,
): string[] {
   return collectAuthorizeExprsForRoute(
      ownLevelNotes(annotations).map((note) => note.text),
      route,
   );
}

/**
 * The gate a struct inherits from the source it was derived from, used when
 * it declares no gate of its own.
 *
 * Once the deriving statement carries ANY annotation (not just an authorize
 * one), Malloy no longer leaves the base's annotations at top level:
 *  - an annotated `extend {}` demotes them to `annotations.inherits`;
 *  - an annotated `join_one:`/`join_many:` replaces them outright, leaving
 *    `sourceID`/`referenceID` as the only link back;
 *  - a `query_source` carries none, leaving only the registry link.
 * So reading own notes alone would lose the base's gate to a stray render tag
 * or doc comment. All links are followed, nearest first; the first ancestor
 * declaring a gate wins ("own wins over ancestor" is what lets a curated
 * extension replace a locked base's gate). Each level is read with
 * {@link gateAtLevel} so a gate under `notes` is found too.
 *
 * Fail-closed: an exhausted walk or an `unresolvable` registry link returns
 * `["false"]`, never `[]`: the chain exists and was not read to its end.
 *
 * Own-wins-over-ancestor is decided per `route`: each level's notes are
 * filtered to that route, so another route's note does not stop the walk.
 * Because the two routes' walks can diverge, neither can rely on the other
 * having already denied, so the sentinel is synthesized on both. Grafting
 * two `false` filters for one query is harmless and does not double the
 * metric.
 */
export function ancestorGateExprs(
   struct: SourceDef,
   modelDef: ModelDef | undefined,
   route: string,
   seen: Set<SourceDef> = new Set(),
): string[] {
   let inherited = struct.annotations?.inherits;
   for (let depth = 0; inherited && depth < ANCESTOR_WALK_MAX_DEPTH; depth++) {
      const exprs = gateAtLevel(inherited, route);
      if (exprs.length > 0) return exprs;
      inherited = inherited.inherits;
   }
   if (inherited) return ["false"];
   // Followed as deep as the inherits chain: `seen` stops cycles, and
   // truncating would lose a gate two declarations up (fail open).
   seen.add(struct);
   if (seen.size > ANCESTOR_WALK_MAX_DEPTH) {
      return ["false"];
   }
   const declared = resolveDeclaredSource(struct, modelDef);
   // A link exists but could not be followed, so the gate is unknown: deny.
   if (declared.kind === "unresolvable") {
      return ["false"];
   }
   // A cycle returns `[]`, not `["false"]`, which is sound only because of a
   // caller precondition: every call site reads the struct's OWN gate first
   // and calls this walk only when that was empty. On an A->B->A cycle the far
   // end is an ancestor already read or the starting struct, so nothing is
   // skipped.
   //
   // Do not change this to `["false"]`: diamonds and self-references
   // legitimately revisit a struct, and that would deny every query on it.
   // Do not hoist this walk above a call site's own-gate read either; losing
   // that read turns this `[]` into a real fail-open.
   if (declared.kind === "none" || seen.has(declared.source)) return [];
   const exprs = gateAtLevel(declared.source.annotations, route);
   return exprs.length > 0
      ? exprs
      : ancestorGateExprs(declared.source, modelDef, route, seen);
}

/**
 * {@link ancestorGateExprs} plus the two hops it does not take: a
 * `query_source`'s base ({@link resolveQuerySourceBase}) and, additively,
 * its composite-member resolution ({@link resolveCompositeResolvedBase}).
 * For `extractSourcesFromModelDef`, which has no recursion of its own for
 * either hop.
 *
 * Returns groups, not a flat list (see `AuthorizeMap`): the base's gate and
 * the composite member's gate belong to different declaring sources and must
 * AND. Flattening them into one list would OR them, a row-level leak. Each
 * hop contributes at most one group.
 *
 * An unresolvable `query_source` base denies (`[["false"]]`): it derives from
 * something by construction, so failing to read it is not evidence of no
 * gate. The composite hop is additive and has no such deny.
 *
 * Malloy's composite resolver copies the base's own note objects onto the
 * resolved member's own notes, by reference. Those are subtracted by identity
 * (`parentOwnNotes`) before reading the member's gate; otherwise the base's
 * condition would be folded into the member's group.
 *
 * `route` is threaded through every read; see {@link ancestorGateExprs}.
 */
export function effectiveAncestorGateExprs(
   struct: SourceDef,
   modelDef: ModelDef | undefined,
   route: string,
   seen: Set<SourceDef> = new Set(),
): string[][] {
   const direct = ancestorGateExprs(struct, modelDef, route, new Set(seen));
   if (direct.length > 0) return [direct];
   if (seen.has(struct)) return [];
   seen.add(struct);
   const groups: string[][] = [];
   const base = resolveQuerySourceBase(struct, modelDef);
   if (!base) {
      const duck = struct as unknown as { type: string };
      // Fail-closed sentinel, on both routes (see `ancestorGateExprs`).
      if (duck.type === "query_source") {
         groups.push(["false"]);
      }
   } else if (!seen.has(base)) {
      const ownExprs = gateAtLevel(base.annotations, route);
      groups.push(
         ...(ownExprs.length > 0
            ? [ownExprs]
            : effectiveAncestorGateExprs(base, modelDef, route, seen)),
      );
   }
   const composite = resolveCompositeResolvedBase(struct);
   if (composite && !seen.has(composite)) {
      const parentOwnNotes = base ? ownLevelNotes(base.annotations) : [];
      const compositeOwnNotes = ownLevelNotes(composite.annotations).filter(
         (note) => !parentOwnNotes.includes(note),
      );
      const compositeOwn = gateAtLevel({ notes: compositeOwnNotes }, route);
      groups.push(
         ...(compositeOwn.length > 0
            ? [compositeOwn]
            : effectiveAncestorGateExprs(composite, modelDef, route, seen)),
      );
   }
   return groups;
}

/**
 * The gate groups in force on a struct for one route: its own notes when it
 * declares any, else what it inherits ({@link effectiveAncestorGateExprs}).
 * A `query_source` is the exception: derivation is ADDITIVE
 * (docs/authorize.md), so its own gate never sheds its base's and both come
 * back, as separate groups that AND.
 */
export function gateExprsForRoute(
   struct: SourceDef,
   modelDef: ModelDef | undefined,
   route: string,
): string[][] {
   const own = gateAtLevel(struct.annotations, route);
   if (own.length === 0) {
      return effectiveAncestorGateExprs(struct, modelDef, route, new Set());
   }
   return struct.type === "query_source"
      ? [own, ...effectiveAncestorGateExprs(struct, modelDef, route, new Set())]
      : [own];
}
