// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Guards against a filter resolving to the WRONG field once a caller frees
 * up the name it was written against (`rename:`, or `except:`/`accept:`
 * dropping the original then a later block reusing the name) — Malloy
 * resolves a filter's field references by name, late, against whatever
 * struct it ends up attached to, and never re-checks that the name still
 * points at what it did when the filter was written. See `docs/authorize.md`.
 *
 * Covers a grafted `#(access_filter)`/`#(authorize)` condition
 * ({@link assertGraftedGateBindsToDeclaringSource}, which resolves the
 * struct whose OWN annotation wrote the note via
 * {@link findAnnotationDeclaringSource}), a plain author `where:` inherited
 * through an `extend` and a joined source's own filters
 * ({@link assertInheritedSourceFiltersBind}, walking
 * {@link nextDerivationLink}'s structural links back to the declaring
 * struct), and `#(filter)` injection (`Model.assertFilterAnnotationsBindToDeclaringSource`
 * in `./model.ts`, via {@link findFilterAnnotationDeclaringSource} and
 * {@link assertFilterDimensionBindsToDeclaringSource}). All three reuse the
 * one field-identity primitive, {@link assertFilterConditionBindsToDeclaringSource}.
 *
 * "Identical field" (this module's one comparison) means: same `name`, same
 * `type`, and a deep-equal `e` — ignoring `location`, `annotations` and
 * `accessModifier`, which can legitimately differ across a derivation without
 * the field being a different one. A join hop additionally requires the same
 * `join` relationship and a deep-equal `onExpression`. A field is looked up
 * by its ACTIVE name (`.as` if aliased, else `.name` — see {@link activeName}),
 * never by `.name` alone, so an aliased join member is compared correctly
 * instead of failing to resolve at all.
 *
 * **The default direction is DENY, not pass.** Whenever this module cannot
 * resolve a filter's declaring source, cannot resolve a field along the way,
 * or a resolution walk hits its bound, the caller-facing answer is "cannot
 * prove this binds correctly" — never "must be fine". A condition needs NO
 * check at all only when it is PROVEN to read no field
 * ({@link conditionReadsNoField}) or PROVEN to be the executed struct's own,
 * freshly-authored filter ({@link isOwnFreshFilter}) — never merely because
 * a resolution attempt came up empty.
 */

import {
   isJoined,
   isSourceDef,
   type FieldDef,
   type FilterCondition,
   type ModelDef,
   type SourceDef,
} from "@malloydata/malloy";
import {
   ANCESTOR_WALK_MAX_DEPTH,
   resolveDeclaredSource,
   resolveQuerySourceBase,
} from "./gate_registry_walk";
import { findSourceByOwnAnnotationIdentity } from "./gate_classification";

/**
 * The next struct back in a derivation chain, trying the same three links
 * `collectEntryPointGatesForRoute`/`ancestorGateExprs` walk for the identical
 * purpose, nearest first: {@link resolveDeclaredSource}'s `sourceRegistry`
 * link (real for a plain join or an unmodified rename), then
 * {@link findSourceByOwnAnnotationIdentity} (a modified derivation that still
 * carries its base's annotation NOTES by reference), then
 * {@link resolveQuerySourceBase} (`query.structRef` — the ONE link
 * `resolveDeclaredSource` never carries, because a `query_source` struct
 * (`Z is X -> {...}`) has no `sourceRegistry` entry of its own; see
 * `gate_registry_walk.ts`'s module doc). Omitting this last link is exactly
 * what let a query-source derivation's gate resolve to "no annotation found"
 * here even though it structurally, and correctly, inherits one.
 */
function nextDerivationLink(
   current: SourceDef,
   modelDef: ModelDef,
   seen: ReadonlySet<SourceDef>,
): SourceDef | undefined {
   const declared = resolveDeclaredSource(current, modelDef);
   if (declared.kind === "resolved" && !seen.has(declared.source)) {
      return declared.source;
   }
   if (declared.kind === "none") {
      const viaIdentity = findSourceByOwnAnnotationIdentity(
         current,
         modelDef,
         new Set(seen),
      );
      if (viaIdentity && !seen.has(viaIdentity)) return viaIdentity;
      const viaQuerySource = resolveQuerySourceBase(current, modelDef);
      if (viaQuerySource && !seen.has(viaQuerySource)) return viaQuerySource;
   }
   return undefined;
}

/**
 * Duck-typed rather than imported: Malloy does not re-export `RefSummary`
 * from its package root (the same situation `authorize.ts`'s
 * `CompiledGateCondition` doc describes). Structurally compatible with the
 * real `FilterCondition.refSummary` / `FieldDef.refSummary`, so no cast is
 * needed at a call site that already has one of those.
 */
interface FieldUsageEntryLike {
   path: readonly string[];
}
interface RefSummaryLike {
   fieldUsage?: readonly FieldUsageEntryLike[];
}

/** Bounds the transitive field-usage walk ({@link fieldUsageClosure}) and the
 *  joined-source recursion ({@link assertInheritedSourceFiltersBind}) — large
 *  enough for any real model, small enough that a pathological/cyclic one
 *  fails closed instead of hanging. Exported so a depth-exhaustion test can
 *  reference the real bound rather than hardcoding a number that could drift. */
const MAX_CLOSURE_SIZE = 256;
export const MAX_JOIN_RECURSION_DEPTH = 16;

/** The name a field goes by in ITS CURRENT context: its `as` alias when it
 *  has one, otherwise its intrinsic `name` — same rule Malloy's own
 *  (unexported-from-package-root) `activeName` uses, matched here rather
 *  than imported (`get_context_tool.ts` keeps an identical local copy for
 *  the same reason). A `refSummary.fieldUsage` path segment is always the
 *  VISIBLE name — a join declared under an alias (`join_one: child is
 *  real_source on …`) carries `real_source`'s own `.name` with `.as:
 *  "child"` on the join field itself, so matching on `.name` alone misses
 *  every aliased join and falsely denies a query that never misbound
 *  anything. */
function activeName(f: { name: string; as?: string }): string {
   return f.as ?? f.name;
}

const IGNORED_KEYS = new Set(["location", "annotations", "accessModifier"]);

/** Structural equality ignoring {@link IGNORED_KEYS}. A key-order mismatch
 *  between two otherwise-identical objects would read as "different" here —
 *  that fails CLOSED (an extra denial), never open, so it is not chased. */
function deepEqualIgnoring(a: unknown, b: unknown): boolean {
   return JSON.stringify(strip(a)) === JSON.stringify(strip(b));
}

function strip(value: unknown): unknown {
   if (Array.isArray(value)) return value.map(strip);
   if (value && typeof value === "object") {
      const out: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
         if (IGNORED_KEYS.has(k)) continue;
         out[k] = strip(v);
      }
      return out;
   }
   return value;
}

/** Whether `a` and `b` are the "same" field per this module's definition —
 *  see the module doc. `undefined` on either side (the name didn't resolve)
 *  is never identical: a caller checking a fieldUsage path already confirmed
 *  it resolves in the DECLARING struct, so a lookup miss on the executed
 *  struct means the field genuinely moved. */
function fieldsIdentical(
   a: FieldDef | undefined,
   b: FieldDef | undefined,
): boolean {
   if (!a || !b) return false;
   if (a.name !== b.name || a.type !== b.type) return false;
   if (!deepEqualIgnoring((a as { e?: unknown }).e, (b as { e?: unknown }).e))
      return false;
   if (isJoined(a) || isJoined(b)) {
      if (!isJoined(a) || !isJoined(b)) return false;
      if (a.join !== b.join) return false;
      if (!deepEqualIgnoring(a.onExpression, b.onExpression)) return false;
   }
   return true;
}

/** Resolve a dotted field path (a join path, for a field reached through one
 *  or more joins) against one struct's own `fields`. `undefined` if any
 *  segment is missing, or an intermediate segment is not itself a join. */
function resolveFieldByPath(
   struct: SourceDef | undefined,
   path: readonly string[],
): FieldDef | undefined {
   let current: SourceDef | undefined = struct;
   let field: FieldDef | undefined;
   for (let i = 0; i < path.length; i++) {
      if (!current) return undefined;
      field = current.fields?.find((f) => activeName(f) === path[i]);
      if (!field) return undefined;
      if (i < path.length - 1) {
         if (!isJoined(field)) return undefined;
         current = field as unknown as SourceDef;
      }
   }
   return field;
}

/**
 * Whether `path` resolves to the same field — see the module doc — in
 * `declaring` as it does in `executed`. Every hop along a join path is
 * checked, not just the leaf: a join swapped for a differently-related one
 * partway down the path is exactly the kind of divergence this exists to
 * catch, even when the LEAF field it ends on happens to match.
 */
function fieldPathIdentical(
   declaring: SourceDef,
   executed: SourceDef,
   path: readonly string[],
): boolean {
   let dCur: SourceDef | undefined = declaring;
   let eCur: SourceDef | undefined = executed;
   for (let i = 0; i < path.length; i++) {
      const name = path[i];
      const dField = dCur?.fields?.find((f) => activeName(f) === name);
      const eField = eCur?.fields?.find((f) => activeName(f) === name);
      if (!fieldsIdentical(dField, eField)) return false;
      if (i < path.length - 1) {
         if (!dField || !isJoined(dField) || !eField || !isJoined(eField)) {
            return false;
         }
         dCur = dField as unknown as SourceDef;
         eCur = eField as unknown as SourceDef;
      }
   }
   return true;
}

/**
 * Every field-usage path reachable from `refSummary`, resolved TRANSITIVELY
 * through `declaring`: a filter that reads a dimension (`org_id in $GROUPS`
 * over `#(access_filter) authorized`, say) only lists `authorized` in its own
 * `fieldUsage` — the fields THAT dimension's own expression reads are only
 * discoverable by following its own `refSummary`, one hop at a time. Returns
 * `truncated: true` (never a partial list) when the walk would exceed
 * {@link MAX_CLOSURE_SIZE} — the caller must treat that as "cannot prove
 * this binds correctly" (deny), not "here is everything there is".
 */
function fieldUsageClosure(
   declaring: SourceDef,
   refSummary: RefSummaryLike | undefined,
): { paths: string[][]; truncated: boolean } {
   const seen = new Set<string>();
   const paths: string[][] = [];
   const queue: string[][] = (refSummary?.fieldUsage ?? []).map(
      (u: FieldUsageEntryLike) => [...u.path],
   );
   while (queue.length > 0) {
      if (paths.length >= MAX_CLOSURE_SIZE) return { paths, truncated: true };
      const path = queue.shift();
      if (!path) break;
      const key = path.join("\u0000");
      if (seen.has(key)) continue;
      seen.add(key);
      paths.push(path);
      const field = resolveFieldByPath(declaring, path);
      const nested = (field as { refSummary?: RefSummaryLike } | undefined)
         ?.refSummary;
      // A nested `FieldUsageEntry.path` is relative to the struct `field`
      // ITSELF lives on (`path`'s join prefix, i.e. everything but its own
      // last segment) — never relative to `declaring` directly. A field
      // reached through a join (`path = ["childtable", "name"]`) whose own
      // nested usage is a bare self-reference (`u.path = ["name"]`, which a
      // plain physical column can carry trivially) would otherwise queue as
      // `["name"]` alone: unresolvable against `declaring` (there is no
      // top-level "name" on `parent`), which reads as "the field moved"
      // rather than "this was never `declaring`-relative to begin with".
      // Re-qualifying against `path`'s own prefix keeps every queued entry
      // in the same coordinate space `resolveFieldByPath`/`fieldPathIdentical`
      // already assume everywhere else in this module.
      const prefix = path.slice(0, -1);
      for (const u of nested?.fieldUsage ?? []) {
         queue.push([...prefix, ...u.path]);
      }
   }
   return { paths, truncated: false };
}

/**
 * Assert that `condition` (declared, or landed, against `declaringStruct`)
 * still reads the SAME fields when evaluated against `executedStruct`. Throws
 * a plain `Error` — never `AccessDeniedError` — so every call site decides
 * its own caller-facing message; the caller is expected to catch and deny.
 */
export function assertFilterConditionBindsToDeclaringSource(
   declaringStruct: SourceDef,
   executedStruct: SourceDef,
   condition: FilterCondition,
): void {
   const { paths, truncated } = fieldUsageClosure(
      declaringStruct,
      condition.refSummary,
   );
   if (truncated) {
      throw new Error(
         "a row-security filter's field-usage closure exceeded the resolution bound",
      );
   }
   for (const path of paths) {
      if (!fieldPathIdentical(declaringStruct, executedStruct, path)) {
         throw new Error(
            `a row-security filter references \`${path.join(".")}\`, which resolves to a different field on the executed source`,
         );
      }
   }
}

/**
 * The `#(filter)` analogue of {@link assertFilterConditionBindsToDeclaringSource}
 * for a bare dimension NAME rather than a compiled `FilterCondition` — there
 * is no condition object here to read a `refSummary` off directly, since the
 * value being injected is a runtime parameter, not a stored expression.
 * Seeds {@link fieldUsageClosure} with a single top-level path (`[dimension]`)
 * and walks its transitive closure exactly the same way: a dimension defined
 * as an alias of another field (`dimension: safe_col is val`) has its OWN
 * `.e` unchanged by a caller renaming `val` out from under it, so comparing
 * only `[dimension]` at the top level sees no difference — the closure is
 * what follows into `val` itself and catches the rebind.
 */
export function assertFilterDimensionBindsToDeclaringSource(
   declaringStruct: SourceDef,
   executedStruct: SourceDef,
   dimension: string,
): void {
   const { paths, truncated } = fieldUsageClosure(declaringStruct, {
      fieldUsage: [{ path: [dimension] }],
   });
   if (truncated) {
      throw new Error(
         "a #(filter) dimension's field-usage closure exceeded the resolution bound",
      );
   }
   for (const path of paths) {
      if (!fieldPathIdentical(declaringStruct, executedStruct, path)) {
         throw new Error(
            `a #(filter) dimension references \`${path.join(".")}\`, which resolves to a different field on the executed source`,
         );
      }
   }
}

/**
 * Whether `a` sits strictly before `b` in a document (line first, then
 * character) — mirrors `source_extraction.ts`'s identical helper (kept
 * self-contained here rather than imported: that module is bundled into the
 * package-load worker and stays free of anything not needed there).
 */
function isEarlierPosition(
   a: { line: number; character: number },
   b: { line: number; character: number },
): boolean {
   return a.line < b.line || (a.line === b.line && a.character < b.character);
}

/** Duck-typed `Note`/`AnnotationsDef` — same reason every other `*Like`
 *  interface in this module is: no need to import Malloy's real types just
 *  to read a few fields off something a caller already has typed loosely. */
interface NoteLike {
   text: string;
   at?: DocumentLocationLike;
}
interface AnnotationsDefLike {
   blockNotes?: readonly NoteLike[];
   notes?: readonly NoteLike[];
   inherits?: AnnotationsDefLike;
}

/** Every note `annotations` carries at EVERY level of its `inherits` chain,
 *  own-level first — not just the top one. Malloy demotes a struct's
 *  inherited annotations to `annotations.inherits` the moment the struct
 *  declares ANY annotation of its own (own-level, ANY kind, not only
 *  `#(access_filter)`/`#(authorize)`/`#(filter)` — see `gate_registry_walk.ts`'s
 *  module doc for the general rule), so a derived source's OWN top-level
 *  notes can be missing the very note that produced a filter it merely
 *  inherited. Reading only top-level notes then makes {@link findNoteOwner}
 *  search for the WRONG note (or nothing), and can land on the derived
 *  struct itself rather than the true declaring ancestor. Bounded the same
 *  as every other ancestor walk here, for the same reason: a resolver that
 *  can loop forever on malformed IR is worse than one that gives up. */
function allAnnotationNotes(
   annotations: AnnotationsDefLike | undefined,
   depth = 0,
): NoteLike[] {
   if (!annotations || depth > ANCESTOR_WALK_MAX_DEPTH) return [];
   return [
      ...(annotations.blockNotes ?? []),
      ...(annotations.notes ?? []),
      ...allAnnotationNotes(annotations.inherits, depth + 1),
   ];
}

/** A struct's OWN-level notes only — never its `inherits` chain. This is
 *  the "did THIS struct itself write this note" test {@link findNoteOwner}
 *  needs for each candidate; widening it to the chain would make every
 *  inheriting derivation match too, which is exactly the ambiguity
 *  {@link findNoteOwner}'s doc warns about. */
function ownLevelNotes(
   annotations: AnnotationsDefLike | undefined,
): NoteLike[] {
   return [...(annotations?.blockNotes ?? []), ...(annotations?.notes ?? [])];
}

/**
 * The top-level struct that ACTUALLY WROTE one of `notes` — by LOCATION,
 * never by object identity or `sourceRegistry`. Mirrors
 * `source_extraction.ts`'s `considerNoteOwner`, kept as its own compact copy
 * for the same reason `isEarlierPosition` above is.
 *
 * Location, not identity: a shared-note identity check can't tell the true
 * declarer from a fellow inheritor sharing the same note by reference, and
 * can land on a sibling derivation instead — denying a perfectly ungated
 * query. Own-level notes only ({@link ownLevelNotes}), never `inherits`, for
 * the same reason.
 */
function findNoteOwner(
   notes: readonly NoteLike[],
   modelDef: ModelDef,
): SourceDef | undefined {
   let best: SourceDef | undefined;
   for (const note of notes) {
      if (!note.at) continue;
      for (const value of Object.values(modelDef.contents)) {
         if (!isSourceDef(value) || !value.location) continue;
         if (!ownLevelNotes(value.annotations).includes(note)) continue;
         if (value.location.url !== note.at.url) continue;
         if (
            !best?.location ||
            isEarlierPosition(
               value.location.range.start,
               best.location.range.start,
            )
         ) {
            best = value;
         }
      }
   }
   return best;
}

/**
 * {@link findNoteOwner} over EVERY note `struct` carries, own-level plus
 * `inherits` (see {@link allAnnotationNotes}). Falls back to the structural
 * walk {@link findFilterOrigin} uses ({@link nextDerivationLink}) when the
 * note search is empty — needed for a `query_source` (`Z is X -> {...}`),
 * which carries no `annotations` at all despite genuinely inheriting `X`'s
 * gate (see `gate_registry_walk.ts`'s module doc). `resolveSibling` is the
 * last resort after both fail — see {@link findSiblingDeclaringSource}.
 */
function findAnnotationDeclaringSource(
   struct: SourceDef,
   modelDef: ModelDef,
   resolveSibling?: SiblingModelDefResolver,
): SourceDef | undefined {
   const notes = allAnnotationNotes(struct.annotations);
   const viaNotes = findNoteOwner(notes, modelDef);
   if (viaNotes) return viaNotes;
   let current = struct;
   const seen = new Set<SourceDef>([struct]);
   for (let depth = 0; depth < ANCESTOR_WALK_MAX_DEPTH; depth++) {
      const next = nextDerivationLink(current, modelDef, seen);
      if (!next) break;
      if (ownLevelNotes(next.annotations).length > 0) return next;
      seen.add(next);
      current = next;
   }
   for (const note of notes) {
      const sibling = findSiblingAnnotationDeclaringSource(
         note,
         resolveSibling,
      );
      if (sibling) return sibling;
   }
   return undefined;
}

/**
 * {@link findNoteOwner} narrowed to the ONE note that produced `filter` — the
 * `#(filter)` analogue of {@link findAnnotationDeclaringSource}. Needed
 * because `filterMap` (`source_extraction.ts`) keys an entry under every
 * INHERITING derivation's own name too, so `getFilters` can hand back a
 * derived, possibly-misbound source's own name as "the declaring source" —
 * comparing it to itself would always look identical. Matches notes by
 * PARSED definition, not object identity, since the caller only has the
 * parsed `FilterDefinition`, not the original `Note`. Falls back to the
 * structural walk and then `resolveSibling`, same as
 * {@link findAnnotationDeclaringSource}.
 */
export function findFilterAnnotationDeclaringSource(
   struct: SourceDef,
   modelDef: ModelDef,
   filter: {
      readonly name: string;
      readonly dimension: string;
      readonly type: string;
   },
   parseAnnotation: (text: string) => {
      name: string;
      dimension: string;
      type: string;
   } | null,
   resolveSibling?: SiblingModelDefResolver,
): SourceDef | undefined {
   const matches = (notes: readonly NoteLike[]) =>
      notes.filter((note) => {
         let parsed: { name: string; dimension: string; type: string } | null;
         try {
            parsed = parseAnnotation(note.text);
         } catch {
            return false;
         }
         return (
            !!parsed &&
            parsed.dimension === filter.dimension &&
            parsed.type === filter.type &&
            parsed.name === filter.name
         );
      });
   const matched = matches(allAnnotationNotes(struct.annotations));
   const viaNotes = findNoteOwner(matched, modelDef);
   if (viaNotes) return viaNotes;
   let current = struct;
   const seen = new Set<SourceDef>([struct]);
   for (let depth = 0; depth < ANCESTOR_WALK_MAX_DEPTH; depth++) {
      const next = nextDerivationLink(current, modelDef, seen);
      if (!next) break;
      if (matches(ownLevelNotes(next.annotations)).length > 0) return next;
      seen.add(next);
      current = next;
   }
   for (const note of matched) {
      const sibling = findSiblingAnnotationDeclaringSource(
         note,
         resolveSibling,
      );
      if (sibling) return sibling;
   }
   return undefined;
}

/**
 * Assert that a GRAFTED `#(access_filter)`/`#(authorize)` condition still
 * binds to the same fields at `executedStruct` (the entry point actually
 * queried) as it did at the struct whose OWN annotation wrote it.
 *
 * Unlike a plain author `where:`, a grafted condition is not the same object
 * copied down an `extend` chain — {@link resolveGraftTarget} compiles the
 * annotation's filter text fresh onto the entry point's own field space, so
 * there is no shared object to walk back through. The declaring struct is
 * instead found by {@link findAnnotationDeclaringSource}, by NOTE identity.
 *
 * Always runs the comparison, even when nothing was derived: a fresh compile
 * allocates its own struct instances, so `entryPointStruct` and
 * `executedStruct` are essentially never reference-equal even in the
 * unmodified case — a shortcut permissive enough to fire there would also
 * skip the misbindable case this exists to catch (`resolveGraftTarget`
 * plants the condition on an ancestor of the actual run target whenever the
 * caller's own derivation has no `modelDef.contents` entry of its own).
 *
 * THROWS when `entryPointStruct` itself or its declaring struct cannot be
 * resolved — an unprovable gate denies, it is never treated as nothing to
 * check.
 */
export function assertGraftedGateBindsToDeclaringSource(
   entryPointStruct: SourceDef | undefined,
   executedStruct: SourceDef,
   condition: FilterCondition,
   modelDef: ModelDef,
   resolveSibling?: SiblingModelDefResolver,
): void {
   if (!entryPointStruct) {
      throw new Error(
         "a row-security gate's entry point could not be resolved",
      );
   }
   const declaring = findAnnotationDeclaringSource(
      entryPointStruct,
      modelDef,
      resolveSibling,
   );
   if (!declaring) {
      throw new Error(
         "a row-security gate's declaring source could not be resolved",
      );
   }
   assertFilterConditionBindsToDeclaringSource(
      declaring,
      executedStruct,
      condition,
   );
}

/** Loose structural shape of `DocumentLocation` — duck-typed for the same
 *  reason {@link RefSummaryLike} is: no need to import it just to read two
 *  fields off something a caller already has typed loosely. */
export interface DocumentLocationLike {
   url: string;
   range: {
      start: { line: number; character: number };
      end: { line: number; character: number };
   };
}

/** Whether `outer` fully contains `inner` — same file, `inner` starting no
 *  earlier and ending no later. */
function locationContains(
   outer: DocumentLocationLike,
   inner: DocumentLocationLike,
): boolean {
   if (outer.url !== inner.url) return false;
   if (isEarlierPosition(inner.range.start, outer.range.start)) return false;
   if (isEarlierPosition(outer.range.end, inner.range.end)) return false;
   return true;
}

/** A rough, same-file-only span size — large enough that line differences
 *  always dominate character ones, which is all {@link findConditionOriginByLocation}
 *  needs to prefer the narrowest (most specific) containing struct over a
 *  wider ancestor that also happens to contain the same position. */
function spanSize(range: DocumentLocationLike["range"]): number {
   return (
      (range.end.line - range.start.line) * 100_000 +
      (range.end.character - range.start.character)
   );
}

/**
 * The struct whose OWN source span CONTAINS `at` — used only as a last
 * resort, when a filter's origin cannot be found any other way: a plain,
 * UNANNOTATED `where:` inherited through a MODIFIED derivation has no
 * surviving `referenceID`/`sourceRegistry` link (cleared on modification,
 * see {@link resolveDeclaredSource}) and no annotation note to
 * identity-match through (see {@link findSourceByOwnAnnotationIdentity}) —
 * so there is nothing structural left to walk. But a field reference INSIDE
 * a condition's own compiled expression still carries the location it was
 * PARSED at (`{node:"field", path, at}`), however many times the condition
 * object itself is later copied unchanged down an `extend` chain — the same
 * fact {@link findAnnotationDeclaringSource} relies on for annotation notes,
 * applied here to a plain filter expression instead. That position sits
 * INSIDE the `source:` block that authored it (a `where:` is textually
 * nested in its enclosing source), so the struct whose own `.location` span
 * contains it is the declaring struct — the narrowest (smallest) containing
 * span wins, in case one source's span is textually nested inside another's.
 *
 * Guarded by the caller's `next.filterList?.includes(condition)` check same
 * as every other candidate here: a location match that turns out not to
 * actually own this condition object is rejected there, not accepted on
 * position alone.
 */
function findConditionOriginByLocation(
   modelDef: ModelDef,
   at: DocumentLocationLike,
): SourceDef | undefined {
   let best: SourceDef | undefined;
   let bestSpan = Infinity;
   for (const value of Object.values(modelDef.contents)) {
      if (!isSourceDef(value) || !value.location) continue;
      if (!locationContains(value.location as DocumentLocationLike, at)) {
         continue;
      }
      const span = spanSize(value.location.range);
      if (span < bestSpan) {
         best = value;
         bestSpan = span;
      }
   }
   return best;
}

/**
 * Resolves the `ModelDef` the PACKAGE independently compiled for the file
 * named by a `file://` URL — every `.malloy`/`.malloynb` file a package
 * serves is its own top-level `Model` (see `Package`'s model discovery),
 * regardless of whether anything else imports it, and regardless of whether
 * the SERVED model promoted it to a `modelDef.contents` entry of its own. A
 * selective `import { name } from "file"` or a notebook cell's narrower
 * per-cell compile can leave an ancestor file with no such entry, which is
 * exactly when `modelDef.contents`-based resolution above comes up empty.
 * Wired up by `Package.applySiblingModelResolverToModels` (a Model held
 * outside a Package has none, and this simply always misses for it — the
 * same conservative "cannot prove, so deny" as before this existed).
 */
export type SiblingModelDefResolver = (url: string) => ModelDef | undefined;

/**
 * The declaring struct for a plain filter CONDITION's field-usage location
 * `at`, via the sibling compile, when nothing in the SERVED model's own
 * `modelDef.contents` covers it. Reuses
 * {@link findConditionOriginByLocation}'s span-containment test against the
 * sibling's `ModelDef` — same test, different compile: a condition's field
 * reference is textually nested INSIDE the `source:` block that wrote it,
 * same as it is in the served compile. The binding check that follows this
 * doesn't care which compile produced the struct (it compares fields
 * structurally, not by object identity), so a wrong candidate simply fails
 * to bind rather than being trusted on identity alone; there is no
 * reference-identity re-check to relax here the way {@link findFilterOrigin}'s
 * own walk needs one.
 */
function findSiblingDeclaringSource(
   at: DocumentLocationLike | undefined,
   resolveSibling: SiblingModelDefResolver | undefined,
): SourceDef | undefined {
   if (!at || !resolveSibling) return undefined;
   const siblingModelDef = resolveSibling(at.url);
   if (!siblingModelDef) return undefined;
   return findConditionOriginByLocation(siblingModelDef, at);
}

/**
 * The declaring struct for an ANNOTATION note, via the sibling compile —
 * `findAnnotationDeclaringSource`/`findFilterAnnotationDeclaringSource`'s own
 * analogue of {@link findSiblingDeclaringSource}. A note's `.at` sits BEFORE
 * the `source:` keyword it annotates, not inside the struct's own location
 * span, so {@link findConditionOriginByLocation}'s containment test does not
 * apply here the way it does for a condition's field reference — and the
 * served-model scan's own proof (`ownLevelNotes(value.annotations).includes(note)`,
 * {@link findNoteOwner}) is reference identity, which can never match a note
 * object from a wholly separate compile.
 *
 * What DOES survive across two independent compiles of the same file is
 * TEXT POSITION: Malloy's parse is deterministic, so the same source
 * produces a note at the exact same `url`+`range` every time. Scans the
 * sibling's own top-level structs for whichever one's OWN-LEVEL notes carry
 * one at that same position — the sibling compile's equivalent of "this
 * struct is who wrote it".
 */
function findSiblingAnnotationDeclaringSource(
   note: NoteLike | undefined,
   resolveSibling: SiblingModelDefResolver | undefined,
): SourceDef | undefined {
   if (!note?.at || !resolveSibling) return undefined;
   const siblingModelDef = resolveSibling(note.at.url);
   if (!siblingModelDef) return undefined;
   for (const value of Object.values(siblingModelDef.contents)) {
      if (!isSourceDef(value)) continue;
      for (const candidate of ownLevelNotes(value.annotations)) {
         if (
            candidate.at &&
            candidate.at.url === note.at.url &&
            JSON.stringify(candidate.at.range) === JSON.stringify(note.at.range)
         ) {
            return value;
         }
      }
   }
   return undefined;
}

/** The first `.at` location carried by `condition`'s own field-usage list —
 *  where its earliest-listed field reference was originally parsed. Any one
 *  entry is enough: they all sit inside the same declaring `source:` block,
 *  since a condition is authored as a single expression in one place. */
function firstFieldUsageLocation(
   condition: FilterCondition,
): DocumentLocationLike | undefined {
   const refSummary = (condition as { refSummary?: RefSummaryLike }).refSummary;
   const first = refSummary?.fieldUsage?.[0] as
      | { at?: DocumentLocationLike }
      | undefined;
   return first?.at;
}

/**
 * The struct in `struct`'s own derivation chain that owns `condition` — the
 * farthest ancestor whose OWN `filterList` still contains `condition` BY
 * REFERENCE. Malloy never mutates a `FilterCondition` object once built — an
 * unmodified `extend` only ever spreads the array, never the elements
 * (`refined-source.js`) — so reference identity survives the copy at every
 * step, making this the same "not by name, not by sourceID" discriminator
 * `resolveGraftTarget` already uses for the analogous graft-target question.
 *
 * Walks THREE links, nearest first: {@link resolveDeclaredSource}'s
 * `sourceRegistry` walk (real for a plain join or an unmodified `source: a is
 * b`), then {@link findSourceByOwnAnnotationIdentity} (real for a MODIFIED
 * derivation that carries an authorize-tagged annotation, since Malloy
 * copies that note object onto the deriving struct BY REFERENCE regardless
 * of modification — see `gate_registry_walk.ts`'s module doc), then
 * {@link findConditionOriginByLocation} (the one link that survives a
 * MODIFIED derivation with NO annotation at all — 0.0.432 clears
 * `referenceID` on any modification and there is no note to identity-match,
 * but the condition's OWN field references still carry the location they
 * were parsed at, which is enough).
 *
 * Returns `struct` itself when none of the three links resolve AND
 * `resolveSibling` (see {@link findSiblingDeclaringSource}) also has nothing
 * — the caller ({@link assertInheritedSourceFiltersBind}) does NOT treat
 * that as "nothing to check": only a location-proven fresh filter (checked
 * separately, before this is even called) means that. An unresolved origin
 * here means "cannot prove where this came from", and the caller denies on
 * it.
 */
function findFilterOrigin(
   struct: SourceDef,
   condition: FilterCondition,
   modelDef: ModelDef | undefined,
   resolveSibling?: SiblingModelDefResolver,
): SourceDef {
   let origin = struct;
   let current = struct;
   const seen = new Set<SourceDef>([struct]);
   for (let depth = 0; depth < ANCESTOR_WALK_MAX_DEPTH; depth++) {
      let next: SourceDef | undefined = modelDef
         ? nextDerivationLink(current, modelDef, seen)
         : undefined;
      if (!next && modelDef) {
         const at = firstFieldUsageLocation(condition);
         if (at) {
            const candidate = findConditionOriginByLocation(modelDef, at);
            if (candidate && !seen.has(candidate)) next = candidate;
         }
      }
      if (!next || !next.filterList?.includes(condition)) break;
      origin = next;
      seen.add(next);
      current = next;
   }
   if (origin === struct) {
      const sibling = findSiblingDeclaringSource(
         firstFieldUsageLocation(condition),
         resolveSibling,
      );
      if (sibling) return sibling;
   }
   return origin;
}

/** Whether `condition` reads any field at all — a `where: true`/`where:
 *  false` (or any filter whose expression names no field) has nothing that
 *  could have moved, so it is safe unconditionally, regardless of whether
 *  its origin resolves. Checked on the TOP-LEVEL `fieldUsage` list only
 *  (never the transitive closure {@link fieldUsageClosure} computes) — an
 *  empty top level means an empty closure too, since the closure only ever
 *  ADDS entries reachable FROM a non-empty seed. */
function conditionReadsNoField(condition: FilterCondition): boolean {
   const refSummary = (condition as { refSummary?: RefSummaryLike }).refSummary;
   return !refSummary?.fieldUsage || refSummary.fieldUsage.length === 0;
}

/**
 * Two structural, POSITIVE signals a condition is genuinely fresh (never a
 * negative inference from "unrecognized" — see {@link isOwnFreshFilter}).
 * `queryLocation` (`prepared._query.location`, set for every run) covers a
 * NAMED query's anonymous inline extend, which does not always get its own
 * `.location` (can carry its base's forward unchanged). `compiledUrl` is the
 * synthetic `internal://` URL a caller/cell compile gets; `undefined` for a
 * NAMED run, which compiles no such text of its own.
 */
export interface FreshnessContext {
   compiledUrl?: string;
   queryLocation?: DocumentLocationLike;
}

/**
 * Whether `condition` is `struct`'s OWN, freshly-authored filter — never
 * merely because {@link findFilterOrigin} failed to resolve anything else.
 * "Unrecognized by `modelDef.contents`" is NOT proof: an inherited condition
 * from a narrower per-cell or selective-import compile is equally
 * unrecognized without being fresh. Proven instead by parse-location
 * containment in `struct`'s own span, or in `queryLocation`, or by
 * `at.url === compiledUrl`.
 */
function isOwnFreshFilter(
   struct: SourceDef,
   condition: FilterCondition,
   freshness: FreshnessContext,
): boolean {
   const at = firstFieldUsageLocation(condition);
   if (!at) return false;
   const structLocation = (struct as { location?: DocumentLocationLike })
      .location;
   if (structLocation && locationContains(structLocation, at)) return true;
   if (
      freshness.queryLocation &&
      locationContains(freshness.queryLocation, at)
   ) {
      return true;
   }
   return !!freshness.compiledUrl && at.url === freshness.compiledUrl;
}

/**
 * Assert every INHERITED entry in `struct.filterList` — an author `where:`
 * carried in from a base, or a grafted `#(access_filter)` condition — still
 * binds to the same fields it did where it was declared, then recurse into
 * every joined source reachable from `struct`. A condition that reads no
 * field or is genuinely `struct`'s OWN fresh filter is skipped; every other
 * condition must resolve to a declaring source and prove binding — an
 * unresolvable origin DENIES, never "must be the struct's own". That default
 * is the gap a caller-modified struct (not itself a `modelDef.contents`
 * entry) can otherwise exploit: "can't tell" and "fine" must not read the
 * same here. Runs for every executed struct regardless of whether an
 * `#(authorize)`/`#(access_filter)` annotation was ever involved — a plain
 * filtered source is just as misbindable and has no graft step to hook.
 *
 * `visited` bounds a genuine CYCLE with a silent return (already checked on
 * this walk). `depth` bounds how deep a walk may go before giving up on ever
 * reaching one; exhausting it THROWS rather than returning, because unlike a
 * cycle it proves nothing — a struct beyond the bound was never checked.
 */
export function assertInheritedSourceFiltersBind(
   struct: SourceDef,
   modelDef: ModelDef | undefined,
   freshness: FreshnessContext = {},
   resolveSibling: SiblingModelDefResolver | undefined = undefined,
   depth = 0,
   visited: Set<SourceDef> = new Set(),
   alreadyProven: ReadonlySet<FilterCondition> = new Set(),
): void {
   if (visited.has(struct)) return;
   if (depth > MAX_JOIN_RECURSION_DEPTH) {
      throw new Error(
         "row-security filter recursion exceeded the join-depth resolution bound",
      );
   }
   visited.add(struct);
   for (const condition of struct.filterList ?? []) {
      // A grafted condition's `.at` is where the graft compiled it, never
      // inside `struct`'s own span even when the gate genuinely IS
      // `struct`'s own — so this walk's location-based classification can't
      // read it, and shouldn't: the caller already proved-or-denied it by
      // annotation-note identity (`assertGraftedGateBindsToDeclaringSource`).
      if (alreadyProven.has(condition)) continue;
      if (conditionReadsNoField(condition)) continue;
      // Runs BEFORE the fresh-filter check: an unnamed inline `extend {}`
      // does not always get its own `.location` (it can carry its BASE's
      // forward unchanged), which would make that check wrongly "prove" a
      // condition the struct never wrote. `findFilterOrigin` only returns
      // something other than `struct` when an ancestor genuinely carries
      // this exact condition object, so it is the safe one to trust first.
      const origin = findFilterOrigin(
         struct,
         condition,
         modelDef,
         resolveSibling,
      );
      if (origin !== struct) {
         assertFilterConditionBindsToDeclaringSource(origin, struct, condition);
         continue;
      }
      if (isOwnFreshFilter(struct, condition, freshness)) continue;
      throw new Error(
         "a row-security filter's declaring source could not be resolved",
      );
   }
   // `struct.fields` absent means `struct` isn't a well-formed compiled
   // `SourceDef` at all (a test double, or a caller that resolved something
   // other than a real struct) — nothing further to walk, and the
   // AUTHORITATIVE own-source gate above already covers the denial case for
   // an unresolvable target, so this skips rather than crashing.
   for (const field of struct.fields ?? []) {
      if (isJoined(field) && isSourceDef(field)) {
         assertInheritedSourceFiltersBind(
            field as unknown as SourceDef,
            modelDef,
            freshness,
            resolveSibling,
            depth + 1,
            visited,
            alreadyProven,
         );
      }
   }
}

/** Exported for `./filter.ts`'s `#(filter)` injection check and tests — see
 *  the module doc. */
export { fieldPathIdentical };
