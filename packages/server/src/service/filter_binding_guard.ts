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
 * The top-level struct that ACTUALLY WROTE one of `notes`, among every
 * `SourceDef` in `modelDef.contents` — by LOCATION, never by object identity
 * or by `sourceRegistry`. Mirrors `source_extraction.ts`'s
 * `considerNoteOwner` (see its doc for the full mechanism and why location is
 * the only thing that works here); kept as its own compact copy rather than
 * imported for the same reason `isEarlierPosition` above is.
 *
 * Every `AnnotationNote` carries `at`, recording where it was PARSED — this
 * travels with the note wherever Malloy copies it BY REFERENCE onto an
 * inheriting derivation, so it always names the ORIGINAL `source:` line,
 * never a derivation's. Filtering candidates to `location.url === note.at.url`
 * therefore already excludes every derivation, same-file or cross-file; the
 * earliest-position tie-break only matters for two genuine same-file
 * candidates (a block-list sharing one note across siblings).
 *
 * This is what keeps {@link findFilterOrigin}'s "not by name, not by
 * sourceID" identity discipline from misfiring here: the identity-only test
 * `resolveGraftTarget`'s own ancestor walk uses (does some OTHER struct
 * share this note object) is silent about WHICH shared struct is the true
 * declarer versus a fellow inheritor — calling it on the declaring struct
 * ITSELF can return a SIBLING derivation instead, comparing the correct
 * struct against the wrong one and denying a perfectly ungated query.
 * Candidates are checked against their OWN-level notes only ({@link
 * ownLevelNotes}) — never their own `inherits` chain — for that same reason:
 * an inheriting sibling would otherwise match too.
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
 * {@link findNoteOwner} over EVERY note `struct` carries — own-level plus its
 * full `inherits` chain (see {@link allAnnotationNotes}) — for the general
 * "what struct actually wrote whatever `struct` is tagged with" question
 * `assertGraftedGateBindsToDeclaringSource` asks.
 *
 * That `.inherits` chain is EMPTY for a `query_source` (`Z is X -> {...}`):
 * Malloy carries no `annotations` at all on this shape (see
 * `gate_registry_walk.ts`'s module doc), even though it genuinely inherits
 * `X`'s gate — `Model.collectEntryPointGates` reaches it structurally, via
 * `query.structRef`, a completely different link than `annotations.inherits`.
 * When the note search above comes up empty, this falls back to the SAME
 * structural walk {@link findFilterOrigin} uses ({@link nextDerivationLink}),
 * stopping at the first ancestor that authored a note of its own — that
 * ancestor is the true declarer even though no annotation object was ever
 * copied onto `struct` to prove it by reference.
 */
function findAnnotationDeclaringSource(
   struct: SourceDef,
   modelDef: ModelDef,
): SourceDef | undefined {
   const viaNotes = findNoteOwner(
      allAnnotationNotes(struct.annotations),
      modelDef,
   );
   if (viaNotes) return viaNotes;
   let current = struct;
   const seen = new Set<SourceDef>([struct]);
   for (let depth = 0; depth < ANCESTOR_WALK_MAX_DEPTH; depth++) {
      const next = nextDerivationLink(current, modelDef, seen);
      if (!next) return undefined;
      if (ownLevelNotes(next.annotations).length > 0) return next;
      seen.add(next);
      current = next;
   }
   return undefined;
}

/**
 * {@link findNoteOwner} narrowed to the ONE note that produced `filter` — the
 * `#(filter)` analogue of {@link findAnnotationDeclaringSource}, needed
 * because `source_extraction.ts`'s `filterMap` build ALSO walks a struct's
 * full `inherits` chain (its own `while (cur) { …; cur = cur.inherits; }`),
 * so a DERIVED source that only ever INHERITS a `#(filter)` — never
 * declaring one of its own — still gets its own `filterMap` entry, keyed
 * under ITS name. `resolveFilterSource`/`getFilters` can then hand back that
 * derived, possibly-misbound source's own name as "the declaring source",
 * which would compare the misbound struct's fields to ITSELF — always
 * "identical". Matching notes by their PARSED definition (dimension, type,
 * name) rather than by object identity, since the caller only has the
 * parsed `FilterDefinition` `filterMap` stored, not the original `Note`.
 *
 * Falls back to the same structural derivation walk
 * {@link findAnnotationDeclaringSource} does when `findNoteOwner`'s
 * `modelDef.contents` scan comes up empty: `resolveDeclaredSource`'s
 * `sourceRegistry` link can resolve to a struct directly (`entry.type !==
 * "source_registry_reference"`), which need not ALSO be independently
 * enumerable as its own `modelDef.contents` key — an imported base a
 * multi-file model re-exposes only under a derived name, say. Without this,
 * an unmodified `extend {}` of such a base (nothing to have drifted from at
 * all) would deny for the wrong reason: not a misbind, but the true declarer
 * simply isn't reachable by scanning `modelDef.contents` alone.
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
   const viaNotes = findNoteOwner(
      matches(allAnnotationNotes(struct.annotations)),
      modelDef,
   );
   if (viaNotes) return viaNotes;
   let current = struct;
   const seen = new Set<SourceDef>([struct]);
   for (let depth = 0; depth < ANCESTOR_WALK_MAX_DEPTH; depth++) {
      const next = nextDerivationLink(current, modelDef, seen);
      if (!next) return undefined;
      if (matches(ownLevelNotes(next.annotations)).length > 0) return next;
      seen.add(next);
      current = next;
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
): void {
   if (!entryPointStruct) {
      throw new Error(
         "a row-security gate's entry point could not be resolved",
      );
   }
   const declaring = findAnnotationDeclaringSource(entryPointStruct, modelDef);
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
interface DocumentLocationLike {
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
 * Returns `struct` itself when none of the three links resolve — the caller
 * ({@link assertInheritedSourceFiltersBind}) does NOT treat that as "nothing
 * to check": only a location-proven fresh filter (checked separately, before
 * this is even called) means that. An unresolved origin here means "cannot
 * prove where this came from", and the caller denies on it.
 */
function findFilterOrigin(
   struct: SourceDef,
   condition: FilterCondition,
   modelDef: ModelDef | undefined,
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
 * Whether `condition` is `struct`'s OWN, freshly-authored filter — never
 * merely because {@link findFilterOrigin} failed to resolve anything else.
 * Proven either of two ways:
 *
 * - `condition`'s parse location sits INSIDE `struct`'s own span — true for a
 *   NAMED derivation (`source: mine is X extend { where: … }`), which gets
 *   its own `.location` covering exactly the text that wrote the filter.
 * - `condition`'s parse location's URL is EXACTLY `compiledUrl` — the
 *   synthetic URL Malloy compiled the currently-executing request's own text
 *   under (an ad-hoc query's `internal://query/…`, a notebook cell's
 *   `internal://extendModel/…` or `internal://loadModel/…`). This is a
 *   POSITIVE identification of "this was parsed as part of THIS request's
 *   own submitted text", not a negative inference from absence: checking
 *   instead "is `at.url` unrecognized by `modelDef.contents`" is unsound —
 *   an inherited condition from a base file that a narrower per-cell (or
 *   selectively-`import { name } from …`d) compile simply never promoted to
 *   a top-level `contents` entry is EQUALLY "unrecognized", and is not
 *   remotely fresh. `compiledUrl` is read once per request, from the exact
 *   compile the struct itself came from (see `resolveRunTargetStruct` in
 *   `./model.ts`), so it can never accidentally match an ancestor file's own
 *   URL.
 */
function isOwnFreshFilter(
   struct: SourceDef,
   condition: FilterCondition,
   compiledUrl: string | undefined,
): boolean {
   const at = firstFieldUsageLocation(condition);
   if (!at) return false;
   const structLocation = (struct as { location?: DocumentLocationLike })
      .location;
   if (structLocation && locationContains(structLocation, at)) return true;
   return !!compiledUrl && at.url === compiledUrl;
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
   compiledUrl: string | undefined = undefined,
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
      const origin = findFilterOrigin(struct, condition, modelDef);
      if (origin !== struct) {
         assertFilterConditionBindsToDeclaringSource(origin, struct, condition);
         continue;
      }
      if (isOwnFreshFilter(struct, condition, compiledUrl)) continue;
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
            compiledUrl,
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
