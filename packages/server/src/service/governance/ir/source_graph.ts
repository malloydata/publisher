// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The source graph governance walks: the derivation hops from one struct to
 * the structs it was built from. Three hops, read off compiler-set IR only:
 *
 *  - the declaration a struct derives from, via `ModelDef.sourceRegistry`
 *    ({@link resolveDeclaredSource});
 *  - a `query_source`'s base, via `query.structRef`
 *    ({@link resolveQuerySourceBase});
 *  - a `query_source`'s resolved composite member, via
 *    `query.compositeResolvedSourceDef` ({@link resolveCompositeResolvedBase}).
 *
 * Moved verbatim from `gate_registry_walk.ts`. Bundled into the package-load
 * worker: keep imports to `@malloydata/malloy` types.
 */

import { isSourceDef, type ModelDef, type SourceDef } from "@malloydata/malloy";

/**
 * The declared source a struct was created from, via `ModelDef.sourceRegistry`
 * (`referenceID` first, then `sourceID`).
 *
 * Collapsing the last two outcomes would fail open:
 *  - `resolved`: the declaration this struct derives from.
 *  - `none`: nothing to follow (no id, no registry, or the entry is `struct`
 *    itself). The common case for a top-level source; means "no gate here".
 *  - `unresolvable`: an entry exists but does not yield a usable `SourceDef`.
 *    The base exists and cannot be read, so the caller denies.
 */
export function resolveDeclaredSource(
   struct: SourceDef,
   modelDef: ModelDef | undefined,
):
   | { kind: "resolved"; source: SourceDef }
   | { kind: "none" }
   | { kind: "unresolvable" } {
   if (!modelDef) return { kind: "none" };
   let sawBrokenEntry = false;
   for (const id of [struct.referenceID, struct.sourceID]) {
      const entry = id ? modelDef.sourceRegistry?.[id]?.entry : undefined;
      if (!entry) continue;
      const declared =
         entry.type === "source_registry_reference"
            ? modelDef.contents[entry.name]
            : entry;
      // Its own declaration — nothing to inherit from, and not a failure.
      if (declared === struct) continue;
      if (!declared || !isSourceDef(declared)) {
         sawBrokenEntry = true;
         continue;
      }
      return { kind: "resolved", source: declared };
   }
   return sawBrokenEntry ? { kind: "unresolvable" } : { kind: "none" };
}

/**
 * The `SourceDef` a `query_source` (`Z is X -> {...}`) derives from, via
 * `query.structRef`; `undefined` if `struct` is not a `query_source` or the
 * ref does not resolve to a `SourceDef`.
 *
 * Duck-typed because `QuerySourceDef` is not exported from the package root.
 */
export function resolveQuerySourceBase(
   struct: SourceDef,
   modelDef: ModelDef | undefined,
): SourceDef | undefined {
   const duck = struct as unknown as {
      type: string;
      query?: { structRef?: SourceDef | string };
   };
   if (duck.type !== "query_source") return undefined;
   const ref = duck.query?.structRef;
   const base = typeof ref === "string" ? modelDef?.contents[ref] : ref;
   return base && isSourceDef(base) ? base : undefined;
}

/**
 * For a `query_source` whose base is a composite (`qs is compose(a, b) ->
 * {...}`), the member Malloy resolved for this derivation. It is carried on
 * `query.compositeResolvedSourceDef`, not `query.structRef` (the raw
 * composite).
 */
export function resolveCompositeResolvedBase(
   struct: SourceDef,
): SourceDef | undefined {
   const duck = struct as unknown as {
      type: string;
      query?: { compositeResolvedSourceDef?: SourceDef };
   };
   return duck.type === "query_source"
      ? duck.query?.compositeResolvedSourceDef
      : undefined;
}

/**
 * Every struct reachable from `roots` (excluded) by the two hops
 * {@link effectiveAncestorGateExprs} takes, transitively.
 *
 * `modelDef.contents` plus `sourceRegistry` does not cover everything the
 * gate walks reach: an imported `query_source`'s base arrives inline on
 * `query.structRef`, and a composite's resolved member lives in neither. A
 * sweep asking "is there a gate anywhere the walks look" must follow both or
 * it answers `false` for a gated model. Identity-guarded against cycles.
 */
export function derivedStructsReachable(
   roots: readonly SourceDef[],
   modelDef: ModelDef | undefined,
): SourceDef[] {
   const seen = new Set<SourceDef>(roots);
   const found: SourceDef[] = [];
   const worklist: SourceDef[] = [...roots];
   for (let i = 0; i < worklist.length; i++) {
      const struct = worklist[i];
      for (const next of [
         resolveQuerySourceBase(struct, modelDef),
         resolveCompositeResolvedBase(struct),
      ]) {
         if (!next || seen.has(next)) continue;
         seen.add(next);
         found.push(next);
         worklist.push(next);
      }
   }
   return found;
}

/**
 * The source `struct` extends (`source: b is a extend { … }` records
 * `extends: "a@url"`), resolved through the source registry; undefined for a
 * source that extends nothing or whose base the registry does not hold.
 */
export function resolveExtendedSource(
   struct: SourceDef,
   modelDef: ModelDef | undefined,
): SourceDef | undefined {
   const id = (struct as unknown as { extends?: unknown }).extends;
   if (typeof id !== "string" || !modelDef) return undefined;
   const entry = modelDef.sourceRegistry?.[id]?.entry;
   if (!entry) return undefined;
   const declared =
      entry.type === "source_registry_reference"
         ? modelDef.contents[entry.name]
         : entry;
   return declared && isSourceDef(declared) && declared !== struct
      ? declared
      : undefined;
}

/**
 * `roots` and every source their walks reach: the declaration each names
 * ({@link resolveDeclaredSource}) and the source each extends
 * ({@link resolveExtendedSource}), as well as what
 * {@link derivedStructsReachable} follows. For a reader asking which sources a
 * set of entry points reads, not which gates they inherit. Identity-guarded
 * against cycles.
 */
export function structsReachedFrom(
   roots: readonly SourceDef[],
   modelDef: ModelDef | undefined,
): SourceDef[] {
   const seen = new Set<SourceDef>(roots);
   const worklist: SourceDef[] = [...roots];
   for (let i = 0; i < worklist.length; i++) {
      const struct = worklist[i];
      const declared = resolveDeclaredSource(struct, modelDef);
      for (const next of [
         declared.kind === "resolved" ? declared.source : undefined,
         resolveExtendedSource(struct, modelDef),
         resolveQuerySourceBase(struct, modelDef),
         resolveCompositeResolvedBase(struct),
      ]) {
         if (!next || seen.has(next)) continue;
         seen.add(next);
         worklist.push(next);
      }
   }
   return worklist;
}
