// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Generic Malloy explore-walk primitives shared by extraction and
 * dim-index discovery. Lives in its own module so the source-name
 * composition rule and the tree-walk recursion semantics stay
 * single-sourced — both passes that decorate explores with derived
 * IDs need to agree on what counts as "the same source" for a given
 * field, or the resolver lookup misses by ``${sourceName}::${fieldName}``.
 */

import { getAccessModifier } from "./field_helpers";
import { AccessModifierEnum } from "./internal_types";
import type { Explore as MalloyExplore } from "@malloydata/malloy";

/**
 * Compose the dotted source-name path the extraction emits for one
 * explore. Root sources keep their bare name (``flights``); explores
 * reached via a nested-index join prepend the parent path
 * (``flights.origin``).
 */
export function composeSourceName(
   parentPath: string | null,
   exploreName: string,
): string {
   return parentPath ? `${parentPath}.${exploreName}` : exploreName;
}

/**
 * Split a composed (possibly dotted) source path into the runnable root
 * source and the join path below it. A dotted path such as
 * ``analytics.order_items`` is NOT a runnable Malloy source — every query
 * against it (the dim-index fingerprint fragment, an indexer's value
 * scans) must run against the root (``analytics``) with a
 * join-qualified field path (``order_items.<field>``). This is the single
 * home for that rule; the extraction's leaf-entity emission and the
 * dim-index id derivation both go through it so they cannot disagree on
 * what the queryable root of a nested source path is.
 */
export function splitSourcePath(sourcePath: string): {
   rootSourceName: string;
   joinPath: string | null;
} {
   const firstDot = sourcePath.indexOf(".");
   if (firstDot === -1) {
      return { rootSourceName: sourcePath, joinPath: null };
   }
   return {
      rootSourceName: sourcePath.slice(0, firstDot),
      joinPath: sourcePath.slice(firstDot + 1),
   };
}

/**
 * Some descents depend on which sources the query boundary leaves
 * unqueryable (see `descendsIntoJoin` in source_extraction.ts). An extraction
 * that does not know them yet takes such a descent provisionally and guards
 * what it finds below it: that holds only when `root`, the runnable source at
 * the head of the walk, is queryable and every `nonQueryable` source is not.
 */
export interface QueryabilityGuard {
   root: string;
   nonQueryable: string[];
}

/** Whether `guard` holds when exactly `nonQueryable` are not queryable. An absent guard holds. */
export function guardHolds(
   guard: QueryabilityGuard | undefined,
   nonQueryable: ReadonlySet<string>,
): boolean {
   return (
      guard === undefined ||
      (!nonQueryable.has(guard.root) &&
         guard.nonQueryable.every((name) => nonQueryable.has(name)))
   );
}

/** `guard` narrowed by a descent taken only while `target` is not queryable. */
export function guardDescent(
   guard: QueryabilityGuard | undefined,
   root: string,
   target: string,
): QueryabilityGuard {
   const nonQueryable = guard?.nonQueryable ?? [];
   return {
      root,
      nonQueryable: nonQueryable.includes(target)
         ? nonQueryable
         : [...nonQueryable, target],
   };
}

/**
 * Whether a walk descends into a join: always, never, or only while `target`
 * is not queryable and the walk's root is ({@link QueryabilityGuard}).
 */
export type JoinDescent = boolean | { unlessQueryable: string };

/**
 * How one extraction decides the descents the query boundary decides.
 * `provisional`: each is taken and guarded; one whose target is its own root
 * can never hold, so it is not taken. Otherwise each is left untaken, which is
 * exact when no source is unqueryable, and the `(root, target)` pairs left
 * untaken are kept so a reader that learns which sources are unqueryable can
 * tell whether the extraction still holds for them ({@link holdsFor}).
 */
export class BoundaryDescents {
   public readonly untaken: [root: string, target: string][] = [];
   private readonly untakenKeys = new Set<string>();

   constructor(public readonly provisional: boolean) {}

   /** The descent into a join, from the walk rooted at `root`, to the top-level source `target`. */
   descent(root: string, target: string): JoinDescent {
      if (this.provisional) {
         return root === target ? false : { unlessQueryable: target };
      }
      const key = `${root}\0${target}`;
      if (!this.untakenKeys.has(key)) {
         this.untakenKeys.add(key);
         this.untaken.push([root, target]);
      }
      return false;
   }
}

/**
 * Whether an extraction that left `untaken` descents untaken (see
 * {@link BoundaryDescents}) is exact when `nonQueryable` are unqueryable: no
 * descent it left untaken would have been taken.
 */
export function holdsFor(
   untaken: readonly (readonly [string, string])[],
   nonQueryable: ReadonlySet<string>,
): boolean {
   return untaken.every(
      ([root, target]) => nonQueryable.has(root) || !nonQueryable.has(target),
   );
}

/** Whether a walk descends into a public `join` of the source at `parentSourceName`. */
export type JoinDescentRule = (
   join: MalloyExplore,
   parentSourceName: string,
) => JoinDescent;

/** An explore a walk reached, its composed source-name path, and the guard on reaching it. */
export interface WalkedExplore {
   explore: MalloyExplore;
   sourceName: string;
   guard?: QueryabilityGuard;
}

/**
 * Yield every explore reachable from ``explores`` through public joins that
 * ``descend`` admits, along with its composed source-name path and the guard
 * any provisional descent on the way put on it.
 */
export function* walkExploreTree(
   explores: MalloyExplore[],
   descend: JoinDescentRule = () => true,
): Generator<WalkedExplore> {
   const recurse = function* (
      explore: MalloyExplore,
      parentPath: string | null,
      root: string,
      guard: QueryabilityGuard | undefined,
   ): Generator<WalkedExplore> {
      const sourceName = composeSourceName(parentPath, explore.name);
      yield guard ? { explore, sourceName, guard } : { explore, sourceName };
      for (const field of explore.allFields) {
         if (field.isExploreField()) {
            // Don't descend into non-public joins; their nested fields
            // aren't usable outside the declaring source.
            if (getAccessModifier(field) !== AccessModifierEnum.Public)
               continue;
            const join = field as unknown as MalloyExplore;
            const descent = descend(join, sourceName);
            if (descent === false) continue;
            yield* recurse(
               join,
               sourceName,
               root,
               descent === true
                  ? guard
                  : guardDescent(guard, root, descent.unlessQueryable),
            );
         }
      }
   };
   for (const explore of explores) {
      yield* recurse(explore, null, explore.name, undefined);
   }
}
