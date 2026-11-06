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

/** Whether a walk descends into a public `join` of the source at `parentSourceName`. */
export type JoinDescentRule = (
   join: MalloyExplore,
   parentSourceName: string,
) => boolean;

/**
 * Yield every explore reachable from ``explores`` through public joins that
 * ``descend`` admits, along with its composed source-name path.
 */
export function* walkExploreTree(
   explores: MalloyExplore[],
   descend: JoinDescentRule = () => true,
): Generator<{ explore: MalloyExplore; sourceName: string }> {
   const recurse = function* (
      explore: MalloyExplore,
      parentPath: string | null,
   ): Generator<{ explore: MalloyExplore; sourceName: string }> {
      const sourceName = composeSourceName(parentPath, explore.name);
      yield { explore, sourceName };
      for (const field of explore.allFields) {
         if (field.isExploreField()) {
            // Don't descend into non-public joins; their nested fields
            // aren't usable outside the declaring source.
            if (getAccessModifier(field) !== AccessModifierEnum.Public)
               continue;
            const join = field as unknown as MalloyExplore;
            if (!descend(join, sourceName)) continue;
            yield* recurse(join, sourceName);
         }
      }
   };
   for (const explore of explores) {
      yield* recurse(explore, null);
   }
}
