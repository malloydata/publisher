// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The reach of a source: the source itself and every source it reaches by
 * declared joins, up to a depth, with the fewest hops it takes and every path
 * that gets there. A field is "in reach" of a source when its owning source
 * is, which is what lets the connect stage ask "can one query from here use
 * both the measure and the date the caller named".
 *
 * Joins only. The sources a source extends (and is extended by) are not in its
 * reach; that is deliberate. An extension already
 * carries its parent's fields and joins, so the parent adds nothing a query
 * from the child can use, and the one thing it would add is a field the child
 * hid (`extend { except: x }`), which a query from the child cannot read.
 */

import type { Relationship } from "@malloydata/malloy-interfaces";
import type { JoinHop } from "./catalog";

export interface ReachEntry {
   /** Catalog key of the source reached. */
   source: string;
   /** Fewest joins from the root; 0 for the root itself. */
   hops: number;
   /** Every join path that reaches it, shortest first; `[]` for the root. */
   paths: string[][];
   /** Widest relationship on the shortest path; undefined for the root. */
   fanout?: Relationship;
}

/**
 * Reach of `from` at `depth`. `joins` holds, per source, every path the
 * topology recorded from it (already multi-hop), so this filters and folds;
 * it never walks. Pure.
 */
export function reachOf(
   joins: ReadonlyMap<string, readonly JoinHop[]>,
   from: string,
   depth: number,
): Map<string, ReachEntry> {
   const out = new Map<string, ReachEntry>([
      [from, { source: from, hops: 0, paths: [[]] }],
   ]);
   const hops = [...(joins.get(from) ?? [])]
      .filter((h) => h.path.length <= depth)
      .sort(
         (a, b) =>
            a.path.length - b.path.length ||
            (a.path.join(".") < b.path.join(".") ? -1 : 1),
      );
   for (const hop of hops) {
      // A source that joins itself (a self-join, a parent-child hierarchy)
      // is its own home already; its path is noted but costs no hop.
      const entry = out.get(hop.to);
      if (entry) {
         entry.paths.push(hop.path);
         continue;
      }
      out.set(hop.to, {
         source: hop.to,
         hops: hop.path.length,
         paths: [hop.path],
         fanout: hop.fanout,
      });
   }
   return out;
}

/**
 * Memoized reach for every source a request touches. The connect stage asks
 * for the reach of every source in the catalog once per request.
 */
export class ReachCache {
   private readonly cache = new Map<string, Map<string, ReachEntry>>();

   constructor(
      private readonly joins: ReadonlyMap<string, readonly JoinHop[]>,
      private readonly depth: number,
   ) {}

   of(source: string): Map<string, ReachEntry> {
      let r = this.cache.get(source);
      if (!r) {
         r = reachOf(this.joins, source, this.depth);
         this.cache.set(source, r);
      }
      return r;
   }
}
