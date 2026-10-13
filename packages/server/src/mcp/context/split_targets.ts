// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * PROVISIONAL (`retrieval.fcj.splitBundles`): how a list target travels
 * through the stages.
 *
 *   expand    a target whose text splits (split.ts) keeps its own index and
 *             text, and gains one engine-made target per item, marked
 *             `partOf` the original
 *   find      every target, items included, is searched on its own
 *   connect   the original keeps its own list (listTopK); each item's list
 *             is cut to `itemListCap` so the judge's input stays bounded
 *   judge     sees the original and its items, numbered, and picks per item
 *   merge     the items' picks, candidates and coverage fold back into the
 *             original, so respond and the response see only the caller's
 *             targets
 *
 * The original's find list in the trace and for "also likely" is the
 * reciprocal-rank fusion of its own list and its items' lists: the whole
 * phrase's candidates are always in it, so a wrong split adds candidates
 * and loses none.
 */

import type { ClusterCandidate, ConnectResult } from "./connect";
import { fuseByRank, type FindResult, type FindTarget } from "./find";
import type { JudgeVerdict } from "./judge";
import { MAX_ITEMS, splitBundle } from "./split";

/** Candidate slots a list's items share in the judge's input, on top of the whole phrase's listTopK. */
export const ITEM_SLOTS = 45;
/** Fewest candidates an item's list is cut to. */
export const MIN_ITEM_LIST = 5;

/** Index of item `j` of the target at `index`: past any caller index, never colliding. */
export function itemIndex(index: number, j: number): number {
   return 10_000 + index * (MAX_ITEMS + 1) + j + 1;
}

/** How many candidates each of `items` items shows the judge. */
export function itemListCap(items: number, listTopK: number): number {
   return Math.min(
      listTopK,
      Math.max(MIN_ITEM_LIST, Math.floor(ITEM_SLOTS / items)),
   );
}

export interface SplitPlan {
   /** The caller's targets followed, after each split one, by its items. */
   targets: FindTarget[];
   /** Item index to the original's index. */
   parentOf: Map<number, number>;
   /** Original index to its items' texts, in order. */
   items: Map<number, string[]>;
   /** Original index to the list cap of its items. */
   cap: Map<number, number>;
}

/** Item targets for every caller target whose text is a list. Values, sources and joins are never split. */
export function planSplit(
   targets: readonly FindTarget[],
   listTopK: number,
   knownPhrase: (phrase: string) => boolean,
): SplitPlan {
   const out: FindTarget[] = [];
   const parentOf = new Map<number, number>();
   const items = new Map<number, string[]>();
   const cap = new Map<number, number>();
   for (const t of targets) {
      out.push(t);
      if (t.type !== "dimension" && t.type !== "measure" && t.type !== "view")
         continue;
      const pieces = splitBundle(t.text, knownPhrase);
      if (pieces.length < 2) continue;
      items.set(t.index, pieces);
      cap.set(t.index, itemListCap(pieces.length, listTopK));
      pieces.forEach((text, j) => {
         const index = itemIndex(t.index, j);
         parentOf.set(index, t.index);
         out.push({
            index,
            type: t.type,
            text,
            kinds: t.kinds,
            partOf: t.index,
         });
      });
   }
   return { targets: out, parentOf, items, cap };
}

/** The find results connect reads: each item's list cut to its cap. */
export function forConnect(
   plan: SplitPlan,
   found: readonly FindResult[],
): FindResult[] {
   return found.map((r) => {
      const parent = plan.parentOf.get(r.target.index);
      if (parent === undefined) return r;
      return { ...r, candidates: r.candidates.slice(0, plan.cap.get(parent)) };
   });
}

/**
 * The caller's targets' find results: a split target's list is the
 * reciprocal-rank fusion of its own list and each item's (uncut), its
 * candidates carrying the fused score.
 */
export function fusedFound(
   plan: SplitPlan,
   found: readonly FindResult[],
): FindResult[] {
   const byIndex = new Map(found.map((r) => [r.target.index, r]));
   const out: FindResult[] = [];
   for (const r of found) {
      if (plan.parentOf.has(r.target.index)) continue;
      const pieces = plan.items.get(r.target.index);
      if (!pieces) {
         out.push(r);
         continue;
      }
      const lists = [
         r,
         ...pieces.map((_, j) => byIndex.get(itemIndex(r.target.index, j))!),
      ];
      const first = new Map<string, FindResult["candidates"][number]>();
      for (const l of lists)
         for (const c of l.candidates)
            if (!first.has(c.key)) first.set(c.key, c);
      const fused = fuseByRank(
         lists.map((l) => l.candidates.map((c) => c.key)),
      );
      out.push({
         ...r,
         candidates: fused.map((f) => ({
            ...first.get(f.key)!,
            score: f.score,
         })),
      });
   }
   return out;
}

/** Fold the items' candidates, coverage and unresolved state back into their originals. */
export function mergeConnected(
   plan: SplitPlan,
   connected: ConnectResult,
): ConnectResult {
   const up = (t: number) => plan.parentOf.get(t) ?? t;
   const uniq = (xs: number[]) =>
      [...new Set(xs.map(up))].sort((a, b) => a - b);
   const clusters = connected.clusters.map((c) => {
      const candidates = new Map<number, ClusterCandidate[]>();
      // The original's own list first, then each item's, in item order.
      const order = [...c.candidates.keys()].sort((a, b) => a - b);
      for (const t of order) {
         const into = up(t);
         const list = candidates.get(into) ?? [];
         for (const x of c.candidates.get(t) ?? []) {
            if (!list.some((y) => y.key === x.key)) list.push(x);
         }
         candidates.set(into, list);
      }
      return {
         ...c,
         covers: uniq(c.covers),
         newlyCovers: uniq(c.newlyCovers),
         candidates,
      };
   });
   const covered = new Set(clusters.flatMap((c) => c.covers));
   return {
      ...connected,
      clusters,
      unresolved: uniq(connected.unresolved).filter((t) => !covered.has(t)),
   };
}

/**
 * Fold each verdict's item picks into one pick per original target: the
 * keys in order (the whole phrase's first), the reasons joined, and the
 * items nothing was picked for named as not found by this search.
 */
export function mergeVerdicts(
   plan: SplitPlan,
   verdicts: readonly JudgeVerdict[],
): JudgeVerdict[] {
   return verdicts.map((v) => {
      const picks: JudgeVerdict["picks"] = [];
      const at = new Map<number, number>();
      const empty = new Map<number, string[]>();
      for (const p of v.picks) {
         const parent = plan.parentOf.get(p.target);
         const into = parent ?? p.target;
         if (parent !== undefined && p.keys.length === 0) {
            const j = p.target - itemIndex(parent, 0);
            const text = plan.items.get(parent)?.[j];
            if (text) empty.set(parent, [...(empty.get(parent) ?? []), text]);
            continue;
         }
         const i = at.get(into);
         if (i === undefined) {
            at.set(into, picks.length);
            picks.push({ ...p, target: into, keys: [...p.keys] });
            continue;
         }
         const prev = picks[i];
         for (const k of p.keys) if (!prev.keys.includes(k)) prev.keys.push(k);
         if (p.reason && !prev.reason?.includes(p.reason)) {
            prev.reason = prev.reason ? `${prev.reason} ${p.reason}` : p.reason;
         }
         if (p.droppedRoutes) {
            prev.droppedRoutes = [
               ...(prev.droppedRoutes ?? []),
               ...p.droppedRoutes,
            ];
         }
      }
      for (const [parent, texts] of empty) {
         const note = `Not found by this search: ${texts.map((t) => JSON.stringify(t)).join(", ")}; search each on its own to be sure.`;
         const i = at.get(parent);
         if (i === undefined) {
            at.set(parent, picks.length);
            picks.push({ target: parent, keys: [], reason: note });
         } else {
            picks[i].reason = picks[i].reason
               ? `${picks[i].reason} ${note}`
               : note;
         }
      }
      for (const p of picks) if (p.reason) p.reason = p.reason.slice(0, 900);
      return { ...v, picks };
   });
}

/** The trace of a split: each item with its list as the judge saw it and what was picked for it. */
export function splitTrace(
   plan: SplitPlan,
   found: readonly FindResult[],
   verdicts: readonly JudgeVerdict[],
   idOf: (key: string) => string | undefined,
) {
   const byIndex = new Map(found.map((r) => [r.target.index, r]));
   return [...plan.items].map(([index, pieces]) => ({
      target_index: index,
      item_list_cap: plan.cap.get(index),
      items: pieces.map((text, j) => {
         const t = itemIndex(index, j);
         return {
            text,
            candidates: (byIndex.get(t)?.candidates ?? [])
               .slice(0, plan.cap.get(index))
               .map((c) => idOf(c.key)),
            picks: verdicts.flatMap((v) =>
               v.picks
                  .filter((p) => p.target === t)
                  .flatMap((p) => p.keys.map(idOf)),
            ),
         };
      }),
   }));
}
