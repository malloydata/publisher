// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Stage 2, connect: take every target's candidates together and pick the few
 * sources that explain them. No model call. This is where "which start_date",
 * "which Denver", twins and several-measure questions are decided, by
 * coherence: one query from one source has to be able to use what the
 * question names.
 *
 * Scoring. For every source S, and every target t:
 *
 *   best(S, t) = the highest candidate score for t among candidates whose
 *                owning source is in the reach of S, times hopDiscount per
 *                join hop (so a field in S beats the same field seen through
 *                a join);
 *   cover(S)   = the targets whose best(S, t) is at least coverFraction of
 *                t's best anywhere;
 *   score(S)   = the sum over covered targets of best(S, t), a measure target
 *                weighted measureWeight (the measure fixes the home source).
 *
 * Selection is greedy set cover: the best-scoring source and its reach is the
 * first cluster; the targets it covers are removed; repeat while a source
 * covers a remaining target, up to maxClusters.
 *
 * In many packages most sources are islands (their joins reach raw tables
 * that are fields of the source itself), so a cluster is often one source,
 * and the rule reduces to "the island whose measure, dates and filters fit
 * the targets together". The same rule handles a joined star.
 *
 * Inside each cluster the pool is small, so every field of the right kind in
 * the cluster is rescored against every target, not only those that made the
 * global top N, and the top k are kept; a field ranked 15th in the catalog is
 * often 2nd in its cluster. Structural extras the judge should see even when
 * nothing matched them are added with the reason: a chosen measure's
 * dependencies, the home source's dates when a target mentions a period,
 * views that use two or more chosen fields, and the home source itself.
 */

import type { FcjSettings } from "../../service/package_retrieval";
import type { CatalogNode } from "./catalog";
import {
   mentionsPeriod,
   rankTarget,
   type Candidate,
   type FindResult,
   type FindTarget,
} from "./find";
import type { FcjIndex } from "./index_build";
import { ReachCache, type ReachEntry } from "./reach";

export type ConnectSettings = Pick<
   FcjSettings,
   | "hopDiscount"
   | "coverFraction"
   | "measureWeight"
   | "maxClusters"
   | "clusterTopK"
   | "reachDepth"
   | "keyword"
   | "values"
>;

/** A source's case for being a home. */
export interface SourceScore {
   source: string;
   score: number;
   /** Targets it covers, by index, ascending. */
   covers: number[];
   /** Per covered target: the candidate that set best(S, t), its discounted score and hops. */
   best: Map<number, { key: string; score: number; hops: number }>;
}

export interface ClusterCandidate {
   key: string;
   /** In-cluster score: fused rank score times hopDiscount per hop. */
   score: number;
   /** Rank inside the cluster for this target, 1-based. */
   rank: number;
   /** Joins from the home source to the field's source; 0 for the home's own. */
   hops: number;
   /** The candidate's find-stage record inside the cluster (ranks, cosine, values). */
   found: Candidate;
}

export interface ClusterExtra {
   key: string;
   reason: string;
}

export interface Cluster {
   home: string;
   score: number;
   /** Every target this cluster covers. */
   covers: number[];
   /** The targets it was picked for (covered and not covered by an earlier cluster). */
   newlyCovers: number[];
   /** Sources with the same answer under another name: same coverage, same fields. */
   twins: string[];
   /** Reach of the home (and the twins), by source key. */
   reach: Map<string, ReachEntry>;
   /** Per target index: the in-cluster top k. Every target gets a list, covered or not. */
   candidates: Map<number, ClusterCandidate[]>;
   extras: ClusterExtra[];
}

export interface ConnectResult {
   clusters: Cluster[];
   /** Target indexes no cluster covers. */
   unresolved: number[];
   /** The best-scoring sources considered, for the trace. */
   considered: SourceScore[];
}

/** For each source, every source (itself included) whose reach holds it, with the hops. */
function reachersOf(
   index: FcjIndex,
   depth: number,
): Map<string, Array<{ from: string; hops: number }>> {
   const out = new Map<string, Array<{ from: string; hops: number }>>();
   const add = (to: string, from: string, hops: number) => {
      const list = out.get(to) ?? [];
      const prev = list.find((x) => x.from === from);
      if (prev) prev.hops = Math.min(prev.hops, hops);
      else list.push({ from, hops });
      out.set(to, list);
   };
   for (const key of index.catalog.sources.keys()) add(key, key, 0);
   for (const [from, hops] of index.catalog.joins) {
      for (const hop of hops) {
         if (hop.path.length <= depth && hop.to !== from) {
            add(hop.to, from, hop.path.length);
         }
      }
   }
   return out;
}

/**
 * Score every source against the targets' candidates. Pure. `admitSource`
 * keeps a request's scope: a source outside it is never a home.
 */
export function scoreSources(
   index: FcjIndex,
   found: readonly FindResult[],
   settings: ConnectSettings,
   admitSource: (source: string) => boolean = () => true,
): SourceScore[] {
   const reachers = reachersOf(index, settings.reachDepth);
   const bySource = new Map<
      string,
      Map<number, { key: string; score: number; hops: number }>
   >();
   const globalBest = new Map<number, number>();
   for (const r of found) {
      const t = r.target.index;
      for (const c of r.candidates) {
         globalBest.set(t, Math.max(globalBest.get(t) ?? 0, c.score));
         const owner = index.catalog.nodes.get(c.key)?.sourceKey;
         if (!owner) continue;
         for (const { from, hops } of reachers.get(owner) ?? []) {
            if (!admitSource(from)) continue;
            const s = c.score * settings.hopDiscount ** hops;
            const m = bySource.get(from) ?? new Map();
            const prev = m.get(t);
            if (!prev || s > prev.score)
               m.set(t, { key: c.key, score: s, hops });
            bySource.set(from, m);
         }
      }
   }
   const weight = new Map(
      found.map((r) => [
         r.target.index,
         r.target.type === "measure" ? settings.measureWeight : 1,
      ]),
   );
   const out: SourceScore[] = [];
   for (const [source, best] of bySource) {
      const covers: number[] = [];
      let score = 0;
      for (const [t, b] of best) {
         if (b.score >= settings.coverFraction * (globalBest.get(t) ?? 0)) {
            covers.push(t);
            score += (weight.get(t) ?? 1) * b.score;
         }
      }
      covers.sort((a, b) => a - b);
      const kept = new Map([...best].filter(([t]) => covers.includes(t)));
      out.push({ source, score, covers, best: kept });
   }
   return out.sort(compareScores);
}

function sumHops(s: SourceScore): number {
   let n = 0;
   for (const b of s.best.values()) n += b.hops;
   return n;
}

/** Best first: score, then more targets covered, then fewer joins used, then key. */
function compareScores(a: SourceScore, b: SourceScore): number {
   return (
      b.score - a.score ||
      b.covers.length - a.covers.length ||
      sumHops(a) - sumHops(b) ||
      (a.source < b.source ? -1 : a.source > b.source ? 1 : 0)
   );
}

/** The score of a source over `remaining` targets only. */
function restrictTo(
   s: SourceScore,
   remaining: ReadonlySet<number>,
   found: readonly FindResult[],
   settings: ConnectSettings,
): SourceScore {
   const covers = s.covers.filter((t) => remaining.has(t));
   const best = new Map([...s.best].filter(([t]) => remaining.has(t)));
   let score = 0;
   for (const t of covers) {
      const type = found.find((r) => r.target.index === t)?.target.type;
      score +=
         (type === "measure" ? settings.measureWeight : 1) *
         (best.get(t)?.score ?? 0);
   }
   return { source: s.source, score, covers, best };
}

/** The last segment of a node's name: `products.category` is `category`. */
function leafOf(index: FcjIndex, key: string): string {
   const node = index.catalog.nodes.get(key);
   if (!node) return key;
   return `${node.kind}:${node.joinPath ? node.name.slice(node.joinPath.length + 1) : node.name}`;
}

/**
 * Sources that give the same answer as `home` under another name: within 10%
 * of its score, covering the same targets, with the same kind and field name
 * as its best candidate for each one (`orders` and `orders_public`, or
 * `pulse_base is item_base`). They join the home's cluster instead of
 * competing for a cluster of their own, so the judge sees both and neither
 * is dropped for being second.
 *
 * Ten percent, because two identical fields rarely tie on rank: one is first
 * and the other second on each list, which is already 1.6% apart, and a
 * keyword tie broken by key order adds more. The name check, not the band,
 * is what keeps an unrelated source out.
 */
const TWIN_SCORE_SHARE = 0.9;

function twinsOf(
   index: FcjIndex,
   home: SourceScore,
   all: readonly SourceScore[],
): string[] {
   return all
      .filter(
         (s) =>
            s.source !== home.source &&
            s.score >= TWIN_SCORE_SHARE * home.score &&
            s.covers.length === home.covers.length &&
            s.covers.every((t, i) => t === home.covers[i]) &&
            home.covers.every(
               (t) =>
                  // Another node of the same name: a source that only
                  // reaches the home's own field through a join is not a
                  // second answer, it is the same answer one hop away.
                  s.best.get(t)?.key !== home.best.get(t)?.key &&
                  leafOf(index, s.best.get(t)?.key ?? "") ===
                     leafOf(index, home.best.get(t)?.key ?? ""),
            ),
      )
      .map((s) => s.source);
}

/** Pick the clusters: greedy set cover over the scored sources. Pure. */
export function selectClusters(
   index: FcjIndex,
   found: readonly FindResult[],
   scores: readonly SourceScore[],
   settings: ConnectSettings,
): {
   homes: Array<{ score: SourceScore; newlyCovers: number[]; twins: string[] }>;
   unresolved: number[];
} {
   const remaining = new Set(
      found.filter((r) => r.candidates.length > 0).map((r) => r.target.index),
   );
   const homes: Array<{
      score: SourceScore;
      newlyCovers: number[];
      twins: string[];
   }> = [];
   const taken = new Set<string>();
   while (remaining.size > 0 && homes.length < settings.maxClusters) {
      const ranked = scores
         .filter((s) => !taken.has(s.source))
         .map((s) => restrictTo(s, remaining, found, settings))
         .filter((s) => s.covers.length > 0)
         .sort(compareScores);
      const pick = ranked[0];
      if (!pick) break;
      const full = scores.find((s) => s.source === pick.source)!;
      const twins = twinsOf(index, full, scores).filter((t) => !taken.has(t));
      homes.push({ score: full, newlyCovers: pick.covers, twins });
      taken.add(pick.source);
      for (const t of twins) taken.add(t);
      for (const t of pick.covers) remaining.delete(t);
   }
   const covered = new Set(homes.flatMap((h) => h.score.covers));
   const unresolved = found
      .map((r) => r.target.index)
      .filter((t) => !covered.has(t));
   return { homes, unresolved };
}

const DATE_TYPES = new Set(["date", "timestamp", "timestamptz"]);
/** Most dates of the home source added for a period phrase. */
const MAX_DATE_EXTRAS = 8;
/** Most views added for using two or more chosen fields. */
const MAX_VIEW_EXTRAS = 5;
/** Measures whose dependencies are added. */
const MEASURES_WITH_DEPENDENCIES = 3;

export function connect(args: {
   index: FcjIndex;
   found: readonly FindResult[];
   queries: ReadonlyMap<number, Float32Array>;
   settings: ConnectSettings;
   /** The request's scope, applied to the in-cluster pool. */
   allow: (node: CatalogNode) => boolean;
}): ConnectResult {
   const { index, found, settings } = args;
   const admitSource = (key: string) => {
      const node = index.catalog.sources.get(key)?.node;
      return node ? args.allow(node) : false;
   };
   const scores = scoreSources(index, found, settings, admitSource);
   const { homes, unresolved } = selectClusters(index, found, scores, settings);
   const reach = new ReachCache(index.catalog.joins, settings.reachDepth);
   const targets = found.map((r) => r.target);

   const clusters: Cluster[] = homes.map(({ score, newlyCovers, twins }) => {
      // The pool: every source in the reach of the home or a twin, with the
      // fewest hops from either.
      const pool = new Map<string, ReachEntry>();
      for (const root of [score.source, ...twins]) {
         for (const [k, e] of reach.of(root)) {
            const prev = pool.get(k);
            if (!prev || e.hops < prev.hops) pool.set(k, e);
         }
      }
      const inPool = (node: CatalogNode) =>
         pool.has(node.sourceKey) && args.allow(node);
      const candidates = new Map<number, ClusterCandidate[]>();
      for (const target of targets) {
         candidates.set(
            target.index,
            rankInCluster(
               index,
               target,
               args.queries.get(target.index),
               inPool,
               pool,
               settings,
            ),
         );
      }
      const extras = extrasOf(
         index,
         score.source,
         pool,
         candidates,
         targets,
         args.allow,
      );
      return {
         home: score.source,
         score: score.score,
         covers: score.covers,
         newlyCovers,
         twins,
         reach: pool,
         candidates,
         extras,
      };
   });
   return {
      clusters,
      unresolved,
      considered: scores.filter((s) => s.covers.length > 0).slice(0, 10),
   };
}

function rankInCluster(
   index: FcjIndex,
   target: FindTarget,
   query: Float32Array | undefined,
   inPool: (node: CatalogNode) => boolean,
   pool: ReadonlyMap<string, ReachEntry>,
   settings: ConnectSettings,
): ClusterCandidate[] {
   const ranked = rankTarget(index, target, query, inPool, {
      keyword: settings.keyword,
      values: settings.values,
      depth: Number.MAX_SAFE_INTEGER,
   });
   return ranked.all
      .map((c) => {
         const owner = index.catalog.nodes.get(c.key)!.sourceKey;
         const hops = pool.get(owner)?.hops ?? 0;
         return {
            key: c.key,
            score: c.score * settings.hopDiscount ** hops,
            hops,
            found: c,
            rank: 0,
         };
      })
      .sort(
         (a, b) =>
            b.score - a.score || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0),
      )
      .slice(0, settings.clusterTopK)
      .map((c, i) => ({ ...c, rank: i + 1 }));
}

function extrasOf(
   index: FcjIndex,
   home: string,
   pool: ReadonlyMap<string, ReachEntry>,
   candidates: ReadonlyMap<number, ClusterCandidate[]>,
   targets: readonly FindTarget[],
   allow: (node: CatalogNode) => boolean,
): ClusterExtra[] {
   const chosen = new Set<string>();
   for (const list of candidates.values())
      for (const c of list) chosen.add(c.key);
   const extras: ClusterExtra[] = [];
   const seen = new Set<string>();
   const add = (key: string, reason: string) => {
      if (chosen.has(key) || seen.has(key)) return;
      const node = index.catalog.nodes.get(key);
      if (!node || !allow(node)) return;
      seen.add(key);
      extras.push({ key, reason });
   };
   const homeSource = index.catalog.sources.get(home);
   if (homeSource) add(homeSource.node.key, "the home source");
   // A chosen measure's dependencies: the fields it is computed from.
   for (const t of targets) {
      if (t.type !== "measure") continue;
      for (const c of (candidates.get(t.index) ?? []).slice(
         0,
         MEASURES_WITH_DEPENDENCIES,
      )) {
         const node = index.catalog.nodes.get(c.key);
         for (const dep of node?.dependsOn ?? []) {
            add(dep, `${node!.name} is computed from it`);
         }
      }
   }
   // The home's dates when a target names a period.
   if (targets.some((t) => mentionsPeriod(t.text)) && homeSource) {
      const dates = homeSource.fields
         .map((k) => index.catalog.nodes.get(k)!)
         .filter(
            (n) => n.kind === "dimension" && DATE_TYPES.has(n.dataType ?? ""),
         )
         .slice(0, MAX_DATE_EXTRAS);
      for (const d of dates)
         add(d.key, "a date of the home source, and a target names a period");
   }
   // Views that use two or more of the chosen fields.
   let views = 0;
   for (const source of pool.keys()) {
      for (const key of index.catalog.sources.get(source)?.fields ?? []) {
         if (views >= MAX_VIEW_EXTRAS) break;
         const node = index.catalog.nodes.get(key)!;
         if (node.kind !== "view") continue;
         const used = node.uses.filter((u) => chosen.has(u));
         if (used.length >= 2) {
            add(
               key,
               `uses ${used.map((u) => index.catalog.nodes.get(u)!.name).join(", ")}`,
            );
            views += 1;
         }
      }
   }
   return extras;
}
