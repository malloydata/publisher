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
 *
 * PROVISIONAL, `retrieval.fcj.connect: "list"`: no cluster filter at all (see
 * connectList). It is the baseline a connect rule has to beat.
 */

import type { FcjSettings } from "../../service/package_retrieval";
import { JOIN_WALK_MAX_HOPS, type CatalogNode } from "./catalog";
import {
   mentionsPeriod,
   rankTarget,
   unionLists,
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
   | "contenderShare"
   | "connect"
   | "listTopK"
   | "listUnion"
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
   /**
    * `listUnion`: set on a candidate below the fused top `listTopK` that is
    * in the judge's list because one of find's own lists ranks it within
    * `listUnion`: which list (or both). `rank` is then its fused rank.
    */
   union?: "close" | "keyword" | "close+keyword";
}

export interface ClusterExtra {
   key: string;
   reason: string;
}

/** With `connect: "list"`: one source the judge sees, as a peer of the others. */
export interface PeerSource {
   source: string;
   /** The other peers a query from this source can use through its joins, fewest hops first. */
   reaches: ReachEntry[];
}

export interface Cluster {
   /**
    * The source the cluster is built around. With `connect: "list"` it is
    * only the group's id and the trace's anchor (the owner of the first
    * measure target's best candidate): no source is preferred there.
    */
   home: string;
   score: number;
   /** Every target this cluster covers. */
   covers: number[];
   /** The targets it was picked for (covered and not covered by an earlier cluster). */
   newlyCovers: number[];
   /** Sources with the same answer under another name: same coverage, same fields. */
   twins: string[];
   /**
    * Sources that scored nearly as well on the same targets with other fields
    * (only with the provisional `contenderShare` on). In the pool, so the
    * judge sees them; their fields are shown on their own cards.
    */
   contenders: string[];
   /**
    * With `connect: "list"` only: every source holding a candidate or an
    * extra, in order of first appearance, each with the other peers its joins
    * reach. Every field is on its own source's card. Absent for a cluster.
    */
   peers?: PeerSource[];
   /** Reach of the home (and the twins and contenders), by source key; in list mode, the peers at 0 hops. */
   reach: Map<string, ReachEntry>;
   /** Reach of the home alone: the routes a field has from the home source. */
   homeReach: Map<string, ReachEntry>;
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

/**
 * With `collapseCopies`, an extension still has every field it inherits, now
 * folded into its parent's node. It reaches that node at this factor, a hair
 * under a field of its own: a question answered entirely by inherited fields
 * goes to the parent that defines them, and an extension wins only when its
 * own fields count.
 */
export const INHERIT_DISCOUNT = 0.99;

/**
 * The sources that hold a node, with the factor each gets: its owner at 1,
 * and, with `collapseCopies`, every source holding a folded copy of it at
 * INHERIT_DISCOUNT.
 */
function holdersOf(
   index: FcjIndex,
   key: string,
): Array<{ source: string; factor: number }> {
   const node = index.catalog.nodes.get(key);
   if (!node) return [];
   const out = [{ source: node.sourceKey, factor: 1 }];
   for (const copy of node.copies ?? []) {
      const holder = index.catalog.nodes.get(copy)?.sourceKey;
      if (holder && !out.some((h) => h.source === holder)) {
         out.push({ source: holder, factor: INHERIT_DISCOUNT });
      }
   }
   return out;
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
         for (const holder of holdersOf(index, c.key)) {
            for (const { from, hops } of reachers.get(holder.source) ?? []) {
               if (!admitSource(from)) continue;
               const s = c.score * holder.factor * settings.hopDiscount ** hops;
               const m = bySource.get(from) ?? new Map();
               const prev = m.get(t);
               if (!prev || s > prev.score)
                  m.set(t, { key: c.key, score: s, hops });
               bySource.set(from, m);
            }
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
   homes: Array<{
      score: SourceScore;
      newlyCovers: number[];
      twins: string[];
      contenders: string[];
   }>;
   unresolved: number[];
} {
   const remaining = new Set(
      found.filter((r) => r.candidates.length > 0).map((r) => r.target.index),
   );
   const homes: Array<{
      score: SourceScore;
      newlyCovers: number[];
      twins: string[];
      contenders: string[];
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
      const contenders =
         settings.contenderShare > 0
            ? ranked
                 .slice(1)
                 .filter(
                    (s) =>
                       !twins.includes(s.source) &&
                       s.score >= settings.contenderShare * pick.score &&
                       pick.covers.every((t) => s.covers.includes(t)) &&
                       // Not a source that only reaches the home's own fields.
                       pick.covers.some(
                          (t) => s.best.get(t)?.key !== pick.best.get(t)?.key,
                       ),
                 )
                 .slice(0, MAX_CONTENDERS)
                 .map((s) => s.source)
            : [];
      homes.push({ score: full, newlyCovers: pick.covers, twins, contenders });
      taken.add(pick.source);
      for (const t of twins) taken.add(t);
      for (const c of contenders) taken.add(c);
      for (const t of pick.covers) remaining.delete(t);
   }
   const covered = new Set(homes.flatMap((h) => h.score.covers));
   const unresolved = found
      .map((r) => r.target.index)
      .filter((t) => !covered.has(t));
   return { homes, unresolved };
}

/** Most near-tied sources one cluster takes in (provisional `contenderShare`). */
const MAX_CONTENDERS = 3;

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
   /** Value phrase vectors (`valueEmbeddings`), as find got them. */
   phraseVectors?: ReadonlyMap<string, Float32Array>;
   settings: ConnectSettings;
   /** The request's scope, applied to the in-cluster pool. */
   allow: (node: CatalogNode) => boolean;
}): ConnectResult {
   const { index, found, settings } = args;
   if (settings.connect === "list") return connectList(args);
   const admitSource = (key: string) => {
      const node = index.catalog.sources.get(key)?.node;
      return node ? args.allow(node) : false;
   };
   const scores = scoreSources(index, found, settings, admitSource);
   const { homes, unresolved } = selectClusters(index, found, scores, settings);
   const reach = new ReachCache(index.catalog.joins, settings.reachDepth);
   const targets = found.map((r) => r.target);

   const clusters: Cluster[] = homes.map(
      ({ score, newlyCovers, twins, contenders }) => {
         // The pool: every source in the reach of the home or a twin, with the
         // fewest hops from either.
         const pool = new Map<string, ReachEntry>();
         for (const root of [score.source, ...twins, ...contenders]) {
            for (const [k, e] of reach.of(root)) {
               const prev = pool.get(k);
               if (!prev || e.hops < prev.hops) pool.set(k, e);
            }
         }
         // A folded node is in the pool when a source of the pool holds a copy
         // of it; it is then ranked under that copy, the name the pool's
         // sources know it by.
         const inPool = (node: CatalogNode) =>
            args.allow(node) &&
            (pool.has(node.sourceKey) ||
               (node.copies ?? []).some((k) =>
                  pool.has(index.catalog.nodes.get(k)?.sourceKey ?? ""),
               ));
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
                  args.phraseVectors,
               ),
            );
         }
         const extras = extrasOf(
            index,
            score.source,
            [score.source],
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
            contenders,
            reach: pool,
            homeReach: reach.of(score.source),
            candidates,
            extras,
         };
      },
   );
   return {
      clusters,
      unresolved,
      considered: scores.filter((s) => s.covers.length > 0).slice(0, 10),
   };
}

/**
 * PROVISIONAL (`connect: "list"`): one group with no cluster filter. Every
 * target gets the find stage's own top `listTopK`, in find order but grouped
 * by owning source (sources in the order of their best candidate for that
 * target), so the judge still sees which fields share a source. Every source
 * holding a candidate or an extra is a peer: none is a home, every field is
 * on its own source's card, and each peer carries the other peers its joins
 * reach, so the judge can tell which picks one query can combine.
 *
 * `rank` is the candidate's find-stage rank and `score` its fused score, with
 * no hop discount: nothing is rescored.
 */
function connectList(args: Parameters<typeof connect>[0]): ConnectResult {
   const { index, found, settings } = args;
   const withCandidates = found.filter((r) => r.candidates.length > 0);
   const anchor =
      withCandidates.find((r) => r.target.type === "measure") ??
      withCandidates[0];
   const unresolved = found
      .filter((r) => r.candidates.length === 0)
      .map((r) => r.target.index);
   if (!anchor) return { clusters: [], unresolved, considered: [] };
   const ownerOf = (key: string) => index.catalog.nodes.get(key)!.sourceKey;
   const pool = new Map<string, ReachEntry>();
   const admit = (source: string) => {
      if (!pool.has(source)) pool.set(source, { source, hops: 0, paths: [[]] });
   };
   const byRank = new Map<number, ClusterCandidate[]>();
   for (const r of found) {
      const list: ClusterCandidate[] = r.candidates
         .slice(0, settings.listTopK)
         .map((c, i) => ({
            key: c.key,
            score: c.score,
            rank: i + 1,
            hops: 0,
            found: c,
         }));
      // `listUnion`: the fused rank buries a field that one of find's own
      // lists ranks high, so such a field joins the list after the top
      // listTopK. A split list's items keep their capped lists.
      const k = settings.listUnion ?? 0;
      if (k > 0 && r.target.partOf === undefined) {
         const below = [
            ...r.candidates
               .slice(settings.listTopK)
               .map((c, i) => ({ c, rank: settings.listTopK + i + 1 })),
            ...(r.beyond ?? []).map((c) => ({ c, rank: c.fusedRank })),
         ];
         for (const { c, rank } of below) {
            const union = unionLists(c, k);
            if (!union || list.some((x) => x.key === c.key)) continue;
            list.push({
               key: c.key,
               score: c.score,
               rank,
               hops: 0,
               found: c,
               union,
            });
         }
      }
      for (const c of list) admit(ownerOf(c.key));
      byRank.set(r.target.index, list);
   }
   const targets = found.map((r) => r.target);
   // A period's dates come from the sources of every measure target's best
   // three candidates (the first target's, with no measure), so no single
   // source frames the list.
   const dateSources: string[] = [];
   const measures = withCandidates.filter((r) => r.target.type === "measure");
   for (const r of measures.length > 0 ? measures : [anchor]) {
      for (const c of (byRank.get(r.target.index) ?? []).slice(
         0,
         MEASURES_WITH_DEPENDENCIES,
      )) {
         const owner = ownerOf(c.key);
         if (!dateSources.includes(owner)) dateSources.push(owner);
      }
   }
   // The extras read the lists in rank order: a measure's inputs are those
   // of the best three measures, wherever they sit.
   const extras = extrasOf(
      index,
      undefined,
      dateSources,
      pool,
      byRank,
      targets,
      args.allow,
   );
   for (const e of extras) admit(ownerOf(e.key));
   // Which peers one query can combine follows the join graph to any depth
   // (the catalog's walk), not `reachDepth`, which weighs cluster scores.
   const reach = new ReachCache(index.catalog.joins, JOIN_WALK_MAX_HOPS);
   const peers: PeerSource[] = [...pool.keys()].map((source) => ({
      source,
      reaches: [...reach.of(source).values()].filter(
         (r) => r.hops > 0 && pool.has(r.source),
      ),
   }));
   const candidates = new Map<number, ClusterCandidate[]>();
   for (const [t, list] of byRank) {
      const order = new Map<string, number>();
      for (const c of list) {
         const owner = ownerOf(c.key);
         if (!order.has(owner)) order.set(owner, order.size);
      }
      candidates.set(
         t,
         [...list].sort(
            (a, b) =>
               order.get(ownerOf(a.key))! - order.get(ownerOf(b.key))! ||
               a.rank - b.rank,
         ),
      );
   }
   const covers = withCandidates
      .map((r) => r.target.index)
      .sort((a, b) => a - b);
   const id = ownerOf(anchor.candidates[0].key);
   return {
      clusters: [
         {
            home: id,
            score: 0,
            covers,
            newlyCovers: covers,
            twins: [],
            contenders: [],
            peers,
            reach: pool,
            homeReach: new Map([[id, pool.get(id)!]]),
            candidates,
            extras,
         },
      ],
      unresolved,
      considered: [],
   };
}

function rankInCluster(
   index: FcjIndex,
   target: FindTarget,
   query: Float32Array | undefined,
   inPool: (node: CatalogNode) => boolean,
   pool: ReadonlyMap<string, ReachEntry>,
   settings: ConnectSettings,
   phraseVectors?: ReadonlyMap<string, Float32Array>,
): ClusterCandidate[] {
   const ranked = rankTarget(index, target, query, inPool, {
      keyword: settings.keyword,
      values: settings.values,
      depth: Number.MAX_SAFE_INTEGER,
      ...(phraseVectors ? { phraseVectors } : {}),
   });
   return ranked.all
      .map((c) => {
         const node = index.catalog.nodes.get(c.key)!;
         let key = c.key;
         let factor = 1;
         if (!pool.has(node.sourceKey)) {
            // Held only through a copy: rank the copy nearest the home.
            const copies = (node.copies ?? [])
               .map((k) => index.catalog.nodes.get(k)!)
               .filter((n) => pool.has(n.sourceKey))
               .sort(
                  (a, b) =>
                     (pool.get(a.sourceKey)?.hops ?? 0) -
                        (pool.get(b.sourceKey)?.hops ?? 0) ||
                     (a.key < b.key ? -1 : 1),
               );
            if (copies[0]) {
               key = copies[0].key;
               factor = INHERIT_DISCOUNT;
            }
         }
         const owner = index.catalog.nodes.get(key)!.sourceKey;
         const hops = pool.get(owner)?.hops ?? 0;
         return {
            key,
            score: c.score * factor * settings.hopDiscount ** hops,
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

/**
 * `home` is the cluster's home, whose source node is an extra; a list has
 * none. `dateSources` are the sources whose dates are added when a target
 * names a period: the home in a cluster, the best measures' sources in a list.
 */
function extrasOf(
   index: FcjIndex,
   home: string | undefined,
   dateSources: readonly string[],
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
   const homeSource =
      home === undefined ? undefined : index.catalog.sources.get(home);
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
            // A dependency on a folded copy is the field it copies.
            add(
               index.catalog.nodes.get(dep)?.copyOf ?? dep,
               `${node!.name} is computed from it`,
            );
         }
      }
   }
   // The date sources' dates when a target names a period.
   if (targets.some((t) => mentionsPeriod(t.text))) {
      for (const key of dateSources) {
         const source = index.catalog.sources.get(key);
         if (!source) continue;
         const reason =
            key === home
               ? "a date of the home source, and a target names a period"
               : `a date of ${source.name}, and a target names a period`;
         const dates = source.fields
            .map((k) => index.catalog.nodes.get(k)!)
            .filter(
               (n) =>
                  n.kind === "dimension" && DATE_TYPES.has(n.dataType ?? ""),
            )
            .slice(0, MAX_DATE_EXTRAS);
         for (const d of dates) add(d.key, reason);
      }
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
