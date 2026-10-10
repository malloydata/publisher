// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Stage 1, find: for each search target, the N nodes of the right type in
 * the whole catalog most likely to be what it asks for. No structure yet and
 * no model call.
 *
 * Three ranked lists per target, fused by reciprocal rank (k = 60):
 *
 * - closeness: the cosine of the phrase against every embedding row of a
 *   node of the right kind, best row per node;
 * - keyword: BM25 over the same nodes' texts;
 * - value hits (dimension and `dimensional_value` targets): the dimensions
 *   whose stored values match the target's example values, or the parts of
 *   its phrase that look like a value.
 *
 * Fusing by rank, not by score, because the three scores are not on one
 * scale: a cosine of 0.6 and a BM25 of 7 say nothing about each other, while
 * "first on both lists" means the same on any package. There is no floor and
 * no per-source window: the only cut is N per target, so this is the one
 * stage that can lose a field for text reasons, and recall at N is its
 * measurement.
 */

import type { CatalogNode } from "./catalog";
import type { FcjIndex } from "./index_build";
import { valueSpans } from "./value_tokens";
import type { ValueMatch } from "./values_index";
import { bestFacet } from "./vectors";
import { terms } from "./keyword";

/** The reciprocal-rank constant; 60 is the value the method was published with. */
export const RRF_K = 60;

/** How deep each list is read before fusing. */
export const LIST_DEPTH = 200;

export type FindTargetType =
   | "source"
   | "dimension"
   | "measure"
   | "view"
   | "join"
   | "dimensional_value";

/** One search target as the engine works on it. */
export interface FindTarget {
   /** Position in the caller's `search_targets`. */
   index: number;
   type: FindTargetType;
   text: string;
   /** Node kinds the target may return. */
   kinds: readonly string[];
   /** Literals the caller extracted, for the value index. */
   exampleValues?: readonly string[];
}

/** The node kinds each target type selects. */
export const KINDS_BY_FIND_TARGET: Record<FindTargetType, readonly string[]> = {
   source: ["source"],
   dimension: ["dimension"],
   measure: ["measure"],
   view: ["view", "query"],
   join: ["join"],
   dimensional_value: ["dimension"],
};

export interface Candidate {
   key: string;
   /** Fused score: the sum of 1 / (k + rank) over the lists the node is on. */
   score: number;
   /** 1-based rank in each list it is on. */
   ranks: { close?: number; keyword?: number; value?: number };
   cosine?: number;
   /** The embedding row that gave the cosine (`derived`, `q1`, ...). */
   facet?: string;
   bm25?: number;
   /** The stored values that nominated it, best first. */
   values?: ValueMatch[];
   valueScore?: number;
}

export interface FindResult {
   target: FindTarget;
   candidates: Candidate[];
   /** Length of each list before fusing, for the trace. */
   lists: { close: number; keyword: number; value: number };
   /** The phrases the value index was asked about. */
   valuePhrases: string[];
}

/** The value phrases of a target: its example values, else its value-looking parts. */
export function valuePhrasesOf(
   target: FindTarget,
   inVocabulary: (term: string) => boolean,
): string[] {
   if (target.exampleValues && target.exampleValues.length > 0) {
      return [...target.exampleValues];
   }
   if (target.type === "dimensional_value") {
      return [target.text, ...valueSpans(target.text, inVocabulary)].filter(
         (p, i, all) => all.indexOf(p) === i,
      );
   }
   if (target.type === "dimension")
      return valueSpans(target.text, inVocabulary);
   return [];
}

/** Fuse ranked key lists by reciprocal rank; ties by key. */
export function fuseByRank(
   lists: ReadonlyArray<readonly string[]>,
   k = RRF_K,
): Array<{ key: string; score: number; ranks: number[] }> {
   const out = new Map<string, { score: number; ranks: number[] }>();
   lists.forEach((list, li) => {
      list.forEach((key, i) => {
         const entry = out.get(key) ?? {
            score: 0,
            ranks: new Array(lists.length).fill(0),
         };
         entry.score += 1 / (k + i + 1);
         entry.ranks[li] = i + 1;
         out.set(key, entry);
      });
   });
   return [...out.entries()]
      .map(([key, v]) => ({ key, ...v }))
      .sort(
         (a, b) =>
            b.score - a.score || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0),
      );
}

/**
 * Score every node `allow` admits against one target, by the three lists.
 * Exported for the connect stage, which runs the same scoring inside one
 * cluster with no N cut.
 */
export function rankTarget(
   index: FcjIndex,
   target: FindTarget,
   query: Float32Array | undefined,
   allow: (node: CatalogNode) => boolean,
   options: { keyword: boolean; values: boolean; depth: number },
): Omit<FindResult, "target"> & { all: Candidate[] } {
   const admitted = new Map<string, CatalogNode>();
   for (const node of index.catalog.nodes.values()) {
      if (target.kinds.includes(node.kind) && allow(node))
         admitted.set(node.key, node);
   }
   // A value target's phrase is the value; how close it is to a field's name
   // says little, so it ranks by values and keywords only.
   const useClose = target.type !== "dimensional_value" && query !== undefined;
   const close: Array<{ key: string; cosine: number; facet: string }> = [];
   if (useClose) {
      for (const key of admitted.keys()) {
         const best = bestFacet(query, index.vectors.get(key));
         if (best) close.push({ key, cosine: best.score, facet: best.facet });
      }
      close.sort((a, b) => b.cosine - a.cosine || (a.key < b.key ? -1 : 1));
   }
   const keyword = options.keyword
      ? index.keyword.search(target.text, (k) => admitted.has(k))
      : [];
   const phrases = options.values
      ? valuePhrasesOf(target, (t) => index.keyword.hasTerm(t))
      : [];
   const nominations =
      phrases.length > 0 &&
      (target.type === "dimension" || target.type === "dimensional_value")
         ? index.values.nominate(phrases, (d) => admitted.has(d))
         : [];
   const closeTop = close.slice(0, options.depth);
   const keywordTop = keyword.slice(0, options.depth);
   const valueTop = nominations.slice(0, options.depth);
   const fused = fuseByRank([
      closeTop.map((c) => c.key),
      keywordTop.map((c) => c.key),
      valueTop.map((c) => c.dimension),
   ]);
   const closeBy = new Map(closeTop.map((c) => [c.key, c]));
   const keywordBy = new Map(keywordTop.map((c) => [c.key, c]));
   const valueBy = new Map(valueTop.map((c) => [c.dimension, c]));
   const all = fused.map<Candidate>((f) => {
      const c = closeBy.get(f.key);
      const kw = keywordBy.get(f.key);
      const v = valueBy.get(f.key);
      return {
         key: f.key,
         score: f.score,
         ranks: {
            ...(f.ranks[0] ? { close: f.ranks[0] } : {}),
            ...(f.ranks[1] ? { keyword: f.ranks[1] } : {}),
            ...(f.ranks[2] ? { value: f.ranks[2] } : {}),
         },
         ...(c ? { cosine: round4(c.cosine), facet: c.facet } : {}),
         ...(kw ? { bm25: round4(kw.score) } : {}),
         ...(v ? { values: v.matches, valueScore: v.score } : {}),
      };
   });
   return {
      candidates: all,
      all,
      lists: {
         close: close.length,
         keyword: keyword.length,
         value: nominations.length,
      },
      valuePhrases: phrases,
   };
}

function round4(x: number): number {
   return Math.round(x * 10_000) / 10_000;
}

/** Stage 1 over every target: the top `perTarget` candidates of each. */
export function find(args: {
   index: FcjIndex;
   targets: readonly FindTarget[];
   /** Unit query vector by target index; absent with no embedding provider. */
   queries: ReadonlyMap<number, Float32Array>;
   perTarget: number;
   keyword: boolean;
   values: boolean;
   /** The request's scope. */
   allow: (node: CatalogNode) => boolean;
}): FindResult[] {
   return args.targets.map((target) => {
      const ranked = rankTarget(
         args.index,
         target,
         args.queries.get(target.index),
         args.allow,
         { keyword: args.keyword, values: args.values, depth: LIST_DEPTH },
      );
      return {
         target,
         candidates: ranked.all.slice(0, args.perTarget),
         lists: ranked.lists,
         valuePhrases: ranked.valuePhrases,
      };
   });
}

/** Whether a phrase mentions a period, for the connect stage's date extras. */
export function mentionsPeriod(text: string): boolean {
   const t = terms(text);
   return (
      t.some((w) =>
         [
            "year",
            "yearly",
            "annual",
            "quarter",
            "quarterly",
            "month",
            "monthly",
            "week",
            "weekly",
            "day",
            "daily",
            "date",
            "time",
            "trend",
            "over",
            "since",
            "between",
            "last",
            "ytd",
            "mtd",
            "recent",
            "period",
            "season",
            "hour",
            "hourly",
            "when",
         ].includes(w),
      ) || /\b(19|20)\d{2}\b/.test(text)
   );
}
