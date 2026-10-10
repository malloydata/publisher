// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The value index: the stored values of `#(index)` dimensions on ungated
 * sources, plus every value list a doc gives, searchable by spelling.
 *
 * Three uses, one index. A dimension target whose phrase carries a value
 * ("orders shipped to Denver") nominates the dimensions that store it; a
 * `dimensional_value` target answers with the stored string; and a value list
 * in a doc ("one of DTC, B2B, Amazon") seeds its dimension even without a tag.
 *
 * Governance. Values are data, so only sources with no gate are read, and
 * they are read once at load through the ordinary query path with no givens,
 * so a source the query path would refuse contributes nothing. A gated
 * source's values are never stored here; they are reachable only through the
 * query-time lookup with the caller's givens (get_context_values.ts). Values
 * are matched in the process and are never embedded (an embedding would send
 * them to the provider, which only the `full` egress preset allows; that
 * option is not built).
 *
 * Matching, in order, stopping at the first that produces hits: exact after
 * normalizing (case, accents, punctuation, spacing); word matches with edit
 * distance up to 2 (see scoreValue); three-letter-gram overlap for longer
 * strings. A value found in many dimensions ("N/A", "2024", "CO") is weak
 * evidence, so a dimension's nomination is divided by how many dimensions
 * store that value, the same idea as rare-word weighting in keyword search.
 */

import type { GivenValue } from "@malloydata/malloy";
import { logger } from "../../logger";
import { tryAcquireQuerySlot } from "../../query_concurrency";
import { runWithQueryTimeout } from "../../query_timeout";
import { quoteMalloyIdentifier } from "../../service/authorize";
import type { EnvironmentStore } from "../../service/environment_store";
import type { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { getModelForQuery } from "../handler_utils";
import {
   editDistance,
   normalizeText,
   scoreValue,
   VALUE_MATCH_FLOOR,
} from "../tools/get_context_value_match";
import type { Catalog, CatalogNode } from "./catalog";
import type { DocParts } from "./doc_parts";

/** One stored value of one dimension. */
export interface IndexedValue {
   value: string;
   normalized: string;
   /** Rows holding the value; 0 for a value a doc lists. */
   count: number;
   /** Node key of the dimension. */
   dimension: string;
   origin: "data" | "doc";
}

/** What one data read returns, most frequent first. */
export interface CountedValues {
   values: Array<{ value: string; count: number }>;
   /** More distinct values exist than were read. */
   truncated: boolean;
}

export interface IndexValueRequest {
   environmentStore: EnvironmentStore;
   environmentName: string;
   packageName: string;
   modelPath: string;
   source: string;
   dimension: string;
   /** Most values to read; one more is asked for to learn whether there are more. */
   limit: number;
   timeoutMs: number;
   givens?: Record<string, unknown>;
}

export type IndexValueFetcher = (
   request: IndexValueRequest,
) => Promise<CountedValues>;

/** Values longer than this are free text, not a category worth matching. */
const MAX_VALUE_CHARS = 200;

/** One read at load. Generous: it runs once per load, not per question. */
export const VALUE_READ_TIMEOUT_MS = 60_000;

/**
 * The read: the `limit` most frequent non-null values of one dimension,
 * through the normal query path, with no givens (an ungated source needs
 * none) and no `#(authorize)` bypass.
 */
export const defaultIndexValueFetcher: IndexValueFetcher = async (request) => {
   const found = await getModelForQuery(
      request.environmentStore,
      request.environmentName,
      request.packageName,
      request.modelPath,
   );
   if ("error" in found) throw new Error(found.error.message);
   const dim = quoteMalloyIdentifier(request.dimension);
   const query = `run: ${quoteMalloyIdentifier(request.source)} -> { where: ${dim} != null group_by: v is ${dim} aggregate: n is count() order_by: n desc limit: ${request.limit + 1} }`;
   const slot = tryAcquireQuerySlot("mcp:getContext:valueIndex");
   try {
      const { compactResult } = await runWithQueryTimeout(
         (signal) =>
            found.model.getQueryResults(
               undefined,
               undefined,
               query,
               undefined,
               undefined,
               request.givens as Record<string, GivenValue> | undefined,
               signal,
               undefined,
               "compact",
               false,
               false,
            ),
         request.timeoutMs,
      );
      const rows = compactResult as unknown as Array<Record<string, unknown>>;
      const values: Array<{ value: string; count: number }> = [];
      for (const row of rows.slice(0, request.limit)) {
         if (row.v === null || row.v === undefined) continue;
         const text = String(row.v);
         if (text.trim() === "" || text.length > MAX_VALUE_CHARS) continue;
         values.push({ value: text, count: Number(row.n) || 0 });
      }
      return { values, truncated: rows.length > request.limit };
   } finally {
      slot.release();
   }
};

/** One value that matched, and how. */
export interface ValueMatch {
   value: string;
   dimension: string;
   score: number;
   how: "exact" | "words" | "ngram";
}

/** A dimension a phrase nominated, with the values that did it. */
export interface ValueNomination {
   dimension: string;
   /** Best match score divided by how many dimensions store that value. */
   score: number;
   matches: ValueMatch[];
}

function trigrams(normalized: string): Set<string> {
   const t = ` ${normalized} `;
   const out = new Set<string>();
   for (let i = 0; i + 3 <= t.length; i++) out.add(t.slice(i, i + 3));
   return out;
}

/** Strings at least this long are matched by n-gram overlap when nothing else matched. */
const NGRAM_MIN_CHARS = 6;
const NGRAM_MIN_OVERLAP = 0.5;

export class ValueIndex {
   readonly values: IndexedValue[];
   private readonly byNormalized = new Map<string, number[]>();
   private readonly byWord = new Map<string, number[]>();
   private readonly byTrigram = new Map<string, number[]>();
   /** Distinct dimensions storing each normalized value. */
   private readonly spread = new Map<string, number>();

   constructor(values: IndexedValue[]) {
      this.values = values;
      const dims = new Map<string, Set<string>>();
      values.forEach((v, i) => {
         push(this.byNormalized, v.normalized, i);
         for (const w of new Set(v.normalized.split(" "))) {
            if (w) push(this.byWord, w, i);
         }
         if (v.normalized.length >= NGRAM_MIN_CHARS) {
            for (const g of trigrams(v.normalized)) push(this.byTrigram, g, i);
         }
         const set = dims.get(v.normalized) ?? new Set<string>();
         set.add(v.dimension);
         dims.set(v.normalized, set);
      });
      for (const [k, s] of dims) this.spread.set(k, s.size);
   }

   get size(): number {
      return this.values.length;
   }

   /** How many dimensions store this value (after normalizing). */
   spreadOf(value: string): number {
      return this.spread.get(normalizeText(value)) ?? 0;
   }

   /**
    * Every matching value, best first. The rules apply in order PER
    * DIMENSION: a dimension with an exact match reports only those, one with
    * none falls through to word matches, then to n-grams. Per dimension, not
    * for the whole index, so an exact "Denver" in one table does not hide
    * "Denver International (DIA)" in another; which of the two the question
    * means is the connect stage's call, not this one's.
    */
   search(phrase: string, allow: (dimension: string) => boolean = () => true) {
      const norm = normalizeText(phrase);
      if (!norm) return [];
      const exact = (this.byNormalized.get(norm) ?? [])
         .map((i) => this.values[i])
         .filter((v) => allow(v.dimension))
         .map<ValueMatch>((v) => ({
            value: v.value,
            dimension: v.dimension,
            score: v.value === phrase ? 1 : 0.98,
            how: "exact",
         }));
      const done = new Set(exact.map((m) => m.dimension));
      const open = (dimension: string) =>
         allow(dimension) && !done.has(dimension);

      // Words: every value sharing a word with the phrase, or holding a word
      // within two edits of one of its words.
      const candidates = new Set<number>();
      const words = norm.split(" ").filter(Boolean);
      for (const w of words) {
         for (const i of this.byWord.get(w) ?? []) candidates.add(i);
         if (w.length < 4) continue;
         const limit = w.length <= 5 ? 1 : 2;
         for (const [vw, idx] of this.byWord) {
            if (vw === w || Math.abs(vw.length - w.length) > limit) continue;
            if (
               vw.startsWith(w) ||
               w.startsWith(vw) ||
               editDistance(w, vw, limit) <= limit
            ) {
               for (const i of idx) candidates.add(i);
            }
         }
      }
      const byWords: ValueMatch[] = [];
      for (const i of candidates) {
         const v = this.values[i];
         if (!open(v.dimension)) continue;
         const score = scoreValue(phrase, v.value);
         if (score >= VALUE_MATCH_FLOOR) {
            byWords.push({
               value: v.value,
               dimension: v.dimension,
               score,
               how: "words",
            });
         }
      }
      for (const m of byWords) done.add(m.dimension);

      if (norm.length < NGRAM_MIN_CHARS) {
         return sortMatches([...exact, ...byWords]);
      }
      const grams = trigrams(norm);
      const overlap = new Map<number, number>();
      for (const g of grams) {
         for (const i of this.byTrigram.get(g) ?? []) {
            overlap.set(i, (overlap.get(i) ?? 0) + 1);
         }
      }
      const byGrams: ValueMatch[] = [];
      for (const [i, shared] of overlap) {
         const v = this.values[i];
         if (!open(v.dimension)) continue;
         const theirs = trigrams(v.normalized).size;
         const jaccard = shared / (grams.size + theirs - shared);
         if (jaccard >= NGRAM_MIN_OVERLAP) {
            byGrams.push({
               value: v.value,
               dimension: v.dimension,
               score: Math.round((0.5 + 0.4 * jaccard) * 10_000) / 10_000,
               how: "ngram",
            });
         }
      }
      return sortMatches([...exact, ...byWords, ...byGrams]);
   }

   /**
    * The dimensions `phrases` nominate: per dimension, its best match score
    * divided by the spread of the matched value, best first.
    */
   nominate(
      phrases: readonly string[],
      allow: (dimension: string) => boolean = () => true,
   ): ValueNomination[] {
      const byDim = new Map<string, ValueNomination>();
      for (const phrase of phrases) {
         for (const m of this.search(phrase, allow)) {
            const weight = m.score / Math.max(1, this.spreadOf(m.value));
            const nom = byDim.get(m.dimension) ?? {
               dimension: m.dimension,
               score: 0,
               matches: [],
            };
            nom.score = Math.max(
               nom.score,
               Math.round(weight * 10_000) / 10_000,
            );
            if (!nom.matches.some((x) => x.value === m.value))
               nom.matches.push(m);
            byDim.set(m.dimension, nom);
         }
      }
      for (const nom of byDim.values())
         nom.matches = sortMatches(nom.matches).slice(0, 10);
      return [...byDim.values()].sort(
         (a, b) =>
            b.score - a.score ||
            (a.dimension < b.dimension
               ? -1
               : a.dimension > b.dimension
                 ? 1
                 : 0),
      );
   }
}

function push(map: Map<string, number[]>, key: string, i: number) {
   const list = map.get(key);
   if (list) list.push(i);
   else map.set(key, [i]);
}

function sortMatches(list: ValueMatch[]): ValueMatch[] {
   return list.sort(
      (a, b) =>
         b.score - a.score ||
         a.value.length - b.value.length ||
         (a.value < b.value ? -1 : a.value > b.value ? 1 : 0) ||
         (a.dimension < b.dimension ? -1 : 1),
   );
}

/** What the value build did, for the build report. */
export interface ValueBuildReport {
   dimensionsRead: number;
   valuesFromData: number;
   valuesFromDocs: number;
   /** Dimensions with more distinct values than the cap: the most frequent were kept. */
   cut: Array<{ dimension: string; kept: number }>;
   /** `#(index)` dimensions not read, and why. */
   skipped: Array<{ dimension: string; reason: string }>;
   ms: number;
}

/** Whether a node's values may be read into the shared index. */
function readable(
   catalog: Catalog,
   node: CatalogNode,
): { ok: true } | { ok: false; reason: string } {
   if (!node.indexed) return { ok: false, reason: "not tagged #(index)" };
   if (node.joinPath) return { ok: false, reason: "a field of a join" };
   if (catalog.sources.get(node.sourceKey)?.gated) {
      return {
         ok: false,
         reason:
            "its source has an access gate; values are read only through a query with the caller's givens",
      };
   }
   if (node.dataType && node.dataType !== "string") {
      return {
         ok: false,
         reason: `a ${node.dataType} dimension; only strings are indexed`,
      };
   }
   return { ok: true };
}

export async function buildValueIndex(args: {
   catalog: Catalog;
   parts: ReadonlyMap<string, DocParts>;
   cap: number;
   read?: {
      environmentStore: EnvironmentStore;
      environmentName: string;
      packageName: string;
      fetcher?: IndexValueFetcher;
   };
   db?: DuckDBConnection;
   shouldStop?: () => boolean;
}): Promise<{ index: ValueIndex; report: ValueBuildReport }> {
   const started = Date.now();
   const values: IndexedValue[] = [];
   const report: ValueBuildReport = {
      dimensionsRead: 0,
      valuesFromData: 0,
      valuesFromDocs: 0,
      cut: [],
      skipped: [],
      ms: 0,
   };
   for (const node of args.catalog.nodes.values()) {
      if (node.kind !== "dimension" || node.copyOf) continue;
      for (const value of args.parts.get(node.key)?.values ?? []) {
         const normalized = normalizeText(value);
         if (!normalized) continue;
         values.push({
            value,
            normalized,
            count: 0,
            dimension: node.key,
            origin: "doc",
         });
         report.valuesFromDocs += 1;
      }
      if (!node.indexed) continue;
      const check = readable(args.catalog, node);
      if (!check.ok) {
         report.skipped.push({ dimension: node.key, reason: check.reason });
         continue;
      }
      if (!args.read) {
         report.skipped.push({
            dimension: node.key,
            reason: "no data read in this build",
         });
         continue;
      }
      if (args.shouldStop?.()) break;
      const fetcher = args.read.fetcher ?? defaultIndexValueFetcher;
      try {
         const got = await fetcher({
            environmentStore: args.read.environmentStore,
            environmentName: args.read.environmentName,
            packageName: args.read.packageName,
            modelPath: node.modelPaths[0],
            source: node.source,
            dimension: node.name,
            limit: args.cap,
            timeoutMs: VALUE_READ_TIMEOUT_MS,
         });
         report.dimensionsRead += 1;
         if (got.truncated)
            report.cut.push({ dimension: node.key, kept: got.values.length });
         for (const { value, count } of got.values) {
            const normalized = normalizeText(value);
            if (!normalized) continue;
            values.push({
               value,
               normalized,
               count,
               dimension: node.key,
               origin: "data",
            });
            report.valuesFromData += 1;
         }
      } catch (error) {
         report.skipped.push({
            dimension: node.key,
            reason: `the read failed: ${error instanceof Error ? error.message.slice(0, 200) : String(error)}`,
         });
      }
   }
   if (args.db && args.read) {
      try {
         await saveValues(
            args.db,
            args.read.environmentName,
            args.read.packageName,
            values,
         );
      } catch (error) {
         logger.warn(
            "[get_context fcj] The value index could not be written to its table",
            {
               error: error instanceof Error ? error.message : String(error),
            },
         );
      }
   }
   report.ms = Date.now() - started;
   return { index: new ValueIndex(values), report };
}

async function saveValues(
   db: DuckDBConnection,
   environmentName: string,
   packageName: string,
   values: readonly IndexedValue[],
): Promise<void> {
   await db.run(
      `DELETE FROM dimension_values WHERE environment_name = ? AND package_name = ?`,
      [environmentName, packageName],
   );
   const now = new Date().toISOString();
   const seen = new Set<string>();
   const rows: unknown[][] = [];
   for (const v of values) {
      const id = `${v.dimension}\u0000${v.value}\u0000${v.origin}`;
      if (seen.has(id)) continue;
      seen.add(id);
      rows.push([
         environmentName,
         packageName,
         v.dimension,
         v.value,
         v.normalized,
         v.count,
         v.origin,
         now,
      ]);
   }
   for (let i = 0; i < rows.length; i += 500) {
      const batch = rows.slice(i, i + 500);
      await db.run(
         `INSERT INTO dimension_values (environment_name, package_name, dimension_key, value, normalized, count, origin, updated_at)
          VALUES ${batch.map(() => "(?, ?, ?, ?, ?, ?, ?, ?)").join(", ")}`,
         batch.flat(),
      );
   }
}
