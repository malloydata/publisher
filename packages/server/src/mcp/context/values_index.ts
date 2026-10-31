// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The value index: the stored values of `#(index)` dimensions on ungated
 * sources, plus every value list a doc gives, searchable by spelling.
 *
 * Only a `dimensional_value` target reads it: its text ("Denver") nominates
 * the dimensions that store that value and answers with the stored string. A
 * value list in a doc ("one of DTC, B2B, Amazon") seeds its dimension even
 * without a tag.
 *
 * Governance. Values are data, so only sources with no gate are read, and
 * they are read once at load through the ordinary query path with no givens,
 * so a source the query path would refuse contributes nothing. A gated
 * source's values are never stored here and never embedded; they are
 * reachable only through the query-time lookup with the caller's givens
 * (get_context_values.ts). The values of `#(index)` dimensions on ungated
 * sources, and the value lists docs give, ARE embedded with
 * `retrieval.fcj.valueEmbeddings` on (the default): the modeler opted those
 * dimensions in with the tag, so their values go to the embedding provider,
 * as the matched values already go to the judge's model.
 *
 * Two ways to match, each giving the find stage its own ranked list:
 *
 * - Spelling (`search`), in order, stopping at the first that produces hits:
 *   exact after normalizing (case, accents, punctuation, spacing); word
 *   matches with edit distance up to 2 (see scoreValue); three-letter-gram
 *   overlap for longer strings.
 * - Meaning (`searchByMeaning`): the cosine of the phrase's embedding against
 *   each value's, so "sneakers" finds "Athletic Shoes" and "NYC" finds "New
 *   York". A value must reach MEANING_FLOOR (MEANING_SHORT_FLOOR for a phrase
 *   of three characters or fewer, whose embedding sits near many short
 *   strings) and be within MEANING_MARGIN of the phrase's best value.
 *
 * Either way, a value found in many dimensions ("N/A", "2024", "CO") is weak
 * evidence, so a dimension's nomination is divided by how many dimensions
 * store that value, the same idea as rare-word weighting in keyword search.
 */

import { QueryTimeoutError, ServiceUnavailableError } from "../../errors";
import { logger } from "../../logger";
import type { EnvironmentStore } from "../../service/environment_store";
import type { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import {
   editDistance,
   normalizeText,
   scoreValue,
   VALUE_MATCH_FLOOR,
} from "../tools/get_context_value_match";
import { queryDistinctValues, usableValue } from "../tools/get_context_values";
import type { Catalog, CatalogNode } from "./catalog";
import type { DocParts } from "./doc_parts";
import { dot } from "./vectors";

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

/** One read at load. Generous: it runs once per load, not per question. */
export const VALUE_READ_TIMEOUT_MS = 60_000;

/**
 * The read: the `limit` most frequent non-null values of one dimension,
 * through the normal query path, with no givens (an ungated source needs
 * none) and no `#(authorize)` bypass.
 */
export const defaultIndexValueFetcher: IndexValueFetcher = async (request) => {
   const rows = await queryDistinctValues({
      ...request,
      counted: true,
      routeLabel: "mcp:getContext:valueIndex",
   });
   const values: Array<{ value: string; count: number }> = [];
   for (const row of rows.slice(0, request.limit)) {
      const text = usableValue(row.v);
      if (text === undefined) continue;
      values.push({ value: text, count: Number(row.n) || 0 });
   }
   return { values, truncated: rows.length > request.limit };
};

/** One value that matched, and how. */
export interface ValueMatch {
   value: string;
   dimension: string;
   score: number;
   /** Spelling: exact, words or ngram. Meaning: the score is the cosine. */
   how: "exact" | "words" | "ngram" | "meaning";
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

/*
 * The meaning thresholds, set for OpenAI text-embedding-3-small at 512
 * dimensions (the engine's embedder). Measured cosines: "NYC" to "New York"
 * 0.80, "Big Apple" to "New York" 0.66, "sneakers" to "Athletic Shoes" 0.58,
 * "Calfornia" to "California" 0.64; unrelated values sit between 0.2 and
 * 0.45. "CO" is 0.61 to "CO2", 0.57 to "COD", 0.54 to "Co-op" and 0.45 to
 * "Colorado", so a short phrase needs the higher floor. Another embedder
 * needs its own numbers.
 */
/** Lowest cosine a value may have to match a phrase by meaning. */
export const MEANING_FLOOR = 0.5;
/** The floor for a phrase of at most MEANING_SHORT_CHARS letters and digits. */
export const MEANING_SHORT_FLOOR = 0.7;
export const MEANING_SHORT_CHARS = 3;
/** A value more than this below the phrase's best value does not match. */
export const MEANING_MARGIN = 0.15;
/** Most distinct values one phrase matches by meaning. */
export const MEANING_MAX_VALUES = 20;

/** The vectors of the distinct value strings, packed row by row. */
interface MeaningVectors {
   texts: string[];
   /** Rows of `values` holding each text, by position in `texts`. */
   rows: number[][];
   matrix: Float32Array;
   dims: number;
}

export class ValueIndex {
   readonly values: IndexedValue[];
   private readonly byNormalized = new Map<string, number[]>();
   private readonly byWord = new Map<string, number[]>();
   private readonly byTrigram = new Map<string, number[]>();
   /** Distinct dimensions storing each normalized value. */
   private readonly spread = new Map<string, number>();
   private meaning: MeaningVectors | undefined;

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

   /** The distinct value strings, each embedded once whatever its dimensions. */
   distinctValues(): string[] {
      return [...new Set(this.values.map((v) => v.value))];
   }

   /**
    * Attach unit vectors by value string (from the `value_embeddings` sync).
    * Values without a vector are matched by spelling only.
    */
   setVectors(byValue: ReadonlyMap<string, Float32Array>): void {
      const rowsOf = new Map<string, number[]>();
      this.values.forEach((v, i) => {
         if (byValue.has(v.value)) push(rowsOf, v.value, i);
      });
      const texts = [...rowsOf.keys()];
      if (texts.length === 0) {
         this.meaning = undefined;
         return;
      }
      const dims = byValue.get(texts[0])!.length;
      const kept = texts.filter((t) => byValue.get(t)!.length === dims);
      const matrix = new Float32Array(kept.length * dims);
      kept.forEach((t, i) => matrix.set(byValue.get(t)!, i * dims));
      this.meaning = {
         texts: kept,
         rows: kept.map((t) => rowsOf.get(t)!),
         matrix,
         dims,
      };
   }

   /** Distinct values that carry a vector. */
   get embedded(): number {
      return this.meaning?.texts.length ?? 0;
   }

   /**
    * Values that mean what `phrase` means, by the cosine of `vec` (the
    * phrase's unit vector) against each value's, best first. See the
    * thresholds above; every dimension holding a kept value is reported.
    */
   searchByMeaning(
      phrase: string,
      vec: Float32Array,
      allow: (dimension: string) => boolean = () => true,
   ): ValueMatch[] {
      const m = this.meaning;
      if (!m || vec.length !== m.dims) return [];
      const compact = normalizeText(phrase).replace(/ /g, "");
      if (!compact) return [];
      const floor =
         compact.length <= MEANING_SHORT_CHARS
            ? MEANING_SHORT_FLOOR
            : MEANING_FLOOR;
      const hits: Array<{ t: number; score: number }> = [];
      for (let t = 0; t < m.texts.length; t++) {
         const score = dot(
            vec,
            m.matrix.subarray(t * m.dims, (t + 1) * m.dims),
         );
         if (score < floor) continue;
         if (!m.rows[t].some((i) => allow(this.values[i].dimension))) continue;
         hits.push({ t, score });
      }
      if (hits.length === 0) return [];
      hits.sort((a, b) => b.score - a.score || a.t - b.t);
      const cut = hits[0].score - MEANING_MARGIN;
      const out: ValueMatch[] = [];
      for (const h of hits.slice(0, MEANING_MAX_VALUES)) {
         if (h.score < cut) break;
         for (const i of m.rows[h.t]) {
            const v = this.values[i];
            if (!allow(v.dimension)) continue;
            out.push({
               value: v.value,
               dimension: v.dimension,
               score: Math.round(Math.min(1, h.score) * 10_000) / 10_000,
               how: "meaning",
            });
         }
      }
      return sortMatches(dedupe(out));
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
      return this.nominateFrom(phrases.map((p) => this.search(p, allow)));
   }

   /**
    * The same, by meaning: each phrase with its unit vector. A phrase with no
    * vector nominates nothing.
    */
   nominateByMeaning(
      phrases: ReadonlyArray<{ phrase: string; vec: Float32Array }>,
      allow: (dimension: string) => boolean = () => true,
   ): ValueNomination[] {
      return this.nominateFrom(
         phrases.map((p) => this.searchByMeaning(p.phrase, p.vec, allow)),
      );
   }

   private nominateFrom(
      perPhrase: ReadonlyArray<readonly ValueMatch[]>,
   ): ValueNomination[] {
      const byDim = new Map<string, ValueNomination>();
      for (const matches of perPhrase) {
         for (const m of matches) {
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

/** One match per (value, dimension). */
function dedupe(list: ValueMatch[]): ValueMatch[] {
   const seen = new Set<string>();
   return list.filter((m) => {
      const k = `${m.dimension}\u0000${m.value}`;
      if (seen.has(k)) return false;
      seen.add(k);
      return true;
   });
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
   /**
    * `#(index)` dimensions whose read was refused for a reason that clears on
    * its own: the server was at its concurrent-query cap, or the read timed
    * out. Their values are missing from this build, and the engine rebuilds
    * the index after a cool-down (engine_state.ts). Any other failed read is
    * in `skipped`, since reading again would fail the same way.
    */
   failed: Array<{ dimension: string; reason: string }>;
   ms: number;
}

/** A read that failed for a reason that clears on its own, so it is worth reading again. */
export function isTransientReadFailure(error: unknown): boolean {
   return (
      error instanceof ServiceUnavailableError ||
      error instanceof QueryTimeoutError
   );
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
      failed: [],
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
         (isTransientReadFailure(error) ? report.failed : report.skipped).push({
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
