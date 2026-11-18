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
 * (../search/values.ts). Tagging a dimension `#(index)` is the author's
 * consent for its values to be searched, so the values read from ungated
 * sources go to whatever provider the operator configured: the judge sees
 * them, the `values` question wording shows them to the LLM, and
 * `retrieval.fcj.valueEmbeddings` (on by default) embeds them. With no
 * provider configured they are matched by spelling alone and sent nowhere.
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

import { createHash } from "crypto";
import { QueryTimeoutError, ServiceUnavailableError } from "../../errors";
import { logger } from "../../logger";
import type { PackageLookup } from "../../service/environment_ports";
import type { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import {
   editDistance,
   normalizeText,
   scoreValue,
   VALUE_MATCH_FLOOR,
} from "../search/value_match";
import {
   queryDistinctValues,
   usableValue,
   type ValueQueryDeps,
} from "../search/values";
import type { Catalog, CatalogNode } from "../model/catalog";
import type { DocParts } from "./doc_parts";
import { dot } from "./vectors";

/** One stored value of one dimension. */
interface IndexedValue {
   value: string;
   normalized: string;
   /** Rows holding the value; 0 for a value a doc lists. */
   count: number;
   /** Node key of the dimension. */
   dimension: string;
   origin: "data" | "doc";
   /**
    * For a source whose gate splits its rows (CatalogSource.partitionKey):
    * the key the value was read under, from {@link partitionKeyText}. A
    * search returns it only to a caller whose givens name that key.
    */
   key?: string;
}

/**
 * Which stored values a search may return: by dimension, and, for a value
 * stored under a key, by that key.
 */
export type ValueAllow = (dimension: string, key?: string) => boolean;

/**
 * One part of a partition key as text, so the database's value and the
 * caller's given compare equal: `362023`, `362023.0` and `"362023"` are one
 * key, and a one-element list is its element. Hosted Credible keys the same
 * way (canonical_key_part).
 */
export function partitionKeyPart(raw: unknown): string | undefined {
   const v = Array.isArray(raw) && raw.length === 1 ? raw[0] : raw;
   if (v === null || v === undefined || Array.isArray(v)) return undefined;
   if (v instanceof Date) {
      const iso = v.toISOString();
      return iso.endsWith("T00:00:00.000Z") ? iso.slice(0, 10) : iso;
   }
   if (typeof v === "number" || typeof v === "bigint") return String(Number(v));
   const text = String(v);
   const n = Number(text);
   return text.trim() !== "" && Number.isFinite(n) ? String(n) : text;
}

/**
 * The key the caller's givens name for each dimension's source, by dimension
 * node key: undefined when the source is not split by a key, or when the
 * caller did not send every given its key needs (then none of its stored
 * values match).
 */
export function callerKeyOf(
   catalog: Catalog,
   givens: Readonly<Record<string, unknown>> | undefined,
): (dimension: string) => string | undefined {
   return (dimension) => {
      const node = catalog.nodes.get(dimension);
      const key = node && catalog.sources.get(node.sourceKey)?.partitionKey;
      if (!key) return undefined;
      return partitionKeyText(key.map((p) => givens?.[p.given]));
   };
}

/** A whole partition key as text, or undefined when a part is missing. */
export function partitionKeyText(
   parts: readonly unknown[],
): string | undefined {
   const out: string[] = [];
   for (const raw of parts) {
      const part = partitionKeyPart(raw);
      if (part === undefined) return undefined;
      out.push(part);
   }
   return out.join("\u0000");
}

/** What one data read returns, most frequent first. */
interface CountedValues {
   /** `key` is set when the read named partition columns. */
   values: Array<{ value: string; count: number; key?: string }>;
   /** More distinct values exist than were read. */
   truncated: boolean;
}

export interface IndexValueRequest {
   environmentStore: PackageLookup;
   environmentName: string;
   packageName: string;
   modelPath: string;
   source: string;
   dimension: string;
   /** Most values to read; one more is asked for to learn whether there are more. */
   limit: number;
   timeoutMs: number;
   givens?: Record<string, unknown>;
   /** Return each value's key; see DistinctValueQuery.partitionColumns. */
   partitionColumns?: readonly string[];
}

export type IndexValueFetcher = (
   request: IndexValueRequest,
) => Promise<CountedValues>;

/** One read at load. Generous: it runs once per load, not per question. */
const VALUE_READ_TIMEOUT_MS = 60_000;

/**
 * The most time one build spends reading values, across all of a package's
 * `#(index)` dimensions. Reads run one at a time, so without it a package
 * with many slow dimensions would hold the build for dimensions x 60s.
 * Dimensions not reached are reported as failed, so the next build (after a
 * cool-down) reads them, and only them.
 */
const VALUE_READ_BUDGET_MS = 300_000;

/**
 * How long a stored read is reused while its source's definition is
 * unchanged (`dimension_value_reads`). A model edit, a reload and a restart
 * all reuse it; past this age the data is read again, so values still follow
 * the warehouse.
 */
const VALUE_REUSE_MAX_AGE_MS = 24 * 60 * 60 * 1000;

/** One stored read: one dimension under one combination of givens. */
interface StoredRead {
   dimension: string;
   /** The combination as JSON, "" with none (givensId). */
   givens: string;
   readKey: string;
   truncated: boolean;
   values: CountedValues["values"];
   readAt: number;
}

/**
 * The read: the `limit` most frequent non-null values of one dimension,
 * through the normal query path and every gate, with the configured givens
 * (none for an ungated source). For a source whose gate splits its rows,
 * each row also carries its key; see DistinctValueQuery.partitionColumns.
 */
export const indexValueFetcherFor =
   (deps: ValueQueryDeps): IndexValueFetcher =>
   async (request) => {
      const rows = await queryDistinctValues(
         {
            ...request,
            counted: true,
            system: true,
            routeLabel: "mcp:getContext:valueIndex",
         },
         deps,
      );
      const values: CountedValues["values"] = [];
      const keyCount = request.partitionColumns?.length ?? 0;
      for (const row of rows.slice(0, request.limit)) {
         const text = usableValue(row.v);
         if (text === undefined) continue;
         if (keyCount === 0) {
            values.push({ value: text, count: Number(row.n) || 0 });
            continue;
         }
         // A row whose key is null belongs to no caller: drop it.
         const key = partitionKeyText(
            Array.from({ length: keyCount }, (_, i) => row[`k${i}`]),
         );
         if (key === undefined) continue;
         values.push({ value: text, count: Number(row.n) || 0, key });
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
interface ValueNomination {
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
const MEANING_SHORT_CHARS = 3;
/** A value more than this below the phrase's best value does not match. */
export const MEANING_MARGIN = 0.15;
/** Most distinct values one phrase matches by meaning. */
const MEANING_MAX_VALUES = 20;

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
      allow: ValueAllow = () => true,
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
         if (!m.rows[t].some((i) => allowed(allow, this.values[i]))) continue;
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
            if (!allowed(allow, v)) continue;
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
   search(phrase: string, allow: ValueAllow = () => true) {
      const norm = normalizeText(phrase);
      if (!norm) return [];
      const exact = (this.byNormalized.get(norm) ?? [])
         .map((i) => this.values[i])
         .filter((v) => allowed(allow, v))
         .map<ValueMatch>((v) => ({
            value: v.value,
            dimension: v.dimension,
            score: v.value === phrase ? 1 : 0.98,
            how: "exact",
         }));
      const done = new Set(exact.map((m) => m.dimension));
      const open = (v: IndexedValue) =>
         allowed(allow, v) && !done.has(v.dimension);

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
         if (!open(v)) continue;
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
         if (!open(v)) continue;
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
      allow: ValueAllow = () => true,
   ): ValueNomination[] {
      return this.nominateFrom(phrases.map((p) => this.search(p, allow)));
   }

   /**
    * The same, by meaning: each phrase with its unit vector. A phrase with no
    * vector nominates nothing.
    */
   nominateByMeaning(
      phrases: ReadonlyArray<{ phrase: string; vec: Float32Array }>,
      allow: ValueAllow = () => true,
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

/**
 * Every combination of the configured givens' values, one per read; `[undefined]`
 * (one read with no givens) when none are configured.
 */
export function combinationsOf(
   givens: Readonly<Record<string, readonly unknown[]>> | undefined,
): Array<Record<string, unknown> | undefined> {
   const entries = Object.entries(givens ?? {});
   if (entries.length === 0) return [undefined];
   let out: Array<Record<string, unknown>> = [{}];
   for (const [name, list] of entries) {
      out = out.flatMap((c) => list.map((v) => ({ ...c, [name]: v })));
   }
   return out;
}

/**
 * One read per combination, merged. A value read under a key keeps it (two
 * combinations name two keys); a value with none is one value, its counts
 * summed. Truncated when any read was.
 */
async function readEachCombination(
   combinations: ReadonlyArray<Record<string, unknown> | undefined>,
   read: (
      givens: Record<string, unknown> | undefined,
      i: number,
   ) => Promise<CountedValues>,
): Promise<CountedValues> {
   if (combinations.length === 1) return read(combinations[0], 0);
   const merged = new Map<string, CountedValues["values"][number]>();
   let truncated = false;
   for (const [i, givens] of combinations.entries()) {
      const got = await read(givens, i);
      truncated ||= got.truncated;
      for (const v of got.values) {
         const id = `${v.key ?? ""}\u0000${v.value}`;
         const seen = merged.get(id);
         if (seen) seen.count += v.count;
         else merged.set(id, { ...v });
      }
   }
   return { values: [...merged.values()], truncated };
}

function allowed(allow: ValueAllow, v: IndexedValue): boolean {
   return v.key === undefined ? allow(v.dimension) : allow(v.dimension, v.key);
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
   /** Dimensions the replaced build read, whose values were kept rather than read again. */
   dimensionsReused: number;
   valuesFromData: number;
   valuesFromDocs: number;
   /** Dimensions with more distinct values than the cap: the most frequent were kept. */
   cut: Array<{ dimension: string; kept: number }>;
   /** `#(index)` dimensions not read, and why. */
   skipped: Array<{ dimension: string; reason: string }>;
   /**
    * `#(index)` dimensions whose read was refused for a reason that clears on
    * its own: the server was at its concurrent-query cap, or the read timed
    * out, or the build's read budget ran out before it. Their values are
    * missing from this build, and the engine rebuilds the index after a
    * cool-down that doubles each time (engine_state.ts), reading only these.
    * Any other failed read is in `skipped`, since reading again would fail
    * the same way.
    */
   failed: Array<{ dimension: string; reason: string }>;
   ms: number;
}

/** A read that failed for a reason that clears on its own, so it is worth reading again. */
function isTransientReadFailure(error: unknown): boolean {
   return (
      error instanceof ServiceUnavailableError ||
      error instanceof QueryTimeoutError
   );
}

/**
 * Whether a node's values may be read into the shared index. A source split
 * by a key is read only for the keys `givens` lists: reading every key at
 * once scans the whole table, and on a production model the cap then keeps
 * a sliver of the (value, key) pairs, so most keys would get nothing.
 */
function readable(
   catalog: Catalog,
   node: CatalogNode,
   givens: Readonly<Record<string, readonly unknown[]>> | undefined,
   packageName: string | undefined,
): { ok: true } | { ok: false; reason: string } {
   if (!node.indexed) return { ok: false, reason: "not tagged #(index)" };
   if (node.joinPath) return { ok: false, reason: "a field of a join" };
   const owner = catalog.sources.get(node.sourceKey);
   const unlisted = owner?.partitionKey
      ?.map((p) => p.given)
      .filter((g) => !givens?.[g]);
   if (unlisted?.length) {
      return {
         ok: false,
         reason:
            `its source's rows are split by ${unlisted.join(", ")}, and reading every value of it scans the whole table. ` +
            `Fix: list the ones to index in publisher.config.json, "retrieval": { "dimensionValues": { "givens": { "${packageName ?? "<package>"}": { "${unlisted[0]}": [...] } } } }`,
      };
   }
   if (owner?.gated && !owner.partitionKey) {
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
      environmentStore: PackageLookup;
      environmentName: string;
      packageName: string;
      /** The read; indexValueFetcherFor in production, a stand-in in a spec. */
      fetcher: IndexValueFetcher;
   };
   /**
    * Where the index is written, and where each read is stored and found
    * again: a read stored by an earlier build is reused while its source's
    * definition, the dimension, the cap and the givens are unchanged and it
    * is younger than {@link VALUE_REUSE_MAX_AGE_MS}.
    */
   db?: DuckDBConnection;
   shouldStop?: () => boolean;
   /** Test seam for {@link VALUE_READ_BUDGET_MS}. */
   readBudgetMs?: number;
   /** Test seam for {@link VALUE_REUSE_MAX_AGE_MS}. */
   reuseMaxAgeMs?: number;
   /**
    * `retrieval.dimensionValues.givens` for this package: every dimension is
    * read once per combination of these values, with that combination as
    * its givens.
    */
   givens?: Readonly<Record<string, readonly unknown[]>>;
}): Promise<{ index: ValueIndex; report: ValueBuildReport }> {
   const started = Date.now();
   const budgetMs = args.readBudgetMs ?? VALUE_READ_BUDGET_MS;
   const maxAgeMs = args.reuseMaxAgeMs ?? VALUE_REUSE_MAX_AGE_MS;
   const stored =
      args.db && args.read
         ? await loadReads(
              args.db,
              args.read.environmentName,
              args.read.packageName,
           )
         : new Map<string, StoredRead>();
   /** Every read this build holds, reused or new: what is stored at the end. */
   const reads: StoredRead[] = [];
   const values: IndexedValue[] = [];
   const givenCombinations = combinationsOf(args.givens);
   const givenIds = givenCombinations.map(givensId);
   const report: ValueBuildReport = {
      dimensionsRead: 0,
      dimensionsReused: 0,
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
      const check = readable(
         args.catalog,
         node,
         args.givens,
         args.read?.packageName,
      );
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
      const source = args.catalog.sources.get(node.sourceKey);
      const partitionKey = source?.partitionKey;
      const readKey = source?.readDefinition
         ? readKeyOf(source.readDefinition, node, args.cap, partitionKey)
         : undefined;
      const now = Date.now();
      const kept = givenIds.map((id) => {
         const old = stored.get(`${node.key}\u0000${id}`);
         return readKey &&
            old?.readKey === readKey &&
            now - old.readAt < maxAgeMs
            ? old
            : undefined;
      });
      const allKept = kept.every((k) => k !== undefined);
      if (!allKept && Date.now() - started >= budgetMs) {
         report.failed.push({
            dimension: node.key,
            reason: `not read: this build's value reads used their ${Math.round(budgetMs / 1000)} s budget`,
         });
         continue;
      }
      const fetcher = args.read.fetcher;
      try {
         const got = await readEachCombination(
            givenCombinations,
            async (givens, i) => {
               const old = kept[i];
               if (old) {
                  reads.push(old);
                  return { values: old.values, truncated: old.truncated };
               }
               const fresh = await fetcher({
                  environmentStore: args.read!.environmentStore,
                  environmentName: args.read!.environmentName,
                  packageName: args.read!.packageName,
                  modelPath: node.modelPaths[0],
                  source: node.source,
                  dimension: node.name,
                  limit: args.cap,
                  timeoutMs: VALUE_READ_TIMEOUT_MS,
                  ...(givens ? { givens } : {}),
                  ...(partitionKey
                     ? { partitionColumns: partitionKey.map((p) => p.column) }
                     : {}),
               });
               if (readKey) {
                  reads.push({
                     dimension: node.key,
                     givens: givenIds[i],
                     readKey,
                     truncated: fresh.truncated,
                     values: fresh.values,
                     readAt: Date.now(),
                  });
               }
               return fresh;
            },
         );
         if (allKept) report.dimensionsReused += 1;
         else report.dimensionsRead += 1;
         if (got.truncated)
            report.cut.push({ dimension: node.key, kept: got.values.length });
         for (const { value, count, key } of got.values) {
            const normalized = normalizeText(value);
            if (!normalized) continue;
            values.push({
               value,
               normalized,
               count,
               dimension: node.key,
               origin: "data",
               ...(key !== undefined ? { key } : {}),
            });
            report.valuesFromData += 1;
         }
      } catch (error) {
         const message = error instanceof Error ? error.message : String(error);
         const unset = /Given '([^']+)' has no value and no default/.exec(
            message,
         );
         (isTransientReadFailure(error) ? report.failed : report.skipped).push({
            dimension: node.key,
            reason: unset
               ? `the read needs the given ${unset[1]}, which has no default. Fix: name its values for this package in publisher.config.json, "retrieval": { "dimensionValues": { "givens": { "${args.read.packageName}": { "${unset[1]}": [...] } } } }`
               : `the read failed: ${message.slice(0, 200)}`,
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
         await saveReads(
            args.db,
            args.read.environmentName,
            args.read.packageName,
            reads,
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

/** A combination of givens as text that does not depend on key order. */
function givensId(givens: Record<string, unknown> | undefined): string {
   if (!givens) return "";
   return JSON.stringify(
      Object.keys(givens)
         .sort()
         .map((k) => [k, givens[k]]),
   );
}

/** What a stored read of `node` must match to be reused. */
function readKeyOf(
   readDefinition: string,
   node: CatalogNode,
   cap: number,
   partitionKey: ReadonlyArray<{ column: string }> | undefined,
): string {
   return createHash("sha256")
      .update(
         JSON.stringify([
            readDefinition,
            node.modelPaths[0],
            node.source,
            node.name,
            cap,
            (partitionKey ?? []).map((p) => p.column),
         ]),
         "utf8",
      )
      .digest("hex");
}

/** The package's stored reads, by dimension and givens. Empty when unreadable. */
async function loadReads(
   db: DuckDBConnection,
   environmentName: string,
   packageName: string,
): Promise<Map<string, StoredRead>> {
   const out = new Map<string, StoredRead>();
   let rows: Array<Record<string, unknown>>;
   try {
      rows = await db.all<Record<string, unknown>>(
         `SELECT dimension_key, givens, read_key, truncated, read_values, epoch_ms(read_at) AS read_ms
          FROM dimension_value_reads WHERE environment_name = ? AND package_name = ?`,
         [environmentName, packageName],
      );
   } catch (error) {
      logger.warn(
         "[get_context fcj] Stored value reads could not be loaded; every dimension is read",
         { error: error instanceof Error ? error.message : String(error) },
      );
      return out;
   }
   for (const row of rows) {
      let parsed: unknown;
      try {
         parsed = JSON.parse(String(row.read_values));
      } catch {
         continue;
      }
      if (!Array.isArray(parsed)) continue;
      const values: CountedValues["values"] = [];
      for (const v of parsed) {
         if (!Array.isArray(v) || typeof v[0] !== "string") continue;
         values.push({
            value: v[0],
            count: Number(v[1]) || 0,
            ...(typeof v[2] === "string" ? { key: v[2] } : {}),
         });
      }
      const read: StoredRead = {
         dimension: String(row.dimension_key),
         givens: String(row.givens),
         readKey: String(row.read_key),
         truncated: Boolean(row.truncated),
         values,
         readAt: Number(row.read_ms),
      };
      out.set(`${read.dimension}\u0000${read.givens}`, read);
   }
   return out;
}

/** Replace the package's stored reads with this build's. */
async function saveReads(
   db: DuckDBConnection,
   environmentName: string,
   packageName: string,
   reads: readonly StoredRead[],
): Promise<void> {
   await db.run(
      `DELETE FROM dimension_value_reads WHERE environment_name = ? AND package_name = ?`,
      [environmentName, packageName],
   );
   const seen = new Set<string>();
   const rows: unknown[][] = [];
   for (const r of reads) {
      const id = `${r.dimension}\u0000${r.givens}`;
      if (seen.has(id)) continue;
      seen.add(id);
      rows.push([
         environmentName,
         packageName,
         r.dimension,
         r.givens,
         r.readKey,
         r.truncated,
         JSON.stringify(
            r.values.map((v) =>
               v.key === undefined
                  ? [v.value, v.count]
                  : [v.value, v.count, v.key],
            ),
         ),
         new Date(r.readAt).toISOString(),
      ]);
   }
   for (let i = 0; i < rows.length; i += 50) {
      const batch = rows.slice(i, i + 50);
      await db.run(
         `INSERT INTO dimension_value_reads (environment_name, package_name, dimension_key, givens, read_key, truncated, read_values, read_at)
          VALUES ${batch.map(() => "(?, ?, ?, ?, ?, ?, ?, ?)").join(", ")}`,
         batch.flat(),
      );
   }
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
