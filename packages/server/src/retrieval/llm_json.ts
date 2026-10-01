// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { stripThinking } from "../service/llm_provider";
import type { RelevanceLevel } from "./retrieval_config";

/**
 * Turning a model's reply into data.
 *
 * A hosted model returns clean JSON when asked. A small local one wraps it in
 * a code fence or a sentence, wraps a bare array in an object, leaves a
 * trailing comma, or runs out of tokens halfway through the array. None of
 * that should cost a whole batch, so extraction is forgiving and the typed
 * parsers below are strict about the fields that matter (index range, score
 * vocabulary) and report what they dropped, so a stage can decide whether the
 * reply was usable.
 */

export type Extracted =
   | { ok: true; value: unknown; salvaged: boolean }
   | { ok: false; error: string };

function unfence(text: string): string {
   const fenced = text.match(/```(?:json|JSON)?\s*([\s\S]*?)```/);
   if (fenced) return fenced[1];
   // An opening fence with no close: the reply was cut off.
   const open = text.match(/```(?:json|JSON)?\s*([\s\S]*)$/);
   return open ? open[1] : text;
}

/** The end index (exclusive) of the balanced value starting at `start`. */
function balancedEnd(text: string, start: number): number {
   const open = text[start];
   const close = open === "[" ? "]" : "}";
   let depth = 0;
   let inString = false;
   for (let i = start; i < text.length; i++) {
      const c = text[i];
      if (inString) {
         if (c === "\\") i++;
         else if (c === '"') inString = false;
         continue;
      }
      if (c === '"') inString = true;
      else if (c === open || c === "{" || c === "[") depth++;
      else if (c === close || c === "}" || c === "]") {
         depth--;
         if (depth === 0) return i + 1;
      }
   }
   return -1;
}

function stripTrailingCommas(text: string): string {
   let out = "";
   let inString = false;
   for (let i = 0; i < text.length; i++) {
      const c = text[i];
      if (inString) {
         out += c;
         if (c === "\\") out += text[++i] ?? "";
         else if (c === '"') inString = false;
         continue;
      }
      if (c === '"') {
         inString = true;
         out += c;
      } else if (c === ",") {
         let j = i + 1;
         while (j < text.length && /\s/.test(text[j])) j++;
         if (text[j] === "]" || text[j] === "}") continue;
         out += c;
      } else {
         out += c;
      }
   }
   return out;
}

/**
 * Cut an unterminated array of objects back to its last complete element and
 * close it. Only used when the reply was truncated, and only for arrays.
 */
function salvageTruncatedArray(text: string, start: number): string | null {
   let depth = 0;
   let inString = false;
   let lastCompleteEnd = -1;
   for (let i = start; i < text.length; i++) {
      const c = text[i];
      if (inString) {
         if (c === "\\") i++;
         else if (c === '"') inString = false;
         continue;
      }
      if (c === '"') inString = true;
      else if (c === "[" || c === "{") depth++;
      else if (c === "]" || c === "}") {
         depth--;
         // depth 1 after closing means a complete element of the outer array.
         if (depth === 1 && c === "}") lastCompleteEnd = i + 1;
      }
   }
   if (lastCompleteEnd < 0) return null;
   return `${text.slice(start, lastCompleteEnd)}]`;
}

/** Pull the first JSON array or object out of a model reply. */
export function extractJson(reply: string): Extracted {
   const text = unfence(stripThinking(reply)).trim();
   const start = text.search(/[[{]/);
   if (start < 0) return { ok: false, error: "no JSON found in the reply" };

   const end = balancedEnd(text, start);
   const candidates: Array<{ src: string; salvaged: boolean }> = [];
   if (end > 0) {
      candidates.push({ src: text.slice(start, end), salvaged: false });
   } else if (text[start] === "[") {
      const cut = salvageTruncatedArray(text, start);
      if (cut) candidates.push({ src: cut, salvaged: true });
   }
   for (const { src, salvaged } of candidates) {
      for (const attempt of [src, stripTrailingCommas(src)]) {
         try {
            return { ok: true, value: JSON.parse(attempt), salvaged };
         } catch {
            // try the next repair
         }
      }
   }
   return { ok: false, error: "the reply was not valid JSON" };
}

/**
 * A list out of a reply that may be a bare array, or an object wrapping one
 * (`{"results": [...]}`, which is what JSON mode forces).
 */
export function asList(value: unknown): unknown[] | null {
   if (Array.isArray(value)) return value;
   if (typeof value === "object" && value !== null) {
      for (const v of Object.values(value)) {
         if (Array.isArray(v)) return v;
      }
   }
   return null;
}

const isObj = (v: unknown): v is Record<string, unknown> =>
   typeof v === "object" && v !== null && !Array.isArray(v);

function asIndex(v: unknown, n: number): number | null {
   const num = typeof v === "string" && /^\s*\d+\s*$/.test(v) ? Number(v) : v;
   return typeof num === "number" &&
      Number.isInteger(num) &&
      num >= 0 &&
      num < n
      ? num
      : null;
}

const LEVEL_NAMES: Record<string, RelevanceLevel | "NONE"> = {
   NONE: "NONE",
   IRRELEVANT: "NONE",
   LOW: "LOW",
   MEDIUM: "MEDIUM",
   MED: "MEDIUM",
   HIGH: "HIGH",
};

/** LOW | MEDIUM | HIGH (any case), or null when it is anything else. */
export function asLevel(v: unknown): RelevanceLevel | "NONE" | null {
   if (typeof v === "string")
      return LEVEL_NAMES[v.trim().toUpperCase()] ?? null;
   return null;
}

export const LEVEL_INDEX: Record<RelevanceLevel, number> = {
   LOW: 0,
   MEDIUM: 1,
   HIGH: 2,
};

export interface Parsed<T> {
   items: T[];
   /** Entries dropped as unusable (bad index, bad score, duplicate). */
   invalid: number;
   /** The reply had no list at all, or more than half of it was unusable. */
   usable: boolean;
   salvaged: boolean;
}

/** A pick result meaning "a valid answer that yields nothing" (e.g. NONE). */
const SKIP = Symbol("skip");

function parseList<T>(
   reply: string,
   pick: (raw: unknown) => T | null | typeof SKIP,
): Parsed<T> {
   const extracted = extractJson(reply);
   if (!extracted.ok) {
      return { items: [], invalid: 0, usable: false, salvaged: false };
   }
   const list = asList(extracted.value);
   if (!list) return { items: [], invalid: 0, usable: false, salvaged: false };
   const items: T[] = [];
   let invalid = 0;
   for (const raw of list) {
      const item = pick(raw);
      if (item === SKIP) continue;
      if (item === null) invalid++;
      else items.push(item);
   }
   // An empty list is a valid answer ("nothing here is relevant"); a list that
   // is mostly garbage is not.
   const usable = list.length === 0 || invalid * 2 <= list.length;
   return { items, invalid, usable, salvaged: extracted.salvaged };
}

export interface RefineItem {
   index: number;
   level: RelevanceLevel;
   reason: string;
}

/**
 * `[{"index": i, "score": "LOW|MEDIUM|HIGH", "reason": "..."}]` for `n`
 * candidates. Duplicate indices keep the first. A score of NONE is a valid
 * "not relevant" and is dropped without counting as invalid.
 */
export function parseRefineReply(reply: string, n: number): Parsed<RefineItem> {
   const seen = new Set<number>();
   return parseList<RefineItem>(reply, (raw) => {
      if (!isObj(raw)) return null;
      const index = asIndex(raw.index, n);
      const level = asLevel(raw.score ?? raw.level ?? raw.relevance);
      if (index === null || level === null) return null;
      if (seen.has(index)) return null;
      seen.add(index);
      if (level === "NONE") return SKIP;
      const reason =
         typeof raw.reason === "string" ? raw.reason.trim().slice(0, 300) : "";
      return { index, level, reason };
   });
}

export interface RerankItem {
   index: number;
   /** 0-3 */
   score: number;
}

/** `[{"index": i, "score": 0-3}]`, best first. Order is kept. */
export function parseRerankReply(reply: string, n: number): Parsed<RerankItem> {
   const seen = new Set<number>();
   return parseList<RerankItem>(reply, (raw) => {
      if (!isObj(raw)) return null;
      const index = asIndex(raw.index, n);
      if (index === null || seen.has(index)) return null;
      let score: number | null = null;
      const s = raw.score ?? raw.level ?? raw.relevance;
      if (typeof s === "number" && Number.isFinite(s)) score = s;
      else if (typeof s === "string" && /^\s*\d+(\.\d+)?\s*$/.test(s))
         score = Number(s);
      else {
         const level = asLevel(s);
         if (level) score = level === "NONE" ? 0 : LEVEL_INDEX[level] + 1;
      }
      if (score === null || score < 0 || score > 3) return null;
      seen.add(index);
      return { index, score };
   });
}

export interface ValueItem {
   index: number;
   level: RelevanceLevel;
}

/** `[{"index": i, "score": "LOW|MEDIUM|HIGH"}]` with no reason. */
export function parseValueRefineReply(
   reply: string,
   n: number,
): Parsed<ValueItem> {
   const parsed = parseRefineReply(reply, n);
   return {
      ...parsed,
      items: parsed.items.map(({ index, level }) => ({ index, level })),
   };
}

/** `{"keyphrase": "..."}` for one entity. */
export function parseKeyphraseReply(reply: string): string | null {
   const extracted = extractJson(reply);
   if (extracted.ok && isObj(extracted.value)) {
      const k = extracted.value.keyphrase;
      if (typeof k === "string" && k.trim()) return k.trim();
   }
   return null;
}

/** `[{"index": i, "keyphrase": "..."}]` for a batch. */
export function parseKeyphraseBatchReply(
   reply: string,
   n: number,
): Map<number, string> {
   const out = new Map<number, string>();
   parseList<true>(reply, (raw) => {
      if (!isObj(raw)) return null;
      const index = asIndex(raw.index, n);
      const k = raw.keyphrase;
      if (index === null || typeof k !== "string" || !k.trim()) return null;
      if (!out.has(index)) out.set(index, k.trim());
      return true;
   });
   return out;
}

export interface SummaryReply {
   summary: string;
   oneLine: string;
}

/** `{"summary": "...", "one_line_summary": "..."}` */
export function parseSummaryReply(reply: string): SummaryReply | null {
   const extracted = extractJson(reply);
   if (!extracted.ok || !isObj(extracted.value)) return null;
   const s = extracted.value.summary;
   const o = extracted.value.one_line_summary ?? extracted.value.oneLineSummary;
   if (typeof s !== "string" || typeof o !== "string") return null;
   const summary = s.replace(/\s+/g, " ").trim();
   const oneLine = o.replace(/\s+/g, " ").trim();
   return summary && oneLine ? { summary, oneLine } : null;
}
