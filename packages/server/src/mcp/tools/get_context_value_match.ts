// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Matching a typed phrase to the strings a dimension stores. Pure functions, no
 * I/O, so the scoring table is pinned by tests that run it directly.
 *
 * The scores are this module's own, 0 to 1, and are published as a value's
 * `relevance`. They are comparable across values and dimensions in one
 * response; they are not the cosine of the entity ranker and are not
 * comparable to it.
 */

/** A value scoring under this is not a match. */
export const VALUE_MATCH_FLOOR = 0.5;

/** Most values one dimension returns for one search target. */
export const MAX_VALUES_PER_DIMENSION_HIT = 10;

/** One dimension's value that matched a phrase. */
export interface ValueHit {
   value: string;
   score: number;
}

/**
 * The comparison form of a string: lowercase, accents removed, anything that is
 * not a letter or digit turned into a space, spaces collapsed. "Auto
 * Intenders (In-Market)" and "auto  intenders in market" compare equal.
 */
export function normalizeText(text: string): string {
   return text
      .normalize("NFKD")
      .replace(/\p{M}+/gu, "")
      .toLowerCase()
      .replace(/[^\p{L}\p{N}]+/gu, " ")
      .trim();
}

function tokensOf(normalized: string): string[] {
   return normalized === "" ? [] : normalized.split(" ");
}

/**
 * Edit distance counting a swap of two neighbouring letters as one edit
 * (optimal string alignment), so "intedners" is one edit from "intenders".
 * Stops early when the distance must exceed `limit`.
 */
export function editDistance(a: string, b: string, limit: number): number {
   if (a === b) return 0;
   if (Math.abs(a.length - b.length) > limit) return limit + 1;
   let prev2: number[] = [];
   let prev: number[] = Array.from({ length: b.length + 1 }, (_, j) => j);
   for (let i = 1; i <= a.length; i++) {
      const row: number[] = [i];
      let rowMin = i;
      for (let j = 1; j <= b.length; j++) {
         const cost = a[i - 1] === b[j - 1] ? 0 : 1;
         let best = Math.min(prev[j] + 1, row[j - 1] + 1, prev[j - 1] + cost);
         if (i > 1 && j > 1 && a[i - 1] === b[j - 2] && a[i - 2] === b[j - 1]) {
            best = Math.min(best, prev2[j - 2] + 1);
         }
         row.push(best);
         if (best < rowMin) rowMin = best;
      }
      if (rowMin > limit) return limit + 1;
      prev2 = prev;
      prev = row;
   }
   return prev[b.length];
}

/** Edits allowed in one word: none for short words, which would match noise. */
function allowedEdits(word: string): number {
   if (word.length <= 3) return 0;
   return word.length <= 5 ? 1 : 2;
}

/** How well one search word matches the best word of the value, 0 for none. */
function wordScore(word: string, valueWords: string[]): number {
   let best = 0;
   for (const candidate of valueWords) {
      if (candidate === word) return 1;
      if (
         word.length >= 3 &&
         candidate.length >= 3 &&
         (candidate.startsWith(word) || word.startsWith(candidate))
      ) {
         best = Math.max(best, 0.9);
         continue;
      }
      const limit = allowedEdits(word);
      if (limit === 0) continue;
      const distance = editDistance(word, candidate, limit);
      if (distance <= limit)
         best = Math.max(best, distance === 1 ? 0.75 : 0.65);
   }
   return best;
}

/**
 * The score of one stored value against a phrase, 0 when it does not match.
 *
 * - 1.00 the same string.
 * - 0.98 the same after normalizing (case, accents, punctuation, spacing).
 * - Otherwise each search word is matched to the value's words (a whole word
 *   scores 1, a word one is a prefix of 0.9, a word within the allowed edits
 *   0.75 or 0.65). The mean over the search words is scaled up to 20% down by
 *   how little of the value the match covers, so "auto intenders" prefers
 *   "Auto Intenders" over "Auto Intenders (In-Market) 2024".
 */
export function scoreValue(phrase: string, value: string): number {
   if (value === phrase) return 1;
   const phraseNorm = normalizeText(phrase);
   const valueNorm = normalizeText(value);
   if (phraseNorm === "" || valueNorm === "") return 0;
   if (valueNorm === phraseNorm) return 0.98;
   const phraseWords = tokensOf(phraseNorm);
   const valueWords = tokensOf(valueNorm);
   const scores = phraseWords.map((word) => wordScore(word, valueWords));
   const mean = scores.reduce((a, b) => a + b, 0) / phraseWords.length;
   const covered = Math.min(1, phraseNorm.length / valueNorm.length);
   const score = mean * (0.8 + 0.2 * covered);
   return score >= VALUE_MATCH_FLOOR ? Math.round(score * 10_000) / 10_000 : 0;
}

/** Stable order: score high to low, then shorter value, then code-unit order. */
export function compareHits(a: ValueHit, b: ValueHit): number {
   return (
      b.score - a.score ||
      a.value.length - b.value.length ||
      (a.value < b.value ? -1 : a.value > b.value ? 1 : 0)
   );
}

/** The best `max` values of a dimension for one phrase, lexical matching only. */
export function matchValues(
   phrase: string,
   values: readonly string[],
   max = MAX_VALUES_PER_DIMENSION_HIT,
): ValueHit[] {
   const hits: ValueHit[] = [];
   for (const value of values) {
      const score = scoreValue(phrase, value);
      if (score > 0) hits.push({ value, score });
   }
   return hits.sort(compareHits).slice(0, max);
}

/**
 * The word a pushed-down text filter looks for when a dimension has too many
 * values to read: the longest word of the phrase, cut to four letters so a
 * typo in the tail of the word still matches. Undefined when the phrase has no
 * word of at least three letters.
 */
export function filterStem(phrase: string): string | undefined {
   const words = tokensOf(normalizeText(phrase)).filter((w) => w.length >= 3);
   if (words.length === 0) return undefined;
   const longest = words.reduce((a, b) => (b.length > a.length ? b : a));
   return longest.slice(0, 4);
}

/** Cosine of two vectors of the same length; 0 when either has no length. */
export function cosine(a: readonly number[], b: readonly number[]): number {
   let dot = 0;
   let na = 0;
   let nb = 0;
   for (let i = 0; i < a.length; i++) {
      dot += a[i] * b[i];
      na += a[i] * a[i];
      nb += b[i] * b[i];
   }
   return na === 0 || nb === 0 ? 0 : dot / Math.sqrt(na * nb);
}
