// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which parts of a dimension phrase look like a stored value, so the value
 * index can be asked about them when the caller sent no `example_values`.
 *
 * A rule, not a model: a quoted string; a run of capitalized words that is
 * not the phrase's first word ("orders shipped to New York"); a code that
 * mixes letters and digits ("SKU 4X12"); or a word the catalog's own text
 * never uses ("ixnayed"). It over-fires on purpose. A wrong guess costs one
 * extra candidate, which the connect stage and the judge discard; a missed
 * value costs the field.
 */

import { terms } from "./keyword";

const CODE = /^(?=.*[A-Za-z])(?=.*\d)[A-Za-z0-9_-]{2,}$/;

/** The value-looking spans of a phrase, in order, without repeats. */
export function valueSpans(
   phrase: string,
   inVocabulary: (term: string) => boolean,
): string[] {
   const spans: string[] = [];
   const add = (s: string) => {
      const t = s.trim();
      if (t && !spans.includes(t)) spans.push(t);
   };
   // Quoted strings first; they are taken out of the rest.
   const unquoted = phrase.replace(
      /"([^"]+)"|'([^']+)'|“([^”]+)”/g,
      (_, a, b, c) => {
         add(a ?? b ?? c);
         return " ";
      },
   );
   const words = unquoted.split(/\s+/).filter(Boolean);
   let run: string[] = [];
   const flush = () => {
      if (run.length > 0) add(run.join(" "));
      run = [];
   };
   words.forEach((raw, i) => {
      const word = raw.replace(/^[^\p{L}\p{N}]+|[^\p{L}\p{N}]+$/gu, "");
      if (!word) {
         flush();
         return;
      }
      const capitalized = /^\p{Lu}/u.test(word) && i > 0;
      if (capitalized) {
         run.push(word);
         return;
      }
      flush();
      if (CODE.test(word)) {
         add(word);
         return;
      }
      const t = terms(word);
      if (
         t.length === 1 &&
         t[0].length >= 4 &&
         !inVocabulary(t[0]) &&
         !/^\d+$/.test(t[0])
      ) {
         add(word);
      }
   });
   flush();
   return spans;
}
