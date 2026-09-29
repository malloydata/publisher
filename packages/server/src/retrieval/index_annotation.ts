// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which dimensions an author has opted into value indexing.
 *
 * A port of Credible's `pycommon/dimensional_index/index_annotation.py`
 * (`INDEX_TAG_PATTERN`), so a model written for one is read the same by the
 * other: `#(index)` and `#(index_values)`, bare or with attributes, with
 * whitespace tolerated after the `#` and the `(`. The word boundary after the
 * keyword keeps `#(indexed)` and `#(index_of_things)` from matching.
 *
 * One deliberate difference. The service parses `n=` and ignores it, because it
 * embeds every distinct value. Here it is a per-dimension cap on how many
 * values are kept, since indexing must finish in minutes; `n=-1` (or `n=0`)
 * means "no cap of my own", leaving the server's caps as the only limit.
 */
export const INDEX_TAG_PATTERN = /#\s*\(\s*index(?:_values)?\b/;

const N_TOKEN = /(?:^|[\s(])n=(-?\d+)(?=$|[\s)])/;

export interface IndexTag {
   /** The author's own cap on values for this dimension, when they set one. */
   n?: number;
}

/**
 * The value-indexing tag on a field's annotations, or null when it has none.
 * Accepts annotation objects (`{ value }`) as well as raw strings, like
 * docOnlyText.
 */
export function parseIndexTag(
   annotations?: ReadonlyArray<string | { value: string }>,
): IndexTag | null {
   if (!annotations) return null;
   for (const a of annotations) {
      const line = (typeof a === "string" ? a : a.value).trim();
      if (!INDEX_TAG_PATTERN.test(line)) continue;
      const n = N_TOKEN.exec(line);
      if (n) {
         const cap = Number(n[1]);
         if (cap > 0) return { n: cap };
      }
      return {};
   }
   return null;
}

/** `*` matches any run of characters; everything else is literal. Case-insensitive. */
export function globToRegExp(glob: string): RegExp {
   const escaped = glob
      .split("*")
      .map((part) => part.replace(/[.+?^${}()|[\]\\]/g, "\\$&"))
      .join(".*");
   return new RegExp(`^${escaped}$`, "i");
}
