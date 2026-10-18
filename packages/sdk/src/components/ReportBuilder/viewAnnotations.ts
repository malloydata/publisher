// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { resolvedChart } from "../../utils/resolvedChart";
import { CHART_TAGS } from "./chartLine";

/**
 * What a view's raw annotations say, read with no Malloy parser: its doc line
 * and the chart it declares. Kept apart from `catalog.ts`, which parses tags,
 * so the main entry (New report) can read them without the parser.
 */

/** `#(doc) Revenue by product category\n` -> `Revenue by product category`. */
export function docOf(annotations: string[] | undefined): string | undefined {
   for (const raw of annotations ?? []) {
      const m = /^#\(doc\)\s*(.*)$/s.exec(raw.trim());
      if (m) return m[1].trim();
   }
   return undefined;
}

/**
 * The chart a view's annotations draw, as the renderer picks it
 * ({@link resolvedChart}): `bar_chart`, `shape_map`, … A `viz=bar` reads as
 * `bar_chart`. Undefined for a table, or no chart.
 */
export function chartOf(annotations: string[] | undefined): string | undefined {
   const chart = resolvedChart(annotations ?? []);
   return chart !== undefined && CHART_TAGS.includes(chart) ? chart : undefined;
}

/**
 * Whether a view's annotations set a multi-colour series palette. A tile's
 * colour is one `# theme.palette.series` value written after the view's tags,
 * and the last line wins, so that colour would replace the palette. A
 * one-colour line is the builder's own form and is not this. The block
 * spelling (`# theme { palette.series=[…] }`) counts as well as the dotted one.
 */
export function authorPaletteOf(
   annotations: string[] | undefined,
): true | undefined {
   for (const raw of annotations ?? []) {
      if (!/\btheme\b/.test(raw)) continue;
      if (paletteLists(raw).some((list) => quotedCount(list) >= 2)) return true;
   }
   return undefined;
}

/**
 * What each `palette.series=[…]` in an annotation lists, between its brackets.
 * Scanned by hand, once, rather than with a regex: an annotation is author
 * text, and a backtracking pattern is quadratic on one that repeats an
 * unclosed `palette.series=[`.
 */
function paletteLists(raw: string): string[] {
   const lists: string[] = [];
   const key = "palette.series";
   const skipSpace = (i: number) => {
      while (i < raw.length && /\s/.test(raw[i])) i++;
      return i;
   };
   for (let from = 0; ; ) {
      const at = raw.indexOf(key, from);
      if (at < 0) return lists;
      from = at + key.length;
      let i = skipSpace(from);
      if (raw[i] !== "=") continue;
      i = skipSpace(i + 1);
      if (raw[i] !== "[") continue;
      const close = raw.indexOf("]", i + 1);
      // No `]` after this one, so none after any later key either.
      if (close < 0) return lists;
      lists.push(raw.slice(i + 1, close));
      from = close + 1;
   }
}

/** How many quoted strings a list holds, `"#a", '#b'` -> 2, a backslash escaping the character after it. */
function quotedCount(list: string): number {
   let count = 0;
   for (let i = 0; i < list.length; i++) {
      if (list[i] !== '"' && list[i] !== "'") continue;
      let j = i + 1;
      while (j < list.length && list[j] !== '"' && list[j] !== "'")
         j += list[j] === "\\" ? 2 : 1;
      if (j >= list.length) return count;
      count++;
      i = j;
   }
   return count;
}
