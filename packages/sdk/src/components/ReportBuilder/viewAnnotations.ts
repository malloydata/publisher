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
      if (!/\btheme\b/.test(raw) || !/palette\.series/.test(raw)) continue;
      for (const match of raw.matchAll(/palette\.series\s*=\s*\[([^\]]*)\]/g)) {
         const quoted = match[1].match(/["'](?:[^"'\\]|\\.)*["']/g) ?? [];
         if (quoted.length >= 2) return true;
      }
   }
   return undefined;
}
