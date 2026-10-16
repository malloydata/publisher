// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { parseChartLine, seriesColorOf } from "./chartLine";

/** The key of the one-colour line the builder writes; see `seriesColorLine`. */
export const SERIES_COLOR_KEY = "theme.palette.series";

/**
 * Which property a tag line carries, so an existing line can be replaced. A
 * colour line is keyed only when it is the builder's own one-colour form: a
 * `# theme.palette.series` that lists a whole palette is the author's, and is
 * keyed apart so it is never rewritten or removed.
 */
export function tagKey(text: string): string | undefined {
   if (seriesColorOf(text) !== undefined) return SERIES_COLOR_KEY;
   const m = /^#\s*([a-z_]+)/.exec(text);
   return m?.[1];
}

/**
 * The tag keys the document MODELS, and therefore the only tag lines the
 * writer may rewrite or remove. Every other `#` line on a tile — `# big_value`,
 * `# bar_chart`, `# currency`, a host's own tag — is the renderer's, not ours,
 * and survives an edit like any other unmodelled Malloy.
 *
 * Without this rule, unticking one filter on the storefront overview's KPI
 * strip deleted its `# big_value` and the strip came back as a one-row table;
 * the round-trip gate cannot catch that, because the projection never held
 * the tag it lost.
 */
export const MODELLED_TAG_KEYS: ReadonlySet<string> = new Set([
   "colspan",
   "break",
   "borderless",
   "label",
   "subtitle",
   SERIES_COLOR_KEY,
]);

/** Whether a tile's tag line is one the builder writes: a modelled property, or a chart line it can rewrite. */
export function isBuilderTag(text: string): boolean {
   if (parseChartLine(text)) return true;
   const key = tagKey(text);
   return key !== undefined && MODELLED_TAG_KEYS.has(key);
}
