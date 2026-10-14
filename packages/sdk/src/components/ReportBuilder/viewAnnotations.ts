// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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

/** `# bar_chart\n` -> `bar_chart`. Only the renderer's own chart tags count. */
export function chartOf(annotations: string[] | undefined): string | undefined {
   for (const raw of annotations ?? []) {
      // A `-name` removes a tag, so it is never the chart the view declares.
      const name = /^#\s*(?:-[A-Za-z_]+\s+)*([a-z_]+)/.exec(raw.trim())?.[1];
      if (name && CHART_TAGS.includes(name)) return name;
   }
   return undefined;
}
