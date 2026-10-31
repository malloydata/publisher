// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { ExplorerSource } from "./schemaModel";

/**
 * What the source picker (`SourcesDropdown`) says about each source, and in what order —
 * the pure half, so every rule is a unit test (ux-design-guide §4 "Navigation scale").
 *
 * A reader opens the picker to find the source an analysis needs, and its rows are the only
 * place every source of the model sits side by side. So each row says what the source is (its
 * `#(doc)`, else the index's generated one-liner, marked), how much is in it (views, fields,
 * joins), and — for a lookup table no one would start from — which source uses it.
 */

/** Plural-aware count: `count(1, "view")` → "1 view". */
function count(n: number, noun: string): string {
   return `${n} ${n === 1 ? noun : `${noun}s`}`;
}

/**
 * The row's meta line: "16 views · 37 fields · 4 joins". The field count is the one the
 * source's own header prints once it is picked (`fieldCount`: dimensions, measures and views),
 * so the picker and the page it opens never disagree; joins are left out when there are none.
 */
export function sourceMetaLine(source: ExplorerSource): string {
   const fields =
      source.dimensions.length + source.measures.length + source.views.length;
   const parts = [count(source.views.length, "view"), count(fields, "field")];
   if (source.joins.length > 0) parts.push(count(source.joins.length, "join"));
   return parts.join(" · ");
}

/** The picker's sources in the order it lists them, and the one its token marks. */
export interface PickerOrder {
   sources: ExplorerSource[];
   /** True on the package's root `index.malloy`, on the surface: its sources are the curated
    *  set, and the pill counts them as public. */
   curated: boolean;
   /** The curated sources are listed as the file's `export { … }` orders them, so the picker's
    *  header may say so. False when the file's text, and so its order, is unknown. */
   exportOrdered: boolean;
   /** The source the model opens on, when it really has the most views — the "Most views"
    *  token. Never set when the claim would be false. */
   mostViews?: string;
}

/**
 * The order the picker lists a model's sources in.
 *
 * - On the package's root `index.malloy`, when it is on the surface (`publicSurface`), the file
 *   IS the author's choice of what to publish, so its sources keep the order it exports them
 *   in, when its text says (`inExportOrder`); else the compiled model's, and `exportOrdered`
 *   is false so nothing claims an order.
 * - On any other model, declaration order is not a ranking (`pickDefaultSource`), so the
 *   source the model opens on comes first and the rest follow by view count, most first;
 *   equal counts keep declaration order. A lookup table with no views sinks to the end.
 *
 * `mostViews` names the default only when no other source has more views than it and it has
 * any: `pickDefaultSource` sets aside a source that extends another, so the default can be
 * the base with fewer views than its extension, and the token must not claim otherwise.
 */
export function orderPickerSources(
   sources: readonly ExplorerSource[],
   defaultSource: ExplorerSource | null,
   /** Set on the root `index.malloy` on the surface: the names its `export { … }` lists, in
    *  order (`exportOrder`), empty when its text is unknown. */
   publicSurface?: { exportOrder: readonly string[] },
): PickerOrder {
   if (publicSurface) {
      const order = publicSurface.exportOrder;
      const exportOrdered =
         order.length > 0 && sources.some((s) => order.includes(s.name));
      return {
         sources: inExportOrder(sources, (s) => s.name, order),
         curated: true,
         exportOrdered,
      };
   }
   const rest = sources
      .map((source, index) => ({ source, index }))
      .filter(({ source }) => source.name !== defaultSource?.name)
      .sort(
         (a, b) =>
            b.source.views.length - a.source.views.length || a.index - b.index,
      )
      .map(({ source }) => source);
   const ordered = defaultSource ? [defaultSource, ...rest] : rest;
   const top = defaultSource?.views.length ?? 0;
   const mostViews =
      defaultSource && top > 0 && sources.every((s) => s.views.length <= top)
         ? defaultSource.name
         : undefined;
   return { sources: ordered, curated: false, exportOrdered: false, mostViews };
}

/** A Malloy file's text without its comments: `//` and `--` to the line's end, and `/* … *\/`. */
function withoutComments(text: string): string {
   return text.replace(/\/\*[\s\S]*?\*\/|\/\/[^\n]*|--[^\n]*/g, " ");
}

/** An identifier as written, its backquotes off: `` `order items` `` → `order items`. */
function unquoteId(word: string): string {
   return /^`.*`$/.test(word) ? word.slice(1, -1) : word;
}

/**
 * The names a model file's `export { … }` statements list, in the order written. The compiled
 * model lists its exports in declaration order (an imported source where the import brings it
 * in), which is not the order an `index.malloy`'s author chose, so the text is read for it.
 * Each item is a bare identifier (Malloy's `exportItem`), backquoted or not; the keyword is
 * matched without case, as Malloy's are, and comments are skipped. Empty when the text is
 * unknown or has no export statement.
 */
export function exportOrder(sourceText: string | undefined): string[] {
   if (!sourceText) return [];
   const text = withoutComments(sourceText);
   const out: string[] = [];
   for (const match of text.matchAll(/(?:^|[\s;])export\s*\{([^}]*)\}/gi)) {
      for (const entry of match[1].split(",")) {
         const name = unquoteId(entry.trim());
         if (name && !out.includes(name)) out.push(name);
      }
   }
   return out;
}

/**
 * `items` in the order `order` names them; anything it does not name keeps its place after
 * them, in the order given. Unchanged when `order` is empty.
 */
export function inExportOrder<T>(
   items: readonly T[],
   nameOf: (item: T) => string,
   order: readonly string[],
): T[] {
   if (order.length === 0) return [...items];
   const rank = (item: T) => {
      const i = order.indexOf(nameOf(item));
      return i === -1 ? order.length : i;
   };
   return items
      .map((item, index) => ({ item, index }))
      .sort((a, b) => rank(a.item) - rank(b.item) || a.index - b.index)
      .map(({ item }) => item);
}

/** A row's description: the author's words, else a host's generated one, marked. */
export interface PickerDescription {
   text: string;
   /** From the host, not the model's author: the row marks it Generated. */
   generated: boolean;
}

/**
 * The row's description: the source's own `#(doc)` when it has one — the author's words win —
 * else a host's summary (`describePickerSource`), marked as generated. Nothing when neither.
 */
export function pickerDescription(
   doc: string | undefined,
   summary: string | undefined,
): PickerDescription | undefined {
   const authored = doc?.trim();
   if (authored) return { text: authored, generated: false };
   const generated = summary?.trim();
   return generated ? { text: generated, generated: true } : undefined;
}
