// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The immediate upstream SOURCE a derived source is built from, parsed from the
 * model's own Malloy text — e.g. `raw_orders` for
 * `source: order_rollup is raw_orders -> { … }`.
 *
 * Only a source-to-source derivation resolves. A table-backed source
 * (`is conn.table('…')`, `is conn.sql('…')`) returns undefined — it has
 * physical table provenance the header shows instead — and the parsed base
 * must be one of the model's own sources, so a connection name or SQL slice is
 * never mistaken for lineage. This is a best-effort read of source text: when
 * the text is absent, or the declaration takes a form this doesn't recognise,
 * it yields undefined and the header simply shows no `from`.
 */
export function baseSourceName(
   sourceText: string | undefined,
   sourceName: string | undefined,
   knownSources: ReadonlySet<string>,
): string | undefined {
   if (!sourceText || !sourceName) return undefined;
   // Anchored at this source's `source: NAME is BASE`, requiring BASE to be a
   // bare identifier followed immediately by `->` (query-derived) or `extend`
   // (extension). A table/SQL base has `.table(`/`.sql(` after the identifier,
   // so the trailing `->|extend` guard fails and no false base is captured.
   const re = new RegExp(
      "source:\\s*`?" +
         escapeRegExp(sourceName) +
         "`?\\s+is\\s+`?([A-Za-z_]\\w*)`?\\s*(?:->|extend\\b)",
   );
   const match = re.exec(sourceText);
   if (!match) return undefined;
   const base = match[1];
   return knownSources.has(base) ? base : undefined;
}

function escapeRegExp(value: string): string {
   return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
