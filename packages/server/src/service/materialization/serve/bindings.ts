// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * How serving reads a bound stored table: its quoted path, the virtual map a
 * compile resolves virtual sources through, whether generated SQL reads a
 * table (and, failing closed, which tables an answer is judged to read for its
 * gates), and whether a source opts out of serving from its table
 * (`#@ -persist`).
 *
 * Read by the served copy, the rollup and view legs, storage bindings and
 * colocated bindings.
 *
 * Pure.
 */

import { quoteManifestTablePath } from "../../quoting";
import type { Binding } from "../domain/binding";
import type { DerivedSourceDef } from "../text/author_model";

/**
 * Whether a source opts out of the pre-built table with `#@ -persist`.
 *
 * Read from the source's OWN block notes, not the `persistent` flag, which is
 * also false for a source that never inherited `#@ persist`.
 */
export function optsOutOfPersist(def: DerivedSourceDef): boolean {
   const notes = (
      def.annotations as { blockNotes?: { text?: unknown }[] } | undefined
   )?.blockNotes;
   return (notes ?? []).some(
      (note) =>
         typeof note?.text === "string" &&
         /^\s*#@\s*-persist\b/.test(note.text),
   );
}

/**
 * A binding's table path as the SQL reads it: a storage table's quoted for
 * DuckDB (the destination), a colocated table's as bound, already canonical
 * for its own connection's dialect.
 */
export function boundTablePath(b: Binding): string {
   return b.tier === "colocated"
      ? b.tablePath
      : quoteManifestTablePath(b.tablePath, "duckdb");
}

/**
 * Whether `sql` reads the table at `tablePath` (as {@link boundTablePath}
 * spells it): the path right after a `FROM` or `JOIN`, and not followed by
 * more of a name. A bare substring test is not enough: a colocated table's
 * path is often one quoted name (`"orders"`), which a result column of the
 * same name (`AS "orders"`) also spells.
 */
export function sqlReadsTable(sql: string, tablePath: string): boolean {
   const path = tablePath.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
   return new RegExp(`\\b(?:FROM|JOIN)\\s+${path}(?![\\w$".\x60])`, "i").test(
      sql,
   );
}

/**
 * The tables an answer is judged to read when deciding whether it keeps an
 * entry point's gate (`answerKeepsGates`): the tables `sql` was found to read,
 * and every re-rooted `storage=` table it was not proven NOT to read. Fail
 * closed: {@link sqlReadsTable} matches one spelling, so a storage table
 * spelled another way (other quoting or case) would drop out and the answer
 * would read as colocated only. A storage table counts as absent only when
 * its path, quotes and case aside, appears nowhere in `sql`.
 */
export function tablesReadForGates(
   sql: string,
   rerooted: readonly Binding[],
   tablesRead: readonly Binding[],
): Binding[] {
   const bare = (text: string) => text.replace(/["`]/g, "").toLowerCase();
   const text = bare(sql);
   const mentions = (path: string) =>
      new RegExp(
         `(?<![\\w$.])${bare(path).replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(?![\\w$])`,
      ).test(text);
   return rerooted.filter(
      (b) =>
         tablesRead.includes(b) ||
         (b.tier === "storage" &&
            [b.tablePath, boundTablePath(b)].some(mentions)),
   );
}

/**
 * Assemble the per-call `virtualMap` (`destinationName -> handle -> table
 * path`). Core pastes each path verbatim, so it must be quoted canonical
 * DuckDB SQL. {@link quoteManifestTablePath} is the same quoting authority the
 * manifest binds use, and never double-quotes an author-quoted `name=`.
 */
export function buildVirtualMap(
   bindings: Binding[],
): Map<string, Map<string, string>> {
   const map = new Map<string, Map<string, string>>();
   for (const b of bindings) {
      let inner = map.get(b.destinationName);
      if (!inner) {
         inner = new Map<string, string>();
         map.set(b.destinationName, inner);
      }
      inner.set(b.virtualHandle, boundTablePath(b));
   }
   return map;
}
