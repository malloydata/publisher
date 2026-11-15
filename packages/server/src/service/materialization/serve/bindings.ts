// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * How serving reads a bound stored table: its quoted path, the virtual map a
 * compile resolves virtual sources through, whether generated SQL reads a
 * table, and whether a source opts out of serving from its table
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
