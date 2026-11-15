// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * How serving reads a bound stored table: its quoted path, the virtual map a
 * compile resolves virtual sources through, whether generated SQL reads a
 * table (and, failing closed, which tables an answer is judged to read for its
 * gates), a stored schema narrowed to a source's public columns, and whether a
 * source opts out of serving from its table (`#@ -persist`).
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
 * Whether a compiled field carries a non-public access modifier. Such a field
 * must NEVER be re-emitted: the virtual source carries no access modifiers, so
 * it would expose what the live path refuses. Anything but absent/`public`
 * counts as restricted, so an unknown future modifier fails closed.
 */
function isAccessRestricted(field: unknown): boolean {
   const am = (field as { accessModifier?: unknown }).accessModifier;
   return am != null && am !== "public";
}

/**
 * Narrow a binding's captured DESCRIBE schema to the columns the source
 * PUBLICLY exposes, dropping any physical column the source hides.
 *
 * A column hidden by `except:` or an access modifier is still materialized and
 * captured by `DESCRIBE`; declaring it would expose it through the virtual
 * source. The serve surface must reproduce the source's PUBLIC surface exactly,
 * never widen it. A query touching a dropped column serves live, where
 * visibility is enforced.
 *
 * A captured name matches EXACTLY first, then case-insensitively, and is
 * emitted under the AUTHOR's spelling; an ambiguous fold is dropped. See the
 * body for why each rule matters.
 */
export function narrowSchemaToPublic(
   schema: { name: string; type: string }[],
   fields: readonly unknown[] | undefined,
): { name: string; type: string }[] {
   // `fields` has the author's spelling; `schema` has DESCRIBE's, which an
   // upper-folding warehouse (Snowflake, Oracle, Redshift, Teradata) reports in
   // its own case. Exact-only matching would narrow to nothing there, and the
   // caller drops an empty binding, silently losing the tier.
   const exactNames = new Set<string>();
   const byFoldedName = new Map<string, string[]>();
   for (const f of fields ?? []) {
      const name = (f as { name?: unknown }).name;
      if (typeof name === "string" && !isAccessRestricted(f)) {
         exactNames.add(name);
         const folded = name.toLowerCase();
         const sameFold = byFoldedName.get(folded);
         if (sameFold) {
            sameFold.push(name);
         } else {
            byFoldedName.set(folded, [name]);
         }
      }
   }
   const emitted = new Set<string>();
   const out: { name: string; type: string }[] = [];
   for (const c of schema) {
      let authorName: string | undefined;
      if (exactNames.has(c.name)) {
         // Exact first: Malloy allows fields differing only in case, and folding
         // first could declare a stored column under a dimension's name, which
         // serves the raw column in place of the computed one (wrong rows).
         authorName = c.name;
      } else {
         const candidates = byFoldedName.get(c.name.toLowerCase());
         // Exactly one, or drop (serves live) rather than guess.
         if (candidates?.length === 1) {
            authorName = candidates[0];
         }
      }
      // Not on the public surface: dropped. Neither lookup can invent a field.
      if (authorName === undefined) {
         continue;
      }
      // A folded and an exact hit can reach one field; declaring it twice fails
      // the shape, so the first in captured order wins.
      if (emitted.has(authorName)) {
         continue;
      }
      emitted.add(authorName);
      out.push({ name: authorName, type: c.type });
   }
   return out;
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
   return sqlTableReads(sql, tablePath) > 0;
}

/** How many times `sql` reads the table at `tablePath` ({@link sqlReadsTable}). */
export function sqlTableReads(sql: string, tablePath: string): number {
   const path = tablePath.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
   return (
      sql.match(
         new RegExp(`\\b(?:FROM|JOIN)\\s+${path}(?![\\w$".\x60])`, "gi"),
      ) ?? []
   ).length;
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
