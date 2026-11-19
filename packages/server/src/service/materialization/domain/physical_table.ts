// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which physical table a name names, and where a table lives. A table is a
 * name in a place (a connection, or a `storage=` destination: separate
 * namespaces that may share a name), and is compared three ways, each
 * named for what it is safe for:
 *
 * - {@link tableKey}: the written name in its place, case-folded only where
 *   the place folds it (every DuckDB destination; a connection whose dialect
 *   folds table names), never elsewhere: on Postgres `"T"` and `"t"` are two
 *   tables. A run's duplicate target and failed target's reuse.
 * - {@link recordedTableKey}: a recorded table exactly as recorded, on its
 *   connection and in its destination: a ledger row (the store matches it
 *   exactly, and a host echoes back the names it was given) and the
 *   reclaim's bookkeeping of the tables it renamed away from and keeps, so
 *   two recorded tables are never decided as one.
 * - {@link leafKey} / {@link placedLeafKey}: the last segment, unquoted and
 *   case-folded (in its place, for the placed form). Over-wide: `a.t` and
 *   `b.t` are one table here, which only ever makes a run refuse, a version
 *   serve live, or a reclaim keep a table.
 * - {@link mayBeSameTable}: every segment unquoted and case-folded, the
 *   containers matched by suffix ({@link tableNamesMeet}); the conservative
 *   answer for deciding a table is NOT another.
 */

import { tableNamesMeet } from "./table_name";

/** Where a physical table lives: on a connection, or in a destination. */
interface TablePlace {
   readonly kind: "connection" | "destination";
   readonly name: string;
}

/** A physical table: a name in a place. */
interface PhysicalTable {
   readonly place: TablePlace;
   readonly name: string;
}

/**
 * The table an instruction writes: in its destination when it names one,
 * else on the source's connection.
 */
export function instructedTable(
   instruction: { destination?: string; physicalTableName: string },
   connectionName: string,
): PhysicalTable {
   return {
      place: instruction.destination
         ? { kind: "destination", name: instruction.destination }
         : { kind: "connection", name: connectionName },
      name: instruction.physicalTableName,
   };
}

/**
 * The table a manifest entry (or any record of a built table) names: in its
 * storage destination when it has one, else on its connection.
 */
export function entryTable(entry: {
   connectionName?: string | null;
   storageDestinationName?: string | null;
   physicalTableName: string;
}): PhysicalTable {
   return {
      place: entry.storageDestinationName
         ? { kind: "destination", name: entry.storageDestinationName }
         : { kind: "connection", name: entry.connectionName ?? "" },
      name: entry.physicalTableName,
   };
}

/**
 * "This table" as a run compares it: the written name in its place, every
 * segment case-folded only when `foldsCase` (the place's own folding:
 * `tableNamesFoldCase`), else exactly as written. JSON-encoded because a
 * physical name can contain a space. Never folds where the warehouse keeps
 * case, so two tables there are never one.
 */
export function tableKey(t: PhysicalTable, foldsCase: boolean): string {
   return JSON.stringify([
      t.place.kind,
      t.place.name,
      foldsCase ? foldedSegments(t.name).join(".") : t.name,
   ]);
}

/**
 * A recorded table's identity: the name as written, on its connection and in
 * its destination (both, as the store keys a ledger row and a run records a
 * table). Exact: the store matches a row exactly, a host echoes back the
 * names it was given, and the reclaim decides each recorded table on its own.
 */
export function recordedTableKey(row: {
   connectionName?: string | null;
   storageDestinationName?: string | null;
   physicalTableName: string;
}): string {
   return JSON.stringify([
      row.connectionName ?? "",
      row.storageDestinationName ?? "",
      row.physicalTableName,
   ]);
}

/**
 * A written name's table segment, unquoted and case-folded. Containers are
 * left out, since a container does not say which table a name reaches (on
 * DuckDB a two-part name may be a catalog's or a schema's: `memory.t` and
 * `main.t` are one table), and nor does a place (two names for one DuckLake).
 */
export function leafKey(physicalTableName: string): string {
   const segments = foldedSegments(physicalTableName);
   return segments[segments.length - 1];
}

/** The table's {@link leafKey}, in its place. */
export function placedLeafKey(t: PhysicalTable): string {
   return JSON.stringify([t.place.kind, t.place.name, leafKey(t.name)]);
}

/**
 * Whether two written names, in one place, may be one table: compared
 * unquoted and case-folded, with {@link tableNamesMeet}'s container rule.
 */
export function mayBeSameTable(a: string, b: string): boolean {
   return tableNamesMeet(
      foldedSegments(a).join("."),
      foldedSegments(b).join("."),
   );
}

/**
 * A written name's segments, each unquoted (every `"`, backtick and bracket
 * dropped, a doubled quote included) and case-folded.
 */
function foldedSegments(name: string): string[] {
   return name.split(".").map((s) => s.replace(/["`[\]]/g, "").toLowerCase());
}
