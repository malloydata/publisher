// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which physical table a name names, and where a table lives. A table is a
 * name in a place (a connection, or a `storage=` destination: separate
 * namespaces that may share a name), and is compared three ways, each
 * named for what it is safe for:
 *
 * - {@link exactTableKey}: the name as written, in its place. Over-narrow:
 *   `Foo` and `foo` are two tables here, though one on a case-folding engine.
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
 * The table, exactly as written, in its place. JSON-encoded because a
 * physical name can contain a space.
 */
export function exactTableKey(t: PhysicalTable): string {
   return JSON.stringify([t.place.kind, t.place.name, t.name]);
}

/**
 * A written name's table segment, unquoted and case-folded. Containers are
 * left out, since a container does not say which table a name reaches (on
 * DuckDB a two-part name may be a catalog's or a schema's: `memory.t` and
 * `main.t` are one table), and nor does a place (two names for one DuckLake).
 */
export function leafKey(physicalTableName: string): string {
   const segments = physicalTableName.split(".");
   return segments[segments.length - 1].replace(/["`[\]]/g, "").toLowerCase();
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
   const canon = (name: string) =>
      name
         .split(".")
         .map((s) => s.replace(/["`[\]]/g, "").toLowerCase())
         .join(".");
   return tableNamesMeet(canon(a), canon(b));
}
