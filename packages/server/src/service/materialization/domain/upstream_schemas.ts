// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The warehouse column types of the tables a persist source's build reads,
 * as one package load read them from each warehouse's catalog, and what a
 * source's content address folds in from them, so a table retyped upstream
 * moves the address of every source that reads it and its table is rebuilt.
 *
 * A reading is per table, keyed by connection and the table path as the
 * model writes it ({@link upstreamTableKey}): its columns with their native
 * types (`NUMERIC(12,4)`, `FLOAT64`, `DOUBLE`), or why it holds none: the
 * dialect gives the table no catalog to read (`no_catalog`), or the read
 * failed and no earlier reading of the table is known (`read_failed`). Both
 * are stable values: the same table read the same way folds the same text,
 * so a failure never moves an address back and forth between loads.
 *
 * A source whose tables cannot be known (`reads` omitted: an `.sql()`
 * source, a raw SQL expression, an unresolved reference in its lineage)
 * folds nothing, and keeps the address its SQL alone gives it.
 *
 * Pure, and plain data, so it crosses the package-load worker.
 */

import type { components } from "../../../api";

/** One table's columns as its warehouse's catalog names their types. */
export type UpstreamTableSchema = components["schemas"]["UpstreamTableSchema"];

/** One table's reading: its columns, or why there are none. */
export type UpstreamSchemaReading =
   | {
        columns: { name: string; nativeType: string }[];
        /**
         * The dialect the types are named in (Snowflake's `FLOAT` is a
         * double, DuckDB's a float4). Not folded into the address: the
         * connection's digest already is.
         */
        dialect?: string;
     }
   | { unread: "no_catalog" | "read_failed" };

/** Every table a plan's sources read, by {@link upstreamTableKey}. */
export type UpstreamSchemaReadings = Readonly<
   Record<string, UpstreamSchemaReading>
>;

/** One table a build reads, as the plan's `reads` names it. */
export interface UpstreamTable {
   connectionName: string;
   table: string;
}

/** The key of one table's reading: its connection and its path. */
export function upstreamTableKey(table: UpstreamTable): string {
   return JSON.stringify([table.connectionName, table.table]);
}

/**
 * A native type spelled one way whatever the catalog's spacing and case:
 * `numeric(12, 4)` and `NUMERIC(12,4)` are one type.
 */
function canonicalType(nativeType: string): string {
   return nativeType
      .replace(/\s+/g, " ")
      .replace(/\s*([(),])\s*/g, "$1")
      .trim()
      .toUpperCase();
}

/**
 * What the content address of a source reading `reads` folds in: one line
 * naming each table it reads with its reading, columns sorted by name.
 * Undefined, so the address is its SQL's alone, when the tables cannot be
 * known (`reads` undefined) or none of them has a reading (a plan derived
 * without reading any, which only a spec does).
 */
export function upstreamSchemaFold(
   reads: readonly UpstreamTable[] | undefined,
   readings: UpstreamSchemaReadings | undefined,
): string | undefined {
   if (!reads || !readings) return undefined;
   const parts: unknown[] = [];
   for (const table of reads) {
      const reading = readings[upstreamTableKey(table)];
      if (!reading) continue;
      parts.push([
         table.connectionName,
         table.table,
         "unread" in reading
            ? reading.unread
            : reading.columns
                 .map((c) => [c.name, canonicalType(c.nativeType)])
                 .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)),
      ]);
   }
   return parts.length === 0 ? undefined : JSON.stringify(parts);
}

/**
 * The tables a build of a source reading `reads` read, with their readings,
 * as a manifest entry records them (`ManifestEntry.upstreamSchemas`).
 * Undefined when the address folded none ({@link upstreamSchemaFold}).
 */
export function upstreamSchemasOf(
   reads: readonly UpstreamTable[] | undefined,
   readings: UpstreamSchemaReadings | undefined,
): UpstreamTableSchema[] | undefined {
   if (!reads || !readings) return undefined;
   const out: UpstreamTableSchema[] = [];
   for (const table of reads) {
      const reading = readings[upstreamTableKey(table)];
      if (!reading) continue;
      out.push({
         connectionName: table.connectionName,
         table: table.table,
         ...("unread" in reading
            ? { unread: reading.unread }
            : {
                 columns: reading.columns.map((c) => ({ ...c })),
                 ...(reading.dialect ? { dialect: reading.dialect } : {}),
              }),
      });
   }
   return out.length === 0 ? undefined : out;
}
