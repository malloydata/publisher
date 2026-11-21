// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A manifest entry as a well-behaved host sends it. The bind compares each
 * bound database table's columns, as its warehouse holds them now, with the
 * types its build recorded, and leaves out a table whose entry records none
 * (`types_unrecorded`, docs/materialization.md): a colocated (database) entry
 * carries its `nativeSchema`, a `storage=` entry its `schema`. A fixture that
 * binds a host manifest builds its entries through here, so it binds as a
 * host must; a fixture whose point is a typeless entry builds it by hand.
 */

import { capturedNativeSchema } from "../service/materialization/run/database_native_types";

/** One column's warehouse type, as a build records it. */
export interface RecordedColumn {
   name: string;
   type: string;
}

/** `entry` with the warehouse column types a build recorded for its table. */
export function withNativeSchema<T extends object>(
   entry: T,
   nativeSchema: readonly RecordedColumn[],
): T & { nativeSchema: RecordedColumn[] } {
   return { ...entry, nativeSchema: nativeSchema.map((c) => ({ ...c })) };
}

/**
 * The warehouse column types of `table`, read as a build records them
 * (`capturedNativeSchema`). Throws when none can be read: a fixture that
 * binds a typeless entry by accident would answer live.
 */
export async function recordedNativeSchema(
   connection: {
      runSQL(
         sql: string,
         options?: { rowLimit?: number },
      ): Promise<{ rows: unknown[] }>;
      dialectName: string;
   },
   table: string,
): Promise<RecordedColumn[]> {
   const columns = await capturedNativeSchema({
      run: (sql, options) => connection.runSQL(sql, options),
      physicalTableName: table,
      dialect: connection.dialectName,
      sourceName: table,
   });
   if (!columns?.length) {
      throw new Error(`no warehouse column types read for table ${table}`);
   }
   return columns.map((c) => ({ name: c.name, type: c.type }));
}

/**
 * Each database entry of `entries` with its table's warehouse column types,
 * read from `connection` (by `tableName`, else `physicalTableName`).
 */
export async function withRecordedTypes<
   T extends { tableName?: string; physicalTableName?: string },
>(
   entries: Record<string, T>,
   connection: Parameters<typeof recordedNativeSchema>[0],
): Promise<Record<string, T & { nativeSchema: RecordedColumn[] }>> {
   const out: Record<string, T & { nativeSchema: RecordedColumn[] }> = {};
   for (const [id, entry] of Object.entries(entries)) {
      const table = entry.tableName ?? entry.physicalTableName;
      if (!table) throw new Error(`entry ${id} names no table`);
      out[id] = withNativeSchema(
         entry,
         await recordedNativeSchema(connection, table.replace(/^"|"$/g, "")),
      );
   }
   return out;
}

/**
 * The warehouse column types a run's build reported for its one table (its
 * record's manifest entry's `nativeSchema`): what a host copies onto the
 * manifest it binds. Throws when the run reported none.
 */
export function runNativeSchema(
   run: Record<string, unknown>,
): RecordedColumn[] {
   const entries = Object.values(
      ((run.manifest as { entries?: Record<string, unknown> } | undefined)
         ?.entries ?? {}) as Record<
         string,
         { nativeSchema?: RecordedColumn[] }
      >,
   );
   const nativeSchema = entries.find(
      (e) => e.nativeSchema?.length,
   )?.nativeSchema;
   if (!nativeSchema) throw new Error("the run reported no column types");
   return nativeSchema.map((c) => ({ name: c.name, type: c.type }));
}
