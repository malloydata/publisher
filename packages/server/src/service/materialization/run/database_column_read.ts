// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The bind's read of each bound database table's columns, as its warehouse
 * holds them now ({@link observedDatabaseColumns}), so a column altered
 * outside a build is read with the type the table holds
 * (`serve/bind.ts`, `applyObservedColumns`).
 *
 * The catalog queries are the build's native-type capture's, batched: one
 * per schema or dataset, one for every Postgres table, DuckDB's in-process
 * `DESCRIBE` per table (`warehouse_sql.ts`, `nativeTypeCatalogBatches`). They
 * run with bounded concurrency, each under a timeout, never per query. A
 * table whose read fails, times out, finds nothing, finds its name in more
 * than one schema, whose name does not parse, or whose connection is
 * unknown, is `unread`: the bind leaves it out and its sources answer live
 * (fail safe). Never throws.
 */

import { getDialect, type Connection } from "@malloydata/malloy";
import { errMessage } from "../../../utils";
import { tablePathParts } from "../../quoting";
import { storedColumnsOfOutput } from "../stored_column_types";
import type { FreshnessManifest, ObservedColumns } from "../domain/types";
import { NATIVE_SCHEMA_ROW_LIMIT } from "./colocated_native_types";
import {
   type CatalogBatch,
   type CatalogTable,
   nativeTypeCatalogBatches,
} from "./warehouse_sql";

/**
 * How long one catalog query may take before its tables are read as unread
 * and answer live: the manifest fetch's bound (`environment.ts`), since both
 * run before a package serves.
 */
const BIND_READ_TIMEOUT_MS = 15_000;

/** Catalog queries in flight at once, across a bind's connections. */
const BIND_READ_CONCURRENCY = 4;

type ReadConnection = Pick<Connection, "runSQL" | "dialectName">;

/**
 * Each entry's columns as its warehouse holds them now, by sourceEntityId,
 * or why they could not be read. Every entry gets an answer.
 */
export async function observedDatabaseColumns(args: {
   entries: FreshnessManifest;
   /** The connection each entry's table lives on, by sourceEntityId. */
   connectionNameOf: (sourceEntityId: string) => string | undefined;
   connection: (connectionName: string) => Promise<ReadConnection>;
   /** Overrides {@link BIND_READ_TIMEOUT_MS} (a test's). */
   timeoutMs?: number;
}): Promise<Map<string, ObservedColumns>> {
   const out = new Map<string, ObservedColumns>();
   const byConnection = new Map<string, string[]>();
   for (const id of Object.keys(args.entries)) {
      const name = args.connectionNameOf(id);
      if (!name) {
         out.set(id, { unread: "the table's connection is not known" });
         continue;
      }
      byConnection.set(name, [...(byConnection.get(name) ?? []), id]);
   }
   const tasks: (() => Promise<void>)[] = [];
   for (const [name, ids] of byConnection) {
      let connection: ReadConnection;
      try {
         connection = await args.connection(name);
      } catch (err) {
         for (const id of ids) out.set(id, { unread: errMessage(err) });
         continue;
      }
      const dialect = connection.dialectName;
      const tables: CatalogTable[] = [];
      for (const id of ids) {
         const parts = tablePathParts(args.entries[id].tableName, dialect);
         if (parts) tables.push({ key: id, parts });
         else out.set(id, { unread: "the table's name does not parse" });
      }
      const batches = nativeTypeCatalogBatches(tables, dialect);
      const batched = new Set(batches.flatMap((b) => b.keys));
      for (const t of tables) {
         if (!batched.has(t.key)) {
            out.set(t.key, {
               unread: "the table's name gives no catalog to read",
            });
         }
      }
      for (const batch of batches) {
         tasks.push(() =>
            readBatch(
               connection,
               dialect,
               batch,
               args.timeoutMs ?? BIND_READ_TIMEOUT_MS,
               out,
            ),
         );
      }
   }
   await runBounded(tasks, BIND_READ_CONCURRENCY);
   return out;
}

/** Run one batch and record each of its keys' answer. */
async function readBatch(
   connection: ReadConnection,
   dialect: string,
   batch: CatalogBatch,
   timeoutMs: number,
   out: Map<string, ObservedColumns>,
): Promise<void> {
   let rows: Record<string, unknown>[];
   try {
      const result = await withTimeout(
         connection.runSQL(batch.sql, { rowLimit: NATIVE_SCHEMA_ROW_LIMIT }),
         timeoutMs,
      );
      rows = result.rows as Record<string, unknown>[];
   } catch (err) {
      for (const key of batch.keys) out.set(key, { unread: errMessage(err) });
      return;
   }
   const columns = batch.columnsOf(rows);
   const malloy = getDialect(dialect);
   for (const key of batch.keys) {
      const read = columns.get(key);
      if (read === undefined) {
         out.set(key, { unread: "the table was not found in its catalog" });
      } else if (read === "ambiguous") {
         out.set(key, {
            unread: "the table's name matched more than one table",
         });
      } else {
         const named = read.filter((c) => c.name !== "" && c.type !== "");
         out.set(
            key,
            named.length === 0
               ? { unread: "the table's columns read empty" }
               : {
                    dialect,
                    columns: named.map((c) => {
                       // Spelled as the build's capture spells it; a type
                       // Malloy reads as no basic type has no spelling.
                       const [spelled] = storedColumnsOfOutput(
                          [
                             {
                                name: c.name,
                                ...malloy.sqlTypeToMalloyType(c.type),
                             },
                          ],
                          { nested: false, bigint: true, timestamptz: true },
                       );
                       return {
                          name: c.name,
                          ...(spelled ? { type: spelled.type } : {}),
                          nativeType: c.type,
                       };
                    }),
                 },
         );
      }
   }
}

/** `promise`, or a rejection after `ms`. */
function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
   let timer: ReturnType<typeof setTimeout> | undefined;
   return Promise.race([
      promise,
      new Promise<never>((_, reject) => {
         timer = setTimeout(
            () => reject(new Error(`the catalog read timed out after ${ms}ms`)),
            ms,
         );
      }),
   ]).finally(() => clearTimeout(timer));
}

/** Run `tasks`, at most `limit` at once. */
async function runBounded(
   tasks: (() => Promise<void>)[],
   limit: number,
): Promise<void> {
   let next = 0;
   const worker = async () => {
      while (next < tasks.length) {
         const task = tasks[next++];
         await task();
      }
   };
   await Promise.all(
      Array.from({ length: Math.min(limit, tasks.length) }, worker),
   );
}
