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
 *
 * A table in a DuckLake catalog (a `ducklake` connection's, or one a DuckDB
 * connection attached) whose catalog names a 128-bit integer column is read
 * as holding a DOUBLE, which its Parquet files do
 * (`ducklakeStoresAsDouble`), so the bind never reads it as exact.
 */

import { runAsInternalStatement } from "../../duckdb_lockdown";
import type { Connection } from "@malloydata/malloy";
import { errMessage } from "../../../utils";
import { foldedTablePathParts, tablePathParts } from "../../quoting";
import { connectionTypeOf } from "../../connection_type";
import {
   ducklakeStoresAsDouble,
   spelledNativeType,
} from "../stored_column_types";
import type { FreshnessManifest, ObservedColumns } from "../domain/types";
import { NATIVE_SCHEMA_ROW_LIMIT } from "./database_native_types";
import { redactConnectionSecrets } from "./connection_redaction";
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
   return readTableColumns({
      tables: Object.entries(args.entries).map(([id, entry]) => ({
         key: id,
         connectionName: args.connectionNameOf(id),
         tableName: entry.tableName,
      })),
      connection: args.connection,
      ...(args.timeoutMs !== undefined ? { timeoutMs: args.timeoutMs } : {}),
   });
}

/**
 * Each table's columns as its warehouse's catalog holds them now, by the
 * caller's key, or why they could not be read: the read
 * {@link observedDatabaseColumns} makes, for any tables (the bind's bound
 * tables, a load's upstream tables). Every key gets an answer; never throws.
 */
export async function readTableColumns(args: {
   tables: readonly {
      key: string;
      /** Undefined: the table's connection is not known. */
      connectionName: string | undefined;
      /** Its path, unquoted and dot-separated. */
      tableName: string;
   }[];
   connection: (connectionName: string) => Promise<ReadConnection>;
   /**
    * The names are a model's (`connection.table('...')`), which the
    * warehouse resolves as SQL: each unquoted part is folded as the dialect
    * folds it ({@link foldedTablePathParts}; Postgres lower-cases, Snowflake
    * upper-cases). Otherwise the names are ones Publisher wrote, each part
    * created quoted, so read with its case kept.
    */
   foldUnquoted?: boolean;
   /** Overrides {@link BIND_READ_TIMEOUT_MS} (a test's). */
   timeoutMs?: number;
}): Promise<Map<string, ObservedColumns>> {
   const pathParts = args.foldUnquoted ? foldedTablePathParts : tablePathParts;
   const out = new Map<string, ObservedColumns>();
   const byConnection = new Map<string, string[]>();
   const tableNameOf = new Map(args.tables.map((t) => [t.key, t.tableName]));
   for (const { key: id, connectionName: name } of args.tables) {
      if (!name) {
         out.set(id, {
            unread: "connection_failed",
            detail: "the table's connection is not known",
         });
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
         const detail = redactConnectionSecrets(errMessage(err));
         for (const id of ids) {
            out.set(id, { unread: "connection_failed", detail });
         }
         continue;
      }
      const dialect = connection.dialectName;
      const tables: CatalogTable[] = [];
      for (const id of ids) {
         const parts = pathParts(tableNameOf.get(id)!, dialect);
         if (parts) tables.push({ key: id, parts });
         else out.set(id, { unread: "unparseable" });
      }
      const batches = nativeTypeCatalogBatches(tables, dialect);
      const batched = new Set(batches.flatMap((b) => b.keys));
      for (const t of tables) {
         if (!batched.has(t.key)) {
            out.set(t.key, {
               unread: "unparseable",
               detail: "the table's name gives no catalog to read",
            });
         }
      }
      // DuckLake writes a 128-bit integer to Parquet as a DOUBLE: decided
      // per table, from the catalog it is in.
      const ducklake = await ducklakeTablesOf(connection, batches);
      for (const batch of batches) {
         tasks.push(() =>
            readBatch(
               connection,
               dialect,
               ducklake,
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

/**
 * Each stored upstream's database table, of `tables` (by the caller's key),
 * that no longer holds the columns its build recorded, with why: read now
 * on `connection`, the one a build reads it through. A table is unmoved
 * when it holds every column with the native type its build captured
 * (`nativeSchema`). A chained
 * build reads the table in place of its source only then: a column retyped,
 * gone, or held as a DOUBLE in DuckLake since the build would feed the
 * downstream other values than live computes, under an address that says
 * otherwise. A table whose build captured no native types, or whose read
 * failed, cannot be told unmoved, so it counts as moved (fail safe: the
 * downstream recomputes it).
 *
 * Native types compare exactly, case and spacing aside: the read is the
 * capture's own catalog query, so an unaltered table reads as captured. A
 * stricter rule than the bind's (which reads a widened column as it is),
 * since a build can always recompute instead.
 */
export async function movedSinceBuild(
   tables: ReadonlyMap<string, MovedTable>,
   connection: ReadConnection,
): Promise<Map<string, UpstreamMove>> {
   const moved = new Map<string, UpstreamMove>();
   const toRead = new Map<string, MovedTable>();
   for (const [key, entry] of tables) {
      const unrecorded = unrecordedTypes(entry);
      if (unrecorded !== undefined) moved.set(key, unrecorded);
      else toRead.set(key, entry);
   }
   if (toRead.size === 0) return moved;
   const read = await readTableColumns({
      tables: [...toRead].map(([key, entry]) => ({
         key,
         connectionName: "build",
         tableName: entry.physicalTableName,
      })),
      connection: async () => connection,
   });
   for (const [key, entry] of toRead) {
      const move = movedColumns(entry, read.get(key) ?? { unread: "not_read" });
      if (move !== undefined) moved.set(key, move);
   }
   return moved;
}

/**
 * Why a stored upstream's table cannot be read in place of its source, by
 * class, for the metric (`publisher_materialization_upstream_recompute_total`),
 * and in words, for the entry and the log: a column `moved`, the table's
 * columns `unreadable` now, or `types_unrecorded` by its build.
 */
export interface UpstreamMove {
   kind: "moved" | "unreadable" | "types_unrecorded";
   reason: string;
}

/** A build that recorded no native type for some column: nothing to compare. */
function unrecordedTypes(built: {
   nativeSchema?: readonly { name?: string; type?: string }[];
}): UpstreamMove | undefined {
   if (
      !built.nativeSchema?.length ||
      built.nativeSchema.some(
         (c) => c.name === undefined || c.type === undefined,
      )
   ) {
      return {
         kind: "types_unrecorded",
         reason: "its build recorded no column types to check it against",
      };
   }
   return undefined;
}

/** {@link movedSinceBuild} for one table with recorded types, as `read` found it. */
function movedColumns(
   built: { nativeSchema?: readonly { name?: string; type?: string }[] },
   read: ObservedColumns,
): UpstreamMove | undefined {
   if ("unread" in read) {
      return {
         kind: "unreadable",
         reason: `its columns could not be read (${read.unread})`,
      };
   }
   for (const column of built.nativeSchema ?? []) {
      const name = column.name ?? "";
      const type = column.type ?? "";
      const now = columnNamed(read.columns, name);
      if (!now)
         return { kind: "moved", reason: `its column '${name}' is gone` };
      const nowType = now.nativeType ?? now.type ?? "";
      if (now.storedAsDouble || spelled(nowType) !== spelled(type)) {
         return {
            kind: "moved",
            reason:
               `its column '${name}' was built as ${type} and is now ` +
               `${nowType || "of no readable type"}`,
         };
      }
   }
   return undefined;
}

/** A table {@link movedSinceBuild} checks, as its manifest entry names it. */
interface MovedTable {
   physicalTableName: string;
   nativeSchema?: readonly { name?: string; type?: string }[];
}

/** A native type spelled one way whatever its case and spacing. */
function spelled(nativeType: string): string {
   return nativeType
      .replace(/\s+/g, " ")
      .replace(/\s*([(),])\s*/g, "$1")
      .trim()
      .toUpperCase();
}

/** The column named `name`: exactly, else the one that matches it case-blind. */
function columnNamed<C extends { name: string }>(
   columns: readonly C[],
   name: string,
): C | undefined {
   const exact = columns.find((c) => c.name === name);
   if (exact) return exact;
   const folded = columns.filter(
      (c) => c.name.toLowerCase() === name.toLowerCase(),
   );
   return folded.length === 1 ? folded[0] : undefined;
}

/**
 * The keys of `tables` that live in a DuckLake catalog. Every table of a
 * `ducklake` connection does; on a DuckDB connection, a table does when its
 * catalog (its path's first part, else the session's current database) is
 * one DuckDB attached as DuckLake, as a `setupSQL` that attaches and USEs a
 * lake makes it. Read from `duckdb_databases()` once per connection; a read
 * that fails counts every table of a DuckDB connection as DuckLake (fail
 * safe: a 128-bit column then answers live).
 */
async function ducklakeTablesOf(
   connection: ReadConnection,
   batches: readonly CatalogBatch[],
): Promise<Set<string>> {
   const all = new Set(batches.flatMap((b) => b.keys));
   if (connectionTypeOf(connection) === "ducklake") return all;
   const probe = batches.find((b) => b.attachedCatalogs)?.attachedCatalogs;
   if (!probe) return new Set();
   let rows: Record<string, unknown>[];
   try {
      // Publisher's own read of the session's catalogs, admitted past a
      // locked instance's gate, which refuses it to caller and author SQL.
      rows = (
         await runAsInternalStatement(probe.sql, () =>
            connection.runSQL(probe.sql, {
               rowLimit: NATIVE_SCHEMA_ROW_LIMIT,
            }),
         )
      ).rows as Record<string, unknown>[];
   } catch {
      return all;
   }
   const lakes = new Set(
      rows
         .filter((r) => String(r.type).toLowerCase() === "ducklake")
         .map((r) => String(r.database_name).toLowerCase()),
   );
   if (lakes.size === 0) return new Set();
   const current = String(rows[0]?.current ?? "").toLowerCase();
   return new Set(
      batches.flatMap((b) =>
         b.attachedCatalogs &&
         lakes.has((b.attachedCatalogs.catalog ?? current).toLowerCase())
            ? b.keys
            : [],
      ),
   );
}

/** Run one batch and record each of its keys' answer. */
async function readBatch(
   connection: ReadConnection,
   dialect: string,
   /** The keys of tables in a DuckLake catalog: a 128-bit integer holds a DOUBLE. */
   ducklake: ReadonlySet<string>,
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
      const detail = redactConnectionSecrets(errMessage(err), connection);
      const unread =
         err instanceof CatalogReadTimeout
            ? "timed_out"
            : "catalog_query_failed";
      for (const key of batch.keys) out.set(key, { unread, detail });
      return;
   }
   const columns = batch.columnsOf(rows);
   for (const key of batch.keys) {
      const read = columns.get(key);
      if (read === undefined) {
         out.set(key, { unread: "not_found" });
      } else if (read === "ambiguous") {
         out.set(key, {
            unread: "ambiguous",
         });
      } else {
         const named = read.filter((c) => c.name !== "" && c.type !== "");
         out.set(
            key,
            named.length === 0
               ? { unread: "not_found", detail: "its columns read empty" }
               : {
                    dialect,
                    columns: named.map((c) => {
                       if (
                          ducklake.has(key) &&
                          ducklakeStoresAsDouble(c.type)
                       ) {
                          return {
                             name: c.name,
                             type: "DOUBLE",
                             nativeType: "DOUBLE",
                             storedAsDouble: true as const,
                          };
                       }
                       const spelled = spelledNativeType(c.type, dialect);
                       return {
                          name: c.name,
                          ...(spelled ? { type: spelled } : {}),
                          nativeType: c.type,
                       };
                    }),
                 },
         );
      }
   }
}

/** An array type as a catalog names it: `DATE[]`, `integer[3]`, `ARRAY<INT64>`. */
/** A catalog read that outlived its bound. */
class CatalogReadTimeout extends Error {
   constructor(ms: number) {
      super(`the catalog read timed out after ${ms}ms`);
   }
}

/** `promise`, or a rejection after `ms`. */
function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
   let timer: ReturnType<typeof setTimeout> | undefined;
   return Promise.race([
      promise,
      new Promise<never>((_, reject) => {
         timer = setTimeout(() => reject(new CatalogReadTimeout(ms)), ms);
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
