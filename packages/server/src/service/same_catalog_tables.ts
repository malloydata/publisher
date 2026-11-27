// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Whether tables a storage destination reads on a managed DuckLake
 * connection's behalf hold only what that connection could read itself.
 *
 * A destination over the same catalog as a `managed` connection reads with a
 * credential that reaches the whole catalog. A served copy serving a join
 * across the two (materialization/serve/same_catalog_reads.ts) and a
 * same-catalog build (materialization_build_session.ts) read the connection's
 * tables that way, so each table they read must be:
 *
 * - a base table: a view's body is not held to the connection's scope;
 * - made only of files under the storage prefix of a schema the connection
 *   may read (`main` and its `readableSchemas`), as its storage secret's
 *   `SCOPE` is computed (`storageSecretScope`): a catalog writer can register
 *   any file into a table (`ducklake_add_data_files`), and the destination
 *   would read a file the connection's secret refuses.
 *
 * Both are read at one catalog snapshot. Loads DuckDB through its imports, so
 * a served copy reaches it through its host, never by import.
 */

import type { MalloyConfig } from "@malloydata/malloy";
import { connectionConfigOf } from "./connection_type";
import {
   metadataCatalogRef,
   type RecordedSchemaPath,
   storageSecretScope,
} from "./dialects/ducklake_catalog";
import { runInternalSQL } from "./duckdb_lockdown";

/** A `[catalog, schema, table]` read through a destination, plain names. */
export type DestinationTable = readonly [string, string, string];

/** What runs a statement on the session that reads the tables: its rows. */
type Run = (sql: string) => Promise<Record<string, unknown>[]>;

const literal = (value: string) => `'${value.replace(/'/g, "''")}'`;
const ident = (value: string) => `"${value.replace(/"/g, '""')}"`;

/**
 * Whether `file` lies under one of `prefixes` (each ending in one `/`), as a
 * storage secret's `SCOPE` matches: byte for byte, so `…/main/` never admits
 * `…/main_evil/x`, and a path spelled in another case fails. A path that
 * could resolve elsewhere than it reads fails too: one with a `.` or `..`
 * segment, a backslash, a percent escape or a NUL.
 */
export function fileInScope(
   file: string,
   prefixes: readonly string[],
): boolean {
   if (/[%\\\0]/.test(file)) return false;
   const at = file.indexOf("://");
   const rest = at >= 0 ? file.slice(at + 3) : file;
   if (rest.split("/").some((part) => part === "." || part === "..")) {
      return false;
   }
   return prefixes.some(
      (prefix) => prefix.endsWith("/") && file.startsWith(prefix),
   );
}

/** Where a managed connection's catalog keeps its files, from its config. */
export interface ManagedStorage {
   /** The configured data path, when its config states one. */
   configuredDataPath?: string;
   /** The schema holding the catalog's `ducklake_*` tables. */
   metadataSchema?: string;
   /** The schemas it may read: `main` and its `readableSchemas`. */
   schemas: ReadonlySet<string>;
}

/** {@link ManagedStorage} of the connection whose config is `config`. */
export function managedStorageOf(
   config: unknown,
   schemas: ReadonlySet<string>,
): ManagedStorage {
   const ducklake = (
      config as {
         ducklakeConnection?: {
            storage?: { bucketUrl?: string };
            catalog?: { metadataSchema?: string };
         };
      }
   )?.ducklakeConnection;
   return {
      configuredDataPath: ducklake?.storage?.bucketUrl,
      metadataSchema: ducklake?.catalog?.metadataSchema,
      schemas,
   };
}

/**
 * Why a table among `tables` (`[schema, table]`, each a base table of
 * `catalog`) holds a file the managed connection's storage scope would not
 * reach, at catalog snapshot `snapshot`; undefined when none does. The scope
 * is computed as the connection's secret is, from the schema paths the catalog
 * records at that snapshot. Throws when anything cannot be read, or the
 * scope cannot be computed: the caller treats that as a refusal.
 */
export async function fileOutsideScope(
   run: Run,
   catalog: string,
   snapshot: string,
   tables: readonly (readonly [string, string])[],
   storage: ManagedStorage,
): Promise<string | undefined> {
   if (!/^\d+$/.test(snapshot)) throw new Error("not a snapshot id");
   const meta = metadataCatalogRef(catalog, storage.metadataSchema);
   const recordedDataPath = storage.configuredDataPath
      ? undefined
      : (
           await run(
              `SELECT value FROM ${meta}.ducklake_metadata WHERE scope IS NULL AND key = 'data_path'`,
           )
        )[0]?.value;
   const schemaPaths = new Map<string, RecordedSchemaPath>();
   for (const row of await run(
      `SELECT schema_name, path, path_is_relative FROM ${meta}.ducklake_schema ` +
         `WHERE begin_snapshot <= ${snapshot} AND (end_snapshot IS NULL OR end_snapshot > ${snapshot})`,
   )) {
      if (typeof row.schema_name === "string" && typeof row.path === "string") {
         schemaPaths.set(row.schema_name, {
            path: row.path,
            relative: row.path_is_relative !== false,
         });
      }
   }
   const prefixes = storageSecretScope({
      catalog,
      configuredDataPath: storage.configuredDataPath,
      recordedDataPath:
         typeof recordedDataPath === "string" ? recordedDataPath : undefined,
      schemaPaths,
      readableSchemas: [...storage.schemas].filter((s) => s !== "main"),
      readOnly: true,
      managed: true,
   });
   if (!prefixes) throw new Error("no storage scope");
   for (const [schema, table] of tables) {
      const files = await run(
         `SELECT data_file, delete_file FROM ducklake_list_files(${literal(catalog)}, ${literal(table)}, ` +
            `schema => ${literal(schema)}, snapshot_version => ${snapshot})`,
      );
      for (const row of files) {
         for (const file of [row.data_file, row.delete_file]) {
            if (file === null || file === undefined) continue;
            if (typeof file !== "string" || !fileInScope(file, prefixes)) {
               return `the table '${schema}.${table}' holds a file outside the connection's storage scope`;
            }
         }
      }
   }
   return undefined;
}

/** The tables a copy reads through one destination for one managed connection. */
export interface SameCatalogRead {
   /** The destination's served-copy alias. */
   alias: string;
   /** The managed connection the tables are its. */
   connection: string;
   /** The schemas the managed connection may read. */
   schemas: ReadonlySet<string>;
   tables: readonly DestinationTable[];
}

/**
 * The newest verdicts per destination connection and catalog, each as of the
 * catalog snapshot it was read at. A verdict holds for exactly that snapshot:
 * any write to the catalog (a table swapped for a view, a file registered into
 * a table) commits a new one, which the next read sees and probes again.
 * Keyed by the connection object, so a reopened destination starts empty.
 */
const verdicts = new WeakMap<
   object,
   Map<string, { snapshot: string; readable: ReadonlyMap<string, boolean> }>
>();

/**
 * Whether every table of `read` is, now, a base table of the destination
 * `read.alias` names in `config`, made only of files the managed connection's
 * storage scope reaches ({@link fileOutsideScope}).
 *
 * Read at every use of a copy that reads them, since the catalog can change
 * under a compiled copy. One read of the catalog's current snapshot id
 * answers from that snapshot's verdicts when they cover the tables; else one
 * statement reads the snapshot id and the tables' types together, the files
 * are read at that snapshot, and the verdicts are kept for it. Throws when a
 * read fails: the caller treats that as no.
 */
export async function sameCatalogTablesReadable(
   config: MalloyConfig,
   read: SameCatalogRead,
): Promise<boolean> {
   if (read.tables.length === 0) return true;
   const [destination, managed] = await Promise.all([
      config.connections.lookupConnection(read.alias),
      config.connections.lookupConnection(read.connection),
   ]);
   const storage = managedStorageOf(connectionConfigOf(managed), read.schemas);
   const run: Run = async (sql) =>
      (await runInternalSQL(destination, sql)).rows as Record<
         string,
         unknown
      >[];
   const scopeKey = [...read.schemas].sort().join(",");
   const key = (schema: string, table: string) =>
      `${scopeKey}\u0000${schema.toLowerCase()}.${table.toLowerCase()}`;
   const byCatalog = new Map<string, DestinationTable[]>();
   for (const t of read.tables) {
      byCatalog.set(t[0], [...(byCatalog.get(t[0]) ?? []), t]);
   }
   let known = verdicts.get(destination);
   if (!known) {
      known = new Map();
      verdicts.set(destination, known);
   }
   for (const [catalog, wanted] of byCatalog) {
      const held = known.get(catalog);
      if (held && wanted.every(([, s, t]) => held.readable.has(key(s, t)))) {
         const [row] = await run(
            `SELECT id FROM ${ident(catalog)}.current_snapshot()`,
         );
         if (row && String(row.id) === held.snapshot) {
            if (wanted.some(([, s, t]) => !held.readable.get(key(s, t)))) {
               return false;
            }
            continue;
         }
      }
      const matches = wanted
         .map(
            ([, s, t]) =>
               `(lower(t.table_schema) = lower(${literal(s)}) AND lower(t.table_name) = lower(${literal(t)}))`,
         )
         .join(" OR ");
      const rows = await run(
         "SELECT s.id AS snapshot_id, t.table_schema, t.table_name, t.table_type " +
            `FROM ${ident(catalog)}.current_snapshot() s ` +
            "LEFT JOIN information_schema.tables t " +
            `ON t.table_catalog = ${literal(catalog)} AND (${matches})`,
      );
      if (rows.length === 0) return false;
      const snapshot = String(rows[0].snapshot_id);
      const types = new Map<string, unknown[]>();
      for (const r of rows) {
         if (r.table_name == null) continue;
         const k = key(String(r.table_schema), String(r.table_name));
         types.set(k, [...(types.get(k) ?? []), r.table_type]);
      }
      const base = wanted.filter(([, s, t]) => {
         const found = types.get(key(s, t)) ?? [];
         return found.length === 1 && found[0] === "BASE TABLE";
      });
      const outside =
         base.length === wanted.length
            ? await fileOutsideScope(
                 run,
                 catalog,
                 snapshot,
                 base.map(([, s, t]) => [s, t] as const),
                 storage,
              )
            : "not a base table";
      // One verdict for the whole set: a file outside the scope is not
      // traced to its table, and the set is read together.
      const ok = outside === undefined;
      // Verdicts of one snapshot add up; another snapshot's are dropped.
      const readable = new Map<string, boolean>(
         held?.snapshot === snapshot ? held.readable : [],
      );
      for (const [, s, t] of wanted) readable.set(key(s, t), ok);
      known.set(catalog, { snapshot, readable });
      if (!ok) return false;
   }
   return true;
}
