// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a served copy reads of its connections to put a managed DuckLake
 * connection's tables on a storage destination over the same catalog
 * (materialization/serve/same_catalog_reads.ts decides from it): the
 * connection's scope, when it and the destination are one catalog, and
 * whether a table on the destination is a base table, which a copy reads
 * again before each query it answers. Each reads the connections a copy
 * compiles with, through `config`.
 */

import type { MalloyConfig } from "@malloydata/malloy";
import type { components } from "../api";
import { connectionConfigOf, managedSchemasOf } from "./connection_type";
import { isSameCatalog } from "./materialization/domain/same_catalog_build";

type ApiConnection = components["schemas"]["Connection"];

/**
 * The schemas managed connection `connection` may read, when it and the
 * destination `alias` names in `config` are one catalog by the build's own
 * check (`isSameCatalog`, over the configuration each was resolved from);
 * undefined for a connection not recorded `managed`, another catalog, or
 * a lookup that fails.
 */
export async function sameCatalogScope(
   config: MalloyConfig,
   connection: string,
   alias: string,
): Promise<ReadonlySet<string> | undefined> {
   const [source, destination] = await Promise.all([
      config.connections.lookupConnection(connection).catch(() => undefined),
      config.connections.lookupConnection(alias).catch(() => undefined),
   ]);
   const schemas = managedSchemasOf(source);
   return schemas &&
      isSameCatalog(
         connectionConfigOf(source) as ApiConnection | undefined,
         connectionConfigOf(destination) as ApiConnection | undefined,
      )
      ? schemas
      : undefined;
}

/** A `[catalog, schema, table]` read through a destination, plain names. */
export type DestinationTable = readonly [string, string, string];

/**
 * The newest base-table verdicts per destination connection and catalog, each
 * as of the catalog snapshot it was read at. A verdict holds for exactly that
 * snapshot: any write to the catalog (a table swapped for a view among them)
 * commits a new one, which the next read sees and probes again. Keyed by the
 * connection object, so a reopened destination starts empty.
 */
const verdicts = new WeakMap<
   object,
   Map<string, { snapshot: string; base: ReadonlyMap<string, boolean> }>
>();

const tableKey = (schema: string, table: string) =>
   `${schema.toLowerCase()}.${table.toLowerCase()}`;

const literal = (value: string) => `'${value.replace(/'/g, "''")}'`;

/**
 * Whether each of `tables` is, now, one row of the destination `alias` names
 * in `config`'s `information_schema.tables`, of type `BASE TABLE`: a view's
 * body would run with the destination's credential, which the managed
 * connection's scope does not hold it to.
 *
 * Read at every use of a copy that reads them, since the catalog can change
 * under a compiled copy. One read of the catalog's current snapshot id
 * answers from the verdicts of that snapshot when they cover `tables`; else
 * one statement reads the snapshot id and the tables' types together (one
 * transaction, so one snapshot), and its verdicts are kept for that snapshot.
 * Every catalog in `tables` must be one the destination attaches. Throws when
 * a read fails: the caller treats that as no.
 */
export async function allBaseTables(
   config: MalloyConfig,
   alias: string,
   tables: readonly DestinationTable[],
): Promise<boolean> {
   if (tables.length === 0) return true;
   const destination = await config.connections.lookupConnection(alias);
   const byCatalog = new Map<string, DestinationTable[]>();
   for (const t of tables) {
      byCatalog.set(t[0], [...(byCatalog.get(t[0]) ?? []), t]);
   }
   let known = verdicts.get(destination);
   if (!known) {
      known = new Map();
      verdicts.set(destination, known);
   }
   for (const [catalog, wanted] of byCatalog) {
      const quoted = `"${catalog.replace(/"/g, '""')}"`;
      const held = known.get(catalog);
      if (held && wanted.every(([, s, t]) => held.base.has(tableKey(s, t)))) {
         const now = await destination.runSQL(
            `SELECT id FROM ${quoted}.current_snapshot()`,
         );
         const [row] = now.rows as { id?: unknown }[];
         if (row && String(row.id) === held.snapshot) {
            if (wanted.some(([, s, t]) => !held.base.get(tableKey(s, t)))) {
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
      const result = await destination.runSQL(
         "SELECT s.id AS snapshot_id, t.table_schema, t.table_name, t.table_type " +
            `FROM ${quoted}.current_snapshot() s ` +
            "LEFT JOIN information_schema.tables t " +
            `ON t.table_catalog = ${literal(catalog)} AND (${matches})`,
      );
      const rows = result.rows as {
         snapshot_id?: unknown;
         table_schema?: unknown;
         table_name?: unknown;
         table_type?: unknown;
      }[];
      if (rows.length === 0) return false;
      const seen = new Map<string, unknown[]>();
      for (const r of rows) {
         if (r.table_name == null) continue;
         const key = tableKey(String(r.table_schema), String(r.table_name));
         seen.set(key, [...(seen.get(key) ?? []), r.table_type]);
      }
      const snapshot = String(rows[0].snapshot_id);
      // Verdicts of one snapshot add up; another snapshot's are dropped.
      const base = new Map<string, boolean>(
         held?.snapshot === snapshot ? held.base : [],
      );
      for (const [, s, t] of wanted) {
         const types = seen.get(tableKey(s, t)) ?? [];
         base.set(
            tableKey(s, t),
            types.length === 1 && types[0] === "BASE TABLE",
         );
      }
      known.set(catalog, { snapshot, base });
      if (wanted.some(([, s, t]) => !base.get(tableKey(s, t)))) return false;
   }
   return true;
}
