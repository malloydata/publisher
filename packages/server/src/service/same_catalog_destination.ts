// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a served copy reads of its connections to put a managed DuckLake
 * connection's tables on a storage destination over the same catalog
 * (materialization/serve/same_catalog_reads.ts decides from it): the
 * connection's scope, when it and the destination are one catalog, and
 * whether a table on the destination is a base table. Both read the
 * connections a copy compiles with, through `config`.
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

/**
 * Whether each of `tables` (`[catalog, schema, table]`, plain names) is one
 * row of the destination `alias` names in `config`'s
 * `information_schema.tables`, of type `BASE TABLE`: a view's body would run
 * with the destination's credential.
 */
export async function allBaseTables(
   config: MalloyConfig,
   alias: string,
   tables: readonly (readonly [string, string, string])[],
): Promise<boolean> {
   const destination = await config.connections.lookupConnection(alias);
   for (const [catalog, schema, table] of tables) {
      const result = await destination.runSQL(
         "SELECT table_type FROM information_schema.tables " +
            `WHERE table_catalog = '${catalog}' ` +
            `AND lower(table_schema) = lower('${schema}') ` +
            `AND lower(table_name) = lower('${table}')`,
      );
      const rows = result.rows as { table_type?: unknown }[];
      if (rows.length !== 1 || rows[0].table_type !== "BASE TABLE") {
         return false;
      }
   }
   return true;
}
