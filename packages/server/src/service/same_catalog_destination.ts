// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a served copy reads of its connections to put a managed DuckLake
 * connection's tables on a storage destination over the same catalog
 * (materialization/serve/same_catalog_reads.ts decides from it): the
 * connection's scope, when it and the destination are one catalog, read from
 * the connections a copy compiles with, through `config`. Whether the tables
 * it would read are readable is same_catalog_tables.ts's, which loads DuckDB
 * and so reaches a served copy through its host.
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
