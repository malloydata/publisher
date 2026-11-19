// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The log and error code for {@link DuckLakeCatalogUnavailableError}. */
export const DUCKLAKE_CATALOG_UNAVAILABLE = "ducklake_catalog_unavailable";

/**
 * A DuckLake catalog is missing from the locked DuckDB instance that should
 * hold it. A locked instance cannot ATTACH again, so the lookup that finds it
 * missing closes that connection and the next lookup opens a fresh instance
 * (connection.ts). A query routed to a storage destination that hits this
 * answers live (materialization/serve/leg.ts).
 */
export class DuckLakeCatalogUnavailableError extends Error {
   readonly code = DUCKLAKE_CATALOG_UNAVAILABLE;

   constructor(readonly connectionName: string) {
      super(
         `DuckLake catalog "${connectionName}" is not attached on its DuckDB instance; ` +
            `the connection is reopened on the next lookup`,
      );
      this.name = "DuckLakeCatalogUnavailableError";
   }
}
