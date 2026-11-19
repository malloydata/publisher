// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The log and error code for {@link DuckLakeCatalogUnavailableError}. */
export const DUCKLAKE_CATALOG_UNAVAILABLE = "ducklake_catalog_unavailable";

/**
 * The log code for a DuckLake attach that failed: the lookups sharing it fail
 * with it, and the next lookup attaches again.
 */
export const DUCKLAKE_ATTACH_FAILED = "ducklake_attach_failed";

/**
 * A DuckLake catalog is missing from the locked DuckDB instance that should
 * hold it. A locked instance cannot ATTACH again, so the lookup that finds it
 * missing closes that connection and the next lookup opens a fresh instance
 * (connection.ts). A query routed to a storage destination that hits this
 * answers live (materialization/serve/leg.ts).
 */
export class DuckLakeCatalogUnavailableError extends Error {
   readonly code = DUCKLAKE_CATALOG_UNAVAILABLE;

   constructor(
      readonly connectionName: string,
      options?: { cause?: unknown },
   ) {
      super(
         `DuckLake catalog "${connectionName}" is not attached on its DuckDB instance; ` +
            `the connection is reopened on the next lookup`,
         options,
      );
      this.name = "DuckLakeCatalogUnavailableError";
   }
}

/**
 * Whether `error`, or an error it was caused by, is a
 * {@link DuckLakeCatalogUnavailableError}. By code, since a runtime may wrap
 * or copy the error it is handed.
 */
export function isDuckLakeCatalogUnavailable(error: unknown): boolean {
   for (let e = error, depth = 0; e && depth < 5; depth++) {
      if ((e as { code?: unknown }).code === DUCKLAKE_CATALOG_UNAVAILABLE) {
         return true;
      }
      e = (e as { cause?: unknown }).cause;
   }
   return false;
}
