// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `ducklake` connection type's registry entry: a DuckDB-dialect connection over a DuckLake catalog. */

import type { ConnectionOrigin } from "../../config";
import { networkedDuckDBOptions, setupIdentity } from "../duckdb_lockdown";
import { validateS3ProviderShape } from "../gcs_s3_utils";
import { validatePostgresSslmode } from "../postgres_conninfo";
import { buildDuckdbEntry } from "./duckdb";
import type {
   ApiConnection,
   ConnectionDialect,
   CoreConnectionEntry,
   NamedConnection,
} from "./types";

export const DUCKLAKE: ConnectionDialect = Object.freeze({
   type: "ducklake",
   attributes: Object.freeze({
      dialectName: "duckdb",
      isPool: false,
      canPersist: true,
      canStream: true,
   }),
   validateShape: validateDuckLakeShape,
   coreEntry: buildDuckLakeEntry,
});

function validateDuckLakeShape(
   connection: ApiConnection,
   origin: ConnectionOrigin,
): void {
   // Every field the ATTACH demands (see attachDuckLakeWithMode). A
   // destination is only attached at its first BUILD, so a field missed here
   // would surface hours after the config change that caused it.
   if (!connection.ducklakeConnection) {
      throw new Error("DuckLake connection configuration is missing.");
   }
   if (!connection.ducklakeConnection.catalog?.postgresConnection) {
      throw new Error(
         `PostgreSQL connection configuration is required for DuckLake catalog: ${connection.name}`,
      );
   }
   // No bucketUrl is allowed: the catalog must then exist, and its recorded
   // data path is used (connection.ts, dataPathClauses).
   // Shape only, as in the duckdb branch: a throw here fails the ENTIRE
   // environment. The mutual-exclusion and enum guards are safe; the key-pair
   // requirement is NOT applied, because failing every other connection over
   // an incomplete s3Connection is worse than reporting it when used.
   // validateStorageDestinations rejects one destination at a time, so it
   // applies the full check.
   const storage = connection.ducklakeConnection.storage;
   if (storage?.s3Connection) {
      validateS3ProviderShape(storage.s3Connection, connection.name, origin);
   }
   // The attach applies the catalog's own sslmode (buildPgConnectionString) and
   // throws on a value it cannot honor, at every query or build. An enum check,
   // like the guards above. A connectionString carries its own sslmode and the
   // field is ignored beside it, so it is not checked there.
   const catalogPg = connection.ducklakeConnection.catalog.postgresConnection;
   if (catalogPg.sslmode != null && !catalogPg.connectionString) {
      validatePostgresSslmode(
         `DuckLake catalog of connection '${connection.name}'`,
         catalogPg.sslmode,
      );
   }
   // metadataSchema reaches the ATTACH as a quoted string literal AND the
   // catalog-format preflight as a quoted identifier. Rather than escape one
   // value for two grammars, restrict it to a plain identifier.
   //
   // The typeof check is load-bearing: the value comes from untyped JSON and
   // RegExp.test() coerces, so `true` or `null` would pass the pattern and
   // reach escapeSQL as a non-string, a TypeError at the first attach.
   if (connection.ducklakeConnection.catalog.metadataSchema !== undefined) {
      const schema = connection.ducklakeConnection.catalog.metadataSchema;
      if (
         typeof schema !== "string" ||
         !/^[A-Za-z_][A-Za-z0-9_]*$/.test(schema)
      ) {
         throw new Error(
            `DuckLake catalog metadataSchema must be a plain identifier ` +
               `([A-Za-z_][A-Za-z0-9_]*), got '${schema}' for connection: ` +
               `${connection.name}`,
         );
      }
   }
   // destinationSchema becomes part of every stored table's path, quoted as an
   // identifier by quoteTablePath; a plain identifier keeps that one grammar.
   const destinationSchema = connection.ducklakeConnection.destinationSchema;
   if (destinationSchema !== undefined && destinationSchema !== null) {
      if (
         typeof destinationSchema !== "string" ||
         !/^[A-Za-z_][A-Za-z0-9_]*$/.test(destinationSchema)
      ) {
         throw new Error(
            `DuckLake destinationSchema must be a plain identifier ` +
               `([A-Za-z_][A-Za-z0-9_]*), got '${destinationSchema}' for connection: ` +
               `${connection.name}`,
         );
      }
   }
}

function buildDuckLakeEntry(
   connection: NamedConnection,
   environmentPath: string,
): CoreConnectionEntry {
   // Shape is validated up front by validateConnectionShape.
   return {
      ...buildDuckdbEntry(
         connection.name,
         environmentPath,
         `${connection.name}_ducklake.duckdb`,
      ),
      // Attached, then locked to its data path (duckdb_lockdown.ts). The key
      // digests the catalog config, so an edit opens a fresh instance rather
      // than re-attaching on a locked one.
      ...networkedDuckDBOptions(
         "ducklake",
         setupIdentity({
            ducklakeConnection: connection.ducklakeConnection ?? null,
            managed: connection.managed === true,
         }),
      ),
   };
}
