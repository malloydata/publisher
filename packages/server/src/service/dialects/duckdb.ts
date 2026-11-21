// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `duckdb` connection type's registry entry. */

import path from "path";
import {
   localLockedDuckDBOptions,
   networkedDuckDBOptions,
   setupIdentity,
} from "../duckdb_lockdown";
import { validateS3ProviderShape } from "../gcs_s3_utils";
import type {
   ApiConnection,
   ConnectionDialect,
   CoreConnectionEntry,
   NamedConnection,
} from "./types";

export const DUCKDB: ConnectionDialect = Object.freeze({
   type: "duckdb",
   attributes: Object.freeze({
      dialectName: "duckdb",
      isPool: false,
      canPersist: true,
      canStream: true,
   }),
   validateShape: validateDuckDBShape,
   coreEntry: buildDuckDBEntry,
});

export function buildDuckdbEntry(
   name: string,
   environmentPath: string,
   databaseFilename = `${name}.duckdb`,
   setupSQL?: string,
): CoreConnectionEntry {
   const entry: CoreConnectionEntry = {
      is: "duckdb",
      databasePath: path.join(environmentPath, databaseFilename),
   };
   if (typeof setupSQL === "string" && setupSQL.trim().length > 0) {
      entry.setupSQL = setupSQL;
   }
   return entry;
}

function validateDuckDBShape(connection: ApiConnection): void {
   if (!connection.duckdbConnection) {
      throw new Error("DuckDB connection configuration is missing.");
   }
   const attached = connection.duckdbConnection.attachedDatabases ?? [];
   // AttachedDatabase reuses the BigqueryConnection schema, but the ATTACH
   // path builds a DuckDB BIGQUERY secret from key JSON (the extension takes a
   // key, not a token), so impersonation would be silently ignored.
   for (const attachedDb of attached) {
      if (
         attachedDb.type === "bigquery" &&
         attachedDb.bigqueryConnection?.impersonateServiceAccount
      ) {
         throw new Error(
            `Attached database '${attachedDb.name}' on DuckDB connection ` +
               `'${connection.name}' sets impersonateServiceAccount, which ` +
               `is not supported on attached databases: DuckDB's BIGQUERY ` +
               `secret authenticates with a service account key, not a ` +
               `token. Use serviceAccountKeyJson for attached BigQuery ` +
               `databases.`,
         );
      }
   }
   const setupSQL = connection.duckdbConnection.setupSQL;
   const hasSetupSQL =
      typeof setupSQL === "string" && setupSQL.trim().length > 0;
   if (attached.length === 0 && !hasSetupSQL) {
      throw new Error(
         `DuckDB connection "${connection.name}" must provide either attachedDatabases or non-empty setupSQL.`,
      );
   }
   // Shape only, deliberately not the full credential check: this throw fails
   // the ENTIRE environment (assembleEnvironmentConnections has no
   // per-connection recovery), so demanding a key pair here would turn a
   // config that fails late on one attachment into one that loads nothing.
   // Only shapes no path accepts are rejected.
   for (const attachedDb of attached) {
      if (attachedDb.type === "s3" && attachedDb.s3Connection) {
         validateS3ProviderShape(
            attachedDb.s3Connection,
            attachedDb.name ?? connection.name,
         );
      }
   }
}

function buildDuckDBEntry(
   connection: NamedConnection,
   environmentPath: string,
): CoreConnectionEntry {
   const attachedDatabases =
      connection.duckdbConnection?.attachedDatabases ?? [];
   if (
      attachedDatabases.some((database) => database.name === connection.name)
   ) {
      throw new Error(
         `DuckDB attached database names cannot conflict with connection name ${connection.name}`,
      );
   }
   const entry = buildDuckdbEntry(
      connection.name,
      environmentPath,
      `${connection.name}.duckdb`,
      connection.duckdbConnection?.setupSQL,
   );
   // duckdb_lockdown.ts. With no attached database the connection needs no
   // network: confined to the environment directory, then locked, by Malloy,
   // around its setupSQL. With one, Publisher attaches, then locks; the key
   // digests what it attaches, so a locked instance is never reused for
   // another setup.
   return attachedDatabases.length === 0
      ? {
           ...entry,
           ...localLockedDuckDBOptions(environmentPath),
        }
      : {
           ...entry,
           ...networkedDuckDBOptions(
              "attached",
              // Not setupSQL: each connection runs its own, on a locked
              // instance too (duckdb_instance_sharing.ts).
              setupIdentity({ attachedDatabases }),
           ),
        };
}
