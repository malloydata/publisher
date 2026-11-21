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
import type { Connection } from "@malloydata/malloy";
import type { components } from "../../api";
import { BadRequestError, InvalidArgumentError } from "../../errors";
import { logger } from "../../logger";
import {
   azureAttachmentCovers,
   describeAzureFile,
   withAttachmentSasToken,
} from "../azure_introspection";
import {
   CloudStorageCredentials,
   gcsConnectionToCredentials,
   getCloudTablesWithColumns,
   listCloudDirectorySchemas,
   listDataFilesInDirectory,
   parseCloudUri,
   s3ConnectionToCredentials,
   standardizeRunSQLResult,
} from "../gcs_s3_utils";
import { runIntrospectionSQL, sqlLiteral } from "../introspection_sql";
import {
   groupColumnRowsIntoTables,
   sqlInFilter,
   withIntrospectionFailure,
} from "../introspection_helpers";
import type {
   ApiConnection,
   ApiSchema,
   ApiTable,
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
   introspector: Object.freeze({
      schemas: getSchemasForDuckDB,
      tables: listTablesForDuckDB,
   }),
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

function getCloudCredentialsFromAttachedDatabases(
   attachedDatabases: components["schemas"]["AttachedDatabase"][],
   storageType: "gcs" | "s3",
): CloudStorageCredentials | null {
   for (const attachedDb of attachedDatabases) {
      if (
         attachedDb.type === "gcs" &&
         storageType === "gcs" &&
         attachedDb.gcsConnection
      ) {
         return gcsConnectionToCredentials(attachedDb.gcsConnection);
      }
      if (
         attachedDb.type === "s3" &&
         storageType === "s3" &&
         attachedDb.s3Connection
      ) {
         return s3ConnectionToCredentials(attachedDb.s3Connection);
      }
   }
   return null;
}

async function getSchemasForDuckDB(
   connection: ApiConnection,
   malloyConnection: Connection,
): Promise<ApiSchema[]> {
   const duckdb = connection.duckdbConnection;
   if (!duckdb) {
      throw new Error("DuckDB connection is required");
   }
   return withIntrospectionFailure(
      `schemas for DuckDB connection ${connection.name}`,
      async () => {
         const result = await runIntrospectionSQL(
            malloyConnection,
            "SELECT DISTINCT schema_name,catalog_name FROM information_schema.schemata ORDER BY catalog_name,schema_name",
         );

         const rows = standardizeRunSQLResult(result);

         const schemas: ApiSchema[] = rows.map((row: unknown) => {
            const typedRow = row as Record<string, unknown>;
            const schemaName = String(typedRow.schema_name ?? "");
            const catalogName = String(typedRow.catalog_name ?? "");

            return {
               name: `${catalogName}.${schemaName}`,
               isHidden:
                  [
                     "information_schema",
                     "performance_schema",
                     "pg_catalog",
                     "pg_toast",
                     "",
                  ].includes(schemaName) ||
                  ["md_information_schema", "system"].includes(catalogName),
               isDefault: catalogName === "main",
            };
         });

         const attachedDatabases = duckdb.attachedDatabases || [];

         const cloudDatabases = attachedDatabases.filter(
            (attachedDb) =>
               (attachedDb.type === "gcs" || attachedDb.type === "s3") &&
               (attachedDb.gcsConnection || attachedDb.s3Connection),
         );

         const cloudDbPromises = cloudDatabases.map(async (attachedDb) => {
            const dbType = attachedDb.type as "gcs" | "s3";
            const credentials =
               dbType === "gcs"
                  ? gcsConnectionToCredentials(attachedDb.gcsConnection!)
                  : s3ConnectionToCredentials(attachedDb.s3Connection!);

            try {
               return await listCloudDirectorySchemas(credentials);
            } catch (cloudError) {
               logger.warn(
                  `Failed to list ${dbType.toUpperCase()} directory schemas for ${attachedDb.name}`,
                  { error: cloudError },
               );
               return [];
            }
         });

         const cloudSchemaArrays = await Promise.all(cloudDbPromises);
         for (const cloudSchemas of cloudSchemaArrays) {
            schemas.push(...cloudSchemas);
         }

         const azureDatabases = attachedDatabases.filter(
            (attachedDb) =>
               attachedDb.type === "azure" && attachedDb.azureConnection,
         );
         for (const attachedDb of azureDatabases) {
            if (attachedDb.name) {
               schemas.push({
                  name: attachedDb.name,
                  isHidden: false,
                  isDefault: false,
               });
            }
         }

         return schemas;
      },
   );
}

async function listTablesForDuckDB(
   connection: ApiConnection,
   schemaName: string,
   malloyConnection: Connection,
   tableNames?: string[],
): Promise<ApiTable[]> {
   if (!connection.duckdbConnection) {
      throw new Error("DuckDB connection is required");
   }

   const attachedDbs = connection.duckdbConnection.attachedDatabases || [];

   // Azure attached database matched by name
   const azureDb = attachedDbs.find(
      (db) =>
         db.type === "azure" && db.name === schemaName && db.azureConnection,
   );
   if (azureDb) {
      const azureConn = azureDb.azureConnection!;
      const fileUrl =
         azureConn.authType === "sas_token"
            ? azureConn.sasUrl
            : azureConn.fileUrl;
      if (fileUrl) {
         return describeAzureFile(malloyConnection, fileUrl, azureConn);
      }
   }

   // Azure ADLS file path (abfss://, https://, az://). Read only inside an
   // attachment's account and container, with that attachment's credentials:
   // otherwise this would fetch any URL a caller names.
   if (
      schemaName.startsWith("abfss://") ||
      schemaName.startsWith("https://") ||
      schemaName.startsWith("az://")
   ) {
      const covering = attachedDbs.find(
         (db) =>
            db.type === "azure" &&
            db.azureConnection &&
            azureAttachmentCovers(db.azureConnection, schemaName),
      )?.azureConnection;
      if (!covering) {
         throw new BadRequestError(
            `"${schemaName}" is not inside an Azure attachment of connection "${connection.name}". Browse an attached Azure database by its name.`,
         );
      }
      return describeAzureFile(
         malloyConnection,
         withAttachmentSasToken(schemaName, covering),
         covering,
      );
   }

   // Cloud storage (GCS/S3)
   const parsedUri = parseCloudUri(schemaName);
   if (parsedUri) {
      const {
         type: cloudType,
         bucket: bucketName,
         path: directoryPath,
      } = parsedUri;
      const credentials = getCloudCredentialsFromAttachedDatabases(
         attachedDbs,
         cloudType,
      );
      if (!credentials) {
         throw new Error(
            `${cloudType.toUpperCase()} credentials not found in attached databases`,
         );
      }
      const fileKeys = await listDataFilesInDirectory(
         credentials,
         bucketName,
         directoryPath,
      );
      return getCloudTablesWithColumns(
         malloyConnection,
         credentials,
         bucketName,
         fileKeys,
      );
   }

   // Regular DuckDB schema — query information_schema.columns
   const dotIdx = schemaName.indexOf(".");
   if (dotIdx < 0) {
      // InvalidArgumentError: a plain Error would classify as an internal fault
      // ("try again later"), which an agent retries forever.
      throw new InvalidArgumentError(
         `DuckDB schema name must be qualified as "catalog.schema", got "${schemaName}". List this connection's schemas and use one of those names verbatim.`,
      );
   }
   const catalogName = schemaName.substring(0, dotIdx);
   const actualSchemaName = schemaName.substring(dotIdx + 1);

   return withIntrospectionFailure(
      `tables for DuckDB schema ${schemaName} in connection ${connection.name}`,
      async () => {
         const result = await runIntrospectionSQL(
            malloyConnection,
            `SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = '${sqlLiteral(actualSchemaName, connection.type)}' AND table_catalog = '${sqlLiteral(catalogName, connection.type)}' ${sqlInFilter("table_name", tableNames, connection.type)} ORDER BY table_name, ordinal_position`,
         );
         const rows = standardizeRunSQLResult(result);
         return groupColumnRowsIntoTables(rows, (t) => `${schemaName}.${t}`);
      },
   );
}
