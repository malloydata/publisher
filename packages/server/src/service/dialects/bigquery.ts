// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `bigquery` connection type's registry entry: Malloy's dialect for it is `standardsql`. */

import { boolean, Mismatch } from "../../settings/types";
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { components } from "../../api";
import { logger } from "../../logger";
import { runInternalSQL } from "../duckdb_lockdown";
import {
   escapeSQL,
   installAndLoadExtension,
   sanitizeSecretName,
} from "../duckdb_session_setup";
import { quoteIdentifier } from "../quoting";
import { BigQuery } from "@google-cloud/bigquery";
import { Impersonated } from "google-auth-library";
import type { Connection, TableSourceDef } from "@malloydata/malloy";
import { externalEnv } from "../../settings";
import { getImpersonatedAuthClient } from "../gcp_impersonation";
import { assertGoogleCredentialsIsNotADirectory } from "../google_credentials";
import {
   columnsOfSchema,
   extractErrorDataFromError,
   type SchemaField,
   withIntrospectionFailure,
} from "../introspection_helpers";
import type {
   ApiConnection,
   ApiSchema,
   ApiTable,
   AttachedDatabase,
   ConnectionDialect,
   CoreConnectionEntry,
   FederatedHandle,
   FederationConfig,
   NamedConnection,
} from "./types";

export const BIGQUERY: ConnectionDialect = Object.freeze({
   type: "bigquery",
   attributes: Object.freeze({
      dialectName: "standardsql",
      isPool: false,
      canPersist: true,
      canStream: true,
   }),
   validateShape: validateBigQueryShape,
   coreEntry: buildBigQueryEntry,
   introspector: Object.freeze({
      schemas: getSchemasForBigQuery,
      tables: listTablesForBigQuery,
   }),
   attached: Object.freeze({
      attach: attachBigQuery,
      test: testAttachedBigQuery,
      secretName: bigQueryDetachSecret,
   }),
   passthrough: Object.freeze({
      federate: federateBigQuery,
      wrap: wrapBigQuery,
   }),
});

// Field names follow Google's service-account key file format, so they are
// fixed by that format and do not track Publisher's own vocabulary.
type ServiceAccountKey = {
   type?: string;
   project_id?: string;
   private_key?: string;
   client_email?: string;
   [key: string]: unknown;
};

function parseServiceAccountKey(json?: string): ServiceAccountKey | undefined {
   if (!json) return undefined;
   const keyData = JSON.parse(json) as ServiceAccountKey;
   const requiredFields = ["type", "project_id", "private_key", "client_email"];
   for (const field of requiredFields) {
      if (!keyData[field]) {
         throw new Error(
            `Invalid service account key: missing "${field}" field`,
         );
      }
   }
   if (keyData.type !== "service_account") {
      throw new Error('Invalid service account key: incorrect "type" field');
   }
   return keyData;
}

function validateBigQueryShape(connection: ApiConnection): void {
   const bigquery = connection.bigqueryConnection;
   if (bigquery?.impersonateServiceAccount) {
      // An authClient replaces the credential entirely, so a key alongside it
      // would look live while the impersonated identity runs every query.
      // Core refuses this too; refusing here makes it a load-time error.
      if (bigquery.serviceAccountKeyJson) {
         throw new Error(
            `Connection '${connection.name}' sets impersonateServiceAccount ` +
               `and serviceAccountKeyJson. Impersonation replaces the ` +
               `credential entirely — the key would be ignored — so supply ` +
               `one or the other.`,
         );
      }
      // With an authClient the SDK resolves the project through the
      // impersonated credential, not ADC, so auto-detection is no fallback.
      if (!bigquery.billingProjectId) {
         throw new Error(
            `Connection '${connection.name}' sets impersonateServiceAccount ` +
               `but no billingProjectId. Impersonated connections resolve ` +
               `the project through the impersonated credential, so the ` +
               `project that runs (and is billed for) jobs must be named ` +
               `explicitly.`,
         );
      }
   }
}

function buildBigQueryEntry(connection: NamedConnection): CoreConnectionEntry {
   const serviceAccountKey = parseServiceAccountKey(
      connection.bigqueryConnection?.serviceAccountKeyJson as
         | string
         | undefined,
   );
   // Impersonation rides the config-overlay mechanism: the pojo carries a
   // plain-JSON reference, which the `gcpImpersonation` overlay registered by
   // buildEnvironmentMalloyConfig resolves to a live auth client. The property
   // is opaque, overlay-sourced and mustHaveValue, so a literal can never
   // satisfy it and an unresolved reference errors instead of falling back to
   // ambient ADC. The reference embeds the SA email, so core's connection
   // digest (and so the BuildID) is per identity.
   const impersonateServiceAccount =
      connection.bigqueryConnection?.impersonateServiceAccount;
   return {
      is: "bigquery",
      projectId:
         connection.bigqueryConnection?.defaultProjectId ||
         serviceAccountKey?.project_id,
      serviceAccountKey,
      // Spread rather than `authClient: x ?? undefined`: the property is
      // mustHaveValue and core checks whether it is SET, so an undefined
      // entry would fail every plain connection with "no value arrived".
      ...(impersonateServiceAccount
         ? {
              authClient: {
                 gcpImpersonation: impersonateServiceAccount,
              },
           }
         : {}),
      location: connection.bigqueryConnection?.location,
      maximumBytesBilled: connection.bigqueryConnection?.maximumBytesBilled,
      timeoutMs: connection.bigqueryConnection?.queryTimeoutMilliseconds,
      billingProjectId: connection.bigqueryConnection?.billingProjectId,
      dbmeta: configBoolean(
         connection.bigqueryConnection?.dbmeta,
         `dbmeta on BigQuery connection "${connection.name}"`,
      ),
   };
}

const BOOLEAN = boolean();

/**
 * A boolean connection setting as a boolean. A string, as `${VAR}`
 * substitution and API clients deliver it, is read the way a boolean setting
 * is, blank as unset; anything else is refused rather than dropped.
 */
export function configBoolean(
   value: unknown,
   name: string,
): boolean | undefined {
   if (value === undefined || value === null) return undefined;
   if (typeof value === "boolean") return value;
   if (typeof value === "string") {
      if (value.trim() === "") return undefined;
      try {
         return BOOLEAN.fromString(value);
      } catch (error) {
         if (!(error instanceof Mismatch)) throw error;
      }
   }
   throw new Error(
      `Invalid value for ${name}: expected ${BOOLEAN.expected}, got ${JSON.stringify(value)}`,
   );
}

interface BigQueryServiceAccount {
   projectId: string;
   serviceAccountJson: string;
   clientEmail: string;
}

/**
 * Validate a BigQuery connection's service account key and resolve its
 * project. The DuckDB BIGQUERY secret authenticates only with a key, so the
 * read-only attach and the build-time federation both require one.
 */
function parseBigQueryServiceAccount(
   name: string | undefined,
   config: components["schemas"]["BigqueryConnection"],
): BigQueryServiceAccount {
   const keyData = parseServiceAccountKey(config.serviceAccountKeyJson);
   if (keyData) {
      return {
         projectId: (keyData.project_id || config.defaultProjectId) as string,
         serviceAccountJson: config.serviceAccountKeyJson as string,
         clientEmail: keyData.client_email as string,
      };
   }
   throw new Error(
      `BigQuery project_id and service account key required for: ${name}`,
   );
}

/** Load the bigquery extension and create the secret scoped to the project. */
async function createBigQuerySecret(
   connection: DuckDBConnection,
   name: string | undefined,
   { projectId, serviceAccountJson }: BigQueryServiceAccount,
): Promise<string> {
   await installAndLoadExtension(connection, "bigquery", true);

   const secretName = sanitizeSecretName(`bigquery_${name}`);
   const escapedJson = escapeSQL(serviceAccountJson);
   await connection.runSQL(`
      CREATE OR REPLACE SECRET ${secretName} (
         TYPE BIGQUERY,
         SCOPE '${escapeSQL(`bq://${projectId}`)}',
         SERVICE_ACCOUNT_JSON '${escapedJson}'
      );
   `);
   return secretName;
}

/**
 * The ATTACH of a BigQuery project under `name`. The alias is quoted as a
 * DuckDB identifier and the project as a string literal, so neither can end
 * the statement, whatever validation the name passed.
 */
export function bigQueryAttachSQL(name: string, projectId: string): string {
   return `ATTACH '${escapeSQL(`project=${projectId}`)}' AS ${quoteIdentifier(name, "duckdb")} (TYPE bigquery, READ_ONLY);`;
}

async function attachBigQuery(
   connection: DuckDBConnection,
   attachedDb: AttachedDatabase,
): Promise<void> {
   if (!attachedDb.bigqueryConnection) {
      throw new Error(
         `BigQuery connection configuration missing for: ${attachedDb.name}`,
      );
   }

   const key = parseBigQueryServiceAccount(
      attachedDb.name,
      attachedDb.bigqueryConnection,
   );
   logger.info(`Using service account: ${key.clientEmail}`);
   const { projectId } = key;
   const secretName = await createBigQuerySecret(
      connection,
      attachedDb.name,
      key,
   );
   logger.info(
      `Created BigQuery secret: ${secretName} for project: ${projectId}`,
   );

   await connection.runSQL(bigQueryAttachSQL(attachedDb.name ?? "", projectId));
   logger.info(`Successfully attached BigQuery database: ${attachedDb.name}`);
}

/** The probe a connection test runs on an attached BigQuery database. */
async function testAttachedBigQuery(
   duckdbConnection: DuckDBConnection,
   name: string,
): Promise<void> {
   await runInternalSQL(
      duckdbConnection,
      `SELECT database_name FROM duckdb_databases() WHERE database_name = '${escapeSQL(name)}'`,
   );
   logger.info(`Attached BigQuery database test passed: ${name}`);
}

/** The secret a BigQuery attachment creates, as its detach drops it. */
function bigQueryDetachSecret(name: string): string {
   return sanitizeSecretName(`bigquery_${name}`);
}

async function federateBigQuery(
   connection: DuckDBConnection,
   config: FederationConfig,
): Promise<FederatedHandle> {
   const bq = config.bigqueryConnection;
   if (!bq) {
      throw new Error(
         `BigQuery connection configuration missing for: ${config.name}`,
      );
   }

   // A build federating from a BigQuery source connection may carry
   // impersonation legitimately (for the serve path). The DuckDB BIGQUERY secret
   // takes a key, not a token, so name that limitation rather than fall through
   // to the generic "service account key required".
   if (bq.impersonateServiceAccount) {
      throw new Error(
         `BigQuery connection '${config.name}' uses impersonateServiceAccount, ` +
            `which cannot federate through DuckDB: the DuckDB BIGQUERY secret ` +
            `authenticates with a service account key, not a token. Federated ` +
            `builds from this source require serviceAccountKeyJson.`,
      );
   }
   const key = parseBigQueryServiceAccount(config.name, bq);
   const { projectId } = key;
   await createBigQuerySecret(connection, config.name, key);
   logger.info(
      `Federated BigQuery source for passthrough: project ${projectId}`,
   );
   // bigquery_query finds the secret scoped to bq://<project>, so the project id
   // is the handle.
   return { handle: projectId, sourceType: "bigquery" };
}

/** A SELECT as a bigquery_query call: the project, then the SQL. */
function wrapBigQuery(handle: string, innerSQL: string): string {
   const sql = `'${escapeSQL(innerSQL)}'`;
   const h = `'${escapeSQL(handle)}'`;
   // bigquery_query(<project/db>, <sql>) — secret resolved by bq:// scope.
   return `SELECT * FROM bigquery_query(${h}, ${sql})`;
}

async function createBigQueryClient(
   connection: ApiConnection,
): Promise<BigQuery> {
   if (!connection.bigqueryConnection) {
      throw new Error("BigQuery connection is required");
   }

   const config: {
      projectId: string;
      credentials?: object;
      keyFilename?: string;
      authClient?: Impersonated;
   } = {
      projectId: connection.bigqueryConnection.defaultProjectId || "",
   };

   // Discovery must run as the same identity as query execution: falling
   // through to ambient ADC would read schemas as the publisher's own broad
   // credential, the identity impersonation exists to avoid.
   if (connection.bigqueryConnection.impersonateServiceAccount) {
      config.authClient = await getImpersonatedAuthClient(
         connection.bigqueryConnection.impersonateServiceAccount,
      );
      if (!config.projectId) {
         throw new Error(
            "BigQuery project ID is required. Set defaultProjectId on the connection when using impersonateServiceAccount.",
         );
      }
   } else if (connection.bigqueryConnection.serviceAccountKeyJson) {
      let credentials: Record<string, unknown>;
      try {
         credentials = JSON.parse(
            connection.bigqueryConnection.serviceAccountKeyJson,
         );
      } catch (parseError) {
         throw new Error(
            `Failed to parse BigQuery service account key JSON: ${(parseError as Error).message}`,
         );
      }
      config.credentials = credentials;

      if (!config.projectId && credentials.project_id) {
         config.projectId = credentials.project_id as string;
      }

      if (!config.projectId) {
         throw new Error(
            "BigQuery project ID is required. Either set the defaultProjectId in the connection configuration or the project_id in the service account key JSON.",
         );
      }
   } else if (
      Object.keys(connection.bigqueryConnection).length === 0 &&
      externalEnv("GOOGLE_APPLICATION_CREDENTIALS")
   ) {
      // Note: The BigQuery client will infer the project ID from the ADC file.
      assertGoogleCredentialsIsNotADirectory();
      config.keyFilename = externalEnv("GOOGLE_APPLICATION_CREDENTIALS") || "";
   } else {
      throw new Error(
         "BigQuery connection is required, either set the bigqueryConnection in the connection configuration or set the GOOGLE_APPLICATION_CREDENTIALS environment variable.",
      );
   }

   return new BigQuery(config);
}

async function getSchemasForBigQuery(
   connection: ApiConnection,
): Promise<ApiSchema[]> {
   if (!connection.bigqueryConnection) {
      throw new Error("BigQuery connection is required");
   }
   return withIntrospectionFailure(
      `schemas for BigQuery connection ${connection.name}`,
      async () => {
         const bigquery = await createBigQueryClient(connection);
         const [datasets] = await bigquery.getDatasets();

         return await Promise.all(
            datasets.map(async (dataset) => {
               const [metadata] = await dataset.getMetadata();
               return {
                  name: dataset.id,
                  isHidden: false,
                  isDefault: false,
                  description: (metadata as { description?: string })
                     ?.description,
               };
            }),
         );
      },
   );
}

/**
 * BigQuery: list tables via API client, then fetch each table's schema
 * individually since BigQuery's INFORMATION_SCHEMA is region-scoped.
 */
async function listTablesForBigQuery(
   connection: ApiConnection,
   schemaName: string,
   malloyConnection: Connection,
   tableNames?: string[],
): Promise<ApiTable[]> {
   return withIntrospectionFailure(
      `tables for BigQuery schema ${schemaName} in connection ${connection.name}`,
      async () => {
         const bigquery = await createBigQueryClient(connection);
         // bigquery.dataset() takes a BARE dataset id, so a project-qualified
         // schema ("project.dataset") must be split, or it resolves to a missing
         // dataset in the default project and the orphan sweep cannot see its
         // tables. Split on the LAST dot so "domain.com:project" ids survive.
         const lastDot = schemaName.lastIndexOf(".");
         const dataset =
            lastDot === -1
               ? bigquery.dataset(schemaName)
               : bigquery.dataset(schemaName.slice(lastDot + 1), {
                    projectId: schemaName.slice(0, lastDot),
                 });
         const [tables] = await dataset.getTables();

         let names = tables
            .map((table) => table.id)
            .filter((id): id is string => id !== undefined);
         if (tableNames) {
            const allowed = new Set(tableNames);
            names = names.filter((id) => allowed.has(id));
         }

         const results = await Promise.all(
            names.map(async (tableName) => {
               const tablePath = `${schemaName}.${tableName}`;
               try {
                  const source = await (
                     malloyConnection as Connection & {
                        fetchTableSchema: (
                           tableKey: string,
                           tablePath: string,
                        ) => Promise<TableSourceDef | undefined>;
                     }
                  ).fetchTableSchema(tableName, tablePath);
                  const columns = columnsOfSchema(
                     source?.fields as SchemaField[] | undefined,
                  );
                  return { resource: tablePath, columns };
               } catch (error) {
                  logger.warn(`Failed to get schema for table ${tableName}`, {
                     error: extractErrorDataFromError(error),
                     schemaName,
                     tableName,
                  });
                  return { resource: tablePath, columns: [] };
               }
            }),
         );
         return results;
      },
   );
}
