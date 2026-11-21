// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `postgres` connection type's registry entry (Redshift connects as it). */

import {
   buildPgConnectionString,
   buildPostgresConnectionString,
   buildProxiedPgAttachString,
   createPgAttachSecret,
} from "../postgres_conninfo";
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { logger } from "../../logger";
import { redactPgSecrets } from "../../pg_helpers";
import { escapeSQL, installAndLoadExtension } from "../duckdb_session_setup";
import { openProxy, type ProxyEndpoint } from "../proxy";
import { quoteIdentifier } from "../quoting";
import type { Connection } from "@malloydata/malloy";
import { standardizeRunSQLResult } from "../gcs_s3_utils";
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
   AttachedDatabase,
   ConnectionDialect,
   CoreConnectionEntry,
   FederatedHandle,
   FederationConfig,
   FederationDeps,
   NamedConnection,
} from "./types";

export const POSTGRES: ConnectionDialect = Object.freeze({
   type: "postgres",
   attributes: Object.freeze({
      dialectName: "postgres",
      isPool: true,
      canPersist: true,
      canStream: true,
   }),
   coreEntry: buildPostgresEntry,
   introspector: Object.freeze({
      schemas: getSchemasForPostgres,
      tables: listTablesForPostgres,
   }),
   attached: Object.freeze({
      attach: attachPostgres,
      test: testAttachedPostgres,
   }),
   passthrough: Object.freeze({
      federate: federatePostgres,
      wrap: wrapPostgres,
   }),
});

function buildPostgresEntry(connection: NamedConnection): CoreConnectionEntry {
   const postgresConnection = connection.postgresConnection;
   return {
      is: "postgres",
      host: postgresConnection?.host,
      port: postgresConnection?.port,
      username: postgresConnection?.userName,
      password: postgresConnection?.password,
      databaseName: postgresConnection?.databaseName,
      connectionString: postgresConnection
         ? buildPostgresConnectionString(postgresConnection, {
              applySslmode: !connection.proxy,
           })
         : undefined,
   };
}

async function attachPostgres(
   connection: DuckDBConnection,
   attachedDb: AttachedDatabase,
): Promise<void> {
   if (!attachedDb.postgresConnection) {
      throw new Error(
         `PostgreSQL connection configuration missing for: ${attachedDb.name}`,
      );
   }
   // Before any SQL is built: an empty alias emits `AS ""`, which DuckDB
   // rejects at a position after the DSN literal, and that error truncates the
   // redactor's anchor away.
   if (!attachedDb.name) {
      throw new Error("Attached database name is required");
   }

   await installAndLoadExtension(connection, "postgres");

   const config = attachedDb.postgresConnection;
   const attachString: string = buildPgConnectionString(config, {
      name: attachedDb.name,
      applyConnectionOptions: true,
   });

   const { path: attachPath, secret } = await createPgAttachSecret(
      connection,
      attachedDb.name,
      attachString,
   );
   const attachCommand = `ATTACH '${escapeSQL(attachPath)}' AS ${quoteIdentifier(attachedDb.name, "duckdb")} (TYPE postgres, READ_ONLY${secret ? `, SECRET ${secret}` : ""});`;
   await connection.runSQL(attachCommand);
   logger.info(`Successfully attached PostgreSQL database: ${attachedDb.name}`);
}

/** The probe a connection test runs on an attached Postgres database. */
async function testAttachedPostgres(
   duckdbConnection: DuckDBConnection,
   name: string,
): Promise<void> {
   await duckdbConnection.runSQL(
      `SELECT schema_name FROM information_schema.schemata WHERE catalog_name = '${escapeSQL(name)}' LIMIT 1`,
   );
   logger.info(`Attached Postgres database test passed: ${name}`);
}

async function federatePostgres(
   connection: DuckDBConnection,
   config: FederationConfig,
   deps: FederationDeps = {},
): Promise<FederatedHandle> {
   const pg = config.postgresConnection;
   if (!pg) {
      throw new Error(
         `PostgreSQL connection configuration missing for: ${config.name}`,
      );
   }

   await installAndLoadExtension(connection, "postgres");

   // The raw name is the postgres_query handle; the ATTACH identifier is quoted
   // (a hyphenated name would otherwise be a parser error).
   const alias = config.name;

   // A proxied connection is reached only through the SSH tunnel, as on the
   // query path (buildProxiedPostgresConnection). The tunnel is build-scoped,
   // closed by the handle's `close`. It is opened inside the try so any throw
   // before the handle is returned closes it here; otherwise the listener is
   // stranded for the life of the process.
   let endpoint: ProxyEndpoint | undefined;
   try {
      let attachString: string;
      if (config.proxy) {
         if (!pg.host || !pg.port) {
            // validateConnectionShape requires both; guard so the tunnel target
            // is never undefined.
            throw new Error(
               `Connection proxy on '${config.name}' requires explicit host and port on the postgres connection.`,
            );
         }
         endpoint = await (deps.openProxy ?? openProxy)(config.proxy, {
            host: pg.host,
            port: pg.port,
         });
         attachString = buildProxiedPgAttachString(config.name, pg, endpoint);
      } else {
         attachString = buildPgConnectionString(pg, {
            name: config.name,
            applyConnectionOptions: true,
         });
      }
      logger.info(
         `Federating Postgres source for passthrough as alias '${alias}'${endpoint ? " through its SSH proxy" : ""}: ${redactPgSecrets(attachString)}`,
      );
      // OR REPLACE only for idempotency within a session. Isolation between
      // builds and tenants comes from each build's private DuckDB instance
      // (createIsolatedBuildSession), which also scopes the bigquery/snowflake
      // secrets.
      const { path: attachPath, secret } = await createPgAttachSecret(
         connection,
         alias,
         attachString,
      );
      await connection.runSQL(
         `ATTACH OR REPLACE '${escapeSQL(attachPath)}' AS ${quoteIdentifier(alias, "duckdb")} (TYPE postgres, READ_ONLY${secret ? `, SECRET ${secret}` : ""});`,
      );
   } catch (e) {
      // The federation error propagates; a tunnel that fails to close outlives
      // the build, so it is logged rather than lost.
      await endpoint
         ?.close()
         .catch((closeErr) =>
            logger.warn(
               `Failed to close the SSH proxy opened for Postgres source '${config.name}' after its federation failed: ${String(closeErr)}`,
            ),
         );
      throw e;
   }
   return {
      handle: alias,
      sourceType: "postgres",
      ...(endpoint ? { close: () => endpoint!.close() } : {}),
   };
}

/** A SELECT as a postgres_query call: the ATTACH alias, then the SQL. */
function wrapPostgres(handle: string, innerSQL: string): string {
   const sql = `'${escapeSQL(innerSQL)}'`;
   const h = `'${escapeSQL(handle)}'`;
   // postgres_query(<attached-db alias>, <sql>).
   return `SELECT * FROM postgres_query(${h}, ${sql})`;
}

async function getSchemasForPostgres(
   connection: ApiConnection,
   malloyConnection: Connection,
): Promise<ApiSchema[]> {
   if (!connection.postgresConnection) {
      throw new Error("Postgres connection is required");
   }
   return withIntrospectionFailure(
      `schemas for Postgres connection ${connection.name}`,
      async () => {
         // Wrap in row_to_json because the Malloy Postgres driver's runSQL
         // de-JSONs each row via row.row (matching Malloy-generated queries).
         const result = await runIntrospectionSQL(
            malloyConnection,
            "SELECT row_to_json(t) as row FROM (SELECT schema_name FROM information_schema.schemata ORDER BY schema_name) t",
         );
         const rows = standardizeRunSQLResult(result);
         return rows.map((row: unknown) => {
            const typedRow = row as Record<string, unknown>;
            const schemaName = String(
               typedRow.schema_name ?? typedRow.SCHEMA_NAME ?? "",
            );
            return {
               name: schemaName,
               isHidden: [
                  "information_schema",
                  "pg_catalog",
                  "pg_toast",
               ].includes(schemaName),
               isDefault: schemaName === "public",
            };
         });
      },
   );
}

async function listTablesForPostgres(
   connection: ApiConnection,
   schemaName: string,
   malloyConnection: Connection,
   tableNames?: string[],
): Promise<ApiTable[]> {
   if (!connection.postgresConnection) {
      throw new Error("Postgres connection is required");
   }
   return withIntrospectionFailure(
      `tables for Postgres schema ${schemaName} in connection ${connection.name}`,
      async () => {
         // Wrap in row_to_json because the Malloy Postgres driver's runSQL
         // de-JSONs each row via row.row (matching Malloy-generated queries).
         const result = await runIntrospectionSQL(
            malloyConnection,
            `SELECT row_to_json(t) as row FROM (SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = '${sqlLiteral(schemaName, connection.type)}' ${sqlInFilter("table_name", tableNames, connection.type)} ORDER BY table_name, ordinal_position) t`,
         );
         const rows = standardizeRunSQLResult(result);
         return groupColumnRowsIntoTables(rows, (t) => `${schemaName}.${t}`);
      },
   );
}
