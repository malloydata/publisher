// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `postgres` connection type's registry entry (Redshift connects as it). */

import {
   buildPgConnectionString,
   buildPostgresConnectionString,
   createPgAttachSecret,
} from "../postgres_conninfo";
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { logger } from "../../logger";
import { escapeSQL, installAndLoadExtension } from "../duckdb_session_setup";
import { quoteIdentifier } from "../quoting";
import type {
   AttachedDatabase,
   ConnectionDialect,
   CoreConnectionEntry,
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
   attached: Object.freeze({
      attach: attachPostgres,
      test: testAttachedPostgres,
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
