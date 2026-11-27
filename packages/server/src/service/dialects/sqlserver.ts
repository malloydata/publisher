// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `sqlserver` connection type's registry entry. */

import { logger } from "../../logger";
import type { Connection } from "@malloydata/malloy";
import { standardizeRunSQLResult } from "../gcs_s3_utils";
import { runIntrospectionSQL, sqlLiteral } from "../introspection_sql";
import {
   groupColumnRowsIntoTables,
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

export const SQLSERVER: ConnectionDialect = Object.freeze({
   type: "sqlserver",
   // The connector is read-only: it creates no tables and has no
   // PersistSQLResults, so a database `#@ persist` on a SQL Server source is
   // refused at load (READ_ONLY_DIALECTS in materialization_eligibility.ts)
   // rather than failing mid-build.
   attributes: Object.freeze({
      dialectName: "sqlserver",
      isPool: true,
      canPersist: false,
      canStream: true,
   }),
   validateShape: validateSqlServerShape,
   coreEntry: buildSqlServerEntry,
   introspector: Object.freeze({
      schemas: getSchemasForSQLServer,
      tables: listTablesForSQLServer,
   }),
});

function validateSqlServerShape(connection: ApiConnection): void {
   const sqlserver = connection.sqlserverConnection;
   if (!sqlserver) {
      throw new Error("SQL Server connection configuration is missing.");
   }
   // The connector reads a non-numeric port as no port and dials 1433, so a
   // typo would surface as a failure naming a port nobody configured.
   const port: unknown = sqlserver.port;
   if (
      port !== undefined &&
      port !== null &&
      (typeof port !== "number" ||
         !Number.isInteger(port) ||
         port < 1 ||
         port > 65535)
   ) {
      throw new Error(
         `Connection '${connection.name}' has an invalid sqlserverConnection.port ` +
            `${JSON.stringify(port)} (expected an integer from 1 to 65535).`,
      );
   }
   // The connector checks these itself, but only when it first connects; a
   // missing one is caught here so a save fails rather than the first query.
   if (sqlserver.authentication === "azure-service-principal") {
      const missing = (
         ["tenantId", "clientId", "clientSecret"] as const
      ).filter((field) => !sqlserver[field]);
      if (missing.length > 0) {
         throw new Error(
            `Connection '${connection.name}' uses azure-service-principal ` +
               `authentication but is missing ${missing.join(", ")}.`,
         );
      }
   }
   // The driver takes the instance name over the port when given both.
   if (sqlserver.instanceName && sqlserver.port) {
      logger.warn(
         `Connection '${connection.name}' sets both sqlserverConnection.instanceName ` +
            `and port; the instance is located through SQL Browser and the port is not used.`,
      );
   }
}

/** An optional string field, with the empty string a form sends for "not set" read as unset. */
function blankAsUnset(value: string | undefined): string | undefined {
   return value === "" ? undefined : value;
}

function buildSqlServerEntry(connection: NamedConnection): CoreConnectionEntry {
   // A pass-through: the API field names are the connector's own. The two
   // optional names are read as unset when blank: the connector drops the
   // port whenever instanceName is DEFINED, so "" would send the driver to SQL
   // Browser for an instance called nothing, and a "" certificate name would
   // be checked against the certificate.
   const sqlserver = connection.sqlserverConnection;
   return {
      is: "sqlserver",
      server: sqlserver?.server,
      port: sqlserver?.port,
      database: sqlserver?.database,
      user: sqlserver?.user,
      password: sqlserver?.password,
      authentication: sqlserver?.authentication,
      tenantId: blankAsUnset(sqlserver?.tenantId),
      clientId: blankAsUnset(sqlserver?.clientId),
      clientSecret: blankAsUnset(sqlserver?.clientSecret),
      encrypt: sqlserver?.encrypt,
      trustServerCertificate: sqlserver?.trustServerCertificate,
      hostNameInCertificate: blankAsUnset(sqlserver?.hostNameInCertificate),
      instanceName: blankAsUnset(sqlserver?.instanceName),
   };
}

/**
 * The SQL Server schemas owning at least one table or view, as a modeler
 * addresses one (`connection.table('schema.table')`). Read from
 * `information_schema.tables`: `schemata` also lists fixed-role schemas
 * (`db_owner`, ...) that never hold a table, and `sys.tables` would miss
 * view-only schemas, which the column listing includes.
 */
async function getSchemasForSQLServer(
   connection: ApiConnection,
   malloyConnection: Connection,
): Promise<ApiSchema[]> {
   if (!connection.sqlserverConnection) {
      throw new Error("SQL Server connection is required");
   }
   return withIntrospectionFailure(
      `schemas for SQL Server connection ${connection.name}`,
      async () => {
         const result = await runIntrospectionSQL(
            malloyConnection,
            "SELECT DISTINCT table_schema AS schema_name FROM information_schema.tables ORDER BY table_schema",
         );
         const rows = standardizeRunSQLResult(result);
         return rows.map((row: unknown) => {
            const typedRow = row as Record<string, unknown>;
            const schemaName = String(
               typedRow.schema_name ?? typedRow.SCHEMA_NAME ?? "",
            );
            return {
               name: schemaName,
               isHidden: false,
               isDefault: schemaName === "dbo",
            };
         });
      },
   );
}

/**
 * A SQL Server Unicode string literal, `N'…'`. Here rather than in
 * {@link sqlLiteral}, which serves dialects with no `N'…'` syntax.
 */
function sqlserverNLiteral(value: string): string {
   return `N'${sqlLiteral(value, "sqlserver")}'`;
}

/** {@link sqlInFilter} for SQL Server, with each value a Unicode literal. */
function sqlserverInFilter(columnName: string, values?: string[]): string {
   if (!values || values.length === 0) return "";
   return `AND ${columnName} IN (${values.map(sqlserverNLiteral).join(", ")})`;
}

async function listTablesForSQLServer(
   connection: ApiConnection,
   schemaName: string,
   malloyConnection: Connection,
   tableNames?: string[],
): Promise<ApiTable[]> {
   if (!connection.sqlserverConnection) {
      throw new Error("SQL Server connection is required");
   }
   return withIntrospectionFailure(
      `tables for SQL Server schema ${schemaName} in connection ${connection.name}`,
      async () => {
         // Rows come back as plain objects, so no row_to_json as for Postgres.
         // Every literal is N'…': a plain '…' is converted to the database code
         // page first, so under a Latin1 collation `Продажи` becomes question
         // marks and matches nothing against the nvarchar catalog columns.
         const result = await runIntrospectionSQL(
            malloyConnection,
            `SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = ${sqlserverNLiteral(schemaName)} ${sqlserverInFilter("table_name", tableNames)} ORDER BY table_name, ordinal_position`,
         );
         const rows = standardizeRunSQLResult(result);
         return groupColumnRowsIntoTables(rows, (t) => `${schemaName}.${t}`);
      },
   );
}
