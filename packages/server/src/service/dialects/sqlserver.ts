// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `sqlserver` connection type's registry entry. */

import { logger } from "../../logger";
import type {
   ApiConnection,
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
      encrypt: sqlserver?.encrypt,
      trustServerCertificate: sqlserver?.trustServerCertificate,
      hostNameInCertificate: blankAsUnset(sqlserver?.hostNameInCertificate),
      instanceName: blankAsUnset(sqlserver?.instanceName),
   };
}
