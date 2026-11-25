// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `motherduck` connection type's registry entry: a DuckDB-dialect connection to MotherDuck. */

import { networkedDuckDBOptions } from "../duckdb_lockdown";
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
   ConnectionDialect,
   CoreConnectionEntry,
   NamedConnection,
} from "./types";

export const MOTHERDUCK: ConnectionDialect = Object.freeze({
   type: "motherduck",
   attributes: Object.freeze({
      dialectName: "duckdb",
      isPool: false,
      canPersist: true,
      canStream: true,
   }),
   validateShape: validateMotherDuckShape,
   coreEntry: buildMotherDuckEntry,
   lockedDown: true,
   introspector: Object.freeze({
      schemas: getSchemasForMotherDuck,
      tables: listTablesForMotherDuck,
   }),
});

function validateMotherDuckShape(connection: ApiConnection): void {
   if (!connection.motherduckConnection) {
      throw new Error("MotherDuck connection configuration is missing.");
   }
   if (!connection.motherduckConnection.accessToken) {
      throw new Error("MotherDuck access token is required.");
   }
}

function buildMotherDuckEntry(
   connection: NamedConnection,
): CoreConnectionEntry {
   if (!connection.motherduckConnection?.accessToken) {
      throw new Error("MotherDuck access token is required.");
   }

   return {
      is: "duckdb",
      databasePath: connection.motherduckConnection.database
         ? `md:${connection.motherduckConnection.database}?attach_mode=single`
         : "md:",
      motherDuckToken: connection.motherduckConnection.accessToken,
      // Locked after open like any networked reader (duckdb_lockdown.ts).
      ...networkedDuckDBOptions("motherduck", ""),
   };
}

async function getSchemasForMotherDuck(
   connection: ApiConnection,
   malloyConnection: Connection,
): Promise<ApiSchema[]> {
   const motherduck = connection.motherduckConnection;
   if (!motherduck) {
      throw new Error("MotherDuck connection is required");
   }
   return withIntrospectionFailure(
      `schemas for MotherDuck connection ${connection.name}`,
      async () => {
         const database = motherduck.database;
         const whereClause = database
            ? `WHERE catalog_name = '${sqlLiteral(database, connection.type)}'`
            : "";
         const result = await runIntrospectionSQL(
            malloyConnection,
            `SELECT DISTINCT schema_name FROM information_schema.schemata ${whereClause} ORDER BY schema_name`,
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
                  "performance_schema",
                  "",
               ].includes(schemaName),
               isDefault: schemaName === "main",
            };
         });
      },
   );
}

async function listTablesForMotherDuck(
   connection: ApiConnection,
   schemaName: string,
   malloyConnection: Connection,
   tableNames?: string[],
): Promise<ApiTable[]> {
   if (!connection.motherduckConnection) {
      throw new Error("MotherDuck connection is required");
   }
   return withIntrospectionFailure(
      `tables for MotherDuck schema ${schemaName} in connection ${connection.name}`,
      async () => {
         const result = await runIntrospectionSQL(
            malloyConnection,
            `SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = '${sqlLiteral(schemaName, connection.type)}' ${sqlInFilter("table_name", tableNames, connection.type)} ORDER BY table_name, ordinal_position`,
         );
         const rows = standardizeRunSQLResult(result);
         return groupColumnRowsIntoTables(rows, (t) => `${schemaName}.${t}`);
      },
   );
}
