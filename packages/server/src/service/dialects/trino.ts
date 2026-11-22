// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `trino` connection type's registry entry. */

import type { components } from "../../api";
import type { Connection } from "@malloydata/malloy";
import { logger } from "../../logger";
import { standardizeRunSQLResult } from "../gcs_s3_utils";
import { runIntrospectionSQL, sqlLiteral } from "../introspection_sql";
import {
   assertSafeSqlIdentifier,
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

export const TRINO: ConnectionDialect = Object.freeze({
   type: "trino",
   attributes: Object.freeze({
      dialectName: "trino",
      isPool: false,
      canPersist: true,
      canStream: false,
   }),
   validateShape: validateTrinoShape,
   coreEntry: buildTrinoEntry,
   introspector: Object.freeze({
      schemas: getSchemasForTrino,
      tables: listTablesForTrino,
   }),
});

function validateTrinoShape(connection: ApiConnection): void {
   if (!connection.trinoConnection) {
      throw new Error("Trino connection configuration is missing.");
   }
}

function buildTrinoEntry(connection: NamedConnection): CoreConnectionEntry {
   return {
      is: "trino",
      ...validateAndBuildTrinoCoreConfig(connection.trinoConnection),
   };
}

function validateAndBuildTrinoCoreConfig(
   trinoConfig: components["schemas"]["TrinoConnection"] | undefined,
): Record<string, unknown> {
   if (!trinoConfig) {
      return {};
   }

   const server =
      trinoConfig.server && trinoConfig.port
         ? trinoConfig.server.includes(trinoConfig.port.toString())
            ? trinoConfig.server
            : `${trinoConfig.server}:${trinoConfig.port}`
         : trinoConfig.server;

   const baseConfig: Record<string, unknown> = {
      server,
      port: trinoConfig.port,
      catalog: trinoConfig.catalog,
      schema: trinoConfig.schema,
      user: trinoConfig.user,
   };

   if (trinoConfig.peakaKey) {
      baseConfig.extraCredential = {
         peakaKey: trinoConfig.peakaKey,
      };
      return baseConfig;
   }

   if (server?.startsWith("https://") && trinoConfig.password) {
      baseConfig.password = trinoConfig.password;
   }

   if (server?.startsWith("http://") || server?.startsWith("https://")) {
      return baseConfig;
   }

   throw new Error(
      `Invalid Trino connection: expected "http://server:port" or "https://server:port".`,
   );
}

async function getSchemasForTrino(
   connection: ApiConnection,
   malloyConnection: Connection,
): Promise<ApiSchema[]> {
   const trino = connection.trinoConnection;
   if (!trino) {
      throw new Error("Trino connection is required");
   }
   return withIntrospectionFailure(
      `schemas for Trino connection ${connection.name}`,
      async () => {
         const configuredSchema = trino.schema;
         let allRows: { catalog: string; schema: string }[] = [];

         if (trino.catalog) {
            const catalog = trino.catalog;
            const result = await runIntrospectionSQL(
               malloyConnection,
               `SELECT schema_name FROM ${assertSafeSqlIdentifier(
                  catalog,
                  "catalog name",
               )}.information_schema.schemata ORDER BY schema_name`,
            );
            const rows = standardizeRunSQLResult(result);
            allRows = rows.map((row: unknown) => {
               const r = row as Record<string, unknown>;
               return {
                  catalog,
                  schema: String(r.schema_name ?? r.Schema ?? ""),
               };
            });
         } else {
            const catalogsResult = await runIntrospectionSQL(
               malloyConnection,
               `SHOW CATALOGS`,
            );
            const catalogNames = standardizeRunSQLResult(catalogsResult).map(
               (row: unknown) => {
                  const r = row as Record<string, unknown>;
                  return String(r.Catalog ?? r.catalog ?? "");
               },
            );

            for (const catalog of catalogNames) {
               try {
                  const result = await runIntrospectionSQL(
                     malloyConnection,
                     `SELECT schema_name FROM ${assertSafeSqlIdentifier(
                        catalog,
                        "catalog name",
                     )}.information_schema.schemata ORDER BY schema_name`,
                  );
                  const rows = standardizeRunSQLResult(result);
                  for (const row of rows) {
                     const r = row as Record<string, unknown>;
                     allRows.push({
                        catalog,
                        schema: String(r.schema_name ?? r.Schema ?? ""),
                     });
                  }
               } catch (catalogError) {
                  logger.warn(
                     `Failed to list schemas for Trino catalog ${catalog}`,
                     { error: catalogError },
                  );
               }
            }
         }

         return allRows.map(({ catalog, schema }) => {
            const name = trino?.catalog ? schema : `${catalog}.${schema}`;
            return {
               name,
               isHidden: ["information_schema", "performance_schema"].includes(
                  schema,
               ),
               isDefault: configuredSchema
                  ? schema === configuredSchema
                  : false,
            };
         });
      },
      "before-log",
   );
}

async function listTablesForTrino(
   connection: ApiConnection,
   schemaName: string,
   malloyConnection: Connection,
   tableNames?: string[],
): Promise<ApiTable[]> {
   const trino = connection.trinoConnection;
   if (!trino) {
      throw new Error("Trino connection is required");
   }
   return withIntrospectionFailure(
      `tables for Trino schema ${schemaName} in connection ${connection.name}`,
      async () => {
         let catalogPrefix: string;
         let schemaOnly: string;
         let resourcePrefix: string;

         if (trino.catalog) {
            catalogPrefix = `${trino.catalog}.`;
            schemaOnly = schemaName;
            resourcePrefix = `${trino.catalog}.${schemaName}`;
         } else {
            const dotIdx = schemaName.indexOf(".");
            if (dotIdx > 0) {
               catalogPrefix = `${assertSafeSqlIdentifier(
                  schemaName.substring(0, dotIdx),
                  "catalog name",
               )}.`;
               schemaOnly = schemaName.substring(dotIdx + 1);
            } else {
               catalogPrefix = "";
               schemaOnly = schemaName;
            }
            resourcePrefix = schemaName;
         }

         const result = await runIntrospectionSQL(
            malloyConnection,
            `SELECT table_name, column_name, data_type FROM ${catalogPrefix}information_schema.columns WHERE table_schema = '${sqlLiteral(schemaOnly, connection.type)}' ${sqlInFilter("table_name", tableNames, connection.type)} ORDER BY table_name, ordinal_position`,
         );
         const rows = standardizeRunSQLResult(result);
         return groupColumnRowsIntoTables(
            rows,
            (t) => `${resourcePrefix}.${t}`,
         );
      },
      "after-log",
   );
}
