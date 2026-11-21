// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `databricks` connection type's registry entry. */

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

export const DATABRICKS: ConnectionDialect = Object.freeze({
   type: "databricks",
   attributes: Object.freeze({
      dialectName: "databricks",
      isPool: false,
      canPersist: true,
      canStream: false,
   }),
   validateShape: validateDatabricksShape,
   coreEntry: buildDatabricksEntry,
   introspector: Object.freeze({
      schemas: getSchemasForDatabricks,
      tables: listTablesForDatabricks,
   }),
});

function validateDatabricksShape(connection: ApiConnection): void {
   const databricks = connection.databricksConnection;
   if (!databricks) {
      throw new Error("Databricks connection configuration is missing.");
   }
   if (!databricks.host) {
      throw new Error("Databricks host is required.");
   }
   if (!databricks.path) {
      throw new Error("Databricks SQL warehouse HTTP path is required.");
   }
   const hasToken = !!databricks.token;
   const hasOAuth =
      !!databricks.oauthClientId && !!databricks.oauthClientSecret;
   if (!hasToken && !hasOAuth) {
      throw new Error(
         "Databricks requires either a personal access token or OAuth M2M client ID and secret.",
      );
   }
   const hasDefaultCatalog = !!databricks.defaultCatalog;
   if (!hasDefaultCatalog) {
      throw new Error("Databricks default catalog is required.");
   }
}

function buildDatabricksEntry(
   connection: NamedConnection,
): CoreConnectionEntry {
   const databricks = connection.databricksConnection;
   return {
      is: "databricks",
      host: databricks?.host,
      path: databricks?.path,
      token: databricks?.token,
      oauthClientId: databricks?.oauthClientId,
      oauthClientSecret: databricks?.oauthClientSecret,
      defaultCatalog: databricks?.defaultCatalog,
      defaultSchema: databricks?.defaultSchema,
      setupSQL: databricks?.setupSQL,
   };
}

async function getSchemasForDatabricks(
   connection: ApiConnection,
   malloyConnection: Connection,
): Promise<ApiSchema[]> {
   const databricks = connection.databricksConnection;
   if (!databricks) {
      throw new Error("Databricks connection is required");
   }
   return withIntrospectionFailure(
      `schemas for Databricks connection ${connection.name}`,
      async () => {
         const configuredSchema = databricks.defaultSchema;
         let allRows: { catalog: string; schema: string }[] = [];

         if (databricks.defaultCatalog) {
            const catalog = databricks.defaultCatalog;
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
                  return String(r.catalog ?? r.Catalog ?? r.catalog_name ?? "");
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
                     `Failed to list schemas for Databricks catalog ${catalog}`,
                     { error: catalogError },
                  );
               }
            }
         }
         logger.info("allRows for Schemas for Databricks", { allRows });
         return allRows.map(({ catalog, schema }) => {
            const name = databricks?.defaultCatalog
               ? schema
               : `${catalog}.${schema}`;
            return {
               name,
               isHidden: ["information_schema"].includes(schema),
               isDefault: configuredSchema
                  ? schema === configuredSchema
                  : false,
            };
         });
      },
      "before-log",
   );
}

async function listTablesForDatabricks(
   connection: ApiConnection,
   schemaName: string,
   malloyConnection: Connection,
   tableNames?: string[],
): Promise<ApiTable[]> {
   const databricks = connection.databricksConnection;
   if (!databricks) {
      throw new Error("Databricks connection is required");
   }
   return withIntrospectionFailure(
      `tables for Databricks schema ${schemaName} in connection ${connection.name}`,
      async () => {
         let catalogPrefix: string;
         let schemaOnly: string;
         let resourcePrefix: string;

         if (databricks.defaultCatalog) {
            catalogPrefix = `${databricks.defaultCatalog}.`;
            schemaOnly = schemaName;
            resourcePrefix = `${databricks.defaultCatalog}.${schemaName}`;
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

         // A STRUCT or ARRAY column reports its full_data_type
         // (`struct<city:string>`, `array<struct<sku:string>>`): data_type
         // says only STRUCT or ARRAY, which names no members, so a listing
         // could neither find an array of structs nor tell a changed struct
         // from an unchanged one (withRecordMembers). Every other column
         // keeps its data_type.
         const result = await runIntrospectionSQL(
            malloyConnection,
            `SELECT table_name, column_name, CASE WHEN upper(data_type) IN ('STRUCT', 'ARRAY') THEN coalesce(full_data_type, data_type) ELSE data_type END AS data_type FROM ${catalogPrefix}information_schema.columns WHERE table_schema = '${sqlLiteral(schemaOnly, connection.type)}' ${sqlInFilter("table_name", tableNames, connection.type)} ORDER BY table_name, ordinal_position`,
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
