// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `mysql` connection type's registry entry. */

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

export const MYSQL: ConnectionDialect = Object.freeze({
   type: "mysql",
   attributes: Object.freeze({
      dialectName: "mysql",
      isPool: false,
      canPersist: true,
      canStream: false,
   }),
   coreEntry: buildMySQLEntry,
   introspector: Object.freeze({
      schemas: getSchemasForMySQL,
      tables: listTablesForMySQL,
   }),
});

function buildMySQLEntry(connection: NamedConnection): CoreConnectionEntry {
   return {
      is: "mysql",
      host: connection.mysqlConnection?.host,
      port: connection.mysqlConnection?.port,
      user: connection.mysqlConnection?.user,
      password: connection.mysqlConnection?.password,
      database: connection.mysqlConnection?.database,
   };
}

async function getSchemasForMySQL(
   connection: ApiConnection,
): Promise<ApiSchema[]> {
   if (!connection.mysqlConnection) {
      throw new Error("Mysql connection is required");
   }
   return [
      {
         name: connection.mysqlConnection.database || "mysql",
         isHidden: false,
         isDefault: true,
      },
   ];
}

async function listTablesForMySQL(
   connection: ApiConnection,
   schemaName: string,
   malloyConnection: Connection,
   tableNames?: string[],
): Promise<ApiTable[]> {
   if (!connection.mysqlConnection) {
      throw new Error("Mysql connection is required");
   }
   return withIntrospectionFailure(
      `tables for MySQL schema ${schemaName} in connection ${connection.name}`,
      async () => {
         const result = await runIntrospectionSQL(
            malloyConnection,
            `SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM information_schema.columns WHERE table_schema = '${sqlLiteral(schemaName, connection.type)}' ${sqlInFilter("TABLE_NAME", tableNames, connection.type)} ORDER BY TABLE_NAME, ORDINAL_POSITION`,
         );
         const rows = standardizeRunSQLResult(result);
         return groupColumnRowsIntoTables(rows, (t) => `${schemaName}.${t}`);
      },
   );
}
