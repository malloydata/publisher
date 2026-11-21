// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `databricks` connection type's registry entry. */

import type {
   ApiConnection,
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
