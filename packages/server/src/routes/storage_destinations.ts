// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * DEPRECATED: the main API's storage-destination routes, the table listing, drop
 * and file cleanup an orchestrator's garbage collection used. The admin API's
 * catalog resources replace them (admin-api-doc.yaml: GET and DELETE
 * `/catalogs/{c}/schemas/{s}/tables[/{t}]`, and DELETE `/catalogs/{c}/snapshots`).
 * Every response carries `Deprecation: true`. Removing them is deleting this
 * module, its controller, and the one call in routes/api.ts.
 */

import type express from "express";
import type { StorageDestinationController } from "../controller/storage_destination.controller";
import { apiRoute } from "./json_route";

/** Marks a response from a route the admin listener now serves. */
const deprecatedOnPublic: express.RequestHandler = (_req, res, next) => {
   res.set("Deprecation", "true");
   next();
};

export function registerStorageDestinationRoutes(
   app: express.Express,
   apiPrefix: string,
   controller: StorageDestinationController,
): void {
   const destination = `${apiPrefix}/environments/:environmentName/storageDestinations/:destinationName`;
   const before = [deprecatedOnPublic];

   app.get(
      `${destination}/schemas/:schemaName/tables`,
      ...before,
      apiRoute((req) =>
         controller.listTables(
            req.params.environmentName,
            req.params.destinationName,
            req.params.schemaName,
         ),
      ),
   );
   app.delete(
      `${destination}/schemas/:schemaName/tables/:tableName`,
      ...before,
      apiRoute(async (req, res) => {
         await controller.dropTable(
            req.params.environmentName,
            req.params.destinationName,
            req.params.schemaName,
            req.params.tableName,
         );
         res.status(204).send();
      }),
   );
   app.post(
      `${destination}/fileCleanups`,
      ...before,
      apiRoute(
         (req) =>
            controller.createFileCleanup(
               req.params.environmentName,
               req.params.destinationName,
               req.body ?? {},
            ),
         { status: 201 },
      ),
   );
}
