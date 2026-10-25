// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The storage-destination routes: an orchestrator's operations on a DuckLake
 * destination's catalog and bucket (admin-api-doc.yaml). None changes this
 * server's own state, so they are served on the admin listener
 * (`routes/admin.ts`). Three of them predate it and are also still served on the
 * main API, deprecated; removing those is deleting the `public` branch below.
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
   surface: "admin" | "public",
): void {
   const destination = `${apiPrefix}/environments/:environmentName/storageDestinations/:destinationName`;
   const before: express.RequestHandler[] =
      surface === "public" ? [deprecatedOnPublic] : [];

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
   if (surface === "public") return;

   app.post(
      `${destination}/schemas/:schemaName/tables`,
      apiRoute(
         (req) =>
            controller.createTable(
               req.params.environmentName,
               req.params.destinationName,
               req.params.schemaName,
               req.body ?? {},
            ),
         { status: 201 },
      ),
   );
   app.post(
      `${destination}/bootstraps`,
      apiRoute(
         (req) =>
            controller.createBootstrap(
               req.params.environmentName,
               req.params.destinationName,
               req.body ?? {},
            ),
         { status: 201 },
      ),
   );
}
