// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The admin listener's app: the DuckLake catalogs in one configured catalog
 * database, as resources (admin-api-doc.yaml). It acts on catalogs and their
 * buckets, never on this server's environments, packages or connections. It
 * starts only when PUBLISHER_ADMIN_PORT is set, and is unauthenticated like the
 * main API, so it binds loopback unless told otherwise.
 */

import bodyParser from "body-parser";
import express from "express";
import { registerHealthEndpoints } from "../health";
import { httpMetricsMiddleware } from "../instrumentation";
import { loggerMiddleware } from "../logger";
import type { WatchModeController } from "../controller/watch-mode.controller";
import { type CatalogAdmin, CatalogAdminError } from "../service/catalog_admin";
import { BadRequestError } from "../errors";
import {
   apiRoute,
   logRefusal,
   sendRouteError,
   unhandledErrorMiddleware,
} from "./json_route";

export const ADMIN_API_PREFIX = "/api/v0";

/**
 * The port, or null when the listener is off (unset or empty). A value that is
 * not a port throws, so a typo fails startup rather than silently leaving an
 * orchestrator nothing to call.
 */
export function resolveAdminPort(env: NodeJS.ProcessEnv): number | null {
   const raw = env.PUBLISHER_ADMIN_PORT?.trim();
   if (!raw) return null;
   const port = Number(raw);
   if (!Number.isInteger(port) || port < 0 || port > 65535) {
      throw new Error(
         `PUBLISHER_ADMIN_PORT must be a port number, got '${raw}'.`,
      );
   }
   return port;
}

/**
 * PUBLISHER_ADMIN_HOST, else loopback. Unlike the MCP listener it does not
 * follow PUBLISHER_HOST: moving the main API onto every interface must not
 * expose the admin operations with it.
 */
export function resolveAdminHost(env: NodeJS.ProcessEnv): string {
   return env.PUBLISHER_ADMIN_HOST || "127.0.0.1";
}

/**
 * A JSON route: the handler's value is the body with `status`; an admin refusal
 * answers its own status with `{code, message, reason}`; anything else maps as
 * every other route's error does.
 */
function adminRoute(
   catalogs: CatalogAdmin | null,
   handler: (
      catalogs: CatalogAdmin,
      req: express.Request,
      res: express.Response,
   ) => Promise<unknown>,
   { status = 200 }: { status?: number } = {},
): express.RequestHandler {
   return async (req, res) => {
      try {
         if (!catalogs) {
            throw new CatalogAdminError(
               503,
               "CATALOG_DATABASE_NOT_CONFIGURED",
               "No catalog database is configured: set PUBLISHER_ADMIN_CATALOG_DATABASE.",
            );
         }
         const result = await handler(catalogs, req, res);
         if (!res.headersSent) res.status(status).json(result);
      } catch (error) {
         if (error instanceof CatalogAdminError) {
            logRefusal(req, error)(error.status);
            res.status(error.status).json({
               code: error.status,
               message: error.message,
               reason: error.reason,
            });
            return;
         }
         sendRouteError(res, error as Error, logRefusal(req, error as Error));
      }
   };
}

export function registerAdminRoutes(
   app: express.Express,
   apiPrefix: string,
   catalogs: CatalogAdmin | null,
): void {
   const catalog = `${apiPrefix}/catalogs/:catalogName`;
   const table = `${catalog}/schemas/:schemaName/tables/:tableName`;

   app.get(
      `${apiPrefix}/catalogs`,
      adminRoute(catalogs, (c) => c.listCatalogs()),
   );
   app.post(
      `${apiPrefix}/catalogs`,
      adminRoute(catalogs, (c, req) => c.createCatalog(req.body ?? {}), {
         status: 201,
      }),
   );
   app.get(
      catalog,
      adminRoute(catalogs, (c, req) => c.getCatalog(req.params.catalogName)),
   );
   app.patch(
      catalog,
      adminRoute(catalogs, (c, req) =>
         c.updateCatalog(req.params.catalogName, req.body ?? {}),
      ),
   );
   app.get(
      `${catalog}/schemas/:schemaName/tables`,
      adminRoute(catalogs, (c, req) =>
         c.listTables(req.params.catalogName, req.params.schemaName),
      ),
   );
   app.get(
      table,
      adminRoute(catalogs, (c, req) =>
         c.getTable(
            req.params.catalogName,
            req.params.schemaName,
            req.params.tableName,
         ),
      ),
   );
   app.put(
      table,
      adminRoute(catalogs, async (c, req, res) => {
         const { table: result, created } = await c.putTable(
            req.params.catalogName,
            req.params.schemaName,
            req.params.tableName,
            req.body ?? {},
            req.get("If-None-Match") === "*",
         );
         res.status(created ? 201 : 200).json(result);
      }),
   );
   app.delete(
      table,
      adminRoute(catalogs, async (c, req, res) => {
         await c.deleteTable(
            req.params.catalogName,
            req.params.schemaName,
            req.params.tableName,
         );
         res.status(204).send();
      }),
   );
   app.get(
      `${catalog}/snapshots`,
      adminRoute(catalogs, async (c, req) => {
         const raw = req.query.limit;
         const limit = raw === undefined ? 100 : Number(raw);
         if (!Number.isInteger(limit) || limit < 1 || limit > 1000) {
            throw new CatalogAdminError(
               400,
               "INVALID_REQUEST",
               "'limit' must be an integer from 1 to 1000.",
            );
         }
         return c.listSnapshots(req.params.catalogName, limit);
      }),
   );
   app.delete(
      `${catalog}/snapshots`,
      adminRoute(catalogs, async (c, req) => {
         const raw = req.query.olderThan;
         const olderThan = typeof raw === "string" ? new Date(raw) : null;
         if (!olderThan || Number.isNaN(olderThan.getTime())) {
            throw new CatalogAdminError(
               400,
               "INVALID_REQUEST",
               "'olderThan' is required: an ISO 8601 date-time.",
            );
         }
         return c.deleteSnapshots(req.params.catalogName, olderThan);
      }),
   );
}

/**
 * Watch mode, one resource: what this process watches for local development.
 * GET reads it, PUT sets the watched environment (turning it on, or moving it),
 * DELETE turns it off. The live-reload event stream a dashboard listens to stays
 * on the main API.
 */
export function registerWatchModeRoutes(
   app: express.Express,
   apiPrefix: string,
   watchMode: WatchModeController,
): void {
   const path = `${apiPrefix}/watch-mode`;
   app.get(
      path,
      apiRoute(async () => watchMode.current()),
   );
   app.put(
      path,
      apiRoute(async (req) => {
         const environmentName = req.body?.environmentName;
         if (typeof environmentName !== "string" || !environmentName) {
            throw new BadRequestError("'environmentName' is required.");
         }
         return watchMode.watch(environmentName);
      }),
   );
   app.delete(
      path,
      apiRoute(async (_req, res) => {
         await watchMode.unwatch();
         res.status(204).send();
      }),
   );
}

/**
 * The admin app; `catalogs` is null when no catalog database is configured, and
 * `watchMode` is null with serving off, which loads no environments to watch.
 * `metrics` serves `/metrics` when the admin listener is the process's only
 * one; in-process, the main API already serves the same registry.
 */
export function createAdminApp(
   catalogs: CatalogAdmin | null,
   watchMode: WatchModeController | null,
   metrics?: express.RequestHandler,
): express.Express {
   const adminApp = express();
   registerHealthEndpoints(adminApp);
   if (metrics) adminApp.get("/metrics", metrics);
   adminApp.use(loggerMiddleware);
   adminApp.use(httpMetricsMiddleware);
   adminApp.use(bodyParser.json({ limit: "1mb" }));
   registerAdminRoutes(adminApp, ADMIN_API_PREFIX, catalogs);
   if (watchMode)
      registerWatchModeRoutes(adminApp, ADMIN_API_PREFIX, watchMode);
   adminApp.use(unhandledErrorMiddleware);
   return adminApp;
}
