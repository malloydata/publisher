// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The admin listener's app: operations an orchestrator runs on a storage
 * destination's catalog and bucket, which a standalone server has no use for
 * (admin-api-doc.yaml). It starts only when PUBLISHER_ADMIN_PORT is set, and is
 * unauthenticated like the main API, so it binds loopback unless told
 * otherwise.
 */

import bodyParser from "body-parser";
import express from "express";
import type { StorageDestinationController } from "../controller/storage_destination.controller";
import { registerHealthEndpoints } from "../health";
import { httpMetricsMiddleware } from "../instrumentation";
import { loggerMiddleware } from "../logger";
import { unhandledErrorMiddleware } from "./json_route";
import { registerStorageDestinationRoutes } from "./storage_destinations";

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

export function createAdminApp(
   storageDestinationController: StorageDestinationController,
): express.Express {
   const adminApp = express();
   registerHealthEndpoints(adminApp);
   adminApp.use(loggerMiddleware);
   adminApp.use(httpMetricsMiddleware);
   adminApp.use(bodyParser.json({ limit: "1mb" }));
   registerStorageDestinationRoutes(
      adminApp,
      ADMIN_API_PREFIX,
      storageDestinationController,
      "admin",
   );
   adminApp.use(unhandledErrorMiddleware);
   return adminApp;
}
