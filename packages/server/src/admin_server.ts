// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// `servingApiEnabled: false` (server_apis.ts): only the admin listener. No main
// API, no MCP, and no environment store, so the process that holds the catalog
// credential has nothing of anyone's to serve.
import "./instrumentation";
import { markReady, registerSignalHandlers } from "./health";
import { getPrometheusMetricsHandler } from "./instrumentation";
import { logger } from "./logger";
import { logListening, longTimeouts } from "./listen";
import { createAdminApp } from "./routes/admin";
import { CatalogAdmin, catalogAdminConfig } from "./service/catalog_admin";
import { settings } from "./settings";

// assertServerApis has already required the port.
const ADMIN_PORT = settings.get("admin.port")!;
const ADMIN_HOST = settings.get("admin.host");
const CATALOG_CONFIG = catalogAdminConfig();
const catalogs = CATALOG_CONFIG ? new CatalogAdmin(CATALOG_CONFIG) : null;

// The only listener, so the only place the process's metrics can be scraped.
const adminApp = createAdminApp(catalogs, null, getPrometheusMetricsHandler());

const adminServer = adminApp.listen(
   ADMIN_PORT,
   ADMIN_HOST,
   function (this: import("net").Server) {
      logListening("Admin server", this, ADMIN_HOST, ADMIN_PORT);
      // Nothing to load: ready once the listener is up.
      markReady();
   },
);
adminServer.on("error", (error: NodeJS.ErrnoException) => {
   logger.error(
      error.code === "EADDRINUSE"
         ? `Port ${ADMIN_PORT} in use; pass --admin_port <n>`
         : `Admin listener failed on port ${ADMIN_PORT}: ${error.message}`,
   );
   process.exit(1);
});
longTimeouts(adminServer);

registerSignalHandlers(
   null,
   null,
   settings.get("shutdown.drainDurationSeconds"),
   settings.get("shutdown.gracefulCloseTimeoutSeconds"),
   [adminServer],
);
