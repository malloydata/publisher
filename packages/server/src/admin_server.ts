// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// `servingApiEnabled: false` (server_apis.ts): only the admin listener. No main
// API, no MCP, and no environment store, so the process that holds the catalog
// credential has nothing of anyone's to serve.
import "./instrumentation";
import { getServerSocketTimeoutMs } from "./config";
import { markReady, registerSignalHandlers } from "./health";
import { getPrometheusMetricsHandler } from "./instrumentation";
import { logger } from "./logger";
import { resolveBoundPort } from "./mcp_config";
import {
   createAdminApp,
   resolveAdminHost,
   resolveAdminPort,
} from "./routes/admin";
import { applyServerTimeouts } from "./server_timeouts";
import {
   CatalogAdmin,
   catalogAdminConfigFromEnv,
} from "./service/catalog_admin";

// assertServerApis has already required the port.
const ADMIN_PORT = resolveAdminPort(process.env)!;
const ADMIN_HOST = resolveAdminHost(process.env);
const CATALOG_CONFIG = catalogAdminConfigFromEnv(process.env);
const catalogs = CATALOG_CONFIG ? new CatalogAdmin(CATALOG_CONFIG) : null;

// The only listener, so the only place the process's metrics can be scraped.
const adminApp = createAdminApp(catalogs, null, getPrometheusMetricsHandler());

const adminServer = adminApp.listen(
   ADMIN_PORT,
   ADMIN_HOST,
   function (this: import("net").Server) {
      const bound = this.address();
      const host =
         typeof bound === "object" && bound ? bound.address : ADMIN_HOST;
      logger.info(
         `Admin server listening at http://${host.includes(":") ? `[${host}]` : host}:${resolveBoundPort(bound, ADMIN_PORT)}`,
      );
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
// A file load or a snapshot delete can run for minutes, so this listener takes
// the main API's socket limits. Read at boot, as there.
applyServerTimeouts(adminServer, getServerSocketTimeoutMs());

registerSignalHandlers(
   null,
   null,
   Number(process.env.SHUTDOWN_DRAIN_DURATION_SECONDS || 0),
   Number(process.env.SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS || 0),
   [adminServer],
);
