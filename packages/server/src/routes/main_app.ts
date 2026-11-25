// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The main API's Express app: every middleware and route table in the order
 * `main_server.ts` serves them, built from the controllers it is handed so a
 * test mounts exactly what production mounts. The order is load-bearing (see
 * the comments below and `main_app.spec.ts`).
 */

import bodyParser from "body-parser";
import cors from "cors";
import express from "express";
import { createProxyMiddleware } from "http-proxy-middleware";
import * as path from "path";
import { MAX_OVERLAY_BYTES } from "../constants";
import { frameAncestorsMiddleware } from "../frame_ancestors";
import { drainingGuard, registerHealthEndpoints } from "../health";
import { httpMetricsMiddleware } from "../instrumentation";
import {
   legacyPackageNameBodyMiddleware,
   legacyPackageNameMiddleware,
} from "../legacy_package_name";
import { loggerMiddleware } from "../logger";
import { rateLimitMiddleware } from "../rate_limit";
import { registerLegacyRoutes } from "../server-old";
import { isOverlayRoute } from "../service/request_overlay";
import type { AppServices } from "../app_services";
import { registerApiRoutes } from "./api";
import {
   registerDataAppListRoute,
   registerDataAppStaticRoutes,
} from "./data_apps";
import { unhandledErrorMiddleware } from "./json_route";
import { registerSpaFallback } from "./spa";

export const API_PREFIX = "/api/v0";

/** The Vite dev server the Console is proxied to in development. */
const DEV_CONSOLE_ORIGIN = "http://localhost:5173";

export interface MainAppOptions {
   controllers: AppServices;
   /** The `publisher.js` runtime data apps load. */
   dataAppRuntimePath: string;
   /**
    * Where the built Console lives, served with its SPA fallback. Absent in
    * development, where the Console is proxied to the Vite dev server instead,
    * and in tests, which serve no Console.
    */
   consoleRoot?: string;
   /** Proxy the Console to the Vite dev server (development only). */
   proxyConsoleToDevServer?: boolean;
   frameAncestors?: string;
   /** Requests per minute per client; 0 turns the limit off. */
   rateLimitPerMinute?: number;
   /** The Prometheus scrape handler, when the exporter is up. */
   metricsHandler?: express.RequestHandler;
}

export function createMainApp(options: MainAppOptions): express.Express {
   const { controllers } = options;
   const { environmentStore } = controllers;
   const app = express();
   app.use(loggerMiddleware);
   app.use(httpMetricsMiddleware);
   // Ahead of every early-answering middleware (the rate limiter's 429
   // included), so the frame-ancestors policy is on every response.
   app.use(frameAncestorsMiddleware(options.frameAncestors));
   // Health probes come before every middleware they do not need, such as
   // static-file serving, which would stat the filesystem first.
   registerHealthEndpoints(app);
   // Mounted before every route; probes and /metrics are exempt inside it.
   app.use(rateLimitMiddleware(options.rateLimitPerMinute ?? 0));

   // Before every package route, the data-app static routes included: a
   // package version named in a combined package name (see legacy_package_name).
   app.use(legacyPackageNameMiddleware());
   // Before the SPA catch-all and the dev-server proxy.
   registerDataAppStaticRoutes(
      app,
      environmentStore,
      options.dataAppRuntimePath,
   );
   if (options.consoleRoot) {
      app.use("/", express.static(options.consoleRoot));
      app.use(
         "/api-doc.html",
         express.static(path.join(options.consoleRoot, "api-doc.html")),
      );
   }
   if (options.proxyConsoleToDevServer) {
      app.use(
         createProxyMiddleware({
            target: DEV_CONSOLE_ORIGIN,
            changeOrigin: true,
            ws: true,
            pathFilter: (path) =>
               !path.startsWith("/api/") &&
               !path.startsWith("/metrics") &&
               !path.startsWith("/health"),
         }),
      );
   }
   app.use(cors({ origin: DEV_CONSOLE_ORIGIN, credentials: true }));

   // Compile takes a `files` overlay of up to MAX_OVERLAY_BYTES of text, which
   // JSON escaping can grow, so it parses a larger body; the overlay's own
   // bounds answer 413 past that. Parsed once: the general parser below skips
   // a body already read.
   const overlayBodyParser = bodyParser.json({
      limit: 2 * MAX_OVERLAY_BYTES + 1024 * 1024,
   });
   app.use(function overlayBody(req, res, next) {
      return isOverlayRoute(req.path)
         ? overlayBodyParser(req, res, next)
         : next();
   });
   app.use(bodyParser.json({ limit: "1mb" }));
   app.use(legacyPackageNameBodyMiddleware());

   if (options.metricsHandler) app.get("/metrics", options.metricsHandler);

   // After the health endpoints and /metrics, before every other route.
   app.use(drainingGuard);

   // After CORS, the body parser and the draining guard, so cross-origin SDK
   // consumers and authenticated requests both work.
   registerDataAppListRoute(app, API_PREFIX, environmentStore);
   registerApiRoutes(app, API_PREFIX, controllers);
   // Legacy `/projects/...` aliases; must precede the SPA catch-all.
   registerLegacyRoutes(app, controllers);
   if (options.consoleRoot) {
      registerSpaFallback(
         app,
         API_PREFIX,
         options.consoleRoot,
         environmentStore,
      );
   }
   app.use(unhandledErrorMiddleware);
   return app;
}
