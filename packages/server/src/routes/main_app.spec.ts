// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The main app's middleware order, which is load-bearing: the frame-ancestors
 * policy ahead of anything that answers early, the overlay body parser ahead
 * of the general one, the draining guard after the probes and before the API,
 * the error mapper last.
 */

import { describe, expect, it, spyOn } from "bun:test";
import request from "supertest";
import { MAX_OVERLAY_BYTES } from "../constants";
import { logger } from "../logger";
import { API_PREFIX, buildRouteApp } from "../test_helpers/route_app";
import { createMainApp } from "./main_app";

type Layer = { name: string; route?: { path: string } };

/** The app's layers, consecutive routes collapsed into their first path segment. */
function layout(app: unknown): string[] {
   const stack = (app as { _router: { stack: Layer[] } })._router.stack;
   const out: string[] = [];
   for (const layer of stack) {
      if (!layer.route) {
         out.push(layer.name);
         continue;
      }
      const entry = layer.route.path.startsWith(API_PREFIX)
         ? "<api routes>"
         : `route ${layer.route.path.replace(/^(\/[^/]*).*/, "$1")}`;
      if (out[out.length - 1] !== entry) out.push(entry);
   }
   return out;
}

describe("createMainApp", () => {
   it("mounts the middleware in production order", () => {
      expect(layout(buildRouteApp())).toEqual([
         "query",
         "expressInit",
         "loggerMiddleware",
         "httpMetricsMiddleware",
         "<anonymous>", // frame-ancestors
         "route /health",
         "<anonymous>", // rate limit
         "<anonymous>", // legacy package name (URL)
         "route /sdk",
         "route /environments",
         "corsMiddleware",
         "overlayBody",
         "jsonParser",
         "<anonymous>", // legacy package name (body)
         "drainingGuard",
         "<api routes>",
         "unhandledErrorMiddleware",
      ]);
   });

   it("serves the Console and its fallback only when given its root, and /metrics only when given its handler", () => {
      const app = createMainApp({
         controllers: {} as never,
         dataAppRuntimePath: "/nonexistent/publisher.js",
         consoleRoot: "/nonexistent/app",
         metricsHandler: (_req, res) => {
            res.end();
         },
      });
      const names = layout(app);
      expect(names.indexOf("serveStatic")).toBeGreaterThan(
         names.indexOf("route /environments"),
      );
      expect(names.indexOf("serveStatic")).toBeLessThan(
         names.indexOf("corsMiddleware"),
      );
      expect(names.indexOf("route /metrics")).toBeLessThan(
         names.indexOf("drainingGuard"),
      );
      expect(names.at(-2)).toBe("route *");
   });

   it("in development proxies the Console and still access-logs each request once", () => {
      const names = layout(
         createMainApp({
            controllers: {} as never,
            dataAppRuntimePath: "/nonexistent/publisher.js",
            proxyConsoleToDevServer: true,
         }),
      );
      expect(names.filter((n) => n === "loggerMiddleware")).toHaveLength(1);
      expect(names).not.toContain("serveStatic");
      expect(names).not.toContain("route *");
   });

   it("puts the frame-ancestors policy on an early refusal", async () => {
      const res = await request(buildRouteApp()).get(
         `${API_PREFIX}/environments?reload=true`,
      );
      expect(res.status).toBe(400);
      expect(res.headers["content-security-policy"]).toContain(
         "frame-ancestors",
      );
   });

   it("parses a compile overlay past the general body limit, and no other body", async () => {
      const app = buildRouteApp({
         compileController: {
            compile: async () => ({ status: "success", problems: [] }),
         },
      });
      const pkg = `${API_PREFIX}/environments/env/packages/pkg/models/m.malloy`;
      const body = { source: "x".repeat(Math.min(MAX_OVERLAY_BYTES, 2 << 20)) };
      // The access log carries the payload: two megabytes of it.
      const quiet = [
         spyOn(logger, "info").mockImplementation(() => logger),
         spyOn(logger, "error").mockImplementation(() => logger),
      ];
      try {
         expect(
            (await request(app).post(`${pkg}/compile`).send(body)).status,
         ).toBe(200);
         // Refused by the general parser before any route runs (its status
         // is the error mapper's to decide).
         expect(
            (await request(app).post(`${pkg}/query`).send(body)).status,
         ).toBeGreaterThanOrEqual(400);
      } finally {
         for (const spy of quiet) spy.mockRestore();
      }
   });
});
