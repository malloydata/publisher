// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type express from "express";
import * as path from "path";
import {
   recordConsoleAliasUse,
   isLegacyReportAlias,
} from "../service/legacy_report_views";
import type { EnvironmentStore } from "../service/environment_store";
import { resolveSpaFallback, type LoadedRoutes } from "../spa_fallback";
import { withRequestQuery } from "./data_apps";

/**
 * The production catch-all: serves the Console's `index.html` for app routes,
 * redirects a guessed URL to its canonical form, and answers a real 404 for an
 * unknown API path or a missing file. Registered last.
 */
export function registerSpaFallback(
   app: express.Express,
   apiPrefix: string,
   appRoot: string,
   environmentStore: EnvironmentStore,
): void {
   const SPA_INDEX = path.resolve(appRoot, "index.html");
   const escapeHtml = (value: string) =>
      value.replace(
         /[<>&]/g,
         (c) => ({ "<": "&lt;", ">": "&gt;", "&": "&amp;" })[c] ?? c,
      );
   // Deliberately the in-memory lists rather than `getEnvironment`, which
   // resolves from storage (see `LoadedRoutes`). The cost: an environment or
   // package not loaded yet (a cold start, a lazily loaded environment) gets
   // the 404 page, which names the canonical URL that does load it.
   const loadedPackage = (environmentName: string, packageName: string) =>
      environmentStore
         .getLoadedEnvironments()
         .find(
            (environment) =>
               environment.getEnvironmentName() === environmentName,
         )
         ?.getLoadedPackages()
         .find((pkg) => pkg.getPackageName() === packageName);
   const loadedRoutes: LoadedRoutes = {
      hasEnvironment: (environmentName) =>
         environmentStore
            .getLoadedEnvironments()
            .some(
               (environment) =>
                  environment.getEnvironmentName() === environmentName,
            ),
      hasPackage: (environmentName, packageName) =>
         loadedPackage(environmentName, packageName) !== undefined,
      isReportRoute: (environmentName, packageName, routePath) => {
         const pkg = loadedPackage(environmentName, packageName);
         return (
            pkg !== undefined &&
            (pkg.isServedReport(`${routePath}.malloy`) ||
               isLegacyReportAlias(pkg, routePath))
         );
      },
   };
   app.get("*", (req, res) => {
      // Not everything unmatched is an app route. A request that names a file
      // gets a real answer rather than the app shell with a 200, which reads as
      // success and then leaves the app blaming the file for a wrong path. See
      // classifySpaFallback for why the decision is by extension, and
      // resolveSpaFallback for how its guesses are settled.
      const fallback = resolveSpaFallback(req.path, apiPrefix, loadedRoutes);
      if (fallback.kind === "redirect") {
         // 302, not a permanent redirect: this maps a mistaken URL onto the
         // right one, and a path the app may later claim as a route of its own
         // must not be cached against it in every browser that guessed once.
         res.redirect(302, withRequestQuery(req, fallback.location));
         return;
      }
      if (fallback.kind === "apiNotFound") {
         // A caller that asked the API for something gets JSON when it is not
         // there, not the HTML app shell it cannot parse.
         res.status(404).json({
            code: 404,
            // No method in the message: this handler is registered on app.get, so
            // it could only ever say GET. Other verbs already 404 through
            // Express's own default, which is why the 200-plus-shell defect was
            // GET-only in the first place.
            message: `Unknown API endpoint: ${fallback.path}. See /api-doc.yaml for the endpoints this server serves.`,
         });
         return;
      }
      if (fallback.kind === "assetNotFound") {
         res.status(404)
            .type("text/html")
            .send(
               `<!doctype html><meta charset="utf-8">
<title>Not found</title>
<style>body{font:14px/1.4 -apple-system,system-ui,sans-serif;margin:40px;max-width:720px;color:#222}code{background:#f4f4f5;padding:1px 4px;border-radius:3px}</style>
<h1>Not found</h1>
<p>Nothing is served at <code>${escapeHtml(fallback.path)}</code>.</p>
<p>A file inside a package is served from that package's <code>public/</code> directory at
<code>/environments/&lt;env&gt;/packages/&lt;pkg&gt;/&lt;file&gt;</code>, where <code>&lt;file&gt;</code> is relative to
<code>public/</code> and does not include it. Models and notebooks open in the web UI at
<code>/&lt;env&gt;/&lt;pkg&gt;/&lt;file&gt;.malloy</code> and <code>.malloynb</code>.</p>
<p><a href="/">Publisher home</a> lists the environments and packages this server has.</p>`,
            );
         return;
      }
      recordConsoleAliasUse(req.path, loadedPackage);
      res.sendFile(SPA_INDEX, (err) => {
         if (!err) return;
         // The SPA bundle isn't built. This happens when running directly
         // from source (`bun run src/server.ts`) without first running
         // `bun run build:app`. Return a friendly placeholder rather than
         // a 500, and surface package URLs the user might be looking for.
         if (res.headersSent) return;
         res.status(404)
            .type("text/html")
            .send(
               `<!doctype html><meta charset="utf-8">
<title>Publisher</title>
<style>body{font:14px/1.4 -apple-system,system-ui,sans-serif;margin:40px;max-width:720px;color:#222}</style>
<h1>Publisher is running, but the SPA bundle isn't built.</h1>
<p>You requested <code>${req.path.replace(/[<>&]/g, (c) => ({ "<": "&lt;", ">": "&gt;", "&": "&amp;" })[c] ?? c)}</code>.
The Publisher API is available at <a href="/api/v0/environments">/api/v0/environments</a>.</p>
<p>To get the Publisher web UI, run <code>cd packages/app &amp;&amp; bunx vite build</code>
or start the server with <code>NODE_ENV=development</code> after launching Vite on <code>:5173</code>.</p>
<p>For in-package HTML data apps, browse to <code>/environments/&lt;env&gt;/packages/&lt;pkg&gt;/&lt;file&gt;</code> directly.</p>`,
            );
      });
   });
}
