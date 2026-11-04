// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * In-package HTML data apps: the runtime helper they load, the files a
 * package serves from its `public/` directory, and the API listing of them.
 * The static routes must be registered before the SPA catch-all and (in dev)
 * the Vite proxy.
 */

import type express from "express";
import type { Dirent } from "fs";
import * as fsp from "fs/promises";
import * as path from "path";
import { internalErrorToHttpError } from "../errors";
import { logger } from "../logger";
import { safeJoinUnderRoot } from "../path_safety";
import type { EnvironmentStore } from "../service/environment_store";
import { apiRoute } from "./json_route";

/** The data-app listing route, relative to the API prefix (api-doc.yaml pins it). */
export const DATA_APPS_ROUTE =
   "/environments/:environmentName/packages/:packageName/data-apps";

// `/environments/<env>/packages/<pkg>/<file>` serves `<pkgRoot>/public/<file>`
// (a trailing slash serves that directory's index.html). The route owns its
// prefix and never falls through to the SPA, which would mask 404s.
//
// Only `public/` is web-served: models, data and publisher.json live outside
// it, so nothing can be downloaded around #(authorize) and the query controls.

// Static, not echoing the path: this route has resolved the environment and
// package, so reflecting the path would confirm which exist. It names the URL
// form because someone landing here has usually guessed at it.
const PACKAGE_FILE_NOT_FOUND_HTML = `<!doctype html><meta charset="utf-8">
<title>Not found</title>
<style>body{font:14px/1.4 -apple-system,system-ui,sans-serif;margin:40px;max-width:720px;color:#222}code{background:#f4f4f5;padding:1px 4px;border-radius:3px}</style>
<h1>Not found</h1>
<p>This package does not serve that file. Only files inside the package's
<code>public/</code> directory are web-served, at
<code>/environments/&lt;env&gt;/packages/&lt;pkg&gt;/&lt;file&gt;</code>, where <code>&lt;file&gt;</code> is
relative to <code>public/</code> and does not include it.</p>
<p>Models and notebooks are not served here; they open in the web UI at
<code>/&lt;env&gt;/&lt;pkg&gt;/&lt;file&gt;.malloy</code>. <a href="/">Publisher home</a> lists what
this server has.</p>`;

async function serveFromPackage(
   environmentStore: EnvironmentStore,
   req: express.Request,
   res: express.Response,
): Promise<void> {
   const subPathRaw = (req.params as Record<string, string>)["0"] ?? "";
   try {
      const environment = await environmentStore.getEnvironment(
         req.params.environmentName,
         false,
      );
      // A page is served from the version its URL names (`?versionId=`), and
      // from `latest` when it names none. Only a version the package has pins
      // the page: a proxy may forward a page's query string unchanged, with a
      // `versionId` of its own that is no version here, and that page still
      // opens (from `latest`) rather than 404ing. The version comes from the
      // URL only.
      const staticVersionId = await environment
         .getVersionService()
         ?.pinnableVersion(req.params.packageName, req.query.versionId);
      const pkg = await environment.getPackage(req.params.packageName, false, {
         versionId: staticVersionId,
      });
      // This directory boundary is the whole access control for static files.
      const publicRoot = path.join(pkg.getPackagePath(), "public");

      let subPath = subPathRaw;
      if (subPath === "" || subPath.endsWith("/")) {
         subPath = subPath + "index.html";
      }

      // Lexical containment first (400 on `..` or encoded traversal), before
      // touching the disk.
      const fullPath = safeJoinUnderRoot(publicRoot, subPath);

      // Then realpath containment: rejects symlinks inside public/ that point
      // out (403), while tolerating a symlinked package root (watch mode's
      // in-place mount). A missing directory or file 404s here.
      let realPublicRoot: string;
      let realFullPath: string;
      try {
         realPublicRoot = await fsp.realpath(publicRoot);
         realFullPath = await fsp.realpath(fullPath);
      } catch {
         if (!res.headersSent) {
            res.status(404).type("text/html").send(PACKAGE_FILE_NOT_FOUND_HTML);
         }
         return;
      }
      const rel = path.relative(realPublicRoot, realFullPath);
      if (rel.startsWith("..") || path.isAbsolute(rel)) {
         res.status(403).end();
         return;
      }

      // The framing policy comes from `frameAncestorsMiddleware`, not here.
      res.setHeader("X-Content-Type-Options", "nosniff");
      res.sendFile(realFullPath, (err) => {
         if (err) {
            if (!res.headersSent) {
               res.status(404)
                  .type("text/html")
                  .send(PACKAGE_FILE_NOT_FOUND_HTML);
            }
         }
      });
   } catch (e) {
      // Service-layer failures keep their real status (400 bad name, 503
      // back-pressure); a missing file was already answered 404 above.
      if (!res.headersSent) {
         const { json, status } = internalErrorToHttpError(e as Error);
         res.status(status).json(json);
      }
   }
}

/**
 * Re-attach the request's query string (e.g. `?embed_token=...`) to a redirect
 * target. Spliced verbatim rather than rebuilt from the parsed query, which
 * would flatten `?filter[a]=1` to `filter=[object Object]`.
 *
 * Safe because callers build `target` from validated segments under
 * `/environments/`, so it cannot change origin, and `res.redirect`
 * percent-encodes CR, LF, space and quotes on the way into the header.
 */
export function withRequestQuery(req: express.Request, target: string): string {
   // Cut the fragment first, so a non-browser client's `#x?y=1` cannot promote
   // fragment text into the redirect's query.
   const hash = req.originalUrl.indexOf("#");
   const target_ =
      hash === -1 ? req.originalUrl : req.originalUrl.slice(0, hash);
   const marker = target_.indexOf("?");
   return marker === -1 ? target : target + target_.slice(marker);
}

/**
 * `/sdk/publisher.js`, the redirect from a bare package URL to its trailing-
 * slash form, and the files under each package's `public/` directory.
 */
export function registerDataAppStaticRoutes(
   app: express.Express,
   environmentStore: EnvironmentStore,
   runtimePath: string,
): void {
   app.get("/sdk/publisher.js", (_req, res) => {
      res.type("application/javascript");
      // Short cache so local-dev edits show up; stable per release otherwise.
      res.setHeader("cache-control", "public, max-age=60");
      res.setHeader("X-Content-Type-Options", "nosniff");
      res.sendFile(runtimePath, (err) => {
         if (err) {
            logger.error("Failed to send publisher.js runtime", { error: err });
            if (!res.headersSent) res.status(500).end();
         }
      });
   });

   // The bare package URL redirects to its trailing-slash form so relative URLs
   // in the served HTML resolve. Loose matching also routes the slash form
   // here, hence the pass-through.
   app.get(
      "/environments/:environmentName/packages/:packageName",
      (req, res, next) => {
         if (req.path.endsWith("/")) return next();
         const canonical =
            `/environments/${encodeURIComponent(req.params.environmentName)}` +
            `/packages/${encodeURIComponent(req.params.packageName)}/`;
         res.redirect(308, withRequestQuery(req, canonical));
      },
   );

   app.get(
      "/environments/:environmentName/packages/:packageName/*",
      (req, res) => serveFromPackage(environmentStore, req, res),
   );
}

/**
 * The API listing of a package's data apps, as `DataApp[]`. Registered after
 * CORS, unlike the static routes, so cross-origin SDK consumers get CORS
 * headers. Each `resource` is the root-relative static URL, not an API path.
 */
export function registerDataAppListRoute(
   app: express.Express,
   apiPrefix: string,
   environmentStore: EnvironmentStore,
): void {
   app.get(
      `${apiPrefix}${DATA_APPS_ROUTE}`,
      apiRoute(async (req) => {
         const environment = await environmentStore.getEnvironment(
            req.params.environmentName,
            false,
         );
         const pkg = await environment.getPackage(
            req.params.packageName,
            false,
            { versionId: req.query.versionId },
         );
         return listPackageDataApps(
            req.params.environmentName,
            req.params.packageName,
            path.join(pkg.getPackagePath(), "public"),
            pkg.getPackageMetadata().versionId,
         );
      }),
   );
}

// Keeps the listing cheap for huge package directories.
const DATA_APPS_DEPTH_CAP = 3;
type DataAppItem = {
   resource: string;
   packageName: string;
   versionId?: string;
   path: string;
   title: string;
   fit?: "viewport";
};

// Where a "<meta ...>" literal is not a live tag: HTML comments (terminated or
// not) and raw-text elements. One alternation, so the fixpoint loop below is a
// single self-referential replace, the shape CodeQL recognizes as complete.
const NON_TAG_TEXT_PATTERN =
   /<!--[\s\S]*?-->|<!--[\s\S]*$|<(script|style|title|textarea)\b[\s\S]*?<\/\1\s*>/gi;

// Repeated to a fixpoint because one removal can splice a new match together
// (CWE-116). Each pass only deletes, so the loop terminates.
function stripNonTagText(input: string): string {
   let current = input;
   let previous: string;
   do {
      previous = current;
      current = current.replace(NON_TAG_TEXT_PATTERN, "");
   } while (current !== previous);
   return current;
}

async function listPackageDataApps(
   environmentName: string,
   packageName: string,
   publicRoot: string,
   /** The published version listed from; its pages are pinned to it. */
   versionId?: string | null,
): Promise<DataAppItem[]> {
   const out: DataAppItem[] = [];

   // Same realpath containment as serveFromPackage, applied before reading any
   // file's head, so a symlink out of public/ is never opened. No public/
   // directory yields an empty list.
   let realPublicRoot: string;
   try {
      realPublicRoot = await fsp.realpath(publicRoot);
   } catch {
      return out;
   }

   async function walk(dir: string, depth: number) {
      if (depth > DATA_APPS_DEPTH_CAP) return;
      let entries: Dirent[];
      try {
         entries = await fsp.readdir(dir, { withFileTypes: true });
      } catch {
         return;
      }
      for (const entry of entries) {
         if (entry.name.startsWith(".") || entry.name === "node_modules")
            continue;
         const full = path.join(dir, entry.name);
         let realFull: string;
         try {
            realFull = await fsp.realpath(full);
         } catch {
            continue;
         }
         const contained = path.relative(realPublicRoot, realFull);
         if (contained.startsWith("..") || path.isAbsolute(contained)) continue;
         if (entry.isDirectory()) {
            await walk(full, depth + 1);
         } else if (
            entry.isFile() &&
            (entry.name.endsWith(".html") || entry.name.endsWith(".htm"))
         ) {
            const rel = path.relative(publicRoot, full).replace(/\\/g, "/");
            // Metadata comes from the first 4KB only.
            let title = rel;
            let fit: "viewport" | undefined;
            try {
               const fh = await fsp.open(full, "r");
               try {
                  const buf = Buffer.alloc(4096);
                  const { bytesRead } = await fh.read(buf, 0, 4096, 0);
                  const head = buf.slice(0, bytesRead).toString("utf8");
                  const m = head.match(/<title[^>]*>([^<]+)<\/title>/i);
                  if (m) title = m[1].trim();
                  // <meta name="publisher:fit" content="viewport"> opts a
                  // full-screen app into a viewport-fill embed. Non-tag text is
                  // stripped BEFORE finding the end of <head>, so a literal
                  // "</head>" in a comment or script cannot hide a real tag, and
                  // a commented example cannot opt the page in. The [\s"'] before
                  // `name` keeps `data-name="publisher:fit"` out.
                  const cleaned = stripNonTagText(head);
                  const headEnd = cleaned.search(/<\/head\s*>|<body[\s>]/i);
                  const headTags =
                     headEnd === -1 ? cleaned : cleaned.slice(0, headEnd);
                  const fitMeta = headTags.match(
                     /<meta\b[^>]*[\s"']name\s*=\s*["']publisher:fit["'][^>]*>/i,
                  );
                  if (
                     fitMeta &&
                     /\bcontent\s*=\s*["']\s*viewport\s*["']/i.test(fitMeta[0])
                  ) {
                     fit = "viewport";
                  }
               } finally {
                  await fh.close();
               }
            } catch {
               // The relative path stays the title.
            }
            out.push({
               // A published version's page keeps opening that version after
               // `latest` moves: the static route reads the version from the URL.
               resource:
                  `/environments/${environmentName}/packages/${packageName}/${rel}` +
                  (versionId
                     ? `?versionId=${encodeURIComponent(versionId)}`
                     : ""),
               packageName,
               ...(versionId ? { versionId } : {}),
               path: rel,
               title,
               fit,
            });
         }
      }
   }

   await walk(publicRoot, 0);
   out.sort((a, b) => {
      if (a.path === "index.html") return -1;
      if (b.path === "index.html") return 1;
      return a.path.localeCompare(b.path);
   });
   return out;
}
