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

// Serve files from inside a package directory at
//   /environments/<env>/packages/<pkg>/<relative-path>
//
// This route fully owns its prefix — it does NOT fall through to the SPA on
// missing files, because doing so would mask 404s (and in dev mode the SPA
// catch-all errors out before it can reply). Behavior:
//   - `/environments/<env>/packages/<pkg>`      → 302 to `…/<pkg>/`
//   - `/environments/<env>/packages/<pkg>/`     → serve `<pkgRoot>/public/index.html`
//   - `/environments/<env>/packages/<pkg>/foo/` → serve `<pkgRoot>/public/foo/index.html`
//   - `/environments/<env>/packages/<pkg>/<file>` → serve `<pkgRoot>/public/<file>`, or 404
// Only the package's `public/` directory is web-served. Models, data files, and
// the publisher.json manifest live outside it and are never reachable here, so
// nothing can be downloaded around the per-model #(authorize) and query
// controls. The data stays reachable through the permission-checked query path.

// Body for this route's 404s. Static because this route is reached with a
// resolved environment and package, so echoing the path back would confirm which
// of them exists; the SPA fallback's own 404 does echo it (escaped), because it
// is reached before anything has been resolved and the path is all it knows.
// An empty 404 is honest but leaves a blank page, and someone arriving here has
// usually guessed at the URL form, so it names the form.
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
      const pkg = await environment.getPackage(req.params.packageName, false);
      // Only the package's public/ directory is web-served. Models, data, and
      // the publisher.json manifest live outside it and are never reachable
      // through this route. This single directory boundary is the whole
      // access-control story for static files.
      const publicRoot = path.join(pkg.getPackagePath(), "public");

      // Directory-style fallback: empty path or trailing slash → look for
      // index.html within that directory.
      let subPath = subPathRaw;
      if (subPath === "" || subPath.endsWith("/")) {
         subPath = subPath + "index.html";
      }

      // Resolve the requested file under public/ and reject anything that
      // escapes it (`..`, encoded traversal) before touching the disk.
      // safeJoinUnderRoot is the shared lexical-containment primitive (it throws
      // BadRequestError on escape, surfaced as 400 by the outer catch); the
      // realpath check below additionally catches symlinks inside public/ that
      // point outward (403).
      const fullPath = safeJoinUnderRoot(publicRoot, subPath);

      // Containment check via realpath against the resolved public/ root.
      // Catches symlinks inside public/ that point out (e.g. a malicious
      // package shipping `public/leak -> /etc/passwd`), and tolerates the
      // package root itself being a symlink (how watch-mode in-place mount
      // works): realpath resolves it transparently and legitimate accesses
      // inside public/ stay within realPublicRoot. Missing public/ dir or
      // missing file: realpath throws ENOENT and we 404 cleanly instead of
      // leaking via Express's default error handler.
      let realPublicRoot: string;
      let realFullPath: string;
      try {
         realPublicRoot = await fsp.realpath(publicRoot);
         realFullPath = await fsp.realpath(fullPath);
      } catch {
         if (!res.headersSent) {
            // Generic 404 with no reflected request input (avoids reflecting
            // user-controlled path/package name into the response body).
            res.status(404).type("text/html").send(PACKAGE_FILE_NOT_FOUND_HTML);
         }
         return;
      }
      const rel = path.relative(realPublicRoot, realFullPath);
      if (rel.startsWith("..") || path.isAbsolute(rel)) {
         res.status(403).end();
         return;
      }

      // The framing policy is set once for every document by
      // `frameAncestorsMiddleware`, mounted ahead of all routes. It used to be
      // set here, on this route alone, which is what left the Console catch-all
      // with no framing header at all -- see the note in `frame_ancestors.ts`.
      // Never let a served asset be MIME-sniffed into a different content type.
      res.setHeader("X-Content-Type-Options", "nosniff");
      res.sendFile(realFullPath, (err) => {
         if (err) {
            // Own the 404 instead of letting Express fall through to a
            // catch-all that may error.
            if (!res.headersSent) {
               // Generic 404, no reflected request input (see above).
               res.status(404)
                  .type("text/html")
                  .send(PACKAGE_FILE_NOT_FOUND_HTML);
            }
         }
      });
   } catch (e) {
      // Map service errors to their real status — a bad package name is a 400,
      // memory back-pressure is a 503 — rather than flattening everything to
      // 404. A genuine missing file is already handled by the realpath/sendFile
      // 404 paths above; this catch only sees service-layer failures.
      if (!res.headersSent) {
         const { json, status } = internalErrorToHttpError(e as Error);
         res.status(status).json(json);
      }
   }
}

// `/environments/<env>/packages/<pkg>` (no trailing slash, no path) redirect so
// relative URLs in the served HTML resolve as expected. Express's default loose
// matching also catches the trailing-slash form here, so only redirect URLs that
// don't already end with `/`.
//
// Build the PATH from the validated route params, so it is always this same
// canonical, same-origin path with a trailing slash, and place the slash before
// any query string (e.g. ?embed_token=...). That is what removes the
// open-redirect surface; the query itself is spliced verbatim and percent-encoded
// by `res.redirect`, which is what removes the header-injection surface. See
// withRequestQuery.
/**
 * Re-attach the request's query string to a redirect target. Spliced verbatim off
 * the request target rather than rebuilt from the parsed query, because rebuilding
 * flattens anything the parser turned into a structure and silently corrupts it.
 *
 * What keeps this safe is not the rebuild: it is that the PATH is assembled from
 * validated segments and always starts `/environments/`, so the target cannot
 * change origin, and that `res.redirect` runs the value through `encodeurl` on the
 * way into the header, which percent-encodes CR, LF, space and quotes. Shared with
 * the SPA fallback's redirect, where dropping the query would strip an embedded
 * page's `?embed_token=...` on the way to the right path.
 */
export function withRequestQuery(req: express.Request, target: string): string {
   // Taken verbatim off the request target rather than rebuilt from the parsed
   // query. Rebuilding flattens anything the parser turned into a structure:
   // Express's default `extended` parser reads `?filter[a]=1` as an object, and
   // `String(value)` then emits `filter=[object Object]`, so a page arrives with
   // a corrupted parameter rather than an intact one. The path is still built
   // from validated segments, which is what keeps the Location same-origin;
   // Express percent-encodes the result on the way into the header.
   // Cut the fragment first: a non-browser client can send `#x?y=1`, and taking
   // the first `?` in the whole target would promote fragment text into the
   // redirect's query, handing the page a parameter the caller never put there.
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
      // Short cache so live edits during local dev show up quickly. In
      // production this file is content-stable per release.
      res.setHeader("cache-control", "public, max-age=60");
      res.setHeader("X-Content-Type-Options", "nosniff");
      res.sendFile(runtimePath, (err) => {
         if (err) {
            logger.error("Failed to send publisher.js runtime", { error: err });
            if (!res.headersSent) res.status(500).end();
         }
      });
   });

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
 * The API listing of a package's data apps. Registered after CORS and the body
 * parser, unlike the static routes, so cross-origin SDK consumers get CORS
 * headers.
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
         );
         return listPackageDataApps(
            req.params.environmentName,
            req.params.packageName,
            path.join(pkg.getPackagePath(), "public"),
         );
      }),
   );
}

// List the in-package HTML data apps bundled inside a package. Used by the
// SPA's package-detail view to surface a clickable list, and by anyone who
// wants to discover them programmatically without scraping the directory.
//
// Returns a `DataApp[]` (see api-doc.yaml) — each item carries the relative
// `path`, the `packageName`, the `title` (from its <title> tag), and a
// `resource` URL. `resource` is the root-relative static-serve URL (NOT under
// `${API_PREFIX}`) because a data app is a static asset served off the server
// root, unlike API resources such as `Package.resource`.
// Recursive depth is capped to keep this cheap for huge package directories.
const DATA_APPS_DEPTH_CAP = 3;
type DataAppItem = {
   resource: string;
   packageName: string;
   path: string;
   title: string;
   fit?: "viewport";
};

// The spots in an HTML head where a "<meta ...>" literal would NOT be a live
// tag: HTML comments (terminated or unterminated) and raw-text/RCDATA elements
// (script/style/title/textarea). One alternation so a single .replace covers
// them all (and so the fixpoint loop below applies one self-referential
// replace, which is the complete-sanitization shape CodeQL recognizes).
const NON_TAG_TEXT_PATTERN =
   /<!--[\s\S]*?-->|<!--[\s\S]*$|<(script|style|title|textarea)\b[\s\S]*?<\/\1\s*>/gi;

// Remove those matches until the string stops changing. A single pass is
// incomplete because removing one match can splice the surrounding text into a
// new one (CWE-116), so re-apply the same pattern to its own output until a
// fixpoint. Each pass only deletes, so the string strictly shrinks and the loop
// terminates, bounded by the input length (callers pass at most the first 4KB).
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
): Promise<DataAppItem[]> {
   const out: DataAppItem[] = [];

   // Resolve the public/ root once and reject any entry whose realpath escapes
   // it. Same containment defense as serveFromPackage: catches symlinks inside
   // public/ pointing outside (e.g. `public/leak -> ../report.malloy`) before we
   // open and read the target's first 4KB for title extraction. A package with
   // no public/ dir fails realpath here and yields an empty list.
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
            // Cheap metadata extraction: read first 4KB and grep the <head>.
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
                  // Full-screen apps (e.g. slide decks) opt into a viewport-fill
                  // embed with <meta name="publisher:fit" content="viewport">.
                  // FIRST strip the spots where the literal string is NOT a live
                  // tag (comments, script/style/title/textarea), THEN look at the
                  // <head> region only (up to </head> or <body>). Order matters:
                  // stripping before locating the boundary keeps a literal
                  // "<body>"/"</head>" inside a comment or <script> from
                  // truncating the scan and hiding a real tag. What's left and
                  // matches is a genuine <meta> the browser would honor too, so a
                  // documented/commented example or a string in a code block
                  // can't opt the page in. Match by name (attribute order/quoting
                  // vary), then confirm content="viewport"; the [\s"'] before
                  // `name` keeps `data-name="publisher:fit"` out. Like the title,
                  // the tag must sit within the first 4KB.
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
               // ignore; fall back to relative path as title
            }
            out.push({
               resource: `/environments/${environmentName}/packages/${packageName}/${rel}`,
               packageName,
               path: rel,
               title,
               fit,
            });
         }
      }
   }

   await walk(publicRoot, 0);
   out.sort((a, b) => {
      // Surface index.html first, then alphabetical.
      if (a.path === "index.html") return -1;
      if (b.path === "index.html") return 1;
      return a.path.localeCompare(b.path);
   });
   return out;
}
