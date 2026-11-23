// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * In-package HTML data apps: the runtime helper they load, the files a
 * package serves from its `public/` directory, and the API listing of them.
 * The static routes must be registered before the SPA catch-all and (in dev)
 * the Vite proxy.
 */

import { createHash } from "crypto";
import type express from "express";
import type { Dirent, Stats } from "fs";
import * as fsp from "fs/promises";
import * as path from "path";
import { API_PREFIX } from "../constants";
import { BadRequestError, internalErrorToHttpError } from "../errors";
import {
   droppedFrameAncestorSources,
   frameAncestorHostOrigins,
} from "../frame_ancestors";
import { logger } from "../logger";
import { safeJoinUnderRoot, stepsOutside } from "../path_safety";
import { confinedPackagePath } from "../utils";
import { pathToFileURL } from "url";
import type { PackageLookup } from "../service/environment_ports";
import { versionRefOf } from "../route_params";
import { LATEST_VERSION_ALIAS } from "../service/versions/version_service";
import { injectAfterDoctype, MAX_INJECTABLE_HTML_BYTES } from "./html_inject";
import { apiRoute } from "./json_route";
import {
   PAGE_CONTEXT_HEADER,
   pageContextScript,
   parsePageContextHeader,
   type PageContext,
} from "./page_context";
import {
   parseEnvironmentName,
   parsePackageName,
   VersionRef,
} from "../service/identity";

/** The data-app listing route, relative to the API prefix (api-doc.yaml pins it). */
export const DATA_APPS_ROUTE =
   "/environments/:environmentName/packages/:packageName/data-apps";

// `/environments/<env>/packages/<pkg>/<file>` serves `<pkgRoot>/public/<file>`
// (a trailing slash serves that directory's index.html). The route owns its
// prefix and never falls through to the SPA, which would mask 404s.
//
// A page load (HTML) of a versioned package is redirected to
// `/environments/<env>/packages/<pkg>/versions/<id>/public/<file>`, the same
// files pinned to one version, so the page's relative asset URLs resolve to
// that version too. HTML is served with a page context spliced in (see
// page_context.ts); every other file is sent as it is.
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
<code>public/</code> directory are web-served, where <code>&lt;file&gt;</code> is
relative to <code>public/</code> and does not include it. Two URL forms reach it: the
latest version at <code>/environments/&lt;env&gt;/packages/&lt;pkg&gt;/&lt;file&gt;</code>, and
one published version at
<code>/environments/&lt;env&gt;/packages/&lt;pkg&gt;/versions/&lt;version&gt;/public/&lt;file&gt;</code>.</p>
<p>Models and notebooks are not served here; they open in the web UI at
<code>/&lt;env&gt;/&lt;pkg&gt;/&lt;file&gt;.malloy</code>. <a href="/">Publisher home</a> lists what
this server has.</p>`;

/** What the static routes need beyond the store: the context every page is served with. */
interface StaticFileDeps {
   environmentStore: PackageLookup;
   /** The host patterns injected into pages, from `frameAncestors`. */
   hostOrigins: string[];
   trustPageContextHeader: boolean;
}

function sendNotFound(res: express.Response): void {
   if (!res.headersSent) {
      res.status(404).type("text/html").send(PACKAGE_FILE_NOT_FOUND_HTML);
   }
}

// Bump when what an HTML response is built from changes, so a cached 304 never outlives the old rules.
const ETAG_FORMAT = 1;

const isHtmlFile = (file: string) => /\.html?$/i.test(file);

// Containment is lexical, so it answers the same before the package's real public/ is known.
const LEXICAL_PUBLIC_ROOT = path.join(path.sep, "public");

/**
 * The request's path under `public/`, normalized and confined to it (400 on a
 * step outside, including `..` segments that climb above `public/` and walk
 * back in); empty for `public/` itself, with a trailing slash kept.
 */
function containedSubPath(raw: string): string {
   if (stepsOutside(path.normalize(raw))) {
      throw new BadRequestError("Path is outside of public/");
   }
   const joined = safeJoinUnderRoot(LEXICAL_PUBLIC_ROOT, raw);
   const rel = path
      .relative(LEXICAL_PUBLIC_ROOT, joined)
      .split(path.sep)
      .join("/");
   return rel !== "" && raw.endsWith("/") ? `${rel}/` : rel;
}

/** The versioned static URL of `subPath` (from `containedSubPath`) under `public/`. */
function versionedPublicUrl(
   req: express.Request,
   versionId: string,
   subPath: string,
): string {
   return (
      `/environments/${encodeURIComponent(req.params.environmentName)}` +
      `/packages/${encodeURIComponent(req.params.packageName)}` +
      `/versions/${encodeURIComponent(versionId)}/public/` +
      subPath.split("/").map(encodeURIComponent).join("/")
   );
}

/**
 * Serves one file of a package's `public/`. `versionInPath` is set by the
 * versioned route and wins over `?versionId=`: a proxy appends its own
 * resolved `versionId` to every request, and the path is what names the
 * version there.
 */
async function serveFromPackage(
   deps: StaticFileDeps,
   req: express.Request,
   res: express.Response,
   versionInPath?: string,
): Promise<void> {
   try {
      // Before either redirect and before the disk: a path outside public/ is refused, never redirected.
      const subPath = containedSubPath(
         (req.params as Record<string, string>)["0"] ?? "",
      );
      const environment = await deps.environmentStore.getEnvironment(
         parseEnvironmentName(req.params.environmentName),
         false,
      );
      const packageName = parsePackageName(req.params.packageName);
      const versions = environment.getVersionService();
      // A page is served from the version its URL names (`?versionId=`, or the
      // path of the versioned route), and from `latest` when it names none.
      // Only a version the package has pins the page: a proxy may forward a
      // page's query string unchanged, with a `versionId` of its own that is
      // no version here, and that page still opens (from `latest`) rather
      // than 404ing. The versioned route is strict instead: its path names a
      // version that must exist, and a package with no versions has none.
      let staticVersionId: string | undefined;
      // Refused from memory: the registry keeps nothing for a package with no versions, so asking would re-read it under the storage lock on every request.
      const loaded = environment.peekPackage(packageName);
      if (
         versionInPath !== undefined &&
         loaded &&
         loaded.getVersionId() === undefined
      ) {
         return sendNotFound(res);
      }
      if (versionInPath === undefined) {
         staticVersionId = await versions?.pinnableVersion(
            packageName,
            req.query.versionId,
         );
      } else if (versionInPath === LATEST_VERSION_ALIAS) {
         const latest = (await versions?.latestOf(packageName)) ?? null;
         if (latest === null) return sendNotFound(res);
         res.setHeader("Cache-Control", "no-store");
         res.redirect(
            302,
            withRequestQuery(
               req,
               versionedPublicUrl(req, latest, subPath),
               "versionId",
            ),
         );
         return;
      } else {
         staticVersionId = await versions?.pinnableVersion(
            packageName,
            versionInPath,
         );
         if (staticVersionId === undefined) return sendNotFound(res);
      }
      const pkg = await environment.getPackage(packageName, false, {
         versionId: VersionRef.parse(staticVersionId),
      });
      // This directory boundary is the whole access control for static files.
      const publicRoot = path.join(pkg.getPackagePath(), "public");

      // The directory form serves its index.html. The join re-checks
      // containment at the sink, before touching the disk.
      const fullPath = safeJoinUnderRoot(
         publicRoot,
         subPath === "" || subPath.endsWith("/")
            ? `${subPath}index.html`
            : subPath,
      );

      // Then containment by the package's own walk, which never looks
      // outside the package. public/ itself is resolved against the package
      // root, so a public/ that links out serves nothing (403), whatever it
      // leads to; one that links inside the package (`public -> web`) serves.
      // A file must then resolve inside the real public/: a link out of it
      // answers 403 whatever it leads to. A symlinked package root (watch
      // mode's in-place mount) is tolerated. A package with no public/
      // directory answers 404 here, and a missing file 404s when it is sent.
      const served = await servedPublic(publicRoot);
      if (served.kind === "none") return sendNotFound(res);
      if (served.kind === "outside") {
         res.status(403).end();
         return;
      }
      const realFullPath = await insidePublic(
         publicRoot,
         served.real,
         fullPath,
      );
      if (realFullPath === undefined) {
         res.status(403).end();
         return;
      }

      // The framing policy comes from `frameAncestorsMiddleware`, not here.
      res.setHeader("X-Content-Type-Options", "nosniff");
      const versionId = pkg.getVersionId() ?? null;
      if (isHtmlFile(realFullPath)) {
         const stat = await fsp.stat(realFullPath).catch(() => undefined);
         // A dotfile is not served, as `sendFile` does for any other type.
         if (!stat?.isFile() || path.basename(realFullPath).startsWith(".")) {
            return sendNotFound(res);
         }
         if (versionInPath === undefined && versionId !== null) {
            res.setHeader("Cache-Control", "no-store");
            res.redirect(
               302,
               withRequestQuery(
                  req,
                  versionedPublicUrl(req, versionId, subPath),
                  "versionId",
               ),
            );
            return;
         }
         if (stat.size <= MAX_INJECTABLE_HTML_BYTES) {
            await sendHtmlWithContext(
               deps,
               req,
               res,
               realFullPath,
               stat,
               versionId,
            );
            return;
         }
      }
      res.sendFile(realFullPath, (err) => {
         if (err) sendNotFound(res);
      });
   } catch (e) {
      // Service-layer failures keep their real status (400 bad name, 503
      // back-pressure, 410 archived version); a missing file was already
      // answered 404 above.
      if (!res.headersSent) {
         const { json, status } = internalErrorToHttpError(e as Error);
         res.status(status).json(json);
      }
   }
}

/**
 * An HTML page with its context spliced in. Sent with `res.send` so Express
 * answers HEAD itself; a range request gets the whole page, because byte
 * offsets into a rewritten body would mean nothing. The ETag is weak because a
 * gzip proxy strips strong ones, and is derived from the file's stat and the
 * context alone, so a conditional request that matches costs one `stat`: no
 * read, no scan. The page is revalidated every time because the context it
 * carries can differ per request under `trustPageContextHeader`.
 */
async function sendHtmlWithContext(
   deps: StaticFileDeps,
   req: express.Request,
   res: express.Response,
   file: string,
   stat: Stats,
   versionId: string | null,
): Promise<void> {
   let override = {};
   if (deps.trustPageContextHeader) {
      res.vary(PAGE_CONTEXT_HEADER);
      const header = parsePageContextHeader(req.get(PAGE_CONTEXT_HEADER));
      if (header && "invalid" in header) {
         logger.warn("Ignoring an invalid page-context header", {
            reason: header.invalid,
         });
      } else if (header) {
         override = header.override;
      }
   }
   const context: PageContext = {
      v: 1,
      environment: req.params.environmentName,
      package: req.params.packageName,
      versionId,
      apiBase: API_PREFIX,
      hostOrigins: deps.hostOrigins,
      ...override,
   };
   const snippet = pageContextScript(context);
   res.setHeader(
      "ETag",
      `W/"${createHash("sha256")
         .update(`${ETAG_FORMAT}:${stat.size}:${stat.mtimeMs}:${snippet}`)
         .digest("hex")}"`,
   );
   res.setHeader("Cache-Control", "no-cache");
   res.type(path.extname(file));
   if (req.fresh) {
      res.status(304).end();
      return;
   }
   const bytes = await fsp.readFile(file).catch(() => undefined);
   if (bytes === undefined) {
      res.removeHeader("ETag");
      return sendNotFound(res);
   }
   res.send(injectAfterDoctype(bytes, snippet) ?? bytes);
}

/**
 * Re-attach the request's query string (e.g. `?embed_token=...`) to a redirect
 * target. Spliced verbatim rather than rebuilt from the parsed query, which
 * would flatten `?filter[a]=1` to `filter=[object Object]`. `omit` drops the
 * parameters of that name, for a redirect whose target now says it in the path.
 *
 * Safe because callers build `target` from validated segments under
 * `/environments/`, so it cannot change origin, and `res.redirect`
 * percent-encodes CR, LF, space and quotes on the way into the header.
 */
export function withRequestQuery(
   req: express.Request,
   target: string,
   omit?: string,
): string {
   // Cut the fragment first, so a non-browser client's `#x?y=1` cannot promote
   // fragment text into the redirect's query.
   const hash = req.originalUrl.indexOf("#");
   const target_ =
      hash === -1 ? req.originalUrl : req.originalUrl.slice(0, hash);
   const marker = target_.indexOf("?");
   if (marker === -1) return target;
   if (omit === undefined) return target + target_.slice(marker);
   const kept = target_
      .slice(marker + 1)
      .split("&")
      .filter((pair) => queryKey(pair) !== omit);
   return kept.length === 0 ? target : `${target}?${kept.join("&")}`;
}

function queryKey(pair: string): string {
   const key = pair.split("=", 1)[0].replace(/\+/g, " ");
   try {
      return decodeURIComponent(key);
   } catch {
      return key;
   }
}

/**
 * `/sdk/publisher.js`, the redirect from a bare package URL to its trailing-
 * slash form, and the files under each package's `public/` directory, at the
 * latest version's URL and at one version's.
 */
export function registerDataAppStaticRoutes(
   app: express.Express,
   environmentStore: PackageLookup,
   runtimePath: string,
   options: { frameAncestors?: string; trustPageContextHeader: boolean },
): void {
   const unusable = droppedFrameAncestorSources(options.frameAncestors);
   if (unusable.length > 0) {
      logger.warn(
         "frameAncestors sources that cannot be data-app host bus origins (the CSP still applies them)",
         { sources: unusable },
      );
   }
   const deps: StaticFileDeps = {
      environmentStore,
      hostOrigins: frameAncestorHostOrigins(options.frameAncestors),
      trustPageContextHeader: options.trustPageContextHeader,
   };
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

   // Both before the catch-all below, which would read `versions/<v>/public/...`
   // as a file of `public/`. The slashless form also matches the slash form.
   const versionedRoot =
      "/environments/:environmentName/packages/:packageName/versions/:versionId/public";
   app.get(versionedRoot, (req, res, next) => {
      if (req.path.endsWith("/")) return next();
      res.redirect(
         308,
         withRequestQuery(
            req,
            versionedPublicUrl(req, req.params.versionId, ""),
         ),
      );
   });
   app.get(`${versionedRoot}/*`, (req, res) =>
      serveFromPackage(deps, req, res, req.params.versionId),
   );

   app.get(
      "/environments/:environmentName/packages/:packageName/*",
      (req, res) => serveFromPackage(deps, req, res),
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
   environmentStore: PackageLookup,
): void {
   app.get(
      `${apiPrefix}${DATA_APPS_ROUTE}`,
      apiRoute(async (req) => {
         const environment = await environmentStore.getEnvironment(
            parseEnvironmentName(req.params.environmentName),
            false,
         );
         const pkg = await environment.getPackage(
            parsePackageName(req.params.packageName),
            false,
            { versionId: versionRefOf(req) },
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

// Elements whose content is text, never markup.
const RAW_TEXT_ELEMENTS = [
   "script",
   "style",
   "title",
   "textarea",
   "noscript",
   "noembed",
   "noframes",
   "iframe",
   "xmp",
];
const RAW_TEXT_OPEN = new RegExp(
   `<(${RAW_TEXT_ELEMENTS.join("|")}|plaintext)(?=[\\s/>])`,
   "iy",
);
const RAW_TEXT_CLOSE = new Map(
   RAW_TEXT_ELEMENTS.map((name) => [
      name,
      new RegExp(`</${name}(?=[\\s/>])[^>]{0,1024}>`, "gi"),
   ]),
);

/** How far an open tag is followed to its closing `>`, quoted attribute values included. */
const MAX_TAG_BYTES = 2048;

/**
 * Where the text that is not markup, starting at the `<` at `at`, ends: an HTML
 * comment, or a script, style, title, textarea, noscript, noembed, noframes,
 * iframe or xmp element through its closing tag, or a plaintext element to the end. One that
 * is never closed runs to the end of `text`. Undefined when `at`
 * starts neither. A forward-only scan, so a caller that continues from the
 * returned index never reads a character twice.
 */
function nonTagTextEnd(text: string, at: number): number | undefined {
   if (text.startsWith("<!--", at)) {
      const close = text.indexOf("-->", at + 2);
      return close === -1 ? text.length : close + 3;
   }
   RAW_TEXT_OPEN.lastIndex = at;
   const open = RAW_TEXT_OPEN.exec(text);
   if (!open) return undefined;
   const closer = RAW_TEXT_CLOSE.get(open[1].toLowerCase());
   if (!closer) return text.length;
   closer.lastIndex = at + open[0].length;
   const close = closer.exec(text);
   return close ? close.index + close[0].length : text.length;
}

/**
 * Where the open tag at the `<` at `at` ends, with quoted attribute values
 * honored (one opens only right after `=`), so a `>` or `<` inside one is not
 * taken for markup. Undefined when `at` starts no tag. `closed` is false when
 * the scan gave up (no `>` or an unpaired quote within the bound): `end` is
 * then where it stopped, which a caller resumes from, so a run of such tags is
 * scanned once, not once per `<`.
 */
function openTagEnd(
   text: string,
   at: number,
): { end: number; closed: boolean } | undefined {
   if (!/[a-z]/i.test(text[at + 1] ?? "")) return undefined;
   const limit = Math.min(text.length, at + MAX_TAG_BYTES);
   const span = text.slice(at + 1, limit);
   // HTML whitespace only; `\s` would also take \v and \xA0.
   const stop = /=[\t\n\f\r ]*(["'])|>/g;
   // lastIndex only moves forward within the capped span, so this stays linear in it.
   for (let m = stop.exec(span); m; m = stop.exec(span)) {
      if (m[0] === ">") return { end: at + 1 + m.index + 1, closed: true };
      const close = span.indexOf(m[1], stop.lastIndex);
      if (close === -1) break;
      stop.lastIndex = close + 1;
   }
   return { end: limit, closed: false };
}

// `input` without the comments and raw-text elements, where a "<meta ...>" literal is not a live tag.
export function stripNonTagText(input: string): string {
   let kept = "";
   let copied = 0;
   for (let i = input.indexOf("<"); i !== -1; ) {
      const end = nonTagTextEnd(input, i);
      if (end === undefined) {
         const tag = openTagEnd(input, i);
         if (tag?.closed) {
            // Brackets inside attribute values are blanked, so they read as neither a tag nor a boundary.
            kept += `${input.slice(copied, i)}<${input.slice(i + 1, tag.end - 1).replace(/[<>]/g, " ")}>`;
            copied = tag.end;
         }
         i = input.indexOf("<", tag?.end ?? i + 1);
         continue;
      }
      kept += input.slice(copied, i);
      copied = end;
      i = input.indexOf("<", end);
   }
   return kept + input.slice(copied);
}

/**
 * Where a package's public/ really is, resolved by the package's own walk
 * against the package root (its parent): `outside` when public/ itself links
 * out of the package, to anything or to nothing, so its target is never
 * looked at; `none` when there is no public/ directory.
 */
async function servedPublic(
   publicRoot: string,
): Promise<
   { kind: "inside"; real: string } | { kind: "outside" } | { kind: "none" }
> {
   let real: string;
   try {
      real = await confinedPackagePath(
         path.dirname(publicRoot),
         pathToFileURL(publicRoot),
      );
   } catch {
      return { kind: "outside" };
   }
   const stat = await fsp.stat(real).catch(() => undefined);
   return stat?.isDirectory() ? { kind: "inside", real } : { kind: "none" };
}

/**
 * `file` resolved by the package's own walk, when it lands inside the real
 * public/ (`realPublic`); undefined for a link out of public/, whatever it
 * leads to.
 */
async function insidePublic(
   publicRoot: string,
   realPublic: string,
   file: string,
): Promise<string | undefined> {
   let real: string;
   try {
      real = await confinedPackagePath(
         path.dirname(publicRoot),
         pathToFileURL(file),
      );
   } catch {
      return undefined;
   }
   return stepsOutside(path.relative(realPublic, real)) ? undefined : real;
}

async function listPackageDataApps(
   environmentName: string,
   packageName: string,
   publicRoot: string,
   /** The published version listed from; its pages are pinned to it. */
   versionId?: string | null,
): Promise<DataAppItem[]> {
   const out: DataAppItem[] = [];

   // Same containment as serveFromPackage, applied before reading any file's
   // head, so a symlink out of public/ (or a public/ that links out) is never
   // opened, nor looked through. No public/ directory yields an empty list,
   // and so does one that links out of the package.
   const served = await servedPublic(publicRoot);
   if (served.kind !== "inside") return out;
   const realPublic = served.real;

   // `dir` is how a page is named; `readFrom` is where the walk resolved it,
   // which is what is read, so no link is followed again by its spelling.
   async function walk(dir: string, readFrom: string, depth: number) {
      if (depth > DATA_APPS_DEPTH_CAP) return;
      let entries: Dirent[];
      try {
         entries = await fsp.readdir(readFrom, { withFileTypes: true });
      } catch {
         return;
      }
      for (const entry of entries) {
         if (entry.name.startsWith(".") || entry.name === "node_modules")
            continue;
         const full = path.join(dir, entry.name);
         // Confined before anything is opened: a page outside public/ (or
         // the package) is never read for its title.
         const real = await insidePublic(publicRoot, realPublic, full);
         if (real === undefined) continue;
         if (entry.isDirectory()) {
            await walk(full, real, depth + 1);
         } else if (entry.isFile() && isHtmlFile(entry.name)) {
            const rel = path.relative(publicRoot, full).replace(/\\/g, "/");
            // Metadata comes from the first 4KB only.
            let title = rel;
            let fit: "viewport" | undefined;
            try {
               const fh = await fsp.open(real, "r");
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

   await walk(publicRoot, realPublic, 0);
   out.sort((a, b) => {
      if (a.path === "index.html") return -1;
      if (b.path === "index.html") return 1;
      return a.path.localeCompare(b.path);
   });
   return out;
}
