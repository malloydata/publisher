// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What the SPA catch-all should do with a request nothing else claimed.
 *
 * Unmatched paths get `index.html` so the SPA owns its client-side routes, but
 * a file request answered with the shell and a 200 reads as a working page. So
 * the shell is served only for paths that can plausibly be app routes:
 *
 *   - `/<env>/<pkg>/<file.ext>`: redirect to the static URL that serves it (a
 *     candidate the caller confirms against what is loaded).
 *   - anything else naming a file: 404, naming the path.
 *   - an unknown path under the API prefix: 404 as JSON.
 *
 * Deciding by extension keeps names containing a dot (`/examples/my.pkg`) app
 * routes; only the extensions below divert.
 */

/**
 * A closed list of what a package's `public/` directory serves; any other
 * extension is treated as an app route, so unusual package names keep working.
 * `.malloy` and `.malloynb` are absent because they are app routes (the
 * viewers). `yaml`, `xml`, `md` and the like fall through to the app, which
 * links to the static URL; `package-asset-urls.spec.ts` relies on `yaml` being
 * absent.
 */
const ASSET_EXTENSIONS = new Set([
   "avif",
   "css",
   "csv",
   "gif",
   "htm",
   "html",
   "ico",
   "jpeg",
   "jpg",
   "js",
   "json",
   "map",
   "mjs",
   "parquet",
   "pdf",
   "png",
   "svg",
   "ttf",
   "txt",
   "webp",
   "woff",
   "woff2",
   "xlsx",
]);

/**
 * Third segments the app owns, so `/<env>/<pkg>/<here>/...` reaches the SPA
 * even with an asset extension (`data-apps/<file>.html` is the embedded data
 * app viewer). Reports are not listed: the caller asks the package whether
 * `<reportPath>.malloy` is a served report (`docs/reports-design.md` §9).
 */
const SPA_OWNED_SEGMENTS = new Set(["data-apps"]);

export type SpaFallbackAction =
   /** Serve the app shell. */
   | { kind: "spa" }
   /**
    * A package file addressed on the app route. A candidate only: the caller
    * confirms both names are loaded, because `/assets/foo/bar.js` has this
    * shape too and the static route would echo a bad name back.
    */
   | {
        kind: "redirect";
        location: string;
        environmentName: string;
        packageName: string;
        /**
         * The path after the package name, still percent-encoded: the caller
         * serves the app shell instead when `<reportPath>.malloy` is a report
         * the package serves, since that is the report's own route.
         */
        reportPath: string;
     }
   /**
    * A file request nothing served. `path` is safe to echo back.
    *
    * `appRouteCandidate` is set for `/<env>` or `/<env>/<pkg>`, since a name may
    * end in a servable extension (`report.html` is a legal package name). The
    * caller turns it into `spa` only if those names are loaded, which keeps
    * `/<env>/style.css` a 404.
    */
   | {
        kind: "assetNotFound";
        path: string;
        appRouteCandidate: {
           environmentName: string;
           packageName?: string;
        } | null;
     }
   /** An unknown endpoint under the API prefix. */
   | { kind: "apiNotFound"; path: string };

/** Lowercased extension of the last path segment, or "" when it has none. */
function extensionOf(segment: string): string {
   const dot = segment.lastIndexOf(".");
   if (dot <= 0 || dot === segment.length - 1) return "";
   return segment.slice(dot + 1).toLowerCase();
}

/**
 * Decide what to do with `requestPath` (a URL path, still percent-encoded as
 * Express hands it over: `req.path`).
 *
 * @param requestPath the request's path, e.g. `/examples/storefront/index.html`
 * @param apiPrefix the API mount point, e.g. `/api/v0`
 */
export function classifySpaFallback(
   requestPath: string,
   apiPrefix: string,
): SpaFallbackAction {
   if (requestPath === apiPrefix || requestPath.startsWith(`${apiPrefix}/`)) {
      return { kind: "apiNotFound", path: requestPath };
   }

   const segments = requestPath.split("/").filter((s) => s.length > 0);
   const last = segments[segments.length - 1] ?? "";
   if (!ASSET_EXTENSIONS.has(extensionOf(last))) return { kind: "spa" };

   // Checked before the redirect so app-owned segments keep reaching the SPA.
   if (segments.length >= 3 && SPA_OWNED_SEGMENTS.has(segments[2])) {
      return { kind: "spa" };
   }

   // `/<env>/<pkg>/<file...>` maps to `/environments/<env>/packages/<pkg>/...`.
   if (segments.length >= 3) {
      // The static route answers its own 404s; this guard turns a future
      // route reordering into a 404 rather than a redirect loop.
      const alreadyStaticForm =
         segments[0] === "environments" && segments[2] === "packages";
      // A dot segment would be re-resolved by the client and walk the Location
      // out of the package prefix. Refused in every spelling: `.`/`..`, encoded
      // `%2e%2e`, and a backslash (browsers fold it to `/`). A malformed escape
      // is treated as suspect.
      const traverses = segments.some((segment) => {
         let decoded: string;
         try {
            decoded = decodeURIComponent(segment);
         } catch {
            return true;
         }
         return decoded === "." || decoded === ".." || decoded.includes("\\");
      });
      if (!alreadyStaticForm && !traverses) {
         const [environmentName, packageName, ...rest] = segments;
         const reportPath = rest.join("/");
         // A guessed `public/` segment is dropped: the static route already
         // joins onto `<pkg>/public`, so keeping it would 404.
         if (rest[0] === "public" && rest.length > 1) rest.shift();
         return {
            kind: "redirect",
            // Built from segments, so it always starts with `/environments/`:
            // it cannot become protocol-relative or point off-origin.
            location: `/environments/${environmentName}/packages/${packageName}/${rest.join(
               "/",
            )}`,
            environmentName,
            packageName,
            reportPath,
         };
      }
   }

   return {
      kind: "assetNotFound",
      path: requestPath,
      appRouteCandidate:
         segments.length === 1
            ? { environmentName: segments[0] }
            : segments.length === 2
              ? { environmentName: segments[0], packageName: segments[1] }
              : null,
   };
}

/** A percent-encoded path segment, decoded; a malformed escape compares as itself, a name that will not match. */
function decodeSegment(segment: string | undefined): string {
   if (segment === undefined) return "";
   try {
      return decodeURIComponent(segment);
   } catch {
      return segment;
   }
}

/**
 * What the fallback needs to know about what this server has loaded, by
 * decoded name. In-memory only: the fallback answers unauthenticated traffic
 * for any path nothing else claimed, so it must not do I/O a caller can trigger
 * by guessing.
 */
export interface LoadedRoutes {
   hasEnvironment(environmentName: string): boolean;
   hasPackage(environmentName: string, packageName: string): boolean;
   /**
    * Whether `routePath` (decoded, under the package) is the Console route of a
    * report the package serves: its path without `.malloy`, or a legacy alias.
    */
   isReportRoute(
      environmentName: string,
      packageName: string,
      routePath: string,
   ): boolean;
}

/**
 * {@link classifySpaFallback}, with its candidates settled against what is
 * loaded: a redirect candidate becomes the app shell when its path is a
 * report's route, a 404 when its package is not loaded, and otherwise stays a
 * redirect; an `appRouteCandidate` becomes the app shell only when its names
 * are loaded.
 */
export function resolveSpaFallback(
   requestPath: string,
   apiPrefix: string,
   loaded: LoadedRoutes,
): SpaFallbackAction {
   const fallback = classifySpaFallback(requestPath, apiPrefix);
   if (fallback.kind === "redirect") {
      const environmentName = decodeSegment(fallback.environmentName);
      const packageName = decodeSegment(fallback.packageName);
      const routePath = fallback.reportPath
         .split("/")
         .map(decodeSegment)
         .join("/");
      if (!loaded.hasPackage(environmentName, packageName)) {
         return {
            kind: "assetNotFound",
            path: requestPath,
            appRouteCandidate: null,
         };
      }
      return loaded.isReportRoute(environmentName, packageName, routePath)
         ? { kind: "spa" }
         : fallback;
   }
   if (fallback.kind === "assetNotFound" && fallback.appRouteCandidate) {
      const environmentName = decodeSegment(
         fallback.appRouteCandidate.environmentName,
      );
      const packageName = fallback.appRouteCandidate.packageName;
      const isAppRoute =
         loaded.hasEnvironment(environmentName) &&
         (packageName === undefined ||
            loaded.hasPackage(environmentName, decodeSegment(packageName)));
      if (isAppRoute) return { kind: "spa" };
   }
   return fallback;
}
