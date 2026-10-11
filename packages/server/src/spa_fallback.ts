// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What the SPA catch-all should do with a request nothing else claimed.
 *
 * The catch-all exists so the single-page app can own its client-side routes:
 * any unmatched path returns `index.html` and React Router decides. The cost is
 * that a request for a *file* also gets `index.html`, with HTTP 200, and a 200
 * carrying HTML is indistinguishable from a working page: an unknown
 * `/api/v0/...` answering 200 plus HTML, or `/<env>/<pkg>/index.html` rendering
 * the app shell and then "Unrecognized file type: index.html", a confident error
 * about the wrong thing, since the path was what was wrong.
 *
 * So the shell is served only for paths that can plausibly BE app routes. A file
 * request gets a real answer instead:
 *
 *   - `/<env>/<pkg>/<file.ext>` is the package's own static file addressed on
 *     the app's route instead of the static route, which is the mistake worth
 *     fixing rather than merely reporting: redirect to the URL that serves it.
 *     A `redirect` is a CANDIDATE, not a verdict: the caller checks that the
 *     environment exists first, because `/assets/foo/bar.js` matches this shape
 *     too and redirecting it produces a worse answer than the 404 below.
 *   - anything else that names a file: 404, saying which path was requested.
 *   - an unknown path under the API prefix: 404 as JSON, since a client that
 *     asked the API for something expects JSON either way.
 *
 * Deciding by extension rather than by "did anything match" is what keeps this
 * from eating real routes: an environment or package whose NAME contains a dot
 * (`/examples/my.pkg`) is still an app route, because `.pkg` is not something
 * the web serves. Only the extensions below divert.
 */

/**
 * Extensions that mean "this is a file, not a view". Deliberately a closed list
 * of what a package's `public/` directory actually serves: a name outside it is
 * treated as an app route, so an unusual package name keeps working.
 *
 * `.malloy` and `.malloynb` are absent on purpose, because those ARE app routes (the
 * model and notebook viewers), and diverting them would break the product's
 * most-used URLs.
 *
 * Also absent, and not an oversight: `yaml`/`yml`, `xml`, `md`, `wasm`,
 * `webmanifest`. Those fall through to the app, which names the path and
 * links to the static URL, so the outcome is already reasonable and the list
 * stays short. `package-asset-urls.spec.ts` depends on `yaml` being absent to
 * exercise that app-side page, so adding it here breaks that test rather than
 * completing anything.
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
 * Third segments that belong to the app rather than to a package's `public/`
 * directory, so `/<env>/<pkg>/<here>/...` must keep reaching the SPA even when
 * the path ends in an asset extension. `data-apps/<file>.html` is the in-app
 * embedded data app viewer.
 *
 * A report is not a segment: its route is its path without `.malloy`
 * (`docs/reports-design.md` §9), wherever it lives, so the caller asks the
 * loaded package whether `<path>.malloy` is a served report (see `reportPath`
 * on the redirect candidate). That keeps a report whose name ends in an asset
 * extension (`sales/report.csv.malloy`) in the app, and a package's own
 * `public/dashboards/<file>` reachable. The deprecated `dashboards/<name>` and
 * `notebooks/<name>` alias links stay in the app only when a report is named
 * `<name>`.
 */
const SPA_OWNED_SEGMENTS = new Set(["data-apps"]);

export type SpaFallbackAction =
   /** Serve the app shell. */
   | { kind: "spa" }
   /**
    * A package file addressed on the app route. A CANDIDATE only: the caller
    * confirms both names against what it has loaded before redirecting, because
    * `/assets/foo/bar.js` has this shape too and the static route would answer a
    * bad name with an internal-error JSON that echoes it back.
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
    * `appRouteCandidate` is set when the path is short enough to be an app route
    * (`/<env>` or `/<env>/<pkg>`) whose NAME merely happens to end in a servable
    * extension, which names may: a package called `report.html` is legal. The
    * caller turns it back into `spa` only if those names really are things it has
    * loaded. Requiring the package too is what keeps `/<env>/style.css` a 404
    * rather than an app shell with a 200, which is the defect this module exists
    * to remove.
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

   // Names a file from here on, except where the app owns the route anyway, as
   // `data-apps/<file>.html` does. Checked before the redirect so those keep
   // reaching the SPA rather than falling through to a 404.
   if (segments.length >= 3 && SPA_OWNED_SEGMENTS.has(segments[2])) {
      return { kind: "spa" };
   }

   // A package's files live under `/environments/<env>/packages/<pkg>/...`, and
   // the app's equivalent route is `/<env>/<pkg>/...`. Three segments or more,
   // so there is a file path after the two names.
   if (segments.length >= 3) {
      // Already the static form: that route owns its prefix and answers 404
      // itself, so it never reaches here. Guarding anyway means a future
      // reordering degrades to a 404 rather than to a redirect loop.
      const alreadyStaticForm =
         segments[0] === "environments" && segments[2] === "packages";
      // A dot segment gets re-resolved by the client against the new prefix,
      // pointing the redirect somewhere other than the package that was typed.
      // Three spellings, all refused: the literal `.`/`..`; `%2e%2e`, which a URL
      // parser treats as a dot segment while resolving the Location, so it walks
      // out of the `/environments/<env>/packages/<pkg>/` prefix this redirect
      // exists to pin; and a backslash, which browsers fold to `/` first. Decode
      // before comparing, and treat a malformed escape as suspect rather than
      // assuming it is inert.
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
         // A reader who knows the files live in `public/` guesses that segment,
         // but the static route joins the request path onto `<pkg>/public`, so
         // passing it through would resolve to `public/public/<file>` and 404.
         // Dropping it is what makes the guess reach the file. A package that
         // really ships `public/public/<file>` is still reachable by asking for
         // it on the static route directly.
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

   // One or two segments can still be `/<env>` or `/<env>/<pkg>`, and a name may
   // legally end in a servable extension. Hand both names back so the caller can
   // tell "a package called report.html" from "a missing asset".
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
 * loaded:
 *
 * - A redirect candidate is the app shell when its path is a report's route (a
 *   report whose name ends in an asset extension is a route, not a file); a
 *   404 when its environment or package is not loaded, since `/assets/foo/bar.js`
 *   has the same shape and the static route would answer a bad name with JSON
 *   that echoes it back; otherwise the redirect.
 * - An `/<env>` or `/<env>/<pkg>` whose name merely ends in a servable
 *   extension (`report.html` is a legal package name) is the app shell when
 *   those names are loaded, and stays a 404 otherwise, which is what keeps
 *   `/examples/style.css` a 404.
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
         // Null, not the names: a candidate whose names did not resolve is
         // not an app route either.
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
