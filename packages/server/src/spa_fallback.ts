/**
 * What the SPA catch-all should do with a request nothing else claimed.
 *
 * The catch-all exists so the single-page app can own its client-side routes:
 * any unmatched path returns `index.html` and React Router decides. The cost is
 * that a request for a *file* also gets `index.html`, with HTTP 200, and a 200
 * carrying HTML is indistinguishable from a working page. That has now misled a
 * reader three times: an unknown `/api/v0/...` answering 200 plus HTML, a
 * `/public/` segment that looked served, and `/<env>/<pkg>/index.html`, which
 * renders the app shell and then reports "Unrecognized file type: index.html",
 * a confident error about the wrong thing, since the path was what was wrong.
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
 * `webmanifest`. Those fall through to the app, which now names the path and
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
 * embedded data app viewer, `workbook/...` is the workbook route, and
 * `dashboards/<slug>` is the dashboard viewer.
 *
 * `dashboards` is here because a slug is a FILENAME with its `.malloy` suffix
 * removed, so `dashboards/report.csv.malloy` publishes the slug `report.csv`,
 * which ends in an asset extension through no choice of the reader's. Without
 * this entry that dashboard is listed on the package page, served by the API,
 * and reachable by in-app navigation, but a deep link or a refresh 302s to the
 * static route and answers 404. Measured before adding it.
 *
 * The cost, which `pages` pays too and is worth stating for this one: a package
 * that ships a `public/dashboards/` directory can no longer address those files
 * as `/<env>/<pkg>/dashboards/<file>`, since that shape now opens the dashboard
 * viewer, which answers "no such dashboard". `data-apps` has a viewer one
 * segment down that catches its own collision; this route has no equivalent, so
 * those files are reachable only on the static URL
 * `/environments/<env>/packages/<pkg>/dashboards/<file>`.
 *
 * `pages` is the OLD spelling of `data-apps`, kept only so an existing bookmark
 * reaches the app and the app can redirect it to the new URL. Without it here the
 * old link is diverted to the static route and 404s, which is a worse answer than
 * a 404 sounds: for a package that ships a `public/pages/` directory it resolves
 * to a real but different document and answers 200.
 *
 * DEPRECATED. Remove one release after the release that ships the rename,
 * together with the redirect in ModelPage.tsx that depends on it. The newest tag
 * when this was written was v0.0.240.
 */
const SPA_OWNED_SEGMENTS = new Set([
   "dashboards",
   "data-apps",
   "pages",
   "workbook",
]);

export type SpaFallbackAction =
   /** Serve the app shell, as before. */
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
