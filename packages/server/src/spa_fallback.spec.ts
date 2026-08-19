import { describe, expect, it } from "bun:test";

import { classifySpaFallback } from "./spa_fallback";

const API_PREFIX = "/api/v0";
const classify = (path: string) => classifySpaFallback(path, API_PREFIX);

/**
 * The catch-all's whole risk is over-reach: every real app route it diverts is a
 * page of the product that stops loading. So the app-route cases below matter
 * more than the ones this change exists to fix, and they are asserted first.
 */
describe("classifySpaFallback", () => {
   describe("keeps serving the app shell for app routes", () => {
      for (const path of [
         "/",
         "/examples",
         "/examples/storefront",
         "/examples/storefront/materializations",
         "/settings",
         "/settings/theme",
      ]) {
         it(`${path} is an app route`, () => {
            expect(classify(path)).toEqual({ kind: "spa" });
         });
      }

      it("keeps model and notebook URLs, the most-used routes in the product", () => {
         expect(classify("/examples/storefront/storefront.malloy")).toEqual({
            kind: "spa",
         });
         expect(classify("/examples/storefront/storefront.malloynb")).toEqual({
            kind: "spa",
         });
         expect(
            classify("/examples/storefront/nested/dir/model.malloy"),
         ).toEqual({ kind: "spa" });
      });

      it("keeps the in-app data app viewer, which addresses an .html on purpose", () => {
         // The Data Apps list in the package view routes clicks to
         // `data-apps/<file>`, and ModelPage iframes it. Diverting this would
         // break that list.
         expect(classify("/examples/storefront/data-apps/index.html")).toEqual({
            kind: "spa",
         });
      });

      it("keeps a dashboard whose slug ends in an asset extension", () => {
         // A slug is a FILENAME with `.malloy` removed, so
         // `dashboards/report.csv.malloy` publishes the slug `report.csv`
         // through no choice of the reader's. Measured before this entry
         // existed: that dashboard was listed on the package page, served by
         // the API, and reachable by in-app navigation, while a deep link or a
         // refresh 302d to the static route and answered 404.
         expect(classify("/examples/storefront/dashboards/report.csv")).toEqual(
            {
               kind: "spa",
            },
         );
         // The ordinary shape reaches the app without needing this set at all,
         // asserted so a regression that removes the entry is distinguishable
         // from one that breaks the whole route.
         expect(classify("/examples/storefront/dashboards/overview")).toEqual({
            kind: "spa",
         });
      });

      it("still claims the OLD `pages` segment, which the app redirects", () => {
         // Kept deliberately, and load-bearing for the deprecation: an old
         // bookmark has to REACH the app, because the app is what rewrites it to
         // `data-apps/<file>`. Divert it here and the old link goes to the static
         // route instead, which 404s, or worse answers 200 with a different
         // document for a package that ships a `public/pages/` directory.
         //
         // Asserted on the old spelling on purpose: the cases above were
         // rewritten from `pages` to `data-apps` rather than supplemented, so
         // without this nothing pins old-URL behaviour in either direction.
         // Delete alongside the redirect in ModelPage.tsx, not before it.
         expect(classify("/examples/storefront/pages/index.html")).toEqual({
            kind: "spa",
         });
         // No asset extension, so this reaches the app by the ordinary route
         // rather than via SPA_OWNED_SEGMENTS. Both shapes have to arrive for
         // the redirect to cover every old link, so both are asserted.
         expect(classify("/examples/storefront/pages/report")).toEqual({
            kind: "spa",
         });
      });

      it("keeps a workbook route whose name looks like a file", () => {
         expect(
            classify("/examples/storefront/workbook/ws/scratch.malloynb"),
         ).toEqual({ kind: "spa" });
      });

      it("keeps a route whose environment or package name contains a dot", () => {
         // Deciding by "does it have a dot" would 404 these. Deciding by a
         // closed list of web extensions leaves them alone.
         expect(classify("/examples/my.pkg")).toEqual({ kind: "spa" });
         expect(classify("/my.env/my.pkg")).toEqual({ kind: "spa" });
         expect(classify("/examples/v1.2.3")).toEqual({ kind: "spa" });
      });

      it("keeps a trailing dot or a dotfile, neither of which names a type", () => {
         expect(classify("/examples/storefront.")).toEqual({ kind: "spa" });
         expect(classify("/.well-known")).toEqual({ kind: "spa" });
      });
   });

   describe("sends a package file to the route that serves it", () => {
      it("redirects the URL that Finding C was reported against", () => {
         expect(classify("/examples/storefront/index.html")).toEqual({
            kind: "redirect",
            location: "/environments/examples/packages/storefront/index.html",
            environmentName: "examples",
            packageName: "storefront",
         });
      });

      it("keeps a nested asset path intact", () => {
         expect(classify("/examples/storefront/assets/app.js")).toEqual({
            kind: "redirect",
            location:
               "/environments/examples/packages/storefront/assets/app.js",
            environmentName: "examples",
            packageName: "storefront",
         });
      });

      it("drops a guessed `public/` segment so the redirect reaches the file", () => {
         // The near-miss from the 07-28 run: a reader who knows the files live
         // in `public/` guesses that segment. The static route does NOT strip
         // it (it joins the request path onto `<pkg>/public`, so passing it
         // through resolves to `public/public/...` and 404s), so the redirect
         // has to drop it. Asserted end to end in package-asset-urls.spec.ts.
         expect(classify("/examples/storefront/public/index.html")).toEqual({
            kind: "redirect",
            location: "/environments/examples/packages/storefront/index.html",
            environmentName: "examples",
            packageName: "storefront",
         });
      });

      it("keeps a `public` file name, which is not the guessed segment", () => {
         // Only a leading `public/` with something after it is dropped.
         expect(classify("/examples/storefront/public.html")).toEqual({
            kind: "redirect",
            location: "/environments/examples/packages/storefront/public.html",
            environmentName: "examples",
            packageName: "storefront",
         });
      });

      it("does not redirect the static form onto itself", () => {
         // Unreachable today (that route answers its own 404s), so this pins the
         // guard: if it ever does reach here, it must not loop.
         expect(
            classify("/environments/examples/packages/storefront/index.html"),
         ).toEqual({
            kind: "assetNotFound",
            path: "/environments/examples/packages/storefront/index.html",
            appRouteCandidate: null,
         });
      });

      it("does not redirect a path with . or .., which the client re-resolves", () => {
         // `/a/b/../c/x.html` would normalize against the new prefix and land on
         // a different package than the caller typed.
         expect(classify("/examples/storefront/../other/index.html").kind).toBe(
            "assetNotFound",
         );
         expect(classify("/examples/storefront/./index.html").kind).toBe(
            "assetNotFound",
         );
      });
   });

   describe("answers a file request that nothing serves", () => {
      it("404s a two-segment asset path, with the path it was asked for", () => {
         // Short paths hand their names back: the caller checks whether they are
         // really a loaded environment and package, because a package may legally
         // be named `report.html`. Neither of these is, so both stay 404s.
         expect(classify("/favicon-missing.ico")).toEqual({
            kind: "assetNotFound",
            path: "/favicon-missing.ico",
            appRouteCandidate: { environmentName: "favicon-missing.ico" },
         });
         expect(classify("/assets/deleted-chunk.js")).toEqual({
            kind: "assetNotFound",
            path: "/assets/deleted-chunk.js",
            appRouteCandidate: {
               environmentName: "assets",
               packageName: "deleted-chunk.js",
            },
         });
      });

      it("leaves an app-owned segment to the app, traversal and all", () => {
         // `data-apps/...` is the in-app viewer's route, so it is the app's to
         // answer; the redirect branch is the only place traversal matters.
         expect(
            classify("/examples/storefront/data-apps/../secret.html").kind,
         ).toBe("spa");
      });
   });

   describe("cases the first version of this got wrong", () => {
      it("keeps a workbook path whose name ends in a servable extension", () => {
         // Workbook names are arbitrary user-typed keys, so `q1.csv` is legal and
         // is exactly what SPA_OWNED_SEGMENTS is for. The `.malloynb` case does
         // not exercise it, because that extension is not in the list at all, so
         // this is the assertion that actually pins `workbook`.
         expect(classify("/examples/storefront/workbook/ws/q1.csv")).toEqual({
            kind: "spa",
         });
      });

      it("flags a two-segment path so a package named like a file survives", () => {
         // `report.html` is a legal package name. Classified as an asset by
         // shape, but with BOTH names handed back: the caller serves the app only
         // if the environment is loaded AND that package really exists, which is
         // what keeps `/examples/style.css` a 404 rather than an app shell.
         expect(classify("/examples/report.html")).toEqual({
            kind: "assetNotFound",
            path: "/examples/report.html",
            appRouteCandidate: {
               environmentName: "examples",
               packageName: "report.html",
            },
         });
      });

      it("does not redirect percent-encoded dot segments either", () => {
         // A URL parser treats %2e%2e as a dot segment while resolving the
         // Location, so it walks out of the package prefix the redirect pins.
         expect(classify("/examples/storefront/%2e%2e/%2e%2e/x.png").kind).toBe(
            "assetNotFound",
         );
      });

      it("refuses a segment with a malformed escape rather than guessing", () => {
         expect(classify("/examples/storefront/%zz/x.png").kind).toBe(
            "assetNotFound",
         );
      });

      it("does not redirect a path with a backslash, which browsers fold to /", () => {
         // Same hazard as `..` in another spelling: the client normalises `\` to
         // `/` and re-resolves the Location somewhere other than intended.
         expect(classify("/examples/storefront/\\..\\x.png").kind).toBe(
            "assetNotFound",
         );
      });
   });

   describe("answers an unknown API endpoint as JSON", () => {
      it("classifies anything under the API prefix, extension or not", () => {
         expect(classify("/api/v0/bogus-endpoint")).toEqual({
            kind: "apiNotFound",
            path: "/api/v0/bogus-endpoint",
         });
         expect(classify("/api/v0/environments/x/nope.json")).toEqual({
            kind: "apiNotFound",
            path: "/api/v0/environments/x/nope.json",
         });
         expect(classify("/api/v0")).toEqual({
            kind: "apiNotFound",
            path: "/api/v0",
         });
      });

      it("leaves the served OpenAPI spec alone: it is not under the prefix", () => {
         // `/api-doc.yaml` and `/api-doc.html` are static files off the root,
         // and a prefix test that used `/api` would have swallowed both.
         expect(classify("/api-doc.yaml").kind).not.toBe("apiNotFound");
         expect(classify("/api-doc.html").kind).not.toBe("apiNotFound");
      });
   });
});
