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

      it("keeps the in-app page viewer, which addresses an .html on purpose", () => {
         // The Pages list in the package view routes clicks to `pages/<file>`,
         // and ModelPage iframes it. Diverting this would break that list.
         expect(classify("/examples/storefront/pages/index.html")).toEqual({
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
         });
      });

      it("keeps a nested asset path intact", () => {
         expect(classify("/examples/storefront/assets/app.js")).toEqual({
            kind: "redirect",
            location:
               "/environments/examples/packages/storefront/assets/app.js",
         });
      });

      it("redirects the `/public/` guess too, which was the near-miss", () => {
         // A reader who knows the file lives in `public/` guesses that segment.
         // The static route strips it, so the redirect target is what serves it.
         expect(classify("/examples/storefront/public/index.html")).toEqual({
            kind: "redirect",
            location:
               "/environments/examples/packages/storefront/public/index.html",
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
         expect(classify("/favicon-missing.ico")).toEqual({
            kind: "assetNotFound",
            path: "/favicon-missing.ico",
         });
         expect(classify("/assets/deleted-chunk.js")).toEqual({
            kind: "assetNotFound",
            path: "/assets/deleted-chunk.js",
         });
      });

      it("leaves an app-owned segment to the app, traversal and all", () => {
         // `pages/...` is the in-app viewer's route, so it is the app's to
         // answer; the redirect branch is the only place traversal matters.
         expect(
            classify("/examples/storefront/pages/../secret.html").kind,
         ).toBe("spa");
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
