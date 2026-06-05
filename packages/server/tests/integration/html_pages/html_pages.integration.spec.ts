/// <reference types="bun-types" />

/**
 * E2E coverage for in-package HTML data apps:
 *   - static-file serving (`serveFromPackage`) from the package's public/
 *     directory only, with realpath containment and HTML-only CSP framing,
 *   - the `/pages` list endpoint (bare `Page[]`, the house list shape),
 *   - the `/events` SSE stream and its input validation.
 *
 * These routes touch the live filesystem and carry the security-relevant
 * branches (403 containment, 404 for files outside public/, 400 name
 * validation), so they're exercised against the real Express app.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ENV_NAME = "html-pages-test-env";
const PACKAGE_NAME = "html-pages-test";
// A second package in the same env that ships NO public/ directory, to pin the
// "package without public/" behavior (file requests 404, /pages returns []).
const NOPUBLIC_PACKAGE = "html-pages-nopublic";

const fixtureDir = path.resolve(__dirname, "../../fixtures/html-pages-test");
const nopublicFixtureDir = path.resolve(
   __dirname,
   "../../fixtures/html-pages-nopublic",
);
// The "malicious package" escape class: a symlink inside the served public/
// directory that points outside it. We plant it in the *served* copy under
// publisher_data AFTER the package mounts — never in the source fixture — so it
// never gets routed through env-creation's `fs.cp`. (Copying an absolute-target
// symlink is not portable across platforms and was breaking CI on Linux.) The
// package is served from a copy (no --watch-env), at <SERVER_ROOT>/
// publisher_data/<env>/<pkg>/public; SERVER_ROOT defaults to the server package
// dir under `bun test`.
const serverPkgRoot = path.resolve(__dirname, "../../..");
const servedEscapeLink = path.join(
   serverPkgRoot,
   "publisher_data",
   ENV_NAME,
   PACKAGE_NAME,
   "public",
   "escape.html",
);
// A second planted symlink: the realistic "escape public/" vector, a link
// inside public/ pointing at a package-root sibling (../report.malloy). It must
// be rejected (403) just like the absolute /etc/hosts escape above.
const servedSiblingLink = path.join(
   serverPkgRoot,
   "publisher_data",
   ENV_NAME,
   PACKAGE_NAME,
   "public",
   "leak.html",
);

interface PageItem {
   resource?: string;
   packageName?: string;
   path?: string;
   title?: string;
}

// Creating a symlink that escapes the package needs privileges the Windows CI
// runner lacks (SeCreateSymbolicLinkPrivilege), and the escape target
// (/etc/hosts) is Unix-only — so the one symlink-escape case is skipped on
// Windows (see `itEscape` below). The rest of the suite (serving, manifest
// blocking, 404s, /pages) runs on every platform and is the valuable Windows
// coverage of serveFromPackage's path handling (separators, drive letters,
// case-insensitive manifest match, realpath containment).
const isWindows = process.platform === "win32";
const itEscape = isWindows ? it.skip : it;

describe("In-package HTML data apps (E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

   const pkgUrl = (sub: string) =>
      `${baseUrl}/environments/${ENV_NAME}/packages/${PACKAGE_NAME}${sub}`;
   const apiUrl = (sub: string) =>
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PACKAGE_NAME}${sub}`;

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;

      const createRes = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [
               { name: PACKAGE_NAME, location: fixtureDir },
               { name: NOPUBLIC_PACKAGE, location: nopublicFixtureDir },
            ],
            connections: [],
         }),
      });
      if (!createRes.ok) {
         const body = await createRes.text();
         throw new Error(
            `Failed to create test environment (${createRes.status}): ${body}`,
         );
      }

      const waitForPackage = async (pkg: string) => {
         const deadline = Date.now() + 30_000;
         while (Date.now() < deadline) {
            try {
               const res = await fetch(
                  `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${pkg}`,
               );
               if (res.ok) return;
            } catch {
               // not ready yet
            }
            await new Promise((r) => setTimeout(r, 500));
         }
         throw new Error(`Package ${pkg} did not become available in time`);
      };
      await waitForPackage(PACKAGE_NAME);
      await waitForPackage(NOPUBLIC_PACKAGE);

      // Now that the package is mounted, plant the escape symlinks directly in
      // the served public/ copy (post-fs.cp): one pointing fully outside the
      // package (/etc/hosts) and one at a package-root sibling (../report.malloy),
      // the realistic "escape public/" vector. Unlink any stale link first, then
      // create fresh WITHOUT swallowing errors, so a failed plant fails the suite
      // loudly instead of silently skipping the security-critical assertions.
      // Skipped on Windows, where the matching tests (itEscape) are skipped too.
      if (!isWindows) {
         for (const link of [servedEscapeLink, servedSiblingLink]) {
            try {
               fs.unlinkSync(link);
            } catch {
               // no stale link to remove
            }
         }
         fs.symlinkSync("/etc/hosts", servedEscapeLink);
         fs.symlinkSync("../report.malloy", servedSiblingLink);
      }
   });

   afterAll(async () => {
      // Always tear down the env so a partially-set-up run can't leave residue
      // in the shared EnvironmentStore for later test files in this process.
      if (baseUrl) {
         try {
            await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
               method: "DELETE",
            });
         } catch {
            // best-effort
         }
      }
      // DELETE removes the served dir (and the symlink within it); this is a
      // belt-and-suspenders unlink in case the env was never created.
      for (const link of [servedEscapeLink, servedSiblingLink]) {
         try {
            fs.unlinkSync(link);
         } catch {
            // best-effort
         }
      }
      await env?.stop();
      env = null;
   });

   // ── static-file serving ──────────────────────────────────────────

   it("serves index.html at the package root (directory index)", async () => {
      const res = await fetch(pkgUrl("/"));
      expect(res.status).toBe(200);
      const body = await res.text();
      expect(body).toContain("Hello from the in-package data app");
   });

   it("308-redirects the package root (no trailing slash) to the canonical path", async () => {
      // The redirect target is rebuilt from the route params + parsed query
      // (canonical, same-origin), not the raw request URL.
      const res = await fetch(
         `${baseUrl}/environments/${ENV_NAME}/packages/${PACKAGE_NAME}`,
         { redirect: "manual" },
      );
      expect(res.status).toBe(308);
      expect(res.headers.get("location")).toBe(
         `/environments/${ENV_NAME}/packages/${PACKAGE_NAME}/`,
      );
      // The query string is preserved, placed before the appended slash.
      const withQuery = await fetch(
         `${baseUrl}/environments/${ENV_NAME}/packages/${PACKAGE_NAME}?embed_token=abc`,
         { redirect: "manual" },
      );
      expect(withQuery.status).toBe(308);
      expect(withQuery.headers.get("location")).toBe(
         `/environments/${ENV_NAME}/packages/${PACKAGE_NAME}/?embed_token=abc`,
      );
   });

   it("sets frame-ancestors CSP on HTML responses and clears X-Frame-Options", async () => {
      const res = await fetch(pkgUrl("/index.html"));
      expect(res.status).toBe(200);
      expect(res.headers.get("content-security-policy")).toBe(
         "frame-ancestors *",
      );
      expect(res.headers.get("x-frame-options")).toBeNull();
      expect(res.headers.get("x-content-type-options")).toBe("nosniff");
   });

   it("does NOT set the framing CSP on non-HTML assets", async () => {
      const res = await fetch(pkgUrl("/assets/app.css"));
      expect(res.status).toBe(200);
      // CSP framing is only meaningful on documents; assets keep their default.
      expect(res.headers.get("content-security-policy")).toBeNull();
      expect(res.headers.get("x-content-type-options")).toBe("nosniff");
   });

   it("404s a missing file", async () => {
      const res = await fetch(pkgUrl("/does-not-exist.html"));
      expect(res.status).toBe(404);
   });

   it("serves a page from a subdirectory", async () => {
      const res = await fetch(pkgUrl("/sub/page2.html"));
      expect(res.status).toBe(200);
      expect(await res.text()).toContain("A page in a subdirectory");
   });

   it("serves only files under public/; package internals are never served", async () => {
      // The manifest, models, and data live at the package root, outside
      // public/. Each exists in the fixture (asserted), so the 404 proves the
      // public/ boundary blocked it, not a missing file. This is what keeps
      // raw data, model source, and secrets off the static route and behind
      // the per-model #(authorize) and query controls.
      const blocked = ["publisher.json", "report.malloy", "data.csv"];
      for (const name of blocked) {
         expect(fs.existsSync(path.join(fixtureDir, name))).toBe(true);
         const res = await fetch(pkgUrl(`/${name}`));
         expect(res.status).toBe(404);
      }
   });

   it("serves any file type placed under public/ (no extension filter)", async () => {
      // public/ is the boundary, not a file-extension allowlist: a data-typed
      // file the author deliberately put under public/ is served. (Raw data at
      // the package root is still never served, per the test above.)
      const res = await fetch(pkgUrl("/data.json"));
      expect(res.status).toBe(200);
   });

   it("404s file requests for a package with no public/ directory", async () => {
      const res = await fetch(
         `${baseUrl}/environments/${ENV_NAME}/packages/${NOPUBLIC_PACKAGE}/index.html`,
      );
      expect(res.status).toBe(404);
   });

   it("lists no pages for a package with no public/ directory", async () => {
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${NOPUBLIC_PACKAGE}/pages`,
      );
      expect(res.status).toBe(200);
      expect(await res.json()).toEqual([]);
   });

   it("rejects URL-encoded path traversal out of public/", async () => {
      // Pre-encoded so the segments aren't normalized away before reaching the
      // server. Whatever the rejection mode (safeJoinUnderRoot 400, realpath
      // containment 403, or normalize-then-missing 404), package internals must
      // never be served (never 200).
      const encoded = [
         "..%2f..%2freport.malloy",
         "%2e%2e%2f%2e%2e%2fpublisher.json",
         "..%2f..%2fdata.csv",
      ];
      for (const p of encoded) {
         const res = await fetch(
            `${baseUrl}/environments/${ENV_NAME}/packages/${PACKAGE_NAME}/${p}`,
         );
         expect([400, 403, 404]).toContain(res.status);
      }
   });

   itEscape(
      "rejects a symlink in public/ that escapes the package with 403",
      async () => {
         // Precondition: the plant succeeded, so a 403 means realpath
         // containment caught the escape, not a missing-file 404.
         expect(fs.lstatSync(servedEscapeLink).isSymbolicLink()).toBe(true);
         const res = await fetch(pkgUrl("/escape.html"));
         expect(res.status).toBe(403);
      },
   );

   itEscape(
      "rejects a symlink from public/ to a package-root sibling with 403",
      async () => {
         // The realistic escape: public/leak.html -> ../report.malloy reaches a
         // file at the package root, outside public/. Must be 403, not served.
         expect(fs.lstatSync(servedSiblingLink).isSymbolicLink()).toBe(true);
         const res = await fetch(pkgUrl("/leak.html"));
         expect(res.status).toBe(403);
      },
   );

   // ── /pages list endpoint ─────────────────────────────────────────

   it("lists pages as a bare Page[] (not a {pages} envelope)", async () => {
      const res = await fetch(apiUrl("/pages"));
      expect(res.status).toBe(200);
      const body = (await res.json()) as unknown;
      expect(Array.isArray(body)).toBe(true);

      const pages = body as PageItem[];
      const paths = pages.map((p) => p.path).sort();
      // Only HTML files under public/ are listed; the toEqual pins the exact
      // set, so non-public files (manifest, models, data) can't appear.
      expect(paths).toEqual(["index.html", "sub/page2.html"]);

      const index = pages.find((p) => p.path === "index.html");
      expect(index?.title).toBe("Carrier Dashboard");
      expect(index?.packageName).toBe(PACKAGE_NAME);
      expect(index?.resource).toBe(
         `/environments/${ENV_NAME}/packages/${PACKAGE_NAME}/index.html`,
      );
   });

   it("400s a malformed environment/package name on /pages", async () => {
      // getEnvironment runs assertSafePackageName, so a name outside
      // IdentifierPattern is a 400 (now documented on list-pages in api-doc).
      const res = await fetch(
         `${baseUrl}/api/v0/environments/bad%20name/packages/${PACKAGE_NAME}/pages`,
      );
      expect(res.status).toBe(400);
   });

   // ── /events SSE stream ───────────────────────────────────────────

   it("400s an illegal environment/package name on /events", async () => {
      // A space is outside IdentifierPattern → assertSafePackageName rejects.
      const res = await fetch(
         `${baseUrl}/api/v0/environments/bad%20name/packages/${PACKAGE_NAME}/events`,
      );
      expect(res.status).toBe(400);
   });

   it("404s an unknown package on /events", async () => {
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/no-such-pkg/events`,
      );
      expect(res.status).toBe(404);
   });

   it("opens an SSE stream announcing hello + mode", async () => {
      const controller = new AbortController();
      const res = await fetch(apiUrl("/events"), {
         signal: controller.signal,
      });
      expect(res.status).toBe(200);
      expect(res.headers.get("content-type")).toContain("text/event-stream");

      const reader = res.body!.getReader();
      const { value } = await reader.read();
      const chunk = new TextDecoder().decode(value);
      expect(chunk).toContain("event: hello");
      expect(chunk).toContain("event: mode");

      await reader.cancel();
      controller.abort();
   });
});
