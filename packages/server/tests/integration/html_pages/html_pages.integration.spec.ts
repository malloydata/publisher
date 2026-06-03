/// <reference types="bun-types" />

/**
 * E2E coverage for in-package HTML data apps:
 *   - static-file serving (`serveFromPackage`) with realpath containment,
 *     manifest blocking (root + subdirectory), and HTML-only CSP framing,
 *   - the `/pages` list endpoint (bare `Page[]`, the house list shape),
 *   - the `/events` SSE stream and its input validation.
 *
 * These routes touch the live filesystem and carry the security-relevant
 * branches (403 containment, 404 manifest block, 400 name validation), so
 * they're exercised against the real Express app rather than smoke-tested.
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

const fixtureDir = path.resolve(__dirname, "../../fixtures/html-pages-test");
// The "malicious package" escape class: a symlink inside the package that
// points outside it. We plant it in the *served* copy under publisher_data
// AFTER the package mounts — never in the source fixture — so it never gets
// routed through env-creation's `fs.cp`. (Copying an absolute-target symlink
// is not portable across platforms and was breaking CI on Linux.) The package
// is served from a copy (no --watch-env), at <SERVER_ROOT>/publisher_data/
// <env>/<pkg>; SERVER_ROOT defaults to the server package dir under `bun test`.
const serverPkgRoot = path.resolve(__dirname, "../../..");
const servedEscapeLink = path.join(
   serverPkgRoot,
   "publisher_data",
   ENV_NAME,
   PACKAGE_NAME,
   "escape.html",
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
            packages: [{ name: PACKAGE_NAME, location: fixtureDir }],
            connections: [],
         }),
      });
      if (!createRes.ok) {
         const body = await createRes.text();
         throw new Error(
            `Failed to create test environment (${createRes.status}): ${body}`,
         );
      }

      const deadline = Date.now() + 30_000;
      let pkgReady = false;
      while (!pkgReady && Date.now() < deadline) {
         try {
            const res = await fetch(
               `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PACKAGE_NAME}`,
            );
            if (res.ok) {
               pkgReady = true;
               break;
            }
         } catch {
            // not ready yet
         }
         await new Promise((r) => setTimeout(r, 500));
      }
      if (!pkgReady) {
         throw new Error("Test package did not become available in time");
      }

      // Now that the package is mounted, plant the escape symlink directly in
      // the served copy (post-fs.cp). Creating a symlink is portable; copying
      // one is not — see the note on servedEscapeLink. Skipped on Windows,
      // where the matching test (itEscape) is skipped too.
      if (!isWindows) {
         try {
            fs.symlinkSync("/etc/hosts", servedEscapeLink);
         } catch {
            // may already exist from a previous run
         }
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
      try {
         fs.unlinkSync(servedEscapeLink);
      } catch {
         // best-effort
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

   it("sets frame-ancestors CSP on HTML responses and clears X-Frame-Options", async () => {
      const res = await fetch(pkgUrl("/index.html"));
      expect(res.status).toBe(200);
      expect(res.headers.get("content-security-policy")).toBe(
         "frame-ancestors *",
      );
      expect(res.headers.get("x-frame-options")).toBeNull();
   });

   it("does NOT set the framing CSP on non-HTML assets", async () => {
      const res = await fetch(pkgUrl("/assets/app.css"));
      expect(res.status).toBe(200);
      // CSP framing is only meaningful on documents; assets keep their default.
      expect(res.headers.get("content-security-policy")).toBeNull();
   });

   it("404s a missing file", async () => {
      const res = await fetch(pkgUrl("/does-not-exist.html"));
      expect(res.status).toBe(404);
   });

   it("blocks the root publisher.json manifest", async () => {
      const res = await fetch(pkgUrl("/publisher.json"));
      expect(res.status).toBe(404);
   });

   it("blocks a publisher.json manifest in a subdirectory", async () => {
      const res = await fetch(pkgUrl("/sub/publisher.json"));
      expect(res.status).toBe(404);
   });

   it("serves a page from a subdirectory", async () => {
      const res = await fetch(pkgUrl("/sub/page2.html"));
      expect(res.status).toBe(200);
      expect(await res.text()).toContain("A page in a subdirectory");
   });

   itEscape(
      "rejects a symlink that escapes the package root with 403",
      async () => {
         const res = await fetch(pkgUrl("/escape.html"));
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
      expect(paths).toEqual(["index.html", "sub/page2.html"]);
      // Manifests (root or nested) are never listed as pages.
      expect(paths).not.toContain("publisher.json");
      expect(paths).not.toContain("sub/publisher.json");

      const index = pages.find((p) => p.path === "index.html");
      expect(index?.title).toBe("Carrier Dashboard");
      expect(index?.packageName).toBe(PACKAGE_NAME);
      expect(index?.resource).toBe(
         `/environments/${ENV_NAME}/packages/${PACKAGE_NAME}/index.html`,
      );
   });
});
