// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * What a data-app page is served with, over HTTP: the injected page context,
 * the versioned static URL (`.../versions/<v>/public/...`), the floating URLs
 * that serve latest directly, and the redirect from a short URL's `?versionId=`.
 *
 * `ver` is a published package with two versions whose pages say which they
 * are, `plain` is a package with no versions, and a third version of `ver`
 * (0.5.0, below latest) is archived for the 410 case.
 */

import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import fs from "fs";
import * as fsp from "fs/promises";
import http from "http";
import os from "os";
import path from "path";
import { logger } from "../../../src/logger";
import { createMainApp } from "../../../src/routes/main_app";
import {
   parseEnvironmentName,
   parsePackageName,
} from "../../../src/service/identity";
import { bootMainServer } from "../../harness/main_server";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const ENV_NAME = "page-context-env";
const VER = "ver";
const PLAIN = "plain";

let env: (RestE2EEnv & { stop(): Promise<void> }) | undefined;
let baseUrl = "";
let root = "";
/** A second app over the same services, with the page-context header trusted. */
let trusted: http.Server | undefined;
let trustedUrl = "";

const PAGE_HEAD = (n: number) =>
   `<!doctype html><html><head><meta charset="utf-8"><title>App v${n}</title></head>`;

/** One package tree whose pages and assets name `n`. */
function writePackage(
   dir: string,
   manifest: Record<string, unknown>,
   n: number,
): string {
   fs.mkdirSync(path.join(dir, "public", "sub"), { recursive: true });
   fs.writeFileSync(path.join(dir, "publisher.json"), JSON.stringify(manifest));
   const page = (text: string) =>
      `${PAGE_HEAD(n)}<body>${text} v${n}<script src="app.js"></script></body></html>`;
   const pub = (file: string, content: string | Buffer) =>
      fs.writeFileSync(path.join(dir, "public", file), content);
   pub("index.html", page("home"));
   pub("sub/page.html", page("sub"));
   pub("my page.html", page("spaced"));
   pub("app.js", `/* js v${n} */`);
   pub("data.txt", `text v${n}`);
   // A latin1 page: the declared charset is not UTF-8 and a byte is not ASCII.
   pub(
      "latin1.html",
      Buffer.concat([
         Buffer.from(
            '<!doctype html><html><head><meta charset="iso-8859-1"></head><body>caf',
            "latin1",
         ),
         Buffer.from([0xe9]),
         Buffer.from("</body></html>", "latin1"),
      ]),
   );
   // UTF-16 with a byte-order mark: its bytes cannot be scanned as ASCII.
   pub(
      "utf16.html",
      Buffer.concat([
         Buffer.from([0xff, 0xfe]),
         Buffer.from("<p>hi</p>", "utf16le"),
      ]),
   );
   // No doctype within reach, so the context goes at the start.
   pub("late-head.html", `${"x".repeat(70 * 1024)}<head></head>`);
   // At the size cap, built to make a naive scan slow.
   pub("hostile.html", "<!--<head>-->".repeat(Math.floor(2097152 / 13)));
   pub(".hidden.html", page("hidden"));
   // Past the size a page is buffered to splice.
   pub(
      "big.html",
      `<!doctype html><head></head><body>${"x".repeat(2 * 1024 * 1024)}</body>`,
   );
   return dir;
}

const versioned = (v: string, sub = "") =>
   `${baseUrl}/environments/${ENV_NAME}/packages/${VER}/versions/${v}/public/${sub}`;
const short = (sub: string, origin = baseUrl) =>
   `${origin}/environments/${ENV_NAME}/packages/${VER}/${sub}`;
const plainPage = (sub: string, origin = baseUrl) =>
   `${origin}/environments/${ENV_NAME}/packages/${PLAIN}/${sub}`;
const absolute = (v: string, sub: string) =>
   `/environments/${ENV_NAME}/packages/${VER}/versions/${v}/public/${sub}`;

const noFollow = (url: string, init: RequestInit = {}) =>
   fetch(url, { redirect: "manual", ...init });

interface PageContext {
   v: number;
   environment: string;
   package: string;
   versionId: string | null;
   apiBase: string;
   hostOrigins: string[];
}

/** The page context a served page carries, parsed back from its element. */
function contextOf(html: string): PageContext {
   const match =
      /<script type="application\/json" id="publisher-context">([^<]*)<\/script>/.exec(
         html,
      );
   if (!match) throw new Error(`no page context in: ${html.slice(0, 300)}`);
   return JSON.parse(match[1]) as PageContext;
}

describe("data-app page context and versioned static URLs", () => {
   beforeAll(async () => {
      root = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "page-context-")),
      );
      env = await startRestE2E();
      baseUrl = env.baseUrl;

      const created = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "content-type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [
               {
                  name: PLAIN,
                  location: writePackage(
                     path.join(root, "plain"),
                     { name: PLAIN },
                     0,
                  ),
               },
            ],
            connections: [],
         }),
      });
      expect(created.status).toBeLessThan(300);

      // 0.5.0 last: a version below latest does not move it.
      for (const [version, n] of [
         ["1.0.0", 1],
         ["2.0.0", 2],
         ["0.5.0", 5],
      ] as const) {
         const location = writePackage(
            path.join(root, `v${version}`),
            { name: VER, version },
            n,
         );
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages`,
            {
               method: "POST",
               headers: { "content-type": "application/json" },
               body: JSON.stringify({ name: VER, location }),
            },
         );
         expect(res.status).toBe(200);
      }

      const { services } = await bootMainServer();
      const app = createMainApp({
         controllers: services,
         dataAppRuntimePath: "/nonexistent/publisher.js",
         frameAncestors:
            "'self' https://*.app.example.com https://x.example/path * https:",
         trustPageContextHeader: true,
      });
      trusted = await new Promise<http.Server>((resolve) => {
         const srv = http
            .createServer(app)
            .listen(0, "127.0.0.1", () => resolve(srv));
      });
      trustedUrl = `http://127.0.0.1:${(trusted.address() as { port: number }).port}`;
   }, 180_000);

   afterAll(async () => {
      trusted?.closeAllConnections?.();
      await new Promise<void>((r) =>
         trusted ? trusted.close(() => r()) : r(),
      );
      await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
         method: "DELETE",
      }).catch(() => undefined);
      await env?.stop();
      fs.rmSync(root, { recursive: true, force: true });
   });

   // ── the versioned route ──────────────────────────────────────────

   it("serves a version's page with its context spliced in after the doctype", async () => {
      const res = await fetch(versioned("1.0.0", "index.html"));
      expect(res.status).toBe(200);
      expect(res.headers.get("content-type")).toBe("text/html; charset=utf-8");
      expect(res.headers.get("cache-control")).toBe("no-cache");
      expect(res.headers.get("x-content-type-options")).toBe("nosniff");
      expect(res.headers.get("content-security-policy")).toBe(
         "frame-ancestors 'self'",
      );
      const body = await res.text();
      expect(body).toContain("home v1");
      expect(body).toContain(
         '<!doctype html><script type="application/json" id="publisher-context">',
      );
      expect(contextOf(body)).toEqual({
         v: 1,
         environment: ENV_NAME,
         package: VER,
         versionId: "1.0.0",
         apiBase: "/api/v0",
         hostOrigins: ["'self'"],
      });
   });

   it("serves the directory index, nested pages and assets of the named version", async () => {
      const index = await (await fetch(versioned("1.0.0"))).text();
      expect(index).toContain("home v1");
      expect(contextOf(index).versionId).toBe("1.0.0");

      const nested = await (
         await fetch(versioned("2.0.0", "sub/page.html"))
      ).text();
      expect(nested).toContain("sub v2");
      expect(contextOf(nested).versionId).toBe("2.0.0");

      const spaced = await fetch(versioned("2.0.0", "my%20page.html"));
      expect(await spaced.text()).toContain("spaced v2");

      // A page's relative asset URL stays inside the version it was loaded from.
      const asset = await fetch(versioned("1.0.0", "app.js"));
      expect(asset.status).toBe(200);
      expect(await asset.text()).toBe("/* js v1 */");
      expect(await (await fetch(versioned("2.0.0", "app.js"))).text()).toBe(
         "/* js v2 */",
      );
   });

   it("sends a non-HTML file as it is, with no injection and no revalidation header", async () => {
      const res = await fetch(versioned("1.0.0", "data.txt"));
      expect(await res.text()).toBe("text v1");
      expect(res.headers.get("cache-control")).not.toBe("no-cache");
   });

   it("lets the path win over ?versionId=", async () => {
      // A proxy appends its own resolved version to every request it forwards.
      const res = await fetch(versioned("1.0.0", "index.html?versionId=2.0.0"));
      expect(res.status).toBe(200);
      expect(contextOf(await res.text()).versionId).toBe("1.0.0");
      const unknown = await fetch(
         versioned("1.0.0", "index.html?versionId=9.9.9"),
      );
      expect(unknown.status).toBe(200);
   });

   it("answers a conditional request with a weak ETag and 304", async () => {
      const first = await fetch(versioned("1.0.0", "index.html"));
      const etag = first.headers.get("etag") ?? "";
      expect(etag).toMatch(/^W\/"[0-9a-f]{64}"$/);
      await first.text();

      const again = await fetch(versioned("1.0.0", "index.html"), {
         headers: { "if-none-match": etag },
      });
      expect(again.status).toBe(304);
      expect(await again.text()).toBe("");

      const stale = await fetch(versioned("1.0.0", "index.html"), {
         headers: { "if-none-match": 'W/"stale"' },
      });
      expect(stale.status).toBe(200);
      await stale.text();

      // A different version is a different page, so a different tag.
      const other = await fetch(versioned("2.0.0", "index.html"));
      expect(other.headers.get("etag")).not.toBe(etag);
      await other.text();
   });

   it("answers a matching conditional request after a stat alone, never reading the file", async () => {
      const first = await fetch(versioned("1.0.0", "index.html"));
      const etag = first.headers.get("etag") ?? "";
      await first.text();

      const read = spyOn(fsp, "readFile");
      try {
         const again = await fetch(versioned("1.0.0", "index.html"), {
            headers: { "if-none-match": etag },
         });
         expect(again.status).toBe(304);
         expect(read).not.toHaveBeenCalled();

         const full = await fetch(versioned("1.0.0", "index.html"));
         expect(full.status).toBe(200);
         await full.text();
         expect(read).toHaveBeenCalledTimes(1);
      } finally {
         read.mockRestore();
      }
   });

   it("changes its ETag when the file changes, and when the injected context does", async () => {
      // The served copy: the package is copied in on publish.
      const { environmentStore } = await bootMainServer();
      const served = await (
         await environmentStore.getEnvironment(
            parseEnvironmentName(ENV_NAME),
            false,
         )
      ).getPackage(parsePackageName(PLAIN), false);
      const file = path.join(served.getPackagePath(), "public", "etag.html");
      const url = plainPage("etag.html");
      const tagOf = async (target: string, headers: HeadersInit = {}) => {
         const res = await fetch(target, { headers });
         await res.text();
         return res.headers.get("etag");
      };
      fs.writeFileSync(file, "<!doctype html><head></head><body>one</body>");
      const first = await tagOf(url);
      expect(await tagOf(url)).toBe(first);

      // Same size, later mtime: still a different page.
      fs.writeFileSync(file, "<!doctype html><head></head><body>two</body>");
      fs.utimesSync(file, new Date(), new Date(Date.now() + 60_000));
      const second = await tagOf(url);
      expect(second).not.toBe(first);

      // The same file with another context is another page, and the first
      // page's tag does not revalidate it.
      const trustedPage = plainPage("etag.html", trustedUrl);
      const base = await tagOf(trustedPage);
      const overridden = await tagOf(trustedPage, {
         "x-publisher-page-context": JSON.stringify({ apiBase: "/gw/api/v0" }),
      });
      expect(overridden).not.toBe(base);
      const res = await fetch(trustedPage, {
         headers: {
            "if-none-match": base ?? "",
            "x-publisher-page-context": JSON.stringify({
               apiBase: "/gw/api/v0",
            }),
         },
      });
      expect(res.status).toBe(200);
      expect(contextOf(await res.text()).apiBase).toBe("/gw/api/v0");
   });

   it("answers a range request for HTML with the whole page", async () => {
      const full = await (await fetch(versioned("1.0.0", "index.html"))).text();
      const ranged = await fetch(versioned("1.0.0", "index.html"), {
         headers: { range: "bytes=0-10" },
      });
      expect(ranged.status).toBe(200);
      expect(ranged.headers.get("content-range")).toBeNull();
      expect(await ranged.text()).toBe(full);
   });

   it("answers HEAD with the headers and no body", async () => {
      const res = await fetch(versioned("1.0.0", "index.html"), {
         method: "HEAD",
      });
      expect(res.status).toBe(200);
      expect(res.headers.get("content-type")).toBe("text/html; charset=utf-8");
      expect(res.headers.get("etag")).toMatch(/^W\/"/);
      expect(await res.text()).toBe("");
   });

   it("splices into the bytes, so a page in another charset is not re-encoded", async () => {
      const res = await fetch(versioned("1.0.0", "latin1.html"));
      const bytes = Buffer.from(await res.arrayBuffer());
      const text = bytes.toString("latin1");
      expect(text).toContain(
         '<!doctype html><script type="application/json" id="publisher-context">',
      );
      expect(bytes.includes(Buffer.from([0xe9]))).toBe(true);
      expect(bytes.toString("latin1").endsWith("café</body></html>")).toBe(
         true,
      );
      expect(contextOf(text).package).toBe(VER);
   });

   it("leaves a UTF-16 page and a page over 2 MiB as they are", async () => {
      const utf16 = Buffer.from(
         await (await fetch(versioned("1.0.0", "utf16.html"))).arrayBuffer(),
      );
      expect(
         utf16.equals(
            Buffer.concat([
               Buffer.from([0xff, 0xfe]),
               Buffer.from("<p>hi</p>", "utf16le"),
            ]),
         ),
      ).toBe(true);

      const big = await fetch(versioned("1.0.0", "big.html"));
      expect(big.status).toBe(200);
      const text = await big.text();
      expect(text).not.toContain("publisher-context");
      expect(text.length).toBeGreaterThan(2 * 1024 * 1024);
   });

   it("puts the context at the start of a page with no doctype, and serves a hostile one promptly", async () => {
      const late = await fetch(versioned("1.0.0", "late-head.html"));
      const lateBody = await late.text();
      expect(lateBody).toStartWith(
         '<script type="application/json" id="publisher-context">',
      );
      expect(lateBody).toEndWith(`${"x".repeat(70 * 1024)}<head></head>`);

      const started = performance.now();
      const hostile = await fetch(versioned("1.0.0", "hostile.html"));
      const body = await hostile.text();
      expect(hostile.status).toBe(200);
      expect(contextOf(body).package).toBe(VER);
      expect(body).toContain("</script><!--<head>--><!--<head>-->");
      expect(performance.now() - started).toBeLessThan(1000);
   });

   it("404s a version the package does not have, with the page that names both URL forms", async () => {
      for (const v of ["9.9.9", "not-a-version", "1.0"]) {
         const res = await fetch(versioned(v, "index.html"));
         expect([v, res.status]).toEqual([v, 404]);
         expect(res.headers.get("content-type")).toContain("text/html");
         const body = await res.text();
         expect(body).toContain(
            "/versions/&lt;version&gt;/public/&lt;file&gt;",
         );
         expect(body).toContain("/packages/&lt;pkg&gt;/&lt;file&gt;");
         // Static: it does not echo what was asked for.
         expect(body).not.toContain(v === "1.0" ? "1.0." : v);
      }
      expect((await fetch(versioned("1.0.0", "missing.html"))).status).toBe(
         404,
      );
   });

   it("404s the versioned route of a package with no versions", async () => {
      for (const v of ["1.0.0", "latest"]) {
         const res = await fetch(
            `${baseUrl}/environments/${ENV_NAME}/packages/${PLAIN}/versions/${v}/public/index.html`,
         );
         expect([v, res.status]).toEqual([v, 404]);
         expect(res.headers.get("content-type")).toContain("text/html");
      }
   });

   it("answers the versioned route of a package with no versions without a registry read", async () => {
      const { environmentStore } = await bootMainServer();
      const versions = (
         await environmentStore.getEnvironment(
            parseEnvironmentName(ENV_NAME),
            false,
         )
      ).getVersionService()!;
      const read = spyOn(
         versions as unknown as { readRecords(name: string): unknown },
         "readRecords",
      );
      try {
         for (const v of ["1.0.0", "latest"]) {
            const res = await fetch(
               `${baseUrl}/environments/${ENV_NAME}/packages/${PLAIN}/versions/${v}/public/index.html`,
            );
            expect([v, res.status]).toEqual([v, 404]);
         }
         expect(read).not.toHaveBeenCalled();
      } finally {
         read.mockRestore();
      }
   });

   it("serves latest directly, without a redirect, whatever the query says", async () => {
      const res = await noFollow(
         versioned(
            "latest",
            "sub/page.html?embed_token=abc&versionId=1.0.0&x=1",
         ),
      );
      expect(res.status).toBe(200);
      expect(res.headers.get("location")).toBeNull();
      expect(res.headers.get("cache-control")).toBe("no-cache");
      const body = await res.text();
      expect(body).toContain("sub v2");
      expect(contextOf(body).versionId).toBe("2.0.0");

      const root = await noFollow(versioned("latest"));
      expect(root.status).toBe(200);
      expect(contextOf(await root.text()).versionId).toBe("2.0.0");
      const asset = await noFollow(versioned("latest", "app.js"));
      expect(asset.status).toBe(200);
      expect(await asset.text()).toBe("/* js v2 */");
      expect(asset.headers.get("cache-control")).toBe("no-cache");
   });

   it("308s the slashless root to its slash form, keeping the query", async () => {
      const res = await noFollow(
         `${baseUrl}/environments/${ENV_NAME}/packages/${VER}/versions/1.0.0/public?embed_token=abc`,
      );
      expect(res.status).toBe(308);
      expect(res.headers.get("location")).toBe(
         absolute("1.0.0", "?embed_token=abc"),
      );
      expect((await noFollow(versioned("1.0.0"))).status).toBe(200);
   });

   it("answers an archived version 410 through the version error mapping", async () => {
      const lifecycle = (archiveStatus: string) =>
         fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${VER}/versions/0.5.0`,
            {
               method: "PATCH",
               headers: { "content-type": "application/json" },
               body: JSON.stringify({ archiveStatus }),
            },
         );
      expect(await versionedStatus("0.5.0")).toBe(200);
      expect((await lifecycle("archive")).status).toBe(200);
      try {
         const res = await fetch(versioned("0.5.0", "index.html"));
         expect(res.status).toBe(410);
         expect(((await res.json()) as { reason?: string }).reason).toBe(
            "VERSION_ARCHIVED",
         );
      } finally {
         expect((await lifecycle("unarchive")).status).toBe(200);
      }
   });

   // ── the short form ───────────────────────────────────────────────

   it("serves a page load at the short URL directly, from latest and revalidated", async () => {
      const res = await noFollow(short("index.html"));
      expect(res.status).toBe(200);
      expect(res.headers.get("location")).toBeNull();
      const cacheControl = res.headers.get("cache-control");
      expect(cacheControl).toBe("no-cache");
      expect(cacheControl).not.toContain("immutable");
      expect(res.headers.get("etag")).toStartWith('W/"');
      // The version it resolved pins the page's queries.
      expect(contextOf(await res.text()).versionId).toBe("2.0.0");
   });

   it("serves the package root and nested and encoded paths directly", async () => {
      for (const [sub, text] of [
         ["", "home v2"],
         ["sub/page.html", "sub v2"],
         ["my%20page.html", "spaced v2"],
         ["sub%2f..%2fsub%2fpage.html", "sub v2"],
      ]) {
         const res = await noFollow(short(sub));
         expect([sub, res.status]).toEqual([sub, 200]);
         const body = await res.text();
         expect([sub, body.includes(text)]).toEqual([sub, true]);
         expect(contextOf(body).versionId).toBe("2.0.0");
      }
   });

   it("redirects only an explicit version of the package, keeping the rest of the query", async () => {
      const pinned = await noFollow(
         short("index.html?embed_token=a&versionId=1.0.0&filter[x]=1"),
      );
      expect(pinned.status).toBe(302);
      expect(pinned.headers.get("location")).toBe(
         absolute("1.0.0", "index.html?embed_token=a&filter[x]=1"),
      );
      expect(pinned.headers.get("cache-control")).toBe("no-store");
      // A pin that names no version of the package opens from latest.
      const unknown = await noFollow(short("index.html?versionId=7.7.7"));
      expect(unknown.status).toBe(200);
      expect(unknown.headers.get("location")).toBeNull();
      expect(contextOf(await unknown.text()).versionId).toBe("2.0.0");
   });

   it("ends on the page of the version the redirect named", async () => {
      const res = await fetch(short("sub/page.html?versionId=1.0.0"));
      expect(res.status).toBe(200);
      expect(new URL(res.url).pathname).toBe(
         absolute("1.0.0", "sub/page.html"),
      );
      const body = await res.text();
      expect(body).toContain("sub v1");
      expect(contextOf(body).versionId).toBe("1.0.0");
   });

   it("serves a non-HTML asset of a versioned package with no redirect, revalidated", async () => {
      const res = await noFollow(short("app.js"));
      expect(res.status).toBe(200);
      expect(await res.text()).toBe("/* js v2 */");
      expect(res.headers.get("cache-control")).toBe("no-cache");
      const pinned = await noFollow(short("app.js?versionId=1.0.0"));
      expect(await pinned.text()).toBe("/* js v1 */");
   });

   it("404s a missing page and a dotfile without redirecting", async () => {
      expect((await noFollow(short("nope.html"))).status).toBe(404);
      for (const url of [
         short(".hidden.html"),
         versioned("latest", ".hidden.html"),
         versioned("1.0.0", ".hidden.html"),
      ]) {
         const res = await noFollow(url);
         expect([url, res.status, res.headers.get("location")]).toEqual([
            url,
            404,
            null,
         ]);
      }
   });

   it("refuses an HTML path that leaves public/ before any redirect, on every route", async () => {
      for (const url of [
         short("..%2f..%2fpublisher.html"),
         short("sub/..%2f..%2f..%2fpublisher.html"),
         short("..%2f..%2fpublisher.html?versionId=1.0.0"),
         versioned("1.0.0", "..%2f..%2fpublisher.html"),
         versioned("latest", "..%2f..%2fpublisher.html"),
         versioned("latest", "sub%2f..%2f..%2f..%2fpublisher.html"),
      ]) {
         const res = await noFollow(url);
         expect([url, res.status, res.headers.get("location")]).toEqual([
            url,
            400,
            null,
         ]);
         await res.text();
      }
   });

   it("builds the redirect from the normalized path under public/", async () => {
      expect(
         (
            await noFollow(short("sub%2f..%2fsub%2fpage.html?versionId=1.0.0"))
         ).headers.get("location"),
      ).toBe(absolute("1.0.0", "sub/page.html"));
   });

   it("serves a package with no versions directly, with a null versionId", async () => {
      for (const sub of ["index.html", "", "index.html?versionId=1.0.0"]) {
         const res = await noFollow(plainPage(sub));
         expect([sub, res.status]).toEqual([sub, 200]);
         const body = await res.text();
         expect(body).toContain("home v0");
         expect(contextOf(body)).toMatchObject({
            package: PLAIN,
            versionId: null,
            apiBase: "/api/v0",
         });
      }
   });

   // ── the override header ──────────────────────────────────────────

   const HEADER = "x-publisher-page-context";
   const override = {
      environment: "acme_prod",
      package: "orders",
      apiBase: "/publisher/api/v0",
      hostOrigins: ["https://app.example.com", "https://*.apps.example.com"],
   };

   it("ignores the header when it is not trusted", async () => {
      const res = await fetch(versioned("1.0.0", "index.html"), {
         headers: { [HEADER]: JSON.stringify(override) },
      });
      expect(contextOf(await res.text())).toMatchObject({
         environment: ENV_NAME,
         package: VER,
         apiBase: "/api/v0",
         hostOrigins: ["'self'"],
      });
      expect(res.headers.get("vary") ?? "").not.toContain(HEADER);
   });

   it("injects the usable frame-ancestors sources when trusted mode is on, header or none", async () => {
      const res = await fetch(
         `${trustedUrl}/environments/${ENV_NAME}/packages/${VER}/versions/1.0.0/public/index.html`,
      );
      expect(contextOf(await res.text()).hostOrigins).toEqual([
         "'self'",
         "https://*.app.example.com",
      ]);
      // Whatever was read, a cache must key the page on the header.
      expect(res.headers.get("vary")).toContain(HEADER);
   });

   it("applies the header when trusted, and says the response varies on it", async () => {
      const res = await fetch(
         `${trustedUrl}/environments/${ENV_NAME}/packages/${VER}/versions/1.0.0/public/index.html`,
         { headers: { [HEADER]: JSON.stringify(override) } },
      );
      expect(res.status).toBe(200);
      expect(res.headers.get("vary")).toContain(HEADER);
      expect(contextOf(await res.text())).toEqual({
         v: 1,
         environment: "acme_prod",
         package: "orders",
         // The version is what was served, not something a header names.
         versionId: "1.0.0",
         apiBase: "/publisher/api/v0",
         hostOrigins: ["https://app.example.com", "https://*.apps.example.com"],
      });
   });

   it("overrides only the fields the header names", async () => {
      const res = await fetch(
         `${trustedUrl}/environments/${ENV_NAME}/packages/${PLAIN}/index.html`,
         { headers: { [HEADER]: JSON.stringify({ apiBase: "/gw/api/v0" }) } },
      );
      expect(contextOf(await res.text())).toMatchObject({
         environment: ENV_NAME,
         package: PLAIN,
         versionId: null,
         apiBase: "/gw/api/v0",
         hostOrigins: ["'self'", "https://*.app.example.com"],
      });
   });

   it("ignores an invalid header whole, and logs it", async () => {
      const warn = spyOn(logger, "warn").mockImplementation(() => logger);
      try {
         for (const bad of [
            JSON.stringify({ environment: "ok", apiBase: "//evil.example" }),
            JSON.stringify({ hostOrigins: ["*"] }),
            JSON.stringify({ package: "x".repeat(257) }),
            "not json",
         ]) {
            const res = await fetch(
               `${trustedUrl}/environments/${ENV_NAME}/packages/${VER}/versions/1.0.0/public/index.html`,
               { headers: { [HEADER]: bad } },
            );
            expect([bad, res.status]).toEqual([bad, 200]);
            expect(res.headers.get("vary")).toContain(HEADER);
            expect(contextOf(await res.text())).toMatchObject({
               environment: ENV_NAME,
               package: VER,
               apiBase: "/api/v0",
            });
         }
         expect(warn).toHaveBeenCalledTimes(4);
         expect(String(warn.mock.calls[0][0])).toContain("page-context header");
      } finally {
         warn.mockRestore();
      }
   });

   it("does not let the header change which version is served", async () => {
      const res = await noFollow(
         `${trustedUrl}/environments/${ENV_NAME}/packages/${VER}/index.html`,
         { headers: { [HEADER]: JSON.stringify({ package: "other" }) } },
      );
      expect(res.status).toBe(200);
      expect(contextOf(await res.text())).toMatchObject({
         package: "other",
         versionId: "2.0.0",
      });
   });
});

async function versionedStatus(v: string): Promise<number> {
   const res = await fetch(versioned(v, "index.html"));
   await res.text();
   return res.status;
}
