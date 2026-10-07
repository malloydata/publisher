// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * Published package versions over real HTTP: a publish reads its version from
 * the package's own publisher.json, every published version keeps serving the
 * content it was published with, `latest` is what a request naming no version
 * gets, and the package can no longer be changed in place.
 *
 * Each version's model answers a different number, so every assertion about
 * which version served a request can be read off the answer.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const ENV_NAME = "package-versions-env";
const PKG = "sales";

interface ErrorBody {
   message?: string;
   reason?: string;
}

describe("published package versions", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   let scratch: string;
   const savedVersioning = process.env.PUBLISHER_PACKAGE_VERSIONING;

   const api = (sub: string) =>
      `${baseUrl}/api/v0/environments/${ENV_NAME}${sub}`;

   /** A package directory whose model answers `n`, published as `version`. */
   async function packageDir(
      version: string | null,
      n: number,
      name: string = PKG,
   ): Promise<string> {
      const dir = await fs.mkdtemp(path.join(scratch, `${name}-`));
      await fs.writeFile(
         path.join(dir, "publisher.json"),
         JSON.stringify(
            version === null ? { name } : { name, version },
            null,
            2,
         ),
      );
      await fs.writeFile(
         path.join(dir, "report.malloy"),
         `source: report is duckdb.sql("SELECT ${n} as n")\n`,
      );
      await fs.mkdir(path.join(dir, "public"));
      await fs.writeFile(
         path.join(dir, "public/index.html"),
         `<!doctype html><title>answers ${n}</title><script src="./app.js"></script>`,
      );
      await fs.writeFile(path.join(dir, "public/app.js"), `window.n = ${n};\n`);
      return dir;
   }

   const publish = (location: string, name: string = PKG) =>
      fetch(api("/packages"), {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({ name, location }),
      });

   async function answer(
      versionId?: string,
      name: string = PKG,
   ): Promise<Response> {
      return fetch(api(`/packages/${name}/models/report.malloy/query`), {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            query: "run: report -> { select: n }",
            compactJson: true,
            ...(versionId ? { versionId } : {}),
         }),
      });
   }

   async function n(versionId?: string, name: string = PKG): Promise<number> {
      const res = await answer(versionId, name);
      expect(res.status).toBe(200);
      const rows = JSON.parse(
         ((await res.json()) as { result: string }).result,
      );
      return rows[0].n;
   }

   beforeAll(async () => {
      process.env.PUBLISHER_PACKAGE_VERSIONING = "on";
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      scratch = await fs.mkdtemp(path.join(os.tmpdir(), "package-versions-"));
      const created = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [],
            connections: [],
         }),
      });
      expect(created.status).toBe(200);
   });

   afterAll(async () => {
      if (savedVersioning === undefined) {
         delete process.env.PUBLISHER_PACKAGE_VERSIONING;
      } else {
         process.env.PUBLISHER_PACKAGE_VERSIONING = savedVersioning;
      }
      if (env) {
         await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
            method: "DELETE",
         });
         await env.stop();
      }
      await fs.rm(scratch, { recursive: true, force: true });
   });

   it("publishes the version publisher.json names, and makes the first one latest", async () => {
      const res = await publish(await packageDir("1.0.0", 1));
      expect(res.status).toBe(200);
      const body = (await res.json()) as {
         versionId?: string;
         latestVersion?: string;
      };
      expect(body.versionId).toBe("1.0.0");
      expect(body.latestVersion).toBe("1.0.0");
      expect(await n()).toBe(1);
   });

   it("succeeds without change when the same version is published again with the same content", async () => {
      const res = await publish(await packageDir("1.0.0", 1));
      expect(res.status).toBe(200);
      const versions = (await (
         await fetch(api(`/packages/${PKG}/versions`))
      ).json()) as unknown[];
      expect(versions).toHaveLength(1);
   });

   it("refuses the same version with different content, and keeps serving the published one", async () => {
      const res = await publish(await packageDir("1.0.0", 99));
      expect(res.status).toBe(409);
      expect(((await res.json()) as ErrorBody).reason).toBe("VERSION_CONFLICT");
      expect(await n("1.0.0")).toBe(1);
   });

   it("refuses a manifest with no version, or one that is not a semantic version", async () => {
      const missing = await publish(await packageDir(null, 5));
      expect(missing.status).toBe(400);
      expect(((await missing.json()) as ErrorBody).reason).toBe(
         "MANIFEST_VERSION_MISSING",
      );
      const invalid = await publish(await packageDir("v2", 5));
      expect(invalid.status).toBe(400);
      expect(((await invalid.json()) as ErrorBody).reason).toBe(
         "MANIFEST_VERSION_INVALID",
      );
   });

   it("serves every published version, and latest to a request that names none", async () => {
      expect((await publish(await packageDir("1.1.0", 2))).status).toBe(200);
      expect(await n()).toBe(2);
      expect(await n("1.1.0")).toBe(2);
      expect(await n("1.0.0")).toBe(1);
   });

   it("does not move latest backwards for a lower version published later", async () => {
      expect((await publish(await packageDir("1.0.5", 3))).status).toBe(200);
      expect(await n()).toBe(2);
      expect(await n("1.0.5")).toBe(3);
   });

   it("lists versions highest first, marking latest", async () => {
      const res = await fetch(api(`/packages/${PKG}/versions`));
      expect(res.status).toBe(200);
      const versions = (await res.json()) as {
         id: string;
         latest: boolean;
         archiveStatus: string;
         contentHash: string;
      }[];
      expect(versions.map((v) => v.id)).toEqual(["1.1.0", "1.0.5", "1.0.0"]);
      expect(versions.map((v) => v.latest)).toEqual([true, false, false]);
      expect(versions.every((v) => v.archiveStatus === "unarchive")).toBe(true);
      expect(versions[0].contentHash).toMatch(/^[0-9a-f]{64}$/);

      const one = await fetch(api(`/packages/${PKG}/versions/1.0.5`));
      expect(one.status).toBe(200);
      expect(((await one.json()) as { id: string }).id).toBe("1.0.5");

      const unknown = await fetch(api(`/packages/${PKG}/versions/9.9.9`));
      expect(unknown.status).toBe(404);
      expect(((await unknown.json()) as ErrorBody).reason).toBe(
         "VERSION_NOT_FOUND",
      );
   });

   it("404s a read naming a version the package does not have", async () => {
      const res = await answer("9.9.9");
      expect(res.status).toBe(404);
      expect(((await res.json()) as ErrorBody).reason).toBe(
         "VERSION_NOT_FOUND",
      );
      const pkg = await fetch(api(`/packages/${PKG}?versionId=9.9.9`));
      expect(pkg.status).toBe(404);
   });

   it("serves a version's package resource with its own versionId", async () => {
      const res = await fetch(api(`/packages/${PKG}?versionId=1.0.0`));
      expect(res.status).toBe(200);
      expect(await res.json()).toMatchObject({
         name: PKG,
         versionId: "1.0.0",
         latestVersion: "1.1.0",
      });
   });

   it("lists a version's data apps with links that keep opening that version", async () => {
      const res = await fetch(
         api(`/packages/${PKG}/data-apps?versionId=1.0.0`),
      );
      expect(res.status).toBe(200);
      const apps = (await res.json()) as {
         resource: string;
         versionId?: string;
      }[];
      const index = apps.find((a) => a.resource.includes("index.html"));
      expect(index?.versionId).toBe("1.0.0");
      expect(index?.resource).toBe(
         `/environments/${ENV_NAME}/packages/${PKG}/index.html?versionId=1.0.0`,
      );
   });

   it("serves a version's static files, its relative assets included, by the page it was opened at", async () => {
      const page = `${baseUrl}/environments/${ENV_NAME}/packages/${PKG}/index.html?versionId=1.0.0`;
      expect(await (await fetch(page)).text()).toContain("answers 1");

      // The page's own script request carries no query string; the Referer
      // names the version the page was opened at.
      const asset = await fetch(
         `${baseUrl}/environments/${ENV_NAME}/packages/${PKG}/app.js`,
         { headers: { Referer: page } },
      );
      expect(await asset.text()).toContain("window.n = 1");

      const latest = await fetch(
         `${baseUrl}/environments/${ENV_NAME}/packages/${PKG}/app.js`,
      );
      expect(await latest.text()).toContain("window.n = 2");
   });

   it("answers 400 VERSION_ID_INVALID for a versionId that is not a semantic version, and ignores one in a Referer", async () => {
      // A bare + in a query string decodes to a space.
      const bad = await fetch(api(`/packages/${PKG}?versionId=1.0.0+build`));
      expect(bad.status).toBe(400);
      expect(((await bad.json()) as { reason?: string }).reason).toBe(
         "VERSION_ID_INVALID",
      );

      // The Referer is incidental, so its malformed version falls back to
      // latest rather than refusing every asset the page loads.
      const asset = await fetch(
         `${baseUrl}/environments/${ENV_NAME}/packages/${PKG}/app.js`,
         {
            headers: {
               Referer: `${baseUrl}/environments/${ENV_NAME}/packages/${PKG}/index.html?versionId=v1`,
            },
         },
      );
      expect(asset.status).toBe(200);
      expect(await asset.text()).toContain("window.n = 2");
   });

   it("reports every loaded version on /status, each with its own versionId", async () => {
      await n("1.0.0");
      await n("1.1.0");
      const status = (await (
         await fetch(`${baseUrl}/api/v0/status`)
      ).json()) as {
         environments: {
            name: string;
            packages: { name: string; versionId?: string }[];
         }[];
      };
      const ours = status.environments.find((e) => e.name === ENV_NAME);
      const loaded = (ours?.packages ?? [])
         .filter((p) => p.name === PKG)
         .map((p) => p.versionId)
         .sort();
      expect(loaded).toContain("1.0.0");
      expect(loaded).toContain("1.1.0");

      // The package list stays one entry per package: what a request naming
      // no version sees.
      const listed = (await (await fetch(api("/packages"))).json()) as {
         name: string;
         versionId?: string;
      }[];
      expect(listed.filter((p) => p.name === PKG)).toEqual([
         expect.objectContaining({ name: PKG, versionId: "1.1.0" }),
      ]);
   });

   it("refuses to change a versioned package in place", async () => {
      const patch = await fetch(api(`/packages/${PKG}`), {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({ name: PKG, description: "edited" }),
      });
      expect(patch.status).toBe(409);
      expect(((await patch.json()) as ErrorBody).reason).toBe(
         "PACKAGE_IS_VERSIONED",
      );

      // With versioning off, a publish is the unversioned install, which a
      // versioned package refuses rather than overwriting its versions.
      process.env.PUBLISHER_PACKAGE_VERSIONING = "off";
      try {
         const legacy = await publish(await packageDir("2.0.0", 7));
         expect(legacy.status).toBe(409);
         expect(((await legacy.json()) as ErrorBody).reason).toBe(
            "PACKAGE_IS_VERSIONED",
         );
      } finally {
         process.env.PUBLISHER_PACKAGE_VERSIONING = "on";
      }
      expect(await n()).toBe(2);
   });

   it("turns an unversioned package into a versioned one on its first versioned publish", async () => {
      process.env.PUBLISHER_PACKAGE_VERSIONING = "off";
      try {
         expect(
            (await publish(await packageDir("1.0.0", 10, "legacy"), "legacy"))
               .status,
         ).toBe(200);
      } finally {
         process.env.PUBLISHER_PACKAGE_VERSIONING = "on";
      }
      expect(await n(undefined, "legacy")).toBe(10);
      const unversioned = (await (
         await fetch(api(`/packages/legacy/versions`))
      ).json()) as unknown[];
      expect(unversioned).toEqual([]);

      expect(
         (await publish(await packageDir("1.0.0", 11, "legacy"), "legacy"))
            .status,
      ).toBe(200);
      expect(await n(undefined, "legacy")).toBe(11);
      const versions = (await (
         await fetch(api(`/packages/legacy/versions`))
      ).json()) as { id: string }[];
      expect(versions.map((v) => v.id)).toEqual(["1.0.0"]);
   });

   it("serves an unversioned package's page whatever versionId its query string carries", async () => {
      // A proxy that resolves versions itself (Credible's router) serves each
      // version as its own unversioned package and forwards the page's query
      // string as it came. The page must still serve.
      process.env.PUBLISHER_PACKAGE_VERSIONING = "off";
      try {
         expect(
            (await publish(await packageDir(null, 40, "proxied"), "proxied"))
               .status,
         ).toBe(200);
      } finally {
         process.env.PUBLISHER_PACKAGE_VERSIONING = "on";
      }
      const page = await fetch(
         `${baseUrl}/environments/${ENV_NAME}/packages/proxied/index.html?versionId=1.0.3`,
      );
      expect(page.status).toBe(200);
      expect(await page.text()).toContain("answers 40");
   });

   it("never moves latest on publish under explicit promotion", async () => {
      const saved = process.env.PUBLISHER_VERSION_PROMOTION;
      process.env.PUBLISHER_VERSION_PROMOTION = "explicit";
      try {
         expect(
            (await publish(await packageDir("1.0.0", 20, "held"), "held"))
               .status,
         ).toBe(200);
      } finally {
         if (saved === undefined)
            delete process.env.PUBLISHER_VERSION_PROMOTION;
         else process.env.PUBLISHER_VERSION_PROMOTION = saved;
      }
      // Published and servable by name, but nothing is latest yet, so a
      // request naming no version has nothing to answer from.
      expect(await n("1.0.0", "held")).toBe(20);
      const nameless = await answer(undefined, "held");
      expect(nameless.status).toBe(404);
      expect(((await nameless.json()) as ErrorBody).reason).toBe(
         "VERSION_NOT_FOUND",
      );
      const versions = (await (
         await fetch(api(`/packages/held/versions`))
      ).json()) as { latest: boolean }[];
      expect(versions.map((v) => v.latest)).toEqual([false]);
      // Still listed, with no version to describe.
      const listed = (await (await fetch(api("/packages"))).json()) as {
         name: string;
         versionId?: string | null;
      }[];
      expect(listed.find((p) => p.name === "held")).toMatchObject({
         versionId: null,
         latestVersion: null,
      });
   });

   it("deletes every version with the package", async () => {
      const res = await fetch(api(`/packages/${PKG}`), { method: "DELETE" });
      expect(res.status).toBe(200);
      expect((await fetch(api(`/packages/${PKG}`))).status).toBe(404);
      expect((await fetch(api(`/packages/${PKG}/versions`))).status).toBe(404);

      // And the name is free again: the next publish starts a new history.
      expect((await publish(await packageDir("3.0.0", 30))).status).toBe(200);
      expect(await n()).toBe(30);
   });
});
