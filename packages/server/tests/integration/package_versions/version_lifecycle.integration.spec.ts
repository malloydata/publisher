// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * A published version's serving state over real HTTP: which version `latest`
 * points at (`PUT …/latest`), which build manifest a version is bound to
 * (`PUT …/versions/{v}/manifest`), and whether it is in service at all
 * (`PATCH …/versions/{v}`). None of them changes a version's content.
 *
 * Each version's model answers a different number, so which version served a
 * request can be read off the answer.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const ENV_NAME = "version-lifecycle-env";
const PKG = "orders";

interface ErrorBody {
   message?: string;
   reason?: string;
}

interface VersionBody {
   id: string;
   latest: boolean;
   archiveStatus: string;
   archivedAt: string | null;
   manifestLocation: string | null;
}

describe("version lifecycle: latest, manifest, archive", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   let scratch: string;
   const savedVersioning = process.env.PUBLISHER_PACKAGE_VERSIONING;
   const savedPromotion = process.env.PUBLISHER_VERSION_PROMOTION;

   const api = (sub: string) =>
      `${baseUrl}/api/v0/environments/${ENV_NAME}${sub}`;
   const json = { "Content-Type": "application/json" };

   async function packageDir(
      version: string | null,
      n: number,
      name: string = PKG,
   ): Promise<string> {
      const dir = await fs.mkdtemp(path.join(scratch, `${name}-`));
      await fs.writeFile(
         path.join(dir, "publisher.json"),
         JSON.stringify(version === null ? { name } : { name, version }),
      );
      await fs.writeFile(
         path.join(dir, "report.malloy"),
         `source: report is duckdb.sql("SELECT ${n} as n")\n`,
      );
      return dir;
   }

   const publish = (location: string, name: string = PKG) =>
      fetch(api("/packages"), {
         method: "POST",
         headers: json,
         body: JSON.stringify({ name, location }),
      });

   const answer = (versionId?: string, name: string = PKG) =>
      fetch(api(`/packages/${name}/models/report.malloy/query`), {
         method: "POST",
         headers: json,
         body: JSON.stringify({
            query: "run: report -> { select: n }",
            compactJson: true,
            ...(versionId ? { versionId } : {}),
         }),
      });

   async function n(versionId?: string, name: string = PKG): Promise<number> {
      const res = await answer(versionId, name);
      expect(res.status).toBe(200);
      return JSON.parse(((await res.json()) as { result: string }).result)[0].n;
   }

   const setLatest = (versionId: unknown, name: string = PKG) =>
      fetch(api(`/packages/${name}/latest`), {
         method: "PUT",
         headers: json,
         body: JSON.stringify({ versionId }),
      });

   const setArchive = (version: string, archiveStatus: unknown) =>
      fetch(api(`/packages/${PKG}/versions/${version}`), {
         method: "PATCH",
         headers: json,
         body: JSON.stringify({ archiveStatus }),
      });

   const setManifest = (version: string, body: unknown) =>
      fetch(api(`/packages/${PKG}/versions/${version}/manifest`), {
         method: "PUT",
         headers: json,
         body: JSON.stringify(body),
      });

   async function versions(name: string = PKG): Promise<VersionBody[]> {
      const res = await fetch(api(`/packages/${name}/versions`));
      expect(res.status).toBe(200);
      return (await res.json()) as VersionBody[];
   }

   async function reasonOf(res: Response): Promise<string | undefined> {
      return ((await res.json()) as ErrorBody).reason;
   }

   /** The package's entries in /status: every version held, loaded or not. */
   async function heldVersions(
      name: string = PKG,
   ): Promise<
      { versionId?: string; loaded?: boolean; archiveStatus?: string }[]
   > {
      const status = (await (
         await fetch(`${baseUrl}/api/v0/status`)
      ).json()) as {
         environments: {
            name: string;
            packages: {
               name: string;
               versionId?: string;
               loaded?: boolean;
               archiveStatus?: string;
            }[];
         }[];
      };
      return (
         status.environments
            .find((e) => e.name === ENV_NAME)
            ?.packages.filter((p) => p.name === name) ?? []
      );
   }

   async function loadedVersions(name: string = PKG): Promise<string[]> {
      return (await heldVersions(name))
         .filter((p) => p.loaded !== false)
         .map((p) => p.versionId ?? "")
         .sort();
   }

   beforeAll(async () => {
      process.env.PUBLISHER_PACKAGE_VERSIONING = "on";
      delete process.env.PUBLISHER_VERSION_PROMOTION;
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      scratch = await fs.mkdtemp(path.join(os.tmpdir(), "version-lifecycle-"));
      const created = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: json,
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [],
            connections: [],
         }),
      });
      expect(created.status).toBe(200);
      expect((await publish(await packageDir("1.0.0", 1))).status).toBe(200);
      expect((await publish(await packageDir("1.1.0", 2))).status).toBe(200);
      expect((await publish(await packageDir("2.0.0", 3))).status).toBe(200);
   });

   afterAll(async () => {
      for (const [key, saved] of [
         ["PUBLISHER_PACKAGE_VERSIONING", savedVersioning],
         ["PUBLISHER_VERSION_PROMOTION", savedPromotion],
      ] as const) {
         if (saved === undefined) delete process.env[key];
         else process.env[key] = saved;
      }
      if (env) {
         await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
            method: "DELETE",
         });
         await env.stop();
      }
      await fs.rm(scratch, { recursive: true, force: true });
   });

   describe("PUT …/latest", () => {
      it("rolls latest back, and a request naming no version follows it", async () => {
         expect(await n()).toBe(3);
         const res = await setLatest("1.0.0");
         expect(res.status).toBe(200);
         expect(await res.json()).toMatchObject({ id: "1.0.0", latest: true });
         expect(await n()).toBe(1);
         expect(
            (await versions()).filter((v) => v.latest).map((v) => v.id),
         ).toEqual(["1.0.0"]);
         // The other versions still serve by name.
         expect(await n("2.0.0")).toBe(3);
      });

      it("drops the version that stopped being latest from memory, and still reports it held", async () => {
         expect((await setLatest("1.1.0")).status).toBe(200);
         expect(await loadedVersions()).not.toContain("1.0.0");
         expect(await loadedVersions()).toContain("1.1.0");
         // Unloaded is not gone: /status still lists it, so an orchestrator
         // reconciling from /status does not publish it again.
         expect(
            (await heldVersions()).find((p) => p.versionId === "1.0.0"),
         ).toMatchObject({ loaded: false, archiveStatus: "unarchive" });
         expect(await n()).toBe(2);
      });

      it("reports the versioning settings it runs with on /status", async () => {
         const status = (await (
            await fetch(`${baseUrl}/api/v0/status`)
         ).json()) as { packageVersioning?: string; versionPromotion?: string };
         expect(status).toMatchObject({
            packageVersioning: "on",
            versionPromotion: "on-publish",
         });
      });

      it("answers 200 and changes nothing when the version is already latest", async () => {
         const res = await setLatest("1.1.0");
         expect(res.status).toBe(200);
         expect(((await res.json()) as VersionBody).latest).toBe(true);
         expect(await n()).toBe(2);
      });

      it("lets the next publish move latest forward again after a rollback", async () => {
         expect((await publish(await packageDir("2.1.0", 4))).status).toBe(200);
         expect(await n()).toBe(4);
      });

      it("refuses a version the package does not have, and a package with no versions", async () => {
         const unknown = await setLatest("9.9.9");
         expect(unknown.status).toBe(404);
         expect(await reasonOf(unknown)).toBe("VERSION_NOT_FOUND");

         process.env.PUBLISHER_PACKAGE_VERSIONING = "off";
         try {
            expect(
               (await publish(await packageDir(null, 50, "plain"), "plain"))
                  .status,
            ).toBe(200);
         } finally {
            process.env.PUBLISHER_PACKAGE_VERSIONING = "on";
         }
         const unversioned = await setLatest("1.0.0", "plain");
         expect(unversioned.status).toBe(404);
         expect(await reasonOf(unversioned)).toBe("VERSION_NOT_FOUND");
         expect(await n(undefined, "plain")).toBe(50);
      });

      it("refuses a body without a version", async () => {
         expect((await setLatest(undefined)).status).toBe(400);
         expect((await setLatest(7)).status).toBe(400);
         expect((await setLatest("")).status).toBe(400);
      });

      it("is how latest moves under explicit promotion", async () => {
         process.env.PUBLISHER_VERSION_PROMOTION = "explicit";
         try {
            expect(
               (await publish(await packageDir("1.0.0", 60, "held"), "held"))
                  .status,
            ).toBe(200);
            expect((await answer(undefined, "held")).status).toBe(404);
            expect((await setLatest("1.0.0", "held")).status).toBe(200);
            expect(await n(undefined, "held")).toBe(60);
            // A later publish is servable by name but leaves latest alone.
            expect(
               (await publish(await packageDir("1.1.0", 61, "held"), "held"))
                  .status,
            ).toBe(200);
            expect(await n(undefined, "held")).toBe(60);
            expect(await n("1.1.0", "held")).toBe(61);
         } finally {
            delete process.env.PUBLISHER_VERSION_PROMOTION;
         }
      });
   });

   describe("PUT …/versions/{versionId}/manifest", () => {
      let manifestFile: string;

      beforeAll(async () => {
         manifestFile = path.join(scratch, "manifest.json");
         await fs.writeFile(
            manifestFile,
            JSON.stringify({
               builtAt: new Date().toISOString(),
               strict: false,
               entries: {},
            }),
         );
      });

      it("binds one version and leaves the others as they were", async () => {
         const res = await setManifest("1.1.0", {
            manifestLocation: manifestFile,
         });
         expect(res.status).toBe(200);
         expect(await res.json()).toMatchObject({
            name: PKG,
            versionId: "1.1.0",
            manifestBindingStatus: "bound",
            boundManifestUri: manifestFile,
         });
         const listed = await versions();
         expect(listed.find((v) => v.id === "1.1.0")?.manifestLocation).toBe(
            manifestFile,
         );
         expect(
            listed.find((v) => v.id === "2.1.0")?.manifestLocation ?? null,
         ).toBeNull();
         const other = (await (
            await fetch(api(`/packages/${PKG}?versionId=2.1.0`))
         ).json()) as { manifestBindingStatus?: string };
         expect(other.manifestBindingStatus).toBe("unbound");
      });

      it("keeps the binding when the version is unloaded and loads again", async () => {
         // Moving latest away and back unloads 1.1.0 and reloads it from the
         // registry row, which is where the binding lives.
         expect((await setLatest("1.1.0")).status).toBe(200);
         expect((await setLatest("2.1.0")).status).toBe(200);
         const reloaded = (await (
            await fetch(api(`/packages/${PKG}?versionId=1.1.0`))
         ).json()) as { boundManifestUri?: string };
         expect(reloaded.boundManifestUri).toBe(manifestFile);
      });

      it("serves the version live again when the location is cleared", async () => {
         const res = await setManifest("1.1.0", { manifestLocation: null });
         expect(res.status).toBe(200);
         expect(await res.json()).toMatchObject({
            versionId: "1.1.0",
            manifestBindingStatus: "unbound",
         });
         expect(
            (await versions()).find((v) => v.id === "1.1.0")
               ?.manifestLocation ?? null,
         ).toBeNull();
         expect(await n("1.1.0")).toBe(2);
      });

      it("refuses a missing or malformed location, and an unknown version", async () => {
         expect((await setManifest("1.1.0", {})).status).toBe(400);
         expect(
            (await setManifest("1.1.0", { manifestLocation: 3 })).status,
         ).toBe(400);
         const unknown = await setManifest("9.9.9", {
            manifestLocation: null,
         });
         expect(unknown.status).toBe(404);
         expect(await reasonOf(unknown)).toBe("VERSION_NOT_FOUND");
      });
   });

   describe("PATCH …/versions/{versionId}", () => {
      it("refuses to archive latest", async () => {
         const res = await setArchive("2.1.0", "archive");
         expect(res.status).toBe(409);
         expect(await reasonOf(res)).toBe("VERSION_IS_LATEST");
         expect(await n()).toBe(4);
      });

      it("takes an archived version out of service, keeping its files", async () => {
         expect(await n("1.0.0")).toBe(1);
         const res = await setArchive("1.0.0", "archive");
         expect(res.status).toBe(200);
         const body = (await res.json()) as VersionBody;
         expect(body.archiveStatus).toBe("archive");
         expect(body.archivedAt).not.toBeNull();

         const read = await answer("1.0.0");
         expect(read.status).toBe(410);
         expect(await reasonOf(read)).toBe("VERSION_ARCHIVED");
         const pkg = await fetch(api(`/packages/${PKG}?versionId=1.0.0`));
         expect(pkg.status).toBe(410);
         expect(await loadedVersions()).not.toContain("1.0.0");

         // Still listed, and still on disk.
         expect(
            (await versions()).find((v) => v.id === "1.0.0")?.archiveStatus,
         ).toBe("archive");
         // The server root is the working directory (SERVER_ROOT unset).
         const tree = path.resolve(
            process.env.SERVER_ROOT || ".",
            "publisher_data",
            ENV_NAME,
            PKG,
            "1.0.0",
            "report.malloy",
         );
         expect(await fs.readFile(tree, "utf8")).toContain("SELECT 1");
      });

      it("answers 200 and changes nothing when the version is already in that state", async () => {
         const again = await setArchive("1.0.0", "archive");
         expect(again.status).toBe(200);
         expect(((await again.json()) as VersionBody).archiveStatus).toBe(
            "archive",
         );
      });

      it("refuses to make an archived version latest, or to bind it", async () => {
         const latest = await setLatest("1.0.0");
         expect(latest.status).toBe(410);
         expect(await reasonOf(latest)).toBe("VERSION_ARCHIVED");
         // A real location, so a write that happened anyway would show.
         const manifest = await setManifest("1.0.0", {
            manifestLocation: path.join(scratch, "never-bound.json"),
         });
         expect(manifest.status).toBe(410);
         expect(
            (await versions()).find((v) => v.id === "1.0.0")
               ?.manifestLocation ?? null,
         ).toBeNull();
      });

      it("refuses a republish of an archived version", async () => {
         const res = await publish(await packageDir("1.0.0", 1));
         expect(res.status).toBe(410);
         expect(await reasonOf(res)).toBe("VERSION_ARCHIVED");
      });

      it("puts an unarchived version back in service", async () => {
         const res = await setArchive("1.0.0", "unarchive");
         expect(res.status).toBe(200);
         const body = (await res.json()) as VersionBody;
         expect(body.archiveStatus).toBe("unarchive");
         expect(body.archivedAt).toBeNull();
         expect(await n("1.0.0")).toBe(1);
      });

      it("refuses an unknown state, and an unknown version", async () => {
         expect((await setArchive("1.0.0", "delete")).status).toBe(400);
         expect((await setArchive("1.0.0", undefined)).status).toBe(400);
         const unknown = await setArchive("9.9.9", "archive");
         expect(unknown.status).toBe(404);
         expect(await reasonOf(unknown)).toBe("VERSION_NOT_FOUND");
      });
   });
});
