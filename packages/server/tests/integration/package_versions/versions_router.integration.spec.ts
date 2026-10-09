// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * The versions routes over HTTP: list, get, archive/unarchive (and what an
 * archived version answers on a read route), manifest binding, and moving
 * `latest`, with every refusal they declare; and the 501 they answer with
 * package versioning off.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const ENV_NAME = "package-versions-router-env";
const PKG = "lifecycle";

let env: (RestE2EEnv & { stop(): Promise<void> }) | undefined;
let baseUrl = "";
let root = "";

function writePackage(version: string, answer: number): string {
   const dir = path.join(root, `v${answer}`);
   fs.mkdirSync(dir, { recursive: true });
   fs.writeFileSync(
      path.join(dir, "publisher.json"),
      JSON.stringify({ name: PKG, version, description: `release ${version}` }),
   );
   fs.writeFileSync(
      path.join(dir, "model.malloy"),
      `source: numbers is duckdb.sql("SELECT ${answer} AS answer") extend {\n` +
         `  view: which_version is { select: answer }\n}\n`,
   );
   return dir;
}

const api = (suffix: string) =>
   `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}${suffix}`;

async function call(
   method: string,
   suffix: string,
   body?: unknown,
): Promise<{ status: number; json: Record<string, unknown> }> {
   const res = await fetch(api(suffix), {
      method,
      headers: { "content-type": "application/json" },
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
   });
   return {
      status: res.status,
      json: (await res.json()) as Record<string, unknown>,
   };
}

async function answerOf(versionId?: string): Promise<number> {
   const { status, json } = await call("POST", "/models/model.malloy/query", {
      query: "run: numbers -> which_version",
      compactJson: true,
      ...(versionId ? { versionId } : {}),
   });
   expect(status).toBe(200);
   return Number(
      (JSON.parse(String(json.result)) as { answer: number }[])[0].answer,
   );
}

describe("versions routes", () => {
   beforeAll(async () => {
      root = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "package-versions-router-")),
      );
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      const created = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "content-type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [],
            connections: [],
         }),
      });
      expect(created.status).toBeLessThan(300);
      for (const [version, answer] of [
         ["1.0.0", 1],
         ["1.1.0", 2],
         ["2.0.0", 3],
      ] as const) {
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages`,
            {
               method: "POST",
               headers: { "content-type": "application/json" },
               body: JSON.stringify({
                  name: PKG,
                  location: writePackage(version, answer),
               }),
            },
         );
         expect(res.status).toBe(200);
      }
   }, 180_000);

   afterAll(async () => {
      await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
         method: "DELETE",
      }).catch(() => undefined);
      await env?.stop();
      fs.rmSync(root, { recursive: true, force: true });
   });

   it("lists every version, highest first, marking latest", async () => {
      const res = await fetch(api("/versions"));
      expect(res.status).toBe(200);
      const versions = (await res.json()) as Record<string, unknown>[];
      expect(versions.map((v) => [v.id, v.latest])).toEqual([
         ["2.0.0", true],
         ["1.1.0", false],
         ["1.0.0", false],
      ]);
      expect(versions[0]).toMatchObject({
         packageName: PKG,
         archiveStatus: "unarchive",
         archivedAt: null,
         description: "release 2.0.0",
         manifestLocation: null,
         gitCommitSha: null,
         gitRef: null,
         resource: `/api/v0/environments/${ENV_NAME}/packages/${PKG}/versions/2.0.0`,
      });
      expect(typeof versions[0].contentHash).toBe("string");
      expect(typeof versions[0].promotedAt).toBe("string");
      expect(versions[1].demotedAt).not.toBeNull();
   });

   it("gets one version, and refuses a malformed or unknown one", async () => {
      expect((await call("GET", "/versions/1.0.0")).json.id).toBe("1.0.0");
      const missing = await call("GET", "/versions/9.9.9");
      expect([missing.status, missing.json.reason]).toEqual([
         404,
         "VERSION_NOT_FOUND",
      ]);
      const malformed = await call("GET", "/versions/latest");
      expect([malformed.status, malformed.json.reason]).toEqual([
         400,
         "VERSION_ID_INVALID",
      ]);
      expect(
         (
            await fetch(
               `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/no-such/versions`,
            )
         ).status,
      ).toBe(404);
   });

   it("archives a version: reads that name it answer 410, others are unaffected", async () => {
      const archived = await call("PATCH", "/versions/1.0.0", {
         archiveStatus: "archive",
      });
      expect(archived.status).toBe(200);
      expect(archived.json.archiveStatus).toBe("archive");
      expect(typeof archived.json.archivedAt).toBe("string");

      const read = await call("GET", "?versionId=1.0.0");
      expect([read.status, read.json.reason]).toEqual([
         410,
         "VERSION_ARCHIVED",
      ]);
      expect(await answerOf("1.1.0")).toBe(2);
      expect(await answerOf()).toBe(3);

      const back = await call("PATCH", "/versions/1.0.0", {
         archiveStatus: "unarchive",
      });
      expect(back.json.archiveStatus).toBe("unarchive");
      expect(await answerOf("1.0.0")).toBe(1);
   });

   it("takes versionId on every materialization route", async () => {
      const listed = await call("GET", "/materializations?versionId=1.1.0");
      expect([listed.status, listed.json]).toEqual([200, []]);
      for (const [versionId, status, reason] of [
         ["9.9.9", 404, "VERSION_NOT_FOUND"],
         ["not-a-version", 400, "VERSION_ID_INVALID"],
      ] as const) {
         const answers = [
            await call("GET", `/materializations?versionId=${versionId}`),
            await call("POST", "/materializations", { versionId }),
            await call("GET", `/materializations/m-1?versionId=${versionId}`),
            await call(
               "POST",
               `/materializations/m-1?action=stop&versionId=${versionId}`,
            ),
            await call(
               "DELETE",
               `/materializations/m-1?versionId=${versionId}`,
            ),
         ];
         for (const answer of answers) {
            expect([answer.status, answer.json.reason]).toEqual([
               status,
               reason,
            ]);
         }
      }
   });

   it("refuses to archive latest, and refuses a bad body", async () => {
      const latest = await call("PATCH", "/versions/2.0.0", {
         archiveStatus: "archive",
      });
      expect([latest.status, latest.json.reason]).toEqual([
         409,
         "VERSION_IS_LATEST",
      ]);
      const bad = await call("PATCH", "/versions/1.0.0", {
         archiveStatus: "delete",
      });
      expect(bad.status).toBe(400);
   });

   it("moves latest with PUT .../latest {version}, and refuses an unknown or archived target", async () => {
      const moved = await call("PUT", "/latest", { version: "1.1.0" });
      expect(moved.status).toBe(200);
      expect(moved.json).toMatchObject({ id: "1.1.0", latest: true });
      expect(await answerOf()).toBe(2);
      expect((await call("GET", "")).json.versionId).toBe("1.1.0");

      const missing = await call("PUT", "/latest", { version: "9.9.9" });
      expect([missing.status, missing.json.reason]).toEqual([
         404,
         "VERSION_NOT_FOUND",
      ]);
      await call("PATCH", "/versions/1.0.0", { archiveStatus: "archive" });
      const archived = await call("PUT", "/latest", { version: "1.0.0" });
      expect([archived.status, archived.json.reason]).toEqual([
         410,
         "VERSION_ARCHIVED",
      ]);
      // The old body name is not the contract.
      const old = await call("PUT", "/latest", { versionId: "2.0.0" });
      expect([old.status, old.json.reason]).toEqual([
         400,
         "VERSION_ID_INVALID",
      ]);
      await call("PATCH", "/versions/1.0.0", { archiveStatus: "unarchive" });
      await call("PUT", "/latest", { version: "2.0.0" });
      expect(await answerOf()).toBe(3);
   });

   it("binds a version's manifest, refuses an empty location, and clears it with null", async () => {
      const empty = await call("PUT", "/versions/1.1.0/manifest", {
         manifestLocation: "",
      });
      expect(empty.status).toBe(400);
      const missing = await call("PUT", "/versions/1.1.0/manifest", {});
      expect(missing.status).toBe(400);

      const cleared = await call("PUT", "/versions/1.1.0/manifest", {
         manifestLocation: null,
      });
      expect(cleared.status).toBe(200);
      expect(cleared.json.versionId).toBe("1.1.0");
      expect(
         (await call("GET", "/versions/1.1.0")).json.manifestLocation,
      ).toBeNull();

      const unknown = await call("PUT", "/versions/9.9.9/manifest", {
         manifestLocation: null,
      });
      expect(unknown.status).toBe(404);
   });

   it("lists every version on /status, with the versioning settings", async () => {
      const status = (await (
         await fetch(`${baseUrl}/api/v0/status`)
      ).json()) as {
         packageVersioning?: string;
         versionPromotion?: string;
         environments: {
            name: string;
            packages?: Record<string, unknown>[];
         }[];
      };
      expect(status.packageVersioning).toBe("on");
      expect(status.versionPromotion).toBe("on-publish");
      const entries = (
         status.environments.find((e) => e.name === ENV_NAME)?.packages ?? []
      ).filter((p) => p.name === PKG);
      expect(
         entries
            .map((p) => [p.versionId, p.latestVersion, p.archiveStatus])
            .sort(),
      ).toEqual([
         ["1.0.0", "2.0.0", "unarchive"],
         ["1.1.0", "2.0.0", "unarchive"],
         ["2.0.0", "2.0.0", "unarchive"],
      ]);
      for (const entry of entries) {
         expect(entry.resource).toBe(
            `/api/v0/environments/${ENV_NAME}/packages/${PKG}`,
         );
         expect(typeof entry.loaded).toBe("boolean");
         expect(entry.status).toMatchObject({ serving: entry.loaded });
      }
   });

   it("lists the package once, as its latest, and its GET carries latestVersion", async () => {
      const listed = (await (
         await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}/packages`)
      ).json()) as Record<string, unknown>[];
      const mine = listed.filter((p) => p.name === PKG);
      expect(mine).toHaveLength(1);
      expect(mine[0]).toMatchObject({
         versionId: "2.0.0",
         latestVersion: "2.0.0",
         status: { serving: true },
      });
      const v1 = await call("GET", "?versionId=1.0.0");
      expect(v1.json).toMatchObject({
         versionId: "1.0.0",
         latestVersion: "2.0.0",
      });
   });

   it("PATCH accepts the package echoed back, and a client's unset fields", async () => {
      // A client that reads the package and sends it back whole.
      const read = await call("GET", "");
      expect(read.status).toBe(200);
      const echoed = await call("PATCH", "", read.json);
      expect([echoed.status, echoed.json.reason]).toEqual([200, undefined]);

      // A generated client that serializes every field it knows, unset ones
      // as empty lists, null, or their defaults.
      const serialized = await call("PATCH", "", {
         name: null,
         location: null,
         manifestLocation: null,
         description: null,
         scope: "package",
         explores: [],
         exploresWarnings: [],
         warnings: [],
         queryableSources: null,
         storageServeBindings: [],
         materialization: null,
         queryMetadata: null,
      });
      expect([serialized.status, serialized.json.reason]).toEqual([
         200,
         undefined,
      ]);
   });

   it("PATCH refuses a description that is not text, and a manifest that is not gs:// or s3://", async () => {
      for (const body of [
         { description: 7 },
         { description: ["x"] },
         { manifestLocation: "/var/data/manifest.json" },
         { manifestLocation: "file:///etc/passwd" },
         { manifestLocation: 3 },
      ]) {
         expect([
            JSON.stringify(body),
            (await call("PATCH", "", body)).status,
         ]).toEqual([JSON.stringify(body), 400]);
      }
   });

   it("a package's own description reads back, and an environment update keeps it", async () => {
      expect(
         (await call("PATCH", "", { description: "Package-level" })).status,
      ).toBe(200);
      expect((await call("GET", "")).json.description).toBe("Package-level");
      const listed = async () =>
         (
            (await (
               await fetch(
                  `${baseUrl}/api/v0/environments/${ENV_NAME}/packages`,
               )
            ).json()) as { name: string; description?: string }[]
         ).find((p) => p.name === PKG);
      expect((await listed())?.description).toBe("Package-level");

      // Re-syncing the environment's rows (as any environment update does)
      // does not write latest's own description over it.
      const updated = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}`,
         {
            method: "PATCH",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ name: ENV_NAME }),
         },
      );
      expect(updated.status).toBeLessThan(300);
      expect((await listed())?.description).toBe("Package-level");
      // Each version keeps its own.
      expect((await call("GET", "/versions/2.0.0")).json.description).toBe(
         "release 2.0.0",
      );
   });

   it("with no description of its own, a package reads as its latest version's, even across an environment update", async () => {
      const name = "described-by-latest";
      const publishVersion = async (version: string) => {
         const dir = path.join(root, `${name}-${version}`);
         fs.mkdirSync(dir, { recursive: true });
         fs.writeFileSync(
            path.join(dir, "publisher.json"),
            JSON.stringify({ name, version, description: `about ${version}` }),
         );
         fs.writeFileSync(
            path.join(dir, "model.malloy"),
            'source: numbers is duckdb.sql("SELECT 1 AS answer")\n',
         );
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages`,
            {
               method: "POST",
               headers: { "content-type": "application/json" },
               body: JSON.stringify({ name, location: dir }),
            },
         );
         expect(res.status).toBe(200);
      };
      const described = async () =>
         (
            (await (
               await fetch(
                  `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${name}`,
               )
            ).json()) as { description?: string }
         ).description;

      await publishVersion("1.0.0");
      expect(await described()).toBe("about 1.0.0");
      const updated = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}`,
         {
            method: "PATCH",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ name: ENV_NAME }),
         },
      );
      expect(updated.status).toBeLessThan(300);
      await publishVersion("2.0.0");
      expect(await described()).toBe("about 2.0.0");
   });

   it("a failed first publish removes only the package row it created, keeping the package's runs", async () => {
      // A package folder the server loads lazily, with no row of its own, and
      // a materialization run recorded under its name.
      const environment = (await (
         await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`)
      ).json()) as { location?: string };
      expect(typeof environment.location).toBe("string");
      const lazy = path.join(environment.location!, "lazy");
      fs.mkdirSync(lazy, { recursive: true });
      fs.writeFileSync(
         path.join(lazy, "publisher.json"),
         JSON.stringify({ name: "lazy" }),
      );
      fs.writeFileSync(
         path.join(lazy, "model.malloy"),
         'source: numbers is duckdb.sql("SELECT 1 AS answer")\n',
      );
      const runs = `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/lazy/materializations`;
      const created = await fetch(runs, {
         method: "POST",
         headers: { "content-type": "application/json" },
         body: "{}",
      });
      expect(created.status).toBe(201);
      const run = (await created.json()) as { id: string };
      const deadline = Date.now() + 30_000;
      for (;;) {
         const status = (
            (await (await fetch(`${runs}/${run.id}`)).json()) as {
               status: string;
            }
         ).status;
         if (["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(status)) {
            break;
         }
         if (Date.now() > deadline) throw new Error("run never settled");
         await new Promise((resolve) => setTimeout(resolve, 100));
      }

      // A first versioned publish over it that fails to compile.
      const broken = path.join(root, "lazy-broken");
      fs.mkdirSync(broken, { recursive: true });
      fs.writeFileSync(
         path.join(broken, "publisher.json"),
         JSON.stringify({ name: "lazy", version: "1.0.0" }),
      );
      fs.writeFileSync(
         path.join(broken, "model.malloy"),
         "source: broken is not malloy\n",
      );
      const published = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages`,
         {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ name: "lazy", location: broken }),
         },
      );
      expect(published.status).toBeGreaterThanOrEqual(400);

      const listed = (await (await fetch(runs)).json()) as { id: string }[];
      expect(listed.map((m) => m.id)).toEqual([run.id]);
   });

   it("a package's description follows latest through an echoed PATCH, and its first version replaces an unversioned tree's", async () => {
      const name = "echoed";
      const environment = (await (
         await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`)
      ).json()) as { location?: string };
      // Unversioned first, its row synced from its own publisher.json.
      const lazy = path.join(environment.location!, name);
      fs.mkdirSync(lazy, { recursive: true });
      fs.writeFileSync(
         path.join(lazy, "publisher.json"),
         JSON.stringify({ name, description: "the unversioned tree" }),
      );
      fs.writeFileSync(
         path.join(lazy, "model.malloy"),
         'source: numbers is duckdb.sql("SELECT 1 AS answer")\n',
      );
      const api = `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${name}`;
      expect((await fetch(api)).status).toBe(200);
      const synced = await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
         method: "PATCH",
         headers: { "content-type": "application/json" },
         body: JSON.stringify({ name: ENV_NAME }),
      });
      expect(synced.status).toBeLessThan(300);

      const publishVersion = async (version: string) => {
         const dir = path.join(root, `${name}-${version}`);
         fs.mkdirSync(dir, { recursive: true });
         fs.writeFileSync(
            path.join(dir, "publisher.json"),
            JSON.stringify({ name, version, description: `about ${version}` }),
         );
         fs.writeFileSync(
            path.join(dir, "model.malloy"),
            'source: numbers is duckdb.sql("SELECT 1 AS answer")\n',
         );
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages`,
            {
               method: "POST",
               headers: { "content-type": "application/json" },
               body: JSON.stringify({ name, location: dir }),
            },
         );
         expect(res.status).toBe(200);
      };
      const described = async () =>
         ((await (await fetch(api)).json()) as { description?: string })
            .description;

      await publishVersion("1.0.0");
      expect(await described()).toBe("about 1.0.0");

      // A client that sends the package back whole sets nothing.
      const read = (await (await fetch(api)).json()) as Record<string, unknown>;
      const echoed = await fetch(api, {
         method: "PATCH",
         headers: { "content-type": "application/json" },
         body: JSON.stringify(read),
      });
      expect(echoed.status).toBe(200);
      await publishVersion("2.0.0");
      expect(await described()).toBe("about 2.0.0");
   });

   it("a package listing refuses a versionId, and reads an empty one as none", async () => {
      const listing = (query: string) =>
         fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}/packages${query}`);
      expect((await listing("?versionId=1.0.0")).status).toBe(400);
      expect((await listing("?versionId=")).status).toBe(200);
   });

   it("a publish refuses a manifest that is not gs:// or s3://, before it fetches anything", async () => {
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages`,
         {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
               name: "never-published",
               location: writePackage("1.0.0", 1),
               manifestLocation: "/var/data/manifest.json",
            }),
         },
      );
      expect(res.status).toBe(400);
      const versions = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/never-published/versions`,
      );
      // Nothing was published: the package does not exist.
      expect(versions.status).toBe(404);
   });

   it("PATCH changes a versioned package's description, not its versions'", async () => {
      const patched = await call("PATCH", "", { description: "changed" });
      expect(patched.status).toBe(200);
      // Each version keeps the description its own publisher.json gave it.
      expect((await call("GET", "/versions/2.0.0")).json.description).toBe(
         "release 2.0.0",
      );
   });

   it("binds only a gs:// or s3:// manifest URI", async () => {
      for (const manifestLocation of [
         "file:///etc/passwd",
         "/var/data/manifest.json",
         "https://example.com/m.json",
         "gs://",
         "",
      ]) {
         const refused = await call("PUT", "/versions/1.1.0/manifest", {
            manifestLocation,
         });
         expect([manifestLocation, refused.status]).toEqual([
            manifestLocation,
            400,
         ]);
      }
   });

   it("refuses lifecycle bodies of the wrong type with 400", async () => {
      for (const body of [
         { archiveStatus: ["archive"] },
         { archiveStatus: 1 },
         {},
      ]) {
         expect((await call("PATCH", "/versions/1.0.0", body)).status).toBe(
            400,
         );
      }
      for (const body of [
         { version: 1 },
         { version: ["1.0.0"] },
         { version: {} },
      ]) {
         const refused = await call("PUT", "/latest", body);
         expect([refused.status, refused.json.reason]).toEqual([
            400,
            "VERSION_ID_INVALID",
         ]);
      }
      for (const body of [
         { manifestLocation: 123 },
         { manifestLocation: [] },
      ]) {
         expect(
            (await call("PUT", "/versions/1.0.0/manifest", body)).status,
         ).toBe(400);
      }
   });

   it("round-trips a version with build metadata through the path and resource", async () => {
      const name = "lifecycle-build";
      const dir = path.join(root, "build-meta");
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name, version: "1.0.0+b.1" }),
      );
      fs.writeFileSync(
         path.join(dir, "model.malloy"),
         'source: numbers is duckdb.sql("SELECT 1 AS answer")\n',
      );
      const published = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages`,
         {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ name, location: dir }),
         },
      );
      expect(published.status).toBe(200);
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${name}/versions/1.0.0%2Bb.1`,
      );
      expect(res.status).toBe(200);
      const version = (await res.json()) as { id: string; resource: string };
      expect(version.id).toBe("1.0.0+b.1");
      expect(version.resource).toBe(
         `/api/v0/environments/${ENV_NAME}/packages/${name}/versions/1.0.0%2Bb.1`,
      );
      expect((await fetch(`${baseUrl}${version.resource}`)).status).toBe(200);
   });

   it("PATCH on a versioned package rebinds latest's manifest and sets the description", async () => {
      // The deprecated PATCH, as an orchestrator that rebinds through it sends
      // it: the location it published from echoed back, a new manifest.
      const versions = (await (await fetch(api("/versions"))).json()) as {
         id: string;
         location?: string;
      }[];
      const latest = versions.find((v) => v.id === "2.0.0")!;
      const patched = await call("PATCH", "", {
         name: PKG,
         location: latest.location,
         manifestLocation: null,
         description: "The package, described",
      });
      expect(patched.status).toBe(200);
      expect(patched.json).toMatchObject({ versionId: "2.0.0" });
      expect(
         (await call("GET", "/versions/2.0.0")).json.manifestLocation,
      ).toBeNull();

      const content = await call("PATCH", "", { explores: ["model.malloy"] });
      expect([content.status, content.json.reason]).toEqual([
         409,
         "PACKAGE_IS_VERSIONED",
      ]);
      const moved = await call("PATCH", "", { location: "/somewhere/else" });
      expect([moved.status, moved.json.reason]).toEqual([
         409,
         "PACKAGE_IS_VERSIONED",
      ]);
   });
});
