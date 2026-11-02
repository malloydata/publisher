// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * `PUBLISHER_PACKAGE_VERSIONING`, the transition flag, over HTTP.
 *
 * - Off (unset, the default): a publish from a location does what it did
 *   before versions, whatever the tree's publisher.json says. Each publish
 *   answers 200 and replaces the package in place; the package has no
 *   versions.
 * - On: a publish from a location publishes the version its publisher.json
 *   declares, and a tree that declares none, or one that is not semver, is
 *   refused with 400.
 *
 * The flag is read when a publish runs, so one server is switched between the
 * two here. Reads and the versions routes work either way.
 */

import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const ENV_NAME = "versioning-flag-env";
const FLAG = "PUBLISHER_PACKAGE_VERSIONING";

let env: (RestE2EEnv & { stop(): Promise<void> }) | undefined;
let baseUrl = "";
let root = "";
const savedFlag = process.env[FLAG];

function versioning(value: "on" | "off" | undefined): void {
   if (value === undefined) delete process.env[FLAG];
   else process.env[FLAG] = value;
}

/** A package folder answering `answer`, with `manifest` as publisher.json. */
function writePackage(
   folder: string,
   manifest: Record<string, unknown>,
   answer: number,
): string {
   const dir = path.join(root, folder);
   fs.mkdirSync(dir, { recursive: true });
   fs.writeFileSync(path.join(dir, "publisher.json"), JSON.stringify(manifest));
   fs.writeFileSync(
      path.join(dir, "model.malloy"),
      `source: numbers is duckdb.sql("SELECT ${answer} AS answer") extend {\n` +
         `  view: which is { select: answer }\n}\n`,
   );
   return dir;
}

const api = (sub: string) =>
   `${baseUrl}/api/v0/environments/${ENV_NAME}/packages${sub}`;

async function publish(
   name: string,
   location: string,
): Promise<{ status: number; json: Record<string, unknown> }> {
   const res = await fetch(api(""), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ name, location }),
   });
   return {
      status: res.status,
      json: (await res.json()) as Record<string, unknown>,
   };
}

async function answerOf(name: string, query = ""): Promise<number> {
   const res = await fetch(api(`/${name}/models/model.malloy/query${query}`), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
         query: "run: numbers -> which",
         compactJson: true,
      }),
   });
   expect(res.status).toBe(200);
   const json = (await res.json()) as { result: string };
   return Number((JSON.parse(json.result) as { answer: number }[])[0].answer);
}

async function packageVersioning(): Promise<unknown> {
   const status = (await (await fetch(`${baseUrl}/api/v0/status`)).json()) as {
      packageVersioning?: unknown;
   };
   return status.packageVersioning;
}

describe("PUBLISHER_PACKAGE_VERSIONING", () => {
   beforeAll(async () => {
      versioning(undefined);
      root = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "versioning-flag-")),
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
   }, 180_000);

   afterEach(() => {
      versioning(undefined);
   });

   afterAll(async () => {
      await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
         method: "DELETE",
      }).catch(() => undefined);
      await env?.stop();
      fs.rmSync(root, { recursive: true, force: true });
      if (savedFlag === undefined) delete process.env[FLAG];
      else process.env[FLAG] = savedFlag;
   });

   it("/status reports the setting", async () => {
      expect(await packageVersioning()).toBe("off");
      versioning("on");
      expect(await packageVersioning()).toBe("on");
   });

   describe("off, the default", () => {
      it("answers 200 every time and replaces the package, as before versions", async () => {
         const first = writePackage("plain-1", { name: "plain" }, 1);
         const published = await publish("plain", first);
         expect(published.status).toBe(200);
         expect(published.json.versionId).toBeUndefined();
         expect(await answerOf("plain")).toBe(1);

         // The same location again: 200, and the package is unchanged.
         expect((await publish("plain", first)).status).toBe(200);
         expect(await answerOf("plain")).toBe(1);

         // Other content, from another location: 200, and it replaces the
         // first.
         const second = writePackage("plain-2", { name: "plain" }, 2);
         expect((await publish("plain", second)).status).toBe(200);
         expect(await answerOf("plain")).toBe(2);

         // Other content at the first location: 200, replaced again.
         writePackage("plain-1", { name: "plain" }, 3);
         expect((await publish("plain", first)).status).toBe(200);
         expect(await answerOf("plain")).toBe(3);

         // It has no versions: a version named is no version of it.
         expect((await fetch(api("/plain?versionId=1.0.0"))).status).toBe(404);
      });

      it("installs a tree that declares a semantic version in place too, never answering 409", async () => {
         // The real off switch: the version in publisher.json is not read, so
         // changed content under the same version replaces the package.
         const first = writePackage(
            "semver-1",
            { name: "semver", version: "1.0.0" },
            1,
         );
         const published = await publish("semver", first);
         expect(published.status).toBe(200);
         expect(published.json.versionId).toBeUndefined();
         const changed = writePackage(
            "semver-2",
            { name: "semver", version: "1.0.0" },
            2,
         );
         expect((await publish("semver", changed)).status).toBe(200);
         expect(await answerOf("semver")).toBe(2);
         const versions = (await (
            await fetch(api("/semver/versions"))
         ).json()) as unknown[];
         expect(versions).toEqual([]);
      });

      it("installs a version that is not semver in place", async () => {
         const first = writePackage(
            "dated-1",
            { name: "dated", version: "1.0" },
            1,
         );
         expect((await publish("dated", first)).status).toBe(200);
         const second = writePackage(
            "dated-2",
            { name: "dated", version: "2026_08_15_1200" },
            2,
         );
         expect((await publish("dated", second)).status).toBe(200);
         expect(await answerOf("dated")).toBe(2);
      });
   });

   describe("on", () => {
      it("refuses a tree that declares no version, or one that is not semver, and creates nothing", async () => {
         versioning("on");
         const missing = await publish(
            "strict",
            writePackage("strict-none", { name: "strict" }, 1),
         );
         expect([missing.status, missing.json.reason]).toEqual([
            400,
            "MANIFEST_VERSION_MISSING",
         ]);
         const invalid = await publish(
            "strict",
            writePackage("strict-bad", { name: "strict", version: "1.0" }, 2),
         );
         expect([invalid.status, invalid.json.reason]).toEqual([
            400,
            "MANIFEST_VERSION_INVALID",
         ]);
         expect((await fetch(api("/strict"))).status).toBe(404);
      });

      it("publishes a version, a package that was installed in place included", async () => {
         // Installed in place while off, then published as a version once on:
         // the version replaces the single slot and is what reads get.
         const plain = writePackage("moved-0", { name: "moved" }, 1);
         expect((await publish("moved", plain)).status).toBe(200);

         versioning("on");
         const versioned = writePackage(
            "moved-1",
            { name: "moved", version: "1.0.0" },
            2,
         );
         const published = await publish("moved", versioned);
         expect([published.status, published.json.versionId]).toEqual([
            200,
            "1.0.0",
         ]);
         expect(await answerOf("moved")).toBe(2);
         expect(await answerOf("moved", "?versionId=1.0.0")).toBe(2);
      });
   });

   it("off again: versions published while on keep serving, and are not replaced in place", async () => {
      versioning("on");
      const v1 = writePackage("kept-1", { name: "kept", version: "1.0.0" }, 1);
      expect((await publish("kept", v1)).status).toBe(200);

      versioning("off");
      expect(await answerOf("kept")).toBe(1);
      expect(await answerOf("kept", "?versionId=1.0.0")).toBe(1);
      const replaced = await publish(
         "kept",
         writePackage("kept-2", { name: "kept" }, 2),
      );
      expect([replaced.status, replaced.json.reason]).toEqual([
         409,
         "PACKAGE_IS_VERSIONED",
      ]);
      expect(String(replaced.json.message)).toContain(`${FLAG}=on`);
      expect(await answerOf("kept")).toBe(1);
   });
});
