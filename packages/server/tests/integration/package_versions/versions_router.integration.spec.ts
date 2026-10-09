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
const savedFlag = process.env.PUBLISHER_PACKAGE_VERSIONING;

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
      process.env.PUBLISHER_PACKAGE_VERSIONING = "on";
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
      if (savedFlag === undefined)
         delete process.env.PUBLISHER_PACKAGE_VERSIONING;
      else process.env.PUBLISHER_PACKAGE_VERSIONING = savedFlag;
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

   it("answers 501 on every versions route with versioning off", async () => {
      process.env.PUBLISHER_PACKAGE_VERSIONING = "off";
      try {
         for (const [method, suffix, body] of [
            ["GET", "/versions", undefined],
            ["GET", "/versions/1.0.0", undefined],
            ["PATCH", "/versions/1.0.0", { archiveStatus: "archive" }],
            ["PUT", "/versions/1.0.0/manifest", { manifestLocation: null }],
            ["PUT", "/latest", { version: "1.0.0" }],
         ] as const) {
            expect([suffix, (await call(method, suffix, body)).status]).toEqual(
               [suffix, 501],
            );
         }
      } finally {
         process.env.PUBLISHER_PACKAGE_VERSIONING = "on";
      }
   });
});
