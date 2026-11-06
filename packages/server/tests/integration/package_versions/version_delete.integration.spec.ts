// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * `DELETE .../packages/{pkg}/versions/{version}` over HTTP: the version goes
 * (its reads answer 404, it leaves /status and the listing) while the others
 * keep serving, its id is free for a new publish, an archived version can be
 * deleted, the package's only version takes the package, and each refusal it
 * declares.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";
import { versioningOnForSuite } from "../../harness/versioning";

const ENV_NAME = "package-versions-delete-env";
const PKG = "deletable";
const SOLO = "solo";

let env: (RestE2EEnv & { stop(): Promise<void> }) | undefined;
let baseUrl = "";
let root = "";
let written = 0;

function writePackage(name: string, version: string, answer: number): string {
   const dir = path.join(root, `${name}-${++written}`);
   fs.mkdirSync(dir, { recursive: true });
   fs.writeFileSync(
      path.join(dir, "publisher.json"),
      JSON.stringify({ name, version }),
   );
   fs.writeFileSync(
      path.join(dir, "model.malloy"),
      `source: numbers is duckdb.sql("SELECT ${answer} AS answer") extend {\n` +
         `  view: which_version is { select: answer }\n}\n`,
   );
   return dir;
}

const packageUrl = (name: string, suffix = "") =>
   `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${name}${suffix}`;

async function call(
   method: string,
   url: string,
   body?: unknown,
): Promise<{ status: number; json: Record<string, unknown> }> {
   const res = await fetch(url, {
      method,
      headers: { "content-type": "application/json" },
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
   });
   const text = await res.text();
   return {
      status: res.status,
      json: (text ? JSON.parse(text) : {}) as Record<string, unknown>,
   };
}

async function publish(name: string, version: string, answer: number) {
   const res = await call(
      "POST",
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages`,
      {
         name,
         location: writePackage(name, version, answer),
      },
   );
   expect(res.status).toBe(200);
}

async function answerOf(versionId: string): Promise<number> {
   const { status, json } = await call(
      "POST",
      packageUrl(PKG, "/models/model.malloy/query"),
      {
         query: "run: numbers -> which_version",
         compactJson: true,
         versionId,
      },
   );
   expect(status).toBe(200);
   return Number(
      (JSON.parse(String(json.result)) as { answer: number }[])[0].answer,
   );
}

async function versionIds(name: string): Promise<string[]> {
   const { status, json } = await call("GET", packageUrl(name, "/versions"));
   expect(status).toBe(200);
   return (json as unknown as { id: string }[]).map((v) => v.id);
}

describe("deleting a version", () => {
   versioningOnForSuite();

   beforeAll(async () => {
      root = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "package-versions-delete-")),
      );
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      const created = await call("POST", `${baseUrl}/api/v0/environments`, {
         name: ENV_NAME,
         packages: [],
         connections: [],
      });
      expect(created.status).toBeLessThan(300);
      await publish(PKG, "1.0.0", 1);
      await publish(PKG, "1.1.0", 2);
      await publish(PKG, "2.0.0", 3);
   }, 180_000);

   afterAll(async () => {
      await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
         method: "DELETE",
      }).catch(() => undefined);
      await env?.stop();
      fs.rmSync(root, { recursive: true, force: true });
   });

   it("refuses latest, an unknown version and a malformed one", async () => {
      const latest = await call("DELETE", packageUrl(PKG, "/versions/2.0.0"));
      expect(latest.status).toBe(409);
      expect(latest.json.reason).toBe("VERSION_IS_LATEST");
      const unknown = await call("DELETE", packageUrl(PKG, "/versions/9.9.9"));
      expect(unknown.status).toBe(404);
      expect(unknown.json.reason).toBe("VERSION_NOT_FOUND");
      const malformed = await call("DELETE", packageUrl(PKG, "/versions/v1"));
      expect(malformed.status).toBe(400);
      expect(malformed.json.reason).toBe("VERSION_ID_INVALID");
      expect(await versionIds(PKG)).toEqual(["2.0.0", "1.1.0", "1.0.0"]);
   });

   it("deletes a version: it answers 404 and leaves /status, the others keep serving", async () => {
      expect(await answerOf("1.0.0")).toBe(1);
      const deleted = await call("DELETE", packageUrl(PKG, "/versions/1.0.0"));
      expect(deleted.status).toBe(200);
      expect(deleted.json.id).toBe("1.0.0");
      expect(deleted.json.packageName).toBe(PKG);

      const read = await call("GET", packageUrl(PKG, "/versions/1.0.0"));
      expect(read.status).toBe(404);
      expect(read.json.reason).toBe("VERSION_NOT_FOUND");
      const query = await call(
         "POST",
         packageUrl(PKG, "/models/model.malloy/query"),
         { query: "run: numbers -> which_version", versionId: "1.0.0" },
      );
      expect(query.status).toBe(404);
      expect(await versionIds(PKG)).toEqual(["2.0.0", "1.1.0"]);

      const status = await call("GET", `${baseUrl}/api/v0/status`);
      const environment = (
         status.json.environments as {
            name: string;
            packages?: { name: string; versionId?: string }[];
         }[]
      ).find((e) => e.name === ENV_NAME);
      const held = (environment?.packages ?? [])
         .filter((p) => p.name === PKG)
         .map((p) => p.versionId)
         .sort();
      expect(held).toEqual(["1.1.0", "2.0.0"]);

      expect(await answerOf("1.1.0")).toBe(2);
      expect(await answerOf("2.0.0")).toBe(3);
   });

   it("frees the version id: a publish of it with other content is a new version", async () => {
      await publish(PKG, "1.0.0", 11);
      expect(await answerOf("1.0.0")).toBe(11);
      expect(await versionIds(PKG)).toEqual(["2.0.0", "1.1.0", "1.0.0"]);
   });

   it("deletes an archived version", async () => {
      const archived = await call("PATCH", packageUrl(PKG, "/versions/1.1.0"), {
         archiveStatus: "archive",
      });
      expect(archived.status).toBe(200);
      const deleted = await call("DELETE", packageUrl(PKG, "/versions/1.1.0"));
      expect(deleted.status).toBe(200);
      expect(deleted.json.archiveStatus).toBe("archive");
      expect(await versionIds(PKG)).toEqual(["2.0.0", "1.0.0"]);
   });

   it("deleting a package's only version deletes the package", async () => {
      await publish(SOLO, "1.0.0", 5);
      const deleted = await call("DELETE", packageUrl(SOLO, "/versions/1.0.0"));
      expect(deleted.status).toBe(200);
      const pkg = await call("GET", packageUrl(SOLO));
      expect(pkg.status).toBe(404);
      const listed = await call(
         "GET",
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages`,
      );
      expect(
         (listed.json as unknown as { name: string }[]).map((p) => p.name),
      ).not.toContain(SOLO);
      // And it can be published again from scratch.
      await publish(SOLO, "1.0.0", 6);
      expect(await versionIds(SOLO)).toEqual(["1.0.0"]);
   });
});
