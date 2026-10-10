// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * A publish from a location whose publisher.json declares no semantic
 * version, over HTTP: it does what every publish from a location did before
 * versions. Each publish answers 200 and replaces the package in place, the
 * same location or another, the same content or new; the package has no
 * versions. Only a package that already has published versions refuses such a
 * tree, with the version it does not declare.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const ENV_NAME = "unversioned-publish-env";

let env: (RestE2EEnv & { stop(): Promise<void> }) | undefined;
let baseUrl = "";
let root = "";

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

async function publish(
   name: string,
   location: string,
): Promise<{ status: number; json: Record<string, unknown> }> {
   const res = await fetch(
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages`,
      {
         method: "POST",
         headers: { "content-type": "application/json" },
         body: JSON.stringify({ name, location }),
      },
   );
   return {
      status: res.status,
      json: (await res.json()) as Record<string, unknown>,
   };
}

async function answerOf(name: string): Promise<number> {
   const res = await fetch(
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${name}/models/model.malloy/query`,
      {
         method: "POST",
         headers: { "content-type": "application/json" },
         body: JSON.stringify({
            query: "run: numbers -> which",
            compactJson: true,
         }),
      },
   );
   expect(res.status).toBe(200);
   const json = (await res.json()) as { result: string };
   return Number((JSON.parse(json.result) as { answer: number }[])[0].answer);
}

describe("publishing from a location without a semantic version", () => {
   beforeAll(async () => {
      root = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "unversioned-publish-")),
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

   afterAll(async () => {
      await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
         method: "DELETE",
      }).catch(() => undefined);
      await env?.stop();
      fs.rmSync(root, { recursive: true, force: true });
   });

   it("answers 200 every time and replaces the package, as before versions", async () => {
      const first = writePackage("plain-1", { name: "plain" }, 1);
      const published = await publish("plain", first);
      expect(published.status).toBe(200);
      expect(published.json.versionId).toBeUndefined();
      expect(await answerOf("plain")).toBe(1);

      // The same location again: 200, and the package is unchanged.
      expect((await publish("plain", first)).status).toBe(200);
      expect(await answerOf("plain")).toBe(1);

      // Other content, from another location: 200, and it replaces the first.
      const second = writePackage("plain-2", { name: "plain" }, 2);
      expect((await publish("plain", second)).status).toBe(200);
      expect(await answerOf("plain")).toBe(2);

      // Other content at the first location: 200, replaced again.
      writePackage("plain-1", { name: "plain" }, 3);
      expect((await publish("plain", first)).status).toBe(200);
      expect(await answerOf("plain")).toBe(3);

      // It has no versions: a version named is no version of it.
      const named = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/plain?versionId=1.0.0`,
      );
      expect(named.status).toBe(404);
   });

   it("treats a version that is not semver the same way", async () => {
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

   it("refuses it for a package that has published versions, naming the missing version", async () => {
      const versioned = writePackage(
         "versioned-1",
         { name: "versioned", version: "1.0.0" },
         1,
      );
      expect((await publish("versioned", versioned)).status).toBe(200);

      const missing = await publish(
         "versioned",
         writePackage("versioned-none", { name: "versioned" }, 2),
      );
      expect([missing.status, missing.json.reason]).toEqual([
         400,
         "MANIFEST_VERSION_MISSING",
      ]);
      const invalid = await publish(
         "versioned",
         writePackage(
            "versioned-bad",
            { name: "versioned", version: "1.0" },
            3,
         ),
      );
      expect([invalid.status, invalid.json.reason]).toEqual([
         400,
         "MANIFEST_VERSION_INVALID",
      ]);
      // The published version still serves.
      expect(await answerOf("versioned")).toBe(1);
   });
});
