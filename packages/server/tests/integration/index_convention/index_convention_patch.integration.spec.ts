// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a package-metadata PATCH does to a surface that came from the
 * `index.malloy` convention.
 *
 * The convention fills in `explores` at load and records nothing about where
 * the value came from, which is what keeps every consumer unchanged. The one
 * place that decision has to be checked rather than reasoned about is the
 * PATCH path, because it both re-derives the served policy in memory and
 * rewrites publisher.json on disk, and those two are fed from different
 * values: `updatePackage` falls back to the package's CURRENT explores, while
 * `writePackageManifest` is handed the REQUEST BODY's.
 *
 * So a name-only PATCH must leave the served surface alone (or a description
 * edit would silently uncurate the package until the next reload) and must not
 * write the derived list into the manifest (or the convention would quietly
 * become an explicit key the author never typed). Both are properties of code
 * that already existed; this pins them against the convention, which is the
 * caller that makes them load-bearing.
 *
 * Over HTTP against the real app, because the disk write is the half that no
 * unit test of `Package` can see.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ENV_NAME = "index-convention-env";
const PACKAGE_NAME = "index-convention-test";
const fixtureDir = path.resolve(
   __dirname,
   "../../fixtures/index-convention-test",
);

// The package is served from a COPY under publisher_data (no --watch-env), and
// that copy is the manifest the PATCH path writes to. Reading the source
// fixture instead would pass no matter what the server wrote.
const servedManifest = path.join(
   path.resolve(__dirname, "../../.."),
   "publisher_data",
   ENV_NAME,
   PACKAGE_NAME,
   "publisher.json",
);

interface ApiPackage {
   name?: string;
   description?: string;
   explores?: string[];
}

describe("a metadata PATCH against a convention-derived surface", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

   const pkgApi = () =>
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PACKAGE_NAME}`;

   const listedModels = async (): Promise<string[]> => {
      const res = await fetch(`${pkgApi()}/models`);
      expect(res.status).toBe(200);
      const models = (await res.json()) as { path?: string }[];
      return models.map((m) => m.path as string).sort();
   };

   const readServedManifest = (): Record<string, unknown> =>
      JSON.parse(fs.readFileSync(servedManifest, "utf8")) as Record<
         string,
         unknown
      >;

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
         throw new Error(
            `Failed to create test environment (${createRes.status}): ` +
               `${await createRes.text()}`,
         );
      }

      const deadline = Date.now() + 30_000;
      for (;;) {
         const res = await fetch(pkgApi());
         if (res.ok) break;
         if (Date.now() > deadline) {
            throw new Error(`Package ${PACKAGE_NAME} did not become available`);
         }
         await new Promise((r) => setTimeout(r, 500));
      }
   });

   afterAll(async () => {
      await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
         method: "DELETE",
      }).catch(() => undefined);
      await env?.stop();
      env = null;
   });

   it("serves the derived surface, and the manifest on disk still declares none", async () => {
      expect(await listedModels()).toEqual(["index.malloy"]);
      // The premise of every assertion below: the surface is real but unwritten.
      expect(readServedManifest()).not.toHaveProperty("explores");
   });

   it("a name-only PATCH keeps the surface and writes no explores", async () => {
      const res = await fetch(pkgApi(), {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: PACKAGE_NAME,
            description: "edited description, nothing else",
         }),
      });
      expect(res.status).toBe(200);
      expect(((await res.json()) as ApiPackage).description).toBe(
         "edited description, nothing else",
      );

      // In memory: the package is still curated. Were the fallback to drop the
      // derived value, every model would list until the next reload.
      expect(await listedModels()).toEqual(["index.malloy"]);

      // On disk: still a convention, not a key. A description edit must not
      // hand the author an "explores" they never wrote.
      expect(readServedManifest()).not.toHaveProperty("explores");
   });

   it("a PATCH that declares a different surface persists it and takes effect", async () => {
      const res = await fetch(pkgApi(), {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: PACKAGE_NAME,
            explores: ["orders.malloy"],
         }),
      });
      expect(res.status).toBe(200);

      // The control for the test above: a body that genuinely names a surface
      // is honored immediately and written through, so the no-op behavior of a
      // name-only PATCH is about the ABSENT key rather than about PATCH being
      // unable to change the surface at all.
      expect(await listedModels()).toEqual(["orders.malloy"]);
      expect(readServedManifest().explores).toEqual(["orders.malloy"]);
   });
});
