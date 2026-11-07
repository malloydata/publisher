// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A new version whose publisher.json makes a hidden source queryable is
 * refused over HTTP when that source exposes a field beyond its `#(carries)`,
 * and the package keeps serving its previous version's metadata. A package's
 * metadata changes only by publishing a new version, so that is where the
 * re-gate has to hold.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const ENV_NAME = "patch-regate-env";
const PACKAGE_NAME = "patch-regate-test";
const fixtureDir = path.resolve(__dirname, "../../fixtures/patch-regate-test");

describe("a new version over a hidden #(carries) leak", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   let tempRoot: string;

   const pkgApi = () =>
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PACKAGE_NAME}`;

   /** Publish a copy of the fixture with `changes` merged into its publisher.json. */
   const publish = (changes: Record<string, unknown>) => {
      const dir = fs.mkdtempSync(path.join(tempRoot, "version-"));
      fs.cpSync(fixtureDir, dir, { recursive: true });
      const manifestPath = path.join(dir, "publisher.json");
      const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
      fs.writeFileSync(
         manifestPath,
         JSON.stringify({ ...manifest, ...changes }),
      );
      return fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}/packages`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({ name: PACKAGE_NAME, location: dir }),
      });
   };

   const current = async () =>
      (await (await fetch(pkgApi())).json()) as {
         versionId?: string;
         explores?: string[];
         queryableSources?: string;
         description?: string;
      };

   beforeAll(async () => {
      tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "patch-regate-"));
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
         if ((await fetch(pkgApi())).ok) break;
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
      fs.rmSync(tempRoot, { recursive: true, force: true });
   });

   it("refuses a version with queryableSources all, and the package still serves the old metadata", async () => {
      const res = await publish({ version: "1.0.1", queryableSources: "all" });
      expect(res.status).toBe(400);
      expect(await res.text()).toContain("source leaky (hidden.malloy)");

      const served = await current();
      expect(served.versionId).toBe("1.0.0");
      expect(served.explores).toEqual(["index.malloy"]);
      expect(served.queryableSources).not.toBe("all");
   });

   it("refuses a version that adds the hidden model to explores", async () => {
      const res = await publish({
         version: "1.0.2",
         explores: ["index.malloy", "hidden.malloy"],
      });
      expect(res.status).toBe(400);
      expect(await res.text()).toContain("source leaky (hidden.malloy)");

      const served = await current();
      expect(served.versionId).toBe("1.0.0");
      expect(served.explores).toEqual(["index.malloy"]);
   });

   it("accepts a version that changes neither", async () => {
      const res = await publish({ version: "1.1.0", description: "edited" });
      expect(res.status).toBe(200);
      const served = await current();
      expect(served.versionId).toBe("1.1.0");
      expect(served.description).toBe("edited");
   });
});
