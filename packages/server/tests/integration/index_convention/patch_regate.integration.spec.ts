// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A PATCH that makes a hidden source queryable is refused over HTTP when that
 * source exposes a field beyond its `#(carries)`, and the package keeps
 * serving its old metadata.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const ENV_NAME = "patch-regate-env";
const PACKAGE_NAME = "patch-regate-test";
const fixtureDir = path.resolve(__dirname, "../../fixtures/patch-regate-test");

describe("a PATCH over a hidden #(carries) leak", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

   const pkgApi = () =>
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PACKAGE_NAME}`;

   const patch = (body: Record<string, unknown>) =>
      fetch(pkgApi(), {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({ name: PACKAGE_NAME, ...body }),
      });

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
   });

   it("refuses queryableSources all with a 400, and a GET still shows the old metadata", async () => {
      const res = await patch({ queryableSources: "all" });
      expect(res.status).toBe(400);
      expect(await res.text()).toContain("source leaky (hidden.malloy)");

      const current = (await (await fetch(pkgApi())).json()) as {
         explores?: string[];
         queryableSources?: string;
      };
      expect(current.explores).toEqual(["index.malloy"]);
      expect(current.queryableSources).not.toBe("all");
   });

   it("refuses adding the hidden model to explores with a 400", async () => {
      const res = await patch({ explores: ["index.malloy", "hidden.malloy"] });
      expect(res.status).toBe(400);
      const current = (await (await fetch(pkgApi())).json()) as {
         explores?: string[];
      };
      expect(current.explores).toEqual(["index.malloy"]);
   });

   it("accepts a PATCH that changes neither", async () => {
      const res = await patch({ description: "edited" });
      expect(res.status).toBe(200);
   });
});
