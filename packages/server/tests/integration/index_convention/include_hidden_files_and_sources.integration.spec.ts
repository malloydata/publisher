// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `includeHiddenFilesAndSources` on the model routes: list, show and run the files a root
 * `index.malloy` hides.
 *
 * A package's authors need to read every file, including the ones its surface
 * leaves out. Publisher does not decide who is an author, so it takes a
 * request option and leaves that decision to the gateway in front of it. This
 * pins what the option changes (the listing, the 404, the withheld text, a query
 * to a hidden file) and that a request without it is answered as before.
 *
 * Over HTTP against the real app, because the option is read at the route.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ENV_NAME = "include-off-surface-env";
const PACKAGE_NAME = "index-convention-test";
const fixtureDir = path.resolve(
   __dirname,
   "../../fixtures/index-convention-test",
);

describe("includeHiddenFilesAndSources on the model routes", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

   const pkgApi = () =>
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PACKAGE_NAME}`;

   const listing = async (query = "") => {
      const res = await fetch(`${pkgApi()}/models${query}`);
      expect(res.status).toBe(200);
      const models = (await res.json()) as {
         path?: string;
         isHidden?: boolean;
      }[];
      return Object.fromEntries(models.map((m) => [m.path, m.isHidden]));
   };

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

   it("lists only the surface by default, and every file when asked", async () => {
      expect(await listing()).toEqual({ "index.malloy": false });
      expect(await listing("?includeHiddenFilesAndSources=false")).toEqual({
         "index.malloy": false,
      });
      expect(await listing("?includeHiddenFilesAndSources=true")).toEqual({
         "index.malloy": false,
         "internal.malloy": true,
         "orders.malloy": true,
      });
   });

   it("refuses a value that is not true or false, on both routes", async () => {
      for (const url of [
         `${pkgApi()}/models?includeHiddenFilesAndSources=yes`,
         `${pkgApi()}/models/internal.malloy?includeHiddenFilesAndSources=1`,
      ]) {
         const res = await fetch(url);
         expect(res.status).toBe(400);
      }
   });

   it("returns a hidden file's text only when asked", async () => {
      const hidden = `${pkgApi()}/models/internal.malloy`;
      expect((await fetch(hidden)).status).toBe(404);

      const res = await fetch(`${hidden}?includeHiddenFilesAndSources=true`);
      expect(res.status).toBe(200);
      const body = (await res.json()) as { sourceText?: string };
      expect(body.sourceText).toBe(
         fs.readFileSync(path.join(fixtureDir, "internal.malloy"), "utf8"),
      );
   });

   it("runs a query to the hidden file only when asked", async () => {
      const query = async (q: string) =>
         fetch(`${pkgApi()}/models/internal.malloy/query${q}`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ query: "run: internal_scratch -> v" }),
         });
      expect((await query("")).status).toBe(404);
      const res = await query("?includeHiddenFilesAndSources=true");
      expect(res.status).toBe(200);
      const rows = JSON.parse(((await res.json()) as { result: string }).result)
         .data.array_value;
      expect(rows.length).toBe(1);
      expect((await query("?includeHiddenFilesAndSources=maybe")).status).toBe(
         400,
      );
   });
});
