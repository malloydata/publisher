// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a package says when its published surface disappears.
 *
 * Every other curation change leaves something to look at: a surface that
 * appears warns at load, a malformed `explores` refuses the load, a broken
 * surface file fails the reload and is reported stale. Deleting the surface
 * FILE is the quiet one. It resolves to no surface, which is an ordinary
 * uncurated package, and an uncurated package has nothing to say about itself
 * -- so the sources it was withholding come back and nothing reports it.
 *
 * Curation, not access control: what widens is what is listed and what answers
 * by name. This pins the notice, and pins that it is the transition being
 * reported rather than the state, since a package that was never curated must
 * stay quiet.
 *
 * Over HTTP against the real app, because the comparison lives in the reload
 * path and needs a package that was already serving.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ENV_NAME = "surface-widening-env";
const PACKAGE_NAME = "index-convention-test";
const fixtureDir = path.resolve(
   __dirname,
   "../../fixtures/index-convention-test",
);

// The served COPY under publisher_data is what the reload reads, so it is the
// tree the test has to edit. Editing the fixture would pass either way.
const servedDir = path.join(
   path.resolve(__dirname, "../../.."),
   "publisher_data",
   ENV_NAME,
   PACKAGE_NAME,
);

interface ApiPackage {
   explores?: string[];
   warnings?: { model?: string; message?: string }[];
}

describe("a package whose published surface disappears", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

   const pkgApi = () =>
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PACKAGE_NAME}`;

   const getPackage = async (): Promise<ApiPackage> =>
      (await (await fetch(pkgApi())).json()) as ApiPackage;

   const listedModels = async (): Promise<string[]> => {
      const res = await fetch(`${pkgApi()}/models`);
      expect(res.status).toBe(200);
      return ((await res.json()) as { path?: string }[])
         .map((m) => m.path as string)
         .sort();
   };

   const widenedWarning = (pkg: ApiPackage): string | undefined =>
      (pkg.warnings ?? [])
         .map((w) => w.message ?? "")
         .find((m) => m.includes("publishes no surface now"));

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

   it("says nothing while the surface is still there", async () => {
      // The premise of the rest: a curated package, quiet about it except for
      // the convention notice, and a reload that changes nothing stays quiet.
      expect(await listedModels()).toEqual(["index.malloy"]);
      const reload = await fetch(`${pkgApi()}?reload=true`);
      expect(reload.status).toBe(200);
      expect(widenedWarning(await getPackage())).toBeUndefined();
   });

   it("says why a hidden model is refused, and refuses a missing one plainly", async () => {
      const refuse = async (model: string) => {
         const res = await fetch(`${pkgApi()}/models/${model}/query`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ query: "run: internal_scratch -> v" }),
         });
         const body = (await res.json()) as { message?: string };
         return { status: res.status, message: body.message };
      };
      // internal.malloy exists and is off the surface; nope.malloy does not
      // exist. Nothing in this package is gated, so the hidden file's 404 says
      // it is off the surface and names the fix (a modeler who just saved it
      // would otherwise read a typo). A gated model keeps the generic 404;
      // query_boundary.spec.ts pins that half.
      const hidden = await refuse("internal.malloy");
      const missing = await refuse("nope.malloy");
      expect(hidden).toEqual({
         status: 404,
         message:
            'No queryable model "internal.malloy". It is not on this ' +
            'package\'s published surface, "index.malloy": only what that ' +
            "file exports is queryable, and only through it. Fix: import it " +
            'in "index.malloy", add it to that file\'s export { ... }, and ' +
            'address the query to "index.malloy".',
      });
      expect(missing).toEqual({
         status: 404,
         message: 'No queryable model "nope.malloy".',
      });
   });

   it("reports the widening when the surface file is deleted", async () => {
      fs.unlinkSync(path.join(servedDir, "index.malloy"));
      const reload = await fetch(`${pkgApi()}?reload=true`);
      expect(reload.status).toBe(200);

      const pkg = await getPackage();
      const message = widenedWarning(pkg);
      expect(message).toBeDefined();
      // Names what was published, so the author can tell which file went.
      expect(message).toContain("index.malloy");
      // Names the consequence, and the remedies, including the deliberate one.
      expect(message).toContain("queryable by name again");
      expect(message).toContain("the name IS the surface");
      expect(message).toContain('"explores": []');

      // And the widening is real, not just reported: the model the surface was
      // withholding is listed and answers now.
      const models = await listedModels();
      expect(models).toContain("internal.malloy");
      expect(models).toContain("orders.malloy");
      expect(pkg.explores).toBeUndefined();

      const query = await fetch(`${pkgApi()}/models/internal.malloy/query`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({ query: "run: internal_scratch -> v" }),
      });
      expect(query.status).toBe(200);
   });

   it("does not repeat itself on the next reload", async () => {
      // A transition, not a state. The package is now an ordinary uncurated
      // one, and an uncurated package that was never curated must read the
      // same as this one -- otherwise the notice becomes permanent noise on
      // every package that has no surface.
      const reload = await fetch(`${pkgApi()}?reload=true`);
      expect(reload.status).toBe(200);
      expect(widenedWarning(await getPackage())).toBeUndefined();
   });
});
