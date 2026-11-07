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
 * path and needs a package that was already serving. Only a package mounted in
 * place reloads from disk (a published version is immutable), so the fixture is
 * served from a copy mounted in place.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import {
   fixture,
   startPackageEnv,
   type PackageEnv,
} from "../../harness/reports";

const ENV_NAME = "surface-widening-env";
const PACKAGE_NAME = "index-convention-test";

interface ApiPackage {
   explores?: string[];
   warnings?: { model?: string; message?: string }[];
}

describe("a package whose published surface disappears", () => {
   let env: PackageEnv | null = null;

   const pkgApi = () => env!.api();

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
      // A copy mounted in place: the reload reads it, so it is the tree the
      // test edits. Editing the fixture would pass either way.
      env = await startPackageEnv(
         ENV_NAME,
         { [PACKAGE_NAME]: fixture(PACKAGE_NAME) },
         { copy: true },
      );
   });

   afterAll(async () => {
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
      // tests/component/query_boundary.component.spec.ts pins that half.
      const hidden = await refuse("internal.malloy");
      const missing = await refuse("nope.malloy");
      expect(hidden).toEqual({
         status: 404,
         message:
            'No queryable model "internal.malloy". It is not on this ' +
            'package\'s published surface, "index.malloy": only what that ' +
            "file exports is queryable, and only through it. Fix: import " +
            '"internal.malloy" in "index.malloy", name the sources you want ' +
            "in that file's export { ... }, and address the query to " +
            '"index.malloy".',
      });
      expect(missing).toEqual({
         status: 404,
         message: 'No queryable model "nope.malloy".',
      });
   });

   it("reports the widening when the surface file is deleted", async () => {
      fs.unlinkSync(path.join(env!.location(), "index.malloy"));
      const reload = await fetch(`${pkgApi()}?reload=true`);
      expect(reload.status).toBe(200);

      const pkg = await getPackage();
      const message = widenedWarning(pkg);
      expect(message).toBeDefined();
      // What was published, the consequence, and the one fix.
      expect(message).toBe(
         `This package published "index.malloy" before the last reload and ` +
            `publishes no surface now, so every model in it is listed and ` +
            `queryable by name again. Fix: if that was not intended, restore ` +
            `index.malloy under that exact name.`,
      );

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
