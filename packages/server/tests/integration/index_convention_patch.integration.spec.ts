/// <reference types="bun-types" />

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../harness/rest_e2e";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PROJECT_NAME = "index-convention-project";
const PACKAGE_NAME = "index-convention-test";
const API = `/api/v0/environments/${PROJECT_NAME}/packages/${PACKAGE_NAME}`;

/**
 * The `index.malloy` discovery convention across a metadata PATCH.
 *
 * The package's surface is defaulted from its `index.malloy`, so it curates
 * listings but does NOT gate queries: `internal.malloy` is hidden from
 * `listModels()` and still answers 200 by name. That asymmetry is the whole
 * point of the convention, and every assertion here is about keeping it true
 * through a PATCH and the reload that follows.
 *
 * These are integration tests rather than unit tests because the defect they
 * pin lived in the seam between three things that no unit sees together: the
 * in-memory origin flag, what `writePackageManifest` puts on disk, and what the
 * next load reads back. It was found by running a server, not by a test.
 */
describe("index.malloy convention across a metadata PATCH", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   let manifestPath: string;

   const hiddenSourceStatus = async (): Promise<number> => {
      const res = await fetch(`${baseUrl}${API}/models/internal.malloy/query`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            query: "run: internal_scratch -> { aggregate: t is n.sum() }",
         }),
      });
      return res.status;
   };

   const readManifest = (): Record<string, unknown> =>
      JSON.parse(fs.readFileSync(manifestPath, "utf8"));

   const packageWarnings = async (): Promise<string[]> => {
      const pkg = (await (await fetch(`${baseUrl}${API}`)).json()) as {
         warnings?: ({ message?: string } | string)[];
      };
      return (pkg.warnings ?? []).map((w) =>
         typeof w === "string" ? w : (w.message ?? ""),
      );
   };

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;

      const fixtureDir = path.resolve(
         __dirname,
         "../fixtures/index-convention-test",
      );
      const createRes = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: PROJECT_NAME,
            packages: [{ name: PACKAGE_NAME, location: fixtureDir }],
            connections: [],
         }),
      });
      if (!createRes.ok) {
         throw new Error(
            `Failed to create test project (${createRes.status}): ${await createRes.text()}`,
         );
      }

      const deadline = Date.now() + 30_000;
      while (Date.now() < deadline) {
         const res = await fetch(`${baseUrl}${API}`);
         if (res.ok) break;
         await new Promise((r) => setTimeout(r, 500));
      }

      // The served copy, not the fixture: the server copies the package into
      // <SERVER_ROOT>/publisher_data/<env>/<pkg> and writes there. SERVER_ROOT
      // defaults to the server package dir under `bun test`.
      manifestPath = path.join(
         path.resolve(__dirname, "../.."),
         "publisher_data",
         PROJECT_NAME,
         PACKAGE_NAME,
         "publisher.json",
      );
   });

   afterAll(async () => {
      if (baseUrl) {
         try {
            await fetch(`${baseUrl}/api/v0/environments/${PROJECT_NAME}`, {
               method: "DELETE",
            });
         } catch {
            /* best effort */
         }
      }
      await env?.stop();
   });

   it("curates listings from index.malloy while leaving every source queryable", async () => {
      const models = (await (
         await fetch(`${baseUrl}${API}/models`)
      ).json()) as {
         path: string;
      }[];
      expect(models.map((m) => m.path)).toEqual(["index.malloy"]);
      expect(await hiddenSourceStatus()).toBe(200);
   });

   it("does not persist a convention surface, so a round-trip PATCH is inert", async () => {
      // The origin flag is not on the wire, so a client that GETs the package,
      // edits one field and PATCHes the whole object back re-sends the
      // convention's own explores having declared nothing.
      const current = (await (await fetch(`${baseUrl}${API}`)).json()) as {
         name: string;
         explores?: string[];
      };
      expect(current.explores).toEqual(["index.malloy"]);

      const res = await fetch(`${baseUrl}${API}`, {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: current.name,
            description: "edited by a round trip",
            explores: current.explores,
         }),
      });
      expect(res.status).toBe(200);

      // Writing the resolved surface out would make disk say "declared" while
      // the running server says "convention", so the boundary would stay off
      // now and switch ON at the next reload, long after the edit that caused
      // it. The key must simply not be there.
      expect(readManifest()).not.toHaveProperty("explores");
      expect(await hiddenSourceStatus()).toBe(200);

      // And the reload is the half that used to fail: same request, 404.
      const reload = await fetch(`${baseUrl}${API}?reload=true`);
      expect(reload.status).toBe(200);
      expect(await hiddenSourceStatus()).toBe(200);
   });

   it("says out loud that an echoed explores did not become a declaration", async () => {
      // The request is ambiguous: an echo, or an author declaring the surface
      // on purpose to opt into the boundary. It is read as the echo, because
      // guessing the other way revokes query access. The author who meant the
      // second reading must not be left believing queries are gated, so the
      // package says so rather than silently doing nothing.
      const res = await fetch(`${baseUrl}${API}`, {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: PACKAGE_NAME,
            explores: ["index.malloy"],
         }),
      });
      expect(res.status).toBe(200);
      expect(await hiddenSourceStatus()).toBe(200);

      expect(
         (await packageWarnings()).some((m) =>
            m.includes("query boundary is NOT enforced"),
         ),
      ).toBe(true);
   });

   it("keeps that warning when the same PATCH triggers a reload", async () => {
      // The trap this guards: every GET emits `manifestLocation: null`, so a
      // client that round-trips the whole object always sends it, and any
      // manifestLocation on the body triggers reloadAllModels, which replaces
      // manifestWarnings wholesale from a fresh disk parse. The warning added
      // moments earlier in the same request was being destroyed before the
      // response was built, so the one client the echo rule exists for was the
      // one client that never saw it.
      const res = await fetch(`${baseUrl}${API}`, {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: PACKAGE_NAME,
            explores: ["index.malloy"],
            manifestLocation: null,
         }),
      });
      expect(res.status).toBe(200);
      expect(await hiddenSourceStatus()).toBe(200);
      expect(
         (await packageWarnings()).some((m) =>
            m.includes("query boundary is NOT enforced"),
         ),
      ).toBe(true);
   });

   it("says a queryableSources sent under the convention has no effect", async () => {
      // Derived at load from the manifest text, so before this it only showed
      // up after the next reload. The PATCH response is when the operator is
      // looking.
      const res = await fetch(`${baseUrl}${API}`, {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: PACKAGE_NAME,
            queryableSources: "declared",
         }),
      });
      expect(res.status).toBe(200);
      expect(
         (await packageWarnings()).some((m) => m.includes("has no effect")),
      ).toBe(true);
   });

   it("a rejected PATCH does not silently delete the warning", async () => {
      // setPackageMetadata runs BEFORE validation and its filter is
      // destructive, so a PATCH that flips the origin and is then rejected
      // would strip the post-load warning on the way in and never put it back.
      // The package would be left serving a curated surface with no signal that
      // its boundary is unenforced, which is the exact state this warning is
      // the only defence against.
      expect(
         (await packageWarnings()).some((m) =>
            m.includes("query boundary is NOT enforced"),
         ),
      ).toBe(true);

      const res = await fetch(`${baseUrl}${API}`, {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: PACKAGE_NAME,
            explores: ["does-not-exist.malloy"],
         }),
      });
      expect(res.status).toBe(400);

      // Checking the GET here alone is NOT enough, and this is the subtle part.
      // The rollback hands a previously-computed metadata object back to
      // setPackageMetadata, which fossilizes a copy of the old warnings inside
      // it, and getPackageMetadata only overwrites that copy when the live list
      // is non-empty. So a deleted warning keeps showing until something else
      // repopulates the list, and a test that stopped here would pass with the
      // fix reverted. Force the live list to repopulate first.
      await fetch(`${baseUrl}${API}`, {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: PACKAGE_NAME,
            queryableSources: "declared",
         }),
      });

      expect(
         (await packageWarnings()).some((m) =>
            m.includes("query boundary is NOT enforced"),
         ),
      ).toBe(true);
      expect(await hiddenSourceStatus()).toBe(200);
   });

   it("supersedes rather than stacks BOTH post-load warnings", async () => {
      // Successive PATCHes answer the same question differently. Each of the
      // two post-load warnings has two variants that differ only in their
      // remedy, so exact de-dupe keeps both and one of them is wrong for the
      // `queryableSources` now in force. Both must supersede; applying it to
      // only one was the defect.
      //
      // `explores` is sent on both PATCHes so the echoed-declaration warning is
      // raised too, not just the inert one.
      await fetch(`${baseUrl}${API}`, {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: PACKAGE_NAME,
            explores: ["index.malloy"],
            queryableSources: "declared",
         }),
      });
      await fetch(`${baseUrl}${API}`, {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: PACKAGE_NAME,
            explores: ["index.malloy"],
            queryableSources: "all",
         }),
      });

      const warnings = await packageWarnings();
      const inert = warnings.filter((m) => m.includes("has no effect"));
      const ignored = warnings.filter((m) =>
         m.includes("re-sent this package's discovery surface"),
      );
      expect(inert).toHaveLength(1);
      expect(ignored).toHaveLength(1);
      // Each is the variant matching the value now in force ("all"), not the
      // earlier one. The stale echo variant is the dangerous half: it says
      // declaring the surface "does enforce it", which under "all" it does not.
      expect(inert[0]).toContain("you can delete it");
      expect(inert[0]).not.toContain('Add an explicit "explores"');
      expect(ignored[0]).toContain("You need BOTH");
      expect(ignored[0]).not.toContain("a surface declared there does enforce");

      // Put it back: "all" persists to the manifest, and leaving it would
      // disarm the boundary for the tests below, which is a property of this
      // shared fixture rather than of the feature.
      await fetch(`${baseUrl}${API}`, {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: PACKAGE_NAME,
            queryableSources: "declared",
         }),
      });
   });

   it("drops the stale warning once the author really does declare a surface", async () => {
      // The inverse of the defect above, and just as misleading. The author
      // reads "the boundary is NOT enforced", does the thing the warning tells
      // them to do (declare `explores` in publisher.json), and reloads. The
      // boundary is now ON, so a surviving warning would insist it is off.
      // This path sets the origin flag directly rather than through
      // setPackageMetadata, so it needs its own clear.
      expect(
         (await packageWarnings()).some((m) =>
            m.includes("query boundary is NOT enforced"),
         ),
      ).toBe(true);

      const manifest = readManifest();
      fs.writeFileSync(
         manifestPath,
         JSON.stringify({ ...manifest, explores: ["index.malloy"] }, null, 2),
      );
      // Deliberately an IN-PLACE reload, not `?reload=true`. The latter runs
      // Package.create and hands back a new object whose post-load warnings are
      // empty anyway, so it would pass whether or not the clear exists. A
      // `manifestLocation` on the body reloads the SAME Package, which is the
      // only path where a stale warning can survive. Note this PATCH carries no
      // `explores`, so setPackageMetadata leaves the origin alone and the
      // worker's fresh read of publisher.json is what flips it.
      const reload = await fetch(`${baseUrl}${API}`, {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({ name: PACKAGE_NAME, manifestLocation: null }),
      });
      expect(reload.status).toBe(200);

      // The surface is declared now, so the boundary really is on...
      expect(await hiddenSourceStatus()).toBe(404);
      // ...and nothing is still claiming otherwise.
      expect(
         (await packageWarnings()).some((m) =>
            m.includes("query boundary is NOT enforced"),
         ),
      ).toBe(false);
   });

   it("persists and enforces a surface the caller actually declared", async () => {
      // The other direction: a body naming a DIFFERENT surface is a real
      // declaration, so it is written to disk and the boundary engages, both
      // immediately and after a reload.
      const res = await fetch(`${baseUrl}${API}`, {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: PACKAGE_NAME,
            explores: ["orders.malloy"],
         }),
      });
      expect(res.status).toBe(200);

      expect(readManifest().explores).toEqual(["orders.malloy"]);
      expect(await hiddenSourceStatus()).toBe(404);

      const reload = await fetch(`${baseUrl}${API}?reload=true`);
      expect(reload.status).toBe(200);
      expect(await hiddenSourceStatus()).toBe(404);
   });
});
