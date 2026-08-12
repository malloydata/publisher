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

      const pkg = (await (await fetch(`${baseUrl}${API}`)).json()) as {
         warnings?: { message?: string }[];
      };
      const messages = (pkg.warnings ?? []).map((w) =>
         typeof w === "string" ? w : (w.message ?? ""),
      );
      expect(
         messages.some((m) => m.includes("query boundary is NOT enforced")),
      ).toBe(true);
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
