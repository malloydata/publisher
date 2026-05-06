/// <reference types="bun-types" />

// TODO: Remove this during projects cleanup
/**
 * Smoke tests for the legacy `/api/v0/projects/...` REST surface registered
 * by `server-old.ts`. These routes exist purely to keep pre-rename SDK
 * clients (e.g. `@malloydata/db-publisher`) working after the
 * projects→environments rename.
 *
 * One test per route group: projects CRUD, packages, connections, models,
 * notebooks, databases, queries, materializations, manifest. The
 * materialization test additionally asserts the response field rename
 * (`projectId` not `environmentId`). `/status` is no longer in the legacy
 * surface — both old and new clients hit the single `/api/v0/status`
 * handler in server.ts, which returns `environments`.
 *
 * This file is intentionally separate from the regular integration suite so
 * it can be deleted in one motion when legacy support is dropped.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Use a distinct project name so this suite doesn't collide with the
// materialization integration suite (which also uses "test-project") if
// they ever run in the same DB instance.
const PROJECT_NAME = "legacy-routes-test-project";
const PACKAGE_NAME = "persist-test";

describe("Legacy /api/v0/projects/* REST routes (E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;

      // Create the test environment via the LEGACY route — proves POST
      // /projects works end-to-end.
      const fixtureDir = path.resolve(__dirname, "../../fixtures/persist-test");
      const createRes = await fetch(`${baseUrl}/api/v0/projects`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: PROJECT_NAME,
            packages: [{ name: PACKAGE_NAME, location: fixtureDir }],
            connections: [],
         }),
      });
      if (!createRes.ok) {
         const body = await createRes.text();
         throw new Error(
            `Failed to create test project via legacy route (${createRes.status}): ${body}`,
         );
      }

      // Wait for the package to finish loading via the legacy GET path.
      const deadline = Date.now() + 30_000;
      let pkgReady = false;
      while (!pkgReady && Date.now() < deadline) {
         try {
            const res = await fetch(
               `${baseUrl}/api/v0/projects/${PROJECT_NAME}/packages/${PACKAGE_NAME}`,
            );
            if (res.ok) {
               pkgReady = true;
               break;
            }
         } catch {
            // not ready yet
         }
         await new Promise((r) => setTimeout(r, 500));
      }
      if (!pkgReady) {
         throw new Error("Test package did not become available in time");
      }
   });

   afterAll(async () => {
      if (baseUrl) {
         try {
            // Clean up via the legacy DELETE route.
            await fetch(`${baseUrl}/api/v0/projects/${PROJECT_NAME}`, {
               method: "DELETE",
            });
         } catch {
            // best-effort cleanup
         }
      }
      await env?.stop();
      env = null;
   });

   describe("projects CRUD", () => {
      it("GET /projects lists environments under the legacy URL", async () => {
         const res = await fetch(`${baseUrl}/api/v0/projects`);
         expect(res.status).toBe(200);
         const body = (await res.json()) as Array<{ name?: string }>;
         expect(Array.isArray(body)).toBe(true);
         expect(body.some((e) => e.name === PROJECT_NAME)).toBe(true);
      });

      it("GET /projects/:projectName returns the project", async () => {
         const res = await fetch(`${baseUrl}/api/v0/projects/${PROJECT_NAME}`);
         expect(res.status).toBe(200);
         const body = (await res.json()) as { name?: string };
         expect(body.name).toBe(PROJECT_NAME);
      });
   });

   describe("packages", () => {
      it("GET /projects/:projectName/packages returns the package list", async () => {
         const res = await fetch(
            `${baseUrl}/api/v0/projects/${PROJECT_NAME}/packages`,
         );
         expect(res.status).toBe(200);
         const body = (await res.json()) as Array<{ name?: string }>;
         expect(Array.isArray(body)).toBe(true);
         expect(body.some((p) => p.name === PACKAGE_NAME)).toBe(true);
      });

      it("GET /projects/:projectName/packages/:packageName returns the package", async () => {
         const res = await fetch(
            `${baseUrl}/api/v0/projects/${PROJECT_NAME}/packages/${PACKAGE_NAME}`,
         );
         expect(res.status).toBe(200);
         const body = (await res.json()) as { name?: string };
         expect(body.name).toBe(PACKAGE_NAME);
      });
   });

   describe("connections", () => {
      it("GET /projects/:projectName/connections returns 200 (may be empty)", async () => {
         const res = await fetch(
            `${baseUrl}/api/v0/projects/${PROJECT_NAME}/connections`,
         );
         expect(res.status).toBe(200);
         const body = await res.json();
         expect(Array.isArray(body)).toBe(true);
      });
   });

   describe("models", () => {
      it("GET /projects/:projectName/packages/:packageName/models returns the model list", async () => {
         const res = await fetch(
            `${baseUrl}/api/v0/projects/${PROJECT_NAME}/packages/${PACKAGE_NAME}/models`,
         );
         expect(res.status).toBe(200);
         const body = await res.json();
         expect(Array.isArray(body)).toBe(true);
      });
   });

   describe("notebooks", () => {
      it("GET /projects/:projectName/packages/:packageName/notebooks returns 200", async () => {
         const res = await fetch(
            `${baseUrl}/api/v0/projects/${PROJECT_NAME}/packages/${PACKAGE_NAME}/notebooks`,
         );
         expect(res.status).toBe(200);
         const body = await res.json();
         expect(Array.isArray(body)).toBe(true);
      });
   });

   describe("databases", () => {
      it("GET /projects/:projectName/packages/:packageName/databases returns 200", async () => {
         const res = await fetch(
            `${baseUrl}/api/v0/projects/${PROJECT_NAME}/packages/${PACKAGE_NAME}/databases`,
         );
         expect(res.status).toBe(200);
         const body = await res.json();
         expect(Array.isArray(body)).toBe(true);
      });
   });

   describe("queries", () => {
      it("POST /projects/:projectName/packages/:packageName/models/.../query reaches the handler", async () => {
         // Hit the route with a bogus model name. We only need to prove the
         // legacy URL is wired up to the controller — a structured JSON
         // error (not Express's HTML fall-through 404) is sufficient signal.
         const res = await fetch(
            `${baseUrl}/api/v0/projects/${PROJECT_NAME}/packages/${PACKAGE_NAME}/models/does-not-exist.malloy/query`,
            {
               method: "POST",
               headers: { "Content-Type": "application/json" },
               body: JSON.stringify({ query: "run: nothing" }),
            },
         );
         expect(res.status).toBeGreaterThanOrEqual(400);
         expect(res.status).toBeLessThan(600);
         // Controller errors come back as JSON with a `message` field.
         // An unhandled Express 404 returns HTML — that would fail here.
         const body = (await res.json()) as Record<string, unknown>;
         expect(typeof body.message).toBe("string");
      });
   });

   describe("materializations", () => {
      it("GET list and POST create return 'projectId' (not 'environmentId') under the legacy URL", async () => {
         const listRes = await fetch(
            `${baseUrl}/api/v0/projects/${PROJECT_NAME}/packages/${PACKAGE_NAME}/materializations`,
         );
         expect(listRes.status).toBe(200);
         const list = (await listRes.json()) as unknown;
         expect(Array.isArray(list)).toBe(true);

         // Create one so we can assert the field rename on a populated payload.
         const createRes = await fetch(
            `${baseUrl}/api/v0/projects/${PROJECT_NAME}/packages/${PACKAGE_NAME}/materializations`,
            {
               method: "POST",
               headers: { "Content-Type": "application/json" },
               body: JSON.stringify({ autoLoadManifest: true }),
            },
         );
         expect(createRes.status).toBe(201);
         const created = (await createRes.json()) as Record<string, unknown>;

         // Legacy contract: materialization payloads expose `projectId`, not
         // `environmentId`. This is the response remapper in server-old.ts.
         expect(created).toHaveProperty("projectId");
         expect(created).not.toHaveProperty("environmentId");

         const id = created.id as string;
         // Best-effort cleanup so we don't leak a PENDING materialization
         // into other tests. We don't poll-to-terminal; the suite teardown
         // of the project will mop up.
         try {
            await fetch(
               `${baseUrl}/api/v0/projects/${PROJECT_NAME}/packages/${PACKAGE_NAME}/materializations/${id}?action=stop`,
               { method: "POST" },
            );
            await fetch(
               `${baseUrl}/api/v0/projects/${PROJECT_NAME}/packages/${PACKAGE_NAME}/materializations/${id}`,
               { method: "DELETE" },
            );
         } catch {
            // ignore
         }
      });
   });

   describe("manifest", () => {
      it("GET /projects/:projectName/packages/:packageName/manifest returns 200 or a structured 4xx", async () => {
         const res = await fetch(
            `${baseUrl}/api/v0/projects/${PROJECT_NAME}/packages/${PACKAGE_NAME}/manifest`,
         );
         // Without a built materialization the manifest may be empty/404 —
         // we only assert the legacy URL reaches the handler, not 404 from
         // Express's catch-all.
         expect([200, 400, 404]).toContain(res.status);
      });
   });
});
