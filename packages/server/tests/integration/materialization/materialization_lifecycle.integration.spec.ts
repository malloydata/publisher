/// <reference types="bun-types" />

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PROJECT_NAME = "test-project";
const PACKAGE_NAME = "persist-test";
const API = `/api/v0/projects/${PROJECT_NAME}/packages/${PACKAGE_NAME}`;

describe("Materialization & Manifest REST API (E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;

      // Create the test project via the REST API using an absolute
      // path to the fixture so it works regardless of SERVER_ROOT.
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
            `Failed to create test project (${createRes.status}): ${body}`,
         );
      }

      // Wait for the package to finish loading.
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
      // Tear down the test project, then the HTTP server.
      if (baseUrl) {
         try {
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

   // ── helpers ──────────────────────────────────────────────────────

   function url(p: string): string {
      return `${baseUrl}${API}${p}`;
   }

   async function createMaterialization(
      body: Record<string, unknown> = {},
   ): Promise<Response> {
      return fetch(url("/materializations"), {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify(body),
      });
   }

   async function pollUntilTerminal(
      id: string,
      timeoutMs = 30_000,
   ): Promise<Record<string, unknown>> {
      const deadline = Date.now() + timeoutMs;
      while (Date.now() < deadline) {
         const res = await fetch(url(`/materializations/${id}`));
         expect(res.status).toBe(200);
         const data = (await res.json()) as Record<string, unknown>;
         const status = data.status as string;
         if (["SUCCESS", "FAILED", "CANCELLED"].includes(status)) {
            return data;
         }
         await new Promise((r) => setTimeout(r, 500));
      }
      throw new Error(`Materialization ${id} did not reach terminal state`);
   }

   /**
    * Clean up a materialization so it doesn't interfere with other tests.
    * Stops it if active, then deletes if terminal.
    */
   async function cleanup(id: string): Promise<void> {
      const res = await fetch(url(`/materializations/${id}`));
      if (res.status !== 200) return;
      const data = (await res.json()) as Record<string, unknown>;
      const status = data.status as string;

      if (status === "PENDING" || status === "RUNNING") {
         await fetch(url(`/materializations/${id}/stop`), { method: "POST" });
         await pollUntilTerminal(id);
      }
      await fetch(url(`/materializations/${id}`), { method: "DELETE" });
   }

   // ── Group A: Full lifecycle with persist sources ──────────────────

   describe("full lifecycle (happy path)", () => {
      let materializationId: string;

      afterAll(async () => {
         if (materializationId) await cleanup(materializationId);
      });

      it(
         "should create, start, build, verify manifest, and delete",
         async () => {
            // 1. Create
            const createRes = await createMaterialization({
               autoLoadManifest: true,
            });
            expect(createRes.status).toBe(201);
            const created = (await createRes.json()) as Record<string, unknown>;
            expect(created.status).toBe("PENDING");
            expect(created.id).toBeDefined();
            materializationId = created.id as string;

            // 2. List
            const listRes = await fetch(url("/materializations"));
            expect(listRes.status).toBe(200);
            const list = (await listRes.json()) as Record<string, unknown>[];
            expect(list.some((m) => m.id === materializationId)).toBe(true);

            // 3. Get by ID
            const getRes = await fetch(
               url(`/materializations/${materializationId}`),
            );
            expect(getRes.status).toBe(200);
            const got = (await getRes.json()) as Record<string, unknown>;
            expect(got.status).toBe("PENDING");

            // 4. Start
            const startRes = await fetch(
               url(`/materializations/${materializationId}/start`),
               { method: "POST" },
            );
            expect(startRes.status).toBe(202);
            const started = (await startRes.json()) as Record<string, unknown>;
            expect(started.status).toBe("RUNNING");

            // 5. Poll until terminal
            const terminal = await pollUntilTerminal(materializationId);
            expect(terminal.status).toBe("SUCCESS");
            const metadata = terminal.metadata as Record<string, unknown>;
            expect(metadata.sourcesBuilt).toBeGreaterThan(0);

            // 6. Get manifest
            const manifestRes = await fetch(url("/manifest"));
            expect(manifestRes.status).toBe(200);
            const manifest = (await manifestRes.json()) as Record<
               string,
               unknown
            >;
            expect(manifest.entries).toBeDefined();
            const entries = manifest.entries as Record<string, unknown>;
            expect(Object.keys(entries).length).toBeGreaterThan(0);
            const firstEntry = Object.values(entries)[0] as Record<
               string,
               unknown
            >;
            expect(firstEntry.tableName).toBe("order_summary");

            // 7. Reload manifest
            const reloadRes = await fetch(url("/manifest/reload"), {
               method: "POST",
            });
            expect(reloadRes.status).toBe(200);
            const reloadedManifest = (await reloadRes.json()) as Record<
               string,
               unknown
            >;
            expect(reloadedManifest.entries).toBeDefined();

            // 8. Delete
            const deleteRes = await fetch(
               url(`/materializations/${materializationId}`),
               { method: "DELETE" },
            );
            expect(deleteRes.status).toBe(204);
            materializationId = ""; // prevent afterAll cleanup
         },
         { timeout: 60_000 },
      );
   });

   // ── Group B: Error cases and state machine validation ────────────

   describe("error cases", () => {
      it("should stop a PENDING materialization (PENDING -> CANCELLED)", async () => {
         const createRes = await createMaterialization();
         expect(createRes.status).toBe(201);
         const created = (await createRes.json()) as Record<string, unknown>;
         const id = created.id as string;

         const stopRes = await fetch(url(`/materializations/${id}/stop`), {
            method: "POST",
         });
         expect(stopRes.status).toBe(200);
         const stopped = (await stopRes.json()) as Record<string, unknown>;
         expect(stopped.status).toBe("CANCELLED");

         await cleanup(id);
      });

      it("should reject a second concurrent materialization with 409", async () => {
         const first = await createMaterialization();
         expect(first.status).toBe(201);
         const firstData = (await first.json()) as Record<string, unknown>;
         const firstId = firstData.id as string;

         const second = await createMaterialization();
         expect(second.status).toBe(409);

         await cleanup(firstId);
      });

      it("should reject starting a CANCELLED materialization with 409", async () => {
         const createRes = await createMaterialization();
         expect(createRes.status).toBe(201);
         const created = (await createRes.json()) as Record<string, unknown>;
         const id = created.id as string;

         await fetch(url(`/materializations/${id}/stop`), { method: "POST" });

         const startRes = await fetch(url(`/materializations/${id}/start`), {
            method: "POST",
         });
         expect(startRes.status).toBe(409);

         await cleanup(id);
      });

      it("should reject deleting a PENDING materialization with 409", async () => {
         const createRes = await createMaterialization();
         expect(createRes.status).toBe(201);
         const created = (await createRes.json()) as Record<string, unknown>;
         const id = created.id as string;

         const deleteRes = await fetch(url(`/materializations/${id}`), {
            method: "DELETE",
         });
         expect(deleteRes.status).toBe(409);

         await cleanup(id);
      });

      it("should return 404 for a non-existent materialization", async () => {
         const res = await fetch(
            url("/materializations/non-existent-id-12345"),
         );
         expect(res.status).toBe(404);
      });
   });
});
