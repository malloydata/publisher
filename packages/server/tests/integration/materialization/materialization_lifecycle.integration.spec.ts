/// <reference types="bun-types" />

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PROJECT_NAME = "test-project";
const PACKAGE_NAME = "persist-test";
const API = `/api/v0/environments/${PROJECT_NAME}/packages/${PACKAGE_NAME}`;

/** Statuses from which no background round is in flight. */
const SETTLED_STATUSES = [
   "BUILD_PLAN_READY",
   "MANIFEST_FILE_READY",
   "FAILED",
   "CANCELLED",
];
const TERMINAL_STATUSES = ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"];

describe("Materialization two-round REST API (E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;

      // Create the test project via the REST API using an absolute
      // path to the fixture so it works regardless of SERVER_ROOT.
      const fixtureDir = path.resolve(__dirname, "../../fixtures/persist-test");
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
               `${baseUrl}/api/v0/environments/${PROJECT_NAME}/packages/${PACKAGE_NAME}`,
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
            await fetch(`${baseUrl}/api/v0/environments/${PROJECT_NAME}`, {
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

   async function getMaterialization(
      id: string,
   ): Promise<Record<string, unknown>> {
      const res = await fetch(url(`/materializations/${id}`));
      expect(res.status).toBe(200);
      return (await res.json()) as Record<string, unknown>;
   }

   /** Poll until `status` satisfies `done`, returning the record. */
   async function pollUntil(
      id: string,
      done: (status: string) => boolean,
      timeoutMs = 30_000,
   ): Promise<Record<string, unknown>> {
      const deadline = Date.now() + timeoutMs;
      while (Date.now() < deadline) {
         const data = await getMaterialization(id);
         if (done(data.status as string)) {
            return data;
         }
         await new Promise((r) => setTimeout(r, 250));
      }
      throw new Error(`Materialization ${id} did not reach the expected state`);
   }

   const pollUntilSettled = (id: string) =>
      pollUntil(id, (s) => SETTLED_STATUSES.includes(s));
   const pollUntilTerminal = (id: string) =>
      pollUntil(id, (s) => TERMINAL_STATUSES.includes(s));

   /**
    * Drive a materialization to a terminal state and delete its record so it
    * doesn't hold the per-package active slot for later tests.
    */
   async function cleanup(id: string): Promise<void> {
      const res = await fetch(url(`/materializations/${id}`));
      if (res.status !== 200) return;

      // Let any in-flight round settle before acting on it.
      const settled = await pollUntilSettled(id);
      if (!TERMINAL_STATUSES.includes(settled.status as string)) {
         await fetch(url(`/materializations/${id}?action=stop`), {
            method: "POST",
         });
         await pollUntilTerminal(id);
      }
      await fetch(url(`/materializations/${id}`), { method: "DELETE" });
   }

   /** First planned source from a BUILD_PLAN_READY materialization. */
   function firstPlannedSource(
      materialization: Record<string, unknown>,
   ): Record<string, unknown> {
      const plan = materialization.buildPlan as Record<string, unknown>;
      expect(plan).toBeDefined();
      const sources = plan.sources as Record<string, Record<string, unknown>>;
      const values = Object.values(sources);
      expect(values.length).toBeGreaterThan(0);
      return values[0];
   }

   // ── Group A: Full two-round lifecycle (happy path) ────────────────

   describe("full two-round lifecycle", () => {
      it(
         "plans (round 1), builds on control-plane instruction (round 2), then deletes",
         async () => {
            // Round 1: create kicks off compile + plan in the background.
            const createRes = await createMaterialization();
            expect(createRes.status).toBe(201);
            const created = (await createRes.json()) as Record<string, unknown>;
            expect(created.status).toBe("PENDING");
            expect(created.id).toBeDefined();
            const id = created.id as string;

            // List should include the new run.
            const listRes = await fetch(url("/materializations"));
            expect(listRes.status).toBe(200);
            const list = (await listRes.json()) as Record<string, unknown>[];
            expect(list.some((m) => m.id === id)).toBe(true);

            // Round 1 completes at BUILD_PLAN_READY with a plan for our source.
            const planned = await pollUntil(
               id,
               (s) => s === "BUILD_PLAN_READY" || TERMINAL_STATUSES.includes(s),
            );
            expect(planned.status).toBe("BUILD_PLAN_READY");
            const source = firstPlannedSource(planned);
            expect(source.name).toBe("order_summary");
            expect(typeof source.buildId).toBe("string");

            // Round 2: control plane instructs a COPY build into a physical name.
            const physicalTableName = "order_summary_built";
            const buildRes = await fetch(
               url(`/materializations/${id}?action=build`),
               {
                  method: "POST",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify({
                     sources: [
                        {
                           buildId: source.buildId,
                           sourceID: source.sourceID,
                           materializedTableId: "mt-order-summary",
                           physicalTableName,
                           realization: "COPY",
                        },
                     ],
                  }),
               },
            );
            expect(buildRes.status).toBe(202);

            // Round 2 completes at MANIFEST_FILE_READY with an inline manifest.
            const built = await pollUntil(
               id,
               (s) =>
                  s === "MANIFEST_FILE_READY" ||
                  s === "FAILED" ||
                  s === "CANCELLED",
            );
            expect(built.status).toBe("MANIFEST_FILE_READY");
            const manifest = built.manifest as Record<string, unknown>;
            expect(manifest).toBeDefined();
            const entries = manifest.entries as Record<
               string,
               Record<string, unknown>
            >;
            const entry = entries[source.buildId as string];
            expect(entry).toBeDefined();
            expect(entry.physicalTableName).toBe(physicalTableName);
            expect(entry.sourceName).toBe("order_summary");

            // A terminal materialization can be deleted; dropTables=true also
            // drops the physical table this run produced in Round 2.
            const deleteRes = await fetch(
               url(`/materializations/${id}?dropTables=true`),
               { method: "DELETE" },
            );
            expect(deleteRes.status).toBe(204);

            // It's gone.
            const getRes = await fetch(url(`/materializations/${id}`));
            expect(getRes.status).toBe(404);
         },
         { timeout: 90_000 },
      );
   });

   // ── Group B: State machine and error cases ───────────────────────

   describe("state machine and errors", () => {
      it("stops a plan-ready materialization (-> CANCELLED)", async () => {
         const createRes = await createMaterialization();
         expect(createRes.status).toBe(201);
         const { id } = (await createRes.json()) as { id: string };

         await pollUntil(id, (s) => s === "BUILD_PLAN_READY");

         const stopRes = await fetch(
            url(`/materializations/${id}?action=stop`),
            {
               method: "POST",
            },
         );
         expect(stopRes.status).toBe(200);
         const stopped = (await stopRes.json()) as Record<string, unknown>;
         expect(stopped.status).toBe("CANCELLED");

         await cleanup(id);
      });

      it("rejects a second concurrent materialization with 409", async () => {
         const first = await createMaterialization();
         expect(first.status).toBe(201);
         const { id } = (await first.json()) as { id: string };

         const second = await createMaterialization();
         expect(second.status).toBe(409);

         await cleanup(id);
      });

      it("rejects building from a non-plan-ready state with 409", async () => {
         const createRes = await createMaterialization();
         expect(createRes.status).toBe(201);
         const { id } = (await createRes.json()) as { id: string };

         await pollUntil(id, (s) => s === "BUILD_PLAN_READY");
         await fetch(url(`/materializations/${id}?action=stop`), {
            method: "POST",
         });
         await pollUntil(id, (s) => s === "CANCELLED");

         const buildRes = await fetch(
            url(`/materializations/${id}?action=build`),
            {
               method: "POST",
               headers: { "Content-Type": "application/json" },
               body: JSON.stringify({
                  sources: [
                     {
                        buildId: "deadbeef",
                        materializedTableId: "mt",
                        physicalTableName: "t",
                        realization: "COPY",
                     },
                  ],
               }),
            },
         );
         expect(buildRes.status).toBe(409);

         await fetch(url(`/materializations/${id}`), { method: "DELETE" });
      });

      it("rejects a build instruction with an unknown buildId (400)", async () => {
         const createRes = await createMaterialization();
         expect(createRes.status).toBe(201);
         const { id } = (await createRes.json()) as { id: string };

         await pollUntil(id, (s) => s === "BUILD_PLAN_READY");

         const buildRes = await fetch(
            url(`/materializations/${id}?action=build`),
            {
               method: "POST",
               headers: { "Content-Type": "application/json" },
               body: JSON.stringify({
                  sources: [
                     {
                        buildId: "not-a-real-build-id",
                        materializedTableId: "mt",
                        physicalTableName: "t",
                        realization: "COPY",
                     },
                  ],
               }),
            },
         );
         expect(buildRes.status).toBe(400);

         await cleanup(id);
      });

      it("rejects deleting a non-terminal materialization with 409", async () => {
         const createRes = await createMaterialization();
         expect(createRes.status).toBe(201);
         const { id } = (await createRes.json()) as { id: string };

         await pollUntil(id, (s) => s === "BUILD_PLAN_READY");

         const deleteRes = await fetch(url(`/materializations/${id}`), {
            method: "DELETE",
         });
         expect(deleteRes.status).toBe(409);

         await cleanup(id);
      });

      it("rejects an unsupported action with 400", async () => {
         const createRes = await createMaterialization();
         expect(createRes.status).toBe(201);
         const { id } = (await createRes.json()) as { id: string };

         await pollUntil(id, (s) => s === "BUILD_PLAN_READY");

         const res = await fetch(
            url(`/materializations/${id}?action=frobnicate`),
            {
               method: "POST",
            },
         );
         expect(res.status).toBe(400);

         await cleanup(id);
      });

      it("returns 404 for a non-existent materialization", async () => {
         const res = await fetch(
            url("/materializations/non-existent-id-12345"),
         );
         expect(res.status).toBe(404);
      });
   });
});
