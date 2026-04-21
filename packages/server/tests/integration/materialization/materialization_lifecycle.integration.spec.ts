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

// Synchronous stderr logger — bun's stdout buffering has been swallowing
// `console.log` output in this suite under Ubuntu CI (see PR discussion).
// stderr writes are line-buffered by the runtime and flush before each
// suite boundary, so diagnostic lines survive a failed `beforeAll`.
function diag(msg: string): void {
   process.stderr.write(`[mat-e2e] ${msg}\n`);
}

describe("Materialization & Manifest REST API (E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

   // Give beforeAll enough time to start the server even on slow CI runners
   // (Ubuntu needs ~60s for malloy-samples initialization when this spec
   // runs before the MCP harness has warmed the shared EnvironmentStore).
   beforeAll(async () => {
      // Verbose DuckDB resolve tracing (stderr) on GitHub Actions / Ubuntu CI.

      diag("beforeAll: starting REST E2E server...");
      try {
         env = await startRestE2E();
      } catch (err) {
         diag(
            `startRestE2E failed: ${err instanceof Error ? err.message : String(err)}`,
         );
         throw err;
      }
      baseUrl = env.baseUrl;
      diag(`REST E2E ready at ${baseUrl}`);

      // Pre-clean: if a prior suite (e.g. MCP harness) left `test-project`
      // around in the shared in-process EnvironmentStore, delete it so we
      // start from a known-empty state. Ignore errors — a 404 is fine.
      try {
         const delRes = await fetch(
            `${baseUrl}/api/v0/environments/${PROJECT_NAME}`,
            { method: "DELETE" },
         );
         diag(`pre-clean DELETE ${PROJECT_NAME} -> ${delRes.status}`);
      } catch (err) {
         diag(
            `pre-clean DELETE failed (ignored): ${err instanceof Error ? err.message : String(err)}`,
         );
      }

      // Create the test environment via the REST API using an absolute
      // path to the fixture so it works regardless of SERVER_ROOT.
      const fixtureDir = path.resolve(__dirname, "../../fixtures/persist-test");
      diag(`POST /environments {name:${PROJECT_NAME}, location:${fixtureDir}}`);
      let createRes: Response;
      try {
         createRes = await fetch(`${baseUrl}/api/v0/environments`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
               name: PROJECT_NAME,
               packages: [{ name: PACKAGE_NAME, location: fixtureDir }],
               connections: [],
            }),
         });
      } catch (err) {
         diag(
            `POST /environments threw: ${err instanceof Error ? err.message : String(err)}`,
         );
         throw err;
      }
      diag(`POST /environments -> ${createRes.status}`);
      if (!createRes.ok) {
         const body = await createRes.text();
         diag(`POST /environments body: ${body}`);
         // Include the fixture path + baseUrl in the error to make CI
         // failures diagnosable without needing a local repro.
         throw new Error(
            `Failed to create test environment (${createRes.status}) at ${baseUrl} ` +
               `from fixture ${fixtureDir}: ${body}`,
         );
      }

      // Wait for the package to finish loading.
      diag(`waiting for package ${PACKAGE_NAME} to load...`);
      const deadline = Date.now() + 30_000;
      let pkgReady = false;
      let lastPkgStatus: number | undefined;
      while (!pkgReady && Date.now() < deadline) {
         try {
            const res = await fetch(
               `${baseUrl}/api/v0/environments/${PROJECT_NAME}/packages/${PACKAGE_NAME}`,
            );
            lastPkgStatus = res.status;
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
         diag(
            `package ${PACKAGE_NAME} not ready (last status: ${lastPkgStatus ?? "no response"})`,
         );
         throw new Error(
            `Test package ${PACKAGE_NAME} did not become available within 30s ` +
               `(last status: ${lastPkgStatus ?? "no response"})`,
         );
      }
      diag(`package ${PACKAGE_NAME} ready`);
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
         await fetch(url(`/materializations/${id}?action=stop`), {
            method: "POST",
         });
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
               url(`/materializations/${materializationId}?action=start`),
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
            const reloadRes = await fetch(url("/manifest?action=reload"), {
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

         await fetch(url(`/materializations/${id}?action=stop`), {
            method: "POST",
         });

         const startRes = await fetch(
            url(`/materializations/${id}?action=start`),
            {
               method: "POST",
            },
         );
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

   // ── Group C: Package Teardown ────────────────────────────────────

   describe("package teardown", () => {
      it(
         "dryRun reports stale entries without dropping tables or deleting rows",
         async () => {
            // Run a full build so there are manifest entries to tear down.
            const createRes = await createMaterialization({
               autoLoadManifest: true,
            });
            expect(createRes.status).toBe(201);
            const created = (await createRes.json()) as Record<string, unknown>;
            const id = created.id as string;
            await fetch(url(`/materializations/${id}?action=start`), {
               method: "POST",
            });
            const terminal = await pollUntilTerminal(id);
            expect(terminal.status).toBe("SUCCESS");

            // Must delete the materialization record before teardown
            // (teardown refuses to run while an active materialization exists).
            await fetch(url(`/materializations/${id}`), { method: "DELETE" });

            // dryRun teardown — should report entries but not actually drop them.
            const teardownRes = await fetch(url("/materializations/teardown"), {
               method: "POST",
               headers: { "Content-Type": "application/json" },
               body: JSON.stringify({ dryRun: true }),
            });
            expect(teardownRes.status).toBe(200);
            const teardownResult = (await teardownRes.json()) as Record<
               string,
               unknown
            >;
            const dropped = teardownResult.dropped as Record<string, unknown>[];
            expect(dropped).toBeDefined();
            expect(teardownResult.errors).toBeDefined();

            // Manifest should still be intact after a dry run.
            const manifestRes = await fetch(url("/manifest"));
            expect(manifestRes.status).toBe(200);
            const manifest = (await manifestRes.json()) as Record<
               string,
               unknown
            >;
            const entries = manifest.entries as Record<string, unknown>;
            expect(Object.keys(entries).length).toBeGreaterThan(0);
         },
         { timeout: 60_000 },
      );

      it(
         "live teardown drops stale manifest entries and cleans up tables",
         async () => {
            // Build so there are manifest entries.
            const createRes = await createMaterialization({
               autoLoadManifest: true,
            });
            expect(createRes.status).toBe(201);
            const created = (await createRes.json()) as Record<string, unknown>;
            const id = created.id as string;
            await fetch(url(`/materializations/${id}?action=start`), {
               method: "POST",
            });
            const terminal = await pollUntilTerminal(id);
            expect(terminal.status).toBe("SUCCESS");

            await fetch(url(`/materializations/${id}`), { method: "DELETE" });

            // Live teardown — should drop everything since all entries are
            // stale (no active build claims them).
            const teardownRes = await fetch(url("/materializations/teardown"), {
               method: "POST",
               headers: { "Content-Type": "application/json" },
               body: JSON.stringify({}),
            });
            expect(teardownRes.status).toBe(200);
            const teardownResult = (await teardownRes.json()) as Record<
               string,
               unknown
            >;
            const dropped = teardownResult.dropped as Record<string, unknown>[];
            expect(dropped.length).toBeGreaterThan(0);
            expect((teardownResult.errors as unknown[]).length).toBe(0);

            // Manifest should be empty after live teardown.
            const manifestRes = await fetch(url("/manifest"));
            expect(manifestRes.status).toBe(200);
            const manifest = (await manifestRes.json()) as Record<
               string,
               unknown
            >;
            const entries = manifest.entries as Record<string, unknown>;
            expect(Object.keys(entries).length).toBe(0);
         },
         { timeout: 60_000 },
      );

      it("teardown rejects while an active materialization exists", async () => {
         const createRes = await createMaterialization();
         expect(createRes.status).toBe(201);
         const created = (await createRes.json()) as Record<string, unknown>;
         const id = created.id as string;

         const teardownRes = await fetch(url("/materializations/teardown"), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({}),
         });
         expect(teardownRes.status).toBe(409);

         await cleanup(id);
      });

      it(
         "forceRefresh rebuilds and post-build GC step executes",
         async () => {
            // First build — populates manifest.
            const first = await createMaterialization({
               autoLoadManifest: true,
            });
            expect(first.status).toBe(201);
            const firstData = (await first.json()) as Record<string, unknown>;
            const firstId = firstData.id as string;
            await fetch(url(`/materializations/${firstId}?action=start`), {
               method: "POST",
            });
            const firstTerminal = await pollUntilTerminal(firstId);
            expect(firstTerminal.status).toBe("SUCCESS");
            await fetch(url(`/materializations/${firstId}`), {
               method: "DELETE",
            });

            // Second build with forceRefresh — the buildId won't change
            // (hash of SQL + connection is identical), but forceRefresh
            // forces a rebuild rather than skipping.
            const second = await createMaterialization({
               forceRefresh: true,
               autoLoadManifest: true,
            });
            expect(second.status).toBe(201);
            const secondData = (await second.json()) as Record<string, unknown>;
            const secondId = secondData.id as string;
            await fetch(url(`/materializations/${secondId}?action=start`), {
               method: "POST",
            });
            const secondTerminal = await pollUntilTerminal(secondId);
            expect(secondTerminal.status).toBe("SUCCESS");

            const metadata = secondTerminal.metadata as Record<string, unknown>;
            // forceRefresh should actually rebuild, not skip.
            expect(metadata.sourcesBuilt).toBeGreaterThan(0);
            expect(metadata.sourcesSkipped).toBe(0);
            // Post-build GC step ran (arrays present even if empty).
            expect(metadata.gcDropped).toBeDefined();
            expect(metadata.gcErrors).toBeDefined();

            // Manifest should still have entries after rebuild.
            const manifestRes = await fetch(url("/manifest"));
            const manifest = (await manifestRes.json()) as Record<
               string,
               unknown
            >;
            const entries = manifest.entries as Record<string, unknown>;
            expect(Object.keys(entries).length).toBeGreaterThan(0);

            await cleanup(secondId);
         },
         { timeout: 90_000 },
      );
   });
});
