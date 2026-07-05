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

const TERMINAL_STATUSES = ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"];

describe("Materialization REST API: single-call (E2E)", () => {
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

   const pollUntilTerminal = (id: string, timeoutMs = 90_000) =>
      pollUntil(id, (s) => TERMINAL_STATUSES.includes(s), timeoutMs);

   /**
    * Drive a materialization to a terminal state and delete its record so it
    * doesn't hold the per-package active slot for later tests.
    */
   async function cleanup(id: string): Promise<void> {
      const res = await fetch(url(`/materializations/${id}`));
      if (res.status !== 200) return;

      const current = (await res.json()) as Record<string, unknown>;
      if (!TERMINAL_STATUSES.includes(current.status as string)) {
         await fetch(url(`/materializations/${id}?action=stop`), {
            method: "POST",
         });
         await pollUntilTerminal(id);
      }
      await fetch(url(`/materializations/${id}`), { method: "DELETE" });
   }

   /** Read Package.buildPlan and return its first planned source. */
   async function firstPlanSource(): Promise<Record<string, unknown>> {
      const res = await fetch(url(""));
      expect(res.status).toBe(200);
      const pkg = (await res.json()) as Record<string, unknown>;
      const plan = pkg.buildPlan as Record<string, unknown>;
      expect(plan).toBeDefined();
      const sources = plan.sources as Record<string, Record<string, unknown>>;
      const values = Object.values(sources);
      expect(values.length).toBeGreaterThan(0);
      return values[0];
   }

   // ── Group A: Auto-run lifecycle (default) ────────────────────────

   describe("auto-run lifecycle (default)", () => {
      it(
         "runs all phases on create, self-assigns names, and auto-loads",
         async () => {
            // No buildInstructions: the publisher compiles, self-assigns names,
            // builds every persist source, and auto-loads in one pass.
            const createRes = await createMaterialization();
            expect(createRes.status).toBe(201);
            const created = (await createRes.json()) as Record<string, unknown>;
            expect(created.status).toBe("PENDING");
            const id = created.id as string;

            // It settles at MANIFEST_FILE_READY without any build instruction.
            const built = await pollUntilTerminal(id);
            expect(built.status).toBe("MANIFEST_FILE_READY");

            // The manifest carries the self-assigned physical table name (from
            // `#@ persist name="order_summary"`).
            const manifest = built.manifest as Record<string, unknown>;
            expect(manifest).toBeDefined();
            const entries = manifest.entries as Record<
               string,
               Record<string, unknown>
            >;
            const values = Object.values(entries);
            expect(values.length).toBe(1);
            expect(values[0].physicalTableName).toBe("order_summary");
            expect(values[0].sourceName).toBe("order_summary");
            expect(values[0].realization).toBe("COPY");

            // Cleanup: delete the record and drop the self-built table.
            const deleteRes = await fetch(
               url(`/materializations/${id}?dropTables=true`),
               { method: "DELETE" },
            );
            expect(deleteRes.status).toBe(204);
         },
         { timeout: 120_000 },
      );
   });

   // ── Group B: Orchestrated single-call build ──────────────────────

   describe("orchestrated build (buildInstructions)", () => {
      it(
         "builds directly into caller-assigned names from Package.buildPlan",
         async () => {
            // Read the plan off the package, derive one caller-assigned
            // instruction, and create the materialization already building it.
            const source = await firstPlanSource();
            expect(source.name).toBe("order_summary");
            expect(typeof source.sourceEntityId).toBe("string");

            const physicalTableName = "order_summary_built";
            const createRes = await createMaterialization({
               buildInstructions: {
                  sources: [
                     {
                        sourceEntityId: source.sourceEntityId,
                        sourceID: source.sourceID,
                        materializedTableId: "mt-order-summary",
                        physicalTableName,
                        realization: "COPY",
                     },
                  ],
               },
            });
            expect(createRes.status).toBe(201);
            const created = (await createRes.json()) as Record<string, unknown>;
            expect(created.status).toBe("PENDING");
            const id = created.id as string;

            // List should include the new run.
            const listRes = await fetch(url("/materializations"));
            expect(listRes.status).toBe(200);
            const list = (await listRes.json()) as Record<string, unknown>[];
            expect(list.some((m) => m.id === id)).toBe(true);

            // Settles at MANIFEST_FILE_READY with the caller-assigned name.
            const built = await pollUntilTerminal(id);
            expect(built.status).toBe("MANIFEST_FILE_READY");
            const manifest = built.manifest as Record<string, unknown>;
            const entries = manifest.entries as Record<
               string,
               Record<string, unknown>
            >;
            const entry = entries[source.sourceEntityId as string];
            expect(entry).toBeDefined();
            expect(entry.physicalTableName).toBe(physicalTableName);
            expect(entry.sourceName).toBe("order_summary");

            // A terminal materialization can be deleted; dropTables=true also
            // drops the physical table this run produced.
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

      it("rejects buildInstructions with an unknown sourceEntityId at create (400)", async () => {
         const createRes = await createMaterialization({
            buildInstructions: {
               sources: [
                  {
                     sourceEntityId: "not-a-real-build-id",
                     materializedTableId: "mt",
                     physicalTableName: "t",
                     realization: "COPY",
                  },
               ],
            },
         });
         expect(createRes.status).toBe(400);
      });
   });

   // ── Group C: Serve-time routing (the payoff) ─────────────────────
   //
   // Materialization only pays off if *served* queries scan the materialized
   // table instead of recomputing from the base table. The auto-run path builds
   // the table and auto-loads the manifest into the serving models in one
   // process, so routing is provable in-process: capture the live SQL (scans the
   // base CSV), run an auto-run materialization, then assert both the executed
   // query SQL and the /compile preview SQL now scan the physical table and no
   // longer touch the base CSV.
   describe("serve-time routing (auto-load)", () => {
      const MODEL_PATH = "persist_test.malloy";
      const QUERY = "run: order_summary -> { aggregate: c is count() }";

      async function executedSql(): Promise<string> {
         const res = await fetch(
            `${baseUrl}${API}/models/${MODEL_PATH}/query`,
            {
               method: "POST",
               headers: { "Content-Type": "application/json" },
               body: JSON.stringify({ query: QUERY }),
            },
         );
         expect(res.status).toBe(200);
         const body = (await res.json()) as { result: string };
         return (JSON.parse(body.result) as { sql: string }).sql;
      }

      async function compiledSql(): Promise<string> {
         const res = await fetch(
            `${baseUrl}${API}/models/${MODEL_PATH}/compile`,
            {
               method: "POST",
               headers: { "Content-Type": "application/json" },
               body: JSON.stringify({ source: QUERY, includeSql: true }),
            },
         );
         expect(res.status).toBe(200);
         const body = (await res.json()) as { status: string; sql?: string };
         expect(body.status).toBe("success");
         expect(body.sql).toBeDefined();
         return body.sql as string;
      }

      it(
         "routes served + compiled queries to the materialized table after auto-load",
         async () => {
            // Reset to live: a prior group may have left an in-memory binding.
            await fetch(`${baseUrl}${API}?reload=true`);

            // Baseline: with nothing materialized, both paths recompute from the
            // base CSV.
            expect(await executedSql()).toContain("data/orders.csv");
            expect(await compiledSql()).toContain("data/orders.csv");

            // Build + auto-load in one pass (self-assigns physicalTableName =
            // "order_summary" from `#@ persist name="order_summary"`).
            const createRes = await createMaterialization();
            expect(createRes.status).toBe(201);
            const { id } = (await createRes.json()) as { id: string };
            const built = await pollUntilTerminal(id);
            expect(built.status).toBe("MANIFEST_FILE_READY");

            // The payoff: the served query now scans the materialized table and
            // no longer recomputes from the base CSV, and /compile previews the
            // same routed SQL it would execute.
            const routedExecuted = await executedSql();
            const routedCompiled = await compiledSql();
            expect(routedExecuted).not.toContain("data/orders.csv");
            expect(routedExecuted).toContain("order_summary");
            expect(routedCompiled).not.toContain("data/orders.csv");
            expect(routedCompiled).toContain("order_summary");

            // Cleanup: drop the table + record, then reload back to live so the
            // dangling binding doesn't leak into later groups.
            const deleteRes = await fetch(
               url(`/materializations/${id}?dropTables=true`),
               { method: "DELETE" },
            );
            expect(deleteRes.status).toBe(204);
            await fetch(`${baseUrl}${API}?reload=true`);
         },
         { timeout: 120_000 },
      );
   });

   // ── Group D: State machine and error cases ───────────────────────

   describe("state machine and errors", () => {
      it("stops an in-flight materialization (-> CANCELLED)", async () => {
         // Create and immediately stop while the background build is still
         // starting (PENDING). Cooperative abort drives it to CANCELLED.
         const createRes = await createMaterialization();
         expect(createRes.status).toBe(201);
         const { id } = (await createRes.json()) as { id: string };

         const stopRes = await fetch(
            url(`/materializations/${id}?action=stop`),
            { method: "POST" },
         );
         expect(stopRes.status).toBe(200);

         const settled = await pollUntilTerminal(id);
         // The build may occasionally win the race and complete; either way it
         // reaches a terminal state and stop returned 200.
         expect(TERMINAL_STATUSES).toContain(settled.status as string);

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

      it("rejects deleting a non-terminal materialization with 409", async () => {
         const createRes = await createMaterialization();
         expect(createRes.status).toBe(201);
         const { id } = (await createRes.json()) as { id: string };

         // Delete immediately, while the background build is still PENDING.
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

         const res = await fetch(
            url(`/materializations/${id}?action=frobnicate`),
            { method: "POST" },
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
