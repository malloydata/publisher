/// <reference types="bun-types" />

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PROJECT_NAME = "test-project-refmanifest";
const PACKAGE_NAME = "persist-multi-level";
const API = `/api/v0/environments/${PROJECT_NAME}/packages/${PACKAGE_NAME}`;

const TERMINAL_STATUSES = ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"];

/**
 * WI-1 (Phase D): the orchestrated build seeds its build Manifest from
 * `referenceManifest` and honors `strictUpstreams`. Proven end-to-end over a
 * two-level persist DAG (`orders_base` -> `orders_rollup`):
 *
 *  - Build the downstream `orders_rollup` ALONE with a `referenceManifest`
 *    pointing at the already-materialized `orders_base` table and
 *    `strictUpstreams=true` -> the build SUCCEEDS, which under strict mode is
 *    only possible if the upstream reference resolved to the physical table
 *    (a strict miss would throw).
 *  - Build `orders_rollup` ALONE under `strictUpstreams=true` with NO
 *    `referenceManifest` -> the build FAILS loudly (runtime-manifest-strict-miss)
 *    instead of silently recomputing the upstream live.
 */
describe("Materialization reference manifest + strictUpstreams (E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;

      const fixtureDir = path.resolve(
         __dirname,
         "../../fixtures/persist-multi-level",
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
         const body = await createRes.text();
         throw new Error(
            `Failed to create test project (${createRes.status}): ${body}`,
         );
      }

      const deadline = Date.now() + 30_000;
      let pkgReady = false;
      while (!pkgReady && Date.now() < deadline) {
         try {
            const res = await fetch(`${baseUrl}${API}`);
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
      timeoutMs = 90_000,
   ): Promise<Record<string, unknown>> {
      const deadline = Date.now() + timeoutMs;
      while (Date.now() < deadline) {
         const res = await fetch(url(`/materializations/${id}`));
         expect(res.status).toBe(200);
         const data = (await res.json()) as Record<string, unknown>;
         if (TERMINAL_STATUSES.includes(data.status as string)) return data;
         await new Promise((r) => setTimeout(r, 250));
      }
      throw new Error(`Materialization ${id} did not reach a terminal state`);
   }

   /** Delete a terminal materialization record (optionally dropping its tables). */
   async function deleteRun(id: string, dropTables = false): Promise<void> {
      await fetch(
         url(`/materializations/${id}${dropTables ? "?dropTables=true" : ""}`),
         { method: "DELETE" },
      );
   }

   /** Build one source alone and drive it to a terminal state. */
   async function buildOneAlone(
      body: Record<string, unknown>,
   ): Promise<Record<string, unknown>> {
      const createRes = await createMaterialization(body);
      expect(createRes.status).toBe(201);
      const { id } = (await createRes.json()) as { id: string };
      const settled = await pollUntilTerminal(id);
      return { id, ...settled };
   }

   /** The planned sources keyed by name (off Package.buildPlan). */
   async function planSourcesByName(): Promise<
      Record<string, Record<string, unknown>>
   > {
      const res = await fetch(url(""));
      expect(res.status).toBe(200);
      const pkg = (await res.json()) as Record<string, unknown>;
      const plan = pkg.buildPlan as Record<string, unknown>;
      expect(plan).toBeDefined();
      const sources = plan.sources as Record<string, Record<string, unknown>>;
      const byName: Record<string, Record<string, unknown>> = {};
      for (const s of Object.values(sources)) {
         byName[s.name as string] = s;
      }
      return byName;
   }

   const UPSTREAM_TABLE = "orders_base_built";

   it(
      "builds a downstream source alone, referencing the upstream's physical table under strict",
      async () => {
         const sources = await planSourcesByName();
         const base = sources["orders_base"];
         const rollup = sources["orders_rollup"];
         expect(base).toBeDefined();
         expect(rollup).toBeDefined();

         // 1) Materialize the upstream `orders_base` into a known physical table.
         const baseRun = await buildOneAlone({
            buildInstructions: {
               sources: [
                  {
                     sourceEntityId: base.sourceEntityId,
                     sourceID: base.sourceID,
                     materializedTableId: "mt-base",
                     physicalTableName: UPSTREAM_TABLE,
                     realization: "COPY",
                  },
               ],
            },
         });
         expect(baseRun.status).toBe("MANIFEST_FILE_READY");
         await deleteRun(baseRun.id as string); // keep the physical table

         // 2) Build the downstream `orders_rollup` ALONE, referencing the
         // upstream's physical table under strictUpstreams. Success is only
         // possible if the reference resolved (strict forbids the live fallback).
         const rollupRun = await buildOneAlone({
            buildInstructions: {
               sources: [
                  {
                     sourceEntityId: rollup.sourceEntityId,
                     sourceID: rollup.sourceID,
                     materializedTableId: "mt-rollup",
                     physicalTableName: "orders_rollup_built",
                     realization: "COPY",
                  },
               ],
               referenceManifest: [
                  {
                     sourceEntityId: base.sourceEntityId,
                     physicalTableName: UPSTREAM_TABLE,
                  },
               ],
               strictUpstreams: true,
            },
         });
         expect(rollupRun.status).toBe("MANIFEST_FILE_READY");

         // Cleanup both physical tables and records.
         await deleteRun(rollupRun.id as string, true);
         // Drop the upstream table via a fresh no-op build+drop is overkill;
         // the environment teardown removes the project. Best-effort direct drop
         // is unnecessary for correctness here.
      },
      { timeout: 120_000 },
   );

   it(
      "fails loudly when a strict upstream is neither built nor referenced",
      async () => {
         const sources = await planSourcesByName();
         const rollup = sources["orders_rollup"];
         expect(rollup).toBeDefined();

         // Build `orders_rollup` ALONE under strictUpstreams with NO
         // referenceManifest for its upstream -> the compile throws
         // runtime-manifest-strict-miss and the run FAILS.
         const run = await buildOneAlone({
            buildInstructions: {
               sources: [
                  {
                     sourceEntityId: rollup.sourceEntityId,
                     sourceID: rollup.sourceID,
                     materializedTableId: "mt-rollup-strict",
                     physicalTableName: "orders_rollup_strict",
                     realization: "COPY",
                  },
               ],
               strictUpstreams: true,
            },
         });

         expect(run.status).toBe("FAILED");
         expect(String(run.error ?? "")).toContain("manifest");

         await deleteRun(run.id as string, true);
      },
      { timeout: 120_000 },
   );
});
