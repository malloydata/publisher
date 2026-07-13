/// <reference types="bun-types" />

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fsp from "fs/promises";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import { MaterializationScheduler } from "../../../src/service/materialization_scheduler";
import { MaterializationService } from "../../../src/service/materialization_service";
import { environmentStore } from "../../../src/server";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PROJECT_NAME = "sched-project";
const PACKAGE_NAME = "persist-schedule-test";
const API = `/api/v0/environments/${PROJECT_NAME}/packages/${PACKAGE_NAME}`;

const TERMINAL = ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"];

// Deterministic clock for the scheduler: we call tick(nowMs) explicitly, so the
// fixture's daily cron ("0 6 * * *") fires without waiting on a real minute.
const T0 = Date.parse("2026-01-01T00:00:00Z");
const THREE_DAYS_MS = 3 * 24 * 60 * 60 * 1000;

describe("MaterializationScheduler (E2E, real build)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   let scheduler: MaterializationScheduler;

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;

      const fixtureDir = path.resolve(
         __dirname,
         "../../fixtures/persist-schedule-test",
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

      // Wait for the package to load (so it's in the environmentStore the
      // scheduler sweeps).
      const deadline = Date.now() + 30_000;
      while (Date.now() < deadline) {
         const res = await fetch(`${baseUrl}${API}`);
         if (res.ok) break;
         await new Promise((r) => setTimeout(r, 500));
      }

      // Drive the real scheduler against the server's real environmentStore and
      // a real MaterializationService (shares the same repository). We call
      // tick() by hand with a controlled clock rather than relying on the timer.
      scheduler = new MaterializationScheduler(
         environmentStore,
         new MaterializationService(environmentStore),
         { tickIntervalMs: 60_000, maxFiresPerTick: 10 },
      );
   });

   afterAll(async () => {
      if (baseUrl) {
         try {
            await fetch(`${baseUrl}/api/v0/environments/${PROJECT_NAME}`, {
               method: "DELETE",
            });
         } catch {
            // best-effort
         }
      }
      await env?.stop();
      env = null;
   });

   async function listMaterializations(): Promise<
      Array<Record<string, unknown>>
   > {
      const res = await fetch(`${baseUrl}${API}/materializations`);
      expect(res.status).toBe(200);
      return (await res.json()) as Array<Record<string, unknown>>;
   }

   function triggerOf(m: Record<string, unknown>): string | undefined {
      return (m.metadata as { trigger?: string } | null)?.trigger;
   }

   async function pollTerminal(id: string): Promise<Record<string, unknown>> {
      const deadline = Date.now() + 60_000;
      while (Date.now() < deadline) {
         const res = await fetch(`${baseUrl}${API}/materializations/${id}`);
         expect(res.status).toBe(200);
         const m = (await res.json()) as Record<string, unknown>;
         if (TERMINAL.includes(m.status as string)) return m;
         await new Promise((r) => setTimeout(r, 250));
      }
      throw new Error(`materialization ${id} did not reach a terminal state`);
   }

   it(
      "fires a due scheduled package as a SCHEDULER build that completes",
      async () => {
         // Arm on the first tick (nextFire is strictly future, so no fire yet).
         await scheduler.tick(T0);
         expect(
            (await listMaterializations()).filter(
               (m) => triggerOf(m) === "SCHEDULER",
            ).length,
         ).toBe(0);

         // Advance well past the daily cron: the sweep fires one SCHEDULER run.
         await scheduler.tick(T0 + THREE_DAYS_MS);

         const scheduled = (await listMaterializations()).filter(
            (m) => triggerOf(m) === "SCHEDULER",
         );
         expect(scheduled.length).toBe(1);

         // The real build settles at MANIFEST_FILE_READY, producing the table.
         const built = await pollTerminal(scheduled[0].id as string);
         expect(built.status).toBe("MANIFEST_FILE_READY");
         const entries = (built.manifest as { entries: Record<string, unknown> })
            .entries;
         expect(Object.keys(entries).length).toBe(1);
         // The SCHEDULER trigger must survive the build commit (not just exist
         // while the run is in-flight) — otherwise a completed scheduled run is
         // indistinguishable from a manual one.
         expect(triggerOf(built)).toBe("SCHEDULER");

         // Cleanup: drop the table + record so the negative case starts clean.
         const del = await fetch(
            `${baseUrl}${API}/materializations/${scheduled[0].id}?dropTables=true`,
            { method: "DELETE" },
         );
         expect(del.status).toBe(204);
      },
      { timeout: 120_000 },
   );

   it(
      "never fires a control-plane-driven package (manifestLocation set)",
      async () => {
         // Make the package orchestrated: point it at a (bind-clean) manifest.
         const manifestFile = path.join(
            os.tmpdir(),
            `sched-empty-manifest-${T0}.json`,
         );
         await fsp.writeFile(
            manifestFile,
            JSON.stringify({
               builtAt: new Date(T0).toISOString(),
               strict: false,
               entries: {},
            }),
            "utf8",
         );
         const patch = await fetch(`${baseUrl}${API}`, {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
               name: PACKAGE_NAME,
               manifestLocation: manifestFile,
            }),
         });
         expect(patch.ok).toBe(true);

         const before = (await listMaterializations()).length;

         // Even armed + well past the cron, an orchestrated package is skipped
         // (Guard 2) — the control plane owns its refresh.
         await scheduler.tick(T0);
         await scheduler.tick(T0 + THREE_DAYS_MS);

         expect((await listMaterializations()).length).toBe(before);

         await fetch(`${baseUrl}${API}`, {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ name: PACKAGE_NAME, manifestLocation: null }),
         });
      },
      { timeout: 60_000 },
   );
});
