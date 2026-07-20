/// <reference types="bun-types" />

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import type { MaterializationScheduler } from "../../../src/service/materialization_scheduler";
import { MaterializationService } from "../../../src/service/materialization_service";
import {
   environmentStore,
   startMaterializationSchedulerFromEnv,
} from "../../../src/server";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PROJECT_NAME = "sched-wiring-project";
const PACKAGE_NAME = "persist-schedule-test";
const API = `/api/v0/environments/${PROJECT_NAME}/packages/${PACKAGE_NAME}`;

const ENABLE = "PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER";
const INTERVAL = "PUBLISHER_MATERIALIZATION_SCHEDULER_INTERVAL_MS";

// The other scheduler tests hand-construct MaterializationScheduler and call
// tick() by hand, bypassing the env-var → getMaterializationSchedulerConfig →
// construct → start()/unref() → real-timer path an operator actually runs. This
// exercises that wiring: set the flag + a short interval, start the scheduler
// through the same seam server.ts uses at boot, and observe a build fire through
// the real timer (not a hand-driven tick).
describe("MaterializationScheduler wiring (E2E, real env-var timer)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   let scheduler: MaterializationScheduler | null = null;
   const prev: Record<string, string | undefined> = {};

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

      // Wait for the package to load, then switch it to an every-minute cron so
      // the real timer fires within the test window (the fixture ships a daily
      // cron, which a wall-clock test can't wait on).
      const deadline = Date.now() + 30_000;
      while (Date.now() < deadline) {
         const res = await fetch(`${baseUrl}${API}`);
         if (res.ok) break;
         await new Promise((r) => setTimeout(r, 500));
      }
      const patch = await fetch(`${baseUrl}${API}`, {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: PACKAGE_NAME,
            materialization: { schedule: "* * * * *" },
         }),
      });
      expect(patch.ok).toBe(true);
   });

   afterAll(async () => {
      scheduler?.stop();
      // Restore env so the shared test process isn't left with the flag set.
      for (const key of [ENABLE, INTERVAL]) {
         if (prev[key] === undefined) delete process.env[key];
         else process.env[key] = prev[key];
      }
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

   it(
      "boots the scheduler from env config and fires a build through the real timer",
      async () => {
         // Enable via the real env-var path and use the minimum 1s sweep so the
         // every-minute cron is picked up promptly once its boundary passes.
         prev[ENABLE] = process.env[ENABLE];
         prev[INTERVAL] = process.env[INTERVAL];
         process.env[ENABLE] = "true";
         process.env[INTERVAL] = "1000";

         scheduler = startMaterializationSchedulerFromEnv(
            environmentStore,
            new MaterializationService(environmentStore),
         );
         // The config path must have constructed + started a scheduler (a null
         // here would mean the enable flag wasn't honored).
         expect(scheduler).not.toBeNull();

         // Wait for the real timer to fire a SCHEDULER build and for it to build.
         const deadline = Date.now() + 150_000;
         let built: Record<string, unknown> | null = null;
         while (Date.now() < deadline && !built) {
            const res = await fetch(`${baseUrl}${API}/materializations`);
            const rows = (await res.json()) as Array<Record<string, unknown>>;
            built =
               rows.find(
                  (m) =>
                     (m.metadata as { trigger?: string } | null)?.trigger ===
                        "SCHEDULER" && m.status === "MANIFEST_FILE_READY",
               ) ?? null;
            if (!built) await new Promise((r) => setTimeout(r, 1_000));
         }

         expect(built).not.toBeNull();
         const entries = (
            built!.manifest as { entries: Record<string, unknown> }
         ).entries;
         expect(Object.keys(entries).length).toBe(1);

         // Stop the timer before the test ends so it can't fire into other files.
         scheduler!.stop();
      },
      { timeout: 180_000 },
   );
});
