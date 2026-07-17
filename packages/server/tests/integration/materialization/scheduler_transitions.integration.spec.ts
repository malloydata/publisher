/// <reference types="bun-types" />

import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import { MaterializationScheduler } from "../../../src/service/materialization_scheduler";
import { MaterializationService } from "../../../src/service/materialization_service";
import { environmentStore } from "../../../src/server";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const FIXTURES = path.resolve(__dirname, "../../fixtures");
const SCHEDULED_FIXTURE = path.join(FIXTURES, "persist-schedule-test"); // cron "0 6 * * *", scope: version
const PERSIST_FIXTURE = path.join(FIXTURES, "persist-test"); // same persist source, no schedule

// Deterministic clock: tick(nowMs) is called explicitly so the daily
// "0 6 * * *" cron fires without waiting on a wall-clock minute.
const T0 = Date.parse("2026-01-01T00:00:00.000Z");
const MIN = 60_000;
const H = 60 * MIN;
const D = 24 * H;

const TERMINAL = ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"];

// Integration coverage for the scheduler's state-machine transitions against the
// real store (#895): the unit spec covers the per-tick guards; these drive the
// transitions end to end through the REST-created packages the scheduler sweeps.
// Each test uses its own environment and tears it down in afterEach — the
// scheduler sweeps every loaded environment, so a leftover env would leak fires
// into another test (and steal the per-tick cap in the carryover case).
describe("MaterializationScheduler transitions (integration, real store)", () => {
   let e2e: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   let currentEnv: string | null = null;

   beforeAll(async () => {
      e2e = await startRestE2E();
      baseUrl = e2e.baseUrl;
   });

   afterEach(async () => {
      if (currentEnv) {
         try {
            await fetch(`${baseUrl}/api/v0/environments/${currentEnv}`, {
               method: "DELETE",
            });
         } catch {
            // best-effort teardown
         }
         currentEnv = null;
      }
   });

   afterAll(async () => {
      await e2e?.stop();
      e2e = null;
   });

   // ── helpers ──────────────────────────────────────────────────────────

   function newScheduler(maxFiresPerTick = 10): MaterializationScheduler {
      return new MaterializationScheduler(
         environmentStore,
         new MaterializationService(environmentStore),
         { tickIntervalMs: 60_000, maxFiresPerTick },
      );
   }

   async function createEnv(
      name: string,
      packages: Array<{ name: string; location: string }>,
   ): Promise<void> {
      const res = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({ name, packages, connections: [] }),
      });
      if (!res.ok) {
         throw new Error(
            `create env ${name} failed (${res.status}): ${await res.text()}`,
         );
      }
      currentEnv = name;
   }

   async function addPackage(
      env: string,
      name: string,
      location: string,
   ): Promise<void> {
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${env}/packages`,
         {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ name, location }),
         },
      );
      if (!res.ok) {
         throw new Error(
            `add package ${name} failed (${res.status}): ${await res.text()}`,
         );
      }
   }

   async function waitForPackage(env: string, pkg: string): Promise<void> {
      const deadline = Date.now() + 30_000;
      while (Date.now() < deadline) {
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${env}/packages/${pkg}`,
         );
         if (res.ok) return;
         await new Promise((r) => setTimeout(r, 500));
      }
      throw new Error(`package ${env}/${pkg} did not load in time`);
   }

   async function deletePackage(env: string, pkg: string): Promise<void> {
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${env}/packages/${pkg}`,
         { method: "DELETE" },
      );
      expect(res.ok).toBe(true);
   }

   // Set scope: version + a cron schedule on a package (makes persist-test
   // standalone-schedulable). editingPolicy is true, so the publish-gate rules
   // run and must pass (version scope + valid cron, no freshness).
   async function setSchedule(
      env: string,
      pkg: string,
      schedule: string,
   ): Promise<void> {
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${env}/packages/${pkg}`,
         {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
               name: pkg,
               scope: "version",
               materialization: { schedule },
            }),
         },
      );
      if (!res.ok) {
         throw new Error(
            `set schedule on ${pkg} failed (${res.status}): ${await res.text()}`,
         );
      }
   }

   type Row = Record<string, unknown>;

   async function envMaterializations(env: string): Promise<Row[]> {
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${env}/packages/materializations`,
      );
      expect(res.status).toBe(200);
      return (await res.json()) as Row[];
   }

   function triggerOf(m: Row): string | undefined {
      return (m.metadata as { trigger?: string } | null)?.trigger;
   }

   async function scheduledFireCount(env: string): Promise<number> {
      return (await envMaterializations(env)).filter(
         (m) => triggerOf(m) === "SCHEDULER",
      ).length;
   }

   // Let every fired build settle so env teardown doesn't race an in-flight one.
   async function waitAllTerminal(env: string): Promise<void> {
      const deadline = Date.now() + 90_000;
      while (Date.now() < deadline) {
         const rows = await envMaterializations(env);
         if (rows.every((m) => TERMINAL.includes(m.status as string))) return;
         await new Promise((r) => setTimeout(r, 250));
      }
      throw new Error(`materializations in ${env} did not all settle`);
   }

   // ── transitions ─────────────────────────────────────────────────────

   it(
      "a cron edit between ticks re-arms from now (no stale fire from the old cadence)",
      async () => {
         const ENV = "sched-tx-cron-edit";
         await createEnv(ENV, [
            { name: "persist-schedule-test", location: SCHEDULED_FIXTURE },
         ]);
         await waitForPackage(ENV, "persist-schedule-test");
         const sched = newScheduler();

         // Arm on the original "0 6 * * *" cadence: nextFire = Jan-01 06:00.
         await sched.tick(T0);

         // Edit the cron to midnight before the old 06:00 occurrence fires.
         await setSchedule(ENV, "persist-schedule-test", "0 0 * * *");

         // Tick just past the OLD 06:00 occurrence. If the edit didn't re-arm,
         // the stale nextFire would fire here; instead arm() re-anchors from now
         // on the new cron (next = Jan-02 00:00), so nothing fires.
         await sched.tick(T0 + 6 * H + MIN);
         expect(await scheduledFireCount(ENV)).toBe(0);

         // Past the NEW cron's next occurrence (Jan-02 00:00): it fires, proving
         // the edit re-armed rather than silently dropping the schedule.
         await sched.tick(T0 + D + MIN);
         expect(await scheduledFireCount(ENV)).toBe(1);

         await waitAllTerminal(ENV);
      },
      { timeout: 120_000 },
   );

   it(
      "a package unload then reload prunes and re-anchors without a spurious fire",
      async () => {
         const ENV = "sched-tx-unload";
         await createEnv(ENV, [
            { name: "persist-schedule-test", location: SCHEDULED_FIXTURE },
         ]);
         await waitForPackage(ENV, "persist-schedule-test");
         const sched = newScheduler();

         // Arm: nextFire = Jan-01 06:00.
         await sched.tick(T0);

         // Unload the package: it leaves getLoadedPackages, so the sweep prunes
         // its arming state (tick's not-seen cleanup).
         await deletePackage(ENV, "persist-schedule-test");
         await sched.tick(T0 + D); // well past the old 06:00 — package is gone
         expect(await scheduledFireCount(ENV)).toBe(0);

         // Reload it: a fresh arm anchors from now (no prior SCHEDULER fire), so
         // the pre-unload nextFire can't resurface as a spurious catch-up.
         await addPackage(ENV, "persist-schedule-test", SCHEDULED_FIXTURE);
         await waitForPackage(ENV, "persist-schedule-test");
         await sched.tick(T0 + D + 5 * MIN); // now < fresh nextFire (Jan-02 06:00)
         expect(await scheduledFireCount(ENV)).toBe(0);
      },
      { timeout: 120_000 },
   );

   it(
      "a fired occurrence advances to the next occurrence (fires once per occurrence, not every tick)",
      async () => {
         const ENV = "sched-tx-cadence";
         await createEnv(ENV, [
            { name: "persist-schedule-test", location: SCHEDULED_FIXTURE },
         ]);
         await waitForPackage(ENV, "persist-schedule-test");
         const sched = newScheduler();

         await sched.tick(T0); // arm
         await sched.tick(T0 + 6 * H + MIN); // due -> fire once
         expect(await scheduledFireCount(ENV)).toBe(1);
         await waitAllTerminal(ENV);

         // Immediately after, still inside the same daily window: the scheduler
         // advanced nextFire to the next occurrence, so it does NOT re-fire every
         // tick. (The advance is outcome-independent — a FAILED fire advances the
         // same way; that isolation is covered in the scheduler unit spec.)
         await sched.tick(T0 + 6 * H + 2 * MIN);
         expect(await scheduledFireCount(ENV)).toBe(1);

         // The next day's 06:00 occurrence fires again.
         await sched.tick(T0 + D + 6 * H + MIN);
         expect(await scheduledFireCount(ENV)).toBe(2);
         await waitAllTerminal(ENV);
      },
      { timeout: 180_000 },
   );

   it(
      "maxFiresPerTick caps a tick and the capped package fires on the next tick",
      async () => {
         const ENV = "sched-tx-cap";
         await createEnv(ENV, [
            { name: "persist-schedule-test", location: SCHEDULED_FIXTURE },
            { name: "persist-test", location: PERSIST_FIXTURE },
         ]);
         await waitForPackage(ENV, "persist-schedule-test");
         await waitForPackage(ENV, "persist-test");
         // Make the second package standalone-schedulable on the same cadence.
         await setSchedule(ENV, "persist-test", "0 6 * * *");

         const sched = newScheduler(1); // cap = 1 fire per tick

         await sched.tick(T0); // arm both
         // Both due at 06:00, but the cap lets only one fire this tick.
         await sched.tick(T0 + 6 * H + MIN);
         expect(await scheduledFireCount(ENV)).toBe(1);

         // The capped package is still due and fires on the following tick.
         await sched.tick(T0 + 6 * H + 2 * MIN);
         expect(await scheduledFireCount(ENV)).toBe(2);

         await waitAllTerminal(ENV);
      },
      { timeout: 180_000 },
   );
});
