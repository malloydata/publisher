/// <reference types="bun-types" />

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import { CronEvaluator } from "../../../src/service/cron_evaluator";
import { MaterializationScheduler } from "../../../src/service/materialization_scheduler";
import { MaterializationService } from "../../../src/service/materialization_service";
import { environmentStore } from "../../../src/server";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PROJECT_NAME = "sched-recovery-project";
const PACKAGE_NAME = "persist-schedule-test";
const CRON = "0 6 * * *"; // the fixture's daily schedule
const API = `/api/v0/environments/${PROJECT_NAME}/packages/${PACKAGE_NAME}`;
const CONFIG = { tickIntervalMs: 60_000, maxFiresPerTick: 10 };
const TERMINAL = ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"];

// The restart-recovery anchor is only unit-tested against a stubbed
// getLatestScheduledFireAt. This drives the real sequence end to end: a real
// SCHEDULER fire lands a row in the store, then a *second* scheduler instance
// (a simulated process restart — empty in-memory arming state) re-anchors from
// that row via the real repo query. We assert exactly one catch-up when an
// occurrence came due during "downtime", and no double-fire when it didn't.
describe("MaterializationScheduler restart recovery (E2E, real store)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   let service: MaterializationService;
   // A fixed anchor for the *first* scheduler's arming clock; the real fire's
   // created_at (wall clock) is what recovery actually re-anchors from, and we
   // read it back from the store rather than assuming it.
   const T0 = Date.parse("2026-01-01T00:00:00Z");
   const THREE_DAYS_MS = 3 * 24 * 60 * 60 * 1000;

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      service = new MaterializationService(environmentStore);

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

      // Wait for the package to load into the store the scheduler sweeps.
      const deadline = Date.now() + 30_000;
      while (Date.now() < deadline) {
         const res = await fetch(`${baseUrl}${API}`);
         if (res.ok) break;
         await new Promise((r) => setTimeout(r, 500));
      }
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

   function newScheduler(): MaterializationScheduler {
      // A fresh instance == a process restart: no in-memory arming state, so the
      // first arm re-anchors from the store.
      return new MaterializationScheduler(environmentStore, service, CONFIG);
   }

   async function scheduledCount(): Promise<number> {
      const res = await fetch(`${baseUrl}${API}/materializations`);
      expect(res.status).toBe(200);
      const rows = (await res.json()) as Array<Record<string, unknown>>;
      return rows.filter(
         (m) =>
            (m.metadata as { trigger?: string } | null)?.trigger ===
            "SCHEDULER",
      ).length;
   }

   async function pollAllTerminal(): Promise<void> {
      const deadline = Date.now() + 90_000;
      while (Date.now() < deadline) {
         const res = await fetch(`${baseUrl}${API}/materializations`);
         const rows = (await res.json()) as Array<Record<string, unknown>>;
         if (rows.every((m) => TERMINAL.includes(m.status as string))) return;
         await new Promise((r) => setTimeout(r, 250));
      }
      throw new Error("materializations did not all reach a terminal state");
   }

   it(
      "recovers exactly one missed occurrence across a restart, then jumps forward",
      async () => {
         const cron = new CronEvaluator();

         // ── Scheduler #1: produce a real baseline SCHEDULER fire. ──
         const s1 = newScheduler();
         await s1.tick(T0); // arm (nextFire strictly future -> no fire)
         expect(await scheduledCount()).toBe(0);
         await s1.tick(T0 + THREE_DAYS_MS); // an occurrence is due -> one fire
         expect(await scheduledCount()).toBe(1);
         await pollAllTerminal();

         // The instant recovery re-anchors from is the newest SCHEDULER row's
         // created_at (wall clock), read through the same path the scheduler uses.
         const lastFire = await service.getLatestScheduledFireAt(
            PROJECT_NAME,
            PACKAGE_NAME,
         );
         expect(lastFire).not.toBeNull();
         const missedOccurrence = cron.nextAfter(CRON, lastFire!).getTime();

         // ── Scheduler #2: simulated restart with an occurrence missed during
         // downtime (now is just past the first occurrence after the last fire). ──
         const s2 = newScheduler();
         await s2.tick(missedOccurrence + 60_000); // due on first arm -> catch up
         expect(await scheduledCount()).toBe(2); // exactly one catch-up
         await pollAllTerminal();
         // Advanced forward: the same instance does not fire again next tick.
         await s2.tick(missedOccurrence + 120_000);
         expect(await scheduledCount()).toBe(2);
      },
      { timeout: 200_000 },
   );

   it(
      "does not double-fire on a restart when no occurrence was missed",
      async () => {
         const cron = new CronEvaluator();
         const before = await scheduledCount();

         // Latest SCHEDULER fire is the catch-up above (created_at ~ now). The
         // next occurrence after it is the next 06:00 UTC, hours away.
         const lastFire = await service.getLatestScheduledFireAt(
            PROJECT_NAME,
            PACKAGE_NAME,
         );
         expect(lastFire).not.toBeNull();
         const nextOccurrence = cron.nextAfter(CRON, lastFire!).getTime();

         // ── Scheduler #3: simulated restart shortly after the last fire, before
         // the next occurrence is due -> arms into the future, no catch-up. ──
         const s3 = newScheduler();
         await s3.tick(lastFire!.getTime() + 1_000);
         expect(await scheduledCount()).toBe(before);
         expect(lastFire!.getTime() + 1_000).toBeLessThan(nextOccurrence);
      },
      { timeout: 120_000 },
   );
});
