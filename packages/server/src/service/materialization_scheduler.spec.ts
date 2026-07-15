import { describe, expect, it } from "bun:test";
import { MaterializationConflictError } from "../errors";
import type { CronEvaluator } from "./cron_evaluator";
import type { EnvironmentStore } from "./environment_store";
import type { MaterializationService } from "./materialization_service";
import { MaterializationScheduler } from "./materialization_scheduler";

// ── Fakes ────────────────────────────────────────────────────────────────

interface FakePkgOpts {
   schedule?: string | null;
   manifestLocation?: string | null;
   policyWarnings?: string[];
}

function fakePackage(name: string, opts: FakePkgOpts = {}) {
   const {
      schedule = null,
      manifestLocation = null,
      policyWarnings = [],
   } = opts;
   return {
      getPackageName: () => name,
      getPackageMetadata: () => ({
         materialization: schedule ? { schedule } : null,
         manifestLocation,
      }),
      persistencePolicyWarnings: () => policyWarnings,
   };
}

function fakeEnv(name: string, pkgs: ReturnType<typeof fakePackage>[]) {
   return {
      getEnvironmentName: () => name,
      getLoadedPackages: () => pkgs,
   };
}

function fakeStore(envs: ReturnType<typeof fakeEnv>[]): EnvironmentStore {
   return {
      getLoadedEnvironments: () => envs,
   } as unknown as EnvironmentStore;
}

interface FireCall {
   env: string;
   pkg: string;
   opts: { forceRefresh?: boolean; trigger?: string };
}

function fakeService(
   opts: {
      throwConflict?: boolean;
      throwError?: boolean;
      // Newest recorded SCHEDULER fire, used by the restart-recovery anchor.
      lastFireAt?: Date | null;
   } = {},
) {
   const calls: FireCall[] = [];
   const service = {
      calls,
      createMaterialization: (
         env: string,
         pkg: string,
         o: { forceRefresh?: boolean; trigger?: string },
      ) => {
         calls.push({ env, pkg, opts: o });
         if (opts.throwConflict) {
            return Promise.reject(new MaterializationConflictError("busy"));
         }
         if (opts.throwError) {
            return Promise.reject(new Error("boom"));
         }
         return Promise.resolve({});
      },
      getLatestScheduledFireAt: () => Promise.resolve(opts.lastFireAt ?? null),
   };
   return service as unknown as MaterializationService & { calls: FireCall[] };
}

// A deterministic cron: nextAfter is always `from + 60s`; isValid configurable.
function fakeCron(
   isValid: (expr: string) => boolean = () => true,
): CronEvaluator {
   return {
      isValid,
      nextAfter: (_expr: string, from: Date) =>
         new Date(from.getTime() + 60_000),
   } as unknown as CronEvaluator;
}

const CONFIG = { tickIntervalMs: 60_000, maxFiresPerTick: 10 };

function makeScheduler(
   pkgs: ReturnType<typeof fakePackage>[],
   service: ReturnType<typeof fakeService>,
   cron: CronEvaluator = fakeCron(),
   config = CONFIG,
) {
   const store = fakeStore([fakeEnv("env1", pkgs)]);
   return new MaterializationScheduler(store, service, config, cron);
}

// ── Tests ────────────────────────────────────────────────────────────────

describe("MaterializationScheduler", () => {
   const t0 = 1_000_000_000_000; // fixed epoch ms
   const dueLater = t0 + 60_001; // just past the armed nextFire (t0 + 60s)

   it("does not fire on the arming tick (nextFire is strictly future)", async () => {
      const service = fakeService();
      const sched = makeScheduler(
         [fakePackage("p", { schedule: "* * * * *" })],
         service,
      );
      await sched.tick(t0);
      expect(service.calls.length).toBe(0);
   });

   it("recovers a missed occurrence on first arm, firing once then jumping forward", async () => {
      // A prior SCHEDULER fire well in the past: anchoring nextFire to
      // nextAfter(lastFire) lands before t0, so the occurrence missed during
      // "downtime" is due on the very first tick (a restart recovering).
      const service = fakeService({ lastFireAt: new Date(t0 - 10 * 60_000) });
      const sched = makeScheduler(
         [fakePackage("p", { schedule: "* * * * *" })],
         service,
      );
      await sched.tick(t0);
      expect(service.calls.length).toBe(1); // one catch-up fire
      // Jumped forward to nextAfter(now); not due again on the next tick.
      await sched.tick(t0 + 1);
      expect(service.calls.length).toBe(1);
   });

   it("does not catch up a never-fired schedule on first arm", async () => {
      // No prior SCHEDULER fire on record -> anchor is now -> strictly future,
      // so a freshly-scheduled (never-fired) package does not fire on arm.
      const service = fakeService({ lastFireAt: null });
      const sched = makeScheduler(
         [fakePackage("p", { schedule: "* * * * *" })],
         service,
      );
      await sched.tick(t0);
      expect(service.calls.length).toBe(0);
   });

   it("fires a due, valid, standalone package as SCHEDULER + forceRefresh", async () => {
      const service = fakeService();
      const sched = makeScheduler(
         [fakePackage("p", { schedule: "* * * * *" })],
         service,
      );
      await sched.tick(t0); // arm
      await sched.tick(dueLater); // due -> fire
      expect(service.calls).toEqual([
         {
            env: "env1",
            pkg: "p",
            opts: { forceRefresh: true, trigger: "SCHEDULER" },
         },
      ]);
   });

   it("skips a package with no schedule", async () => {
      const service = fakeService();
      const sched = makeScheduler([fakePackage("p", {})], service);
      await sched.tick(t0);
      await sched.tick(dueLater);
      expect(service.calls.length).toBe(0);
   });

   it("skips an orchestrated package (manifestLocation set) — Guard 2", async () => {
      const service = fakeService();
      const sched = makeScheduler(
         [
            fakePackage("p", {
               schedule: "* * * * *",
               manifestLocation: "gs://cp/manifest.json",
            }),
         ],
         service,
      );
      await sched.tick(t0);
      await sched.tick(dueLater);
      expect(service.calls.length).toBe(0);
   });

   it("skips a package whose persistence policy is invalid", async () => {
      const service = fakeService();
      const sched = makeScheduler(
         [
            fakePackage("p", {
               schedule: "* * * * *",
               policyWarnings: ["schedule requires scope: version"],
            }),
         ],
         service,
      );
      await sched.tick(t0);
      await sched.tick(dueLater);
      expect(service.calls.length).toBe(0);
   });

   it("skips an invalid cron", async () => {
      const service = fakeService();
      const sched = makeScheduler(
         [fakePackage("p", { schedule: "not a cron" })],
         service,
         fakeCron(() => false),
      );
      await sched.tick(t0);
      await sched.tick(dueLater);
      expect(service.calls.length).toBe(0);
   });

   it("coalesces when a materialization is already active (no throw)", async () => {
      const service = fakeService({ throwConflict: true });
      const sched = makeScheduler(
         [fakePackage("p", { schedule: "* * * * *" })],
         service,
      );
      await sched.tick(t0);
      await sched.tick(dueLater); // due -> fire attempt -> conflict, swallowed
      expect(service.calls.length).toBe(1); // attempted once
      // Advanced past this occurrence: not due again until the next window.
      await sched.tick(dueLater + 1);
      expect(service.calls.length).toBe(1);
   });

   it("isolates an unexpected fire error (sweep keeps going)", async () => {
      const service = fakeService({ throwError: true });
      const sched = makeScheduler(
         [fakePackage("p", { schedule: "* * * * *" })],
         service,
      );
      await sched.tick(t0);
      await expect(sched.tick(dueLater)).resolves.toBeUndefined();
      expect(service.calls.length).toBe(1);
   });

   it("caps fires per tick (stampede guard)", async () => {
      const service = fakeService();
      const pkgs = [
         fakePackage("a", { schedule: "* * * * *" }),
         fakePackage("b", { schedule: "* * * * *" }),
         fakePackage("c", { schedule: "* * * * *" }),
      ];
      const sched = makeScheduler(pkgs, service, fakeCron(), {
         tickIntervalMs: 60_000,
         maxFiresPerTick: 2,
      });
      await sched.tick(t0); // arm all
      await sched.tick(dueLater); // all due, but cap = 2
      expect(service.calls.length).toBe(2);
      // The capped one is still due and fires on the next tick.
      await sched.tick(dueLater + 1);
      expect(service.calls.length).toBe(3);
   });
});
