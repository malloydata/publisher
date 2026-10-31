// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it, spyOn } from "bun:test";
import { MaterializationConflictError } from "../errors";
import { logger } from "../logger";
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

/** A published version, as the registry lists it. */
interface FakeVersion {
   packageName: string;
   versionId: string;
   dirName: string;
   archiveStatus: "archive" | "unarchive";
   manifestPath: string | null;
   /** Its publisher.json. */
   manifest: Record<string, unknown> | null;
}

function fakeVersion(
   versionId: string,
   manifest: Record<string, unknown> | null,
   overrides: Partial<FakeVersion> = {},
): FakeVersion {
   return {
      packageName: "v",
      versionId,
      dirName: versionId,
      archiveStatus: "unarchive",
      manifestPath: null,
      manifest,
      ...overrides,
   };
}

function fakeEnv(
   name: string,
   pkgs: ReturnType<typeof fakePackage>[],
   versions: FakeVersion[] | null = null,
) {
   return {
      getEnvironmentName: () => name,
      getLoadedPackages: () => pkgs,
      peekVersion: () => undefined,
      getVersionService: () =>
         versions === null
            ? null
            : {
                 activeVersions: async () =>
                    versions.filter((v) => v.archiveStatus !== "archive"),
                 publishedManifestOf: async (v: FakeVersion) => v.manifest,
              },
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
   opts: { forceRefresh?: boolean; trigger?: string; versionId?: string };
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
         o: { forceRefresh?: boolean; trigger?: string; versionId?: string },
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
      anchorsAsked: [] as (string | undefined)[],
      getLatestScheduledFireAt: (
         _env: string,
         _pkg: string,
         versionId?: string,
      ) => {
         service.anchorsAsked.push(versionId);
         return Promise.resolve(opts.lastFireAt ?? null);
      },
   };
   return service as unknown as MaterializationService & {
      calls: FireCall[];
      anchorsAsked: (string | undefined)[];
   };
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

   it("prunes arming state when a package unloads, then re-anchors on reload without a stale fire", async () => {
      // A never-fired schedule (fakeService default lastFireAt = null), so a
      // fresh arm anchors from `now` and does not catch up.
      const service = fakeService();
      const p = fakePackage("p", { schedule: "* * * * *" });
      // makeScheduler closes over this array via getLoadedPackages, so mutating
      // it simulates the package leaving and re-entering the loaded set.
      const loaded = [p];
      const sched = makeScheduler(loaded, service);

      await sched.tick(t0); // arm: nextFire = t0 + 60s

      // Unload: the package leaves the loaded set, so the sweep prunes its state
      // (tick's not-seen cleanup) instead of leaving a stale nextFire behind.
      loaded.length = 0;
      await sched.tick(dueLater); // past the old nextFire, but the package is gone
      expect(service.calls.length).toBe(0);

      // Reload: with the state pruned, arm is fresh — anchored from `now`, so the
      // pre-unload nextFire can't resurface as a spurious catch-up.
      loaded.push(p);
      await sched.tick(dueLater + 1); // now < fresh nextFire -> no fire
      expect(service.calls.length).toBe(0);

      // It is genuinely re-armed (not inert): the next occurrence still fires.
      await sched.tick(dueLater + 1 + 60_001);
      expect(service.calls.length).toBe(1);
   });
});

describe("MaterializationScheduler: published versions", () => {
   const t0 = 1_000_000_000_000;
   const dueLater = t0 + 60_001;
   const scheduled = (scope = "version") => ({
      name: "v",
      materialization: { scope, schedule: "* * * * *" },
   });

   function versionScheduler(
      versions: FakeVersion[],
      service: ReturnType<typeof fakeService>,
   ) {
      return new MaterializationScheduler(
         fakeStore([fakeEnv("env1", [], versions)]),
         service,
         CONFIG,
         fakeCron(),
      );
   }

   it("fires each version on its own schedule, naming it, without loading it first", async () => {
      const service = fakeService();
      const sched = versionScheduler(
         [fakeVersion("1.0.0", scheduled()), fakeVersion("2.0.0", scheduled())],
         service,
      );
      await sched.tick(t0);
      expect(service.calls).toEqual([]);
      await sched.tick(dueLater);
      expect(
         service.calls.map((c) => [c.pkg, c.opts.versionId, c.opts.trigger]),
      ).toEqual([
         ["v", "1.0.0", "SCHEDULER"],
         ["v", "2.0.0", "SCHEDULER"],
      ]);
      // Each version anchors on its own scheduled fires.
      expect(service.anchorsAsked).toEqual(["1.0.0", "2.0.0"]);
   });

   it("archiving a version stops its schedule, and unarchiving it arms it again", async () => {
      const service = fakeService();
      const v1 = fakeVersion("1.0.0", scheduled());
      const sched = versionScheduler([v1], service);
      await sched.tick(t0);

      v1.archiveStatus = "archive";
      await sched.tick(dueLater);
      expect(service.calls).toEqual([]);

      v1.archiveStatus = "unarchive";
      // Armed afresh: not due on the arming tick.
      await sched.tick(dueLater + 1);
      expect(service.calls).toEqual([]);
      await sched.tick(dueLater + 60_002);
      expect(service.calls.map((c) => c.opts.versionId)).toEqual(["1.0.0"]);
   });

   it("skips a version with no schedule, outside scope version, or bound to a host's manifest", async () => {
      const service = fakeService();
      const sched = versionScheduler(
         [
            fakeVersion("1.0.0", { name: "v" }),
            fakeVersion("2.0.0", scheduled("package")),
            fakeVersion("3.0.0", scheduled(), {
               manifestPath: "gs://bucket/m.json",
            }),
            fakeVersion("4.0.0", null),
            fakeVersion("5.0.0", {
               name: "v",
               scope: "package",
               materialization: { scope: "version", schedule: "* * * * *" },
            }),
         ],
         service,
      );
      await sched.tick(t0);
      await sched.tick(dueLater);
      expect(service.calls).toEqual([]);
   });

   it("warns once when a version's publisher.json cannot be read, and again after it was read", async () => {
      // A publish checked that file, so not reading it means the tree is
      // missing on disk. The version stays in the sweep, so the warning is
      // not repeated every tick.
      const warn = spyOn(logger, "warn").mockImplementation(() => logger);
      try {
         const service = fakeService();
         const v1 = fakeVersion("1.0.0", null);
         const sched = versionScheduler([v1], service);
         const unreadable = () =>
            (warn.mock.calls as unknown as unknown[][]).filter(([message]) =>
               String(message).includes("cannot be read"),
            );
         await sched.tick(t0);
         await sched.tick(dueLater);
         expect(service.calls).toEqual([]);
         expect(unreadable()).toHaveLength(1);
         expect(unreadable()[0][1]).toEqual({
            environmentName: "env1",
            packageName: "v",
            versionId: "1.0.0",
         });

         v1.manifest = scheduled();
         await sched.tick(dueLater + 1);
         v1.manifest = null;
         await sched.tick(dueLater + 2);
         expect(unreadable()).toHaveLength(2);
      } finally {
         warn.mockRestore();
      }
   });

   it("leaves a package with no versions to the package sweep", async () => {
      const service = fakeService();
      const sched = new MaterializationScheduler(
         fakeStore([
            fakeEnv("env1", [fakePackage("p", { schedule: "* * * * *" })], []),
         ]),
         service,
         CONFIG,
         fakeCron(),
      );
      await sched.tick(t0);
      await sched.tick(dueLater);
      expect(service.calls.map((c) => [c.pkg, c.opts.versionId])).toEqual([
         ["p", undefined],
      ]);
   });
});
