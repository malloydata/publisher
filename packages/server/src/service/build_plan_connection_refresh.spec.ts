// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   afterEach,
   beforeEach,
   describe,
   expect,
   it,
   mock,
   spyOn,
} from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../storage/duckdb/DuckDBRepository";
import type { ResourceRepository } from "../storage/DatabaseInterface";
import { initializeSchema } from "../storage/duckdb/schema";
import { changedConnectionNames, Environment } from "./environment";
import { Package } from "./package";
import type { EnvironmentStore } from "./environment_store";
import { MaterializationService } from "./materialization/run/materialization_service";
import {
   BadRequestError,
   BuildPlanRefreshingError,
   BuildPlanUnavailableError,
} from "../errors";
import { compilePackageBuildPlan } from "./materialization/plan/build_plan_compile";
import { computeSourceEntityId } from "./materialization/plan/build_plan";
import { installSettings } from "../test_helpers/settings_env";
import { writePackageFiles } from "../test_helpers/temp_package";

// Build plans of real packages loaded through a real Environment, around an
// edit of the connection their persisted source reads.

const ENV_ID = "env-1";
const PKG = "sales";
const MODEL = `##! experimental.persistence
#@ persist name="daily"
source: daily is duck.sql("SELECT 1 AS id, 2 AS amount") -> { select: id, amount }
`;
/** An environment DuckDB connection; its setupSQL is part of its digest. */
function duck(setupSQL: string) {
   return { name: "duck", type: "duckdb", duckdbConnection: { setupSQL } };
}

let root: string;
let envPath: string;
let sourcesPath: string;
let db: DuckDBConnection;
let repo: ResourceRepository;
let restoreSettings: () => void;

/** A package tree with one persisted source over the `duck` connection. */
function writePackage(
   dir: string,
   manifest: Record<string, unknown>,
   model: string = MODEL,
): string {
   writePackageFiles(
      dir,
      {
         "model.malloy": model,
      },
      { manifest: { name: PKG, ...manifest } },
   );
   return dir;
}

function source(version: string, model: string = MODEL): string {
   return writePackage(
      path.join(sourcesPath, version),
      {
         version,
         description: `release ${version}`,
      },
      model,
   );
}

async function newEnvironment(
   extraConnections: unknown[] = [],
): Promise<Environment> {
   const env = await Environment.create("testEnv", envPath, [
      duck("SELECT 1") as never,
      ...(extraConnections as never[]),
   ]);
   env.bindVersions(repo, ENV_ID, {
      downloaderFor: (_packageName, location) => async (stagingPath) => {
         await fs.promises.cp(location, stagingPath, { recursive: true });
      },
      ensurePackageRecord: async (packageName, description) => {
         if (await repo.getPackageByName(ENV_ID, packageName)) return false;
         await repo.createPackage({
            environmentId: ENV_ID,
            name: packageName,
            description,
            manifestPath: "",
         });
         return true;
      },
      removePackageRecord: async (packageName) => {
         const row = await repo.getPackageByName(ENV_ID, packageName);
         if (row) await repo.deletePackage(row.id);
      },
   });
   return env;
}

async function publish(
   env: Environment,
   location: string,
   promotion: "on-publish" | "explicit" = "on-publish",
) {
   return env.getVersionService()!.publish(
      PKG,
      async (stagingPath) => {
         await fs.promises.cp(location, stagingPath, { recursive: true });
      },
      { sourceLocation: location, promotion },
   );
}

beforeEach(async () => {
   // The edit under test changes a DuckDB connection's setupSQL.
   restoreSettings = installSettings({
      env: { PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true" },
   }).restore;
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "environment-versions-")),
   );
   envPath = path.join(root, "env");
   sourcesPath = path.join(root, "sources");
   fs.mkdirSync(envPath);
   db = new DuckDBConnection(":memory:");
   await db.initialize();
   await initializeSchema(db);
   await db.run(
      "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, 'testEnv', ?, now(), now())",
      [ENV_ID, envPath],
   );
   repo = createDuckDBRepository(db);
});

afterEach(async () => {
   mock.restore();
   restoreSettings();
   await db.close();
   fs.rmSync(root, { recursive: true, force: true });
});

/** The addresses the loaded package hands out (GET build-plan, validation). */
function planned(pkg: Package): string[] {
   return Object.values(pkg.getBuildPlan()?.sources ?? {}).map(
      (s) => s.sourceEntityId,
   );
}

/** The addresses a run computes: a fresh compile with the connections as they are now. */
async function compiled(pkg: Package): Promise<string[]> {
   const plan = await compilePackageBuildPlan(pkg);
   return Object.values(plan.sources).map((s) =>
      computeSourceEntityId(
         s,
         plan.connectionDigests,
         plan.targets?.[s.sourceID],
      ),
   );
}

async function editConnection(
   env: Environment,
   setupSQL: string,
   extra: unknown[] = [],
) {
   await env.update({
      name: "testEnv",
      connections: [duck(setupSQL), ...extra],
   } as never);
}

/** A MaterializationService over `environment`, as main_server.ts wires it. */
function serviceOver(environment: Environment): MaterializationService {
   return new MaterializationService({
      storageManager: { getRepository: () => repo },
      getEnvironment: async () => environment,
      getLoadedEnvironments: () => [environment],
   } as unknown as EnvironmentStore);
}

/** One instructed table for the package's persisted source at `address`. */
function instruction(address: string) {
   return {
      sourceEntityId: address,
      materializedTableId: "mt-1",
      physicalTableName: "daily_g1",
      realization: "COPY" as const,
   };
}

// ms2data/service#6145. A content address folds in the connection's digest,
// which reads connection config, so a connection edit moves it. The plan
// computed at load used to keep the old address: a host echoed ledger entries
// recorded under it, create-time validation passed against that same stale
// plan, and the run's own compile refused them as "measured under a different
// definition", failing every refresh until the package reloaded.
describe("a connection edit re-derives the loaded build plans", () => {
   it("a published version hands out the address a run computes", async () => {
      const env = await newEnvironment();
      const v1 = (await publish(env, source("1.0.0"))).loaded;
      const before = planned(v1);
      expect(before).toHaveLength(1);
      expect(await compiled(v1)).toEqual(before);

      await editConnection(env, "SELECT 2");
      await v1.whenBuildPlanCurrent();

      const after = await compiled(v1);
      expect(after).not.toEqual(before);
      expect(planned(v1)).toEqual(after);
   });

   it("every loaded version follows, and identical content keeps one address across versions", async () => {
      const env = await newEnvironment();
      const v1 = (await publish(env, source("1.0.0"))).loaded;
      const v2 = (await publish(env, source("2.0.0"))).loaded;
      expect(planned(v2)).toEqual(planned(v1));

      await editConnection(env, "SELECT 3");
      await Promise.all([v1.whenBuildPlanCurrent(), v2.whenBuildPlanCurrent()]);

      expect(planned(v1)).toEqual(await compiled(v1));
      expect(planned(v2)).toEqual(planned(v1));
   });

   it("an edit that changes nothing in the digest keeps the address", async () => {
      const env = await newEnvironment();
      const v1 = (await publish(env, source("1.0.0"))).loaded;
      const before = planned(v1);

      await editConnection(env, "SELECT 1");
      await v1.whenBuildPlanCurrent();

      expect(planned(v1)).toEqual(before);
   });
});

// The review of #1620: a failed re-derivation, the cost of an edit, the wait,
// and loads an edit overtakes.
describe("re-deriving after a connection edit", () => {
   it("a failed re-derivation leaves no pre-edit plan: run creation refuses instead of validating against it", async () => {
      const env = await newEnvironment();
      const v1 = (await publish(env, source("1.0.0"))).loaded;
      const before = planned(v1);
      expect(before).toHaveLength(1);

      // The re-derivation fails (as a model that no longer compiles would).
      spyOn(Package.prototype, "refreshBuildPlan").mockImplementationOnce(
         function (this: Package) {
            return (
               this as unknown as {
                  materialization: {
                     refreshBuildPlan(
                        d: () => Promise<unknown>,
                     ): Promise<boolean>;
                  };
               }
            ).materialization.refreshBuildPlan(async () => {
               throw new Error("does not compile");
            });
         },
      );
      await editConnection(env, "SELECT 9");
      await v1.whenBuildPlanCurrent();

      expect(v1.isBuildPlanUnavailable()).toBe(true);
      expect(v1.getBuildPlan()).toBeNull();
      const err = await serviceOver(env)
         .createMaterialization("testEnv", PKG, {
            buildInstructions: [instruction(before[0])],
         })
         .catch((e: unknown) => e);
      expect(err).toBeInstanceOf(BuildPlanUnavailableError);
   });

   it("run creation waits for the re-derivation and validates against the new plan, end to end", async () => {
      const env = await newEnvironment();
      const v1 = (await publish(env, source("1.0.0"))).loaded;
      const [before] = planned(v1);

      await editConnection(env, "SELECT 4");
      // Not awaited: run creation must wait for the re-derivation itself.
      const err = await serviceOver(env)
         .createMaterialization("testEnv", PKG, {
            buildInstructions: [instruction(before)],
         })
         .catch((e: unknown) => e);
      // The pre-edit address is refused at validation, never let through to
      // fail later as "measured under a different definition".
      expect(err).toBeInstanceOf(BadRequestError);
      expect(String((err as Error).message)).toContain(before);
      expect(planned(v1)).toEqual(await compiled(v1));
   });

   it("an edit to a connection no persisted source uses re-derives nothing", async () => {
      const env = await newEnvironment();
      const v1 = (await publish(env, source("1.0.0"))).loaded;
      const refreshes = spyOn(Package.prototype, "refreshBuildPlan");

      await editConnection(env, "SELECT 1", [
         {
            name: "other",
            type: "duckdb",
            duckdbConnection: { setupSQL: "SELECT 1" },
         },
      ]);

      expect(v1.isBuildPlanStale()).toBe(false);
      expect(refreshes).not.toHaveBeenCalled();
      refreshes.mockRestore();
   });

   it("re-derives at most two packages at once, and a burst of edits costs one more each", async () => {
      const env = await newEnvironment();
      const loaded: Package[] = [];
      for (const v of ["1.0.0", "2.0.0", "3.0.0", "4.0.0"]) {
         loaded.push((await publish(env, source(v))).loaded);
      }
      let running = 0;
      let peak = 0;
      let calls = 0;
      const original = Package.prototype.refreshBuildPlan;
      const spy = spyOn(
         Package.prototype,
         "refreshBuildPlan",
      ).mockImplementation(async function (this: Package) {
         calls++;
         running++;
         peak = Math.max(peak, running);
         try {
            return await original.call(this);
         } finally {
            running--;
         }
      });
      await editConnection(env, "SELECT 5");
      await editConnection(env, "SELECT 6");
      await Promise.all(loaded.map((p) => p.whenBuildPlanCurrent()));
      spy.mockRestore();

      expect(peak).toBeLessThanOrEqual(2);
      // Each package once for the first edit, at most once more for the second.
      expect(calls).toBeLessThanOrEqual(loaded.length * 2);
      for (const p of loaded) expect(planned(p)).toEqual(await compiled(p));
   }, 60_000);

   it("a version published while an edit lands derives under the new connection", async () => {
      const env = await newEnvironment();
      await publish(env, source("1.0.0"));
      const publishing = publish(env, source("2.0.0"));
      await editConnection(env, "SELECT 7");
      const v2 = (await publishing).loaded;
      await v2.whenBuildPlanCurrent();
      expect(planned(v2)).toEqual(await compiled(v2));
   });

   it("past the wait's cap, a reader gets BuildPlanRefreshingError rather than the pre-edit plan", async () => {
      const env = await newEnvironment();
      const v1 = (await publish(env, source("1.0.0"))).loaded;
      v1.markBuildPlanStale();
      const err = await v1.whenBuildPlanCurrent(5).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(BuildPlanRefreshingError);
      expect(await v1.refreshBuildPlan()).toBe(true);
      await v1.whenBuildPlanCurrent(5);
   });
});

describe("a failed re-derivation", () => {
   /** Fail the next re-derivation, as a load-worker timeout would. */
   function failNextRefresh() {
      spyOn(Package.prototype, "refreshBuildPlan").mockImplementationOnce(
         function (this: Package) {
            return (
               this as unknown as {
                  materialization: {
                     refreshBuildPlan(
                        d: () => Promise<unknown>,
                     ): Promise<boolean>;
                  };
               }
            ).materialization.refreshBuildPlan(async () => {
               throw new Error("load worker timed out");
            });
         },
      );
   }

   type Internals = {
      materialization: {
         sourceEligibility: unknown;
         colocatedSourceEligibility: unknown;
         servingPlanWhileUnavailable: unknown;
         buildConnectionNames: ReadonlySet<string>;
      };
   };

   function colocated(pkg: Package): {
      eligibleEntityIds: Set<string>;
      refused: Record<string, string>;
   } {
      return (
         pkg as unknown as {
            materialization: {
               colocatedSourceEligibility: {
                  eligibleEntityIds: Set<string>;
                  refused: Record<string, string>;
               };
            };
         }
      ).materialization.colocatedSourceEligibility;
   }

   it("keeps what serving reads, so bindings and a host's rebind keep serving while run creation is refused", async () => {
      const env = await newEnvironment();
      const v1 = (await publish(env, source("1.0.0"))).loaded;
      const internals = (v1 as unknown as Internals).materialization;
      const planBefore = v1.getBuildPlan();
      const storageBefore = internals.sourceEligibility;
      const [address] = planned(v1);
      expect(storageBefore).toBeDefined();
      expect(colocated(v1).eligibleEntityIds.has(address)).toBe(true);

      failNextRefresh();
      await editConnection(env, "SELECT 9");
      await v1.whenBuildPlanCurrent();

      expect(v1.isBuildPlanUnavailable()).toBe(true);
      // No route or run sees the pre-edit plan...
      expect(v1.getBuildPlan()).toBeNull();
      // ...but binding still matches entries as it did before the edit.
      expect(internals.sourceEligibility).toBe(storageBefore);
      expect(internals.servingPlanWhileUnavailable).toBe(planBefore);
      // Except a colocated table on the connection the edit changed: it was
      // built through that connection as it was, and would be read through it
      // as it is now, so it serves live until the plan is re-derived.
      expect(colocated(v1).eligibleEntityIds.has(address)).toBe(false);
      expect(colocated(v1).refused[address]).toContain("connection changed");
   });

   it("keeps colocated eligibility for sources on connections the edit did not change", async () => {
      const duck2 = {
         name: "duck2",
         type: "duckdb",
         duckdbConnection: { setupSQL: "SELECT 2" },
      };
      const env = await newEnvironment([duck2]);
      const v1 = (
         await publish(
            env,
            source(
               "1.0.0",
               `##! experimental.persistence
#@ persist name="daily"
source: daily is duck.sql("SELECT 1 AS id, 2 AS amount") -> { select: id, amount }
#@ persist name="weekly"
source: weekly is duck2.sql("SELECT 3 AS id, 4 AS amount") -> { select: id, amount }
`,
            ),
         )
      ).loaded;
      const addressOf = (name: string) =>
         Object.values(v1.getBuildPlan()?.sources ?? {}).find(
            (s) => s.name === name,
         )!.sourceEntityId;
      const daily = addressOf("daily");
      const weekly = addressOf("weekly");
      expect(colocated(v1).eligibleEntityIds.has(weekly)).toBe(true);

      failNextRefresh();
      await editConnection(env, "SELECT 9", [duck2]);
      await v1.whenBuildPlanCurrent();

      expect(v1.isBuildPlanUnavailable()).toBe(true);
      expect(colocated(v1).eligibleEntityIds.has(daily)).toBe(false);
      expect(colocated(v1).eligibleEntityIds.has(weekly)).toBe(true);
   });

   it("is not retried when the plan compute itself failed, only when the load pool did", async () => {
      const env = await newEnvironment();
      const v1 = (await publish(env, source("1.0.0"))).loaded;
      // Counted per package: the spy is on the prototype, and other tests'
      // environments may still have retries pending.
      const original = Package.prototype.refreshBuildPlan;
      let calls = 0;
      const refreshes = spyOn(
         Package.prototype,
         "refreshBuildPlan",
      ).mockImplementation(function (this: Package, ...args) {
         if (this !== v1) return original.apply(this, args);
         calls++;
         if (calls > 1) return original.apply(this, args);
         return (
            this as unknown as {
               materialization: {
                  refreshBuildPlan(d: () => Promise<unknown>): Promise<boolean>;
               };
            }
         ).materialization.refreshBuildPlan(async () => ({
            ok: false,
            error: "Malloy compile failed",
            durationMs: 0,
            digestSkipped: [],
            eligibilityRefused: {},
         }));
      });

      await editConnection(env, "SELECT 6");
      await v1.whenBuildPlanCurrent();
      expect(v1.isBuildPlanUnavailable()).toBe(true);
      expect(v1.lastBuildPlanRefreshFailedTransiently()).toBe(false);

      // Past the first backoff (2s): no retry recompiles it.
      await new Promise((resolve) => setTimeout(resolve, 2_500));
      expect(calls).toBe(1);
      expect(v1.isBuildPlanUnavailable()).toBe(true);
      expect(
         (env as unknown as { planRefreshBackoff: Map<unknown, unknown> })
            .planRefreshBackoff.size,
      ).toBe(0);
      refreshes.mockRestore();
   }, 15_000);

   it("stops retrying a package this environment no longer serves", async () => {
      const env = await newEnvironment();
      const gone = {
         isBuildPlanUnavailable: () => true,
         getPackageName: () => "gone",
         getVersionId: () => undefined,
      };
      const internals = env as unknown as {
         schedulePlanRetry(p: unknown): void;
         planRefreshBackoff: Map<unknown, unknown>;
         planRefreshQueue: Set<unknown>;
      };
      internals.schedulePlanRetry(gone);
      expect(internals.planRefreshBackoff.size).toBe(1);
      await new Promise((resolve) => setTimeout(resolve, 2_500));
      expect(internals.planRefreshBackoff.size).toBe(0);
      expect(internals.planRefreshQueue.size).toBe(0);
   }, 15_000);

   it("is retried with backoff, and a successful retry clears the 409", async () => {
      const env = await newEnvironment();
      const v1 = (await publish(env, source("1.0.0"))).loaded;

      failNextRefresh();
      await editConnection(env, "SELECT 8");
      await v1.whenBuildPlanCurrent();
      expect(v1.isBuildPlanUnavailable()).toBe(true);

      // The first retry waits 2s (backoffAfter(1)); no edit or reload needed.
      const deadline = Date.now() + 15_000;
      while (v1.isBuildPlanUnavailable() && Date.now() < deadline) {
         await new Promise((resolve) => setTimeout(resolve, 100));
      }
      expect(v1.isBuildPlanUnavailable()).toBe(false);
      expect(planned(v1)).toEqual(await compiled(v1));
      expect(
         (v1 as unknown as Internals).materialization
            .servingPlanWhileUnavailable,
      ).toBeNull();
   }, 30_000);

   it("re-derives when a connection a skipped source needed is created", async () => {
      const env = await newEnvironment();
      const v1 = (await publish(env, source("1.0.0"))).loaded;
      // As a source whose connection did not resolve leaves it: named in the
      // plan's build connections, absent from its sources.
      (v1 as unknown as Internals).materialization.buildConnectionNames =
         new Set(["duck", "late"]);
      const refreshes = spyOn(Package.prototype, "refreshBuildPlan");

      await editConnection(env, "SELECT 1", [
         {
            name: "late",
            type: "duckdb",
            duckdbConnection: { setupSQL: "SELECT 1" },
         },
      ]);
      await v1.whenBuildPlanCurrent();

      expect(refreshes).toHaveBeenCalled();
      refreshes.mockRestore();
   });
});

describe("changedConnectionNames", () => {
   it("names added, removed and reconfigured connections, and nothing else", () => {
      const a = { name: "a", type: "duckdb", duckdbConnection: {} };
      const b = { name: "b", type: "duckdb", duckdbConnection: {} };
      const b2 = { ...b, duckdbConnection: { setupSQL: "SELECT 1" } };
      const c = { name: "c", type: "duckdb", duckdbConnection: {} };
      expect(
         [
            ...changedConnectionNames([a, b] as never, [a, b2, c] as never),
         ].sort(),
      ).toEqual(["b", "c"]);
      expect([
         ...changedConnectionNames([a, b] as never, [a] as never),
      ]).toEqual(["b"]);
      expect(changedConnectionNames([a] as never, [a] as never).size).toBe(0);
   });

   it("compares fingerprinted connections by fingerprint, so a credential rotation changes nothing", () => {
      const pg = {
         name: "wh",
         type: "postgres",
         fingerprint: "fp-1",
         postgresConnection: { host: "h", password: "old" },
      };
      const rotated = {
         ...pg,
         postgresConnection: { host: "h", password: "new" },
      };
      expect(
         changedConnectionNames([pg] as never, [rotated] as never).size,
      ).toBe(0);
      // A new fingerprint is a change (the digest is the fingerprint).
      expect([
         ...changedConnectionNames(
            [pg] as never,
            [{ ...rotated, fingerprint: "fp-2" }] as never,
         ),
      ]).toEqual(["wh"]);
      // Without a fingerprint the whole config is compared, secrets included.
      const bare = { ...pg, fingerprint: undefined };
      const bareRotated = { ...rotated, fingerprint: undefined };
      expect([
         ...changedConnectionNames([bare] as never, [bareRotated] as never),
      ]).toEqual(["wh"]);
   });
});
