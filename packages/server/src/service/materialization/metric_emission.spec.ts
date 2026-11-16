// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import type { DuckDBConnection as BuildSession } from "@malloydata/db-duckdb";
import {
   afterAll,
   afterEach,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import * as sinon from "sinon";
import { MaterializationConflictError } from "../../errors";
import { logger } from "../../logger";
import { resetMaterializationTelemetryForTesting } from "../../materialization_metrics";
import type { MaterializationUpdate } from "../../storage/DatabaseInterface";
import { tempBuildPlanPackages } from "../../test_helpers/incremental_test_harness";
import { createMocks } from "../../test_helpers/materialization_service_harness";
import {
   fakeSource,
   makeMaterialization,
   persistTables,
} from "../../test_helpers/materialization_test_fixtures";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../../test_helpers/metrics_harness";
import type { EnvironmentStore } from "../environment_store";
import type { BuildInstruction, MaterializationStatus } from "./domain/types";
import {
   PackageMaterialization,
   type PackageMaterializationOwner,
} from "./package_materialization";
import {
   compilePackageBuildPlan,
   computePackageBuildPlan,
} from "./plan/build_plan_compile";
import { deriveBuildPlanOutcome } from "./plan/build_plan";
import {
   createTableWithDeclaredTypes,
   issuePassthroughRead,
} from "./run/materialization_build_session";
import { noteWrongTierRecompute } from "./run/materialization_run_support";
import { MaterializationScheduler } from "./run/materialization_scheduler";
import {
   MaterializationService,
   type RunBuildOptions,
} from "./run/materialization_service";

/**
 * Each materialization instrument below, by name, emitted from the code that
 * emits it in production, with today's counts. A change to a name, a label or
 * how often a call site counts shows up here.
 */

let harness: MetricsHarness;
let quiet: sinon.SinonStub[] = [];
beforeEach(async () => {
   harness = await startMetricsHarness();
   resetMaterializationTelemetryForTesting();
   quiet = [
      sinon.stub(logger, "warn"),
      sinon.stub(logger, "info"),
      sinon.stub(logger, "error"),
   ];
});
afterEach(async () => {
   for (const stub of quiet) stub.restore();
   resetMaterializationTelemetryForTesting();
   await harness.shutdown();
});

const packages = tempBuildPlanPackages();
afterAll(() => packages.cleanup());

/**
 * How many times `publisher_materialization_eligibility_refused_total` counts
 * ONE refused source while a package's build plan is derived: once, at the
 * plan's one verdict for its table.
 */
const ELIGIBILITY_REFUSALS_PER_REFUSED_SOURCE = 1;

const NOW_SOURCE = `##! experimental.persistence
#@ persist
source: recent is duckdb.sql("select 1 as id, now() as at") -> { where: at > now() - 1 day; select: id }
`;

const BLANK_STORAGE_SOURCE = `##! experimental.persistence
source: orders is duckdb.sql("select 1 as id, 'a' as category")

#@ persist storage=""
source: by_category is orders -> { group_by: category; aggregate: n is count() }
`;

const ONE_PERSIST_SOURCE = `##! experimental.persistence
source: orders is duckdb.sql("select 1 as id, 'a' as category")

#@ persist name="by_category"
source: by_category is orders -> { group_by: category; aggregate: n is count() }
`;

describe("publisher_materialization_eligibility_refused_total", () => {
   it("counts one refused source once per plan", async () => {
      const pkg = await packages.create(NOW_SOURCE);
      const outcome = await computePackageBuildPlan(pkg);
      expect(
         Object.keys(outcome.sourceEligibility?.refused ?? {}),
      ).toHaveLength(1);
      expect(
         await harness.collectAttributes(
            "publisher_materialization_eligibility_refused_total",
         ),
      ).toEqual([{ reason: "current_time" }]);
      expect(
         await harness.collectCounter(
            "publisher_materialization_eligibility_refused_total",
         ),
      ).toBe(ELIGIBILITY_REFUSALS_PER_REFUSED_SOURCE);
   }, 30_000);

   it("counts a source under its wire refusal, when the gates disagree", async () => {
      // A blank `storage=` is refused for its placement on the wire, and is
      // colocated to every other gate.
      const pkg = await packages.create(BLANK_STORAGE_SOURCE);
      const outcome = await computePackageBuildPlan(pkg);
      const refused = Object.values(outcome.plan?.refusedSources ?? {});
      expect(refused.map((r) => r.reason)).toEqual(["empty_storage"]);
      expect(
         await harness.collectAttributes(
            "publisher_materialization_eligibility_refused_total",
         ),
      ).toEqual([{ reason: "empty_storage" }]);
      expect(
         await harness.collectCounter(
            "publisher_materialization_eligibility_refused_total",
         ),
      ).toBe(1);
   }, 30_000);

   it("counts no source the plan does not refuse", async () => {
      // Colocated and buildable on the package's DuckDB sandbox, which a
      // `storage=` build cannot pass through: the storage gate's verdict
      // (only whether it may be served from a stored table) is not a refusal.
      const pkg = await packages.create(ONE_PERSIST_SOURCE);
      const compiled = await compilePackageBuildPlan(pkg);
      const outcome = deriveBuildPlanOutcome(
         { ...compiled, connectionTypes: { duckdb: "duckdb" } },
         null,
      );
      expect(outcome.plan?.refusedSources ?? {}).toEqual({});
      expect(Object.values(outcome.sourceEligibility.refused)).toEqual([
         expect.stringContaining("cannot be passed through"),
      ]);
      expect(
         await harness.collectCounter(
            "publisher_materialization_eligibility_refused_total",
         ),
      ).toBe(0);
   }, 30_000);
});

describe("a run's instruments", () => {
   /** A service whose build loop and commit are stubbed, as a run sees them. */
   function stubbedRun(ctx: ReturnType<typeof createMocks>) {
      const service = new MaterializationService(ctx.environmentStore, {
         executeInstructedBuild: sinon.stub().resolves({
            entries: {},
            failures: {},
            sourcesRefused: 0,
         }),
         commitManifest: sinon.stub().resolves(),
         autoLoadManifest: sinon.stub().resolves(),
      });
      return (opts: Partial<RunBuildOptions> = {}) =>
         service.collaborators.runBuild(
            "mat-1",
            "my-env",
            "pkg",
            {
               sourceNames: undefined,
               forceRefresh: true,
               buildInstructions: undefined,
               ...opts,
            } as RunBuildOptions,
            new AbortController().signal,
         );
   }

   it("publisher_materialization_run_duration_ms: one sample per run, by mode and outcome", async () => {
      const ctx = createMocks();
      await stubbedRun(ctx)();
      expect(
         await harness.collectCounter("publisher_materialization_runs_total", {
            mode: "auto",
            outcome: "success",
         }),
      ).toBe(1);
      const duration = await harness.collectHistogram(
         "publisher_materialization_run_duration_ms",
      );
      expect(duration.count).toBe(1);
      expect(
         await harness.collectAttributes(
            "publisher_materialization_run_duration_ms",
         ),
      ).toEqual([{ mode: "auto", outcome: "success" }]);
   });

   it("publisher_materialization_auto_load_total: once per commit, by the load's outcome", async () => {
      const ctx = createMocks();
      let status: MaterializationStatus = "PENDING";
      ctx.repository.getMaterializationById.callsFake(async () =>
         makeMaterialization({ status, metadata: { mode: "auto" } }),
      );
      ctx.repository.updateMaterialization.callsFake(
         async (_id: string, update: MaterializationUpdate) => {
            if (update.status) status = update.status;
            return makeMaterialization({ status });
         },
      );
      const commit = (
         ctx.service.collaborators as unknown as {
            commitManifest: (
               id: string,
               entries: Record<string, unknown>,
               failures: Record<string, unknown>,
               metadata: Record<string, unknown>,
               serve: (
                  finish: (served: {
                     metadata: Record<string, unknown>;
                  }) => Promise<void>,
               ) => Promise<void>,
            ) => Promise<void>;
         }
      ).commitManifest;
      const entries = {
         ok: { sourceEntityId: "ok", sourceName: "s", physicalTableName: "t" },
      };

      await commit("mat-1", entries, {}, { mode: "auto" }, async (finish) => {
         await finish({ metadata: { autoLoad: "loaded" } });
         // A second finish is the same settle, not another count.
         await finish({ metadata: { autoLoad: "loaded" } });
      });
      status = "PENDING";
      await commit("mat-2", entries, {}, { mode: "auto" }, async () => {
         throw new Error("the lock broke");
      });

      expect(
         await harness.collectCounter(
            "publisher_materialization_auto_load_total",
            { outcome: "success" },
         ),
      ).toBe(1);
      expect(
         await harness.collectCounter(
            "publisher_materialization_auto_load_total",
            { outcome: "failure" },
         ),
      ).toBe(1);
   });
});

describe("the build loop's instruments", () => {
   function execute(
      compiled: unknown,
      instructions: BuildInstruction[],
      seed: Record<string, unknown> = {},
   ): Promise<unknown> {
      const ctx = createMocks();
      const environment = {
         getApiConnection: () => {
            throw new Error("no connection config in this test");
         },
         getEnvironmentPath: () => "/tmp/env",
      };
      return (
         ctx.service.collaborators as unknown as {
            executeInstructedBuild: (...args: unknown[]) => Promise<unknown>;
         }
      ).executeInstructedBuild({
         compiled,
         environment,
         instructions,
         seedEntries: seed,
         signal: new AbortController().signal,
         strict: false,
         priorRefusals: [],
         hostInstructed: false,
      });
   }

   const instruction = (
      sourceEntityId: string,
      physicalTableName: string,
   ): BuildInstruction => ({
      sourceEntityId,
      materializedTableId: `mt-${physicalTableName}`,
      physicalTableName,
      realization: "COPY",
   });

   it("publisher_materialization_duplicate_target_skipped_total: once per extra route to a table built this run", async () => {
      const connection = {
         runSQL: sinon.stub().resolves(),
      } as unknown as MalloyConnection;
      // A base and its extension: one address, so one table.
      const base = fakeSource({ name: "base", sourceEntityId: "bshared1" });
      const ext = fakeSource({ name: "ext", sourceEntityId: "bshared1" });
      await execute(
         {
            builds: [
               {
                  connectionName: "duckdb",
                  order: ["base", "ext"],
                  dependsOn: {},
               },
            ],
            tables: persistTables({ base, ext }),
            connectionDigests: { duckdb: "dig" },
            connections: new Map([["duckdb", connection]]),
         },
         [instruction("bshared1", "shared_v1")],
      );
      expect(
         await harness.collectCounter(
            "publisher_materialization_duplicate_target_skipped_total",
         ),
      ).toBe(1);
   });

   it("publisher_materialization_manifest_bind_degraded_total: a seed naming a connection the build lacks", async () => {
      const connection = {
         runSQL: sinon.stub().resolves(),
         dialectName: "snowflake",
      } as unknown as MalloyConnection;
      const down = fakeSource({
         name: "down",
         sourceEntityId: "bdown1",
         dialectName: "snowflake",
      });
      await execute(
         {
            builds: [
               { connectionName: "duckdb", order: ["down"], dependsOn: {} },
            ],
            tables: persistTables({ down }),
            connectionDigests: { duckdb: "dig" },
            connections: new Map([["duckdb", connection]]),
         },
         [instruction("bdown1", "down_v1")],
         {
            // Named but absent: degraded.
            up: {
               sourceEntityId: "up",
               physicalTableName: "orders_base",
               connectionName: "elsewhere",
            },
            // No name at all: the benign older-caller default, not counted.
            old: { sourceEntityId: "old", physicalTableName: "older_base" },
         },
      );
      expect(
         await harness.collectCounter(
            "publisher_materialization_manifest_bind_degraded_total",
         ),
      ).toBe(1);
   });

   it("publisher_materialization_stale_upstream_entry_total: once per run per upstream, by the recomputing build's tier", async () => {
      const planFacts = {};
      const note = (upstream: string, build: "colocated" | "storage") =>
         noteWrongTierRecompute({
            planFacts,
            build,
            sourceName: "down",
            upstream,
            detail: "moved",
         });
      note("up_a", "storage");
      note("up_a", "storage");
      note("up_b", "colocated");
      // Another run (its own plan facts) counts the same upstream again.
      noteWrongTierRecompute({
         planFacts: {},
         build: "storage",
         sourceName: "down",
         upstream: "up_a",
         detail: "moved",
      });
      expect(
         await harness.collectCounter(
            "publisher_materialization_stale_upstream_entry_total",
            { build: "storage" },
         ),
      ).toBe(2);
      expect(
         await harness.collectCounter(
            "publisher_materialization_stale_upstream_entry_total",
            { build: "colocated" },
         ),
      ).toBe(1);
   });
});

describe("a storage build's instruments", () => {
   it("publisher_storage_build_attribution_skipped_total: a labelled BigQuery read whose jobs cannot be listed", async () => {
      const runSQL = async (sql: string) => {
         if (sql.includes("maxResults := 1")) {
            throw new Error("Access Denied: bigquery.jobs.list");
         }
         return { rows: [] };
      };
      const read = await issuePassthroughRead(
         { runSQL } as unknown as BuildSession,
         "bigquery",
         "proj",
         "SELECT 1",
         { team: "growth" },
      );
      // The read still runs, unlabelled.
      expect(read.jobId).toBeNull();
      expect(
         await harness.collectAttributes(
            "publisher_storage_build_attribution_skipped_total",
         ),
      ).toEqual([{ reason: "job_listing_unavailable" }]);
      expect(
         await harness.collectCounter(
            "publisher_storage_build_attribution_skipped_total",
         ),
      ).toBe(1);
   });

   it("publisher_storage_column_type_mismatch_total: a declared-type cast that refuses a value", async () => {
      const conn = new DuckDBConnection("metric_emission_cast", ":memory:");
      try {
         await createTableWithDeclaredTypes(
            conn,
            "t",
            `SELECT 'open' AS flag`,
            {
               columns: [{ name: "flag", type: "boolean" }],
            },
         );
      } finally {
         await conn.close();
      }
      expect(
         await harness.collectAttributes(
            "publisher_storage_column_type_mismatch_total",
         ),
      ).toEqual([{ stage: "build", declared: "cast_failed" }]);
   });
});

describe("a package load's build-plan instruments", () => {
   async function materialization(files: string) {
      const pkg = await packages.create(files);
      const owner = {
         packageName: () => "pkg",
         versionId: () => undefined,
         models: () => new Map(),
         buildPlanPackage: pkg,
      } as unknown as PackageMaterializationOwner;
      return { pm: new PackageMaterialization(owner), pkg };
   }

   it("publisher_materialization_build_plan_compute_duration_ms: one sample per load, by where it was computed", async () => {
      const { pm, pkg } = await materialization(ONE_PERSIST_SOURCE);
      expect(await pm.applyLoadBuildPlan(undefined)).toBeNumber();
      const outcome = await computePackageBuildPlan(pkg);
      await pm.applyLoadBuildPlan({
         ok: true,
         outcome,
         durationMs: 42,
         digestSkipped: [],
         eligibilityRefused: {},
      });
      expect(
         (
            await harness.collectHistogram(
               "publisher_materialization_build_plan_compute_duration_ms",
               { computed_in: "main_thread" },
            )
         ).count,
      ).toBe(1);
      expect(
         await harness.collectHistogram(
            "publisher_materialization_build_plan_compute_duration_ms",
            { computed_in: "worker" },
         ),
      ).toMatchObject({ count: 1, sum: 42 });
   }, 30_000);

   it("publisher_materialization_build_plan_compute_failed_total: a plan the worker failed, its refusals still counted", async () => {
      const { pm } = await materialization(ONE_PERSIST_SOURCE);
      expect(
         await pm.applyLoadBuildPlan({
            ok: false,
            error: "compile failed",
            durationMs: 5,
            digestSkipped: [],
            eligibilityRefused: { current_time: 2 },
         }),
      ).toBeUndefined();
      expect(
         await harness.collectCounter(
            "publisher_materialization_build_plan_compute_failed_total",
         ),
      ).toBe(1);
      // The worker's refusals are re-counted on the main thread, as handed back.
      expect(
         await harness.collectCounter(
            "publisher_materialization_eligibility_refused_total",
            { reason: "current_time" },
         ),
      ).toBe(2);
      expect(
         (
            await harness.collectHistogram(
               "publisher_materialization_build_plan_compute_duration_ms",
            )
         ).count,
      ).toBe(0);
   }, 30_000);
});

describe("publisher_materialization_scheduled_fires_total", () => {
   const t0 = 1_000_000_000_000;

   function scheduler(createMaterialization: () => Promise<unknown>) {
      const pkg = {
         getPackageName: () => "p",
         getPackageMetadata: () => ({
            materialization: { schedule: "* * * * *" },
            manifestLocation: null,
         }),
         persistencePolicyWarnings: () => [],
      };
      const env = {
         getEnvironmentName: () => "env1",
         getLoadedPackages: () => [pkg],
         peekVersion: () => undefined,
         getVersionService: () => null,
      };
      const store = {
         getLoadedEnvironments: () => [env],
      } as unknown as EnvironmentStore;
      const service = {
         createMaterialization,
         getLatestScheduledFireAt: async () => null,
      } as unknown as MaterializationService;
      const cron = {
         isValid: () => true,
         nextAfter: (_e: string, from: Date) =>
            new Date(from.getTime() + 60_000),
      };
      return new MaterializationScheduler(
         store,
         service,
         { tickIntervalMs: 60_000, maxFiresPerTick: 10 },
         cron as never,
      );
   }

   it("counts each fire by its outcome", async () => {
      for (const create of [
         async () => ({}),
         async () => {
            throw new MaterializationConflictError("busy");
         },
         async () => {
            throw new Error("boom");
         },
      ]) {
         const s = scheduler(create);
         await s.tick(t0); // arm
         await s.tick(t0 + 60_001); // due
      }
      for (const outcome of ["fired", "conflict", "error"]) {
         expect(
            await harness.collectCounter(
               "publisher_materialization_scheduled_fires_total",
               { outcome },
            ),
         ).toBe(1);
      }
   });
});
