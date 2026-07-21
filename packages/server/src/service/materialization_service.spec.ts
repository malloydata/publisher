import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { Manifest } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import {
   BadRequestError,
   EnvironmentNotFoundError,
   InvalidStateTransitionError,
   MaterializationConflictError,
   MaterializationNotFoundError,
} from "../errors";
import {
   BuildInstruction,
   MaterializationStatus,
   ResourceRepository,
} from "../storage/DatabaseInterface";
import { DuplicateActiveMaterializationError } from "../storage/duckdb/MaterializationRepository";
import { EnvironmentStore } from "./environment_store";
import {
   compiledWith,
   fakeSource,
   makeBuildPlan,
   makeInstruction,
   makeMaterialization,
} from "./materialization_test_fixtures";
import {
   manifestExcludingStorage,
   MaterializationService,
   redactConnectionSecrets,
   stagingSuffix,
   storagePhysicalTableName,
} from "./materialization_service";
import { resetMaterializationTelemetryForTesting } from "../materialization_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../test_helpers/metrics_harness";

type MockRepo = sinon.SinonStubbedInstance<ResourceRepository>;

describe("redactConnectionSecrets", () => {
   it("strips credential values but keeps the message legible", () => {
      const source = {
         name: "orders_pg",
         type: "postgres",
         postgresConnection: {
            host: "db.internal",
            userName: "svc",
            password: "sup3r-secret-pw",
         },
      };
      const msg =
         "IO Error: could not connect password=sup3r-secret-pw to db.internal";
      const out = redactConnectionSecrets(msg, source);
      expect(out).not.toContain("sup3r-secret-pw");
      expect(out).toContain("could not connect");
      expect(out).toContain("db.internal"); // host isn't a secret — stays
   });

   it("passes a credential-free error (e.g. schema not found) through verbatim", () => {
      const source = {
         name: "lake",
         type: "ducklake",
         ducklakeConnection: {
            catalog: { postgresConnection: { password: "catpw123" } },
         },
      };
      const msg =
         "Catalog Error: Schema 'analytics' not found in DuckLake 'lake'";
      expect(redactConnectionSecrets(msg, source)).toBe(msg);
   });
});

function createMocks() {
   const sandbox = sinon.createSandbox();

   const repository = {
      getEnvironmentByName: sandbox.stub(),
      listMaterializations: sandbox.stub(),
      getMaterializationById: sandbox.stub(),
      getActiveMaterialization: sandbox.stub(),
      createMaterialization: sandbox.stub(),
      updateMaterialization: sandbox.stub(),
      deleteMaterialization: sandbox.stub(),
   } as unknown as MockRepo;

   const storageManager = { getRepository: () => repository };

   const environmentStore = {
      storageManager,
      getEnvironment: sandbox.stub(),
   } as unknown as EnvironmentStore;

   // Default: environment resolves and yields a package whose build plan is
   // empty (no persist sources). Individual tests override getBuildPlan to
   // exercise the orchestrated path. getModelPaths()=>[] keeps the background
   // build from touching the Malloy runtime.
   repository.getEnvironmentByName.resolves({
      id: "env-1",
      name: "my-env",
      path: "/test",
      createdAt: new Date(),
      updatedAt: new Date(),
   });
   setPackage(environmentStore, { getBuildPlan: () => null });

   const service = new MaterializationService(environmentStore);

   return { sandbox, repository, environmentStore, service };
}

/** Point environmentStore.getEnvironment at a package with the given overrides. */
function setPackage(
   environmentStore: EnvironmentStore,
   pkgOverrides: Record<string, unknown>,
): void {
   const pkg = {
      getBuildPlan: () => null,
      getModelPaths: () => [],
      getPackagePath: () => "/test",
      getMalloyConfig: () => ({}),
      getMalloyConnection: async () => ({}),
      ...pkgOverrides,
   };
   (environmentStore.getEnvironment as sinon.SinonStub).resolves({
      getPackage: sinon.stub().resolves(pkg),
      withPackageLock: async (_name: string, fn: () => Promise<unknown>) =>
         fn(),
      reloadAllModelsForPackage: sinon.stub().resolves(),
   });
}

describe("MaterializationService", () => {
   let ctx: ReturnType<typeof createMocks>;

   beforeEach(() => {
      ctx = createMocks();
   });

   describe("resolveEnvironmentId", () => {
      it("throws EnvironmentNotFoundError when the environment is missing", async () => {
         ctx.repository.getEnvironmentByName.resolves(null);
         await expect(
            ctx.service.listMaterializations("unknown", "pkg"),
         ).rejects.toThrow(EnvironmentNotFoundError);
      });
   });

   describe("deleteMaterialization drop of a storage= table", () => {
      it("routes a storage entry to a destination-aware RW drop (best-effort, delete still completes)", async () => {
         // getApiConnection returns a non-storage type so the RW drop refuses
         // at its destination gate (pre-session), exercising the routing + the
         // best-effort failure handling without needing a live DuckDB attach.
         const getApiConnection = sinon
            .stub()
            .returns({ name: "lake", type: "postgres" });
         (ctx.environmentStore.getEnvironment as sinon.SinonStub).resolves({
            getPackage: sinon
               .stub()
               .resolves({ getMalloyConnection: async () => ({}) }),
            getApiConnection,
            getEnvironmentPath: () => "/test",
         });
         ctx.repository.getMaterializationById.resolves(
            makeMaterialization({
               status: "MANIFEST_FILE_READY",
               manifest: {
                  entries: {
                     se1: {
                        sourceEntityId: "se1",
                        sourceName: "daily",
                        physicalTableName: "daily__mabc123",
                        connectionName: "wh",
                        storageConnectionName: "lake",
                     },
                  },
                  // eslint-disable-next-line @typescript-eslint/no-explicit-any
               } as any,
            }),
         );

         await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1", {
            dropTables: true,
         });

         // Routed to the storage path (resolved the destination connection for
         // an RW drop, not the old skip), and the delete completed despite the
         // drop's best-effort failure.
         expect(getApiConnection.calledWith("lake")).toBe(true);
         expect(
            (
               ctx.repository.deleteMaterialization as sinon.SinonStub
            ).calledWith("mat-1"),
         ).toBe(true);
      });
   });

   describe("queries", () => {
      it("lists materializations for a package", async () => {
         const rows = [
            makeMaterialization(),
            makeMaterialization({ id: "m2" }),
         ];
         ctx.repository.listMaterializations.resolves(rows);
         expect(
            await ctx.service.listMaterializations("my-env", "pkg"),
         ).toEqual(rows);
      });

      it("returns a specific materialization", async () => {
         const m = makeMaterialization();
         ctx.repository.getMaterializationById.resolves(m);
         expect(
            await ctx.service.getMaterialization("my-env", "pkg", "mat-1"),
         ).toEqual(m);
      });

      it("throws when the materialization is missing", async () => {
         ctx.repository.getMaterializationById.resolves(null);
         await expect(
            ctx.service.getMaterialization("my-env", "pkg", "nope"),
         ).rejects.toThrow(MaterializationNotFoundError);
      });

      it("throws when the materialization belongs to another package", async () => {
         ctx.repository.getMaterializationById.resolves(
            makeMaterialization({ packageName: "other" }),
         );
         await expect(
            ctx.service.getMaterialization("my-env", "pkg", "mat-1"),
         ).rejects.toThrow(MaterializationNotFoundError);
      });
   });

   describe("createMaterialization (auto-run)", () => {
      it("creates a PENDING record and persists options with mode=auto", async () => {
         ctx.repository.getActiveMaterialization.resolves(null);
         const pending = makeMaterialization({ status: "PENDING" });
         ctx.repository.createMaterialization.resolves(pending);

         const result = await ctx.service.createMaterialization(
            "my-env",
            "pkg",
            {
               forceRefresh: true,
               sourceNames: ["orders"],
            },
         );

         expect(result.status).toBe("PENDING");
         expect(ctx.repository.createMaterialization.firstCall.args).toEqual([
            "env-1",
            "pkg",
            "PENDING",
            {
               forceRefresh: true,
               sourceNames: ["orders"],
               mode: "auto",
               trigger: "ON_DEMAND",
            },
         ]);
      });

      it("defaults to auto mode in metadata", async () => {
         ctx.repository.getActiveMaterialization.resolves(null);
         ctx.repository.createMaterialization.resolves(
            makeMaterialization({ status: "PENDING" }),
         );

         await ctx.service.createMaterialization("my-env", "pkg", {});

         expect(ctx.repository.createMaterialization.firstCall.args[3]).toEqual(
            {
               forceRefresh: false,
               sourceNames: null,
               mode: "auto",
               trigger: "ON_DEMAND",
            },
         );
      });

      it("records trigger=SCHEDULER when the scheduler fires the run", async () => {
         ctx.repository.getActiveMaterialization.resolves(null);
         ctx.repository.createMaterialization.resolves(
            makeMaterialization({ status: "PENDING" }),
         );

         await ctx.service.createMaterialization("my-env", "pkg", {
            forceRefresh: true,
            trigger: "SCHEDULER",
         });

         expect(
            ctx.repository.createMaterialization.firstCall.args[3],
         ).toMatchObject({ mode: "auto", trigger: "SCHEDULER" });
      });

      it("rejects when an active materialization already exists", async () => {
         ctx.repository.getActiveMaterialization.resolves(
            makeMaterialization({
               id: "existing",
               status: "MANIFEST_ROWS_READY",
            }),
         );
         await expect(
            ctx.service.createMaterialization("my-env", "pkg"),
         ).rejects.toThrow(MaterializationConflictError);
      });

      it("translates a lost create race into a conflict", async () => {
         ctx.repository.getActiveMaterialization
            .onFirstCall()
            .resolves(null)
            .onSecondCall()
            .resolves(makeMaterialization({ id: "winner" }));
         ctx.repository.createMaterialization.rejects(
            new DuplicateActiveMaterializationError("env-1", "pkg"),
         );
         await expect(
            ctx.service.createMaterialization("my-env", "pkg"),
         ).rejects.toThrow(/winner/);
      });
   });

   describe("createMaterialization (orchestrated)", () => {
      it("creates a PENDING record with mode=orchestrated for valid instructions", async () => {
         setPackage(ctx.environmentStore, {
            getBuildPlan: () => makeBuildPlan(),
         });
         ctx.repository.getActiveMaterialization.resolves(null);
         ctx.repository.createMaterialization.resolves(
            makeMaterialization({ status: "PENDING" }),
         );

         const result = await ctx.service.createMaterialization(
            "my-env",
            "pkg",
            { buildInstructions: [makeInstruction()] },
         );

         expect(result.status).toBe("PENDING");
         expect(
            ctx.repository.createMaterialization.firstCall.args[3],
         ).toMatchObject({ mode: "orchestrated" });
      });

      it("rejects instructions referencing an unknown sourceEntityId before creating", async () => {
         setPackage(ctx.environmentStore, {
            getBuildPlan: () => makeBuildPlan(),
         });
         await expect(
            ctx.service.createMaterialization("my-env", "pkg", {
               buildInstructions: [
                  makeInstruction({ sourceEntityId: "ghost" }),
               ],
            }),
         ).rejects.toThrow(BadRequestError);
         expect(ctx.repository.createMaterialization.called).toBe(false);
      });

      it("rejects SNAPSHOT realization", async () => {
         setPackage(ctx.environmentStore, {
            getBuildPlan: () => makeBuildPlan(),
         });
         await expect(
            ctx.service.createMaterialization("my-env", "pkg", {
               buildInstructions: [
                  makeInstruction({ realization: "SNAPSHOT" }),
               ],
            }),
         ).rejects.toThrow(BadRequestError);
      });

      it("rejects buildInstructions when the package has no persist sources", async () => {
         setPackage(ctx.environmentStore, { getBuildPlan: () => null });
         await expect(
            ctx.service.createMaterialization("my-env", "pkg", {
               buildInstructions: [makeInstruction()],
            }),
         ).rejects.toThrow(BadRequestError);
      });
   });

   describe("stopMaterialization", () => {
      const cancellable = ["PENDING", "MANIFEST_ROWS_READY"] as const;

      for (const status of cancellable) {
         it(`cancels a ${status} materialization`, async () => {
            ctx.repository.getMaterializationById.resolves(
               makeMaterialization({ status }),
            );
            ctx.repository.updateMaterialization.resolves(
               makeMaterialization({ status: "CANCELLED" }),
            );
            const result = await ctx.service.stopMaterialization(
               "my-env",
               "pkg",
               "mat-1",
            );
            expect(result.status).toBe("CANCELLED");
         });
      }

      const terminal = ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"] as const;
      for (const status of terminal) {
         it(`rejects stopping a ${status} materialization`, async () => {
            ctx.repository.getMaterializationById.resolves(
               makeMaterialization({ status }),
            );
            await expect(
               ctx.service.stopMaterialization("my-env", "pkg", "mat-1"),
            ).rejects.toThrow(InvalidStateTransitionError);
         });
      }
   });

   describe("deleteMaterialization", () => {
      const terminal = ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"] as const;
      for (const status of terminal) {
         it(`deletes a ${status} materialization`, async () => {
            ctx.repository.getMaterializationById.resolves(
               makeMaterialization({ status }),
            );
            await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1");
            expect(
               ctx.repository.deleteMaterialization.calledOnceWith("mat-1"),
            ).toBe(true);
         });
      }

      const active = ["PENDING", "MANIFEST_ROWS_READY"] as const;
      for (const status of active) {
         it(`rejects deleting a ${status} materialization`, async () => {
            ctx.repository.getMaterializationById.resolves(
               makeMaterialization({ status }),
            );
            await expect(
               ctx.service.deleteMaterialization("my-env", "pkg", "mat-1"),
            ).rejects.toThrow(InvalidStateTransitionError);
            expect(ctx.repository.deleteMaterialization.called).toBe(false);
         });
      }

      it("does not drop tables by default (record-only delete)", async () => {
         const runSQL = sinon.stub().resolves();
         const getMalloyConnection = sinon.stub().resolves({ runSQL });
         (ctx.environmentStore.getEnvironment as sinon.SinonStub).resolves({
            getPackage: sinon.stub().resolves({ getMalloyConnection }),
            withPackageLock: async (_n: string, fn: () => Promise<unknown>) =>
               fn(),
         });
         ctx.repository.getMaterializationById.resolves(
            makeMaterialization({
               status: "MANIFEST_FILE_READY",
               manifest: {
                  builtAt: new Date().toISOString(),
                  strict: false,
                  entries: {
                     b1: {
                        sourceEntityId: "b1",
                        physicalTableName: "orders_mz",
                        connectionName: "duckdb",
                     },
                  },
               },
            }),
         );

         await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1");

         expect(getMalloyConnection.called).toBe(false);
         expect(runSQL.called).toBe(false);
         expect(
            ctx.repository.deleteMaterialization.calledOnceWith("mat-1"),
         ).toBe(true);
      });

      it("drops manifest tables (and staging) when dropTables is set", async () => {
         const runSQL = sinon.stub().resolves();
         const getMalloyConnection = sinon
            .stub()
            .resolves({ runSQL, dialectName: "duckdb" });
         (ctx.environmentStore.getEnvironment as sinon.SinonStub).resolves({
            getPackage: sinon.stub().resolves({ getMalloyConnection }),
            withPackageLock: async (_n: string, fn: () => Promise<unknown>) =>
               fn(),
         });
         ctx.repository.getMaterializationById.resolves(
            makeMaterialization({
               status: "MANIFEST_FILE_READY",
               manifest: {
                  builtAt: new Date().toISOString(),
                  strict: false,
                  entries: {
                     b1: {
                        sourceEntityId: "b1",
                        physicalTableName: "orders_mz",
                        connectionName: "duckdb",
                     },
                  },
               },
            }),
         );

         await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1", {
            dropTables: true,
         });

         expect(getMalloyConnection.calledOnceWith("duckdb")).toBe(true);
         const dropped = runSQL.getCalls().map((c) => c.args[0] as string);
         expect(dropped).toContain('DROP TABLE IF EXISTS "orders_mz"');
         expect(dropped.some((s) => s.includes('"orders_mz_'))).toBe(true);
         expect(
            ctx.repository.deleteMaterialization.calledOnceWith("mat-1"),
         ).toBe(true);
      });

      it("dialect-quotes the drop DDL from the live connection (backtick + container path)", async () => {
         const runSQL = sinon.stub().resolves();
         const getMalloyConnection = sinon
            .stub()
            .resolves({ runSQL, dialectName: "standardsql" });
         (ctx.environmentStore.getEnvironment as sinon.SinonStub).resolves({
            getPackage: sinon.stub().resolves({ getMalloyConnection }),
            withPackageLock: async (_n: string, fn: () => Promise<unknown>) =>
               fn(),
         });
         ctx.repository.getMaterializationById.resolves(
            makeMaterialization({
               status: "MANIFEST_FILE_READY",
               manifest: {
                  builtAt: new Date().toISOString(),
                  strict: false,
                  entries: {
                     b1: {
                        sourceEntityId: "b1",
                        // Container path on a hyphenated BigQuery project: each
                        // segment must be backtick-quoted independently.
                        physicalTableName: "my-proj.ds.engaged",
                        connectionName: "bq",
                     },
                  },
               },
            }),
         );

         await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1", {
            dropTables: true,
         });

         const dropped = runSQL.getCalls().map((c) => c.args[0] as string);
         expect(dropped).toContain(
            "DROP TABLE IF EXISTS `my-proj`.`ds`.`engaged`",
         );
      });

      it("still deletes the record when a table drop fails", async () => {
         const runSQL = sinon.stub().rejects(new Error("boom"));
         const getMalloyConnection = sinon.stub().resolves({ runSQL });
         (ctx.environmentStore.getEnvironment as sinon.SinonStub).resolves({
            getPackage: sinon.stub().resolves({ getMalloyConnection }),
            withPackageLock: async (_n: string, fn: () => Promise<unknown>) =>
               fn(),
         });
         ctx.repository.getMaterializationById.resolves(
            makeMaterialization({
               status: "FAILED",
               manifest: {
                  builtAt: new Date().toISOString(),
                  strict: false,
                  entries: {
                     b1: {
                        sourceEntityId: "b1",
                        physicalTableName: "orders_mz",
                        connectionName: "duckdb",
                     },
                  },
               },
            }),
         );

         await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1", {
            dropTables: true,
         });

         expect(
            ctx.repository.deleteMaterialization.calledOnceWith("mat-1"),
         ).toBe(true);
      });
   });
});

describe("deleteMaterialization telemetry", () => {
   let ctx: ReturnType<typeof createMocks>;
   let harness: MetricsHarness;

   beforeEach(async () => {
      ctx = createMocks();
      harness = await startMetricsHarness();
      // Drop cached instruments so recordDropTables re-binds to this provider.
      resetMaterializationTelemetryForTesting();
   });

   afterEach(async () => {
      resetMaterializationTelemetryForTesting();
      await harness.shutdown();
   });

   const DROP_COUNTER = "publisher_materialization_drop_tables_total";

   // A terminal materialization whose manifest names one physical table, so
   // dropMaterializedTables issues exactly one DROP — the emission under test.
   function dropTablesFixture(runSQL: sinon.SinonStub) {
      const getMalloyConnection = sinon
         .stub()
         .resolves({ runSQL, dialectName: "duckdb" });
      (ctx.environmentStore.getEnvironment as sinon.SinonStub).resolves({
         getPackage: sinon.stub().resolves({ getMalloyConnection }),
         withPackageLock: async (_n: string, fn: () => Promise<unknown>) =>
            fn(),
      });
      ctx.repository.getMaterializationById.resolves(
         makeMaterialization({
            status: "MANIFEST_FILE_READY",
            manifest: {
               builtAt: new Date().toISOString(),
               strict: false,
               entries: {
                  b1: {
                     sourceEntityId: "b1",
                     physicalTableName: "orders_mz",
                     connectionName: "duckdb",
                  },
               },
            },
         }),
      );
   }

   it("meters a successful physical-table drop as outcome=success", async () => {
      dropTablesFixture(sinon.stub().resolves());

      await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1", {
         dropTables: true,
      });

      expect(
         await harness.collectCounter(DROP_COUNTER, { outcome: "success" }),
      ).toBe(1);
      expect(
         await harness.collectCounter(DROP_COUNTER, { outcome: "failure" }),
      ).toBe(0);
   });

   it("meters a failed drop as outcome=failure and still deletes the record", async () => {
      dropTablesFixture(sinon.stub().rejects(new Error("drop boom")));

      // The drop is best-effort: a failure is metered but never surfaces to the
      // caller, and the record is still removed.
      await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1", {
         dropTables: true,
      });

      expect(
         await harness.collectCounter(DROP_COUNTER, { outcome: "failure" }),
      ).toBe(1);
      expect(ctx.repository.deleteMaterialization.calledOnceWith("mat-1")).toBe(
         true,
      );
   });
});

describe("stagingSuffix", () => {
   it("derives a short, stable suffix from the sourceEntityId", () => {
      expect(stagingSuffix("abcdef1234567890")).toBe("_abcdef123456");
   });
});

describe("storagePhysicalTableName", () => {
   const ID = "abcdef1234567890fedcba9876543210";
   it("content-addresses the table with a 16-char id fragment", () => {
      expect(storagePhysicalTableName("daily", ID)).toBe(
         "daily__mabcdef1234567890",
      );
   });
   it("decorates only the table part of a dotted schema.table name", () => {
      expect(storagePhysicalTableName("analytics.daily", ID)).toBe(
         "analytics.daily__mabcdef1234567890",
      );
   });
   it("is deterministic and distinct across differing entity ids", () => {
      expect(storagePhysicalTableName("t", ID)).toBe(
         storagePhysicalTableName("t", ID),
      );
      expect(storagePhysicalTableName("t", ID)).not.toBe(
         storagePhysicalTableName("t", "0000000000000000ffff"),
      );
   });
   it("strips hyphens so a UUID5 id yields a bare identifier fragment", () => {
      expect(
         storagePhysicalTableName("t", "1b4e28ba-2fa1-11d2-883f-0016d3cca427"),
      ).toBe("t__m1b4e28ba2fa111d2");
   });
});

describe("manifestExcludingStorage (Tier 2 chained-storage inline)", () => {
   it("drops storage entries, keeps path-C entries with the manifest's quoting, preserves strict", () => {
      // The source manifest is already dialect-quoted by the seed loop (#904);
      // manifestExcludingStorage reuses that quoting for the kept entries.
      const manifest = new Manifest();
      manifest.strict = true;
      manifest.update("wh_up", { tableName: '"wh_up_v1"' });
      manifest.update("lake_up", { tableName: '"lake"."lake_up__mabc"' });
      const built: Record<string, unknown> = {
         wh_up: {
            sourceEntityId: "wh_up",
            physicalTableName: "wh_up_v1",
            connectionName: "duckdb",
            // no storageConnectionName -> in-warehouse (path C), keep it
         },
         lake_up: {
            sourceEntityId: "lake_up",
            physicalTableName: "lake_up__mabc",
            connectionName: "wh",
            storageConnectionName: "lake", // storage -> exclude it
         },
      };
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const reduced = manifestExcludingStorage(manifest, built as any) as any;
      expect(Object.keys(reduced.entries)).toEqual(["wh_up"]);
      // Kept with the manifest's dialect-quoted name, not the raw physical name.
      expect(reduced.entries.wh_up.tableName).toBe('"wh_up_v1"');
      expect(reduced.entries.lake_up).toBeUndefined();
      expect(reduced.strict).toBe(true);
   });
});

describe("deriveSelfInstructions", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   it("carries forward unchanged sourceEntityIds and builds the rest (deduping repeats)", () => {
      const compiled = compiledWith(
         {
            s1: fakeSource({ name: "s1", sourceEntityId: "b1aaaaaaaaaaaaaa" }),
            s2: fakeSource({ name: "s2", sourceEntityId: "b2bbbbbbbbbbbbbb" }),
         },
         [["s1", "s2"], ["s2"]],
      );
      const priorEntries = {
         b1aaaaaaaaaaaaaa: {
            sourceEntityId: "b1aaaaaaaaaaaaaa",
            physicalTableName: "s1_prev",
            connectionName: "duckdb",
         },
      };

      const { instructions, carried } = (
         ctx.service as unknown as {
            deriveSelfInstructions: (
               c: unknown,
               n: string[] | undefined,
               p: unknown,
            ) => { instructions: BuildInstruction[]; carried: unknown };
         }
      ).deriveSelfInstructions(compiled, undefined, priorEntries);

      expect(instructions).toHaveLength(1);
      expect(instructions[0].sourceEntityId).toBe("b2bbbbbbbbbbbbbb");
      expect(instructions[0].physicalTableName).toBe("s2");
      expect(instructions[0].realization).toBe("COPY");
      expect(instructions[0].materializedTableId).toBe("local-b2bbbbbbbbbb");
      expect(
         (carried as Record<string, unknown>)["b1aaaaaaaaaaaaaa"],
      ).toBeDefined();
   });

   it("content-addresses a storage= source's physical table; path C keeps the logical name", () => {
      process.env.PERSIST_STORAGE_MODE = "on";
      try {
         const compiled = compiledWith(
            {
               lake_src: fakeSource({
                  name: "lake_src",
                  sourceEntityId: "aa11aa11aa11aa11aa11",
                  annotationFields: { storage: "lake", name: "daily" },
               }),
               wh_src: fakeSource({
                  name: "wh_src",
                  sourceEntityId: "bb22bb22bb22bb22bb22",
               }),
            },
            [["lake_src", "wh_src"]],
         );
         const { instructions } = (
            ctx.service as unknown as {
               deriveSelfInstructions: (
                  c: unknown,
                  n: string[] | undefined,
                  p: unknown,
               ) => { instructions: BuildInstruction[] };
            }
         ).deriveSelfInstructions(compiled, undefined, {});
         const byId = Object.fromEntries(
            instructions.map((i) => [i.sourceEntityId, i]),
         );
         const lake = byId["aa11aa11aa11aa11aa11"];
         const wh = byId["bb22bb22bb22bb22bb22"];
         // storage= → content-addressed hashed name in the destination.
         expect(lake.destination).toBe("lake");
         expect(lake.physicalTableName).toBe(
            storagePhysicalTableName("daily", "aa11aa11aa11aa11aa11"),
         );
         expect(lake.physicalTableName).toBe("daily__maa11aa11aa11aa11");
         // path C → the plain logical (source) name, no destination.
         expect(wh.destination).toBeUndefined();
         expect(wh.physicalTableName).toBe("wh_src");
      } finally {
         delete process.env.PERSIST_STORAGE_MODE;
      }
   });

   it("honors the sourceNames filter (excluded sources are neither built nor carried)", () => {
      const compiled = compiledWith(
         {
            s1: fakeSource({ name: "s1", sourceEntityId: "b1aaaaaaaaaaaaaa" }),
            s2: fakeSource({ name: "s2", sourceEntityId: "b2bbbbbbbbbbbbbb" }),
         },
         [["s1", "s2"]],
      );
      const { instructions, carried } = (
         ctx.service as unknown as {
            deriveSelfInstructions: (
               c: unknown,
               n: string[] | undefined,
               p: unknown,
            ) => { instructions: BuildInstruction[]; carried: unknown };
         }
      ).deriveSelfInstructions(compiled, ["s2"], {});

      expect(instructions).toHaveLength(1);
      expect(instructions[0].sourceEntityId).toBe("b2bbbbbbbbbbbbbb");
      expect(Object.keys(carried as Record<string, unknown>)).toHaveLength(0);
   });
});

describe("getMostRecentManifestEntries (skip-if-unchanged)", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   it("returns entries from the most recent successful run, excluding the in-flight one", async () => {
      const entries = {
         b1: {
            sourceEntityId: "b1",
            physicalTableName: "orders_v1",
            connectionName: "duckdb",
         },
      };
      ctx.repository.listMaterializations.resolves([
         makeMaterialization({
            id: "in-flight",
            status: "MANIFEST_ROWS_READY",
         }),
         makeMaterialization({
            id: "old-success",
            status: "MANIFEST_FILE_READY",
            manifest: { builtAt: "t", strict: false, entries },
         }),
      ]);

      const result = await (
         ctx.service as unknown as {
            getMostRecentManifestEntries: (
               e: string,
               p: string,
               x: string,
            ) => Promise<unknown>;
         }
      ).getMostRecentManifestEntries("env-1", "pkg", "in-flight");

      expect(result).toEqual(entries);
   });

   it("returns {} when the only successful run is the excluded one", async () => {
      ctx.repository.listMaterializations.resolves([
         makeMaterialization({
            id: "self",
            status: "MANIFEST_FILE_READY",
            manifest: {
               builtAt: "t",
               strict: false,
               entries: {
                  b1: { sourceEntityId: "b1", physicalTableName: "t1" },
               },
            },
         }),
      ]);

      const result = await (
         ctx.service as unknown as {
            getMostRecentManifestEntries: (
               e: string,
               p: string,
               x: string,
            ) => Promise<unknown>;
         }
      ).getMostRecentManifestEntries("env-1", "pkg", "self");

      expect(result).toEqual({});
   });
});

describe("stopMaterialization (in-flight)", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   it("aborts a running build cooperatively without forcing a transition", async () => {
      ctx.repository.getMaterializationById.resolves(
         makeMaterialization({ status: "MANIFEST_ROWS_READY" }),
      );
      const controller = new AbortController();
      (
         ctx.service as unknown as {
            runningAbortControllers: Map<string, AbortController>;
         }
      ).runningAbortControllers.set("mat-1", controller);

      const result = await ctx.service.stopMaterialization(
         "my-env",
         "pkg",
         "mat-1",
      );

      expect(controller.signal.aborted).toBe(true);
      // The background run records the terminal state; stop must not also write.
      expect(ctx.repository.updateMaterialization.called).toBe(false);
      expect(result.status).toBe("MANIFEST_ROWS_READY");
   });
});

describe("executeInstructedBuild", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   function callExecute(
      compiled: unknown,
      instructions: BuildInstruction[],
      seed: Record<string, unknown>,
   ): Promise<Record<string, { physicalTableName?: string }>> {
      // A stub BuildEnvironment: only the `storage=` branch touches it, and
      // these tests exercise the default in-warehouse path, so it is never read.
      const environment = {
         getApiConnection: () => {
            throw new Error("no connection config in this test");
         },
         getEnvironmentPath: () => "/tmp/env",
      };
      return (
         ctx.service as unknown as {
            executeInstructedBuild: (
               c: unknown,
               env: unknown,
               i: BuildInstruction[],
               s: Record<string, unknown>,
               sig: AbortSignal,
            ) => Promise<Record<string, { physicalTableName?: string }>>;
         }
      ).executeInstructedBuild(
         compiled,
         environment,
         instructions,
         seed,
         new AbortController().signal,
      );
   }

   it("builds instructed sources in dependency order and seeds carried entries", async () => {
      const runSQL = sinon.stub().resolves();
      const connection = { runSQL } as unknown as MalloyConnection;
      const s1 = fakeSource({ name: "s1", sourceEntityId: "b1aaaaaaaaaaaaaa" });
      const s2 = fakeSource({ name: "s2", sourceEntityId: "b2bbbbbbbbbbbbbb" });
      const compiled = compiledWith(
         { s1, s2 },
         [["s1"], ["s2"]],
         new Map([["duckdb", connection]]),
      );

      const entries = await callExecute(
         compiled,
         [
            {
               sourceEntityId: "b1aaaaaaaaaaaaaa",
               materializedTableId: "mt-1",
               physicalTableName: "s1_v1",
               realization: "COPY",
            },
            {
               sourceEntityId: "b2bbbbbbbbbbbbbb",
               materializedTableId: "mt-2",
               physicalTableName: "s2_v1",
               realization: "COPY",
            },
         ],
         {
            carried0: {
               sourceEntityId: "carried0",
               physicalTableName: "carried_tbl",
               connectionName: "duckdb",
            },
         },
      );

      // Both instructed sources built, plus the carried seed entry retained.
      expect(entries["b1aaaaaaaaaaaaaa"].physicalTableName).toBe("s1_v1");
      expect(entries["b2bbbbbbbbbbbbbb"].physicalTableName).toBe("s2_v1");
      expect(entries["carried0"].physicalTableName).toBe("carried_tbl");
   });

   it("builds an intermediate persist source nested under a root node", async () => {
      // Mirrors malloy getBuildPlan(): only the terminal `root` is a graph
      // node; its persist dependency `mid` is nested in dependsOn (not a node).
      // Before the iterGraphSources fix, `mid` was silently never built even
      // though the caller instructed it. Both must now build, `mid` first.
      const runSQL = sinon.stub().resolves();
      const connection = { runSQL } as unknown as MalloyConnection;
      const root = fakeSource({
         name: "root",
         sourceEntityId: "brootaaaaaaaaaa",
      });
      const mid = fakeSource({
         name: "mid",
         sourceEntityId: "bmidbbbbbbbbbbb",
      });
      const compiled = {
         graphs: [
            {
               connectionName: "duckdb",
               nodes: [
                  [
                     {
                        sourceID: "root",
                        dependsOn: [{ sourceID: "mid", dependsOn: [] }],
                     },
                  ],
               ],
            },
         ],
         sources: { root, mid },
         connectionDigests: { duckdb: "dig" },
         connections: new Map([["duckdb", connection]]),
      };

      const entries = await callExecute(
         compiled,
         [
            {
               sourceEntityId: "brootaaaaaaaaaa",
               materializedTableId: "mt-r",
               physicalTableName: "root_v1",
               realization: "COPY",
            },
            {
               sourceEntityId: "bmidbbbbbbbbbbb",
               materializedTableId: "mt-m",
               physicalTableName: "mid_v1",
               realization: "COPY",
            },
         ],
         {},
      );

      expect(entries["bmidbbbbbbbbbbb"].physicalTableName).toBe("mid_v1");
      expect(entries["brootaaaaaaaaaa"].physicalTableName).toBe("root_v1");
      // The dependency's CREATE precedes its dependent's (dependency order).
      const creates = runSQL
         .getCalls()
         .map((c) => c.args[0] as string)
         .filter((s) => s.startsWith("CREATE TABLE"));
      const midIdx = creates.findIndex((s) => s.includes("mid_v1"));
      const rootIdx = creates.findIndex((s) => s.includes("root_v1"));
      expect(midIdx).toBeGreaterThanOrEqual(0);
      expect(rootIdx).toBeGreaterThan(midIdx);
   });

   it("seeds a downstream build with the QUOTED upstream reference (case-folding dialect)", async () => {
      // A carried/seeded upstream (built in a prior run or unit, e.g. via
      // referenceManifest) must reach the downstream's SQL generation as the
      // SAME quoted path the builder CREATEd it with; otherwise the downstream
      // CREATE misses the case-preserved table on a case-folding engine
      // (Snowflake). Capture the buildManifest Malloy is handed for the
      // downstream to prove the seed was quoted for its connection's dialect
      // before it becomes a FROM.
      let seen:
         | {
              buildManifest?: {
                 entries?: Record<string, { tableName?: string }>;
              };
           }
         | undefined;
      const runSQL = sinon.stub().resolves();
      const connection = {
         runSQL,
         dialectName: "snowflake",
      } as unknown as MalloyConnection;
      const down = fakeSource({
         name: "down",
         sourceEntityId: "bdownaaaaaaaaaa",
         dialectName: "snowflake",
         onGetSQL: (o) => {
            seen = o as typeof seen;
         },
      });
      const compiled = compiledWith(
         { down },
         [["down"]],
         new Map([["duckdb", connection]]),
      );

      await callExecute(
         compiled,
         [
            {
               sourceEntityId: "bdownaaaaaaaaaa",
               materializedTableId: "mt-d",
               physicalTableName: "down_v1",
               realization: "COPY",
            },
         ],
         {
            up: {
               sourceEntityId: "up",
               physicalTableName: "schema.orders_base",
               connectionName: "duckdb",
            },
         },
      );

      expect(seen?.buildManifest?.entries?.up?.tableName).toBe(
         '"schema"."orders_base"',
      );
   });

   it("seeds unquoted when the upstream's connection is absent from the build (older CP / cross-connection)", async () => {
      // No connectionName on the seed (an older control plane) -> bind verbatim,
      // exactly the pre-change behavior. Non-case-folding engines are unaffected;
      // a case-folding engine self-signals by failing the downstream CREATE.
      let seen:
         | {
              buildManifest?: {
                 entries?: Record<string, { tableName?: string }>;
              };
           }
         | undefined;
      const runSQL = sinon.stub().resolves();
      const connection = {
         runSQL,
         dialectName: "snowflake",
      } as unknown as MalloyConnection;
      const down = fakeSource({
         name: "down",
         sourceEntityId: "bdownaaaaaaaaaa",
         dialectName: "snowflake",
         onGetSQL: (o) => {
            seen = o as typeof seen;
         },
      });
      const compiled = compiledWith(
         { down },
         [["down"]],
         new Map([["duckdb", connection]]),
      );

      await callExecute(
         compiled,
         [
            {
               sourceEntityId: "bdownaaaaaaaaaa",
               materializedTableId: "mt-d",
               physicalTableName: "down_v1",
               realization: "COPY",
            },
         ],
         { up: { sourceEntityId: "up", physicalTableName: "orders_base" } },
      );

      expect(seen?.buildManifest?.entries?.up?.tableName).toBe("orders_base");
   });

   it("throws when an instructed graph's connection is missing", async () => {
      const s1 = fakeSource({ name: "s1", sourceEntityId: "b1aaaaaaaaaaaaaa" });
      const compiled = compiledWith({ s1 }, [["s1"]], new Map());

      await expect(
         callExecute(
            compiled,
            [
               {
                  sourceEntityId: "b1aaaaaaaaaaaaaa",
                  materializedTableId: "mt-1",
                  physicalTableName: "s1_v1",
                  realization: "COPY",
               },
            ],
            {},
         ),
      ).rejects.toThrow(BadRequestError);
   });
});

describe("buildOneSource", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   function callBuildOneSource(
      connection: { runSQL: sinon.SinonStub },
      physicalTableName: string,
      dialectName?: string,
      manifest: Manifest = new Manifest(),
   ): Promise<{ sourceEntityId: string; physicalTableName: string }> {
      const source = fakeSource({
         name: "orders",
         sourceEntityId: "abcdef1234567890",
         sql: "SELECT * FROM t",
         dialectName,
      });
      const instruction: BuildInstruction = {
         sourceEntityId: "abcdef1234567890",
         materializedTableId: "mt-1",
         physicalTableName,
         realization: "COPY",
      };
      return (
         ctx.service as unknown as {
            buildOneSource: (
               s: unknown,
               i: BuildInstruction,
               c: unknown,
               d: Record<string, string>,
               m: Manifest,
            ) => Promise<{ sourceEntityId: string; physicalTableName: string }>;
         }
      ).buildOneSource(
         source,
         instruction,
         connection,
         { duckdb: "dig" },
         manifest,
      );
   }

   it("stages, swaps, and renames in a crash-safe order", async () => {
      const runSQL = sinon.stub().resolves();
      const entry = await callBuildOneSource({ runSQL }, "orders_v1");

      const sql = runSQL.getCalls().map((c) => c.args[0] as string);
      expect(sql).toEqual([
         'DROP TABLE IF EXISTS "orders_v1_abcdef123456"',
         'CREATE TABLE "orders_v1_abcdef123456" AS (SELECT * FROM t)',
         'DROP TABLE IF EXISTS "orders_v1"',
         'ALTER TABLE "orders_v1_abcdef123456" RENAME TO "orders_v1"',
      ]);
      expect(entry.physicalTableName).toBe("orders_v1");
      expect(entry.sourceEntityId).toBe("abcdef1234567890");
   });

   it("drops the staging table and rethrows when the build SQL fails", async () => {
      const runSQL = sinon.stub();
      runSQL.onCall(0).resolves(); // initial staging drop
      runSQL.onCall(1).rejects(new Error("create boom")); // CREATE TABLE AS
      runSQL.onCall(2).resolves(); // cleanup staging drop
      await expect(callBuildOneSource({ runSQL }, "orders_v1")).rejects.toThrow(
         "create boom",
      );
      expect(runSQL.lastCall.args[0]).toBe(
         'DROP TABLE IF EXISTS "orders_v1_abcdef123456"',
      );
   });

   it("rethrows the original build error when staging cleanup also fails", async () => {
      const runSQL = sinon.stub();
      runSQL.onCall(0).resolves(); // initial staging drop
      runSQL.onCall(1).rejects(new Error("create boom")); // CREATE TABLE AS
      runSQL.onCall(2).rejects(new Error("cleanup boom")); // cleanup drop fails
      // The cleanup failure is swallowed (logged as a physical leak); the
      // original build error is what propagates, so the run is classified by its
      // real cause rather than the cleanup noise.
      await expect(callBuildOneSource({ runSQL }, "orders_v1")).rejects.toThrow(
         "create boom",
      );
      expect(runSQL.callCount).toBe(3);
      expect(runSQL.lastCall.args[0]).toBe(
         'DROP TABLE IF EXISTS "orders_v1_abcdef123456"',
      );
   });

   it("drops staging when the build is cancelled mid-run (no _staging orphan)", async () => {
      // buildOneSource is cancellation-agnostic: a cancel surfaces as a thrown
      // build SQL and takes the same staging-cleanup path as a failure, so a
      // cancelled build leaves no orphaned _staging table behind.
      const runSQL = sinon.stub();
      runSQL.onCall(0).resolves(); // initial staging drop
      runSQL.onCall(1).rejects(new Error("Build cancelled")); // aborted mid-CREATE
      runSQL.onCall(2).resolves(); // cleanup staging drop
      await expect(callBuildOneSource({ runSQL }, "orders_v1")).rejects.toThrow(
         "Build cancelled",
      );
      expect(runSQL.lastCall.args[0]).toBe(
         'DROP TABLE IF EXISTS "orders_v1_abcdef123456"',
      );
   });

   it("quotes each segment of a container path for a backtick dialect", async () => {
      const runSQL = sinon.stub().resolves();
      // Container path (dataset.table) on a backtick dialect (BigQuery): each
      // segment is quoted independently, and the rename targets the bare name.
      const entry = await callBuildOneSource(
         { runSQL },
         "ds.orders_v1",
         "standardsql",
      );

      const sql = runSQL.getCalls().map((c) => c.args[0] as string);
      expect(sql).toEqual([
         "DROP TABLE IF EXISTS `ds`.`orders_v1_abcdef123456`",
         "CREATE TABLE `ds`.`orders_v1_abcdef123456` AS (SELECT * FROM t)",
         "DROP TABLE IF EXISTS `ds`.`orders_v1`",
         "ALTER TABLE `ds`.`orders_v1_abcdef123456` RENAME TO `orders_v1`",
      ]);
      expect(entry.physicalTableName).toBe("ds.orders_v1");
   });

   it("records the QUOTED just-built name in the build manifest (matches the CREATE)", async () => {
      // The build manifest feeds a downstream persist's FROM verbatim, so the
      // recorded name must be the SAME quoted path the CREATE used — else a
      // downstream built later in this run misses the case-preserved table on a
      // case-folding engine. buildManifest also passes through Malloy's canonical
      // path validation (requireCanonicalTablePathAnyDialect), so this proves the
      // quoted form is accepted, not just produced.
      const runSQL = sinon.stub().resolves();
      const manifest = new Manifest();
      await callBuildOneSource(
         { runSQL },
         "schema.orders_base",
         "snowflake",
         manifest,
      );
      expect(manifest.buildManifest.entries["abcdef1234567890"].tableName).toBe(
         '"schema"."orders_base"',
      );
      // The logical (unquoted) name is what the CREATE was told to produce, and
      // is exactly what quoteTablePath quoted for the DDL — read mirrors write.
      const created = runSQL
         .getCalls()
         .map((c) => c.args[0] as string)
         .find((s) => s.startsWith("CREATE TABLE"))!;
      expect(created).toContain('"schema"."orders_base_abcdef123456"');
   });
});

describe("buildOneSourceIntoStorage (Tier-3 fallback ladder)", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   function callInto(opts: {
      strict: boolean;
      tier3: "ok" | "throw";
   }): Promise<{
      physicalTableName: string;
      storageConnectionName?: string;
   }> {
      const source = fakeSource({
         name: "monthly",
         sourceEntityId: "abcdef1234567890",
         annotationFields: { storage: "lake" },
      });
      const instruction: BuildInstruction = {
         sourceEntityId: "abcdef1234567890",
         materializedTableId: "mt",
         physicalTableName: "monthly__mabc",
         realization: "COPY",
         destination: "lake",
      };
      const manifest = new Manifest();
      manifest.strict = opts.strict;
      const environment = {
         getApiConnection: (n: string) => ({
            name: n,
            type: n === "lake" ? "duckdb" : "postgres",
         }),
         getEnvironmentPath: () => "/tmp/env",
      };
      const svc = ctx.service as unknown as {
         buildDownstreamViaParents: unknown;
         buildOneSourceIntoStorage: (
            s: unknown,
            i: BuildInstruction,
            m: Manifest,
            e: unknown,
            sql: string,
            built: Record<string, unknown>,
            dep: boolean,
         ) => Promise<{
            physicalTableName: string;
            storageConnectionName?: string;
         }>;
      };
      // Stub the Tier-3 seam so the test exercises the LADDER, not the build.
      svc.buildDownstreamViaParents =
         opts.tier3 === "ok"
            ? sinon.stub().resolves({
                 storageConnectionName: "lake",
                 schema: [
                    { name: "order_month", type: "DATE" },
                    { name: "monthly_total", type: "DOUBLE" },
                 ],
              })
            : sinon.stub().rejects(new Error("uncarried parent"));
      return svc.buildOneSourceIntoStorage(
         source,
         instruction,
         manifest,
         environment,
         "SELECT should_not_run",
         {
            up: {
               sourceEntityId: "up",
               sourceName: "daily",
               physicalTableName: "daily__mabc",
               storageConnectionName: "lake",
               schema: [{ name: "order_date", type: "DATE" }],
            },
         },
         true, // dependsOnStorageUpstream
      );
   }

   it("Tier 3 success: builds by reading the parent and returns the storage entry", async () => {
      const entry = await callInto({ strict: false, tier3: "ok" });
      expect(entry.storageConnectionName).toBe("lake");
      expect(entry.physicalTableName).toBe("monthly__mabc");
   });

   it("strict + Tier 3 ineligible: refuses loudly instead of recomputing from raw", async () => {
      await expect(callInto({ strict: true, tier3: "throw" })).rejects.toThrow(
         /strict upstreams forbid/i,
      );
   });
});

describe("runBuild (branch behavior)", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   // Exercise runBuild's orchestration in isolation: the build engine and
   // manifest commit are stubbed so the test asserts which instructions flow
   // through and whether the manifest is auto-loaded, not how a source is built.
   type RunBuildInternals = {
      executeInstructedBuild: sinon.SinonStub;
      commitManifest: sinon.SinonStub;
      autoLoadManifest: sinon.SinonStub;
      runBuild: (
         id: string,
         env: string,
         pkg: string,
         opts: {
            sourceNames: string[] | undefined;
            forceRefresh: boolean;
            buildInstructions: BuildInstruction[] | undefined;
            referenceManifest?:
               | {
                    sourceEntityId: string;
                    physicalTableName: string;
                    connectionName?: string;
                 }[]
               | undefined;
            strictUpstreams?: boolean | undefined;
         },
         signal: AbortSignal,
      ) => Promise<void>;
   };

   function stubEngine(): RunBuildInternals {
      const entries = {
         "build-orders": {
            sourceEntityId: "build-orders",
            physicalTableName: '"orders_v1"',
            connectionName: "duckdb",
         },
      };
      const svc = ctx.service as unknown as RunBuildInternals;
      svc.executeInstructedBuild = sinon.stub().resolves(entries);
      svc.commitManifest = sinon.stub().resolves();
      svc.autoLoadManifest = sinon.stub().resolves();
      return svc;
   }

   it("orchestrated: builds caller instructions and does not auto-load", async () => {
      const svc = stubEngine();
      const instructions = [makeInstruction()];

      await svc.runBuild(
         "mat-1",
         "my-env",
         "pkg",
         {
            sourceNames: undefined,
            forceRefresh: false,
            buildInstructions: instructions,
         },
         new AbortController().signal,
      );

      expect(svc.executeInstructedBuild.calledOnce).toBe(true);
      // Caller instructions pass straight through; nothing is carried/reused.
      // (arg[1] is the environment; instructions are arg[2], seed entries arg[3].)
      expect(svc.executeInstructedBuild.firstCall.args[2]).toEqual(
         instructions,
      );
      expect(svc.executeInstructedBuild.firstCall.args[3]).toEqual({});
      expect(svc.commitManifest.firstCall.args[2]).toMatchObject({
         mode: "orchestrated",
         sourcesBuilt: 1,
         sourcesReused: 0,
      });
      // Orchestrated leaves distribution to the caller.
      expect(svc.autoLoadManifest.called).toBe(false);
   });

   it("orchestrated: seeds the build from referenceManifest and honors strictUpstreams", async () => {
      const svc = stubEngine();
      const instructions = [makeInstruction()];

      await svc.runBuild(
         "mat-1",
         "my-env",
         "pkg",
         {
            sourceNames: undefined,
            forceRefresh: false,
            buildInstructions: instructions,
            referenceManifest: [
               {
                  sourceEntityId: "up-1",
                  physicalTableName: "upstream_tbl",
                  connectionName: "sf",
               },
            ],
            strictUpstreams: true,
         },
         new AbortController().signal,
      );

      // The reference manifest becomes the seed (carried) entries so a
      // downstream build resolves its upstream to the existing physical table.
      // arg[1] is environment (the storage build session), so the seed
      // (carried) entries are arg[3]. connectionName is carried through so the
      // seed can be quoted for the upstream's dialect (see quoteSeedTablePath).
      expect(svc.executeInstructedBuild.firstCall.args[3]).toEqual({
         "up-1": {
            sourceEntityId: "up-1",
            physicalTableName: "upstream_tbl",
            connectionName: "sf",
         },
      });
      // strictUpstreams flows through as the strict flag (6th arg).
      expect(svc.executeInstructedBuild.firstCall.args[5]).toBe(true);
      // Reused upstreams are counted as carried, not built.
      expect(svc.commitManifest.firstCall.args[2]).toMatchObject({
         mode: "orchestrated",
         sourcesBuilt: 1,
         sourcesReused: 1,
      });
   });

   it("auto-run: derives instructions and auto-loads the manifest", async () => {
      const svc = stubEngine();

      await svc.runBuild(
         "mat-1",
         "my-env",
         "pkg",
         {
            sourceNames: undefined,
            forceRefresh: true,
            buildInstructions: undefined,
         },
         new AbortController().signal,
      );

      expect(svc.commitManifest.firstCall.args[2]).toMatchObject({
         mode: "auto",
      });
      // Auto-run owns distribution: it loads the fresh manifest into the models.
      expect(svc.autoLoadManifest.calledOnce).toBe(true);
   });
});

describe("autoLoadManifest", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   it("loads only entries with a physical table into the package models", async () => {
      const reloadAllModelsForPackage = sinon.stub().resolves();
      const entries = {
         b1: {
            sourceEntityId: "b1",
            physicalTableName: "orders_v1",
            connectionName: "duckdb",
         },
         b2: { sourceEntityId: "b2", physicalTableName: undefined },
      };

      await (
         ctx.service as unknown as {
            autoLoadManifest: (
               env: { reloadAllModelsForPackage: sinon.SinonStub },
               pkg: string,
               entries: Record<string, unknown>,
            ) => Promise<void>;
         }
      ).autoLoadManifest({ reloadAllModelsForPackage }, "pkg", entries);

      expect(reloadAllModelsForPackage.calledOnce).toBe(true);
      // connectionName is carried alongside tableName so the bind step
      // (Package.quoteBoundTableNames) can quote the path for its dialect.
      expect(reloadAllModelsForPackage.firstCall.args).toEqual([
         "pkg",
         { b1: { tableName: "orders_v1", connectionName: "duckdb" } },
      ]);
   });

   it("swallows a reload failure (best-effort; run already succeeded)", async () => {
      const reloadAllModelsForPackage = sinon
         .stub()
         .rejects(new Error("reload boom"));
      await expect(
         (
            ctx.service as unknown as {
               autoLoadManifest: (
                  env: { reloadAllModelsForPackage: sinon.SinonStub },
                  pkg: string,
                  entries: Record<string, unknown>,
               ) => Promise<void>;
            }
         ).autoLoadManifest({ reloadAllModelsForPackage }, "pkg", {
            b1: { sourceEntityId: "b1", physicalTableName: "t" },
         }),
      ).resolves.toBeUndefined();
   });
});

describe("runInBackground (terminal recording)", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   const flush = () => new Promise((r) => setTimeout(r, 20));

   function background(): {
      runInBackground: (
         id: string,
         run: (signal: AbortSignal) => Promise<void>,
      ) => void;
      runningAbortControllers: Map<string, AbortController>;
   } {
      return ctx.service as unknown as {
         runInBackground: (
            id: string,
            run: (signal: AbortSignal) => Promise<void>,
         ) => void;
         runningAbortControllers: Map<string, AbortController>;
      };
   }

   it("records FAILED with the error message when the run rejects", async () => {
      background().runInBackground("bg-1", async () => {
         throw new Error("boom");
      });
      await flush();

      expect(ctx.repository.updateMaterialization.calledOnce).toBe(true);
      expect(ctx.repository.updateMaterialization.firstCall.args[0]).toBe(
         "bg-1",
      );
      expect(
         ctx.repository.updateMaterialization.firstCall.args[1],
      ).toMatchObject({ status: "FAILED", error: "boom" });
   });

   it("records CANCELLED when the run aborts cooperatively", async () => {
      const bg = background();
      bg.runInBackground("bg-2", async () => {
         bg.runningAbortControllers.get("bg-2")!.abort();
         throw new Error("boom");
      });
      await flush();

      expect(
         ctx.repository.updateMaterialization.firstCall.args[1],
      ).toMatchObject({ status: "CANCELLED", error: "Cancelled" });
   });

   it("clears the abort controller once the run settles", async () => {
      const bg = background();
      bg.runInBackground("bg-3", async () => {});
      await flush();
      expect(bg.runningAbortControllers.has("bg-3")).toBe(false);
   });
});

describe("transition (state machine)", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   function transition(): (
      id: string,
      next: MaterializationStatus,
   ) => Promise<unknown> {
      return (
         ctx.service as unknown as {
            transition: (
               id: string,
               next: MaterializationStatus,
            ) => Promise<unknown>;
         }
      ).transition.bind(ctx.service);
   }

   it("throws MaterializationNotFoundError when the record is gone", async () => {
      ctx.repository.getMaterializationById.resolves(undefined);
      await expect(
         transition()("mat-1", "MANIFEST_ROWS_READY"),
      ).rejects.toThrow(MaterializationNotFoundError);
   });

   it("rejects a transition the state machine disallows", async () => {
      ctx.repository.getMaterializationById.resolves(
         makeMaterialization({ status: "MANIFEST_FILE_READY" }),
      );
      // MANIFEST_FILE_READY is terminal; nothing follows it.
      await expect(transition()("mat-1", "PENDING")).rejects.toThrow(
         InvalidStateTransitionError,
      );
      expect(ctx.repository.updateMaterialization.called).toBe(false);
   });

   it("persists a valid transition", async () => {
      ctx.repository.getMaterializationById.resolves(
         makeMaterialization({ status: "PENDING" }),
      );
      ctx.repository.updateMaterialization.resolves(
         makeMaterialization({ status: "MANIFEST_ROWS_READY" }),
      );
      await transition()("mat-1", "MANIFEST_ROWS_READY");
      expect(
         ctx.repository.updateMaterialization.calledOnceWith("mat-1", {
            status: "MANIFEST_ROWS_READY",
         }),
      ).toBe(true);
   });
});
