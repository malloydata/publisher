import { beforeEach, describe, expect, it } from "bun:test";
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
   BuildPlan,
   Materialization,
   ResourceRepository,
} from "../storage/DatabaseInterface";
import { DuplicateActiveMaterializationError } from "../storage/duckdb/MaterializationRepository";
import { EnvironmentStore } from "./environment_store";
import {
   MaterializationService,
   stagingSuffix,
} from "./materialization_service";

function makeMaterialization(
   overrides: Partial<Materialization> = {},
): Materialization {
   return {
      id: "mat-1",
      environmentId: "env-1",
      packageName: "pkg",
      status: "PENDING",
      buildPlan: null,
      manifest: null,
      startedAt: null,
      completedAt: null,
      error: null,
      metadata: null,
      createdAt: new Date("2026-01-01"),
      updatedAt: new Date("2026-01-01"),
      ...overrides,
   };
}

function makeBuildPlan(overrides: Partial<BuildPlan> = {}): BuildPlan {
   return {
      graphs: [
         {
            connectionName: "duckdb",
            nodes: [[{ sourceID: "orders@m.malloy", dependsOn: [] }]],
         },
      ],
      sources: {
         "orders@m.malloy": {
            name: "orders",
            sourceID: "orders@m.malloy",
            connectionName: "duckdb",
            dialect: "duckdb",
            buildId: "build-orders",
            sql: "SELECT 1",
            columns: [],
         },
      },
      ...overrides,
   };
}

function makeInstruction(
   overrides: Partial<BuildInstruction> = {},
): BuildInstruction {
   return {
      buildId: "build-orders",
      materializedTableId: "mt-1",
      physicalTableName: '"orders_v1"',
      realization: "COPY",
      ...overrides,
   };
}

type MockRepo = sinon.SinonStubbedInstance<ResourceRepository>;

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

   // Default: environment resolves and yields a package.
   repository.getEnvironmentByName.resolves({
      id: "env-1",
      name: "my-env",
      path: "/test",
      createdAt: new Date(),
      updatedAt: new Date(),
   });
   (environmentStore.getEnvironment as sinon.SinonStub).resolves({
      getPackage: sinon.stub().resolves({}),
      withPackageLock: async (_name: string, fn: () => Promise<unknown>) => fn(),
   });

   const service = new MaterializationService(environmentStore);

   return { sandbox, repository, environmentStore, service };
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

   describe("queries", () => {
      it("lists materializations for a package", async () => {
         const rows = [makeMaterialization(), makeMaterialization({ id: "m2" })];
         ctx.repository.listMaterializations.resolves(rows);
         expect(await ctx.service.listMaterializations("my-env", "pkg")).toEqual(
            rows,
         );
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

   describe("createMaterialization (Round 1)", () => {
      it("creates a PENDING record and persists options", async () => {
         ctx.repository.getActiveMaterialization.resolves(null);
         const pending = makeMaterialization({ status: "PENDING" });
         ctx.repository.createMaterialization.resolves(pending);

         const result = await ctx.service.createMaterialization(
            "my-env",
            "pkg",
            { forceRefresh: true, sourceNames: ["orders"] },
         );

         expect(result.status).toBe("PENDING");
         expect(ctx.repository.createMaterialization.firstCall.args).toEqual([
            "env-1",
            "pkg",
            "PENDING",
            { forceRefresh: true, sourceNames: ["orders"] },
         ]);
      });

      it("rejects when an active materialization already exists", async () => {
         ctx.repository.getActiveMaterialization.resolves(
            makeMaterialization({ id: "existing", status: "BUILD_PLAN_READY" }),
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

   describe("buildMaterialization (Round 2)", () => {
      it("rejects when not at BUILD_PLAN_READY", async () => {
         ctx.repository.getMaterializationById.resolves(
            makeMaterialization({ status: "PENDING" }),
         );
         await expect(
            ctx.service.buildMaterialization("my-env", "pkg", "mat-1", [
               makeInstruction(),
            ]),
         ).rejects.toThrow(InvalidStateTransitionError);
      });

      it("rejects instructions that reference an unknown buildId", async () => {
         ctx.repository.getMaterializationById.resolves(
            makeMaterialization({
               status: "BUILD_PLAN_READY",
               buildPlan: makeBuildPlan(),
            }),
         );
         await expect(
            ctx.service.buildMaterialization("my-env", "pkg", "mat-1", [
               makeInstruction({ buildId: "ghost" }),
            ]),
         ).rejects.toThrow(BadRequestError);
      });

      it("rejects SNAPSHOT when the connection does not support it", async () => {
         ctx.repository.getMaterializationById.resolves(
            makeMaterialization({
               status: "BUILD_PLAN_READY",
               buildPlan: makeBuildPlan(),
            }),
         );
         await expect(
            ctx.service.buildMaterialization("my-env", "pkg", "mat-1", [
               makeInstruction({ realization: "SNAPSHOT" }),
            ]),
         ).rejects.toThrow(BadRequestError);
      });

      it("accepts a valid build and returns the record (202)", async () => {
         const m = makeMaterialization({
            status: "BUILD_PLAN_READY",
            buildPlan: makeBuildPlan(),
         });
         ctx.repository.getMaterializationById.resolves(m);
         const result = await ctx.service.buildMaterialization(
            "my-env",
            "pkg",
            "mat-1",
            [makeInstruction()],
         );
         expect(result.id).toBe("mat-1");
      });
   });

   describe("stopMaterialization", () => {
      const cancellable = [
         "PENDING",
         "BUILD_PLAN_READY",
         "MANIFEST_ROWS_READY",
      ] as const;

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

      const active = ["PENDING", "BUILD_PLAN_READY", "MANIFEST_ROWS_READY"] as const;
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
   });
});

describe("stagingSuffix", () => {
   it("derives a short, stable suffix from the buildId", () => {
      expect(stagingSuffix("abcdef1234567890")).toBe("_abcdef123456");
   });
});
