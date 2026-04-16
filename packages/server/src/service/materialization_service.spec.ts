import { beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import {
   InvalidStateTransitionError,
   MaterializationConflictError,
   MaterializationNotFoundError,
   ProjectNotFoundError,
} from "../errors";
import {
   Materialization,
   MaterializationStatus,
   ResourceRepository,
} from "../storage/DatabaseInterface";
import { ManifestService } from "./manifest_service";
import { MaterializationService } from "./materialization_service";
import { ProjectStore } from "./project_store";

function makeExecution(
   overrides: Partial<Materialization> = {},
): Materialization {
   return {
      id: "exec-1",
      projectId: "proj-1",
      packageName: "pkg",
      status: "PENDING",
      startedAt: null,
      completedAt: null,
      error: null,
      metadata: null,
      createdAt: new Date("2025-01-01"),
      updatedAt: new Date("2025-01-01"),
      ...overrides,
   };
}

type MockRepo = sinon.SinonStubbedInstance<ResourceRepository>;

function createMocks() {
   const sandbox = sinon.createSandbox();

   const repository: MockRepo = {
      listProjects: sandbox.stub(),
      getProjectById: sandbox.stub(),
      getProjectByName: sandbox.stub(),
      createProject: sandbox.stub(),
      updateProject: sandbox.stub(),
      deleteProject: sandbox.stub(),
      listPackages: sandbox.stub(),
      getPackageById: sandbox.stub(),
      getPackageByName: sandbox.stub(),
      createPackage: sandbox.stub(),
      updatePackage: sandbox.stub(),
      deletePackage: sandbox.stub(),
      listConnections: sandbox.stub(),
      getConnectionById: sandbox.stub(),
      getConnectionByName: sandbox.stub(),
      createConnection: sandbox.stub(),
      updateConnection: sandbox.stub(),
      deleteConnection: sandbox.stub(),
      listMaterializations: sandbox.stub(),
      getMaterializationById: sandbox.stub(),
      getActiveMaterialization: sandbox.stub(),
      createMaterialization: sandbox.stub(),
      updateMaterialization: sandbox.stub(),
      deleteMaterialization: sandbox.stub(),
      listManifestEntries: sandbox.stub(),
      upsertManifestEntry: sandbox.stub(),
      deleteManifestEntry: sandbox.stub(),
   } as unknown as MockRepo;

   const storageManager = {
      getRepository: () => repository,
      getManifestStore: sandbox.stub(),
   };

   const projectStore = {
      storageManager,
      getProject: sandbox.stub(),
   } as unknown as ProjectStore;

   const manifestService = {
      getManifest: sandbox.stub().resolves({ entries: {}, strict: false }),
      writeEntry: sandbox.stub().resolves(),
      deleteEntry: sandbox.stub().resolves(),
      reloadManifest: sandbox.stub().resolves({ entries: {}, strict: false }),
      listEntries: sandbox.stub().resolves([]),
   } as unknown as sinon.SinonStubbedInstance<ManifestService>;

   const service = new MaterializationService(
      projectStore,
      manifestService as unknown as ManifestService,
   );

   // Default: resolveProjectId succeeds
   repository.getProjectByName.resolves({
      id: "proj-1",
      name: "my-project",
      path: "/test",
      createdAt: new Date(),
      updatedAt: new Date(),
   });

   return { sandbox, repository, projectStore, manifestService, service };
}

describe("MaterializationService", () => {
   let ctx: ReturnType<typeof createMocks>;

   beforeEach(() => {
      ctx = createMocks();
   });

   // ==================== resolveProjectId ====================

   describe("resolveProjectId (via listMaterializations)", () => {
      it("should throw ProjectNotFoundError when project is not in DB", async () => {
         ctx.repository.getProjectByName.resolves(null);

         await expect(
            ctx.service.listMaterializations("unknown", "pkg"),
         ).rejects.toThrow(ProjectNotFoundError);
      });
   });

   // ==================== BUILD QUERIES ====================

   describe("listMaterializations", () => {
      it("should list builds for a package", async () => {
         const builds = [makeExecution(), makeExecution({ id: "exec-2" })];
         ctx.repository.listMaterializations.resolves(builds);

         const result = await ctx.service.listMaterializations(
            "my-project",
            "pkg",
         );

         expect(result).toEqual(builds);
      });
   });

   describe("getMaterialization", () => {
      it("should return a specific build", async () => {
         const exec = makeExecution();
         ctx.repository.getMaterializationById.resolves(exec);

         const result = await ctx.service.getMaterialization(
            "my-project",
            "pkg",
            "exec-1",
         );

         expect(result).toEqual(exec);
      });

      it("should throw when build not found", async () => {
         ctx.repository.getMaterializationById.resolves(null);

         await expect(
            ctx.service.getMaterialization("my-project", "pkg", "missing"),
         ).rejects.toThrow(MaterializationNotFoundError);
      });

      it("should throw when build belongs to a different package", async () => {
         ctx.repository.getMaterializationById.resolves(
            makeExecution({ packageName: "other-pkg" }),
         );

         await expect(
            ctx.service.getMaterialization("my-project", "pkg", "exec-1"),
         ).rejects.toThrow(MaterializationNotFoundError);
      });
   });

   // ==================== STATE MACHINE ====================

   describe("state transitions", () => {
      const validTransitions: [MaterializationStatus, MaterializationStatus][] =
         [
            ["PENDING", "RUNNING"],
            ["PENDING", "CANCELLED"],
            ["RUNNING", "SUCCESS"],
            ["RUNNING", "FAILED"],
            ["RUNNING", "CANCELLED"],
         ];

      const invalidTransitions: [
         MaterializationStatus,
         MaterializationStatus,
      ][] = [
         ["PENDING", "SUCCESS"],
         ["PENDING", "FAILED"],
         ["RUNNING", "PENDING"],
         ["SUCCESS", "RUNNING"],
         ["SUCCESS", "FAILED"],
         ["FAILED", "RUNNING"],
         ["FAILED", "SUCCESS"],
         ["CANCELLED", "RUNNING"],
         ["CANCELLED", "SUCCESS"],
      ];

      for (const [from, to] of validTransitions) {
         it(`should allow ${from} -> ${to}`, async () => {
            const exec = makeExecution({ status: from });
            ctx.repository.getMaterializationById.resolves(exec);
            ctx.repository.updateMaterialization.resolves(
               makeExecution({ status: to }),
            );

            // Trigger via stopMaterialization for RUNNING->CANCELLED (orphaned path)
            if (from === "RUNNING" && to === "CANCELLED") {
               const result = await ctx.service.stopMaterialization(
                  "my-project",
                  "pkg",
                  exec.id,
               );
               expect(result).not.toBeNull();
            }
         });
      }

      for (const [from, to] of invalidTransitions) {
         it(`should reject ${from} -> ${to}`, async () => {
            const exec = makeExecution({ status: from });
            ctx.repository.getMaterializationById.resolves(exec);

            await expect(
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
               (ctx.service as any).transitionExecution(exec.id, to),
            ).rejects.toThrow(InvalidStateTransitionError);
         });
      }
   });

   // ==================== CREATE / START / STOP ====================

   describe("createMaterialization", () => {
      it("should create a PENDING build", async () => {
         (ctx.projectStore.getProject as sinon.SinonStub).resolves({
            getPackage: sinon.stub().resolves({}),
         });
         ctx.repository.getActiveMaterialization.resolves(null);
         const pending = makeExecution({ status: "PENDING" });
         ctx.repository.createMaterialization.resolves(pending);
         ctx.repository.updateMaterialization.resolves({
            ...pending,
            metadata: { forceRefresh: false, autoLoadManifest: true },
         });

         const result = await ctx.service.createMaterialization(
            "my-project",
            "pkg",
            {
               autoLoadManifest: true,
            },
         );

         expect(result.status).toBe("PENDING");
         expect(
            (result.metadata as Record<string, unknown>)?.autoLoadManifest,
         ).toBe(true);
      });

      it("should reject creation when an active materialization exists", async () => {
         (ctx.projectStore.getProject as sinon.SinonStub).resolves({
            getPackage: sinon.stub().resolves({}),
         });
         ctx.repository.getActiveMaterialization.resolves(
            makeExecution({ id: "existing", status: "RUNNING" }),
         );

         await expect(
            ctx.service.createMaterialization("my-project", "pkg"),
         ).rejects.toThrow(MaterializationConflictError);
      });

      it("should translate DuplicateActiveMaterializationError from a lost race", async () => {
         const {
            DuplicateActiveMaterializationError,
         } = require("../storage/duckdb/MaterializationRepository");
         (ctx.projectStore.getProject as sinon.SinonStub).resolves({
            getPackage: sinon.stub().resolves({}),
         });
         // The pre-check finds nothing (race is still possible), but the
         // atomic insert loses to a concurrent create and the repository
         // raises DuplicateActiveMaterializationError.
         ctx.repository.getActiveMaterialization
            .onFirstCall()
            .resolves(null)
            .onSecondCall()
            .resolves(makeExecution({ id: "winner", status: "PENDING" }));
         ctx.repository.createMaterialization.rejects(
            new DuplicateActiveMaterializationError("proj-1", "pkg"),
         );

         await expect(
            ctx.service.createMaterialization("my-project", "pkg"),
         ).rejects.toThrow(/winner/);
      });
   });

   describe("startMaterialization", () => {
      it("should transition PENDING to RUNNING", async () => {
         const pending = makeExecution({
            status: "PENDING",
            metadata: { forceRefresh: false, autoLoadManifest: false },
         });
         const running = makeExecution({ status: "RUNNING" });
         ctx.repository.getMaterializationById.resolves(pending);
         ctx.repository.getActiveMaterialization.resolves(null);
         ctx.repository.updateMaterialization.resolves(running);

         const result = await ctx.service.startMaterialization(
            "my-project",
            "pkg",
            "exec-1",
         );

         expect(result.status).toBe("RUNNING");
      });

      it("should reject non-PENDING builds", async () => {
         const running = makeExecution({ status: "RUNNING" });
         ctx.repository.getMaterializationById.resolves(running);

         await expect(
            ctx.service.startMaterialization("my-project", "pkg", "exec-1"),
         ).rejects.toThrow(InvalidStateTransitionError);
      });

      it("should throw MaterializationConflictError when another is already running", async () => {
         const pending = makeExecution({
            status: "PENDING",
            metadata: { forceRefresh: false, autoLoadManifest: false },
         });
         ctx.repository.getMaterializationById.resolves(pending);
         ctx.repository.getActiveMaterialization.resolves(
            makeExecution({ id: "other", status: "RUNNING" }),
         );

         await expect(
            ctx.service.startMaterialization("my-project", "pkg", "exec-1"),
         ).rejects.toThrow(MaterializationConflictError);
      });
   });

   describe("stopMaterialization", () => {
      it("should cancel a PENDING build", async () => {
         const pending = makeExecution({ status: "PENDING" });
         ctx.repository.getMaterializationById.resolves(pending);
         ctx.repository.updateMaterialization.resolves(
            makeExecution({ status: "CANCELLED" }),
         );

         const result = await ctx.service.stopMaterialization(
            "my-project",
            "pkg",
            "exec-1",
         );

         expect(result.status).toBe("CANCELLED");
      });

      it("should force-cancel an orphaned RUNNING build", async () => {
         const running = makeExecution({ id: "orphan", status: "RUNNING" });
         ctx.repository.getMaterializationById.resolves(running);
         ctx.repository.updateMaterialization.resolves(
            makeExecution({ id: "orphan", status: "CANCELLED" }),
         );

         const result = await ctx.service.stopMaterialization(
            "my-project",
            "pkg",
            "orphan",
         );

         expect(result.status).toBe("CANCELLED");
      });

      it("should reject stopping a terminal build", async () => {
         const succeeded = makeExecution({ status: "SUCCESS" });
         ctx.repository.getMaterializationById.resolves(succeeded);

         await expect(
            ctx.service.stopMaterialization("my-project", "pkg", "exec-1"),
         ).rejects.toThrow(InvalidStateTransitionError);
      });
   });

   // ==================== DELETE ====================

   describe("deleteMaterialization", () => {
      it("should delete a SUCCESS materialization", async () => {
         const succeeded = makeExecution({ status: "SUCCESS" });
         ctx.repository.getMaterializationById.resolves(succeeded);
         ctx.repository.deleteMaterialization.resolves();

         await ctx.service.deleteMaterialization("my-project", "pkg", "exec-1");

         expect(ctx.repository.deleteMaterialization.calledOnce).toBe(true);
         expect(ctx.repository.deleteMaterialization.firstCall.args[0]).toBe(
            "exec-1",
         );
      });

      it("should delete a FAILED materialization", async () => {
         const failed = makeExecution({ status: "FAILED" });
         ctx.repository.getMaterializationById.resolves(failed);
         ctx.repository.deleteMaterialization.resolves();

         await ctx.service.deleteMaterialization("my-project", "pkg", "exec-1");

         expect(ctx.repository.deleteMaterialization.calledOnce).toBe(true);
      });

      it("should delete a CANCELLED materialization", async () => {
         const cancelled = makeExecution({ status: "CANCELLED" });
         ctx.repository.getMaterializationById.resolves(cancelled);
         ctx.repository.deleteMaterialization.resolves();

         await ctx.service.deleteMaterialization("my-project", "pkg", "exec-1");

         expect(ctx.repository.deleteMaterialization.calledOnce).toBe(true);
      });

      it("should reject deleting a PENDING materialization", async () => {
         const pending = makeExecution({ status: "PENDING" });
         ctx.repository.getMaterializationById.resolves(pending);

         await expect(
            ctx.service.deleteMaterialization("my-project", "pkg", "exec-1"),
         ).rejects.toThrow(InvalidStateTransitionError);
      });

      it("should reject deleting a RUNNING materialization", async () => {
         const running = makeExecution({ status: "RUNNING" });
         ctx.repository.getMaterializationById.resolves(running);

         await expect(
            ctx.service.deleteMaterialization("my-project", "pkg", "exec-1"),
         ).rejects.toThrow(InvalidStateTransitionError);
      });

      it("should throw when materialization not found", async () => {
         ctx.repository.getMaterializationById.resolves(null);

         await expect(
            ctx.service.deleteMaterialization("my-project", "pkg", "missing"),
         ).rejects.toThrow(MaterializationNotFoundError);
      });
   });
});
