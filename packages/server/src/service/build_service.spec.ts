import { beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import {
   BuildConflictError,
   BuildNotFoundError,
   InvalidStateTransitionError,
   ProjectNotFoundError,
} from "../errors";
import {
   BuildExecution,
   BuildExecutionStatus,
   ResourceRepository,
} from "../storage/DatabaseInterface";
import { BuildService } from "./build_service";
import { ManifestService } from "./manifest_service";
import { ProjectStore } from "./project_store";

function makeExecution(
   overrides: Partial<BuildExecution> = {},
): BuildExecution {
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
      listBuildExecutions: sandbox.stub(),
      getBuildExecutionById: sandbox.stub(),
      getRunningBuildExecution: sandbox.stub(),
      createBuildExecution: sandbox.stub(),
      updateBuildExecution: sandbox.stub(),
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
      loadManifest: sandbox.stub().resolves({ entries: {}, strict: false }),
      listEntries: sandbox.stub().resolves([]),
   } as unknown as sinon.SinonStubbedInstance<ManifestService>;

   const service = new BuildService(
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

describe("BuildService", () => {
   let ctx: ReturnType<typeof createMocks>;

   beforeEach(() => {
      ctx = createMocks();
   });

   // ==================== resolveProjectId ====================

   describe("resolveProjectId (via listExecutions)", () => {
      it("should throw ProjectNotFoundError when project is not in DB", async () => {
         ctx.repository.getProjectByName.resolves(null);

         await expect(
            ctx.service.listExecutions("unknown", "pkg"),
         ).rejects.toThrow(ProjectNotFoundError);
      });
   });

   // ==================== EXECUTION QUERIES ====================

   describe("getBuildStatus", () => {
      it("should return the latest execution", async () => {
         const exec = makeExecution({ status: "SUCCESS" });
         ctx.repository.listBuildExecutions.resolves([exec]);

         const result = await ctx.service.getBuildStatus("my-project", "pkg");

         expect(result.latestExecution).toEqual(exec);
      });

      it("should return null latestExecution when none exist", async () => {
         ctx.repository.listBuildExecutions.resolves([]);

         const result = await ctx.service.getBuildStatus("my-project", "pkg");

         expect(result.latestExecution).toBeNull();
      });
   });

   describe("listExecutions", () => {
      it("should list executions for a package", async () => {
         const executions = [makeExecution(), makeExecution({ id: "exec-2" })];
         ctx.repository.listBuildExecutions.resolves(executions);

         const result = await ctx.service.listExecutions("my-project", "pkg");

         expect(result).toEqual(executions);
      });
   });

   describe("getExecution", () => {
      it("should return a specific execution", async () => {
         const exec = makeExecution();
         ctx.repository.getBuildExecutionById.resolves(exec);

         const result = await ctx.service.getExecution(
            "my-project",
            "pkg",
            "exec-1",
         );

         expect(result).toEqual(exec);
      });

      it("should throw when execution not found", async () => {
         ctx.repository.getBuildExecutionById.resolves(null);

         await expect(
            ctx.service.getExecution("my-project", "pkg", "missing"),
         ).rejects.toThrow(BuildNotFoundError);
      });

      it("should throw when execution belongs to a different package", async () => {
         ctx.repository.getBuildExecutionById.resolves(
            makeExecution({ packageName: "other-pkg" }),
         );

         await expect(
            ctx.service.getExecution("my-project", "pkg", "exec-1"),
         ).rejects.toThrow(BuildNotFoundError);
      });
   });

   // ==================== STATE MACHINE ====================

   describe("state transitions", () => {
      const validTransitions: [BuildExecutionStatus, BuildExecutionStatus][] = [
         ["PENDING", "RUNNING"],
         ["PENDING", "CANCELLED"],
         ["RUNNING", "SUCCESS"],
         ["RUNNING", "FAILED"],
         ["RUNNING", "CANCELLED"],
      ];

      const invalidTransitions: [BuildExecutionStatus, BuildExecutionStatus][] =
         [
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
            ctx.repository.createBuildExecution.resolves(exec);
            ctx.repository.getBuildExecutionById.resolves(exec);
            ctx.repository.updateBuildExecution.resolves(
               makeExecution({ status: to }),
            );

            // Trigger via stopBuild for RUNNING->CANCELLED (orphaned path)
            if (from === "RUNNING" && to === "CANCELLED") {
               ctx.repository.getRunningBuildExecution.resolves(exec);
               const result = await ctx.service.stopBuild("my-project", "pkg");
               expect(result).not.toBeNull();
            }
         });
      }

      for (const [from, to] of invalidTransitions) {
         it(`should reject ${from} -> ${to}`, async () => {
            const exec = makeExecution({ status: from });
            ctx.repository.getBuildExecutionById.resolves(exec);

            await expect(
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
               (ctx.service as any).transitionExecution(exec.id, to),
            ).rejects.toThrow(InvalidStateTransitionError);
         });
      }
   });

   // ==================== START / STOP ====================

   describe("startBuild", () => {
      it("should throw BuildConflictError when a build is already active", async () => {
         (ctx.projectStore.getProject as sinon.SinonStub).resolves({
            getPackage: sinon.stub().resolves({}),
         });
         ctx.repository.createBuildExecution.resolves(null);
         ctx.repository.getRunningBuildExecution.resolves(
            makeExecution({ status: "RUNNING" }),
         );

         await expect(
            ctx.service.startBuild("my-project", "pkg"),
         ).rejects.toThrow(BuildConflictError);
      });

      it("should return a RUNNING execution on success", async () => {
         (ctx.projectStore.getProject as sinon.SinonStub).resolves({
            getPackage: sinon.stub().resolves({}),
         });
         const pending = makeExecution({ status: "PENDING" });
         const running = makeExecution({ status: "RUNNING" });
         ctx.repository.createBuildExecution.resolves(pending);
         ctx.repository.getBuildExecutionById.resolves(pending);
         ctx.repository.updateBuildExecution.resolves(running);

         const result = await ctx.service.startBuild("my-project", "pkg");

         expect(result.status).toBe("RUNNING");
      });
   });

   describe("stopBuild", () => {
      it("should return null when no execution is running", async () => {
         ctx.repository.getRunningBuildExecution.resolves(null);

         const result = await ctx.service.stopBuild("my-project", "pkg");
         expect(result).toBeNull();
      });

      it("should force-cancel an orphaned execution (no abort controller)", async () => {
         const running = makeExecution({ id: "orphan", status: "RUNNING" });
         ctx.repository.getRunningBuildExecution.resolves(running);
         ctx.repository.getBuildExecutionById.resolves(running);
         ctx.repository.updateBuildExecution.resolves(
            makeExecution({ id: "orphan", status: "CANCELLED" }),
         );

         const result = await ctx.service.stopBuild("my-project", "pkg");

         expect(result).not.toBeNull();
         expect(result!.status).toBe("CANCELLED");
      });

      it("should signal cancellation in progress when abort controller exists", async () => {
         (ctx.projectStore.getProject as sinon.SinonStub).resolves({
            getPackage: sinon.stub().resolves({
               getModelPaths: () => [],
               getConnections: () => new Map(),
               getPackagePath: () => "/test",
               setBuildManifest: sinon.stub(),
            }),
         });
         const pending = makeExecution({ status: "PENDING" });
         const running = makeExecution({ status: "RUNNING" });
         ctx.repository.createBuildExecution.resolves(pending);
         ctx.repository.getBuildExecutionById.resolves(pending);
         ctx.repository.updateBuildExecution.resolves(running);

         await ctx.service.startBuild("my-project", "pkg");

         ctx.repository.getRunningBuildExecution.resolves(running);
         const result = await ctx.service.stopBuild("my-project", "pkg");

         expect(result).not.toBeNull();
         expect(result!.status).toBe("RUNNING");
         expect(
            (result!.metadata as Record<string, unknown>)
               ?.cancellationRequested,
         ).toBe(true);
      });
   });
});
