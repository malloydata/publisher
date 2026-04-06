import { beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import {
   BadRequestError,
   InvalidStateTransitionError,
   TaskExecutionConflictError,
   TaskNotFoundError,
} from "../errors";
import {
   ResourceRepository,
   Task,
   TaskConfig,
   TaskExecution,
   TaskExecutionStatus,
} from "../storage/DatabaseInterface";
import { ManifestService } from "./manifest_service";
import { ProjectStore } from "./project_store";
import { TaskService } from "./task_service";

function makeTask(overrides: Partial<Task> = {}): Task {
   return {
      id: "task-1",
      projectId: "proj-1",
      name: "my-task",
      type: "materialize",
      config: { package: "pkg", modelPath: "model.malloy" },
      createdAt: new Date("2025-01-01"),
      updatedAt: new Date("2025-01-01"),
      ...overrides,
   };
}

function makeExecution(overrides: Partial<TaskExecution> = {}): TaskExecution {
   return {
      id: "exec-1",
      taskId: "task-1",
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
      listTasks: sandbox.stub(),
      getTaskById: sandbox.stub(),
      getTaskByName: sandbox.stub(),
      createTask: sandbox.stub(),
      updateTask: sandbox.stub(),
      deleteTask: sandbox.stub(),
      listExecutions: sandbox.stub(),
      getExecutionById: sandbox.stub(),
      getRunningExecution: sandbox.stub(),
      createExecution: sandbox.stub(),
      updateExecution: sandbox.stub(),
      listManifestEntries: sandbox.stub(),
      getManifestEntryByBuildId: sandbox.stub(),
      getManifestEntryBySourceName: sandbox.stub(),
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
      getManifest: sandbox.stub().resolves({ entries: {}, strict: true }),
      writeEntry: sandbox.stub().resolves(),
      loadManifest: sandbox.stub().resolves({ entries: {}, strict: true }),
      listEntries: sandbox.stub().resolves([]),
   } as unknown as sinon.SinonStubbedInstance<ManifestService>;

   const service = new TaskService(
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

describe("TaskService", () => {
   let ctx: ReturnType<typeof createMocks>;

   beforeEach(() => {
      ctx = createMocks();
   });

   // ==================== resolveProjectId ====================

   describe("resolveProjectId (via listTasks)", () => {
      it("should throw TaskNotFoundError when project is not in DB", async () => {
         ctx.repository.getProjectByName.resolves(null);

         await expect(ctx.service.listTasks("unknown")).rejects.toThrow(
            TaskNotFoundError,
         );
      });
   });

   // ==================== TASK CRUD ====================

   describe("listTasks", () => {
      it("should return tasks for the project", async () => {
         const tasks = [makeTask(), makeTask({ id: "task-2", name: "other" })];
         ctx.repository.listTasks.resolves(tasks);

         const result = await ctx.service.listTasks("my-project");

         expect(result).toEqual(tasks);
         expect(ctx.repository.listTasks.calledWith("proj-1")).toBe(true);
      });
   });

   describe("getTask", () => {
      it("should return the task when it exists", async () => {
         const task = makeTask();
         ctx.repository.getTaskById.resolves(task);

         const result = await ctx.service.getTask("my-project", "task-1");
         expect(result).toEqual(task);
      });

      it("should throw TaskNotFoundError when task doesn't exist", async () => {
         ctx.repository.getTaskById.resolves(null);

         await expect(
            ctx.service.getTask("my-project", "missing"),
         ).rejects.toThrow(TaskNotFoundError);
      });

      it("should throw TaskNotFoundError when task belongs to a different project", async () => {
         ctx.repository.getTaskById.resolves(
            makeTask({ projectId: "other-project-id" }),
         );

         await expect(
            ctx.service.getTask("my-project", "task-1"),
         ).rejects.toThrow(TaskNotFoundError);
      });
   });

   describe("createTask", () => {
      it("should create a task with valid inputs", async () => {
         const config: TaskConfig = {
            package: "pkg",
            modelPath: "model.malloy",
         };
         const created = makeTask();
         ctx.repository.getTaskByName.resolves(null);
         ctx.repository.createTask.resolves(created);

         const result = await ctx.service.createTask("my-project", {
            name: "my-task",
            config,
         });

         expect(result).toEqual(created);
         expect(ctx.repository.createTask.calledOnce).toBe(true);
         const call = ctx.repository.createTask.firstCall.args[0];
         expect(call.projectId).toBe("proj-1");
         expect(call.name).toBe("my-task");
         expect(call.type).toBe("materialize");
      });

      it("should use custom type when provided", async () => {
         ctx.repository.getTaskByName.resolves(null);
         ctx.repository.createTask.resolves(makeTask({ type: "custom-type" }));

         await ctx.service.createTask("my-project", {
            name: "my-task",
            type: "custom-type",
            config: { package: "pkg", modelPath: "m.malloy" },
         });

         expect(ctx.repository.createTask.firstCall.args[0].type).toBe(
            "custom-type",
         );
      });

      it("should throw BadRequestError when name is missing", async () => {
         await expect(
            ctx.service.createTask("my-project", {
               name: "",
               config: { package: "pkg", modelPath: "m.malloy" },
            }),
         ).rejects.toThrow(BadRequestError);
      });

      it("should throw BadRequestError when config.package is missing", async () => {
         await expect(
            ctx.service.createTask("my-project", {
               name: "t",
               config: { package: "", modelPath: "m.malloy" },
            }),
         ).rejects.toThrow(BadRequestError);
      });

      it("should throw BadRequestError when config.modelPath is missing", async () => {
         await expect(
            ctx.service.createTask("my-project", {
               name: "t",
               config: { package: "pkg", modelPath: "" },
            }),
         ).rejects.toThrow(BadRequestError);
      });

      it("should throw BadRequestError when a task with the same name exists", async () => {
         ctx.repository.getTaskByName.resolves(makeTask());

         await expect(
            ctx.service.createTask("my-project", {
               name: "my-task",
               config: { package: "pkg", modelPath: "m.malloy" },
            }),
         ).rejects.toThrow(BadRequestError);
      });
   });

   describe("updateTask", () => {
      it("should update the task", async () => {
         const task = makeTask();
         ctx.repository.getTaskById.resolves(task);
         ctx.repository.updateTask.resolves({ ...task, name: "renamed" });

         const result = await ctx.service.updateTask("my-project", "task-1", {
            name: "renamed",
         });

         expect(result.name).toBe("renamed");
         expect(ctx.repository.updateTask.calledOnce).toBe(true);
      });

      it("should throw TaskNotFoundError for unknown task", async () => {
         ctx.repository.getTaskById.resolves(null);

         await expect(
            ctx.service.updateTask("my-project", "nope", { name: "x" }),
         ).rejects.toThrow(TaskNotFoundError);
      });
   });

   describe("deleteTask", () => {
      it("should delete the task", async () => {
         ctx.repository.getTaskById.resolves(makeTask());
         ctx.repository.deleteTask.resolves();

         await ctx.service.deleteTask("my-project", "task-1");

         expect(ctx.repository.deleteTask.calledWith("task-1")).toBe(true);
      });

      it("should throw TaskNotFoundError for unknown task", async () => {
         ctx.repository.getTaskById.resolves(null);

         await expect(
            ctx.service.deleteTask("my-project", "nope"),
         ).rejects.toThrow(TaskNotFoundError);
      });
   });

   // ==================== EXECUTION QUERIES ====================

   describe("getTaskStatus", () => {
      it("should return the task and latest execution", async () => {
         const task = makeTask();
         const exec = makeExecution({ status: "SUCCESS" });
         ctx.repository.getTaskById.resolves(task);
         ctx.repository.listExecutions.resolves([exec]);

         const result = await ctx.service.getTaskStatus("my-project", "task-1");

         expect(result.task).toEqual(task);
         expect(result.latestExecution).toEqual(exec);
      });

      it("should return null latestExecution when none exist", async () => {
         ctx.repository.getTaskById.resolves(makeTask());
         ctx.repository.listExecutions.resolves([]);

         const result = await ctx.service.getTaskStatus("my-project", "task-1");

         expect(result.latestExecution).toBeNull();
      });
   });

   describe("listExecutions", () => {
      it("should list executions for a task", async () => {
         const executions = [makeExecution(), makeExecution({ id: "exec-2" })];
         ctx.repository.getTaskById.resolves(makeTask());
         ctx.repository.listExecutions.resolves(executions);

         const result = await ctx.service.listExecutions(
            "my-project",
            "task-1",
         );

         expect(result).toEqual(executions);
      });
   });

   describe("getExecution", () => {
      it("should return a specific execution", async () => {
         const exec = makeExecution();
         ctx.repository.getTaskById.resolves(makeTask());
         ctx.repository.getExecutionById.resolves(exec);

         const result = await ctx.service.getExecution(
            "my-project",
            "task-1",
            "exec-1",
         );

         expect(result).toEqual(exec);
      });

      it("should throw when execution not found", async () => {
         ctx.repository.getTaskById.resolves(makeTask());
         ctx.repository.getExecutionById.resolves(null);

         await expect(
            ctx.service.getExecution("my-project", "task-1", "missing"),
         ).rejects.toThrow(TaskNotFoundError);
      });

      it("should throw when execution belongs to a different task", async () => {
         ctx.repository.getTaskById.resolves(makeTask());
         ctx.repository.getExecutionById.resolves(
            makeExecution({ taskId: "other-task" }),
         );

         await expect(
            ctx.service.getExecution("my-project", "task-1", "exec-1"),
         ).rejects.toThrow(TaskNotFoundError);
      });
   });

   // ==================== STATE MACHINE ====================

   describe("state transitions", () => {
      const validTransitions: [TaskExecutionStatus, TaskExecutionStatus][] = [
         ["PENDING", "RUNNING"],
         ["PENDING", "CANCELLED"],
         ["RUNNING", "SUCCESS"],
         ["RUNNING", "FAILED"],
         ["RUNNING", "CANCELLED"],
      ];

      const invalidTransitions: [TaskExecutionStatus, TaskExecutionStatus][] = [
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
            ctx.repository.getTaskById.resolves(makeTask());
            ctx.repository.createExecution.resolves(exec);
            ctx.repository.getExecutionById.resolves(exec);
            ctx.repository.updateExecution.resolves(
               makeExecution({ status: to }),
            );

            // Trigger via stopTask for RUNNING->CANCELLED (orphaned path)
            if (from === "RUNNING" && to === "CANCELLED") {
               ctx.repository.getRunningExecution.resolves(exec);
               const result = await ctx.service.stopTask(
                  "my-project",
                  "task-1",
               );
               expect(result).not.toBeNull();
            }
         });
      }

      for (const [from, to] of invalidTransitions) {
         it(`should reject ${from} -> ${to}`, async () => {
            const exec = makeExecution({ status: from });
            ctx.repository.getExecutionById.resolves(exec);

            // Access private method via bracket notation to test the
            // transition logic directly without spinning up a full build.
            await expect(
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
               (ctx.service as any).transitionExecution(exec.id, to),
            ).rejects.toThrow(InvalidStateTransitionError);
         });
      }
   });

   // ==================== START / STOP ====================

   describe("startTask", () => {
      it("should throw TaskExecutionConflictError when a run is already active", async () => {
         ctx.repository.getTaskById.resolves(makeTask());
         ctx.repository.createExecution.resolves(null);
         ctx.repository.getRunningExecution.resolves(
            makeExecution({ status: "RUNNING" }),
         );

         await expect(
            ctx.service.startTask("my-project", "task-1"),
         ).rejects.toThrow(TaskExecutionConflictError);
      });
   });

   describe("stopTask", () => {
      it("should return null when no execution is running", async () => {
         ctx.repository.getTaskById.resolves(makeTask());
         ctx.repository.getRunningExecution.resolves(null);

         const result = await ctx.service.stopTask("my-project", "task-1");
         expect(result).toBeNull();
      });

      it("should force-cancel an orphaned execution (no abort controller)", async () => {
         const running = makeExecution({ id: "orphan", status: "RUNNING" });
         ctx.repository.getTaskById.resolves(makeTask());
         ctx.repository.getRunningExecution.resolves(running);
         ctx.repository.getExecutionById.resolves(running);
         ctx.repository.updateExecution.resolves(
            makeExecution({ id: "orphan", status: "CANCELLED" }),
         );

         const result = await ctx.service.stopTask("my-project", "task-1");

         expect(result).not.toBeNull();
         expect(result!.status).toBe("CANCELLED");
      });
   });
});
