import { Manifest } from "@malloydata/malloy";
import {
   BadRequestError,
   InvalidStateTransitionError,
   TaskExecutionConflictError,
   TaskNotFoundError,
} from "../errors";
import { logger } from "../logger";
import {
   ResourceRepository,
   Task,
   TaskConfig,
   TaskExecution,
   TaskExecutionStatus,
} from "../storage/DatabaseInterface";
import { ManifestService } from "./manifest_service";
import { ProjectStore } from "./project_store";

/**
 * Allowed execution status transitions. SUCCESS, FAILED, and CANCELLED are
 * terminal — once an execution reaches one of these states it is immutable.
 */
const VALID_TRANSITIONS: Record<TaskExecutionStatus, TaskExecutionStatus[]> = {
   PENDING: ["RUNNING", "CANCELLED"],
   RUNNING: ["SUCCESS", "FAILED", "CANCELLED"],
   SUCCESS: [],
   FAILED: [],
   CANCELLED: [],
};

/**
 * Orchestrates materialization task lifecycle: CRUD, execution scheduling,
 * cancellation, and the actual Malloy build that materializes sources into
 * database tables. Each task targets a specific model within a package and
 * can optionally filter to a subset of persist sources.
 *
 * Enforces at-most-one concurrent execution per task via the repository's
 * atomic `createExecution`, and supports cooperative cancellation through
 * `AbortController`.
 */
export class TaskService {
   /**
    * Tracks in-flight executions so they can be cancelled. This map only
    * lives in-process memory — entries are lost on server restart, which is
    * why `stopTask` has an orphaned-execution fallback path.
    */
   private runningAbortControllers = new Map<string, AbortController>();

   constructor(
      private projectStore: ProjectStore,
      private manifestService: ManifestService,
   ) {}

   private get repository(): ResourceRepository {
      return this.projectStore.storageManager.getRepository();
   }

   // ==================== STATE MACHINE ====================

   private validateTransition(
      current: TaskExecutionStatus,
      next: TaskExecutionStatus,
   ): void {
      const allowed = VALID_TRANSITIONS[current];
      if (!allowed.includes(next)) {
         throw new InvalidStateTransitionError(
            `Cannot transition from ${current} to ${next}`,
         );
      }
   }

   /** Validates the transition, then atomically updates the execution row. */
   private async transitionExecution(
      executionId: string,
      newStatus: TaskExecutionStatus,
      extra?: {
         startedAt?: Date;
         completedAt?: Date;
         error?: string | null;
         metadata?: Record<string, unknown> | null;
      },
   ): Promise<TaskExecution> {
      const execution = await this.repository.getExecutionById(executionId);
      if (!execution) {
         throw new TaskNotFoundError(`Execution ${executionId} not found`);
      }
      this.validateTransition(execution.status, newStatus);
      return this.repository.updateExecution(executionId, {
         status: newStatus,
         ...extra,
      });
   }

   // ==================== TASK CRUD ====================

   async listTasks(projectName: string): Promise<Task[]> {
      const projectId = await this.resolveProjectId(projectName);
      return this.repository.listTasks(projectId);
   }

   async getTask(projectName: string, taskId: string): Promise<Task> {
      const projectId = await this.resolveProjectId(projectName);
      return this.resolveTask(projectId, taskId);
   }

   private async resolveTask(projectId: string, taskId: string): Promise<Task> {
      const task = await this.repository.getTaskById(taskId);
      if (!task || task.projectId !== projectId) {
         throw new TaskNotFoundError(`Task ${taskId} not found`);
      }
      return task;
   }

   async createTask(
      projectName: string,
      body: { name: string; type?: string; config: TaskConfig },
   ): Promise<Task> {
      const projectId = await this.resolveProjectId(projectName);

      if (!body.name) {
         throw new BadRequestError("Task name is required");
      }
      if (!body.config?.package || !body.config?.modelPath) {
         throw new BadRequestError(
            "Task config must include package and modelPath",
         );
      }

      const existing = await this.repository.getTaskByName(
         projectId,
         body.name,
      );
      if (existing) {
         throw new BadRequestError(
            `Task with name '${body.name}' already exists in this project`,
         );
      }

      return this.repository.createTask({
         projectId,
         name: body.name,
         type: body.type || "materialize",
         config: body.config,
      });
   }

   async updateTask(
      projectName: string,
      taskId: string,
      body: Partial<{ name: string; type: string; config: TaskConfig }>,
   ): Promise<Task> {
      const task = await this.getTask(projectName, taskId);

      const updates: Partial<Task> = {};
      if (body.name !== undefined) updates.name = body.name;
      if (body.type !== undefined) updates.type = body.type;
      if (body.config !== undefined) updates.config = body.config;

      return this.repository.updateTask(task.id, updates);
   }

   async deleteTask(projectName: string, taskId: string): Promise<void> {
      await this.getTask(projectName, taskId);
      await this.repository.deleteTask(taskId);
   }

   // ==================== EXECUTION QUERIES ====================

   async getTaskStatus(
      projectName: string,
      taskId: string,
   ): Promise<{ task: Task; latestExecution: TaskExecution | null }> {
      const task = await this.getTask(projectName, taskId);
      const executions = await this.repository.listExecutions(taskId);
      return {
         task,
         latestExecution: executions.length > 0 ? executions[0] : null,
      };
   }

   async listExecutions(
      projectName: string,
      taskId: string,
   ): Promise<TaskExecution[]> {
      await this.getTask(projectName, taskId);
      return this.repository.listExecutions(taskId);
   }

   async getExecution(
      projectName: string,
      taskId: string,
      executionId: string,
   ): Promise<TaskExecution> {
      await this.getTask(projectName, taskId);
      const execution = await this.repository.getExecutionById(executionId);
      if (!execution || execution.taskId !== taskId) {
         throw new TaskNotFoundError(
            `Execution ${executionId} not found for task ${taskId}`,
         );
      }
      return execution;
   }

   // ==================== BUILD EXECUTION ====================

   async startTask(
      projectName: string,
      taskId: string,
      options: { autoLoadManifest?: boolean; forceRefresh?: boolean } = {},
   ): Promise<TaskExecution> {
      const projectId = await this.resolveProjectId(projectName);
      const task = await this.resolveTask(projectId, taskId);

      // createExecution returns null when the task already has a non-terminal
      // execution (PENDING or RUNNING), enforcing at-most-one concurrency.
      const execution = await this.repository.createExecution(
         taskId,
         "PENDING",
      );
      if (!execution) {
         const running = await this.repository.getRunningExecution(taskId);
         throw new TaskExecutionConflictError(
            `Task ${taskId} already has an active execution${running ? ` (${running.id})` : ""}`,
         );
      }

      // Transition to RUNNING before returning so the response includes
      // startedAt and reflects the actual state.
      const running = await this.transitionExecution(execution.id, "RUNNING", {
         startedAt: new Date(),
      });

      // Fire-and-forget: run the build in the background so the HTTP
      // response returns immediately.
      this.runBuild(execution.id, projectName, projectId, task, options);

      return running;
   }

   /**
    * Runs the build for a single execution that is already in RUNNING state.
    * Transitions to SUCCESS / FAILED / CANCELLED on completion.
    * Called from `startTask` as a fire-and-forget background task.
    */
   private async runBuild(
      executionId: string,
      projectName: string,
      projectId: string,
      task: Task,
      options: { autoLoadManifest?: boolean; forceRefresh?: boolean },
   ): Promise<void> {
      const abortController = new AbortController();
      this.runningAbortControllers.set(executionId, abortController);

      try {
         const buildMetadata = await this.executeBuild(
            projectName,
            projectId,
            task,
            options.forceRefresh || false,
            abortController.signal,
         );

         // Reload the affected model with the updated manifest so subsequent
         // queries resolve persist references to the materialized tables.
         if (options.autoLoadManifest) {
            const manifest = await this.manifestService.getManifest(
               projectId,
               task.config.package,
            );
            const project = await this.projectStore.getProject(
               projectName,
               false,
            );
            const pkg = await project.getPackage(task.config.package, false);
            await pkg.reloadModel(task.config.modelPath, manifest.entries);
         }

         await this.transitionExecution(executionId, "SUCCESS", {
            completedAt: new Date(),
            metadata: buildMetadata,
         });
      } catch (err) {
         const error = err as Error;

         if (abortController.signal.aborted) {
            await this.transitionExecution(executionId, "CANCELLED", {
               completedAt: new Date(),
               error: "Build cancelled",
            });
         } else {
            await this.transitionExecution(executionId, "FAILED", {
               completedAt: new Date(),
               error: error.message,
            });
         }
      } finally {
         this.runningAbortControllers.delete(executionId);
      }
   }

   /**
    * Signals a running execution to stop. Returns null if nothing is running.
    *
    * Two cancellation paths:
    * 1. Normal — the abort controller is still in memory; signal it and let
    *    the `runBuild` catch block handle the state transition.
    * 2. Orphaned — no controller exists (server restarted mid-execution).
    *    Directly transition to CANCELLED so the task isn't permanently stuck.
    */
   async stopTask(
      projectName: string,
      taskId: string,
   ): Promise<TaskExecution | null> {
      await this.getTask(projectName, taskId);
      const running = await this.repository.getRunningExecution(taskId);
      if (!running) {
         return null;
      }

      const abortController = this.runningAbortControllers.get(running.id);
      if (abortController) {
         abortController.abort();
         return {
            ...running,
            status: "CANCELLED" as const,
            completedAt: new Date(),
            error: "Build cancelled",
            updatedAt: new Date(),
         };
      } else {
         return this.transitionExecution(running.id, "CANCELLED", {
            completedAt: new Date(),
            error: "Force cancelled: execution was orphaned",
         });
      }
   }

   // ==================== BUILD LOGIC ====================

   /**
    * Core build loop following the Malloy builder contract (5 steps):
    *
    *   1. LOAD  — Load existing manifest into an in-memory Manifest object.
    *   2. COMPILE — Compile the model without the manifest (plain IR).
    *   3. PLAN  — Get the dependency-ordered build plan.
    *   4. BUILD — Walk graphs → levels → nodes in dependency order.
    *              For each source compute a stable BuildID from the
    *              no-substitution SQL, then execute the manifest-substituted
    *              build SQL. Update the in-memory manifest immediately so
    *              downstream sources reference pre-built tables.
    *   5. WRITE — Persist only active manifest entries (GC).
    */
   private async executeBuild(
      projectName: string,
      projectId: string,
      task: Task,
      forceRefresh: boolean,
      signal: AbortSignal,
   ): Promise<Record<string, unknown>> {
      const config = task.config;

      logger.info("Starting materialization build", {
         taskId: task.id,
         package: config.package,
         modelPath: config.modelPath,
      });

      const project = await this.projectStore.getProject(projectName, false);
      const pkg = await project.getPackage(config.package, false);
      const model = pkg.getModel(config.modelPath);

      if (!model) {
         throw new BadRequestError(
            `Model ${config.modelPath} not found in package ${config.package}`,
         );
      }

      // ── STEP 1: LOAD — Load existing manifest ──────────────────────
      // The in-memory Manifest maps BuildIDs to table names from prior
      // builds. Its `buildManifest` is a stable reference — updates made
      // during the build loop are immediately visible to subsequent
      // `getSQL({buildManifest, ...})` calls.
      const manifest = new Manifest();
      const existingManifest = await this.manifestService.getManifest(
         projectId,
         config.package,
      );
      manifest.loadText(JSON.stringify(existingManifest));

      // ── STEP 2: COMPILE — Compile the model (no manifest) ──────────
      // The manifest is NOT passed to the compiler. Manifest substitution
      // happens later in step 4 via getSQL({buildManifest, ...}).
      const { runtime, modelURL, importBaseURL } = await (
         await import("./model")
      ).Model.getModelRuntime(
         pkg.getPackagePath(),
         config.modelPath,
         pkg.getConnections(),
      );

      const modelMaterializer = runtime.loadModel(modelURL, { importBaseURL });
      const malloyModel = await modelMaterializer.getModel();

      // ── STEP 3: PLAN — Get the dependency-ordered build plan ───────
      const buildPlan = malloyModel.getBuildPlan();

      if (buildPlan.tagParseLog.length > 0) {
         for (const msg of buildPlan.tagParseLog) {
            logger.warn("Persist annotation issue", {
               message: msg.message,
               severity: msg.severity,
               code: msg.code,
            });
         }
      }

      logger.info("Build plan", {
         sourceCount: Object.keys(buildPlan.sources).length,
         sourceIds: Object.keys(buildPlan.sources),
         graphCount: buildPlan.graphs.length,
      });

      if (buildPlan.graphs.length === 0) {
         logger.info("No persist sources to build");
         return { sourcesBuilt: 0, sourcesSkipped: 0 };
      }

      // Pre-compute connection digests. The digest captures connection-
      // specific settings so two different configs produce different
      // BuildIDs for the same Malloy source.
      const connections = pkg.getConnections();
      const connectionDigests: Record<string, string> = {};
      for (const graph of buildPlan.graphs) {
         const conn = connections.get(graph.connectionName);
         if (conn) {
            connectionDigests[graph.connectionName] =
               await conn.getDigest();
         }
      }

      // ── STEP 4: BUILD — Walk graphs in dependency order ────────────
      // Each graph groups sources for one connection. Within a graph,
      // nodes are organized into levels: all nodes within a level are
      // independent (parallel-safe), levels must be processed in order.
      let sourcesBuilt = 0;
      let sourcesSkipped = 0;
      const sourceResults: Record<string, unknown>[] = [];

      for (const graph of buildPlan.graphs) {
         const connection = connections.get(graph.connectionName);
         if (!connection) {
            throw new BadRequestError(
               `Connection '${graph.connectionName}' not found`,
            );
         }

         for (const level of graph.nodes) {
            for (const node of level) {
               if (signal.aborted) {
                  throw new Error("Build cancelled");
               }

               const persistSource = buildPlan.sources[node.sourceID];
               if (!persistSource) {
                  logger.warn(
                     `Source ${node.sourceID} not found in build plan, skipping`,
                  );
                  continue;
               }

               // BuildID SQL — fully inlined, no manifest substitution.
               // This produces a stable hash regardless of build order.
               const buildIdSQL = persistSource.getSQL();
               const digest =
                  connectionDigests[persistSource.connectionName];
               const buildId = persistSource.makeBuildId(
                  digest,
                  buildIdSQL,
               );

               // Already built — mark active so it survives GC.
               if (
                  manifest.buildManifest.entries[buildId] &&
                  !forceRefresh
               ) {
                  manifest.touch(buildId);
                  logger.info(
                     `Source ${persistSource.name} up to date, skipping`,
                     { buildId },
                  );
                  sourcesSkipped++;
                  sourceResults.push({
                     name: persistSource.name,
                     status: "skipped",
                     buildId,
                  });
                  continue;
               }

               // Build SQL — with manifest substitution so dependencies
               // resolve to their pre-built table names instead of the
               // full inline SQL.
               const buildSQL = persistSource.getSQL({
                  buildManifest: manifest.buildManifest,
                  connectionDigests,
               });

               // Table name from `#@ name` annotation, falling back to
               // source name.
               const connectionName = persistSource.connectionName;
               const tableName =
                  persistSource
                     .tagParse({ prefix: /^#@ / })
                     .tag.text("name") || persistSource.name;
               const lastDot = tableName.lastIndexOf(".");
               const stagingTableName =
                  lastDot >= 0
                     ? `${tableName.substring(0, lastDot + 1)}${tableName.substring(lastDot + 1)}__staging`
                     : `${tableName}__staging`;
               const dialect = persistSource.dialect;
               const quotePath = (p: string) =>
                  p
                     .split(".")
                     .map((seg) => dialect.quoteTablePath(seg))
                     .join(".");

               logger.info(`Building source ${persistSource.name}`, {
                  tableName,
                  connectionName,
               });

               const startTime = performance.now();

               // Build into a staging table so the live table stays
               // queryable throughout the (potentially slow) CREATE.
               await connection.runSQL(
                  `DROP TABLE IF EXISTS ${quotePath(stagingTableName)}`,
               );
               await connection.runSQL(
                  `CREATE TABLE ${quotePath(stagingTableName)} AS (${buildSQL})`,
               );

               // Swap: drop old, rename staging → final.
               await connection.runSQL(
                  `DROP TABLE IF EXISTS ${quotePath(tableName)}`,
               );
               const bareTableName = tableName.includes(".")
                  ? tableName.substring(tableName.lastIndexOf(".") + 1)
                  : tableName;
               await connection.runSQL(
                  `ALTER TABLE ${quotePath(stagingTableName)} RENAME TO ${dialect.quoteTablePath(bareTableName)}`,
               );

               const duration = performance.now() - startTime;

               // Update the in-memory manifest IMMEDIATELY so subsequent
               // sources in this build see the table name via
               // getSQL({buildManifest, ...}) instead of inline SQL.
               manifest.update(buildId, { tableName });

               // Also persist to the database.
               await this.manifestService.writeEntry(
                  projectId,
                  config.package,
                  buildId,
                  {
                     tableName,
                     sourceName: persistSource.name,
                     connectionName,
                  },
               );

               sourcesBuilt++;
               sourceResults.push({
                  name: persistSource.name,
                  status: "built",
                  buildId,
                  tableName,
                  durationMs: Math.round(duration),
               });

               logger.info(`Built source ${persistSource.name}`, {
                  tableName,
                  durationMs: Math.round(duration),
               });
            }
         }
      }

      // ── STEP 5: WRITE — GC stale manifest entries ─────────────────
      // activeEntries contains only entries that were touch()ed or
      // update()d this run. Delete any DB entries not in the active set
      // so removed/renamed sources are pruned automatically.
      const activeManifest = manifest.activeEntries;
      const allDbEntries = await this.manifestService.listEntries(
         projectId,
         config.package,
      );
      for (const dbEntry of allDbEntries) {
         if (!activeManifest.entries[dbEntry.buildId]) {
            await this.manifestService.deleteEntry(dbEntry.id);
         }
      }

      logger.info("Materialization build complete", {
         sourcesBuilt,
         sourcesSkipped,
      });

      return { sourcesBuilt, sourcesSkipped, sources: sourceResults };
   }

   // ==================== HELPERS ====================

   /** Maps a user-facing project name to its database primary key. */
   private async resolveProjectId(projectName: string): Promise<string> {
      const dbProject = await this.repository.getProjectByName(projectName);
      if (!dbProject) {
         throw new TaskNotFoundError(
            `Project '${projectName}' not found in database`,
         );
      }
      return dbProject.id;
   }
}
