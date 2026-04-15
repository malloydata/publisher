import type { BuildGraph, PersistSource } from "@malloydata/malloy";
import { Manifest } from "@malloydata/malloy";
import {
   BadRequestError,
   BuildConflictError,
   BuildNotFoundError,
   InvalidStateTransitionError,
   ProjectNotFoundError,
} from "../errors";
import { logger } from "../logger";
import {
   BuildExecution,
   BuildExecutionStatus,
   ResourceRepository,
} from "../storage/DatabaseInterface";
import { ManifestService } from "./manifest_service";
import { Model } from "./model";
import { ProjectStore } from "./project_store";

/**
 * Allowed execution status transitions. SUCCESS, FAILED, and CANCELLED are
 * terminal — once an execution reaches one of these states it is immutable.
 */
const VALID_TRANSITIONS: Record<BuildExecutionStatus, BuildExecutionStatus[]> =
   {
      PENDING: ["RUNNING", "CANCELLED"],
      RUNNING: ["SUCCESS", "FAILED", "CANCELLED"],
      SUCCESS: [],
      FAILED: [],
      CANCELLED: [],
   };

/**
 * Orchestrates package-level materialization builds: triggering builds,
 * cancellation, and the actual Malloy build that materializes persist
 * sources into database tables.
 *
 * A build targets an entire package — all models are compiled and all
 * persist sources across all models are processed in dependency order.
 * The manifest is automatically activated after a successful build so
 * subsequent queries resolve persist references to materialized tables.
 *
 * Enforces at-most-one concurrent build per (project, package) via the
 * repository's atomic `createBuildExecution`, and supports cooperative
 * cancellation through `AbortController`.
 */
export class BuildService {
   /**
    * Tracks in-flight executions so they can be cancelled. This map only
    * lives in-process memory — entries are lost on server restart, which is
    * why `stopBuild` has an orphaned-execution fallback path.
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
      current: BuildExecutionStatus,
      next: BuildExecutionStatus,
   ): void {
      const allowed = VALID_TRANSITIONS[current];
      if (!allowed.includes(next)) {
         throw new InvalidStateTransitionError(
            `Cannot transition from ${current} to ${next}`,
         );
      }
   }

   private async transitionExecution(
      executionId: string,
      newStatus: BuildExecutionStatus,
      extra?: {
         startedAt?: Date;
         completedAt?: Date;
         error?: string | null;
         metadata?: Record<string, unknown> | null;
      },
   ): Promise<BuildExecution> {
      const execution =
         await this.repository.getBuildExecutionById(executionId);
      if (!execution) {
         throw new BuildNotFoundError(`Execution ${executionId} not found`);
      }
      this.validateTransition(execution.status, newStatus);
      return this.repository.updateBuildExecution(executionId, {
         status: newStatus,
         ...extra,
      });
   }

   // ==================== EXECUTION QUERIES ====================

   async getBuildStatus(
      projectName: string,
      packageName: string,
   ): Promise<{ latestExecution: BuildExecution | null }> {
      const projectId = await this.resolveProjectId(projectName);
      const executions = await this.repository.listBuildExecutions(
         projectId,
         packageName,
      );
      return {
         latestExecution: executions.length > 0 ? executions[0] : null,
      };
   }

   async listExecutions(
      projectName: string,
      packageName: string,
   ): Promise<BuildExecution[]> {
      const projectId = await this.resolveProjectId(projectName);
      return this.repository.listBuildExecutions(projectId, packageName);
   }

   async getExecution(
      projectName: string,
      packageName: string,
      executionId: string,
   ): Promise<BuildExecution> {
      const projectId = await this.resolveProjectId(projectName);
      const execution =
         await this.repository.getBuildExecutionById(executionId);
      if (
         !execution ||
         execution.projectId !== projectId ||
         execution.packageName !== packageName
      ) {
         throw new BuildNotFoundError(
            `Execution ${executionId} not found for package ${packageName}`,
         );
      }
      return execution;
   }

   // ==================== BUILD EXECUTION ====================

   async startBuild(
      projectName: string,
      packageName: string,
      options: { forceRefresh?: boolean } = {},
   ): Promise<BuildExecution> {
      const projectId = await this.resolveProjectId(projectName);

      // Verify the package exists.
      const project = await this.projectStore.getProject(projectName, false);
      await project.getPackage(packageName, false);

      // Atomically create an execution — returns null if one is already active.
      const execution = await this.repository.createBuildExecution(
         projectId,
         packageName,
         "PENDING",
      );
      if (!execution) {
         const running = await this.repository.getRunningBuildExecution(
            projectId,
            packageName,
         );
         throw new BuildConflictError(
            `Package ${packageName} already has an active build${running ? ` (${running.id})` : ""}`,
         );
      }

      const running = await this.transitionExecution(execution.id, "RUNNING", {
         startedAt: new Date(),
      });

      // Fire-and-forget: run the build in the background so the HTTP
      // response returns immediately.
      this.runBuild(
         execution.id,
         projectName,
         projectId,
         packageName,
         options,
      ).catch((err) => {
         logger.error("Unhandled error in background build", {
            executionId: execution.id,
            error: err instanceof Error ? err.message : String(err),
         });
      });

      return running;
   }

   private async runBuild(
      executionId: string,
      projectName: string,
      projectId: string,
      packageName: string,
      options: { forceRefresh?: boolean },
   ): Promise<void> {
      const abortController = new AbortController();
      this.runningAbortControllers.set(executionId, abortController);

      try {
         const buildMetadata = await this.executeBuild(
            projectName,
            projectId,
            packageName,
            options.forceRefresh || false,
            abortController.signal,
         );

         // Auto-activate: store the updated manifest on the package so
         // subsequent queries resolve persist references to materialized tables.
         const updatedManifest = await this.manifestService.getManifest(
            projectId,
            packageName,
         );
         const project = await this.projectStore.getProject(projectName, false);
         const pkg = await project.getPackage(packageName, false);
         pkg.setBuildManifest(updatedManifest.entries);

         await this.transitionExecution(executionId, "SUCCESS", {
            completedAt: new Date(),
            metadata: buildMetadata,
         });
      } catch (err) {
         const errorMessage = err instanceof Error ? err.message : String(err);

         if (abortController.signal.aborted) {
            await this.transitionExecution(executionId, "CANCELLED", {
               completedAt: new Date(),
               error: "Build cancelled",
            });
         } else {
            await this.transitionExecution(executionId, "FAILED", {
               completedAt: new Date(),
               error: errorMessage,
            });
         }
      } finally {
         this.runningAbortControllers.delete(executionId);
      }
   }

   async stopBuild(
      projectName: string,
      packageName: string,
   ): Promise<BuildExecution | null> {
      const projectId = await this.resolveProjectId(projectName);
      const running = await this.repository.getRunningBuildExecution(
         projectId,
         packageName,
      );
      if (!running) {
         return null;
      }

      const abortController = this.runningAbortControllers.get(running.id);
      if (abortController) {
         abortController.abort();
         return {
            ...running,
            metadata: { ...running.metadata, cancellationRequested: true },
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
    *   2. COMPILE — Compile ALL models in the package without the manifest.
    *   3. PLAN  — Get the dependency-ordered build plan from each model.
    *   4. BUILD — Walk graphs -> levels -> nodes in dependency order.
    *   5. WRITE — Persist only active manifest entries (GC).
    *
    * Iterates all models in the package so the manifest and GC cover
    * every persist source regardless of which model defines it.
    */
   private async executeBuild(
      projectName: string,
      projectId: string,
      packageName: string,
      forceRefresh: boolean,
      signal: AbortSignal,
   ): Promise<Record<string, unknown>> {
      logger.info("Starting materialization build", {
         projectName,
         packageName,
      });

      const project = await this.projectStore.getProject(projectName, false);
      const pkg = await project.getPackage(packageName, false);

      // ── STEP 1: LOAD — Load existing manifest ──────────────────────
      const manifest = new Manifest();
      const existingManifest = await this.manifestService.getManifest(
         projectId,
         packageName,
      );
      manifest.loadText(JSON.stringify(existingManifest));

      // ── STEP 2 & 3: COMPILE & PLAN — Compile all models, collect build plans
      const modelPaths = pkg.getModelPaths();
      const allGraphs: BuildGraph[] = [];
      const allSources: Record<string, PersistSource> = {};

      for (const modelPath of modelPaths) {
         if (signal.aborted) throw new Error("Build cancelled");

         const { runtime, modelURL, importBaseURL } =
            await Model.getModelRuntime(
               pkg.getPackagePath(),
               modelPath,
               pkg.getConnections(),
            );

         const modelMaterializer = runtime.loadModel(modelURL, {
            importBaseURL,
         });
         const malloyModel = await modelMaterializer.getModel();
         const buildPlan = malloyModel.getBuildPlan();

         if (buildPlan.tagParseLog.length > 0) {
            for (const msg of buildPlan.tagParseLog) {
               logger.warn("Persist annotation issue", {
                  modelPath,
                  message: msg.message,
                  severity: msg.severity,
                  code: msg.code,
               });
            }
         }

         if (buildPlan.graphs.length > 0) {
            allGraphs.push(...buildPlan.graphs);
            for (const [sourceID, source] of Object.entries(
               buildPlan.sources,
            )) {
               if (allSources[sourceID]) {
                  logger.warn(
                     `Duplicate sourceID "${sourceID}" from model ${modelPath}, overwriting previous definition`,
                  );
               }
               allSources[sourceID] = source;
            }
         }
      }

      logger.info("Build plan", {
         sourceCount: Object.keys(allSources).length,
         graphCount: allGraphs.length,
      });

      if (allGraphs.length === 0) {
         logger.info("No persist sources to build");
         return { sourcesBuilt: 0, sourcesSkipped: 0 };
      }

      // Pre-compute connection digests.
      const connections = pkg.getConnections();
      const connectionDigests: Record<string, string> = {};
      for (const graph of allGraphs) {
         const conn = connections.get(graph.connectionName);
         if (conn && !connectionDigests[graph.connectionName]) {
            connectionDigests[graph.connectionName] = await conn.getDigest();
         }
      }

      // ── STEP 4: BUILD — Walk graphs in dependency order ────────────
      let sourcesBuilt = 0;
      let sourcesSkipped = 0;
      const sourceResults: Record<string, unknown>[] = [];

      for (const graph of allGraphs) {
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

               const persistSource = allSources[node.sourceID];
               if (!persistSource) {
                  logger.warn(
                     `Source ${node.sourceID} not found in build plan, skipping`,
                  );
                  continue;
               }

               // BuildID SQL — fully inlined, no manifest substitution.
               const buildIdSQL = persistSource.getSQL();
               const digest = connectionDigests[persistSource.connectionName];
               const buildId = persistSource.makeBuildId(digest, buildIdSQL);

               // Already built — mark active so it survives GC.
               if (manifest.buildManifest.entries[buildId] && !forceRefresh) {
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

               // Build SQL — with manifest substitution.
               const buildSQL = persistSource.getSQL({
                  buildManifest: manifest.buildManifest,
                  connectionDigests,
               });

               const connectionName = persistSource.connectionName;
               const tableName =
                  persistSource.tagParse({ prefix: /^#@ / }).tag.text("name") ||
                  persistSource.name;
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

               await connection.runSQL(
                  `DROP TABLE IF EXISTS ${quotePath(stagingTableName)}`,
               );
               await connection.runSQL(
                  `CREATE TABLE ${quotePath(stagingTableName)} AS (${buildSQL})`,
               );

               await connection.runSQL(
                  `DROP TABLE IF EXISTS ${quotePath(tableName)}`,
               );
               await connection.runSQL(
                  `ALTER TABLE ${quotePath(stagingTableName)} RENAME TO ${quotePath(tableName)}`,
               );

               const duration = performance.now() - startTime;

               manifest.update(buildId, { tableName });

               await this.manifestService.writeEntry(
                  projectId,
                  packageName,
                  buildId,
                  tableName,
                  persistSource.name,
                  connectionName,
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
      const activeManifest = manifest.activeEntries;
      const allDbEntries = await this.manifestService.listEntries(
         projectId,
         packageName,
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

   private async resolveProjectId(projectName: string): Promise<string> {
      const dbProject = await this.repository.getProjectByName(projectName);
      if (!dbProject) {
         throw new ProjectNotFoundError(`Project '${projectName}' not found`);
      }
      return dbProject.id;
   }
}
