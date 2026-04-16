import type { BuildGraph, PersistSource } from "@malloydata/malloy";
import { Manifest } from "@malloydata/malloy";
import {
   BadRequestError,
   InvalidStateTransitionError,
   MaterializationConflictError,
   MaterializationNotFoundError,
} from "../errors";
import { logger } from "../logger";
import {
   Materialization,
   MaterializationStatus,
   ResourceRepository,
} from "../storage/DatabaseInterface";
import { DuplicateActiveMaterializationError } from "../storage/duckdb/MaterializationRepository";
import { ManifestService } from "./manifest_service";
import { Model } from "./model";
import { ProjectStore } from "./project_store";
import { resolveProjectId } from "./resolve_project";

/**
 * Quote a potentially schema-qualified table path (e.g. "schema.table")
 * by quoting each segment individually with the dialect's quoteTablePath.
 */
function quoteTablePath(
   path: string,
   dialect: { quoteTablePath(seg: string): string },
): string {
   return path
      .split(".")
      .map((seg) => dialect.quoteTablePath(seg))
      .join(".");
}

/**
 * Allowed execution status transitions. SUCCESS, FAILED, and CANCELLED are
 * terminal — once an execution reaches one of these states it is immutable.
 */
const VALID_TRANSITIONS: Record<
   MaterializationStatus,
   MaterializationStatus[]
> = {
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
 * The manifest is optionally activated after a successful build so
 * subsequent queries resolve persist references to materialized tables.
 *
 * Enforces at-most-one concurrent build per (project, package) via a
 * DB-level unique index on `materializations.active_key` (see
 * `MaterializationRepository`), and supports cooperative cancellation
 * through `AbortController`.
 *
 * **Multi-worker caveat:** the `materializations` table lives in each
 * worker's *local* DuckDB, so the active-materialization lock is only
 * enforced within a single Publisher process. In orchestrated deployments
 * (shared DuckLake manifest catalog), builds must be externally
 * single-writer until a shared lease is added — see the scope note on
 * `DuckLakeManifestStore`.
 */
export class MaterializationService {
   /**
    * Tracks in-flight executions so they can be cancelled. This map only
    * lives in-process memory — entries are lost on server restart, which is
    * why `stopMaterialization` has an orphaned-execution fallback path.
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
      current: MaterializationStatus,
      next: MaterializationStatus,
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
      newStatus: MaterializationStatus,
      extra?: {
         startedAt?: Date;
         completedAt?: Date;
         error?: string | null;
         metadata?: Record<string, unknown> | null;
      },
   ): Promise<Materialization> {
      const execution =
         await this.repository.getMaterializationById(executionId);
      if (!execution) {
         throw new MaterializationNotFoundError(
            `Execution ${executionId} not found`,
         );
      }
      this.validateTransition(execution.status, newStatus);
      return this.repository.updateMaterialization(executionId, {
         status: newStatus,
         ...extra,
      });
   }

   // ==================== BUILD QUERIES ====================

   async listMaterializations(
      projectName: string,
      packageName: string,
      options?: { limit?: number; offset?: number },
   ): Promise<Materialization[]> {
      const projectId = await this.resolveProjectId(projectName);
      return this.repository.listMaterializations(
         projectId,
         packageName,
         options,
      );
   }

   async getMaterialization(
      projectName: string,
      packageName: string,
      buildId: string,
   ): Promise<Materialization> {
      const projectId = await this.resolveProjectId(projectName);
      const execution = await this.repository.getMaterializationById(buildId);
      if (
         !execution ||
         execution.projectId !== projectId ||
         execution.packageName !== packageName
      ) {
         throw new MaterializationNotFoundError(
            `Materialization ${buildId} not found for package ${packageName}`,
         );
      }
      return execution;
   }

   // ==================== BUILD LIFECYCLE ====================

   /**
    * Creates a new build in PENDING state. Build options are stored in
    * metadata so `startMaterialization` can read them back.
    */
   async createMaterialization(
      projectName: string,
      packageName: string,
      options: { forceRefresh?: boolean; autoLoadManifest?: boolean } = {},
   ): Promise<Materialization> {
      const projectId = await this.resolveProjectId(projectName);

      // Verify the package exists.
      const project = await this.projectStore.getProject(projectName, false);
      await project.getPackage(packageName, false);

      // A non-atomic probe for a helpful error message. The DB-level unique
      // index on active_key is the actual race-free guard — see the catch
      // block below.
      const active = await this.repository.getActiveMaterialization(
         projectId,
         packageName,
      );
      if (active) {
         throw new MaterializationConflictError(
            `Package ${packageName} already has an active materialization (${active.id})`,
         );
      }

      let materialization;
      try {
         materialization = await this.repository.createMaterialization(
            projectId,
            packageName,
            "PENDING",
         );
      } catch (err) {
         if (err instanceof DuplicateActiveMaterializationError) {
            // Lost the race with a concurrent create. Re-read to report the
            // winner's id for parity with the non-racy error above.
            const winner = await this.repository.getActiveMaterialization(
               projectId,
               packageName,
            );
            throw new MaterializationConflictError(
               winner
                  ? `Package ${packageName} already has an active materialization (${winner.id})`
                  : `Package ${packageName} already has an active materialization`,
            );
         }
         throw err;
      }

      // Store build options in metadata so startMaterialization can retrieve them.
      return this.repository.updateMaterialization(materialization.id, {
         metadata: {
            forceRefresh: options.forceRefresh ?? false,
            autoLoadManifest: options.autoLoadManifest ?? false,
         },
      });
   }

   /**
    * Transitions a PENDING build to RUNNING and starts execution in the
    * background. Returns the RUNNING execution immediately.
    */
   async startMaterialization(
      projectName: string,
      packageName: string,
      buildId: string,
   ): Promise<Materialization> {
      const projectId = await this.resolveProjectId(projectName);
      const execution = await this.getMaterialization(
         projectName,
         packageName,
         buildId,
      );

      if (execution.status !== "PENDING") {
         throw new InvalidStateTransitionError(
            `Materialization ${buildId} is ${execution.status}, expected PENDING`,
         );
      }

      // Check for a *different* active materialization on this package.
      const active = await this.repository.getActiveMaterialization(
         projectId,
         packageName,
      );
      if (active && active.id !== execution.id) {
         throw new MaterializationConflictError(
            `Package ${packageName} already has an active materialization (${active.id})`,
         );
      }

      const running = await this.transitionExecution(execution.id, "RUNNING", {
         startedAt: new Date(),
      });

      const buildOptions = (execution.metadata ?? {}) as {
         forceRefresh?: boolean;
         autoLoadManifest?: boolean;
      };

      // Fire-and-forget: run the build in the background.
      this.runMaterialization(
         execution.id,
         projectName,
         projectId,
         packageName,
         buildOptions,
         (execution.metadata ?? {}) as Record<string, unknown>,
      ).catch((err) => {
         logger.error("Unhandled error in background build", {
            executionId: execution.id,
            error: err instanceof Error ? err.message : String(err),
         });
      });

      return running;
   }

   private async runMaterialization(
      executionId: string,
      projectName: string,
      projectId: string,
      packageName: string,
      options: { forceRefresh?: boolean; autoLoadManifest?: boolean },
      baseMetadata: Record<string, unknown>,
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

         // If autoLoadManifest is set, reload all models with the updated
         // manifest so subsequent queries resolve persist references.
         if (options.autoLoadManifest) {
            const updatedManifest = await this.manifestService.getManifest(
               projectId,
               packageName,
            );
            const project = await this.projectStore.getProject(
               projectName,
               false,
            );
            const pkg = await project.getPackage(packageName, false);
            await pkg.reloadAllModels(updatedManifest.entries);
         }

         await this.transitionExecution(executionId, "SUCCESS", {
            completedAt: new Date(),
            metadata: {
               ...baseMetadata,
               ...buildMetadata,
            },
         });
      } catch (err) {
         const errorMessage = err instanceof Error ? err.message : String(err);

         try {
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
         } catch (transitionErr) {
            logger.error("Failed to transition execution after build error", {
               executionId,
               originalError: errorMessage,
               transitionError:
                  transitionErr instanceof Error
                     ? transitionErr.message
                     : String(transitionErr),
            });
         }
      } finally {
         this.runningAbortControllers.delete(executionId);
      }
   }

   /**
    * Cancels a running build. Takes a specific buildId.
    */
   async stopMaterialization(
      projectName: string,
      packageName: string,
      buildId: string,
   ): Promise<Materialization> {
      const execution = await this.getMaterialization(
         projectName,
         packageName,
         buildId,
      );

      if (execution.status !== "RUNNING" && execution.status !== "PENDING") {
         throw new InvalidStateTransitionError(
            `Materialization ${buildId} is ${execution.status}, cannot stop`,
         );
      }

      if (execution.status === "PENDING") {
         return this.transitionExecution(execution.id, "CANCELLED", {
            completedAt: new Date(),
            error: "Build cancelled before starting",
         });
      }

      const abortController = this.runningAbortControllers.get(execution.id);
      if (abortController) {
         abortController.abort();
         return execution;
      } else {
         return this.transitionExecution(execution.id, "CANCELLED", {
            completedAt: new Date(),
            error: "Force cancelled: execution was orphaned",
         });
      }
   }

   /**
    * Deletes a materialization record. Only terminal materializations
    * (SUCCESS, FAILED, CANCELLED) can be deleted.
    */
   async deleteMaterialization(
      projectName: string,
      packageName: string,
      materializationId: string,
   ): Promise<void> {
      const execution = await this.getMaterialization(
         projectName,
         packageName,
         materializationId,
      );

      if (execution.status === "PENDING" || execution.status === "RUNNING") {
         throw new InvalidStateTransitionError(
            `Cannot delete materialization ${materializationId} while it is ${execution.status}`,
         );
      }

      await this.repository.deleteMaterialization(execution.id);
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
    *
    * **GC scope.** Step 5 prunes manifest *metadata* only: stale
    * `build_manifests` rows are removed so queries stop resolving to
    * retired BuildIDs. The physical tables behind retired materializations
    * are not dropped — if a persist source's SQL changes and it is
    * re-materialized under a new BuildID, the old table remains in the
    * target connection until an operator cleans it up manually.
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

         // Skip models that don't opt in to persistence. getBuildPlan() throws
         // with "Model must have ##! experimental.persistence to use
         // getBuildPlan()" if the tag is missing, so check the tag first to
         // keep plain models in the same package buildable.
         const modelTag = malloyModel.tagParse({ prefix: /^##! / }).tag;
         if (!modelTag.has("experimental", "persistence")) {
            logger.debug(
               "Model has no ##! experimental.persistence tag, skipping",
               { modelPath },
            );
            continue;
         }

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

      // Fail fast if two persist sources target the same (connection, table).
      // Without this, the later source's DDL would silently clobber the
      // earlier one's physical table while both manifest entries remain live.
      const tableOwners = new Map<string, string>();
      for (const [sourceID, source] of Object.entries(allSources)) {
         const tableName =
            source.tagParse({ prefix: /^#@ / }).tag.text("name") || source.name;
         const key = `${source.connectionName}\u0000${tableName}`;
         const existing = tableOwners.get(key);
         if (existing) {
            throw new BadRequestError(
               `Persist target collision: sources '${existing}' and '${sourceID}' both resolve to table '${tableName}' on connection '${source.connectionName}'. Disambiguate with '#@ persist name=...'.`,
            );
         }
         tableOwners.set(key, sourceID);
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
               const quoted = (p: string) => quoteTablePath(p, dialect);

               logger.info(`Building source ${persistSource.name}`, {
                  tableName,
                  connectionName,
               });

               const startTime = performance.now();

               await connection.runSQL(
                  `DROP TABLE IF EXISTS ${quoted(stagingTableName)}`,
               );
               await connection.runSQL(
                  `CREATE TABLE ${quoted(stagingTableName)} AS (${buildSQL})`,
               );

               await connection.runSQL(
                  `DROP TABLE IF EXISTS ${quoted(tableName)}`,
               );
               const bareTableName = tableName.includes(".")
                  ? tableName.substring(tableName.lastIndexOf(".") + 1)
                  : tableName;
               await connection.runSQL(
                  `ALTER TABLE ${quoted(stagingTableName)} RENAME TO ${dialect.quoteTablePath(bareTableName)}`,
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
            await this.manifestService.deleteEntry(projectId, dbEntry.id);
         }
      }

      logger.info("Materialization build complete", {
         sourcesBuilt,
         sourcesSkipped,
      });

      return { sourcesBuilt, sourcesSkipped, sources: sourceResults };
   }

   // ==================== HELPERS ====================

   private resolveProjectId(projectName: string): Promise<string> {
      return resolveProjectId(this.repository, projectName);
   }
}
