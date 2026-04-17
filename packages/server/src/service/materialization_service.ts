import type {
   BuildGraph,
   Connection as MalloyConnection,
   PersistSource,
} from "@malloydata/malloy";
import { Manifest } from "@malloydata/malloy";
import {
   BadRequestError,
   InvalidStateTransitionError,
   MaterializationConflictError,
   MaterializationNotFoundError,
} from "../errors";
import { logger } from "../logger";
import type { ManifestEntry } from "../storage/DatabaseInterface";
import {
   Materialization,
   MaterializationStatus,
   ResourceRepository,
} from "../storage/DatabaseInterface";
import { DuplicateActiveMaterializationError } from "../storage/duckdb/MaterializationRepository";
import { ManifestService } from "./manifest_service";
import {
   dropManifestEntries,
   GcResult,
   liveTableKey,
} from "./materialized_table_gc";
import { Model } from "./model";
import { ProjectStore } from "./project_store";
import { quoteTablePath, splitTablePath } from "./quoting";
import { resolveProjectId } from "./resolve_project";

/**
 * Length of the BuildID prefix used when synthesizing staging table names.
 * BuildID is a 64-char SHA-256 hex string; 12 hex chars is 48 bits of entropy
 * — plenty of uniqueness per source, and keeps the final identifier well
 * inside every dialect's limit (Postgres is the tightest at 63).
 */
const STAGING_BUILD_ID_LEN = 12;

/**
 * Return the staging suffix `_<truncatedBuildId>` appended to a table name
 * while it is being built. The suffix is fully derivable from a manifest
 * entry's `buildId`, which is how GC finds and drops orphaned staging tables.
 */
export function stagingSuffix(buildId: string): string {
   return `_${buildId.substring(0, STAGING_BUILD_ID_LEN)}`;
}

/**
 * Build a stable key for a `(connectionName, tableName)` pair.
 * Used to check whether a persist target was created by a previous build.
 */
export function manifestTableKey(
   connectionName: string,
   tableName: string,
): string {
   return `${connectionName}::${tableName}`;
}

/**
 * Probe whether a table physically exists on the given connection by
 * running a zero-row SELECT. Returns `true` if the table resolves,
 * `false` if the query fails (assumed "table not found").
 */
export async function tablePhysicallyExists(
   connection: MalloyConnection,
   quotedTableName: string,
): Promise<boolean> {
   try {
      await connection.runSQL(`SELECT 1 FROM ${quotedTableName} WHERE 1=0`);
      return true;
   } catch {
      return false;
   }
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

      const metadata = {
         forceRefresh: options.forceRefresh ?? false,
         autoLoadManifest: options.autoLoadManifest ?? false,
      };

      try {
         return await this.repository.createMaterialization(
            projectId,
            packageName,
            "PENDING",
            metadata,
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

      const metadata = (execution.metadata ?? {}) as Record<string, unknown>;

      // Fire-and-forget: run the build in the background.
      this.runMaterialization(
         execution.id,
         projectName,
         projectId,
         packageName,
         metadata,
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
      metadata: Record<string, unknown>,
   ): Promise<void> {
      const abortController = new AbortController();
      this.runningAbortControllers.set(executionId, abortController);

      try {
         const buildMetadata = await this.executeBuild(
            projectName,
            projectId,
            packageName,
            !!metadata.forceRefresh,
            abortController.signal,
         );

         if (metadata.autoLoadManifest) {
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
            metadata: { ...metadata, ...buildMetadata },
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

   // ==================== GARBAGE COLLECTION ====================

   /**
    * Drop every materialized table and manifest row for a package.
    *
    * This is the only out-of-band GC surface exposed by the publisher and
    * is intended for a single caller: the controlplane, invoking it on the
    * truly destructive path before the package/project is torn down. The
    * publisher's `DELETE` endpoints are *not* wired into GC because the
    * controlplane invokes them for non-destructive unload too (down-
    * replicate, drain, archive) where the entity still exists on other
    * replicas, and dropping shared tables there would corrupt surviving
    * workers.
    *
    * Reconciliation of stale rows against the package's live source code
    * happens inline at the end of every successful build (see
    * {@link executeBuild} Step 5) — that is where "active" vs. "orphan"
    * is authoritatively determined, using the manifest's own `touch()`
    * bookkeeping. This endpoint does not re-derive that information; it
    * drops the manifest in its entirety, which is the correct behavior
    * for the pre-deletion use case and avoids pulling the package's
    * source through the compiler just to reach a foregone conclusion.
    *
    * Refuses to run while a materialization is active for the package
    * (same serialization the inline GC gets by piggy-backing on the
    * build).
    *
    * `dryRun` returns what would be dropped without issuing any DROP or
    * deleting manifest rows.
    */
   async gcPackage(
      projectName: string,
      packageName: string,
      options: { dryRun?: boolean } = {},
   ): Promise<GcResult> {
      const projectId = await this.resolveProjectId(projectName);

      const active = await this.repository.getActiveMaterialization(
         projectId,
         packageName,
      );
      if (active) {
         throw new MaterializationConflictError(
            `Package ${packageName} has an active materialization (${active.id}); cannot GC`,
         );
      }

      const project = await this.projectStore.getProject(projectName, false);
      const pkg = await project.getPackage(packageName, false);
      const connections = pkg.getConnections();

      const entries = await this.manifestService.listEntries(
         projectId,
         packageName,
      );

      // `forceDeleteRowOnMissingConnection`: teardown is the one place
      // where we'd rather lose the manifest row than leave it pointing at
      // a vanished connection. We also deliberately omit `liveTables`:
      // in teardown everything is stale, nothing is live.
      return dropManifestEntries(entries, {
         connections,
         manifestService: this.manifestService,
         projectId,
         dryRun: options.dryRun,
         forceDeleteRowOnMissingConnection: true,
      });
   }

   // ==================== BUILD LOGIC ====================

   /**
    * Core build pipeline (5 steps):
    *   1. LOAD  — Load existing manifest.
    *   2. COMPILE & PLAN — Compile all models, collect dependency graphs.
    *   3. BUILD — Walk graphs in dependency order, materialize each source.
    *   4. GC — Drop stale physical tables + prune manifest rows.
    *
    * Build success is not gated on GC — failures are surfaced in
    * `gcErrors` metadata so the controller/UI can show them.
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

      // ── STEP 1: LOAD ───────────────────────────────────────────────
      const manifest = new Manifest();
      const existingManifest = await this.manifestService.getManifest(
         projectId,
         packageName,
      );
      manifest.loadText(JSON.stringify(existingManifest));

      const existingEntries = await this.manifestService.listEntries(
         projectId,
         packageName,
      );
      const knownMaterializedTables = new Set(
         existingEntries.map((e: ManifestEntry) =>
            manifestTableKey(e.connectionName, e.tableName),
         ),
      );

      // ── STEP 2: COMPILE & PLAN ─────────────────────────────────────
      const { graphs, sources, connectionDigests } =
         await this.compilePackageBuildPlan(pkg, signal);

      if (graphs.length === 0) {
         logger.info("No persist sources to build");
         return { sourcesBuilt: 0, sourcesSkipped: 0 };
      }

      // ── STEP 3: BUILD ──────────────────────────────────────────────
      const connections = pkg.getConnections();
      let sourcesBuilt = 0;
      let sourcesSkipped = 0;
      const sourceResults: Record<string, unknown>[] = [];

      for (const graph of graphs) {
         const connection = connections.get(graph.connectionName);
         if (!connection) {
            throw new BadRequestError(
               `Connection '${graph.connectionName}' not found`,
            );
         }

         for (const level of graph.nodes) {
            for (const node of level) {
               if (signal.aborted) throw new Error("Build cancelled");

               const persistSource = sources[node.sourceID];
               if (!persistSource) {
                  logger.warn(
                     `Source ${node.sourceID} not found in build plan, skipping`,
                  );
                  continue;
               }

               const result = await this.buildOneSource(
                  persistSource,
                  manifest,
                  connection,
                  connectionDigests,
                  forceRefresh,
                  projectId,
                  packageName,
                  knownMaterializedTables,
               );

               sourceResults.push(result);
               if (result.status === "built") sourcesBuilt++;
               else sourcesSkipped++;
            }
         }
      }

      // ── STEP 4: GC ─────────────────────────────────────────────────
      const gcResult = await this.runPostBuildGc(
         manifest,
         projectId,
         packageName,
         connections,
      );

      logger.info("Materialization build complete", {
         sourcesBuilt,
         sourcesSkipped,
         gcDropped: gcResult.dropped.length,
         gcErrors: gcResult.errors.length,
      });

      return {
         sourcesBuilt,
         sourcesSkipped,
         sources: sourceResults,
         gcDropped: gcResult.dropped,
         gcErrors: gcResult.errors,
      };
   }

   // ==================== BUILD HELPERS ====================

   /**
    * Compile every model in the package and collect the dependency-ordered
    * build graphs, persist sources, and pre-computed connection digests.
    */
   private async compilePackageBuildPlan(
      pkg: {
         getModelPaths(): string[];
         getPackagePath(): string;
         getConnections(): Map<string, MalloyConnection>;
      },
      signal: AbortSignal,
   ): Promise<{
      graphs: BuildGraph[];
      sources: Record<string, PersistSource>;
      connectionDigests: Record<string, string>;
   }> {
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

         // getBuildPlan() throws if the tag is missing, so check first to
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

         for (const msg of buildPlan.tagParseLog) {
            logger.warn("Persist annotation issue", {
               modelPath,
               message: msg.message,
               severity: msg.severity,
               code: msg.code,
            });
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

      // Fail fast if two persist sources target the same (connection, table).
      const tableOwners = new Map<string, string>();
      for (const [sourceID, source] of Object.entries(allSources)) {
         const tableName =
            source.tagParse({ prefix: /^#@ / }).tag.text("name") || source.name;
         const key = `${source.connectionName}::${tableName}`;
         const existing = tableOwners.get(key);
         if (existing) {
            throw new BadRequestError(
               `Persist target collision: sources '${existing}' and '${sourceID}' both resolve to table '${tableName}' on connection '${source.connectionName}'. Disambiguate with '#@ persist name=...'.`,
            );
         }
         tableOwners.set(key, sourceID);
      }

      const connections = pkg.getConnections();
      const connectionDigests: Record<string, string> = {};
      for (const graph of allGraphs) {
         const conn = connections.get(graph.connectionName);
         if (conn && !connectionDigests[graph.connectionName]) {
            connectionDigests[graph.connectionName] = await conn.getDigest();
         }
      }

      return { graphs: allGraphs, sources: allSources, connectionDigests };
   }

   /**
    * Materialize a single persist source: skip if up-to-date, otherwise
    * build via staging table (CREATE → DROP old → RENAME), then write
    * the manifest entry. Stale entries are cleaned up by post-build GC.
    */
   private async buildOneSource(
      persistSource: PersistSource,
      manifest: Manifest,
      connection: MalloyConnection,
      connectionDigests: Record<string, string>,
      forceRefresh: boolean,
      projectId: string,
      packageName: string,
      knownMaterializedTables: Set<string>,
   ): Promise<Record<string, unknown>> {
      const buildIdSQL = persistSource.getSQL();
      const digest = connectionDigests[persistSource.connectionName];
      const buildId = persistSource.makeBuildId(digest, buildIdSQL);

      // Already built — mark active so it survives GC.
      if (manifest.buildManifest.entries[buildId] && !forceRefresh) {
         manifest.touch(buildId);
         logger.info(`Source ${persistSource.name} up to date, skipping`, {
            buildId,
         });
         return { name: persistSource.name, status: "skipped", buildId };
      }

      const buildSQL = persistSource.getSQL({
         buildManifest: manifest.buildManifest,
         connectionDigests,
      });

      const connectionName = persistSource.connectionName;
      const tableName =
         persistSource.tagParse({ prefix: /^#@ / }).tag.text("name") ||
         persistSource.name;
      const { schemaPrefix, bareName } = splitTablePath(tableName);
      const stagingTableName = `${schemaPrefix}${bareName}${stagingSuffix(buildId)}`;
      const dialect = persistSource.dialect;
      const quoted = (p: string) => quoteTablePath(p, dialect);

      // Guard: refuse to overwrite a pre-existing table that was not
      // created by a previous materialization build. Without this check a
      // model author could accidentally target a table name that already
      // holds real data (e.g. `#@ persist name=customers`), and the
      // DROP TABLE below would silently destroy it.
      const tableKey = manifestTableKey(connectionName, tableName);
      if (!knownMaterializedTables.has(tableKey)) {
         if (await tablePhysicallyExists(connection, quoted(tableName))) {
            throw new BadRequestError(
               `Refusing to materialize source '${persistSource.name}': ` +
                  `target table '${tableName}' already exists on connection ` +
                  `'${connectionName}' but was not created by a previous ` +
                  `materialization build. Use '#@ persist name=...' to ` +
                  `choose a different table name, or drop the existing ` +
                  `table manually if it is no longer needed.`,
            );
         }
      }

      logger.info(`Building source ${persistSource.name}`, {
         tableName,
         connectionName,
      });

      const startTime = performance.now();

      await connection.runSQL(
         `DROP TABLE IF EXISTS ${quoted(stagingTableName)}`,
      );

      // If any step after CREATE throws we must best-effort drop the
      // staging table, else it orphans under a name that GC will never
      // find (no manifest row is written for a failed build).
      try {
         await connection.runSQL(
            `CREATE TABLE ${quoted(stagingTableName)} AS (${buildSQL})`,
         );
         await connection.runSQL(`DROP TABLE IF EXISTS ${quoted(tableName)}`);
         await connection.runSQL(
            `ALTER TABLE ${quoted(stagingTableName)} RENAME TO ${dialect.quoteTablePath(bareName)}`,
         );
      } catch (err) {
         try {
            await connection.runSQL(
               `DROP TABLE IF EXISTS ${quoted(stagingTableName)}`,
            );
         } catch (cleanupErr) {
            logger.warn(
               "Build: failed to clean up staging table after a failed rebuild; physical leak",
               {
                  stagingTableName,
                  connectionName,
                  cleanupError:
                     cleanupErr instanceof Error
                        ? cleanupErr.message
                        : String(cleanupErr),
               },
            );
         }
         throw err;
      }

      const duration = performance.now() - startTime;

      knownMaterializedTables.add(tableKey);
      manifest.update(buildId, { tableName });

      await this.manifestService.writeEntry(
         projectId,
         packageName,
         buildId,
         tableName,
         persistSource.name,
         connectionName,
      );

      logger.info(`Built source ${persistSource.name}`, {
         tableName,
         durationMs: Math.round(duration),
      });

      return {
         name: persistSource.name,
         status: "built",
         buildId,
         tableName,
         durationMs: Math.round(duration),
      };
   }

   /**
    * Post-build GC: drop physical tables + manifest rows for entries whose
    * BuildID is no longer produced by an active persist source.
    *
    * `liveTables` prevents a fresh build from having its table dropped when
    * a stale row still references the same `(connection, tableName)` pair.
    */
   private async runPostBuildGc(
      manifest: Manifest,
      projectId: string,
      packageName: string,
      connections: Map<string, MalloyConnection>,
   ): Promise<GcResult> {
      const activeManifest = manifest.activeEntries;
      const allDbEntries = await this.manifestService.listEntries(
         projectId,
         packageName,
      );

      const liveTables = new Set<string>();
      for (const entry of allDbEntries) {
         if (activeManifest.entries[entry.buildId]) {
            liveTables.add(liveTableKey(entry.connectionName, entry.tableName));
         }
      }

      const staleEntries = allDbEntries.filter(
         (entry) => !activeManifest.entries[entry.buildId],
      );

      const gcResult = await dropManifestEntries(staleEntries, {
         connections,
         manifestService: this.manifestService,
         projectId,
         liveTables,
      });

      if (gcResult.errors.length > 0) {
         logger.warn("Materialization GC surfaced errors", {
            errorCount: gcResult.errors.length,
            droppedCount: gcResult.dropped.length,
         });
      }

      return gcResult;
   }

   // ==================== HELPERS ====================

   private resolveProjectId(projectName: string): Promise<string> {
      return resolveProjectId(this.repository, projectName);
   }
}
