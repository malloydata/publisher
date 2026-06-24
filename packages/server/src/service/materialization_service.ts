import type {
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
import {
   MaterializationMode,
   recordAutoLoadOutcome,
   recordDropTables,
   recordMaterializationRun,
   recordSourceBuildDuration,
   recordSourcesOutcome,
} from "../materialization_metrics";
import {
   BuildInstruction,
   BuildManifest,
   BuildManifestResult,
   BuildPlan,
   Materialization,
   MaterializationStatus,
   MaterializationUpdate,
   ManifestEntry,
   ResourceRepository,
} from "../storage/DatabaseInterface";
import { DuplicateActiveMaterializationError } from "../storage/duckdb/MaterializationRepository";
import {
   CompiledBuildPlan,
   compilePackageBuildPlan,
   computeBuildId,
} from "./build_plan";
import { EnvironmentStore } from "./environment_store";
import { splitTablePath } from "./quoting";
import { resolveEnvironmentId } from "./resolve_environment";

/**
 * Length of the buildId prefix used when synthesizing staging table names.
 * buildId is a 64-char SHA-256 hex string; 12 hex chars is 48 bits of
 * entropy, well inside every dialect's identifier limit (Postgres is the
 * tightest at 63).
 */
const STAGING_BUILD_ID_LEN = 12;

/** Staging suffix appended to a table name while it is being built. */
export function stagingSuffix(buildId: string): string {
   return `_${buildId.substring(0, STAGING_BUILD_ID_LEN)}`;
}

/**
 * Physical table name the publisher self-assigns in auto-run mode: the
 * `#@ persist name=<table>` value if present, else the Malloy source name.
 * The author owns quoting the `name=` value for the dialect.
 */
function selfAssignTableName(persistSource: PersistSource): string {
   try {
      return (
         persistSource.annotations.parseAsTag("@").tag.text("name") ||
         persistSource.name
      );
   } catch {
      return persistSource.name;
   }
}

/** Classify a thrown build error as cancelled (cooperative abort) or failed. */
function outcomeFor(
   _err: unknown,
   signal: AbortSignal | undefined,
): "failed" | "cancelled" {
   return signal?.aborted ? "cancelled" : "failed";
}

/**
 * Allowed status transitions. The build runs without an intermediate
 * plan-ready pause: PENDING advances straight to MANIFEST_ROWS_READY once the
 * tables are built, then to MANIFEST_FILE_READY. MANIFEST_FILE_READY, FAILED,
 * and CANCELLED are terminal.
 */
const VALID_TRANSITIONS: Record<
   MaterializationStatus,
   MaterializationStatus[]
> = {
   PENDING: ["MANIFEST_ROWS_READY", "FAILED", "CANCELLED"],
   MANIFEST_ROWS_READY: ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"],
   MANIFEST_FILE_READY: [],
   FAILED: [],
   CANCELLED: [],
};

/**
 * Orchestrates single-call materialization builds.
 *
 * The build plan is a deterministic property of the compiled package
 * (`Package.buildPlan`), so there is no separate plan round-trip. On create
 * the publisher either auto-runs (self-assigns physical names from the
 * `#@ persist name=` annotation and builds + auto-loads every persist source)
 * or, when the caller supplies `buildInstructions` derived from
 * `Package.buildPlan`, builds directly into the caller-assigned names without
 * auto-loading the manifest. Both paths build in the background and return the
 * PENDING record immediately.
 *
 * At most one active materialization per (environment, package) is enforced
 * by the DB-level unique index on `materializations.active_key` (see
 * {@link MaterializationRepository}). Cancellation is cooperative via
 * AbortController.
 */
export class MaterializationService {
   /** In-flight runs, so they can be cancelled. In-process only. */
   private runningAbortControllers = new Map<string, AbortController>();

   constructor(private environmentStore: EnvironmentStore) {}

   private get repository(): ResourceRepository {
      return this.environmentStore.storageManager.getRepository();
   }

   // ==================== STATE MACHINE ====================

   private validateTransition(
      current: MaterializationStatus,
      next: MaterializationStatus,
   ): void {
      if (!VALID_TRANSITIONS[current].includes(next)) {
         throw new InvalidStateTransitionError(
            `Cannot transition from ${current} to ${next}`,
         );
      }
   }

   private async transition(
      id: string,
      next: MaterializationStatus,
      extra?: Omit<MaterializationUpdate, "status">,
   ): Promise<Materialization> {
      const current = await this.repository.getMaterializationById(id);
      if (!current) {
         throw new MaterializationNotFoundError(
            `Materialization ${id} not found`,
         );
      }
      this.validateTransition(current.status, next);
      // Terminal transitions are operationally interesting (info); the
      // intermediate MANIFEST_ROWS_READY hop is routine bookkeeping (debug).
      const terminal =
         next === "MANIFEST_FILE_READY" ||
         next === "FAILED" ||
         next === "CANCELLED";
      logger[terminal ? "info" : "debug"]("Materialization transition", {
         materializationId: id,
         packageName: current.packageName,
         from: current.status,
         to: next,
      });
      return this.repository.updateMaterialization(id, {
         status: next,
         ...extra,
      });
   }

   // ==================== QUERIES ====================

   async listMaterializations(
      environmentName: string,
      packageName: string,
      options?: { limit?: number; offset?: number },
   ): Promise<Materialization[]> {
      const environmentId = await this.resolveEnvironmentId(environmentName);
      return this.repository.listMaterializations(
         environmentId,
         packageName,
         options,
      );
   }

   async getMaterialization(
      environmentName: string,
      packageName: string,
      id: string,
   ): Promise<Materialization> {
      const environmentId = await this.resolveEnvironmentId(environmentName);
      const m = await this.repository.getMaterializationById(id);
      if (
         !m ||
         m.environmentId !== environmentId ||
         m.packageName !== packageName
      ) {
         throw new MaterializationNotFoundError(
            `Materialization ${id} not found for package ${packageName}`,
         );
      }
      return m;
   }

   // ==================== CREATE + BUILD ====================

   /**
    * Create a materialization and build it in the background. Returns the
    * PENDING record immediately.
    *
    * Auto-run (default, no `buildInstructions`): self-assign physical names and
    * build + auto-load every persist source. Orchestrated (`buildInstructions`
    * present): build directly into the caller-assigned names from the package's
    * build plan, without auto-loading the manifest. When `buildInstructions` is
    * present it is validated synchronously against the package's compiled build
    * plan so a bad instruction is rejected at create time rather than failing
    * the background run.
    */
   async createMaterialization(
      environmentName: string,
      packageName: string,
      options: {
         forceRefresh?: boolean;
         sourceNames?: string[];
         buildInstructions?: BuildInstruction[];
      } = {},
   ): Promise<Materialization> {
      const environmentId = await this.resolveEnvironmentId(environmentName);

      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const pkg = await environment.getPackage(packageName, false);

      const buildInstructions = options.buildInstructions;
      const orchestrated = buildInstructions !== undefined;
      if (orchestrated) {
         this.validateInstructions(pkg.getBuildPlan(), buildInstructions);
      }

      const active = await this.repository.getActiveMaterialization(
         environmentId,
         packageName,
      );
      if (active) {
         throw new MaterializationConflictError(
            `Package ${packageName} already has an active materialization (${active.id})`,
         );
      }

      const forceRefresh = options.forceRefresh ?? false;
      const metadata = {
         forceRefresh,
         sourceNames: options.sourceNames ?? null,
         mode: orchestrated ? "orchestrated" : "auto",
      };

      let created: Materialization;
      try {
         created = await this.repository.createMaterialization(
            environmentId,
            packageName,
            "PENDING",
            metadata,
         );
      } catch (err) {
         if (err instanceof DuplicateActiveMaterializationError) {
            const winner = await this.repository.getActiveMaterialization(
               environmentId,
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

      this.runInBackground(created.id, (signal) =>
         this.runBuild(
            created.id,
            environmentName,
            packageName,
            {
               sourceNames: options.sourceNames,
               forceRefresh,
               buildInstructions,
            },
            signal,
         ),
      );

      return created;
   }

   /**
    * Single-call build, shared by auto-run and orchestrated mode. Compiles the
    * package build plan, derives the build instructions (self-assigned for
    * auto-run; caller-supplied for orchestrated), builds the instructed sources
    * into their physical tables, and commits the manifest. Auto-run additionally
    * loads the fresh manifest into the package models; orchestrated leaves
    * distribution to the caller.
    */
   private async runBuild(
      id: string,
      environmentName: string,
      packageName: string,
      opts: {
         sourceNames: string[] | undefined;
         forceRefresh: boolean;
         buildInstructions: BuildInstruction[] | undefined;
      },
      signal: AbortSignal,
   ): Promise<void> {
      const orchestrated = opts.buildInstructions !== undefined;
      const mode: MaterializationMode = orchestrated ? "orchestrated" : "auto";
      logger.info("Materialization build started", {
         materializationId: id,
         packageName,
         mode,
      });
      const startedAt = Date.now();

      try {
         const environmentId = await this.resolveEnvironmentId(environmentName);
         const environment = await this.environmentStore.getEnvironment(
            environmentName,
            false,
         );
         const pkg = await environment.getPackage(packageName, false);

         const compiled = await environment.withPackageLock(packageName, () =>
            compilePackageBuildPlan(pkg, signal),
         );

         let instructions: BuildInstruction[];
         let carried: Record<string, ManifestEntry>;
         if (orchestrated) {
            instructions = opts.buildInstructions!;
            carried = {};
         } else {
            // Skip-if-unchanged: reuse tables from the most recent successful
            // manifest for sources whose buildId is unchanged, unless
            // forceRefresh.
            const priorEntries = opts.forceRefresh
               ? {}
               : await this.getMostRecentManifestEntries(
                    environmentId,
                    packageName,
                    id,
                 );
            ({ instructions, carried } = this.deriveSelfInstructions(
               compiled,
               opts.sourceNames,
               priorEntries,
            ));
         }

         const entries = await this.executeInstructedBuild(
            compiled,
            instructions,
            carried,
            signal,
         );

         const sourcesBuilt = instructions.length;
         const sourcesReused = Object.keys(carried).length;
         const durationMs = Date.now() - startedAt;
         await this.commitManifest(id, entries, {
            forceRefresh: opts.forceRefresh,
            sourceNames: opts.sourceNames ?? null,
            mode,
            sourcesBuilt,
            sourcesReused,
            durationMs,
         });

         // Auto-run owns distribution: load the fresh manifest into the package
         // models so subsequent queries resolve to the materialized tables.
         // Orchestrated leaves distribution to the caller (manifestLocation).
         if (!orchestrated) {
            await this.autoLoadManifest(environment, packageName, entries);
         }

         recordSourcesOutcome("built", sourcesBuilt);
         recordSourcesOutcome("reused", sourcesReused);
         this.recordRun(mode, "success", startedAt);
         logger.info("Materialization build complete", {
            materializationId: id,
            packageName,
            mode,
            sourcesBuilt,
            sourcesReused,
            durationMs,
         });
      } catch (err) {
         this.recordRun(mode, outcomeFor(err, signal), startedAt);
         throw err;
      }
   }

   /**
    * Derive the publisher's own build instructions for auto-run. Each persist
    * source (respecting the optional sourceNames filter) gets a self-assigned
    * physical table name and COPY realization, unless its buildId is unchanged
    * since `priorEntries` — those are carried forward (reused) instead of
    * rebuilt.
    */
   private deriveSelfInstructions(
      compiled: CompiledBuildPlan,
      sourceNames: string[] | undefined,
      priorEntries: Record<string, ManifestEntry>,
   ): {
      instructions: BuildInstruction[];
      carried: Record<string, ManifestEntry>;
   } {
      const include = sourceNames ? new Set(sourceNames) : null;
      const instructions: BuildInstruction[] = [];
      const carried: Record<string, ManifestEntry> = {};
      const seen = new Set<string>();

      for (const graph of compiled.graphs) {
         for (const level of graph.nodes) {
            for (const node of level) {
               const persistSource = compiled.sources[node.sourceID];
               if (!persistSource) continue;
               if (include && !include.has(persistSource.name)) continue;

               const buildId = computeBuildId(
                  persistSource,
                  compiled.connectionDigests,
               );
               if (seen.has(buildId)) continue;
               seen.add(buildId);

               const prior = priorEntries[buildId];
               if (prior && prior.physicalTableName) {
                  carried[buildId] = prior;
                  continue;
               }

               instructions.push({
                  buildId,
                  materializedTableId: `local-${buildId.substring(
                     0,
                     STAGING_BUILD_ID_LEN,
                  )}`,
                  physicalTableName: selfAssignTableName(persistSource),
                  realization: "COPY",
               });
            }
         }
      }

      return { instructions, carried };
   }

   /**
    * Entries of the most recent successful (MANIFEST_FILE_READY) materialization
    * for this package, used for skip-if-unchanged. Excludes the in-flight run.
    */
   private async getMostRecentManifestEntries(
      environmentId: string,
      packageName: string,
      excludeId: string,
   ): Promise<Record<string, ManifestEntry>> {
      const list = await this.repository.listMaterializations(
         environmentId,
         packageName,
      );
      for (const m of list) {
         if (m.id === excludeId) continue;
         if (m.status === "MANIFEST_FILE_READY" && m.manifest?.entries) {
            return m.manifest.entries;
         }
      }
      return {};
   }

   /**
    * Load a freshly produced manifest into the package's models so persist
    * references resolve to the materialized tables. Best-effort: a load failure
    * is logged, not fatal (the run already reached MANIFEST_FILE_READY).
    */
   private async autoLoadManifest(
      environment: {
         reloadAllModelsForPackage(
            packageName: string,
            manifest: BuildManifest["entries"],
         ): Promise<void>;
      },
      packageName: string,
      entries: Record<string, ManifestEntry>,
   ): Promise<void> {
      const manifestEntries: BuildManifest["entries"] = {};
      for (const [buildId, entry] of Object.entries(entries)) {
         if (entry.physicalTableName) {
            manifestEntries[buildId] = { tableName: entry.physicalTableName };
         }
      }
      try {
         await environment.reloadAllModelsForPackage(
            packageName,
            manifestEntries,
         );
         recordAutoLoadOutcome("success");
         logger.info("Auto-run: loaded manifest into package models", {
            packageName,
            entryCount: Object.keys(manifestEntries).length,
         });
      } catch (err) {
         recordAutoLoadOutcome("failure");
         logger.warn("Auto-run: failed to load manifest into package models", {
            packageName,
            error: err instanceof Error ? err.message : String(err),
         });
      }
   }

   /**
    * Validate caller-supplied build instructions against the package's compiled
    * build plan: every instructed buildId must be a planned source, and only
    * COPY realization is supported. Throws when the package declares no persist
    * source (no plan to build against).
    */
   private validateInstructions(
      plan: BuildPlan | null,
      instructions: BuildInstruction[],
   ): void {
      if (!plan) {
         throw new BadRequestError(
            "Package has no persist sources; buildInstructions cannot be applied",
         );
      }
      const plannedBuildIds = new Set<string>();
      for (const source of Object.values(plan.sources)) {
         plannedBuildIds.add(source.buildId);
      }

      for (const instruction of instructions) {
         if (!plannedBuildIds.has(instruction.buildId)) {
            throw new BadRequestError(
               `Instruction references unknown buildId '${instruction.buildId}'`,
            );
         }
         // COPY-only for now; SNAPSHOT lands once clone semantics are defined.
         if (instruction.realization === "SNAPSHOT") {
            throw new BadRequestError(
               "realization=SNAPSHOT is not supported (COPY only)",
            );
         }
      }
   }

   /**
    * Shared build loop for both auto-run and orchestrated builds. Seeds the
    * manifest with carried-forward (reused) upstream entries so downstream
    * references resolve, then builds each instructed source in dependency
    * order. Returns the full entry map (carried + freshly built). The package
    * was compiled by the caller; this runs outside the package lock.
    */
   private async executeInstructedBuild(
      compiled: CompiledBuildPlan,
      instructions: BuildInstruction[],
      seedEntries: Record<string, ManifestEntry>,
      signal: AbortSignal,
   ): Promise<Record<string, ManifestEntry>> {
      const { graphs, sources, connectionDigests, connections } = compiled;

      const byBuildId = new Map<string, BuildInstruction>();
      for (const instruction of instructions) {
         byBuildId.set(instruction.buildId, instruction);
      }

      // Accumulates physical names as sources are built so downstream sources
      // resolve their upstream references to the freshly-assigned tables. Seed
      // it with carried-forward entries so reused upstreams resolve too.
      const manifest = new Manifest();
      const entries: Record<string, ManifestEntry> = {};
      for (const [buildId, entry] of Object.entries(seedEntries)) {
         if (entry.physicalTableName) {
            manifest.update(buildId, { tableName: entry.physicalTableName });
         }
         entries[buildId] = entry;
      }

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
               if (!persistSource) continue;

               const buildId = computeBuildId(persistSource, connectionDigests);
               const instruction = byBuildId.get(buildId);
               if (!instruction) continue;

               const entry = await this.buildOneSource(
                  persistSource,
                  instruction,
                  connection,
                  connectionDigests,
                  manifest,
               );
               entries[buildId] = entry;
            }
         }
      }

      return entries;
   }

   /**
    * Build a single instructed source into its assigned physical table.
    * COPY uses a staging table + atomic rename for crash-safety; the staging
    * name derives from the buildId. Records and returns the manifest entry.
    */
   private async buildOneSource(
      persistSource: PersistSource,
      instruction: BuildInstruction,
      connection: MalloyConnection,
      connectionDigests: Record<string, string>,
      manifest: Manifest,
   ): Promise<ManifestEntry> {
      const buildId = instruction.buildId;
      const physicalTableName = instruction.physicalTableName;
      const buildSQL = persistSource.getSQL({
         buildManifest: manifest.buildManifest,
         connectionDigests,
      });

      const { bareName } = splitTablePath(physicalTableName);
      const stagingTableName = `${physicalTableName}${stagingSuffix(buildId)}`;

      const startTime = performance.now();
      await connection.runSQL(`DROP TABLE IF EXISTS ${stagingTableName}`);
      try {
         await connection.runSQL(
            `CREATE TABLE ${stagingTableName} AS (${buildSQL})`,
         );
         await connection.runSQL(`DROP TABLE IF EXISTS ${physicalTableName}`);
         await connection.runSQL(
            `ALTER TABLE ${stagingTableName} RENAME TO ${bareName}`,
         );
      } catch (err) {
         try {
            await connection.runSQL(`DROP TABLE IF EXISTS ${stagingTableName}`);
         } catch (cleanupErr) {
            logger.warn(
               "Failed to clean up staging table after a failed build; physical leak",
               {
                  stagingTableName,
                  connectionName: persistSource.connectionName,
                  cleanupError:
                     cleanupErr instanceof Error
                        ? cleanupErr.message
                        : String(cleanupErr),
               },
            );
         }
         throw err;
      }

      // Make this table visible to downstream sources built later in this run.
      manifest.update(buildId, { tableName: physicalTableName });

      const durationMs = Math.round(performance.now() - startTime);
      recordSourceBuildDuration(durationMs);
      logger.info(`Built materialized source ${persistSource.name}`, {
         physicalTableName,
         durationMs,
      });

      return {
         buildId,
         sourceName: persistSource.name,
         materializedTableId: instruction.materializedTableId,
         physicalTableName,
         connectionName: persistSource.connectionName,
         realization: instruction.realization,
         rowCount: null,
      };
   }

   // ==================== CANCELLATION ====================

   /** Cancel a non-terminal materialization. */
   async stopMaterialization(
      environmentName: string,
      packageName: string,
      id: string,
   ): Promise<Materialization> {
      const m = await this.getMaterialization(environmentName, packageName, id);

      const cancellable: MaterializationStatus[] = [
         "PENDING",
         "MANIFEST_ROWS_READY",
      ];
      if (!cancellable.includes(m.status)) {
         throw new InvalidStateTransitionError(
            `Materialization ${id} is ${m.status}, cannot stop`,
         );
      }

      const abortController = this.runningAbortControllers.get(id);
      if (abortController) {
         abortController.abort();
         return m;
      }
      return this.transition(id, "CANCELLED", {
         completedAt: new Date(),
         error: "Cancelled",
      });
   }

   /**
    * Delete a materialization record. Only terminal materializations
    * (MANIFEST_FILE_READY, FAILED, CANCELLED) can be deleted; an active run must
    * be stopped first.
    *
    * By default this removes the publisher's record only — physical-table GC is
    * the caller's responsibility. When `dropTables` is set, the publisher
    * additionally drops the physical tables recorded in this run's manifest as a
    * best-effort cleanup (a drop failure is logged, not fatal, so the record
    * still deletes).
    */
   async deleteMaterialization(
      environmentName: string,
      packageName: string,
      id: string,
      options: { dropTables?: boolean } = {},
   ): Promise<void> {
      const m = await this.getMaterialization(environmentName, packageName, id);

      const terminal: MaterializationStatus[] = [
         "MANIFEST_FILE_READY",
         "FAILED",
         "CANCELLED",
      ];
      if (!terminal.includes(m.status)) {
         throw new InvalidStateTransitionError(
            `Cannot delete materialization ${id} while it is ${m.status}`,
         );
      }

      if (options.dropTables) {
         await this.dropMaterializedTables(environmentName, packageName, m);
      }

      await this.repository.deleteMaterialization(id);
   }

   /**
    * Best-effort drop of every physical table this run produced, read from the
    * materialization's manifest. Resolves each entry's connection by name and
    * issues `DROP TABLE IF EXISTS` for the physical table and its (possible)
    * leftover staging table. Failures are logged and swallowed so a partial
    * cleanup never blocks deletion of the record.
    */
   private async dropMaterializedTables(
      environmentName: string,
      packageName: string,
      m: Materialization,
   ): Promise<void> {
      const entries = m.manifest?.entries;
      if (!entries || Object.keys(entries).length === 0) {
         return;
      }

      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const pkg = await environment.getPackage(packageName, false);
      const connectionCache = new Map<string, MalloyConnection>();

      for (const entry of Object.values(entries)) {
         const connectionName = entry.connectionName;
         const physicalTableName = entry.physicalTableName;
         if (!connectionName || !physicalTableName) {
            logger.warn("Skipping manifest entry with no connection/table", {
               materializationId: m.id,
               buildId: entry.buildId,
            });
            continue;
         }

         try {
            let connection = connectionCache.get(connectionName);
            if (!connection) {
               connection = await pkg.getMalloyConnection(connectionName);
               connectionCache.set(connectionName, connection);
            }
            await connection.runSQL(
               `DROP TABLE IF EXISTS ${physicalTableName}`,
            );
            // A crash between staging-create and rename can leave the staging
            // table behind; clean it up too while we hold the connection.
            await connection.runSQL(
               `DROP TABLE IF EXISTS ${physicalTableName}${stagingSuffix(
                  entry.buildId,
               )}`,
            );
            recordDropTables("success");
            logger.info("Dropped materialized table on delete", {
               materializationId: m.id,
               physicalTableName,
               connectionName,
            });
         } catch (err) {
            recordDropTables("failure");
            logger.warn("Failed to drop materialized table on delete", {
               materializationId: m.id,
               physicalTableName,
               connectionName,
               error: err instanceof Error ? err.message : String(err),
            });
         }
      }
   }

   /**
    * Finalize a successful build: advance to MANIFEST_ROWS_READY then
    * MANIFEST_FILE_READY and persist the assembled manifest. The caller
    * supplies the per-run metadata. The build itself happens before this.
    */
   private async commitManifest(
      id: string,
      entries: Record<string, ManifestEntry>,
      metadata: Record<string, unknown>,
   ): Promise<void> {
      await this.transition(id, "MANIFEST_ROWS_READY");
      const manifest: BuildManifestResult = {
         builtAt: new Date().toISOString(),
         entries,
         strict: false,
      };
      await this.transition(id, "MANIFEST_FILE_READY", {
         completedAt: new Date(),
         manifest,
         metadata,
      });
   }

   // ==================== HELPERS ====================

   private recordRun(
      mode: MaterializationMode,
      outcome: "success" | "failed" | "cancelled",
      startedAtMs: number,
   ): void {
      recordMaterializationRun(mode, outcome, Date.now() - startedAtMs);
   }

   private runInBackground(
      id: string,
      run: (signal: AbortSignal) => Promise<void>,
   ): void {
      const abortController = new AbortController();
      this.runningAbortControllers.set(id, abortController);

      run(abortController.signal)
         .catch(async (err) => {
            const message = err instanceof Error ? err.message : String(err);
            const next = abortController.signal.aborted
               ? "CANCELLED"
               : "FAILED";
            try {
               await this.repository.updateMaterialization(id, {
                  status: next,
                  completedAt: new Date(),
                  error: abortController.signal.aborted ? "Cancelled" : message,
               });
            } catch (transitionErr) {
               logger.error("Failed to record materialization failure", {
                  materializationId: id,
                  originalError: message,
                  transitionError:
                     transitionErr instanceof Error
                        ? transitionErr.message
                        : String(transitionErr),
               });
            }
         })
         .finally(() => {
            this.runningAbortControllers.delete(id);
         });
   }

   private resolveEnvironmentId(environmentName: string): Promise<string> {
      return resolveEnvironmentId(this.repository, environmentName);
   }
}
