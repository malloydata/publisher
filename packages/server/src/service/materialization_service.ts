import type {
   AtomicField,
   BuildGraph as MalloyBuildGraph,
   MalloyConfig,
   Connection as MalloyConnection,
   PersistSource,
} from "@malloydata/malloy";
import { Manifest } from "@malloydata/malloy";
import { components } from "../api";
import {
   BadRequestError,
   InvalidStateTransitionError,
   MaterializationConflictError,
   MaterializationNotFoundError,
} from "../errors";
import { logger } from "../logger";
import {
   MaterializationRound,
   recordMaterializationRound,
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
import { EnvironmentStore } from "./environment_store";
import { Model } from "./model";
import { splitTablePath } from "./quoting";
import { resolveEnvironmentId } from "./resolve_environment";

type WireBuildGraph = components["schemas"]["BuildGraph"];
type WirePersistSourcePlan = components["schemas"]["PersistSourcePlan"];
type WireColumn = components["schemas"]["Column"];

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

/** Output columns of a persist source, degrading to [] if unavailable. */
function deriveColumns(persistSource: PersistSource): WireColumn[] {
   try {
      return persistSource._explore.intrinsicFields
         .filter((f) => f.isAtomicField())
         .map((f) => ({
            name: f.name,
            type: String((f as AtomicField).type),
         }));
   } catch (err) {
      logger.warn("Failed to derive columns for persist source", {
         sourceID: persistSource.sourceID,
         error: err instanceof Error ? err.message : String(err),
      });
      return [];
   }
}

/** Flatten Malloy's nested BuildNode.dependsOn into a list of sourceIDs. */
function flattenDependsOn(node: {
   dependsOn: { sourceID: string }[];
}): string[] {
   return node.dependsOn.map((d) => d.sourceID);
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

/** Classify a thrown round error as cancelled (cooperative abort) or failed. */
function outcomeFor(
   _err: unknown,
   signal: AbortSignal | undefined,
): "failed" | "cancelled" {
   return signal?.aborted ? "cancelled" : "failed";
}

/**
 * Resolve a Map<name, Connection> for the names a step is about to touch.
 * The package's MalloyConfig caches each lookup, so repeated calls are cheap.
 * A failed lookup is logged and omitted; downstream code reports the missing
 * connection explicitly.
 */
async function resolvePackageConnections(
   pkg: { getMalloyConnection(name: string): Promise<MalloyConnection> },
   names: Iterable<string>,
): Promise<Map<string, MalloyConnection>> {
   const map = new Map<string, MalloyConnection>();
   const seen = new Set<string>();
   for (const name of names) {
      if (!name || seen.has(name)) continue;
      seen.add(name);
      try {
         map.set(name, await pkg.getMalloyConnection(name));
      } catch (err) {
         logger.warn(`Failed to resolve connection ${name}`, {
            error: err instanceof Error ? err.message : String(err),
         });
      }
   }
   return map;
}

/**
 * Allowed status transitions for the two-round protocol. MANIFEST_FILE_READY,
 * FAILED, and CANCELLED are terminal.
 */
const VALID_TRANSITIONS: Record<
   MaterializationStatus,
   MaterializationStatus[]
> = {
   PENDING: ["BUILD_PLAN_READY", "FAILED", "CANCELLED"],
   BUILD_PLAN_READY: ["MANIFEST_ROWS_READY", "FAILED", "CANCELLED"],
   MANIFEST_ROWS_READY: ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"],
   MANIFEST_FILE_READY: [],
   FAILED: [],
   CANCELLED: [],
};

/**
 * Orchestrates the two-round materialization protocol.
 *
 * Round 1 (on create): compile the package, compute a build plan (per-source
 * buildId, columns, lineage inputs, connection capability) and pause at
 * BUILD_PLAN_READY without writing any tables. Round 2 (action=build): build
 * only the control-plane-instructed sources into their assigned physical
 * names and assemble the manifest, which is returned inline for the control
 * plane to persist.
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

   // ==================== ROUND 1: CREATE + PLAN ====================

   /**
    * Create a materialization and start Round 1 (compile + plan) in the
    * background. Returns the PENDING record immediately.
    */
   async createMaterialization(
      environmentName: string,
      packageName: string,
      options: {
         forceRefresh?: boolean;
         sourceNames?: string[];
         pauseBetweenPhases?: boolean;
      } = {},
   ): Promise<Materialization> {
      const environmentId = await this.resolveEnvironmentId(environmentName);

      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      await environment.getPackage(packageName, false);

      const active = await this.repository.getActiveMaterialization(
         environmentId,
         packageName,
      );
      if (active) {
         throw new MaterializationConflictError(
            `Package ${packageName} already has an active materialization (${active.id})`,
         );
      }

      const pauseBetweenPhases = options.pauseBetweenPhases ?? false;
      const forceRefresh = options.forceRefresh ?? false;
      const metadata = {
         forceRefresh,
         sourceNames: options.sourceNames ?? null,
         pauseBetweenPhases,
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

      // Default (auto-run): compile, self-assign names, build, and auto-load in
      // one pass. Opt-in two-round: pause at BUILD_PLAN_READY for the control
      // plane to drive Round 2.
      this.runInBackground(created.id, (signal) =>
         pauseBetweenPhases
            ? this.runRound1(
                 created.id,
                 environmentName,
                 packageName,
                 options.sourceNames,
                 signal,
              )
            : this.runAuto(
                 created.id,
                 environmentName,
                 packageName,
                 options.sourceNames,
                 forceRefresh,
                 signal,
              ),
      );

      return created;
   }

   private async runRound1(
      id: string,
      environmentName: string,
      packageName: string,
      sourceNames: string[] | undefined,
      signal?: AbortSignal,
   ): Promise<void> {
      logger.info("Materialization Round 1: compile + plan", {
         materializationId: id,
         packageName,
      });
      const startedAt = Date.now();

      try {
         const environment = await this.environmentStore.getEnvironment(
            environmentName,
            false,
         );
         const pkg = await environment.getPackage(packageName, false);

         const { graphs, sources, connectionDigests } =
            await environment.withPackageLock(packageName, () =>
               this.compilePackageBuildPlan(pkg, signal),
            );

         const buildPlan = this.deriveBuildPlan(
            graphs,
            sources,
            connectionDigests,
            sourceNames,
         );

         await this.transition(id, "BUILD_PLAN_READY", { buildPlan });
         this.recordRound("round1", "success", startedAt);
         logger.info("Materialization Round 1 complete", {
            materializationId: id,
            packageName,
            sourceCount: Object.keys(buildPlan.sources).length,
            durationMs: Date.now() - startedAt,
         });
      } catch (err) {
         this.recordRound("round1", outcomeFor(err, signal), startedAt);
         throw err;
      }
   }

   // ==================== AUTO-RUN (DEFAULT) ====================

   /**
    * Auto-run (default, pauseBetweenPhases=false). Compile + plan, then
    * self-assign physical table names (the `#@ persist name=` value, else the
    * source name) with realization=COPY, skip sources whose buildId is
    * unchanged since the most recent successful manifest (unless forceRefresh),
    * build the rest, assemble the manifest, and auto-load it into the package
    * models so subsequent queries resolve to the materialized tables. No pause,
    * no second round.
    */
   private async runAuto(
      id: string,
      environmentName: string,
      packageName: string,
      sourceNames: string[] | undefined,
      forceRefresh: boolean,
      signal: AbortSignal,
   ): Promise<void> {
      logger.info("Materialization auto-run: compile + build + load", {
         materializationId: id,
         packageName,
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
            this.compilePackageBuildPlan(pkg, signal),
         );

         const buildPlan = this.deriveBuildPlan(
            compiled.graphs,
            compiled.sources,
            compiled.connectionDigests,
            sourceNames,
         );
         await this.transition(id, "BUILD_PLAN_READY", { buildPlan });

         // Skip-if-unchanged: reuse tables from the most recent successful
         // manifest for sources whose buildId is unchanged, unless forceRefresh.
         const priorEntries = forceRefresh
            ? {}
            : await this.getMostRecentManifestEntries(
                 environmentId,
                 packageName,
                 id,
              );
         const { instructions, carried } = this.deriveSelfInstructions(
            compiled,
            sourceNames,
            priorEntries,
         );

         const entries = await this.executeInstructedBuild(
            compiled,
            instructions,
            carried,
            signal,
         );

         await this.transition(id, "MANIFEST_ROWS_READY");

         const manifestResult: BuildManifestResult = {
            builtAt: new Date().toISOString(),
            entries,
            strict: false,
         };
         const durationMs = Date.now() - startedAt;
         await this.transition(id, "MANIFEST_FILE_READY", {
            completedAt: new Date(),
            manifest: manifestResult,
            metadata: {
               forceRefresh,
               sourceNames: sourceNames ?? null,
               pauseBetweenPhases: false,
               sourcesBuilt: instructions.length,
               sourcesReused: Object.keys(carried).length,
               durationMs,
            },
         });

         await this.autoLoadManifest(environment, packageName, entries);

         this.recordRound("auto", "success", startedAt);
         logger.info("Materialization auto-run complete", {
            materializationId: id,
            packageName,
            sourcesBuilt: instructions.length,
            sourcesReused: Object.keys(carried).length,
            durationMs,
         });
      } catch (err) {
         this.recordRound("auto", outcomeFor(err, signal), startedAt);
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
      compiled: {
         graphs: MalloyBuildGraph[];
         sources: Record<string, PersistSource>;
         connectionDigests: Record<string, string>;
      },
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

               const buildId = persistSource.makeBuildId(
                  compiled.connectionDigests[persistSource.connectionName],
                  persistSource.getSQL(),
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
         logger.info("Auto-run: loaded manifest into package models", {
            packageName,
            entryCount: Object.keys(manifestEntries).length,
         });
      } catch (err) {
         logger.warn("Auto-run: failed to load manifest into package models", {
            packageName,
            error: err instanceof Error ? err.message : String(err),
         });
      }
   }

   // ==================== ROUND 2: BUILD ====================

   /**
    * Round 2. Validates the instructions against the stored build plan,
    * transitions out of BUILD_PLAN_READY, and builds the instructed sources
    * in the background. Returns the accepted record (202).
    */
   async buildMaterialization(
      environmentName: string,
      packageName: string,
      id: string,
      instructions: BuildInstruction[],
   ): Promise<Materialization> {
      const m = await this.getMaterialization(environmentName, packageName, id);

      if (!m.pauseBetweenPhases) {
         throw new InvalidStateTransitionError(
            `Materialization ${id} is an auto-run materialization; action=build does not apply (set pauseBetweenPhases=true for the two-round flow)`,
         );
      }
      if (m.status !== "BUILD_PLAN_READY") {
         throw new InvalidStateTransitionError(
            `Materialization ${id} is ${m.status}, expected BUILD_PLAN_READY`,
         );
      }
      const plan = m.buildPlan;
      if (!plan) {
         throw new InvalidStateTransitionError(
            `Materialization ${id} has no build plan`,
         );
      }

      this.validateInstructions(plan, instructions);

      this.runInBackground(id, (signal) =>
         this.runRound2(id, environmentName, packageName, instructions, signal),
      );

      return m;
   }

   private validateInstructions(
      plan: BuildPlan,
      instructions: BuildInstruction[],
   ): void {
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
         // v0 is COPY-only; SNAPSHOT lands once clone semantics are defined.
         if (instruction.realization === "SNAPSHOT") {
            throw new BadRequestError(
               "realization=SNAPSHOT is not supported in v0 (COPY only)",
            );
         }
      }
   }

   private async runRound2(
      id: string,
      environmentName: string,
      packageName: string,
      instructions: BuildInstruction[],
      signal: AbortSignal,
   ): Promise<void> {
      logger.info("Materialization Round 2: build", {
         materializationId: id,
         packageName,
         sourceCount: instructions.length,
      });
      const startedAt = Date.now();

      try {
         const environment = await this.environmentStore.getEnvironment(
            environmentName,
            false,
         );
         const pkg = await environment.getPackage(packageName, false);

         const compiled = await environment.withPackageLock(packageName, () =>
            this.compilePackageBuildPlan(pkg, signal),
         );

         const entries = await this.executeInstructedBuild(
            compiled,
            instructions,
            {},
            signal,
         );

         await this.transition(id, "MANIFEST_ROWS_READY");

         const manifestResult: BuildManifestResult = {
            builtAt: new Date().toISOString(),
            entries,
            strict: false,
         };
         const durationMs = Date.now() - startedAt;
         await this.transition(id, "MANIFEST_FILE_READY", {
            completedAt: new Date(),
            manifest: manifestResult,
            metadata: {
               sourcesBuilt: Object.keys(entries).length,
               durationMs,
            },
         });

         this.recordRound("round2", "success", startedAt);
         logger.info("Materialization Round 2 complete", {
            materializationId: id,
            packageName,
            sourcesBuilt: Object.keys(entries).length,
            durationMs,
         });
      } catch (err) {
         this.recordRound("round2", outcomeFor(err, signal), startedAt);
         throw err;
      }
   }

   /**
    * Shared build loop for both auto-run and Round 2. Seeds the manifest with
    * carried-forward (reused) upstream entries so downstream references resolve,
    * then builds each instructed source in dependency order. Returns the full
    * entry map (carried + freshly built). The package was compiled by the
    * caller; this runs outside the package lock.
    */
   private async executeInstructedBuild(
      compiled: {
         graphs: MalloyBuildGraph[];
         sources: Record<string, PersistSource>;
         connectionDigests: Record<string, string>;
         connections: Map<string, MalloyConnection>;
      },
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

               const buildId = persistSource.makeBuildId(
                  connectionDigests[persistSource.connectionName],
                  persistSource.getSQL(),
               );
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
               "Round 2: failed to clean up staging table after a failed build; physical leak",
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

      // Make this table visible to downstream sources built later this round.
      manifest.update(buildId, { tableName: physicalTableName });

      logger.info(`Round 2 built source ${persistSource.name}`, {
         physicalTableName,
         durationMs: Math.round(performance.now() - startTime),
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
         "BUILD_PLAN_READY",
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
    * By default this removes the publisher's record only — the control plane
    * owns table GC. When `dropTables` is set, the publisher additionally drops
    * the physical tables recorded in this run's manifest as a best-effort
    * cleanup (a drop failure is logged, not fatal, so the record still deletes).
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
            logger.info("Dropped materialized table on delete", {
               materializationId: m.id,
               physicalTableName,
               connectionName,
            });
         } catch (err) {
            logger.warn("Failed to drop materialized table on delete", {
               materializationId: m.id,
               physicalTableName,
               connectionName,
               error: err instanceof Error ? err.message : String(err),
            });
         }
      }
   }

   // ==================== BUILD PLAN COMPILATION ====================

   /**
    * Compile every model in the package and collect the dependency-ordered
    * build graphs, persist sources, connection digests, and resolved
    * connections.
    */
   private async compilePackageBuildPlan(
      pkg: {
         getModelPaths(): string[];
         getPackagePath(): string;
         getMalloyConfig(): MalloyConfig;
         getMalloyConnection(name: string): Promise<MalloyConnection>;
      },
      signal?: AbortSignal,
   ): Promise<{
      graphs: MalloyBuildGraph[];
      sources: Record<string, PersistSource>;
      connectionDigests: Record<string, string>;
      connections: Map<string, MalloyConnection>;
   }> {
      const allGraphs: MalloyBuildGraph[] = [];
      const allSources: Record<string, PersistSource> = {};

      for (const modelPath of pkg.getModelPaths()) {
         if (signal?.aborted) throw new Error("Build cancelled");

         const { runtime, modelURL, importBaseURL } =
            await Model.getModelRuntime(
               pkg.getPackagePath(),
               modelPath,
               pkg.getMalloyConfig(),
            );
         const malloyModel = await runtime
            .loadModel(modelURL, { importBaseURL })
            .getModel();

         // getBuildPlan() returns empty graphs for models with no #@ persist
         // sources, so non-persist models are simply skipped below.
         const buildPlan = malloyModel.getBuildPlan();
         for (const msg of buildPlan.tagParseLog) {
            logger.warn("Persist annotation issue", {
               modelPath,
               message: msg.message,
               severity: msg.severity,
            });
         }
         if (buildPlan.graphs.length === 0) continue;

         allGraphs.push(...buildPlan.graphs);
         for (const [sourceID, source] of Object.entries(buildPlan.sources)) {
            allSources[sourceID] = source;
         }
      }

      const connections = await resolvePackageConnections(
         pkg,
         allGraphs.map((g) => g.connectionName),
      );
      const connectionDigests: Record<string, string> = {};
      for (const graph of allGraphs) {
         const conn = connections.get(graph.connectionName);
         if (conn && !connectionDigests[graph.connectionName]) {
            connectionDigests[graph.connectionName] = await conn.getDigest();
         }
      }

      return {
         graphs: allGraphs,
         sources: allSources,
         connectionDigests,
         connections,
      };
   }

   /** Project the Malloy build plan into the trimmed v0 wire BuildPlan. */
   private deriveBuildPlan(
      graphs: MalloyBuildGraph[],
      sources: Record<string, PersistSource>,
      connectionDigests: Record<string, string>,
      sourceNames: string[] | undefined,
   ): BuildPlan {
      const include = sourceNames ? new Set(sourceNames) : null;

      const wireGraphs: WireBuildGraph[] = graphs.map((graph) => ({
         connectionName: graph.connectionName,
         nodes: graph.nodes.map((level) =>
            level.map((node) => ({
               sourceID: node.sourceID,
               dependsOn: flattenDependsOn(node),
            })),
         ),
      }));

      const wireSources: Record<string, WirePersistSourcePlan> = {};
      for (const [sourceID, source] of Object.entries(sources)) {
         if (include && !include.has(source.name)) continue;
         wireSources[sourceID] = {
            name: source.name,
            sourceID: source.sourceID,
            connectionName: source.connectionName,
            dialect: source.dialectName,
            buildId: source.makeBuildId(
               connectionDigests[source.connectionName],
               source.getSQL(),
            ),
            sql: source.getSQL(),
            columns: deriveColumns(source),
         };
      }

      return { graphs: wireGraphs, sources: wireSources };
   }

   // ==================== HELPERS ====================

   private recordRound(
      round: MaterializationRound,
      outcome: "success" | "failed" | "cancelled",
      startedAtMs: number,
   ): void {
      recordMaterializationRound(round, outcome, Date.now() - startedAtMs);
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
