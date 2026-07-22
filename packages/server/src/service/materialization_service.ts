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
   recordManifestBindDegraded,
   recordMaterializationRun,
   recordSourceBuildDuration,
   recordSourcesOutcome,
} from "../materialization_metrics";
import {
   BuildInstruction,
   BuildManifestResult,
   BuildPlan,
   FreshnessManifest,
   Materialization,
   MaterializationStatus,
   MaterializationUpdate,
   ManifestEntry,
   ManifestReference,
   ResourceRepository,
} from "../storage/DatabaseInterface";
import { DuplicateActiveMaterializationError } from "../storage/duckdb/MaterializationRepository";
import { errMessage } from "../utils";
import {
   CompiledBuildPlan,
   compilePackageBuildPlan,
   computeSourceEntityId,
   deriveAnnotationFields,
   deriveColumns,
   iterGraphSources,
} from "./build_plan";
import { EnvironmentStore } from "./environment_store";
import {
   bareTableName,
   quoteIdentifier,
   quoteManifestTablePath,
   quoteTablePath,
} from "./quoting";
import { resolveEnvironmentId } from "./resolve_environment";

/**
 * Length of the sourceEntityId prefix used when synthesizing staging table
 * names. 12 hex chars is 48 bits of entropy, well inside every dialect's
 * identifier limit (Postgres is the tightest at 63).
 */
const STAGING_ID_LEN = 12;

/** Staging suffix appended to a table name while it is being built. */
export function stagingSuffix(sourceEntityId: string): string {
   // Drop hyphens so the suffix stays a bare identifier fragment when the id
   // becomes a UUID5 (a no-op for the current hex ids).
   return `_${sourceEntityId.replace(/-/g, "").substring(0, STAGING_ID_LEN)}`;
}

/**
 * Physical table name the publisher self-assigns in auto-run mode: the
 * `#@ persist name=<table>` value if present, else the Malloy source name.
 * The author owns quoting the `name=` value for the dialect.
 */
function selfAssignTableName(persistSource: PersistSource): string {
   return deriveAnnotationFields(persistSource).name || persistSource.name;
}

/** The declared `#@ persist ... refresh=...` value, defaulting to "full". */
export function refreshMode(
   persistSource: PersistSource,
): "full" | "incremental" {
   return deriveAnnotationFields(persistSource).refresh === "incremental"
      ? "incremental"
      : "full";
}

/**
 * Reserved CTE name for the compile-safe self-reference primitive. An
 * incremental source's SQL declares
 * `WITH __malloy_incremental_keys AS (<empty placeholder>)` and references it
 * (e.g. `WHERE key NOT IN (SELECT key FROM __malloy_incremental_keys)`). The
 * empty body compiles with no reference to the not-yet-existent target and, on
 * the first-run seed, matches nothing so every row is processed. On an
 * incremental MERGE the publisher hydrates the body to select the primary key
 * from the already-materialized target, so only new rows are computed — crucially
 * before an expensive step like ML.GENERATE_TEXT. This is the dbt
 * `is_incremental()` + `{{ this }}` analog, realized without a compiler change.
 */
const INCREMENTAL_KEYS_CTE = "__malloy_incremental_keys";

/**
 * Rewrite the reserved {@link INCREMENTAL_KEYS_CTE} body to select the primary
 * key from the target. Returns the SQL unchanged if the CTE is absent (the
 * source did not opt into the self-reference; the MERGE then upserts every row
 * the source returns). Boundary-safe: balances parentheses from the CTE's
 * opening paren.
 *
 * The CTE must be written in the canonical `WITH <name> AS ( ... )` form. The
 * anchor matches the name + `AS` keyword + opening paren together and is
 * case-INSENSITIVE on `AS`: a lowercase `as` is valid SQL, and a case-sensitive
 * scan would silently skip hydration, leaving the incremental filter inert (the
 * MERGE would then reprocess every row). Matching name+`AS` as a unit also
 * avoids anchoring on an `AS` substring inside an unrelated upstream identifier.
 * A column-list form (`<name> (cols) AS (...)`) is not recognized — the reserved
 * CTE needs no column list.
 */
export function hydrateIncrementalKeysCte(
   sql: string,
   quotedTarget: string,
   primaryKey: string,
   dialect: string,
): string {
   const anchor = new RegExp(`${INCREMENTAL_KEYS_CTE}\\s+AS\\s*\\(`, "i").exec(
      sql,
   );
   if (!anchor) return sql;
   const asOpen = anchor.index + anchor[0].length - 1; // index of the "("
   let depth = 0;
   let close = asOpen;
   for (; close < sql.length; close++) {
      if (sql[close] === "(") depth++;
      else if (sql[close] === ")") {
         depth--;
         if (depth === 0) break;
      }
   }
   const body = ` SELECT ${quoteIdentifier(primaryKey, dialect)} FROM ${quotedTarget} `;
   return sql.slice(0, asOpen + 1) + body + sql.slice(close);
}

/**
 * A standard SQL MERGE that upserts the source rows into the target on the
 * primary key: update non-key columns on match, insert on no-match. Append-once
 * (classify) falls out naturally when the source's candidate set excludes
 * already-present keys (via {@link INCREMENTAL_KEYS_CTE}); dedup upserts changed
 * rows. Idempotent under retry, unlike a raw INSERT.
 */
export function buildMergeSQL(
   quotedTarget: string,
   usingSQL: string,
   primaryKey: string,
   columns: string[],
   dialect: string,
): string {
   const q = (c: string) => quoteIdentifier(c, dialect);
   const pk = q(primaryKey);
   const nonKey = columns.filter((c) => c !== primaryKey);
   // The target column on the LHS of SET is UNQUALIFIED: BigQuery and Postgres 15+
   // reject a target-alias-qualified column there ("... cannot reference table
   // alias"), and Snowflake accepts the unqualified form too — so this is the
   // portable spelling. The RHS is source-qualified (S.col).
   const setClause = nonKey.map((c) => `${q(c)} = S.${q(c)}`).join(", ");
   const colList = columns.map(q).join(", ");
   const valList = columns.map((c) => `S.${q(c)}`).join(", ");
   const whenMatched = setClause
      ? `WHEN MATCHED THEN UPDATE SET ${setClause} `
      : "";
   return (
      `MERGE INTO ${quotedTarget} T USING (${usingSQL}) S ON T.${pk} = S.${pk} ` +
      whenMatched +
      `WHEN NOT MATCHED THEN INSERT (${colList}) VALUES (${valList})`
   );
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

   /**
    * Every materialization across all packages in an environment, newest first.
    * Each record carries its `packageName`, so an env-scoped view can group or
    * label by package without a per-package fan-out.
    */
   async listEnvironmentMaterializations(
      environmentName: string,
      options?: { limit?: number; offset?: number },
   ): Promise<Materialization[]> {
      const environmentId = await this.resolveEnvironmentId(environmentName);
      return this.repository.listMaterializationsByEnvironment(
         environmentId,
         options,
      );
   }

   /**
    * `created_at` of the newest scheduler-fired materialization for a package,
    * or null if none. The standalone scheduler uses this on its first arm to
    * recover a fire missed during downtime (see MaterializationScheduler.arm).
    */
   async getLatestScheduledFireAt(
      environmentName: string,
      packageName: string,
   ): Promise<Date | null> {
      const environmentId = await this.resolveEnvironmentId(environmentName);
      return this.repository.getLatestScheduledFireAt(
         environmentId,
         packageName,
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
         /**
          * Escape hatch (dbt `--full-refresh` analog): rebuild every incremental
          * source from scratch via the full staging+rename path this run, instead
          * of MERGE-ing. Use after a schema or logic migration. Distinct from
          * `forceRefresh` (which only bypasses content-hash reuse for full
          * sources): the scheduler sets `forceRefresh` on every run, so it must
          * NOT force incremental sources full, or scheduled refreshes would stop
          * being incremental.
          */
         forceFullRebuild?: boolean;
         sourceNames?: string[];
         buildInstructions?: BuildInstruction[];
         referenceManifest?: ManifestReference[];
         strictUpstreams?: boolean;
         /**
          * What initiated this run. `ON_DEMAND` (default) = a manual/API create;
          * `SCHEDULER` = the standalone materialization scheduler firing a
          * package's `materialization.schedule` cron. Recorded on the run
          * metadata so a scheduled rebuild is distinguishable from a manual one.
          */
         trigger?: "ON_DEMAND" | "SCHEDULER";
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
         throw this.activeConflict(packageName, active.id);
      }

      const forceRefresh = options.forceRefresh ?? false;
      const forceFullRebuild = options.forceFullRebuild ?? false;
      const trigger = options.trigger ?? "ON_DEMAND";
      const metadata = {
         forceRefresh,
         forceFullRebuild,
         sourceNames: options.sourceNames ?? null,
         mode: orchestrated ? "orchestrated" : "auto",
         trigger,
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
            throw this.activeConflict(packageName, winner?.id);
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
               forceFullRebuild,
               buildInstructions,
               referenceManifest: options.referenceManifest,
               strictUpstreams: options.strictUpstreams,
               trigger,
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
         forceFullRebuild: boolean;
         buildInstructions: BuildInstruction[] | undefined;
         referenceManifest: ManifestReference[] | undefined;
         strictUpstreams: boolean | undefined;
         trigger: "ON_DEMAND" | "SCHEDULER";
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
         // Persist the run's start time so the UI can compute a duration
         // (start -> now while in-flight, start -> completed once terminal).
         await this.repository.updateMaterialization(id, {
            startedAt: new Date(startedAt),
         });

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
            // Seed the build Manifest with the caller-supplied upstream
            // references so a downstream source's persist upstream (built in a
            // prior run, not rebuilt here) resolves to its existing physical
            // table instead of recomputing live. The reference key is the
            // compiler's manifest-lookup sourceEntityId (see ManifestReference).
            carried = this.referenceManifestToEntries(opts.referenceManifest);
         } else {
            // Skip-if-unchanged: reuse tables from the most recent successful
            // manifest for sources whose sourceEntityId is unchanged, unless
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
            opts.strictUpstreams ?? false,
            opts.forceFullRebuild,
         );

         const sourcesBuilt = instructions.length;
         const sourcesReused = Object.keys(carried).length;
         const durationMs = Date.now() - startedAt;
         await this.commitManifest(id, entries, {
            forceRefresh: opts.forceRefresh,
            sourceNames: opts.sourceNames ?? null,
            mode,
            trigger: opts.trigger,
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
    * physical table name and COPY realization, unless its sourceEntityId is unchanged
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
         for (const persistSource of iterGraphSources(
            graph,
            compiled.sources,
         )) {
            if (include && !include.has(persistSource.name)) continue;

            const sourceEntityId = computeSourceEntityId(
               persistSource,
               compiled.connectionDigests,
            );
            if (seen.has(sourceEntityId)) continue;
            seen.add(sourceEntityId);

            // Content-hash reuse assumes "same SQL => reusable table". That holds
            // for full rebuilds but NOT for incremental sources, whose table is a
            // function of run history, not just SQL: carrying one forward would
            // silently stop it advancing on a non-forced run. Incremental sources
            // are always (re)built.
            const prior = priorEntries[sourceEntityId];
            if (
               prior &&
               prior.physicalTableName &&
               refreshMode(persistSource) !== "incremental"
            ) {
               carried[sourceEntityId] = prior;
               continue;
            }

            instructions.push({
               sourceEntityId,
               materializedTableId: `local-${sourceEntityId.substring(
                  0,
                  STAGING_ID_LEN,
               )}`,
               physicalTableName: selfAssignTableName(persistSource),
               realization: "COPY",
            });
         }
      }

      return { instructions, carried };
   }

   /**
    * Project the caller-supplied upstream reference manifest into the seed
    * entry map `executeInstructedBuild` consumes. Each reference is keyed by the
    * compiler's manifest-lookup sourceEntityId and carries the physical table
    * name plus the connection it lives on — enough for the build Manifest to
    * resolve a downstream persist reference to the existing table, and to quote
    * that reference for the connection's dialect so it resolves on a
    * case-folding engine (see {@link quoteSeedTablePath}). `connectionName` is
    * optional on the wire: an older control plane that omits it seeds unquoted,
    * exactly as before.
    */
   private referenceManifestToEntries(
      referenceManifest: ManifestReference[] | undefined,
   ): Record<string, ManifestEntry> {
      const entries: Record<string, ManifestEntry> = {};
      for (const ref of referenceManifest ?? []) {
         entries[ref.sourceEntityId] = {
            sourceEntityId: ref.sourceEntityId,
            physicalTableName: ref.physicalTableName,
            connectionName: ref.connectionName,
         };
      }
      return entries;
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
            manifest: FreshnessManifest,
         ): Promise<void>;
      },
      packageName: string,
      entries: Record<string, ManifestEntry>,
   ): Promise<void> {
      // The post-build auto-load binds tableName-only entries: the control plane
      // stamps freshness (dataAsOf/window/fallback) on the wire manifest it
      // distributes, not on this in-memory post-build load, so these sources are
      // bound un-gated (always serve the freshly-built table).
      const manifestEntries: FreshnessManifest = {};
      for (const [sourceEntityId, entry] of Object.entries(entries)) {
         if (entry.physicalTableName) {
            manifestEntries[sourceEntityId] = {
               tableName: entry.physicalTableName,
               // Carried so the bind step can quote the physical path for the
               // connection's dialect (Package.quoteBoundTableNames) — the
               // build CREATEd it quoted, so an unquoted read would miss on a
               // case-folding engine in the window before the control plane's
               // wire-manifest rebind.
               connectionName: entry.connectionName,
            };
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
    * build plan: every instructed sourceEntityId must be a planned source, and only
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
      const plannedSourceEntityIds = new Set<string>();
      for (const source of Object.values(plan.sources)) {
         plannedSourceEntityIds.add(source.sourceEntityId);
      }

      for (const instruction of instructions) {
         if (!plannedSourceEntityIds.has(instruction.sourceEntityId)) {
            throw new BadRequestError(
               `Instruction references unknown sourceEntityId '${instruction.sourceEntityId}'`,
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
    * Quote a seeded upstream's physical path for the in-memory build Manifest,
    * mirroring the CREATE side ({@link quoteManifestTablePath}) so a downstream
    * persist source resolves the reference on a case-folding engine. The seed's
    * own connection (carried on the entry) supplies the dialect. If it can't be
    * resolved — an older control plane omitted `connectionName`, or the named
    * connection isn't part of this build — the path binds unquoted: the build
    * then fails loudly at the downstream CREATE on a case-folding engine (a
    * self-signaling miss, unlike the serve path), and is unaffected elsewhere.
    */
   private quoteSeedTablePath(
      sourceEntityId: string,
      physicalTableName: string,
      connectionName: string | undefined,
      connections: Map<string, MalloyConnection>,
   ): string {
      const connection = connectionName
         ? connections.get(connectionName)
         : undefined;
      if (!connection) {
         // A named-but-absent connection is a real gap (the seed can't be
         // quoted); a missing name is the benign older-CP default. Only the
         // former is worth a signal.
         if (connectionName) {
            recordManifestBindDegraded();
            logger.warn(
               "Seeded upstream names a connection not present in this build; " +
                  "leaving its manifest path unquoted (a downstream build will " +
                  "fail on a case-folding engine)",
               { sourceEntityId, connectionName },
            );
         }
         return physicalTableName;
      }
      return quoteManifestTablePath(physicalTableName, connection.dialectName);
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
      strict = false,
      forceFullRebuild = false,
   ): Promise<Record<string, ManifestEntry>> {
      const { graphs, sources, connectionDigests, connections } = compiled;

      // Index instructions by sourceID (the stable per-source handle) so the
      // build no longer recomputes the sourceEntityId to find an instruction.
      // Recomputing it here forced a caller's sourceEntityId to equal the publisher's
      // content hash, so a caller that derives sourceEntityIds by any other scheme
      // would have its sources silently skipped (the recomputed sourceEntityId would
      // not match the instruction). sourceEntityId is treated as opaque, caller-assigned
      // identity. A sourceEntityId index is kept as a fallback for instructions without
      // a sourceID (e.g. standalone auto-run).
      const bySourceID = new Map<string, BuildInstruction>();
      const bySourceEntityId = new Map<string, BuildInstruction>();
      for (const instruction of instructions) {
         if (instruction.sourceID) {
            bySourceID.set(instruction.sourceID, instruction);
         }
         bySourceEntityId.set(instruction.sourceEntityId, instruction);
      }

      // Accumulates physical names as sources are built so downstream sources
      // resolve their upstream references to the freshly-assigned tables. Seed
      // it with carried-forward entries so reused upstreams resolve too. In
      // strict mode, an upstream persist reference that is neither built here
      // nor seeded fails the compile (runtime-manifest-strict-miss) instead of
      // silently recomputing live.
      const manifest = new Manifest();
      manifest.strict = strict;
      const entries: Record<string, ManifestEntry> = {};
      for (const [sourceEntityId, entry] of Object.entries(seedEntries)) {
         if (entry.physicalTableName) {
            // The build Manifest feeds a downstream persist's `FROM` verbatim,
            // so a seeded upstream must carry the SAME quoting the builder
            // CREATEd it with — else the downstream CREATE misses the
            // case-preserved table on a case-folding engine. The seed keeps its
            // logical (unquoted) name in `entries` (the committed manifest, in
            // logical-name space); only the in-memory build Manifest is quoted.
            manifest.update(sourceEntityId, {
               tableName: this.quoteSeedTablePath(
                  sourceEntityId,
                  entry.physicalTableName,
                  entry.connectionName,
                  connections,
               ),
            });
         }
         entries[sourceEntityId] = entry;
      }

      for (const graph of graphs) {
         const connection = connections.get(graph.connectionName);
         if (!connection) {
            throw new BadRequestError(
               `Connection '${graph.connectionName}' not found`,
            );
         }
         for (const persistSource of iterGraphSources(graph, sources)) {
            if (signal.aborted) throw new Error("Build cancelled");

            // The manifest is keyed by the content sourceEntityId — what Malloy
            // recomputes to resolve upstream persist references during SQL
            // generation — independent of the instruction's identity sourceEntityId.
            const sourceEntityId = computeSourceEntityId(
               persistSource,
               connectionDigests,
            );
            // Prefer sourceID matching (so the caller's sourceEntityId scheme stays
            // opaque to the build); fall back to sourceEntityId for instructions
            // without a sourceID (auto-run).
            const instruction =
               bySourceID.get(persistSource.sourceID) ??
               bySourceEntityId.get(sourceEntityId);
            if (!instruction) continue;

            const entry = await this.buildOneSource(
               persistSource,
               instruction,
               connection,
               connectionDigests,
               manifest,
               forceFullRebuild,
            );
            entries[sourceEntityId] = entry;
         }
      }

      return entries;
   }

   /**
    * Build a single instructed source into its assigned physical table.
    * Dispatches on the source's `#@ persist ... refresh=...` mode: "incremental"
    * takes the MERGE path ({@link buildIncrementalSource}); unset/"full" keeps the
    * unchanged full-rebuild path ({@link buildFullSource}).
    *
    * `forceFullRebuild` is the escape hatch (dbt `--full-refresh` analog): when
    * the run requests it, an incremental source is rebuilt from scratch via the
    * full staging+rename path. Its self-reference CTE stays empty, so every row
    * is reprocessed and the table is atomically swapped — the way to recover
    * from a schema or logic migration without hand-dropping the table.
    */
   private async buildOneSource(
      persistSource: PersistSource,
      instruction: BuildInstruction,
      connection: MalloyConnection,
      connectionDigests: Record<string, string>,
      manifest: Manifest,
      forceFullRebuild = false,
   ): Promise<ManifestEntry> {
      if (!forceFullRebuild && refreshMode(persistSource) === "incremental") {
         return this.buildIncrementalSource(
            persistSource,
            instruction,
            connection,
            connectionDigests,
            manifest,
         );
      }
      return this.buildFullSource(
         persistSource,
         instruction,
         connection,
         connectionDigests,
         manifest,
      );
   }

   /**
    * Full rebuild (default; `refresh` unset or "full"): staging table + atomic
    * rename for crash-safety; the staging name derives from the sourceEntityId.
    * Records and returns the manifest entry. Behavior is byte-for-byte unchanged
    * from before the incremental feature.
    */
   private async buildFullSource(
      persistSource: PersistSource,
      instruction: BuildInstruction,
      connection: MalloyConnection,
      connectionDigests: Record<string, string>,
      manifest: Manifest,
   ): Promise<ManifestEntry> {
      const sourceEntityId = instruction.sourceEntityId;
      const physicalTableName = instruction.physicalTableName;
      const buildSQL = persistSource.getSQL({
         buildManifest: manifest.buildManifest,
         connectionDigests,
      });

      const bareName = bareTableName(physicalTableName);
      const stagingTableName = `${physicalTableName}${stagingSuffix(sourceEntityId)}`;
      // The control plane sends the logical (unquoted) physical name; dialect-
      // quote each identifier here so a container path or quote-requiring name
      // (e.g. a hyphenated BigQuery project id) produces valid DDL. The manifest
      // echoes the logical name (below) so the CP stays in logical-name space.
      const dialect = persistSource.dialectName;
      const quotedStaging = quoteTablePath(stagingTableName, dialect);
      const quotedPhysical = quoteTablePath(physicalTableName, dialect);
      const quotedBareName = quoteIdentifier(bareName, dialect);

      const startTime = performance.now();
      await connection.runSQL(`DROP TABLE IF EXISTS ${quotedStaging}`);
      try {
         await connection.runSQL(
            `CREATE TABLE ${quotedStaging} AS (${buildSQL})`,
         );
         await connection.runSQL(`DROP TABLE IF EXISTS ${quotedPhysical}`);
         await connection.runSQL(
            `ALTER TABLE ${quotedStaging} RENAME TO ${quotedBareName}`,
         );
      } catch (err) {
         try {
            await connection.runSQL(`DROP TABLE IF EXISTS ${quotedStaging}`);
         } catch (cleanupErr) {
            logger.warn(
               "Failed to clean up staging table after a failed build; physical leak",
               {
                  stagingTableName,
                  connectionName: persistSource.connectionName,
                  cleanupError: errMessage(cleanupErr),
               },
            );
         }
         throw err;
      }

      // Make this table visible to downstream sources built later in this run.
      // Record the SAME quoted path the CREATE used (not the logical name): the
      // build Manifest is pasted into a downstream `FROM` verbatim, so on a
      // case-folding engine an unquoted name would miss the case-preserved table
      // just written. The returned entry (below) keeps the logical name for the
      // committed manifest; only this in-memory build Manifest is quoted.
      manifest.update(sourceEntityId, { tableName: quotedPhysical });

      const durationMs = Math.round(performance.now() - startTime);
      recordSourceBuildDuration(durationMs);
      logger.info(`Built materialized source ${persistSource.name}`, {
         physicalTableName,
         durationMs,
      });

      return {
         sourceEntityId,
         sourceName: persistSource.name,
         materializedTableId: instruction.materializedTableId,
         physicalTableName,
         connectionName: persistSource.connectionName,
         realization: instruction.realization,
         rowCount: null,
      };
   }

   /**
    * Incremental build (`refresh="incremental"`): seed on first run, MERGE
    * thereafter. First run (target absent) is a plain CTAS of the source SQL —
    * the compile-safe self-reference CTE is empty, so all rows seed and the
    * warehouse infers the exact schema (no derived DDL). Subsequent runs hydrate
    * the self-reference to the target and MERGE on the source's primary_key, so
    * only new rows are computed. In-place (no staging swap); a single MERGE is
    * statement-atomic and idempotent under retry.
    */
   private async buildIncrementalSource(
      persistSource: PersistSource,
      instruction: BuildInstruction,
      connection: MalloyConnection,
      connectionDigests: Record<string, string>,
      manifest: Manifest,
   ): Promise<ManifestEntry> {
      const sourceEntityId = instruction.sourceEntityId;
      const physicalTableName = instruction.physicalTableName;
      const buildSQL = persistSource.getSQL({
         buildManifest: manifest.buildManifest,
         connectionDigests,
      });
      const dialect = persistSource.dialectName;
      const quotedPhysical = quoteTablePath(physicalTableName, dialect);

      const startTime = performance.now();
      const exists = await this.tableExists(connection, quotedPhysical);
      if (!exists) {
         // First run: full seed. The self-reference CTE is empty, so the source
         // processes every row; the warehouse infers the target schema.
         await connection.runSQL(
            `CREATE TABLE ${quotedPhysical} AS (${buildSQL})`,
         );
      } else {
         const primaryKey = persistSource._explore.primaryKey;
         if (!primaryKey) {
            throw new BadRequestError(
               `Incremental persist source "${persistSource.name}" requires a ` +
                  `primary_key to MERGE on; declare one on the source.`,
            );
         }
         const columns = deriveColumns(persistSource)
            .map((c) => c.name)
            .filter((n): n is string => n !== undefined);
         if (columns.length === 0) {
            throw new BadRequestError(
               `Incremental persist source "${persistSource.name}" has no ` +
                  `resolvable columns; cannot build a MERGE.`,
            );
         }
         const hydrated = hydrateIncrementalKeysCte(
            buildSQL,
            quotedPhysical,
            primaryKey,
            dialect,
         );
         await connection.runSQL(
            buildMergeSQL(
               quotedPhysical,
               hydrated,
               primaryKey,
               columns,
               dialect,
            ),
         );
      }

      // Downstream sources built later in this run resolve their FROM against
      // the live target (incremental mutates in place; there is no staging).
      manifest.update(sourceEntityId, { tableName: quotedPhysical });

      const durationMs = Math.round(performance.now() - startTime);
      recordSourceBuildDuration(durationMs);
      logger.info(
         `Built incremental materialized source ${persistSource.name}`,
         { physicalTableName, firstRun: !exists, durationMs },
      );

      return {
         sourceEntityId,
         sourceName: persistSource.name,
         materializedTableId: instruction.materializedTableId,
         physicalTableName,
         connectionName: persistSource.connectionName,
         realization: instruction.realization,
         rowCount: null,
      };
   }

   /**
    * Cheap, dialect-agnostic existence probe: `SELECT 1 FROM <t> LIMIT 0` scans
    * nothing and throws if the table is absent. Distinguishes an incremental
    * first-run seed from a MERGE.
    */
   private async tableExists(
      connection: MalloyConnection,
      quotedTable: string,
   ): Promise<boolean> {
      try {
         await connection.runSQL(`SELECT 1 FROM ${quotedTable} LIMIT 0`);
         return true;
      } catch {
         return false;
      }
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
               sourceEntityId: entry.sourceEntityId,
            });
            continue;
         }

         try {
            let connection = connectionCache.get(connectionName);
            if (!connection) {
               connection = await pkg.getMalloyConnection(connectionName);
               connectionCache.set(connectionName, connection);
            }
            // Dialect-quote from the live connection, the same way
            // buildOneSource quoted at build time, so a name that built
            // successfully also drops successfully (container paths, hyphenated
            // BigQuery project ids, etc.).
            const dialect = connection.dialectName;
            await connection.runSQL(
               `DROP TABLE IF EXISTS ${quoteTablePath(
                  physicalTableName,
                  dialect,
               )}`,
            );
            // A crash between staging-create and rename can leave the staging
            // table behind; clean it up too while we hold the connection.
            await connection.runSQL(
               `DROP TABLE IF EXISTS ${quoteTablePath(
                  `${physicalTableName}${stagingSuffix(entry.sourceEntityId)}`,
                  dialect,
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
               error: errMessage(err),
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

   /** The single-active conflict error, with the winning run's id when known. */
   private activeConflict(
      packageName: string,
      activeId?: string,
   ): MaterializationConflictError {
      const suffix = activeId ? ` (${activeId})` : "";
      return new MaterializationConflictError(
         `Package ${packageName} already has an active materialization${suffix}`,
      );
   }

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
            const message = errMessage(err);
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
                  transitionError: errMessage(transitionErr),
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
