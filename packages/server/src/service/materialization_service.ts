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
   iterGraphSources,
} from "./build_plan";
import { getPersistStorageMode } from "../config";
import { EnvironmentStore } from "./environment_store";
import { assertMaterializationEligible } from "./materialization_eligibility";
import { buildSourceIntoStorage } from "./materialization_build_session";
import type { ApiConnection } from "./model";
import {
   bareTableName,
   quoteIdentifier,
   quoteManifestTablePath,
   quoteTablePath,
} from "./quoting";
import { resolveEnvironmentId } from "./resolve_environment";
import { redactPgSecrets } from "../pg_helpers";

/**
 * The narrow environment surface the build path needs to materialize into a
 * `storage=` destination: resolve a connection's config by name (source creds to
 * federate, destination catalog to attach) and the environment root (to derive a
 * plain-DuckDB destination's file path). Kept minimal to avoid coupling the
 * materialization service to the full Environment type.
 */
interface BuildEnvironment {
   getApiConnection(connectionName: string): ApiConnection;
   getEnvironmentPath(): string;
}

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

/**
 * Content-address of a `storage=` physical table: the logical name decorated
 * with a `sourceEntityId`-derived fragment, so one physical table exists per
 * content generation. A definition change (new `sourceEntityId`) yields a NEW
 * physical table — generations coexist, the serve binding for each generation
 * resolves to its own table, and a swap is a pointer flip (the old table
 * survives until GC) rather than an in-place `CREATE OR REPLACE` of a table a
 * live binding still reads. A freshness refresh of the SAME definition reuses
 * the same name (an atomic, same-schema replace — safe).
 *
 * Only the STANDALONE self-instruction path uses this: an orchestrated build
 * trusts the host-supplied `physicalTableName` verbatim (matching path C), and
 * serve/GC read the recorded name from the manifest, so the two never need to
 * agree on a derivation. A dotted `name="schema.table"` (a user-owned DuckLake
 * honoring `name=` as schema.table) decorates only the table part; the schema
 * must pre-exist, unchanged from today.
 */
export function storagePhysicalTableName(
   logicalName: string,
   sourceEntityId: string,
): string {
   // Hyphen-stripped so the fragment stays a bare identifier when the id becomes
   // a UUID5 (a no-op for the current hex ids), matching stagingSuffix.
   const fragment = sourceEntityId
      .replace(/-/g, "")
      .substring(0, STORAGE_ID_LEN);
   const dot = logicalName.lastIndexOf(".");
   const schema = dot >= 0 ? logicalName.slice(0, dot + 1) : "";
   const table = dot >= 0 ? logicalName.slice(dot + 1) : logicalName;
   return `${schema}${table}__m${fragment}`;
}

/**
 * Length of the sourceEntityId fragment in a `storage=` physical name. 16 hex
 * chars is 64 bits — ample collision headroom for the generations one source
 * accumulates — and, with the `__m` join, stays well inside Postgres's 63-char
 * catalog identifier limit for any reasonable logical name.
 */
const STORAGE_ID_LEN = 16;

/**
 * The reserved `storage=` value meaning "materialize into the persist source's
 * own warehouse" (path C, the default). A connection may not be named this
 * (enforced at registration in connection_config), so it never collides with a
 * real destination.
 */
const STORAGE_SOURCE_SENTINEL = "source";

/**
 * Resolve a persist source's `#@ persist storage=<ref>` to the EFFECTIVE
 * destination connection name for a build, or undefined for the default
 * in-warehouse path. Read publisher-side from the compiled annotation (the same
 * `annotationFields` map the plan echoes); the reference resolves generically
 * against registered connections. Absent or the reserved `source` value ⇒
 * undefined (path C). Any managed-tier alias (e.g. `credible`) is resolved
 * upstream by the control plane and set on the wire instruction's `destination`
 * — it never reaches this publisher-side generic resolution.
 *
 * When `PERSIST_STORAGE_MODE=off` this returns undefined regardless of the
 * annotation — the source builds path C — so the feature is a runtime kill
 * switch that never fails a package (the ignored `storage=` is surfaced as a
 * package warning, not an error).
 */
function resolveStorageDestination(
   persistSource: PersistSource,
): string | undefined {
   if (getPersistStorageMode() === "off") return undefined;
   const storage = deriveAnnotationFields(persistSource).storage?.trim();
   if (!storage || storage === STORAGE_SOURCE_SENTINEL) return undefined;
   return storage;
}

/** Connection-config keys whose string values are credentials to redact. */
const SENSITIVE_KEY =
   /pass(word)?|secret|private_?key|service_?account|access_?key|token|connection_?string|account/i;

/** Collect credential string values (from sensitively-named keys) in a config. */
function collectSensitiveValues(value: unknown, out: Set<string>): void {
   if (value === null || typeof value !== "object") return;
   if (Array.isArray(value)) {
      for (const v of value) collectSensitiveValues(v, out);
      return;
   }
   for (const [key, v] of Object.entries(value)) {
      if (typeof v === "string" && v.length >= 4 && SENSITIVE_KEY.test(key)) {
         out.add(v);
      } else {
         collectSensitiveValues(v, out);
      }
   }
}

/**
 * Redact the actual credential values (from the given connection configs) out
 * of an error message, then surface the message. This keeps a build error
 * legible — a "schema not found" or "table does not exist" tells the operator
 * exactly what to fix — while never leaking the passwords / secrets / service
 * account JSON / connection strings a federation or attach error can echo. Only
 * the concrete secret values are removed, not the message structure.
 */
export function redactConnectionSecrets(
   message: string,
   ...connections: unknown[]
): string {
   const secrets = new Set<string>();
   for (const c of connections) collectSensitiveValues(c, secrets);
   let redacted = redactPgSecrets(message);
   for (const s of secrets) redacted = redacted.split(s).join("***");
   return redacted;
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
      const trigger = options.trigger ?? "ON_DEMAND";
      const metadata = {
         forceRefresh,
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
            environment,
            instructions,
            carried,
            signal,
            opts.strictUpstreams ?? false,
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

            const destination = resolveStorageDestination(persistSource);
            if (destination) {
               // Gate BEFORE computeSourceEntityId: an unbound parameter or a
               // given makes getSQL() (called inside computeSourceEntityId)
               // throw opaquely, so the eligibility refusal must fire first to
               // give a clean, actionable 422.
               assertMaterializationEligible(persistSource);
            }

            const sourceEntityId = computeSourceEntityId(
               persistSource,
               compiled.connectionDigests,
            );
            if (seen.has(sourceEntityId)) continue;
            seen.add(sourceEntityId);

            const prior = priorEntries[sourceEntityId];
            // Destination-scoped reuse: carry a prior table forward only when it
            // landed in the SAME destination. sourceEntityId is a pure content
            // address and does NOT encode the destination, so a source that adds,
            // drops, or switches `storage=` must rebuild — otherwise a
            // warehouse-landed (path-C) table would be silently reused for a
            // DuckLake serve that cannot resolve it (design §4).
            if (
               prior &&
               prior.physicalTableName &&
               (prior.storageConnectionName ?? undefined) === destination
            ) {
               carried[sourceEntityId] = prior;
               continue;
            }

            // A `storage=` table is content-addressed (one physical table per
            // generation) so generations coexist and a swap is a pointer flip;
            // path C keeps the plain logical name (in-warehouse, unchanged).
            const logicalName = selfAssignTableName(persistSource);
            instructions.push({
               sourceEntityId,
               materializedTableId: `local-${sourceEntityId.substring(
                  0,
                  STAGING_ID_LEN,
               )}`,
               physicalTableName: destination
                  ? storagePhysicalTableName(logicalName, sourceEntityId)
                  : logicalName,
               realization: "COPY",
               ...(destination ? { destination } : {}),
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
         bindPackageStorageServeBindings(
            packageName: string,
            entries: Record<string, ManifestEntry>,
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
         // Storage entries serve cross-connection via the virtual-source
         // bindings (below), NOT the same-connection manifest substitution —
         // putting one here would make the original model try to substitute the
         // source with a table on its OWN (source) connection, which doesn't
         // exist there. Only path-C entries go into the tableName manifest.
         if (entry.physicalTableName && !entry.storageConnectionName) {
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
         // Separately bind the FULL entries as storage serve bindings — sources
         // materialized into a storage destination serve cross-connection via
         // the virtual-source transform, not the tableName manifest above. No-op
         // for a package with no storage= sources (deriveServeBindings → []).
         await environment.bindPackageStorageServeBindings(
            packageName,
            entries,
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
      environment: BuildEnvironment,
      instructions: BuildInstruction[],
      seedEntries: Record<string, ManifestEntry>,
      signal: AbortSignal,
      strict = false,
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

            // Enforce the eligibility gate for any storage-targeted build,
            // including orchestrated (CP-supplied) instructions — the publisher
            // refuses an ineligible source into the tier itself, not on trust.
            // (Auto-run already gated pre-getSQL in deriveSelfInstructions; a
            // second call here is idempotent and covers the orchestrated path.)
            // Skipped when the mode is off: the kill switch ignores a
            // CP-supplied destination too and builds path C.
            if (instruction.destination && getPersistStorageMode() !== "off") {
               assertMaterializationEligible(persistSource);
            }

            const entry = await this.buildOneSource(
               persistSource,
               instruction,
               connection,
               connectionDigests,
               manifest,
               environment,
            );
            entries[sourceEntityId] = entry;
         }
      }

      return entries;
   }

   /**
    * Build a single instructed source into its assigned physical table.
    * COPY uses a staging table + atomic rename for crash-safety; the staging
    * name derives from the sourceEntityId. Records and returns the manifest entry.
    */
   private async buildOneSource(
      persistSource: PersistSource,
      instruction: BuildInstruction,
      connection: MalloyConnection,
      connectionDigests: Record<string, string>,
      manifest: Manifest,
      environment: BuildEnvironment,
   ): Promise<ManifestEntry> {
      const sourceEntityId = instruction.sourceEntityId;
      const physicalTableName = instruction.physicalTableName;
      const buildSQL = persistSource.getSQL({
         buildManifest: manifest.buildManifest,
         connectionDigests,
      });

      // `storage=` build: materialize into a DuckDB/DuckLake destination via a
      // build-scoped session (never on the source or serve connection). Diverges
      // fully from the in-warehouse CTAS below — different engine, credential
      // federation, and a captured authoritative schema for the serve transform.
      // Gated by the kill switch: when off, ignore a destination and build path C.
      if (instruction.destination && getPersistStorageMode() !== "off") {
         return this.buildOneSourceIntoStorage(
            persistSource,
            instruction,
            manifest,
            environment,
            buildSQL,
         );
      }

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
    * Materialize a source into a `storage=` destination (a DuckDB/DuckLake
    * connection) via a native query-passthrough CTAS on a build-scoped session.
    * Records the destination connection and the captured authoritative DuckDB
    * schema on the manifest entry so the source can later be served
    * cross-dialect from the destination (the serve transform declares that
    * schema). `connectionName` still names the SOURCE warehouse (where data is
    * read from); `storageConnectionName` names where the table now lives.
    */
   private async buildOneSourceIntoStorage(
      persistSource: PersistSource,
      instruction: BuildInstruction,
      manifest: Manifest,
      environment: BuildEnvironment,
      buildSQL: string,
   ): Promise<ManifestEntry> {
      const sourceEntityId = instruction.sourceEntityId;
      const physicalTableName = instruction.physicalTableName;
      const destinationName = instruction.destination!;
      const sourceConnection = environment.getApiConnection(
         persistSource.connectionName,
      );
      const destinationConnection =
         environment.getApiConnection(destinationName);

      const startTime = performance.now();
      let result;
      try {
         result = await buildSourceIntoStorage({
            destinationName,
            destinationConnection,
            sourceConnection,
            buildSQL,
            physicalTableName,
            // Expose the logical name as a convenience view over the hashed
            // physical table (best-effort; publisher always reads the physical
            // name). Skipped when they coincide.
            logicalViewName: selfAssignTableName(persistSource),
            environmentPath: environment.getEnvironmentPath(),
         });
      } catch (err) {
         // Redaction (design §5): a failed federation / passthrough / attach
         // error can echo source- or catalog-connection detail (connstrings,
         // account names, service-account JSON) from the DuckDB engine. Strip
         // the actual credential VALUES but keep the message, so an operator sees
         // a legible, actionable error (e.g. "schema 'analytics' not found" when
         // a `name=schema.table` target's schema wasn't provisioned) rather than
         // an opaque failure — without leaking secrets into the user-visible run
         // `error` column.
         const safeDetail = redactConnectionSecrets(
            errMessage(err),
            sourceConnection,
            destinationConnection,
         );
         logger.warn("Storage materialization build failed", {
            sourceName: persistSource.name,
            destinationName,
            error: safeDetail,
         });
         throw new Error(
            `Failed to materialize source '${persistSource.name}' into storage ` +
               `destination '${destinationName}': ${safeDetail}`,
         );
      }

      // Make this table visible to downstream sources built later in this run
      // (single-source spike; chained materialized sources across stores are a
      // tracked follow-on).
      manifest.update(sourceEntityId, { tableName: physicalTableName });

      const durationMs = Math.round(performance.now() - startTime);
      recordSourceBuildDuration(durationMs);
      logger.info(
         `Built materialized source ${persistSource.name} into storage`,
         {
            physicalTableName,
            storageConnectionName: result.storageConnectionName,
            columns: result.schema.length,
            durationMs,
         },
      );

      return {
         sourceEntityId,
         sourceName: persistSource.name,
         materializedTableId: instruction.materializedTableId,
         physicalTableName,
         connectionName: persistSource.connectionName,
         storageConnectionName: result.storageConnectionName,
         schema: result.schema,
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
               sourceEntityId: entry.sourceEntityId,
            });
            continue;
         }

         // A storage= table lives in `storageConnectionName` (a DuckDB/DuckLake
         // destination), not in `connectionName` (the source warehouse). Dropping
         // it needs a build-scoped read-write attach — the serve attach is
         // read-only — so it is NOT dropped here; issuing the drop on the source
         // connection would target the wrong engine. Destination-aware GC is a
         // tracked follow-on; skip (and log) rather than mis-drop.
         if (entry.storageConnectionName) {
            logger.info(
               "Skipping delete-time drop of a storage-materialized table " +
                  "(destination-aware GC is handled separately)",
               {
                  materializationId: m.id,
                  physicalTableName,
                  storageConnectionName: entry.storageConnectionName,
               },
            );
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
