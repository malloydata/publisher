import type {
   Connection as MalloyConnection,
   PersistSource,
} from "@malloydata/malloy";
import { Manifest } from "@malloydata/malloy";
import {
   BadRequestError,
   InvalidStateTransitionError,
   MaterializationConflictError,
   MaterializationEligibilityError,
   MaterializationNotFoundError,
} from "../errors";
import { logger } from "../logger";
import {
   MaterializationMode,
   recordAutoLoadOutcome,
   recordChainedStorageBuild,
   recordDropTables,
   recordManifestBindDegraded,
   recordMaterializationRun,
   recordSourceBuildDuration,
   recordSourcesOutcome,
   recordStorageBuildFailure,
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
   projectToPublicColumns,
   iterGraphSources,
} from "./build_plan";
import { getPersistStorageMode } from "../config";
import { EnvironmentStore } from "./environment_store";
import { assertMaterializationEligible } from "./materialization_eligibility";
import {
   assertStorageServeShapeCompiles,
   buildDownstreamIntoStorage,
   buildSourceIntoStorage,
   dropStorageTable,
   type StorageBuildResult,
} from "./materialization_build_session";
import { escapeSQL } from "./connection";
import {
   buildChainedStorageBuildModel,
   buildVirtualMap,
   deriveServeBindings,
   type ServeBinding,
   type SourceLocation,
   sliceSourceRange,
} from "./materialization_serve_transform";
import type { ApiConnection } from "./model";
import { fetchManifestEntries, splitManifestEntries } from "./manifest_loader";
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
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
 * The build manifest for a `storage=` build, with storage-materialized entries
 * removed. A storage build runs in the source warehouse (passthrough), so it
 * cannot reference an upstream that landed in a DuckDB/DuckLake store — dropping
 * those entries makes the compiler INLINE the upstream (non-strict) or raise a
 * clean strict-miss (strict), instead of emitting a cross-engine table
 * reference. colocated entries are kept — the warehouse build can
 * reference them, carrying forward the DIALECT-QUOTED table path the seed loop
 * already stamped (publisher #904's quoteSeedTablePath), so the downstream FROM
 * resolves a case-preserved upstream on a case-folding engine. Preserves the
 * manifest's `strict` flag.
 */
export function manifestExcludingStorage(
   manifest: Manifest,
   builtEntries: Record<string, ManifestEntry>,
): Manifest["buildManifest"] {
   const reduced = new Manifest();
   reduced.strict = manifest.strict;
   // The source manifest's entries are already dialect-quoted (#904); reuse that
   // quoting rather than the raw physical name so the kept colocated references
   // stay canonical for the downstream FROM.
   const quoted = manifest.buildManifest.entries;
   for (const [id, entry] of Object.entries(builtEntries)) {
      if (!entry.storageConnectionName && quoted[id]) {
         reduced.update(id, { tableName: quoted[id].tableName });
      }
   }
   return reduced.buildManifest;
}

/**
 * The `storage=` destination a source DECLARES (external-tier intent), or
 * undefined. Independent of `PERSIST_STORAGE_MODE` — reflects author intent, so
 * the build can tell a `storage=` source apart from a plain colocated
 * `#@ persist` even when the tier is off.
 */
function declaredStorage(persistSource: PersistSource): string | undefined {
   return deriveAnnotationFields(persistSource).storage?.trim() || undefined;
}

/**
 * Resolve a persist source's `#@ persist storage=<ref>` to the EFFECTIVE
 * destination connection name for a build, or undefined for the default
 * colocated path (the source materializes into its own warehouse). Read
 * publisher-side from the compiled annotation (the same `annotationFields` map
 * the plan echoes); the reference resolves generically against registered
 * connections. Absent `storage=` ⇒ undefined (colocated); any value names a
 * registered connection to materialize into. Any managed-tier alias is resolved
 * by the host upstream and set on the wire instruction's `destination` — it
 * never reaches this publisher-side generic resolution.
 *
 * When `PERSIST_STORAGE_MODE=off` this returns undefined regardless of the
 * annotation, so the feature is a runtime kill switch that never fails a
 * package (the ignored `storage=` is surfaced as a package warning, not an
 * error). Undefined here does NOT mean "build it colocated" for a source that
 * declared `storage=` — `deriveSelfInstructions` skips such a source entirely so
 * it serves live; see {@link declaredStorage}.
 */
function resolveStorageDestination(
   persistSource: PersistSource,
): string | undefined {
   if (getPersistStorageMode() === "off") return undefined;
   return declaredStorage(persistSource);
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
   for (const s of secrets) {
      redacted = redacted.split(s).join("***");
      // A DuckDB error often echoes the offending SQL statement, in which a
      // secret containing a single quote appears single-quote-escaped (`''`) —
      // so the raw value won't match. Also redact the escaped form.
      const escaped = escapeSQL(s);
      if (escaped !== s) redacted = redacted.split(escaped).join("***");
   }
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

         // Backstop: refuse loudly if the package annotated a `#@ persist` source
         // that Malloy's getBuildPlan() silently dropped (a shape it doesn't
         // treat as a materializable root — see detectDroppedPersistSources).
         // Without this the build would report success with an empty manifest and
         // the source would serve live, contradicting the "hard refuse, never a
         // silent fallback" contract. Scoped to the sources this build targets so
         // a build of unrelated sources isn't blocked by a dropped sibling.
         const relevantDropped = (compiled.droppedPersistSources ?? []).filter(
            (d) => !opts.sourceNames || opts.sourceNames.includes(d.name),
         );
         if (relevantDropped.length > 0) {
            const names = relevantDropped.map((d) => `'${d.name}'`).join(", ");
            throw new MaterializationEligibilityError({
               message:
                  `Source(s) ${names} are annotated '#@ persist' but were not ` +
                  `recognized as a materializable source, so nothing would be ` +
                  `built (they would be served live). Only query/aggregate ` +
                  `sources materialize; a filtered pass-through does not. Persist ` +
                  `a query source, or invoke a parameterized source with a bound ` +
                  `argument, or drop the annotation to serve live.`,
            });
         }

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
            // Upstream resolution for an orchestrated build draws on three
            // sources, in DESCENDING precedence — a reference is an IDENTITY, not
            // a copy the caller must fully courier:
            //   1. The explicit `referenceManifest` couriered in THIS build call.
            //   2. The package's BOUND manifest (`manifestLocation`) — the set the
            //      orchestrator distributed to this worker. This is what makes the
            //      cross-worker flow work: a worker that never built the upstream
            //      still holds its full entry via the refreshed manifest, so a
            //      downstream can reuse the upstream's materialized table.
            //   3. This worker's own most-recent local manifest (skip-if-unchanged
            //      cache) — same-worker reuse.
            // Each fills the storage fields (sourceName, storageConnectionName,
            // schema) a thin reference can't carry — required for a `storage=`
            // upstream's Tier-3 "stack on the parent" rebind. Higher-precedence
            // fields win; each step is best-effort (a fetch/read failure leaves
            // the entries as they are).
            await this.seedFromBoundManifest(carried, pkg, instructions);
            if (Object.keys(carried).length > 0) {
               await this.resolveReferencesFromStore(
                  carried,
                  environmentId,
                  packageName,
                  id,
               );
            }
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

            // Safety: a source that DECLARES `storage=` must never silently
            // downgrade to a colocated build when the external tier is disabled
            // (PERSIST_STORAGE_MODE=off). A colocated build writes a CTAS into the
            // source's OWN warehouse — which the author did not intend (they asked
            // for external storage; production grants this server read-only
            // warehouse access) and which could fail or land in an unexpected
            // schema. Skip it: the source is not materialized and serves LIVE, and
            // the mode warning (Package.persistenceModeWarnings) surfaces the
            // degraded state. A plain `#@ persist` (no `storage=`) is unaffected —
            // colocated IS its author's intent (the v0 path, ungated by the
            // storage kill switch).
            if (
               getPersistStorageMode() === "off" &&
               declaredStorage(persistSource)
            ) {
               continue;
            }

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
            // warehouse-landed (colocated) table would be silently reused for a
            // DuckLake serve that cannot resolve it (design §4).
            if (
               prior &&
               prior.physicalTableName &&
               (prior.storageConnectionName ?? undefined) === destination
            ) {
               carried[sourceEntityId] = prior;
               continue;
            }

            // Self-assign the physical name from `name=` (or the source name)
            // verbatim for BOTH the colocated and storage
            // destinations — the only difference between the two is which
            // connection the table lands in. A storage build replaces the table
            // atomically (`CREATE OR REPLACE`), so no generational decoration is
            // needed to make a rebuild safe. An orchestrated build ignores this
            // and trusts the host-supplied `physicalTableName`; the host owns any
            // generational, ownership-scoped naming.
            const logicalName = selfAssignTableName(persistSource);
            instructions.push({
               sourceEntityId,
               materializedTableId: `local-${sourceEntityId.substring(
                  0,
                  STAGING_ID_LEN,
               )}`,
               physicalTableName: logicalName,
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
    * Resolve upstream references against this publisher's OWN most-recent
    * manifest, keyed by sourceEntityId (the same cache skip-if-unchanged reads).
    * A reference is fundamentally an IDENTITY, not a copy: for each one that
    * matches a locally persisted entry, fill in the fields a thin reference
    * can't carry (sourceName, storageConnectionName, schema — needed for a
    * `storage=` upstream's Tier-3 rebind) from the local entry, while letting any
    * caller-supplied field WIN (the orchestrator is authoritative across a
    * stateless fleet). Mutates `carried` in place. Best-effort: a lookup failure
    * leaves the references exactly as supplied, so the cross-worker courier path
    * (a worker that never built the upstream, fed the full entry) is unaffected.
    */
   private async resolveReferencesFromStore(
      carried: Record<string, ManifestEntry>,
      environmentId: string,
      packageName: string,
      excludeId: string,
   ): Promise<void> {
      let cached: Record<string, ManifestEntry>;
      try {
         cached = await this.getMostRecentManifestEntries(
            environmentId,
            packageName,
            excludeId,
         );
      } catch (err) {
         logger.warn(
            "Reference resolve-local lookup failed; using references as supplied",
            {
               packageName,
               error: err instanceof Error ? err.message : String(err),
            },
         );
         return;
      }
      for (const [sourceEntityId, ref] of Object.entries(carried)) {
         const local = cached[sourceEntityId];
         if (!local) continue;
         // Local entry is the base; caller-supplied (defined) fields override it.
         const merged: ManifestEntry = { ...local };
         for (const [key, value] of Object.entries(ref)) {
            if (value !== undefined) {
               (merged as Record<string, unknown>)[key] = value;
            }
         }
         carried[sourceEntityId] = merged;
      }
   }

   /**
    * Seed upstream reuse from the package's BOUND manifest — the set the
    * orchestrator distributed to this worker via `manifestLocation`. This is the
    * cross-worker path: a worker that never built an upstream still holds its full
    * entry here (`storageConnectionName` + `schema` + `sourceName`), so a
    * downstream can reuse the upstream's materialized table instead of recomputing
    * it from raw. Adds any bound upstream not already carried (so a build needs no
    * explicit reference when the manifest is refreshed), and fills gaps in a
    * thin explicit reference — but an explicit `referenceManifest` field always
    * wins (it targets THIS build). Sources this build is producing are skipped
    * (built fresh, not reused). Best-effort: a fetch failure is ignored (the build
    * falls back to the local store / inline recompute). Mutates `carried`.
    */
   private async seedFromBoundManifest(
      carried: Record<string, ManifestEntry>,
      pkg: { getPackageMetadata(): { manifestLocation?: string | null } },
      instructions: BuildInstruction[],
   ): Promise<void> {
      const manifestLocation = pkg.getPackageMetadata().manifestLocation;
      if (!manifestLocation) return;
      let fetched;
      try {
         fetched = await fetchManifestEntries(manifestLocation);
      } catch (err) {
         logger.warn(
            "Build upstream resolution: bound manifest fetch failed; ignoring",
            {
               manifestLocation,
               error: err instanceof Error ? err.message : String(err),
            },
         );
         return;
      }
      // Reconstruct a ManifestEntry map from both tiers of the fetched manifest:
      // storage entries carry their full shape; colocated entries reconstruct from
      // the tableName manifest.
      const bound: Record<string, ManifestEntry> = {
         ...fetched.storageEntries,
      };
      for (const [eid, e] of Object.entries(fetched.tableNameManifest)) {
         if (!bound[eid]) {
            bound[eid] = {
               sourceEntityId: eid,
               physicalTableName: e.tableName,
               connectionName: e.connectionName,
            };
         }
      }
      const building = new Set(instructions.map((i) => i.sourceEntityId));
      for (const [eid, entry] of Object.entries(bound)) {
         if (building.has(eid)) continue;
         const existing = carried[eid];
         if (!existing) {
            carried[eid] = entry;
            continue;
         }
         // Explicit reference wins; the bound entry fills the gaps it left.
         const merged: ManifestEntry = { ...entry };
         for (const [key, value] of Object.entries(existing)) {
            if (value !== undefined) {
               (merged as Record<string, unknown>)[key] = value;
            }
         }
         carried[eid] = merged;
      }
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
      const list =
         (await this.repository.listMaterializations(
            environmentId,
            packageName,
         )) ?? [];
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
         // exist there. Only colocated entries go into the tableName manifest.
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

            // Prefer sourceID matching (so the caller's sourceEntityId scheme
            // stays opaque to the build); the sourceEntityId lookup below is the
            // fallback for instructions without a sourceID (auto-run). Resolved
            // before computeSourceEntityId so the eligibility gate wins: that
            // call invokes getSQL(), which throws opaquely for a free-parameter
            // or given source, losing the clean 422.
            const orchestratedInstruction = bySourceID.get(
               persistSource.sourceID,
            );

            // Enforce the eligibility gate for any storage-targeted build,
            // including orchestrated (host-supplied) instructions — the publisher
            // refuses an ineligible source into the tier itself, not on trust.
            // Skipped when the mode is off: the kill switch ignores a
            // host-supplied destination too and does a colocated build.
            if (
               orchestratedInstruction?.destination &&
               getPersistStorageMode() !== "off"
            ) {
               assertMaterializationEligible(persistSource);
            }

            // The manifest is keyed by the content sourceEntityId — what Malloy
            // recomputes to resolve upstream persist references during SQL
            // generation — independent of the instruction's identity sourceEntityId.
            const sourceEntityId = computeSourceEntityId(
               persistSource,
               connectionDigests,
            );
            const instruction =
               orchestratedInstruction ?? bySourceEntityId.get(sourceEntityId);
            if (!instruction) continue;

            // Auto-run already gated pre-getSQL in deriveSelfInstructions;
            // re-assert (idempotent) so no path into a storage build is ungated.
            if (
               !orchestratedInstruction &&
               instruction.destination &&
               getPersistStorageMode() !== "off"
            ) {
               assertMaterializationEligible(persistSource);
            }

            const entry = await this.buildOneSource(
               persistSource,
               instruction,
               connection,
               connectionDigests,
               manifest,
               environment,
               entries,
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
      builtEntries: Record<string, ManifestEntry>,
   ): Promise<ManifestEntry> {
      const sourceEntityId = instruction.sourceEntityId;
      const physicalTableName = instruction.physicalTableName;
      const isStorageBuild =
         !!instruction.destination && getPersistStorageMode() !== "off";
      // ANY warehouse-executed build SQL — a colocated CTAS or a storage build's
      // native passthrough — runs against the SOURCE warehouse, which cannot see
      // a storage-materialized upstream's DuckDB/DuckLake table (a different
      // engine). Substituting that upstream's lake table name into warehouse SQL
      // would reference a table the warehouse can't resolve (a confusing "table
      // not found"), whether the downstream is a storage build OR a colocated
      // source reading a storage upstream. So exclude storage-materialized
      // upstreams from the build manifest in BOTH cases: non-strict, they INLINE
      // (recompute from raw against the warehouse) so a chained source still
      // materializes; under `strictUpstreams` the excluded reference becomes a
      // clean strict-miss error (the orchestrated contract — don't silently
      // recompute). Tier 3 ("stack on the parent") reads the parent's lake table
      // instead, but via a separate DuckDB recompile that does NOT use this
      // warehouse buildSQL — this remains its recompute-from-raw fallback.
      const buildManifest = manifestExcludingStorage(manifest, builtEntries);
      const buildSQL = persistSource.getSQL({
         buildManifest,
         connectionDigests,
      });

      // `storage=` build: materialize into a DuckDB/DuckLake destination via a
      // build-scoped session (never on the source or serve connection). Diverges
      // fully from the in-warehouse CTAS below — different engine, credential
      // federation, and a captured authoritative schema for the serve transform.
      // Gated by the kill switch: when off, ignore a destination and do a colocated build.
      if (isStorageBuild) {
         // Tier-3 detection: does the source's SQL change when storage upstreams
         // are PRESENT in the manifest (mapped to their lake tables) vs EXCLUDED
         // (inlined, the buildSQL above)? If so it reads a storage-materialized
         // upstream, so it can be built by reading the parent's lake table
         // ("stack on the parent") instead of recomputing from raw. The compare
         // is graph-free and self-contained; a single-source build's two SQLs are
         // identical, so it skips straight to the passthrough below.
         const dependsOnStorageUpstream =
            persistSource.getSQL({
               buildManifest: manifest.buildManifest,
               connectionDigests,
            }) !== buildSQL;
         // Materialize ONLY the source's PUBLIC columns. `getSQL` projects every
         // underlying column, including ones the source hides (`except:`, non-public
         // access modifiers). The serve transform narrows the declared ::Shape to
         // the public surface, but a DuckLake virtual source is not restricted to
         // its declared shape — it exposes every physical column of the stored
         // table — so a hidden column materialized here would be reachable over
         // storage (and would also sit at rest). Projecting to the public columns
         // makes the physical table equal the public surface, closing that leak at
         // the source. Falls back to the full buildSQL if the public columns can't
         // be derived (never widens — worst case is the prior behavior).
         const publicBuildSQL = projectToPublicColumns(persistSource, buildSQL);
         return this.buildOneSourceIntoStorage(
            persistSource,
            instruction,
            manifest,
            environment,
            publicBuildSQL,
            builtEntries,
            dependsOnStorageUpstream,
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
      recordSourceBuildDuration(durationMs, "in_warehouse");
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
      builtEntries: Record<string, ManifestEntry>,
      dependsOnStorageUpstream: boolean,
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

      // Tier 3 ("stack on the parent"): a source that reads a storage-materialized
      // upstream is built by reading the parent's STORED lake table instead of
      // recomputing it from raw against the warehouse (Tier 2). This reuses the
      // parent's work and is consistent-by-construction (the downstream is a pure
      // function of the parent's stored rows). Attempted only when the source
      // actually reads a storage upstream; on any ineligibility (a parent
      // refinement not carried into the rebind, a live-warehouse join, a
      // cross-catalog parent) it throws and we fall back: recompute-from-raw when
      // non-strict, or a loud refusal under `strictUpstreams` (the orchestrated
      // contract — never silently recompute).
      if (dependsOnStorageUpstream) {
         try {
            result = await this.buildDownstreamViaParents(
               persistSource,
               destinationName,
               destinationConnection,
               builtEntries,
               environment,
               physicalTableName,
            );
            recordChainedStorageBuild("parent_reuse");
         } catch (err) {
            // Same redaction contract as the Tier-2 path below (design §5): this
            // branch's read-write ATTACH or CTAS can fail with the offending SQL
            // echoed back, catalog `password=` included. Redact before the
            // message reaches the thrown run `error` or the log.
            const safeDetail = redactConnectionSecrets(
               errMessage(err),
               sourceConnection,
               destinationConnection,
            );
            if (manifest.strict) {
               recordChainedStorageBuild("strict_refused");
               recordStorageBuildFailure(destinationName);
               throw new Error(
                  `Failed to materialize chained source '${persistSource.name}' ` +
                     `into storage destination '${destinationName}' by reading ` +
                     `its materialized upstream, and strict upstreams forbid ` +
                     `recomputing it from raw: ${safeDetail}`,
               );
            }
            recordChainedStorageBuild("inline_fallback");
            logger.warn(
               "Chained storage build could not reuse the parent table; " +
                  "recomputing the upstream from raw (Tier 2 fallback)",
               {
                  sourceName: persistSource.name,
                  destinationName,
                  reason: safeDetail,
               },
            );
         }
      }

      // Tier 2 / single-source passthrough: materialize `buildSQL` (with storage
      // upstreams inlined) in the source warehouse and CTAS the result into the
      // destination. Skipped when Tier 3 already produced the table above.
      if (!result) {
         try {
            result = await buildSourceIntoStorage({
               destinationName,
               destinationConnection,
               sourceConnection,
               buildSQL,
               physicalTableName,
               environmentPath: environment.getEnvironmentPath(),
            });
         } catch (err) {
            // Redaction (design §5): a failed federation / passthrough / attach
            // error can echo source- or catalog-connection detail (connstrings,
            // account names, service-account JSON) from the DuckDB engine. Strip
            // the actual credential VALUES but keep the message, so an operator
            // sees a legible, actionable error (e.g. "schema 'analytics' not
            // found" when a `name=schema.table` target's schema wasn't
            // provisioned) rather than an opaque failure — without leaking
            // secrets into the user-visible run `error` column.
            const safeDetail = redactConnectionSecrets(
               errMessage(err),
               sourceConnection,
               destinationConnection,
            );
            recordStorageBuildFailure(destinationName);
            logger.warn("Storage materialization build failed", {
               sourceName: persistSource.name,
               destinationName,
               error: safeDetail,
            });
            throw new Error(
               `Failed to materialize source '${persistSource.name}' into ` +
                  `storage destination '${destinationName}': ${safeDetail}`,
            );
         }
      }

      // Build-time servability gate: the serve-shape must compile in DuckDB
      // against the authoritative post-build schema, or the build is refused
      // (HTTP 422) — a serve-time execution error turned into a fail-loud
      // build-time refusal. Outside the redaction try above so the eligibility
      // error surfaces as-is (it carries no connection secrets). Runs here, in
      // stage→validate, because it needs the captured schema.
      try {
         await assertStorageServeShapeCompiles({
            destinationName,
            sourceName: persistSource.name,
            virtualHandle: sourceEntityId,
            physicalTableName,
            schema: result.schema,
         });
      } catch (gateErr) {
         // The table was already CTAS'd before this post-build gate, and no
         // manifest entry records it yet — so a refusal would strand it where
         // manifest-driven GC (which only drops names it recorded building) can
         // never see it. Best-effort drop of the just-built table so a refused
         // build doesn't leak an orphaned table.
         //
         // Note (in-place naming): the CTAS above already replaced any prior
         // generation at this name, so a failed-gate rebuild has no earlier
         // table to fall back to — the source reverts to serving live until a
         // subsequent successful build. Rollback-safe regeneration (keep the
         // prior generation until the new one is validated) requires the
         // host-generational orchestrated path, not the auto-run server.
         try {
            await dropStorageTable({
               destinationName,
               destinationConnection,
               physicalTableName,
               environmentPath: environment.getEnvironmentPath(),
            });
         } catch (dropErr) {
            logger.warn(
               "Failed to drop a storage table stranded by a serve-shape gate " +
                  "refusal (physical leak)",
               {
                  sourceName: persistSource.name,
                  destinationName,
                  physicalTableName,
                  // The drop runs on a read-write attach, so a failure can echo
                  // the catalog connstring.
                  error: redactConnectionSecrets(
                     errMessage(dropErr),
                     sourceConnection,
                     destinationConnection,
                  ),
               },
            );
         }
         throw gateErr;
      }

      // Make this table visible to downstream sources built later in this run,
      // so a chained storage source can be built by reading it (Tier 3, above).
      manifest.update(sourceEntityId, { tableName: physicalTableName });

      const durationMs = Math.round(performance.now() - startTime);
      recordSourceBuildDuration(durationMs, "storage");
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

   /**
    * Tier 3 build: materialize a chained storage source by reading its
    * already-materialized upstream(s) from the SAME destination store. Rebinds
    * every same-destination materialized upstream to a virtual source (base-only,
    * the captured schema), re-declares the downstream over them (its definition
    * text lifted from the author's model), and hands the assembled transient
    * model to {@link buildDownstreamIntoStorage}, which compiles it against the
    * build session and CTASes the downstream's SQL — now reading the parents'
    * lake tables — into the destination.
    *
    * Throws (⇒ the caller falls back to recompute-from-raw) when there is no
    * same-destination upstream to build on, the definition text can't be lifted,
    * or the transient model doesn't compile (a parent refinement not carried, a
    * live-warehouse join, a cross-catalog parent).
    */
   private async buildDownstreamViaParents(
      persistSource: PersistSource,
      destinationName: string,
      destinationConnection: ApiConnection,
      builtEntries: Record<string, ManifestEntry>,
      environment: BuildEnvironment,
      physicalTableName: string,
   ): Promise<StorageBuildResult> {
      // Rebind every upstream materialized into THIS destination. A parent in a
      // DIFFERENT destination is absent here, so the downstream def fails to
      // compile against the rebind model and the caller falls back — cross-catalog
      // parent reuse is out of scope for the spike.
      const upstreams: ServeBinding[] = deriveServeBindings(
         builtEntries,
      ).filter((b) => b.connectionName === destinationName);
      if (upstreams.length === 0) {
         throw new Error(
            "no materialized upstream is available in this destination to build on",
         );
      }
      const downstreamDefText = this.liftDownstreamDefText(persistSource);
      if (!downstreamDefText) {
         throw new Error(
            "could not recover the downstream source definition text from the model",
         );
      }
      const transientModel = buildChainedStorageBuildModel({
         upstreams,
         downstreamName: persistSource.name,
         downstreamDefText,
         destinationName,
      });
      return buildDownstreamIntoStorage({
         destinationName,
         destinationConnection,
         transientModel,
         downstreamName: persistSource.name,
         virtualMap: buildVirtualMap(upstreams),
         physicalTableName,
         environmentPath: environment.getEnvironmentPath(),
      });
   }

   /**
    * Lift the verbatim RHS of a persist source's `source: <name> is …`
    * declaration from the author's model file (same technique as the serve
    * transform's join/view lift): read the file named by the source's compiled
    * `location`, slice the covered range. A top-level source's range starts just
    * after the `source: ` keyword, so the result is `<name> is <def>` and the
    * transient-model assembler prepends `source: `. Returns undefined (⇒ fall
    * back) when there is no file-backed location or the file can't be read/sliced.
    */
   private liftDownstreamDefText(
      persistSource: PersistSource,
   ): string | undefined {
      const location = (
         persistSource._explore as unknown as { location?: SourceLocation }
      ).location;
      if (!location?.url?.startsWith("file:")) return undefined;
      let text: string;
      try {
         text = readFileSync(fileURLToPath(location.url), "utf8");
      } catch {
         return undefined;
      }
      return sliceSourceRange(text, location.range);
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

      // Re-derive the package's serve routing from the latest REMAINING
      // successful materialization — BOTH tiers. The deleted run may have been
      // the one bound for serving — and with `dropTables` its tables are now gone
      // — so without this a query would keep routing (schema-on-faith, no
      // run-time fallback) to a deleted/dropped table and error until the next
      // reload or build. Rebinding picks the next-latest generation, or CLEARS
      // the bindings when none remain (empty ⇒ serve live).
      await this.rebindServeBindingsAfterDelete(environmentName, packageName);
   }

   /**
    * Re-derive a package's serve routing from its latest remaining successful
    * materialization and push it onto the loaded models — BOTH tiers, mirroring
    * the load-time {@link Environment.rebindServeBindingsFromLocalStore}. Called
    * after a delete so serving never points at a removed table; picks the
    * next-latest generation, or clears the bindings when none remain.
    * Best-effort (a failure logs and leaves the current bindings — a later
    * reload/build re-derives).
    *
    * The manifest is split by tier:
    *  - **colocated** (same-connection) → re-derived regardless of
    *    `PERSIST_STORAGE_MODE`: colocated is the v0 path and is not gated by the
    *    storage kill switch, so a reclaimed colocated table must not be left
    *    routed even when the tier is off.
    *  - **storage=** (cross-connection) → re-derived only when the tier is not
    *    `off` (its serve routing requires the tier on; an off deployment does no
    *    extra work here).
    */
   private async rebindServeBindingsAfterDelete(
      environmentName: string,
      packageName: string,
   ): Promise<void> {
      try {
         const environmentId = await this.resolveEnvironmentId(environmentName);
         // "" excludes nothing — the deleted record is already gone from the repo.
         const entries = await this.getMostRecentManifestEntries(
            environmentId,
            packageName,
            "",
         );
         const environment = await this.environmentStore.getEnvironment(
            environmentName,
            false,
         );
         const { tableNameManifest, storageEntries } = splitManifestEntries(
            entries,
            `post-delete rebind (package ${packageName})`,
         );
         // Colocated: re-derive (or clear) regardless of mode.
         await environment.bindPackageColocatedServeManifest(
            packageName,
            tableNameManifest,
         );
         // Storage=: only meaningful when the tier is not off.
         if (getPersistStorageMode() !== "off") {
            await environment.bindPackageStorageServeBindings(
               packageName,
               storageEntries,
            );
         }
      } catch (err) {
         logger.warn(
            "Failed to rebind serve bindings after delete (leaving current " +
               "bindings; a reload/build will re-derive)",
            { packageName, error: errMessage(err) },
         );
      }
   }

   /**
    * Best-effort drop of every physical table this run produced, read from the
    * materialization's manifest. Resolves each entry's connection by name and
    * issues `DROP TABLE IF EXISTS` for the physical table and its (possible)
    * leftover staging table. Failures are logged and swallowed so a partial
    * cleanup never blocks deletion of the record.
    *
    * A physical name is dropped only when NO other remaining MANIFEST_FILE_READY
    * run references it. Physical names are the source's `name=` verbatim (or a
    * host-assigned name), so multiple generations of a source share one physical
    * name — dropping a superseded record's table would otherwise take out the
    * table the current generation still serves. The skip incidentally also
    * protects the colocated case, where generations likewise share a
    * name.
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

      // Physical names still referenced by ANOTHER MANIFEST_FILE_READY run for
      // this package (keyed destination-and-name), so a shared name is never
      // dropped out from under a live generation. `m` is still in the repo at
      // this point (deletion happens after this sweep), so exclude it by id.
      const tableKey = (dest: string, table: string) => `${dest}:${table}`;
      const stillReferenced = new Set<string>();
      const environmentId = await this.resolveEnvironmentId(environmentName);
      const others =
         (await this.repository.listMaterializations(
            environmentId,
            packageName,
         )) ?? [];
      for (const other of others) {
         if (other.id === m.id) continue;
         if (other.status !== "MANIFEST_FILE_READY") continue;
         for (const e of Object.values(other.manifest?.entries ?? {})) {
            const dest = e.storageConnectionName ?? e.connectionName;
            if (dest && e.physicalTableName) {
               stillReferenced.add(tableKey(dest, e.physicalTableName));
            }
         }
      }

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

         // Do not drop a table another live generation still serves (shared
         // physical name — see the method doc).
         const destForKey = entry.storageConnectionName ?? connectionName;
         if (stillReferenced.has(tableKey(destForKey, physicalTableName))) {
            logger.info(
               "Skipping drop: table still referenced by another materialization",
               {
                  materializationId: m.id,
                  physicalTableName,
                  destination: destForKey,
               },
            );
            continue;
         }

         // A storage= table lives in `storageConnectionName` (a DuckDB/DuckLake
         // destination), not in `connectionName` (the source warehouse), and
         // dropping it needs a build-scoped READ-WRITE attach — the serve attach
         // is read-only — so it is dropped on its own RW session rather than on
         // the (wrong-engine, read-only) source connection. Best-effort: a
         // failure is logged and the sweep continues, so one unreachable
         // destination never blocks reclaiming the rest.
         if (entry.storageConnectionName) {
            const destinationConnection = environment.getApiConnection(
               entry.storageConnectionName,
            );
            try {
               await dropStorageTable({
                  destinationName: entry.storageConnectionName,
                  destinationConnection,
                  physicalTableName,
                  environmentPath: environment.getEnvironmentPath(),
               });
               recordDropTables("success", "storage");
               logger.info("Dropped materialized storage table on delete", {
                  materializationId: m.id,
                  physicalTableName,
                  storageConnectionName: entry.storageConnectionName,
               });
            } catch (err) {
               recordDropTables("failure", "storage");
               logger.warn("Failed to drop a storage-materialized table", {
                  materializationId: m.id,
                  physicalTableName,
                  storageConnectionName: entry.storageConnectionName,
                  // The drop attaches read-write, so a failure can echo the
                  // catalog connstring.
                  error: redactConnectionSecrets(
                     errMessage(err),
                     destinationConnection,
                  ),
               });
            }
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
            recordDropTables("success", "in_warehouse");
            logger.info("Dropped materialized table on delete", {
               materializationId: m.id,
               physicalTableName,
               connectionName,
            });
         } catch (err) {
            recordDropTables("failure", "in_warehouse");
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
