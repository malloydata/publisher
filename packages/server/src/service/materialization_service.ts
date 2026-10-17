// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type {
   Connection as MalloyConnection,
   PersistSource,
} from "@malloydata/malloy";
import { Manifest } from "@malloydata/malloy";
import {
   BadRequestError,
   ChainedShapeNotCarriedError,
   ChainedUpstreamMissingError,
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
   recordDuplicateTargetSkipped,
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
   isLegacyFailedEntry,
   LedgerEntry,
   Materialization,
   MaterializationStatus,
   MaterializationUpdate,
   ManifestEntry,
   ManifestReference,
   RefusedSource,
   ResourceRepository,
   SourceFailure,
} from "../storage/DatabaseInterface";
import { DuplicateActiveMaterializationError } from "../storage/duckdb/MaterializationRepository";
import { errMessage } from "../utils";
import {
   CompiledBuildPlan,
   computeSourceEntityId,
   deriveAnnotationFields,
   projectToPublicColumns,
   iterGraphSources,
   resolveQueryMetadata,
} from "./build_plan";
import { compilePackageBuildPlan } from "./build_plan_compile";
import { warehouseDeltaTarget } from "./incremental_apply";
import {
   incrementalRunContext,
   ledgerFields,
   refreshFields,
   refreshOneSourceIncrementally,
   storageRefreshFor,
   advanceLedgerAfterSeed,
   incrementalLineage,
   reportIncrementalStep,
   resetLedger,
   type IncrementalRunContext,
} from "./incremental_build";
import { mergeQueryMetadata, type QueryMetadata } from "./query_metadata";
import type { components } from "../api";
import { getPersistStorageMode } from "../config";
import { EnvironmentStore } from "./environment_store";
import {
   assertColocatedPersistNotAuthorizeGated,
   assertMaterializationEligible,
} from "./materialization_eligibility";
import {
   assertStorageServeShapeCompiles,
   BilledReadNotCapturedError,
   buildDownstreamIntoStorage,
   buildSourceIntoStorage,
   dropStorageTable,
   STORAGE_TARGET_DIALECT,
   type StorageBuildResult,
   type StorageIncrementalRefresh,
} from "./materialization_build_session";
import {
   authorModelLiftContext,
   buildChainedStorageBuildModel,
   buildVirtualMap,
   deriveServeBindings,
   liftDerivedSources,
   missingPersistedTables,
   reachedPersistedSources,
   type ServeBinding,
   type SourceLocation,
   documentFlagsForLifts,
   authorRefinementsFor,
   buildOverTiers,
   REFINEMENT_TIER_KINDS,
} from "./materialization_serve_transform";

import type { ApiConnection } from "./model";
import { fetchManifestEntries, splitManifestEntries } from "./manifest_loader";
import {
   quoteRenameTarget,
   quoteManifestTablePath,
   quoteTablePath,
} from "./quoting";
import { resolveEnvironmentId } from "./resolve_environment";
import { redactPgSecrets } from "../pg_helpers";
import {
   ChainedPlanFacts,
   NO_PLAN_FACTS,
   manifestForCompiler,
   upstreamReuseFromManifest,
   compilerMissingUpstream,
   chainedPlanFacts,
   readAuthorFile,
   BuildEnvironment,
   BuildQueryMetadata,
   connectionMetadataLayers,
   STAGING_ID_LEN,
   stagingSuffix,
   selfAssignTableName,
   manifestExcludingStorage,
   declaredStorage,
   chainedBuildGivens,
   partitionColumnsForBuild,
   resolveStorageDestination,
   tallySources,
   refusalRecord,
   allSourcesRefusedError,
   redactConnectionSecrets,
   outcomeFor,
   VALID_TRANSITIONS,
   tableKeyOf,
   reportRetainedStorageTables,
   isReclaimableStorageTable,
   physicalTargetKey,
} from "./materialization_run_support";
import {
   assertInstructedDestinationWritable,
   assertNoTableCollisions,
   indexInstructions,
   recordInstructedBuildFailure,
   resolveInstruction,
   type InstructedRun,
   type ResolvedInstruction,
} from "./materialization_instructed_run";

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
          * Rebuild the in-scope incremental sources from scratch, ignoring their
          * recorded boundaries. Distinct from `forceRefresh`, which only defeats
          * skip-if-unchanged and never re-seeds.
          */
         reseed?: boolean;
         sourceNames?: string[];
         buildInstructions?: BuildInstruction[];
         referenceManifest?: ManifestReference[];
         strictUpstreams?: boolean;
         /**
          * `BuildInstructions.ledger`: the incremental ledger for this run,
          * supplied by the caller (each entry a `ManifestEntry.ledger` an
          * earlier run reported). When present — even empty — it replaces the
          * publisher's local store for the whole run. Orchestrated-only, and
          * validated against the build plan at create time so an entry that
          * names an uninstructed table or a moved source is a 400, not a
          * failed background run.
          */
         ledger?: LedgerEntry[];
         /**
          * What initiated this run. `ON_DEMAND` (default) = a manual/API create;
          * `SCHEDULER` = the standalone materialization scheduler firing a
          * package's `materialization.schedule` cron. Recorded on the run
          * metadata so a scheduled rebuild is distinguishable from a manual one.
          */
         trigger?: "ON_DEMAND" | "SCHEDULER";
         /**
          * What the caller knows about this run and the publisher does not,
          * attached as query metadata to the statements the build issues. Its
          * `trigger` also covers the case the publisher's own `trigger` cannot
          * express (a publish), and its `runId` lets a caller's own id group the
          * build's statements instead of the publisher's materialization id.
          */
         runContext?: components["schemas"]["RunContext"] | null;
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
         this.validateInstructions(
            pkg.getBuildPlan(),
            buildInstructions,
            options.ledger,
            options.reseed ?? false,
         );
      }

      const active = await this.repository.getActiveMaterialization(
         environmentId,
         packageName,
      );
      if (active) {
         throw this.activeConflict(packageName, active.id);
      }

      const forceRefresh = options.forceRefresh ?? false;
      const reseed = options.reseed ?? false;
      const trigger = options.trigger ?? "ON_DEMAND";
      const metadata = {
         forceRefresh,
         // Recorded separately from forceRefresh because it answers a different
         // question about the run — whether an incremental source rebuilt or
         // advanced — and the history is where that is read back.
         reseed,
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
               reseed,
               buildInstructions,
               // Orchestrated-only, and clamped here rather than trusted: an
               // auto-run has no caller to take a ledger from, and taking one
               // would replace the local store it depends on.
               ledger: orchestrated ? options.ledger : undefined,
               referenceManifest: options.referenceManifest,
               strictUpstreams: options.strictUpstreams,
               trigger,
               runContext: options.runContext ?? undefined,
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
         reseed: boolean;
         buildInstructions: BuildInstruction[] | undefined;
         ledger: LedgerEntry[] | undefined;
         referenceManifest: ManifestReference[] | undefined;
         strictUpstreams: boolean | undefined;
         trigger: "ON_DEMAND" | "SCHEDULER";
         runContext?: components["schemas"]["RunContext"];
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

         // Assembled BEFORE the instructions because skip-if-unchanged consults
         // it: an incremental source is exempt from the carry-forward below.
         const incremental = this.incrementalRunContext(compiled, {
            environmentId,
            packageName,
            materializationId: id,
            // `reseed`, never `forceRefresh`. The latter only defeats
            // skip-if-unchanged, which an incremental source is exempt from
            // anyway, so it has nothing to say about how one is built — and every
            // scheduled fire sets it, which would make a schedule a series of
            // full rebuilds that no delta could ever follow.
            forceRefresh: opts.reseed ?? false,
            now: new Date(startedAt),
            ledger: opts.ledger,
         });

         let instructions: BuildInstruction[];
         let carried: Record<string, ManifestEntry>;
         let derivedRefused: Record<string, RefusedSource> = {};
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
            // Each fills the storage fields (sourceName, storageDestinationName,
            // schema) a thin reference can't carry — required for a `storage=`
            // upstream's stack-on-the-parent rebind. Higher-precedence
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
            ({
               instructions,
               carried,
               refused: derivedRefused,
            } = this.deriveSelfInstructions(
               compiled,
               opts.sourceNames,
               priorEntries,
               incremental,
            ));
         }

         const {
            entries,
            failures,
            sourcesRefused: instructedRefused,
         } = await this.executeInstructedBuild(
            compiled,
            environment,
            instructions,
            carried,
            signal,
            opts.strictUpstreams ?? false,
            // Failure-path reclaim is ORCHESTRATED-ONLY on purpose — see
            // reclaimStorageTablesFromFailedRun.
            orchestrated ? { environmentId, packageName } : undefined,
            {
               // Optional for the same reason build_plan reads it optionally:
               // callers that build from a lighter package surface still resolve,
               // just without a package-level layer.
               packageMaterialization: pkg.getMaterializationConfig?.() ?? null,
               context: {
                  queryClass: "materialize",
                  environment: environmentName,
                  package: packageName,
                  // The caller's trigger wins because it can express a publish,
                  // which the publisher's own two-value trigger cannot.
                  trigger:
                     opts.runContext?.trigger ?? opts.trigger?.toLowerCase(),
                  // Default to the materialization id: the publisher always has a
                  // run id, so a build's statements group in the backend's query
                  // history whether or not the caller supplied one.
                  runId: opts.runContext?.runId ?? id,
               },
            },
            incremental,
            Object.values(derivedRefused),
         );

         const { sourcesBuilt, sourcesFailed, sourcesReused } = tallySources(
            entries,
            failures,
            carried,
         );
         // What auto-run skipped rides the run's free-form metadata, not the
         // manifest: a refused source binds no table, and the manifest is what a
         // strict-schema caller parses. An orchestrated refusal is in `failures`
         // instead, where that caller already looks.
         const sourcesRefused =
            Object.keys(derivedRefused).length + instructedRefused;
         const durationMs = Date.now() - startedAt;
         await this.commitManifest(id, entries, failures, {
            forceRefresh: opts.forceRefresh,
            sourceNames: opts.sourceNames ?? null,
            mode,
            trigger: opts.trigger,
            sourcesBuilt,
            sourcesReused,
            ...(sourcesRefused > 0 ? { sourcesRefused } : {}),
            ...(Object.keys(derivedRefused).length > 0
               ? { refusedSources: derivedRefused }
               : {}),
            durationMs,
         });

         // Auto-run owns distribution: load the fresh manifest into the package
         // models so subsequent queries resolve to the materialized tables.
         // Orchestrated leaves distribution to the caller (manifestLocation).
         if (!orchestrated) {
            await this.autoLoadManifest(environment, packageName, entries);
         }

         recordSourcesOutcome("built", sourcesBuilt, mode);
         recordSourcesOutcome("reused", sourcesReused, mode);
         // An instructed refusal is in `failures` but metered once, as refused.
         recordSourcesOutcome(
            "failed",
            sourcesFailed - instructedRefused,
            mode,
         );
         recordSourcesOutcome("refused", sourcesRefused, mode);
         // A run that lost sources is a partial success, not a success: the
         // manifest it committed is missing tables a consumer expected. A
         // refusal auto-run skipped is not a loss: it is a property of the
         // model, reported by the build plan before any run, and would make
         // every run of that package partial until the model changed.
         const partial = sourcesFailed > 0;
         this.recordRun(mode, partial ? "partial" : "success", startedAt);
         logger[partial || sourcesRefused > 0 ? "warn" : "info"](
            sourcesFailed > 0
               ? "Materialization build complete with failed sources"
               : sourcesRefused > 0
                 ? "Materialization build complete with refused sources"
                 : "Materialization build complete",
            {
               materializationId: id,
               packageName,
               mode,
               sourcesBuilt,
               sourcesReused,
               sourcesFailed,
               sourcesRefused,
               durationMs,
            },
         );
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
    *
    * An INCREMENTAL source is exempt from that carry-forward. Skip-if-unchanged
    * reuses a table because the SQL is unchanged, which is precisely the wrong
    * inference for a source whose whole premise is that the DATA moved while the
    * SQL stayed put — left in, an incremental source would be carried forward on
    * every non-forced run and its delta would never be reached. The exemption
    * costs nothing when there is no work: the ledger, not the content address,
    * decides, and planIncrementalStep answers "nothing new" with a skip that is
    * cheaper than the rebuild the carry was avoiding.
    *
    * A source the eligibility gate refuses is returned in `refused` rather than
    * instructed. It throws instead when `sourceNames` targets it, or when every
    * authored source the run targets was refused, so nothing is left to build or
    * reuse. A refused rollup never causes the throw.
    */
   private deriveSelfInstructions(
      compiled: CompiledBuildPlan,
      sourceNames: string[] | undefined,
      priorEntries: Record<string, ManifestEntry>,
      incremental?: IncrementalRunContext,
   ): {
      instructions: BuildInstruction[];
      carried: Record<string, ManifestEntry>;
      refused: Record<string, RefusedSource>;
   } {
      const include = sourceNames ? new Set(sourceNames) : null;
      const instructions: BuildInstruction[] = [];
      const carried: Record<string, ManifestEntry> = {};
      const refused: Record<string, RefusedSource> = {};
      const refusals: MaterializationEligibilityError[] = [];
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
            // the mode warning (Package.storageWarnings) surfaces the degraded
            // state. A plain `#@ persist` (no `storage=`) is unaffected —
            // colocated IS its author's intent (the v0 path, ungated by the
            // storage kill switch).
            if (
               getPersistStorageMode() === "off" &&
               declaredStorage(persistSource)
            ) {
               continue;
            }

            const destination = resolveStorageDestination(persistSource);
            const isRollup =
               !!compiled.preaggregatePlans?.[persistSource.sourceID];
            // Gate BEFORE computeSourceEntityId: an unbound parameter or a given
            // makes getSQL() (called inside computeSourceEntityId) throw
            // opaquely, so the eligibility refusal must fire first.
            //
            // A refused source is SKIPPED and recorded, not thrown for. A
            // refusal is a compile-time fact the build plan already reports, and
            // throwing here aborts the whole run, so one ineligible source would
            // stop every other source in the package refreshing, on every tick,
            // until the model changed. The run throws only when there is nothing
            // else to build (below), or when the caller named this source in
            // `sourceNames` and so asked for exactly the table that cannot be
            // built.
            try {
               if (destination) {
                  assertMaterializationEligible(
                     persistSource,
                     deriveAnnotationFields(persistSource),
                  );
               } else {
                  // No storage destination: this is the colocated `#@ persist`
                  // path (a CTAS into the source's own warehouse). It is not
                  // covered by assertMaterializationEligible above (that gate
                  // only runs when a storage destination resolved), but it is
                  // just as frozen as a storage build, so an authorize-gated
                  // source materialized here would still be served to every
                  // caller.
                  //
                  // The origin is passed so a REFUSED ROLLUP names
                  // `#@ preaggregate` and not `#@ persist`: a rollup's name is
                  // synthesized and appears nowhere in the author's model, so the
                  // default message sends them hunting for a line they never
                  // wrote. See the function's doc.
                  assertColocatedPersistNotAuthorizeGated(
                     persistSource,
                     persistSource.name,
                     isRollup ? "preaggregate" : "persist",
                     compiled.sourceGateOutcomes?.[persistSource.sourceID],
                     deriveAnnotationFields(persistSource),
                  );
               }
            } catch (err) {
               if (!(err instanceof MaterializationEligibilityError)) throw err;
               if (include) throw err;
               if (!refused[persistSource.sourceID]) {
                  refused[persistSource.sourceID] = refusalRecord(
                     persistSource,
                     err,
                     isRollup
                        ? "preaggregate"
                        : destination
                          ? "storage"
                          : "colocated",
                     compiled.sourceModelPaths?.[persistSource.sourceID],
                  );
                  // A rollup is recorded but never makes the run fail: it asked
                  // for nothing an author can see, so a package whose only
                  // refused work is a rollup has nothing to answer for.
                  if (!isRollup) refusals.push(err);
                  logger.warn(
                     "Skipping a persist source the eligibility gate refuses",
                     {
                        sourceName: persistSource.name,
                        reason: errMessage(err),
                     },
                  );
               }
               continue;
            }

            const sourceEntityId = computeSourceEntityId(
               persistSource,
               compiled.connectionDigests,
            );
            if (seen.has(sourceEntityId)) continue;
            seen.add(sourceEntityId);

            // Self-assign the physical name from `name=` (or the source name)
            // verbatim for BOTH the colocated and storage destinations — the only
            // difference between the two is which connection the table lands in. A
            // storage build replaces the table atomically — a single `CREATE OR
            // REPLACE`, or, when the source declares `partition=`, the
            // create/alter/insert trio inside ONE transaction for exactly this
            // reason — so no generational decoration is needed to make a rebuild
            // safe. Without that transaction a rebuild would empty the table it
            // is serving. An
            // orchestrated build ignores this and trusts the host-supplied
            // `physicalTableName`; the host owns any generational,
            // ownership-scoped naming.
            const logicalName = selfAssignTableName(persistSource);

            // Gated on the SAME predicate the build's dispatch uses, so the two
            // can never disagree about which sources are incremental. A
            // declaration the delta path cannot act on — an unsupported dialect,
            // an unusable watermark — returns undefined and keeps today's
            // skip-if-unchanged behavior rather than rebuilding fully on every
            // run in the name of an incremental refresh it will not perform.
            const deltaEligible =
               incremental !== undefined &&
               incrementalLineage({
                  declaration: incremental.declarations[persistSource.sourceID],
                  dialect: persistSource.dialectName,
                  targetDialect:
                     destination !== undefined
                        ? STORAGE_TARGET_DIALECT
                        : persistSource.dialectName,
                  physicalTableName: logicalName,
                  connectionName: persistSource.connectionName,
                  storageDestinationName: destination,
                  sourceEntityId,
               }) !== undefined;

            const prior = priorEntries[sourceEntityId];
            // Destination-scoped reuse: carry a prior table forward only when it
            // landed in the SAME destination. sourceEntityId is a pure content
            // address and does NOT encode the destination, so a source that adds,
            // drops, or switches `storage=` must rebuild — otherwise a
            // warehouse-landed (colocated) table would be silently reused for a
            // DuckLake serve that cannot resolve it.
            if (
               !deltaEligible &&
               prior &&
               prior.physicalTableName &&
               (prior.storageDestinationName ?? undefined) === destination
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
               physicalTableName: logicalName,
               realization: "COPY",
               ...(destination ? { destination } : {}),
            });
         }
      }

      if (
         refusals.length > 0 &&
         instructions.length === 0 &&
         Object.keys(carried).length === 0
      ) {
         throw allSourcesRefusedError(refusals);
      }

      return { instructions, carried, refused };
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
    * can't carry (sourceName, storageDestinationName, schema — needed for a
    * `storage=` upstream's stack-on-the-parent rebind) from the local entry, while
    * letting any caller-supplied field WIN (the orchestrator is authoritative across a
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
    * entry here (`storageDestinationName` + `schema` + `sourceName`), so a
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
            // Legacy tolerance, removable with `ManifestEntry.error`: a manifest
            // written by 0.0.245-0.0.246 records a failed source among its
            // entries. Carrying one forward as reuse would retire the source from
            // every later run -- a warehouse error that clears on its own would
            // never be retried -- and seeding a downstream FROM from it would
            // compile against a table that was never created.
            return Object.fromEntries(
               Object.entries(m.manifest.entries).filter(
                  ([, entry]) => !isLegacyFailedEntry(entry),
               ),
            );
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
      // A failed source is mirrored into `entries` for the `ManifestEntry.error`
      // deprecation window, under the physical name it was HEADED for. With
      // auto-run's stable names that is the previous generation's real table,
      // so binding it would serve stale rows as fresh; like every other read
      // boundary, this one skips it.
      const builtEntries = Object.fromEntries(
         Object.entries(entries).filter(
            ([, entry]) => !isLegacyFailedEntry(entry),
         ),
      );
      const manifestEntries: FreshnessManifest = {};
      for (const [sourceEntityId, entry] of Object.entries(builtEntries)) {
         // Storage entries serve cross-connection via the virtual-source
         // bindings (below), NOT the same-connection manifest substitution —
         // putting one here would make the original model try to substitute the
         // source with a table on its OWN (source) connection, which doesn't
         // exist there. Only colocated entries go into the tableName manifest.
         if (entry.physicalTableName && !entry.storageDestinationName) {
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
            builtEntries,
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
    *
    * When the caller supplied the ledger, each of its entries is validated here
    * too, per the API contract that an invalid entry is a 400 rather than a
    * quiet rebuild: an entry must name a table this run instructs, name it once,
    * belong to a source that declares incremental refresh, and carry the content
    * address the plan carries for that source. The address check is the one
    * that fires in ordinary operation — a publish or rollback that changes a
    * source moves its address, and the caller's stored entry was measured under
    * the old one — which is exactly why it is checked HERE, synchronously,
    * where the caller can react (delete the entry, or set `reseed`), instead of
    * failing a background run. An entry for a source being reseeded is ignored
    * unvalidated: `reseed` is the documented recovery from this 400, so it must
    * not itself trip it.
    */
   private validateInstructions(
      plan: BuildPlan | null,
      instructions: BuildInstruction[],
      ledger?: LedgerEntry[],
      runReseed = false,
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

      if (ledger === undefined || ledger.length === 0) return;

      // What this run is building, by the table each instruction writes — the
      // same key a ledger entry carries — with what the plan knows about the
      // source writing it. Resolved by sourceID when the instruction echoes
      // one, else by content address (the same order the build loop matches
      // instructions in).
      const instructed = new Map<
         string,
         { sourceEntityId: string; refresh: string | null; reseed: boolean }
      >();
      // The same tables keyed WITHOUT their destination, which is what lets a
      // destination-less entry from an older caller be recognized as stale rather
      // than as naming a table this run does not build.
      const instructedIntoStorage = new Set<string>();
      const byAddress = new Map(
         Object.values(plan.sources).map((s) => [s.sourceEntityId, s]),
      );
      for (const instruction of instructions) {
         const planSource =
            (instruction.sourceID
               ? plan.sources[instruction.sourceID]
               : undefined) ?? byAddress.get(instruction.sourceEntityId);
         if (!planSource) continue; // Unreachable: every instruction matched above.
         instructed.set(
            tableKeyOf(
               planSource.connectionName,
               instruction.destination,
               instruction.physicalTableName,
            ),
            {
               sourceEntityId: planSource.sourceEntityId,
               refresh: planSource.refresh ?? null,
               reseed: runReseed || instruction.reseed === true,
            },
         );
         if (instruction.destination) {
            instructedIntoStorage.add(
               tableKeyOf(
                  planSource.connectionName,
                  undefined,
                  instruction.physicalTableName,
               ),
            );
         }
      }

      const seen = new Set<string>();
      for (const entry of ledger) {
         const key = tableKeyOf(
            entry.connectionName,
            entry.storageDestinationName,
            entry.physicalTableName,
         );
         const table =
            `'${entry.physicalTableName}' ` +
            (entry.storageDestinationName
               ? `in storage destination '${entry.storageDestinationName}'`
               : `on connection '${entry.connectionName}'`);
         if (seen.has(key)) {
            throw new BadRequestError(
               `Ledger entry for table ${table} appears more than once`,
            );
         }
         seen.add(key);
         const target = instructed.get(key);
         if (!target) {
            // A caller that predates `storageDestinationName` sends the entry
            // without it, and the table it names IS one this run builds — just in
            // a destination the entry cannot describe. That entry is STALE, not
            // wrong, so the source seeds (the index the build reads keys on the
            // destination too, so it finds no boundary) rather than the run being
            // refused. An entry naming a DIFFERENT destination is a caller error
            // and falls through to the refusal below.
            const storedHere = instructedIntoStorage.has(
               tableKeyOf(
                  entry.connectionName,
                  undefined,
                  entry.physicalTableName,
               ),
            );
            if (entry.storageDestinationName === undefined && storedHere) {
               continue;
            }
            throw new BadRequestError(
               `Ledger entry names table ${table}, which this run's ` +
                  `instructions do not build`,
            );
         }
         if (target.reseed) continue;
         if (target.refresh !== "incremental") {
            throw new BadRequestError(
               `Ledger entry names table ${table}, whose source does not ` +
                  `declare refresh="incremental"`,
            );
         }
         if (entry.sourceEntityId !== target.sourceEntityId) {
            throw new BadRequestError(
               `Ledger entry for table ${table} was measured under a ` +
                  `different definition of its source (its sourceEntityId does ` +
                  `not match the plan's) — expected after a publish or ` +
                  `rollback that changed the source. Delete the entry to ` +
                  `rebuild the source, or set reseed.`,
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
    * The run's incremental context over this service's ledger store. A method
    * rather than a direct call so a test can substitute the context a run gets.
    */
   private incrementalRunContext(
      compiled: CompiledBuildPlan,
      run: Parameters<typeof incrementalRunContext>[1],
   ): IncrementalRunContext | undefined {
      return incrementalRunContext(compiled, run, this.repository);
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
      // Identity of the run, used only to reclaim storage tables this run created
      // if it fails part-way (see reclaimStorageTablesFromFailedRun).
      owner?: { environmentId: string; packageName: string },
      buildMetadata?: BuildQueryMetadata,
      // The run's incremental context, when any source declared incremental
      // refresh. Undefined leaves every source on the full-rebuild path.
      incremental?: IncrementalRunContext,
      // Sources auto-run refused before instructing anything. Named only in the
      // error of a run whose every instructed source failed, so that error
      // accounts for every source the run did not build.
      priorRefusals: RefusedSource[] = [],
   ): Promise<{
      entries: Record<string, ManifestEntry>;
      failures: Record<string, SourceFailure>;
      /**
       * Instructed sources the eligibility gate refused. Each is also in
       * `failures`; the count is kept apart so a refusal is metered as one.
       */
      sourcesRefused: number;
   }> {
      const { graphs, sources, connectionDigests, connections } = compiled;
      const index = indexInstructions(instructions);
      const run: InstructedRun = {
         compiled,
         environment,
         owner,
         buildMetadata,
         incremental,
         index,
         // Computed once for the run: a chained build asks it for every source it
         // reaches, and the answer is the plan's, not the run's.
         planFacts: chainedPlanFacts(sources, connectionDigests),
         ...this.seedInstructedRun(seedEntries, connections, strict),
         builtThisRun: [],
         retainedThisRun: [],
         failures: {},
         failedReasons: [],
         refusals: [],
         builtSources: [],
         writtenTargets: new Map(),
      };
      assertNoTableCollisions(graphs, sources, connectionDigests, index);

      try {
         for (const graph of graphs) {
            const connection = connections.get(graph.connectionName);
            if (!connection) {
               throw new BadRequestError(
                  `Connection '${graph.connectionName}' not found`,
               );
            }
            for (const persistSource of iterGraphSources(graph, sources)) {
               if (signal.aborted) throw new Error("Build cancelled");
               await this.buildInstructedSource(
                  run,
                  persistSource,
                  graph.connectionName,
                  connection,
               );
            }
         }

         // Every source this run built failed, so it produced nothing. A build
         // with no output must not report itself as one that succeeded with
         // errors attached; the partial path above covers the case where at least
         // one source is usable. Thrown from inside the try so a run that wrote
         // nothing reclaimable still takes the same cleanup path as any other
         // total failure.
         if (run.builtSources.length === 0 && run.failedReasons.length > 0) {
            throw new Error(
               [
                  ...run.failedReasons,
                  ...run.refusals.map((e) => e.message),
                  ...priorRefusals.map((r) => r.message),
               ].join("; "),
            );
         }
         // Every instructed source was refused, so there was nothing to build:
         // the refusal is the run's answer, as it is for auto-run.
         if (run.builtSources.length === 0 && run.refusals.length > 0) {
            throw allSourcesRefusedError(run.refusals);
         }
      } catch (err) {
         // A part-way failure returns a manifest that records the sources which
         // built and the reason each failed one gave, so those tables stay
         // reachable to manifest-driven GC. This path is for a build that
         // produced nothing to record -- an orchestration failure, or every
         // source failing -- where a table an earlier source wrote would
         // otherwise be unreachable forever. Reclaim those before rethrowing.
         // Best-effort and non-fatal: the build's own failure is what the caller
         // needs to see.
         if (owner) {
            reportRetainedStorageTables(run.retainedThisRun, owner.packageName);
            await this.reclaimStorageTablesFromFailedRun(
               run.builtThisRun,
               environment,
               owner,
            );
         }
         throw err;
      }

      return {
         entries: run.entries,
         failures: run.failures,
         sourcesRefused: run.refusals.length,
      };
   }

   /**
    * The build Manifest and entry map a run starts from: every carried-forward
    * entry, so references to an upstream this run does not rebuild resolve.
    */
   private seedInstructedRun(
      seedEntries: Record<string, ManifestEntry>,
      connections: CompiledBuildPlan["connections"],
      strict: boolean,
   ): Pick<InstructedRun, "manifest" | "entries"> {
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
      return { manifest, entries };
   }

   /**
    * One persist source's turn in the build loop: resolve its instruction, gate
    * it, and build it — recording the outcome on `run`. A source that fails or is
    * refused is recorded and returns normally; only an orchestration failure
    * throws.
    */
   private async buildInstructedSource(
      run: InstructedRun,
      persistSource: PersistSource,
      graphConnectionName: string,
      connection: MalloyConnection,
   ): Promise<void> {
      const { compiled } = run;
      const resolved = resolveInstruction(
         run.index,
         persistSource,
         compiled.connectionDigests,
      );
      if (!resolved) return;
      const { instruction, orchestrated } = resolved;

      if (this.refuseIneligibleInstructedSource(run, persistSource, resolved)) {
         return;
      }

      // The manifest is keyed by the content sourceEntityId — what Malloy
      // recomputes to resolve upstream persist references during SQL
      // generation — independent of the instruction's identity sourceEntityId.
      // Already computed above when this source matched via the
      // sourceEntityId fallback; a sourceID match skips that path, so
      // compute it now that eligibility has cleared.
      const sourceEntityId =
         resolved.sourceEntityId ??
         computeSourceEntityId(persistSource, compiled.connectionDigests);

      // A caller-instructed destination that cannot be honored REFUSES; it
      // never falls through to a colocated build. Auto-run already applies
      // this rule, by skipping a `storage=` source while the tier is off
      // (see deriveSelfInstructions), for the reason given there: a
      // colocated build CTASes into the source's OWN warehouse, which the
      // author asked this data not to be written to. An instructed build
      // needs the rule more, not less — the caller named a destination AND
      // a physical name, so falling through writes that name into the
      // warehouse and answers success, and the caller can only detect it by
      // comparing the echoed destination against the one it sent.
      //
      // Refused rather than skipped because the caller asked for a specific
      // table: silence reads as "built". Stepping the mode down is a
      // rollback, so it has to stay safe on a loaded package — declining a
      // write is safe, redirecting one is not. Auto-run cannot reach this:
      // resolveStorageDestination returns undefined while off, so an
      // instruction still carrying a destination here is caller-supplied.

      // Auto-run already gated pre-getSQL in deriveSelfInstructions;
      // re-assert (idempotent) so no path into a storage build is ungated.
      if (
         !orchestrated &&
         instruction.destination &&
         getPersistStorageMode() !== "off"
      ) {
         assertMaterializationEligible(
            persistSource,
            deriveAnnotationFields(persistSource),
         );
      }

      // One physical table, written once. Several sources routinely map
      // onto one artifact: `#@ persist` is inherited and `extend` does not
      // change a source's materialization SQL, so a base and its extension
      // compile to the same address and resolve to the same instruction.
      // Iterating per SOURCE wrote the table once per name that reached it
      // — and once per graph that reached it, since a source declared in one
      // model and consumed in another appears in both models' graphs.
      //
      // Only the same-content case is decided here. Two DIFFERENT
      // definitions on one table was already reported, and refused under
      // PERSIST_COLLISION_ENFORCE, by the pre-pass above — before anything
      // was written. Reaching it here means the deployment chose to warn
      // and carry on, so the write proceeds as it did before.
      const target = physicalTargetKey(instruction, graphConnectionName);
      const written = run.writtenTargets.get(target);
      if (written && written.sourceEntityId === sourceEntityId) {
         // The extra name is another route to one artifact. Its address
         // already names the table it was built under, so there is nothing
         // to build and nothing to record.
         recordDuplicateTargetSkipped();
         logger.debug("Skipping a source whose table this run built", {
            sourceName: persistSource.name,
            builtAs: written.sourceName,
            physicalTableName: instruction.physicalTableName,
         });
         return;
      }

      let entry;
      try {
         assertInstructedDestinationWritable(
            persistSource,
            instruction,
            compiled,
         );

         entry = await this.buildOneSource(
            persistSource,
            instruction,
            connection,
            compiled.connectionDigests,
            run.manifest,
            run.environment,
            run.entries,
            run.buildMetadata,
            run.incremental,
            // The CONTENT address, distinct from the instruction's
            // caller-assigned identity: the ledger is keyed by it so a
            // boundary can never be read against different SQL.
            sourceEntityId,
            run.planFacts,
         );
         // Stamp what this table was built FOR, here rather than inside
         // buildOneSource, because this is the scope that holds the
         // compiled plan the answer comes from.
         //
         // Not decorative. A manifest travels without its build plan, so
         // this is the only thing that tells a consumer a table belongs to
         // a source appearing in no model file — and the serve path needs
         // it: a rollup handled as an ordinary binding is looked up by
         // name in the author's model, finds nothing, and is silently
         // dropped rather than served (see preaggregation_serve_bindings).
         if (compiled.preaggregatePlans?.[persistSource.sourceID]) {
            entry = { ...entry, origin: "preaggregate" as const };
         }
      } catch (buildErr) {
         recordInstructedBuildFailure(
            run,
            persistSource,
            instruction,
            sourceEntityId,
            buildErr,
         );
         return;
      }
      run.builtSources.push(persistSource.name);
      run.entries[sourceEntityId] = entry;
      run.writtenTargets.set(target, {
         sourceEntityId,
         sourceName: persistSource.name,
      });
      if (isReclaimableStorageTable(entry)) {
         run.builtThisRun.push(entry);
      } else if (entry.storageDestinationName) {
         // Kept so a failed run can SAY what it left behind. Recorded
         // here rather than logged here because a run that goes on to
         // succeed leaves nothing behind at all — its manifest names
         // every one of these.
         run.retainedThisRun.push(entry);
      }
   }

   /**
    * Gate one instructed source, recording a refusal as that source's failure.
    * Returns true when the source was refused and must not be built.
    */
   private refuseIneligibleInstructedSource(
      run: InstructedRun,
      persistSource: PersistSource,
      resolved: ResolvedInstruction,
   ): boolean {
      const { compiled } = run;
      const { instruction, orchestrated } = resolved;
      // A refusal here fails THIS source, not the run, for the same
      // reason auto-run skips one (deriveSelfInstructions): the run
      // throws only when every instructed source was refused. It is
      // reported in `failures`, not skipped silently, because the caller
      // asked for this table and an absent entry reads as "built". A
      // host that builds from the build plan never instructs a source the
      // plan refused, so reaching the catch means the plan and this gate
      // disagreed; the warning and the orchestrated `refused` source
      // count are how that shows up.
      try {
         // Enforce the eligibility gate for any storage-targeted build,
         // including orchestrated (host-supplied) instructions — the publisher
         // refuses an ineligible source into the tier itself, not on trust.
         // Skipped when the mode is off: the refusal below owns that case.
         if (
            orchestrated &&
            instruction.destination &&
            getPersistStorageMode() !== "off"
         ) {
            assertMaterializationEligible(
               persistSource,
               deriveAnnotationFields(persistSource),
            );
         } else {
            // The gate refusal above only fires for a STORAGE-targeted
            // build, so on its own it leaves every other instruction —
            // the colocated one with no destination, and any build while
            // the mode is off — unexamined. An orchestrated host chooses
            // the destination, so this path reaches that case with no
            // `#@ persist`-vs-`storage=` distinction to lean on; refuse
            // a gated source however it was instructed, unless its
            // compile-time gate outcome proves the colocated relaxation
            // applies (see `assertColocatedPersistNotAuthorizeGated`'s
            // doc). `else` rather than an unconditional call:
            // `assertMaterializationEligible` already runs the identical
            // `referencesAuthorize` IR walk, so calling both on the
            // storage path would walk every persist source's whole
            // `SourceDef` twice per build for an answer the first call
            // has already acted on.
            assertColocatedPersistNotAuthorizeGated(
               persistSource,
               persistSource.name,
               compiled.preaggregatePlans?.[persistSource.sourceID]
                  ? "preaggregate"
                  : "persist",
               compiled.sourceGateOutcomes?.[persistSource.sourceID],
               deriveAnnotationFields(persistSource),
            );
         }
      } catch (err) {
         if (!(err instanceof MaterializationEligibilityError)) throw err;
         // Keyed by the address the caller dispatched: a refused source
         // may have no content address of its own (getSQL throws for a
         // given or a free parameter), and the instruction's is the one
         // the caller staked a claim for and resolves failures by.
         const failedKey =
            resolved.sourceEntityId ?? instruction.sourceEntityId;
         if (!run.failures[failedKey]) {
            const reason = errMessage(err);
            run.refusals.push(err);
            logger.warn("Instructed source refused by the eligibility gate", {
               packageName: run.owner?.packageName,
               sourceName: persistSource.name,
               physicalTableName: instruction.physicalTableName,
               reason,
            });
            run.failures[failedKey] = {
               sourceEntityId: failedKey,
               sourceName: persistSource.name,
               materializedTableId: instruction.materializedTableId,
               physicalTableName: instruction.physicalTableName,
               reason,
               // Marks the failure permanent, so a caller that retries
               // failures can tell it from one that may clear.
               refused: true,
               connectionName: persistSource.connectionName,
               ...(instruction.destination
                  ? { storageDestinationName: instruction.destination }
                  : {}),
            };
            // Mirrored into `entries` for the `ManifestEntry.error`
            // deprecation window, as the build-failure path below does.
            run.entries[failedKey] = {
               sourceEntityId: failedKey,
               sourceName: persistSource.name,
               physicalTableName: instruction.physicalTableName,
               materializedTableId: instruction.materializedTableId,
               error: reason,
            } as ManifestEntry;
         }
         return true;
      }
      return false;
   }

   /**
    * Drop the storage tables a FAILED run created, so a partial build does not
    * leak an unreferenced table. For DuckLake that is data plus Parquet files at
    * rest, and nothing else will ever name them: the run commits no manifest, and
    * GC reclaims only what a manifest records.
    *
    * Three guards make this safe, and each closes a real way to destroy live data:
    *
    * - ORCHESTRATED runs only (the caller passes no `owner` for auto-run). Those
    *   names are host-assigned and generational, so unique by construction — which
    *   is both where the leak actually bites and the only case where a drop cannot
    *   hit something another run owns. Auto-run's STABLE names are overwritten in
    *   place by the next build, so skipping them forgoes little.
    *
    *   This gate is what bounds the cross-environment hazard. The
    *   still-referenced check below reads THIS environment and package only, and a
    *   BuildID carries no environment input, so two environments sharing a
    *   destination can resolve a source to the SAME physical name. A reclaim that
    *   trusted a per-environment check could then drop a table another environment
    *   is actively serving — the failure mode behind a real cross-environment
    *   data-loss incident on the hosted side. Generational names remove the
    *   collision rather than racing it. The durable fix is refusing a colliding
    *   persist target at validation time; until then, do not widen this.
    *
    * - Only entries this run CREATED (never a carried-forward one).
    * - Only names no other MANIFEST_FILE_READY run references, the same
    *   destination-and-name check {@link dropMaterializedTables} applies.
    *
    * Scoped to `storage=` entries. A colocated failure is left alone: those names
    * are stable and in the customer's own warehouse, so the next successful build
    * overwrites in place and a failure-path DROP there would be a far larger
    * blast radius for no reclaim.
    */
   /**
    * The destination's config for a best-effort table drop, or undefined when it
    * is no longer configured.
    *
    * The cleanup sweeps must keep going over the rest of a manifest rather than
    * abort on the first destination that has since been un-registered: a table
    * left behind is reclaimable later, whereas an aborted sweep leaves every
    * table after it behind too, and — in the delete path — throws out of a
    * sweep documented as best-effort. The build path deliberately does NOT go
    * through here; there, an unresolvable destination must fail the run.
    */
   private destinationForCleanup(
      environment: BuildEnvironment,
      destinationName: string,
   ): ApiConnection | undefined {
      try {
         return environment.getStorageDestination(destinationName);
      } catch (error) {
         logger.warn(
            "Skipping a table drop: its storage destination is no longer configured",
            { destinationName, error: errMessage(error) },
         );
         return undefined;
      }
   }

   private async reclaimStorageTablesFromFailedRun(
      builtThisRun: ManifestEntry[],
      environment: BuildEnvironment,
      owner: { environmentId: string; packageName: string },
   ): Promise<void> {
      if (builtThisRun.length === 0) return;
      try {
         const tableKey = (dest: string, table: string) => `${dest}:${table}`;
         const stillReferenced = new Set<string>();
         const others =
            (await this.repository.listMaterializations(
               owner.environmentId,
               owner.packageName,
            )) ?? [];
         for (const other of others) {
            if (other.status !== "MANIFEST_FILE_READY") continue;
            for (const e of Object.values(other.manifest?.entries ?? {})) {
               const dest = e.storageDestinationName ?? e.connectionName;
               if (dest && e.physicalTableName) {
                  stillReferenced.add(tableKey(dest, e.physicalTableName));
               }
            }
         }

         for (const entry of builtThisRun) {
            const dest = entry.storageDestinationName;
            const table = entry.physicalTableName;
            if (!dest || !table) continue;
            if (stillReferenced.has(tableKey(dest, table))) {
               logger.info(
                  "Keeping a table from a failed run: a live manifest still serves it",
                  { destinationName: dest, physicalTableName: table },
               );
               continue;
            }
            const destinationConnection = this.destinationForCleanup(
               environment,
               dest,
            );
            if (!destinationConnection) continue;
            try {
               await dropStorageTable({
                  destinationName: dest,
                  destinationConnection,
                  physicalTableName: table,
                  environmentPath: environment.getEnvironmentPath(),
               });
               recordDropTables("success", "storage");
               logger.info("Reclaimed a table stranded by a failed build", {
                  destinationName: dest,
                  physicalTableName: table,
               });
            } catch (dropErr) {
               recordDropTables("failure", "storage");
               logger.warn(
                  "Failed to reclaim a table stranded by a failed build",
                  {
                     destinationName: dest,
                     physicalTableName: table,
                     error: redactConnectionSecrets(
                        errMessage(dropErr),
                        destinationConnection,
                     ),
                  },
               );
            }
         }
      } catch (err) {
         // Never let cleanup mask the build failure that triggered it.
         logger.warn("Failed-run table reclaim did not complete", {
            error: errMessage(err),
         });
      }
   }

   /**
    * The `RunSQLOptions` for one source's build statements: its resolved
    * per-query metadata, merged under this run's context.
    *
    * Layers, least specific first: the executing connection's default, then what
    * the model side declared for this source (package → model-file → `#@ persist`,
    * already resolved by {@link resolveQueryMetadata}), then the run's context,
    * which names the source. There is no request layer — a build has no request.
    *
    * Fails OPEN: a build must not fail because metadata could not be assembled,
    * so an unresolvable connection just contributes no default, and a dropped
    * property is logged and metered rather than thrown.
    */
   private buildRunSQLOptions(
      persistSource: PersistSource,
      environment: BuildEnvironment,
      buildMetadata: BuildQueryMetadata | undefined,
   ): { queryMetadata?: QueryMetadata } {
      if (!buildMetadata) return {};
      const connectionLayers = connectionMetadataLayers(
         environment,
         persistSource.connectionName,
      );
      const resolved = mergeQueryMetadata({
         connection: connectionLayers.default,
         enforced: connectionLayers.enforced,
         model: resolveQueryMetadata(
            persistSource,
            buildMetadata.packageMaterialization,
         ),
         context: { ...buildMetadata.context, source: persistSource.name },
      });
      if (resolved.drops.length > 0) {
         logger.warn("Dropped query-metadata properties for a build", {
            sourceName: persistSource.name,
            drops: resolved.drops,
         });
      }
      return resolved.metadata ? { queryMetadata: resolved.metadata } : {};
   }

   /**
    * The `RunSQLOptions` for the drops that retire a materialization's tables.
    * No model layer: the source's declaration described how to BUILD it, and the
    * source may no longer exist by the time its table is retired.
    */
   private dropRunSQLOptions(
      environment: BuildEnvironment,
      connectionName: string,
      environmentName: string,
      packageName: string,
      materializationId: string,
   ): { queryMetadata?: QueryMetadata } {
      const connectionLayers = connectionMetadataLayers(
         environment,
         connectionName,
      );
      const resolved = mergeQueryMetadata({
         connection: connectionLayers.default,
         enforced: connectionLayers.enforced,
         context: {
            queryClass: "ops",
            environment: environmentName,
            package: packageName,
            runId: materializationId,
         },
      });
      return resolved.metadata ? { queryMetadata: resolved.metadata } : {};
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
      buildMetadata?: BuildQueryMetadata,
      // Present only when the run resolved at least one incremental declaration;
      // absent leaves this method exactly the full-rebuild path it was.
      incremental?: IncrementalRunContext,
      // The source's content address — NOT `instruction.sourceEntityId`, which is
      // caller-assigned and opaque (see the index comment in
      // executeInstructedBuild). Recorded on the covered_through ledger and
      // COMPARED on every read, which is what guarantees a boundary is never read
      // against SQL other than the SQL it was computed from: different SQL is a
      // mismatch, so a changed source seeds instead of deltaing onto a stale table.
      // (It used to be part of the ledger's key, which gave the same guarantee for
      // free but made a boundary un-findable across a package's versions.)
      contentSourceEntityId?: string,
      // Which names the plan holds as stored tables, and which share one. The
      // default knows nothing, which only the test seams rely on.
      planFacts: ChainedPlanFacts = NO_PLAN_FACTS,
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
      // recompute). A stack-on-the-parent build reads the parent's lake table
      // instead, but via a separate DuckDB recompile that does NOT use this
      // warehouse buildSQL — this remains its recompute-from-raw fallback.
      //
      // Which is why a storage build generates it permissively. For that build this
      // SQL is only the fallback, so refusing to GENERATE it refuses the source
      // before the parent-reuse attempt below can run: the strict-miss fires on SQL
      // nothing was going to execute, and a chained storage source becomes
      // unbuildable under `strictUpstreams` even though reading the parent needs no
      // recompute at all. Strict is enforced where a recompute would really happen —
      // buildOneSourceIntoStorage refuses if stacking on the parent proves
      // impossible. A colocated build keeps strict here: its buildSQL IS what runs,
      // and it has no parent to stack on.
      const reducedManifest = manifestExcludingStorage(manifest, builtEntries);
      const buildManifest = isStorageBuild
         ? { ...reducedManifest, strict: false }
         : reducedManifest;
      const buildSQL = persistSource.getSQL({
         buildManifest,
         connectionDigests,
      });

      // Which stored tables this source reads, and whether the manifest the
      // compiler substituted from holds each — decided once here, for both
      // tiers, from the walk over the author model and the content addresses
      // the compiler looks tables up by. Everything below that turns on a
      // stored upstream reads this: the `storage=` branch's choice to stack on
      // the parents, strict's refusal of an upstream nothing supplied, and the
      // `upstreamReuse` the entry reports.
      //
      // `_model._modelDef` is the compiler's internal shape of the compiled
      // model. Should a compiler bump take it away, the walk finds nothing, the
      // build proceeds as a source with no stored upstream, and the entry
      // carries no `upstreamReuse` — a visible gap, not a wrong answer.
      const lift = authorModelLiftContext(
         persistSource._model?._modelDef,
         readAuthorFile,
      );
      const reached = reachedPersistedSources(
         lift,
         persistSource.name,
         (_name, sourceID) => planFacts.persistSourceIds.has(sourceID),
      );
      // The compiler's reach, twice. Permissively, with every stored entry
      // available, compared with the SQL this build runs: a difference means a
      // stored table the full manifest holds was inlined, whether or not the
      // walk named it. Strictly, with the same manifest: the one miss possible
      // is a persist source the SQL reads that nothing supplied — the recompute
      // strict refuses — named by the compiler rather than inferred from
      // declared joins.
      //
      // Both renders are the classification's, not the build's: a colocated
      // build's SQL is already in hand and runs regardless. So on that path a
      // render that fails leaves the entry without `upstreamReuse` (logged)
      // rather than failing a warehouse build that was never in question; a
      // storage build keeps failing on it, as its compare always has, because
      // the stack-on-parent decision needs the answer. Neither render runs
      // when nothing was excluded from the build manifest — no stored table
      // the compiler could inline differently — and the strict render is
      // skipped for a strict colocated build, whose build SQL was itself
      // rendered strictly a moment ago.
      const fullForCompiler = manifestForCompiler(
         manifest.buildManifest,
         planFacts,
      );
      const excludedSomething =
         Object.keys(reducedManifest.entries).length !==
         Object.keys(manifest.buildManifest.entries).length;
      let sqlInlinesStored = false;
      let compilerMissing: { name: string; sourceID: string }[] = [];
      let classified = true;
      try {
         if (excludedSomething) {
            sqlInlinesStored =
               persistSource.getSQL({
                  buildManifest: { ...fullForCompiler, strict: false },
                  connectionDigests,
               }) !== buildSQL;
         }
         if (isStorageBuild || !manifest.strict) {
            compilerMissing = compilerMissingUpstream(
               persistSource,
               fullForCompiler,
               connectionDigests,
               lift.sourceNameById,
            );
         }
      } catch (err) {
         if (isStorageBuild) throw err;
         classified = false;
         logger.warn(
            "Could not classify a colocated build's stored upstreams; the entry carries no upstreamReuse",
            {
               sourceName: persistSource.name,
               error: err instanceof Error ? err.message : String(err),
            },
         );
      }
      const reuse = classified
         ? upstreamReuseFromManifest({
              reached: reached.persisted,
              addressBySourceId: planFacts.addressBySourceId,
              substituted: buildManifest.entries,
              full: manifest.buildManifest.entries,
              builtEntries,
              tier: isStorageBuild ? "storage" : "colocated",
              sqlInlinesStored,
              compilerMissing,
              // The content address, not the instruction's id: an orchestrator
              // may assign its own ids, and the walk's addresses are content.
              rootAddress: contentSourceEntityId ?? instruction.sourceEntityId,
           })
         : { fields: {}, inManifestOnly: [], missing: [] };

      // Every statement of this source's build carries the same metadata, so the
      // warehouse's query history shows the staging CTAS, the drop and the rename
      // as one attributable unit of work.
      //
      // Resolved before the storage branch below so BOTH build paths carry the
      // same bag, layered the same way. A `storage=` build reads through DuckDB's
      // query-passthrough rather than a Malloy connection, so it applies these
      // itself instead of handing them to `runSQL` — but what it applies has to be
      // the same properties, or one deployment's query history attributes the two
      // paths differently.
      const runOptions = this.buildRunSQLOptions(
         persistSource,
         environment,
         buildMetadata,
      );

      // `storage=` build: materialize into a DuckDB/DuckLake destination via a
      // build-scoped session (never on the source or serve connection). Diverges
      // fully from the in-warehouse CTAS below — different engine, credential
      // federation, and a captured authoritative schema for the serve transform.
      // Gated by the kill switch: when off, ignore a destination and do a colocated build.
      if (isStorageBuild) {
         // A stored upstream nothing supplied — neither built in this run nor
         // handed over by reference — is one only a recompute can stand in for,
         // and under strict that recompute is exactly what the orchestrator
         // forbade. The warehouse build SQL above was rendered permissively
         // (`strict: false`), so the refusal is made here, from the compiler's
         // own strict render over the full manifest (`compilerMissing`), which
         // names what the SQL reads. Non-strict, the build recomputes and the
         // entry says so.
         if (manifest.strict && reuse.missing.length > 0) {
            recordChainedStorageBuild("strict_refused");
            recordStorageBuildFailure(instruction.destination!);
            throw new Error(
               `Failed to materialize source '${persistSource.name}' into ` +
                  `storage destination '${instruction.destination}': persisted ` +
                  `upstream ${reuse.missing.map((n) => `'${n}'`).join(", ")} is ` +
                  `not in this build's manifest (neither built in this run nor ` +
                  `supplied by reference), and strict upstreams forbid ` +
                  `recomputing it from raw`,
            );
         }
         // Stack on the parent when the source reads a stored upstream whose
         // table a storage destination holds — one the walk named, or one the
         // compiler reached through a join on a stored stop: either way the
         // build SQL above inlined it, and reading the parent's lake table
         // instead is the chained build.
         //
         // The compare alone decides: a stored table the SQL reads is one the
         // permissive render substitutes and the build render inlines, so the
         // two differ exactly when there is a parent to stack on. A walk stop
         // the SQL never reads (a declared join the query does not use) must
         // not start a chained attempt the passthrough build did not need.
         const dependsOnStorageUpstream = sqlInlinesStored;
         // Materialize ONLY the source's PUBLIC columns. `getSQL` projects every
         // underlying column, including ones the source hides (`except:`, non-public
         // access modifiers). Query reachability is bounded by the declared
         // ::Shape, which the serve transform narrows to the public surface
         // (proven by the shape-bounds-physical-columns scenario) — so what this
         // prevents is the hidden column's VALUES sitting at rest in the
         // destination store, reachable by direct catalog access and possibly
         // across a trust boundary the source's visibility was meant to hold.
         // Refuses the build (422) if the public surface can't be determined.
         const publicBuildSQL = projectToPublicColumns(persistSource, buildSQL);
         return this.buildOneSourceIntoStorage({
            persistSource,
            instruction,
            manifest,
            environment,
            publicBuildSQL,
            // The UNprojected form as well, for a delta: it applies the public
            // projection outermost, around its own range predicate, so the two
            // write the same columns without the predicate having to survive a
            // projection that may not carry the watermark.
            buildSQL,
            builtEntries,
            dependsOnStorageUpstream,
            // What the entry reports when the build does not stack on a parent:
            // the manifest's answer for the upstreams the passthrough SQL read.
            baseReuse: reuse.fields,
            queryMetadata: runOptions.queryMetadata,
            incremental,
            contentSourceEntityId,
            planFacts,
            lift,
         });
      }

      // Incremental refresh: a source that declares `refresh="incremental"` and a
      // usable watermark can advance its serving table by a bounded delta instead
      // of being rebuilt below. Every source that declares nothing — and every
      // one whose declaration the delta path cannot act on — gets `undefined`
      // here and falls straight through to the CTAS, unchanged.
      const dialect = persistSource.dialectName;
      const quotedPhysicalPath = quoteTablePath(physicalTableName, dialect);
      // Colocated by construction: the storage branch above has already returned,
      // so the table lives in the source's own warehouse and one dialect answers
      // both halves.
      const lineage =
         incremental && contentSourceEntityId
            ? incrementalLineage({
                 declaration: incremental.declarations[persistSource.sourceID],
                 dialect,
                 targetDialect: dialect,
                 physicalTableName,
                 connectionName: persistSource.connectionName,
                 sourceEntityId: contentSourceEntityId,
              })
            : undefined;
      // One narrowing for the three incremental touchpoints below, so they can
      // never disagree about whether this source is on the delta path.
      const incrementalRefresh =
         incremental && lineage && contentSourceEntityId
            ? {
                 context: incremental,
                 lineage,
              }
            : undefined;
      if (incrementalRefresh) {
         const applied = await refreshOneSourceIncrementally({
            ...incrementalRefresh,
            persistSource,
            instruction,
            target: warehouseDeltaTarget({
               dialect,
               runner: (sql) => connection.runSQL(sql, runOptions),
               quotedTablePath: quotedPhysicalPath,
               lineage: incrementalRefresh.lineage,
               // The CTAS's own SQL, manifest-resolved. The delta filters this
               // exact string, so it computes what a rebuild would — see
               // deltaSelect.
               sourceSQL: buildSQL,
            }),
            manifest,
            upstreamReuse: reuse.fields,
         });
         if (applied) return applied;
      }

      const stagingTableName = `${physicalTableName}${stagingSuffix(sourceEntityId)}`;
      // The control plane sends the logical (unquoted) physical name; dialect-
      // quote each identifier here so a container path or quote-requiring name
      // (e.g. a hyphenated BigQuery project id) produces valid DDL. The manifest
      // echoes the logical name (below) so the CP stays in logical-name space.
      const quotedStaging = quoteTablePath(stagingTableName, dialect);
      const quotedPhysical = quotedPhysicalPath;
      // Not the bare name unconditionally: on a dialect that resolves an
      // unqualified rename target against the session, a bare target moves the
      // finished table into the session's default container.
      const quotedRenameTarget = quoteRenameTarget(physicalTableName, dialect);

      // The rebuild below replaces the table the boundary describes, so the
      // boundary is dropped BEFORE it starts rather than overwritten after: a
      // crash mid-rebuild would otherwise leave a boundary pointing at data that
      // no longer exists, and the next run would apply a delta on top of it.
      if (incrementalRefresh) {
         await resetLedger(
            incrementalRefresh.context,
            incrementalRefresh.lineage,
         );
      }

      const startTime = performance.now();
      await connection.runSQL(
         `DROP TABLE IF EXISTS ${quotedStaging}`,
         runOptions,
      );
      // The CTAS is the only statement here that reads the source data, so it is
      // the only one whose cost is worth reporting: the DROPs and the RENAME are
      // metadata operations. Undefined on any backend that does not report a
      // figure — see the manifest entry's queryCostBytes.
      let buildCostBytes: number | undefined;
      try {
         const ctas = await connection.runSQL(
            `CREATE TABLE ${quotedStaging} AS (${buildSQL})`,
            runOptions,
         );
         buildCostBytes = ctas?.runStats?.queryCostBytes;
         await connection.runSQL(
            `DROP TABLE IF EXISTS ${quotedPhysical}`,
            runOptions,
         );
         await connection.runSQL(
            `ALTER TABLE ${quotedStaging} RENAME TO ${quotedRenameTarget}`,
            runOptions,
         );
      } catch (err) {
         try {
            await connection.runSQL(
               `DROP TABLE IF EXISTS ${quotedStaging}`,
               runOptions,
            );
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

      // The table now holds a full snapshot, so record where that snapshot
      // reaches: this is what turns the NEXT refresh into a delta.
      const seededThrough = incrementalRefresh
         ? await advanceLedgerAfterSeed({
              context: incrementalRefresh.context,
              lineage: incrementalRefresh.lineage,
              quotedTablePath: quotedPhysical,
              dialect,
              runner: (sql) => connection.runSQL(sql, runOptions),
           })
         : undefined;

      return {
         sourceEntityId,
         sourceName: persistSource.name,
         materializedTableId: instruction.materializedTableId,
         physicalTableName,
         connectionName: persistSource.connectionName,
         realization: instruction.realization,
         // The table now holds a full snapshot, so the boundary probed from it
         // is what turns the NEXT refresh into a delta.
         ...ledgerFields(incrementalRefresh?.lineage, seededThrough),
         // Reported for an incremental source whatever brought it here — no
         // recorded boundary, `reseed`, a table the delta path could not build
         // on — because from outside, a rebuild in the name of an incremental
         // refresh is otherwise indistinguishable from a delta.
         ...refreshFields(incrementalRefresh ? "full" : undefined),
         rowCount: null,
         buildDurationMs: durationMs,
         // The recurring warehouse cost of keeping this source materialized —
         // the debit against whatever the materialization saves on the read side.
         queryCostBytes: buildCostBytes ?? null,
         ...reuse.fields,
      };
   }

   /**
    * Materialize a source into a `storage=` destination (a DuckDB/DuckLake
    * connection) via a native query-passthrough CTAS on a build-scoped session.
    * Records the destination connection and the captured authoritative DuckDB
    * schema on the manifest entry so the source can later be served
    * cross-dialect from the destination (the serve transform declares that
    * schema). `connectionName` still names the SOURCE warehouse (where data is
    * read from); `storageDestinationName` names where the table now lives.
    */
   private async buildOneSourceIntoStorage(params: {
      persistSource: PersistSource;
      instruction: BuildInstruction;
      manifest: Manifest;
      environment: BuildEnvironment;
      /** The public-column projection of the build SQL, which the CTAS reads. */
      publicBuildSQL: string;
      /** The same SQL unprojected, which a delta wraps in its range predicate. */
      buildSQL: string;
      builtEntries: Record<string, ManifestEntry>;
      dependsOnStorageUpstream: boolean;
      /**
       * Applied to the warehouse read by the passthrough itself — see
       * {@link buildSourceIntoStorage}. Resolved by the caller through the same
       * layering the colocated path uses.
       */
      queryMetadata?: QueryMetadata;
      /** Present when any source in the run declared an incremental refresh. */
      incremental?: IncrementalRunContext;
      /** The source's CONTENT address — see buildOneSource's parameter of the same name. */
      contentSourceEntityId?: string;
      /** The plan's stored-table facts — see buildOneSource's parameter of the same name. */
      planFacts?: ChainedPlanFacts;
      /** The author-model lift context `buildOneSource` built for this source. */
      lift?: ReturnType<typeof authorModelLiftContext>;
      /**
       * The `upstreamReuse` fields for a build that does NOT stack on a parent,
       * decided by the caller from the manifest the passthrough SQL was rendered
       * with ({@link upstreamReuseFromManifest}).
       */
      baseReuse?: Pick<
         ManifestEntry,
         "upstreamReuse" | "upstreamRecomputeReason"
      >;
   }): Promise<ManifestEntry> {
      const {
         persistSource,
         instruction,
         manifest,
         environment,
         builtEntries,
         dependsOnStorageUpstream,
         queryMetadata,
         incremental,
         planFacts = NO_PLAN_FACTS,
         baseReuse = {},
         lift = authorModelLiftContext(
            params.persistSource._model?._modelDef,
            readAuthorFile,
         ),
      } = params;
      const sourceEntityId = instruction.sourceEntityId;
      const physicalTableName = instruction.physicalTableName;
      const destinationName = instruction.destination!;
      const sourceConnection = environment.getApiConnection(
         persistSource.connectionName,
      );
      // Resolved from the destination list, never the connection list: a build
      // naming a destination that is not configured has to fail here rather than
      // fall through to a same-named connection and write a tenant's own
      // warehouse. The throw surfaces as a 422 on the run.
      const destinationConnection =
         environment.getStorageDestination(destinationName);

      // Incremental refresh of a STORED table. The delta's rows are computed by the
      // source warehouse and its DML runs in the destination engine, which is what
      // the target below expresses; everything about deciding seed-vs-delta is the
      // shared planner's, exactly as for a colocated table.
      const lineage =
         incremental && params.contentSourceEntityId
            ? incrementalLineage({
                 declaration: incremental.declarations[persistSource.sourceID],
                 dialect: persistSource.dialectName,
                 targetDialect: STORAGE_TARGET_DIALECT,
                 physicalTableName,
                 connectionName: persistSource.connectionName,
                 storageDestinationName: destinationName,
                 sourceEntityId: params.contentSourceEntityId,
              })
            : undefined;
      // A CHAINED stored source is built by recompiling over its parents' lake
      // tables, and a delta over that is not designed yet: if the parent is itself
      // incremental, its own delta can restate rows BELOW this source's frontier,
      // which a half-open range would never revisit. So it rebuilds — reported
      // under its own reason code rather than left to look like a source that was
      // never incremental, which is what an absent `refresh` field would say.
      const chainedSeed = lineage !== undefined && dependsOnStorageUpstream;
      if (chainedSeed && lineage) {
         reportIncrementalStep({
            step: {
               mode: "seed",
               reasonCode: "chained_storage",
               reason:
                  `the source reads a storage-materialized upstream, and a delta ` +
                  `over a stored parent is not supported yet`,
            },
            sourceName: persistSource.name,
            packageName: incremental!.packageName,
            physicalTableName,
         });
         // Same reason the delta path clears a boundary before a rebuild: the
         // build below replaces the table this one describes, and a crash in
         // between must not leave a delta reading it. Reachable when a source that
         // was NOT chained becomes chained, which keeps both its name and its
         // content address.
         await resetLedger(incremental!, lineage);
      }
      const refresh: StorageIncrementalRefresh | undefined =
         incremental && lineage && !chainedSeed
            ? storageRefreshFor({
                 context: incremental,
                 lineage,
                 persistSource,
                 instruction,
                 destinationName,
                 physicalTableName,
                 buildSQL: params.buildSQL,
                 queryMetadata,
              })
            : undefined;

      const startTime = performance.now();
      let result;
      // Set when stacking on the parents was declined and the recompute below
      // ran instead, so the entry can say so and why.
      let upstreamRecomputeReason: string | undefined;

      // Stack on the parent: a source that reads a storage-materialized
      // upstream is built by reading the parent's STORED lake table instead of
      // recomputing it from raw against the warehouse. This reuses the
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
               planFacts,
               params.contentSourceEntityId ?? instruction.sourceEntityId,
               lift,
            );
            recordChainedStorageBuild("parent_reuse");
         } catch (err) {
            // Same redaction contract as the recompute-from-raw path below: this
            // branch's read-write ATTACH or CTAS can fail with the offending SQL
            // echoed back, catalog `password=` included. Redact before the
            // message reaches the thrown run `error` or the log.
            const safeDetail = redactConnectionSecrets(
               errMessage(err),
               sourceConnection,
               destinationConnection,
            );
            // Only a SHAPE failure justifies recomputing from raw: the downstream
            // could not be expressed over its rebound parents, so building it the
            // other way is a genuinely different attempt. An INFRA failure (the
            // read-write attach, the CTAS, the destination being down) is not —
            // recompute-from-raw writes to the SAME destination and fails the same
            // way, and metering it as `inline_fallback` files an outage in the same
            // bucket as a legitimate shape miss.
            // Strict's line runs between what the build was HANDED and what it
            // can EXPRESS. A persisted upstream it cannot see, or a shape over
            // stored upstreams it could not carry, would both be built by
            // recomputing tables the orchestrator pinned — refused. A downstream
            // that reaches the source warehouse has no build over the parents at
            // all, so the recompute is the only build there is, and strict
            // permits it — reported, on the entry and the counter, because the
            // rows then come from the warehouse at this build's time rather than
            // from the parents' snapshots.
            const pinnedRecompute =
               err instanceof ChainedUpstreamMissingError ||
               err instanceof ChainedShapeNotCarriedError;
            if (
               !pinnedRecompute &&
               !(err instanceof MaterializationEligibilityError)
            ) {
               recordChainedStorageBuild("infra_failure");
               recordStorageBuildFailure(destinationName);
               throw new Error(
                  `Failed to materialize chained source '${persistSource.name}' ` +
                     `into storage destination '${destinationName}': ${safeDetail}`,
               );
            }
            if (manifest.strict && pinnedRecompute) {
               recordChainedStorageBuild("strict_refused");
               recordStorageBuildFailure(destinationName);
               throw new Error(
                  `Failed to materialize chained source '${persistSource.name}' ` +
                     `into storage destination '${destinationName}' by reading ` +
                     `its materialized upstream, and strict upstreams forbid ` +
                     `recomputing it from raw: ${safeDetail}`,
               );
            }
            recordChainedStorageBuild(
               manifest.strict ? "strict_shape_fallback" : "inline_fallback",
            );
            upstreamRecomputeReason = safeDetail;
            logger.warn(
               "Chained storage build could not reuse the parent table; " +
                  "recomputing the upstream from raw",
               {
                  sourceName: persistSource.name,
                  destinationName,
                  strict: manifest.strict,
                  reason: safeDetail,
               },
            );
         }
      }

      // Recompute from raw (the single-source passthrough): materialize `buildSQL`
      // (with storage upstreams inlined) in the source warehouse and CTAS the result
      // into the destination. Skipped when stacking on the parent already produced
      // the table above.
      if (!result) {
         try {
            result = await buildSourceIntoStorage({
               destinationName,
               destinationConnection,
               sourceConnection,
               buildSQL: params.publicBuildSQL,
               physicalTableName,
               partitionColumns: partitionColumnsForBuild(persistSource),
               environmentPath: environment.getEnvironmentPath(),
               queryMetadata,
               incremental: refresh,
            });
         } catch (err) {
            // Redaction: a failed federation / passthrough / attach
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
            // Whether the warehouse read was already BILLED survives the
            // redaction, because it changes what a caller should do next. This
            // service's own scheduler advances "regardless of outcome so a
            // persistent failure retries on the next cron occurrence" and fires
            // with forceRefresh, which defeats skip-if-unchanged — so a
            // persistent cause re-runs the same read every occurrence, and on
            // this branch that means paying for it every occurrence. Rewrapping
            // as a bare Error made that indistinguishable from an attach
            // failure, which is free to retry.
            const alreadyBilled = err instanceof BilledReadNotCapturedError;
            recordStorageBuildFailure(
               destinationName,
               alreadyBilled ? "billed_read_not_captured" : "build_failed",
            );
            logger.warn("Storage materialization build failed", {
               sourceName: persistSource.name,
               destinationName,
               alreadyBilled,
               error: safeDetail,
            });
            const failure =
               `Failed to materialize source '${persistSource.name}' into ` +
               `storage destination '${destinationName}': ${safeDetail}`;
            throw alreadyBilled
               ? new BilledReadNotCapturedError(failure, { cause: err })
               : new Error(failure, { cause: err });
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
         // An in-place refresh is exempt from the drop below, and that exemption
         // is the whole point of the branch: the table it advanced is the LIVE
         // serving generation, so dropping it would take a serving table out over
         // a shape the delta could not have changed (a definitional change
         // re-addresses the source, and a shape that drifted anyway forces a
         // rebuild). The refusal still fails the run, which leaves the boundary
         // unrecorded by the caller and the next refresh re-applying the same
         // idempotent range.
         if (result.refresh) throw gateErr;
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
      // so a chained storage source can stack on it (above).
      manifest.update(sourceEntityId, { tableName: physicalTableName });

      const durationMs = Math.round(performance.now() - startTime);
      // A delta and a rebuild have different cost profiles, so they get different
      // series — and a SKIP is timed by neither, having done no work at all.
      if (result.refresh?.refresh === "delta") {
         recordSourceBuildDuration(durationMs, "delta_storage");
      } else if (!result.refresh) {
         recordSourceBuildDuration(durationMs, "storage");
      }
      logger.info(
         result.refresh
            ? `Refreshed materialized source ${persistSource.name} in storage`
            : `Built materialized source ${persistSource.name} into storage`,
         {
            physicalTableName,
            storageDestinationName: result.storageDestinationName,
            columns: result.schema.length,
            durationMs,
            refresh: result.refresh?.refresh,
            // The whole cost, not just the one field the manifest carries. These
            // are the numbers that answer a cost question and the ids that let a
            // human reach the job in the warehouse's own console — the manifest
            // has room for neither. Null says the read's shape reported nothing,
            // which is not the same as free: see BuildReadCost.
            readCost: result.readCost,
         },
      );

      return {
         sourceEntityId,
         sourceName: persistSource.name,
         materializedTableId: instruction.materializedTableId,
         physicalTableName,
         connectionName: persistSource.connectionName,
         storageDestinationName: result.storageDestinationName,
         schema: result.schema,
         realization: instruction.realization,
         rowCount: null,
         // A refresh reports what the APPLY took, matching the colocated path and
         // the field's own definition ("around the build itself"), rather than the
         // session setup around it. A SKIP applied nothing and so reports null:
         // the wall-clock of deciding that would average into the series as an
         // implausibly fast build.
         buildDurationMs: result.refresh
            ? (result.refresh.durationMs ?? null)
            : durationMs,
         // Where the table's coverage now reaches, and what this run DID to it —
         // reported for a stored table exactly as for a colocated one, so a caller
         // reads incremental progress rather than inferring it. Absent for a
         // source that is not refreshed incrementally.
         ...ledgerFields(
            lineage,
            result.refresh?.coveredThrough ?? result.seededThrough,
         ),
         ...refreshFields(
            result.refresh?.refresh ?? (lineage ? "full" : undefined),
         ),
         // How this source's stored upstreams were read. A build that stacked
         // on its parents, or declined to, answers for itself; one with no
         // parent to stack on reports what the manifest said about the
         // upstreams its passthrough SQL read. A source with no persisted
         // upstream has only one way to build and says nothing here.
         ...(dependsOnStorageUpstream
            ? {
                 upstreamReuse: result.upstreamReuse ?? "recomputed",
                 ...(upstreamRecomputeReason
                    ? { upstreamRecomputeReason }
                    : {}),
              }
            : baseReuse),
         // SCANNED, matching the colocated path above, which fills this from the
         // connector's runStats -- and that is totalBytesProcessed, i.e. scanned.
         // Reporting billed here would put two different quantities in one field,
         // differing by up to BigQuery's 10MB floor, and anyone summing it across
         // a package's sources would add them together.
         //
         // Null when the read's shape reported nothing: a rows-returning
         // passthrough call hands back no job to account for. Today that means a
         // build whose metadata bag was empty, since it is the label that makes
         // BigQuery's read a form that reports.
         queryCostBytes: result.readCost?.bytesScanned ?? null,
      };
   }

   /**
    * Stack on the parent: materialize a chained storage source by reading its
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
      planFacts: ChainedPlanFacts,
      /** The content address being built; a walk stop sharing it is this source's own table. */
      rootAddress: string,
      lift: ReturnType<typeof authorModelLiftContext>,
   ): Promise<StorageBuildResult> {
      // Rebind every upstream materialized into THIS destination, under every
      // name that is that table: an entry names only the source that built it,
      // and a rename of it (`source: x is daily`) is the same table under another
      // name, so it is bound to the same handle, as the serve path binds it. An
      // EXTENSION of a parent is the same table too, but with refinements the
      // table does not hold, so it is not bound here — it is carried below as a
      // lift over its base, which re-declares what it adds. A parent in a
      // DIFFERENT destination is absent here, and reported as missing below.
      const aliasesForBinding: Record<string, string[]> = {};
      for (const [name, aliases] of Object.entries(
         planFacts.aliasesBySourceName,
      )) {
         aliasesForBinding[name] = aliases.filter(
            (alias) => typeof lift.contents[alias]?.extends !== "string",
         );
      }
      const upstreams: ServeBinding[] = deriveServeBindings(
         builtEntries,
         aliasesForBinding,
      )
         .filter((b) => b.destinationName === destinationName)
         // A rollup is never an upstream: nothing can reference one, because its
         // name is synthesized and appears in no model file. Inert if left in —
         // but inert for a reason that expired once on the serve path, where these
         // same bindings later acquired refinements and the assumption that they
         // carry none stopped holding. Filtered so the set means what it is named,
         // the parents a downstream could build on.
         //
         // It also restores the legible refusal below: a destination holding only
         // rollups now reports "no materialized upstream is available" instead of
         // proceeding and failing later on a compile against an absent parent.
         .filter((b) => b.origin !== "preaggregate");
      // Each parent re-declares what its source adds to the table — its
      // extend-block `where:`, dimensions, measures, joins and views — as the
      // serve shape does. A persist source's build SQL is the persisted
      // relation alone, so a binding without them reads the table unfiltered.
      const present = new Set(upstreams.map((b) => b.sourceName));
      for (let i = 0; i < upstreams.length; i++) {
         upstreams[i] = {
            ...upstreams[i],
            refinements: authorRefinementsFor(
               upstreams[i].sourceName,
               lift,
               present,
            ),
         };
      }
      // Which stored tables the downstream reads, directly or through the
      // sources between them, by the names it reaches them under. One whose
      // table is absent from this destination's parents is a table this build
      // cannot see — neither built here nor referenced, or built into another
      // destination. That is the dispatch miss strict refuses, and it is named
      // here, by source, before any compile: the compiler would report only
      // "undefined object", which reads the same for a missing parent and for a
      // warehouse table.
      const reached = reachedPersistedSources(
         lift,
         persistSource.name,
         (_name, sourceID) => planFacts.persistSourceIds.has(sourceID),
      );
      // The stops that are upstreams: one sharing the root's address is the
      // table being built (a persisted extension and its base), not a parent.
      const stops = reached.persisted.filter(
         (p) => planFacts.addressBySourceId[p.sourceID] !== rootAddress,
      );
      const missing = missingPersistedTables(
         stops.map((p) => p.name),
         planFacts.aliasesBySourceName,
         present,
      );
      if (missing.length > 0) {
         // Say where the table is, not only where it is not: a parent in the
         // source warehouse or in another destination is fine where it lives
         // and is simply out of this build's reach, and the tables strict is
         // protecting are the stored ones a recompute of this source would
         // rebuild.
         const where = (name: string): string => {
            const sourceID = reached.persisted.find(
               (p) => p.name === name,
            )?.sourceID;
            const address =
               sourceID === undefined
                  ? undefined
                  : planFacts.addressBySourceId[sourceID];
            const entry =
               address === undefined ? undefined : builtEntries[address];
            if (entry?.storageDestinationName !== undefined) {
               return (
                  `'${name}' is materialized in destination ` +
                  `'${entry.storageDestinationName}', not '${destinationName}'`
               );
            }
            if (entry !== undefined) {
               return (
                  `'${name}' is materialized outside destination ` +
                  `'${destinationName}' (a warehouse table, or a reference ` +
                  `that does not say where it lives), which this build cannot read`
               );
            }
            return (
               `'${name}' is not materialized in destination ` +
               `'${destinationName}' for this build`
            );
         };
         throw new ChainedUpstreamMissingError(
            missing,
            `persisted upstream of '${persistSource.name}': ` +
               `${missing.map(where).join("; ")} — the build cannot stack on ` +
               `it, and the recompute that remains would rebuild the stored ` +
               `tables its SQL inlines`,
         );
      }
      // A path to the source warehouse — a table joined beside a stored parent,
      // a SQL source — is one no stored table stands in for, so no build over
      // the parents exists and the recompute is the only build there is. Decided
      // here, from the model, rather than left to the compile: the compiler
      // would say only "undefined object", and the caller needs to know this
      // was a shape the destination cannot express rather than one the build
      // could not carry, because strict treats the two differently.
      if (reached.raw) {
         throw new MaterializationEligibilityError({
            message:
               `'${persistSource.name}' reads the source warehouse ` +
               `(${reached.rawLeaves.join(", ")}) through ` +
               `${reached.rawVia.map((n) => `'${n}'`).join(", ")}, which no ` +
               `stored table can stand in for`,
         });
      }
      if (upstreams.length === 0) {
         throw new ChainedShapeNotCarriedError(
            "no materialized upstream is available in this destination to build on",
         );
      }
      const downstreamDefText = this.liftDownstreamDefText(
         persistSource,
         lift.liftText,
      );
      if (!downstreamDefText) {
         throw new ChainedShapeNotCarriedError(
            "could not recover the downstream source definition text from the model",
         );
      }
      // The sources between the downstream and its parents, each carried only
      // when everything it names is already in the model: a parent, or an
      // intermediate carried before it. An extension of a parent is one of
      // them: by design it inherits `#@ persist` and reads the parent's table,
      // so here it is the parent's rebound virtual source plus the refinements
      // the extension adds.
      // Only the sources on this downstream's path. The lift offers every
      // derivable source in the model; one off the path is another chain's
      // business, and a lift of it that does not compile would fail this
      // build over a source it never reads.
      const onPath = new Set(reached.visited);
      const derived = liftDerivedSources({
         contents: lift.contents,
         sourceNameById: lift.sourceNameById,
         shapeSourceNames: present,
         liftText: lift.liftText,
         carryPersistExtensions: true,
      }).filter((d) => onPath.has(d.sourceName));
      // The `##!` flags of every file whose declarations this model carries.
      const documentFlags = documentFlagsForLifts(
         [{ sourceName: persistSource.name }, ...derived],
         lift,
      );
      const givens = chainedBuildGivens(persistSource);
      const buildOver = (
         parents: ServeBinding[],
      ): Promise<StorageBuildResult> =>
         buildDownstreamIntoStorage({
            destinationName,
            destinationConnection,
            transientModel: buildChainedStorageBuildModel({
               upstreams: parents,
               downstreamName: persistSource.name,
               downstreamDefText,
               destinationName,
               derived,
               documentFlags,
               givens,
            }),
            downstreamName: persistSource.name,
            virtualMap: buildVirtualMap(parents),
            physicalTableName,
            partitionColumns: partitionColumnsForBuild(persistSource),
            environmentPath: environment.getEnvironmentPath(),
         });
      // The parents' refinements, in the serve shape's tiers
      // (`REFINEMENT_TIER_KINDS`): everything the source declares first, then
      // without views, then without joins, then only the kinds that change
      // rows (the extend-block `where:`). The optional kinds are emitted
      // optimistically — a dimension or view on the parent that reads a join
      // to a source this destination cannot bind names an alias the model
      // does not declare — and a tier is retried only on a compile-time
      // eligibility error, before anything is written. A downstream that
      // reads a thinned kind fails on every lower tier too, and strict refuses
      // it as not carried. The filter is never thinned, so no tier reads a
      // parent unfiltered.
      try {
         return await buildOverTiers(
            upstreams,
            REFINEMENT_TIER_KINDS,
            buildOver,
            (err) => err instanceof MaterializationEligibilityError,
         );
      } catch (err) {
         // The model reached only stored parents (checked above), so a shape
         // failure here is one the build could not carry, not one the
         // destination cannot express. Infrastructure failures pass through.
         if (err instanceof MaterializationEligibilityError) {
            throw new ChainedShapeNotCarriedError(
               `'${persistSource.name}' reaches only stored upstreams ` +
                  `(${stops.map((p) => `'${p.name}'`).join(", ")}), but ` +
                  `the build could not express it over them — a refinement ` +
                  `declared on a stored upstream that the build does not ` +
                  `re-declare, or a construct the destination's dialect lacks: ` +
                  `${err.message}`,
            );
         }
         throw err;
      }
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
      liftText: (location: SourceLocation) => string | undefined,
   ): string | undefined {
      const location = (
         persistSource._explore as unknown as { location?: SourceLocation }
      ).location;
      return location ? liftText(location) : undefined;
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
            const dest = e.storageDestinationName ?? e.connectionName;
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
         const destForKey = entry.storageDestinationName ?? connectionName;
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

         // A storage= table lives in `storageDestinationName` (a DuckDB/DuckLake
         // destination), not in `connectionName` (the source warehouse), and
         // dropping it needs a build-scoped READ-WRITE attach — the serve attach
         // is read-only — so it is dropped on its own RW session rather than on
         // the (wrong-engine, read-only) source connection. Best-effort: a
         // failure is logged and the sweep continues, so one unreachable
         // destination never blocks reclaiming the rest.
         if (entry.storageDestinationName) {
            const destinationConnection = this.destinationForCleanup(
               environment,
               entry.storageDestinationName,
            );
            if (!destinationConnection) continue;
            try {
               await dropStorageTable({
                  destinationName: entry.storageDestinationName,
                  destinationConnection,
                  physicalTableName,
                  environmentPath: environment.getEnvironmentPath(),
               });
               recordDropTables("success", "storage");
               logger.info("Dropped materialized storage table on delete", {
                  materializationId: m.id,
                  physicalTableName,
                  storageDestinationName: entry.storageDestinationName,
               });
            } catch (err) {
               recordDropTables("failure", "storage");
               logger.warn("Failed to drop a storage-materialized table", {
                  materializationId: m.id,
                  physicalTableName,
                  storageDestinationName: entry.storageDestinationName,
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
            // Dropping a materialized table is warehouse work someone will have
            // to account for later, so it is tagged like the build that created
            // it — `ops` rather than `materialize`, because this is the
            // lifecycle operation, not a build.
            const dropOptions = this.dropRunSQLOptions(
               environment,
               connectionName,
               environmentName,
               packageName,
               m.id,
            );
            await connection.runSQL(
               `DROP TABLE IF EXISTS ${quoteTablePath(
                  physicalTableName,
                  dialect,
               )}`,
               dropOptions,
            );
            // A crash between staging-create and rename can leave the staging
            // table behind; clean it up too while we hold the connection.
            await connection.runSQL(
               `DROP TABLE IF EXISTS ${quoteTablePath(
                  `${physicalTableName}${stagingSuffix(entry.sourceEntityId)}`,
                  dialect,
               )}`,
               dropOptions,
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
      failures: Record<string, SourceFailure>,
      metadata: Record<string, unknown>,
   ): Promise<void> {
      await this.transition(id, "MANIFEST_ROWS_READY");
      const manifest: BuildManifestResult = {
         builtAt: new Date().toISOString(),
         entries,
         // Omitted entirely on a clean run rather than written as {}: a manifest
         // that records no failures and one that records an empty set are the
         // same fact, and the absent key is the one every earlier manifest has.
         ...(Object.keys(failures).length > 0 ? { failures } : {}),
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
      outcome: "success" | "partial" | "failed" | "cancelled",
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
            // Per-source build failures arrive already redacted (see the "Source
            // failed to materialize" site). A whole-run throw carries no
            // connection to redact against, but it can still echo a DSN -- a
            // connect failure while resolving the package's connections does --
            // and redactPgSecrets needs none, so it is the floor for both the log
            // and the persisted record.
            const message = redactPgSecrets(errMessage(err));
            const cancelled = abortController.signal.aborted;
            const next = cancelled ? "CANCELLED" : "FAILED";
            // The materialization record carries the message to whoever polls
            // it, but a background run's throw answers no request -- so without
            // a log here the failure is invisible in the server's own output,
            // and the stack, the only thing that locates the throw, is dropped.
            if (cancelled) {
               // The error too: an abort that races a genuine failure records
               // "Cancelled", and this is then the only place the real one is
               // reported.
               logger.info("Materialization run cancelled", {
                  materializationId: id,
                  error: message,
               });
            } else {
               logger.error("Materialization run failed", {
                  materializationId: id,
                  error: message,
                  stack: err instanceof Error ? err.stack : undefined,
               });
            }
            try {
               await this.repository.updateMaterialization(id, {
                  status: next,
                  completedAt: new Date(),
                  error: cancelled ? "Cancelled" : message,
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
