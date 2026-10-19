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
   PackageVersionError,
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
   isVersionOwnedRun,
   newestServingEntries,
   ownedVersionOf,
   ServingReader,
   SUPERSEDED_TABLES_KEY,
   tableIdentity,
   tableNamesIn,
   WRITES_TABLES_KEY,
   WRITES_TARGETS_KEY,
   type WriteTarget,
   writeTargetsIn,
   versionedTableName,
   versionTableSuffix,
} from "./versions/materialization_scope";
import { requestedVersionId } from "./versions/version_service";
import { Mutex } from "async-mutex";
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
   type EligibilityTier,
   type InstructedRun,
   type ResolvedInstruction,
} from "./materialization_instructed_run";

/** A physical table's key for drop bookkeeping: where it lives, and its name. */
function tableKey(destination: string, physicalTableName: string): string {
   return `${destination}:${physicalTableName}`;
}

/**
 * The tables an archived version's run owes back: its manifest's entries, and
 * every table it recorded writing ({@link WRITES_TARGETS_KEY}) that the
 * manifest does not name. For a run that failed or was cancelled before it
 * committed, the manifest names none, so the recorded tables are all of them;
 * without them the reclaim would delete the run's record and leave its tables
 * with nothing that names them.
 */
function reclaimableEntries(m: Materialization): ManifestEntry[] {
   const entries = Object.values(m.manifest?.entries ?? {});
   const named = new Set(
      entries.map((e) =>
         tableKeyOf(
            e.connectionName ?? "",
            e.storageDestinationName,
            e.physicalTableName,
         ),
      ),
   );
   for (const target of writeTargetsIn(m.metadata ?? null)) {
      const key = tableKeyOf(
         target.connectionName,
         target.storageDestinationName,
         target.physicalTableName,
      );
      if (named.has(key)) continue;
      named.add(key);
      entries.push({
         sourceEntityId: target.sourceEntityId,
         physicalTableName: target.physicalTableName,
         connectionName: target.connectionName,
         ...(target.storageDestinationName
            ? { storageDestinationName: target.storageDestinationName }
            : {}),
      });
   }
   return entries;
}

/**
 * Where each instructed table lives, as {@link WRITES_TARGETS_KEY} records it:
 * the instruction's table and destination, with the connection of the plan
 * source it builds. Matched by sourceID when the instruction carries one, else
 * by content address, the order the build matches instructions in. An
 * instruction the plan does not know is left out.
 */
function writeTargetsFor(
   plan: BuildPlan | null,
   instructions: BuildInstruction[],
): WriteTarget[] {
   if (!plan) return [];
   const byAddress = new Map(
      Object.values(plan.sources).map((s) => [s.sourceEntityId, s]),
   );
   const targets: WriteTarget[] = [];
   for (const instruction of instructions) {
      const source =
         (instruction.sourceID
            ? plan.sources[instruction.sourceID]
            : undefined) ?? byAddress.get(instruction.sourceEntityId);
      if (!source?.connectionName) continue;
      targets.push({
         sourceEntityId: instruction.sourceEntityId,
         physicalTableName: instruction.physicalTableName,
         connectionName: source.connectionName,
         ...(instruction.destination
            ? { storageDestinationName: instruction.destination }
            : {}),
      });
   }
   return targets;
}

/**
 * Orchestrates single-call materialization builds.
 *
 * On create the publisher either auto-runs (self-assigns physical names from
 * `#@ persist name=`, builds and auto-loads every persist source) or, given
 * `buildInstructions` derived from `Package.buildPlan`, builds into the
 * caller-assigned names without auto-loading the manifest. Both build in the
 * background and return the PENDING record immediately.
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
      // Terminal transitions log at info; the MANIFEST_ROWS_READY hop at debug.
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

   /**
    * A package's runs, newest first. For a versioned package, the runs of one
    * version (the one named, or `latest`) and the runs from before its first
    * versioned publish, which still drive what a `scope: package` version
    * serves. An unversioned package lists every run, as before.
    */
   async listMaterializations(
      environmentName: string,
      packageName: string,
      options?: { limit?: number; offset?: number; versionId?: unknown },
   ): Promise<Materialization[]> {
      const environmentId = await this.resolveEnvironmentId(environmentName);
      // Naming no version of a package that has no `latest` yet (explicit
      // promotion, before the first move) lists every run, as before.
      const versions = (
         await this.environmentStore.getEnvironment(environmentName, false)
      ).getVersionService();
      const version =
         requestedVersionId(options?.versionId) === undefined &&
         versions !== null &&
         (await versions.latestOf(packageName)) === null
            ? null
            : await this.resolveRunVersion(
                 environmentName,
                 packageName,
                 options?.versionId,
              );
      return this.repository.listMaterializations(environmentId, packageName, {
         limit: options?.limit,
         offset: options?.offset,
         ...(version !== null ? { version, includeUnversioned: true } : {}),
      });
   }

   /**
    * The published version a materialization request is about: the one it
    * names, or `latest`; null for an unversioned package. Resolved the way a
    * read is: a malformed id is 400, an unknown version 404 and an archived
    * one 410, and naming a version of a package that has none is 404.
    */
   private async resolveRunVersion(
      environmentName: string,
      packageName: string,
      versionId: unknown,
   ): Promise<string | null> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const versions = environment.getVersionService();
      if (!versions) {
         const named = requestedVersionId(versionId);
         if (named !== undefined) {
            throw new PackageVersionError(
               "VERSION_NOT_FOUND",
               `Package ${packageName} has no version ${named}.`,
            );
         }
         return null;
      }
      return (
         (await versions.resolve(packageName, versionId))?.versionId ?? null
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
      // One published version's fires; undefined, the package's.
      versionId?: string,
   ): Promise<Date | null> {
      const environmentId = await this.resolveEnvironmentId(environmentName);
      return this.repository.getLatestScheduledFireAt(
         environmentId,
         packageName,
         versionId,
      );
   }

   /**
    * One run of the package. A run's id is unique, so with no `versionId` it
    * is found whichever version built it; naming a version also requires the
    * run to be that version's, or one from before any version existed.
    */
   async getMaterialization(
      environmentName: string,
      packageName: string,
      id: string,
      versionId?: unknown,
   ): Promise<Materialization> {
      const environmentId = await this.resolveEnvironmentId(environmentName);
      // The version first, so an unknown or archived one says so (404/410,
      // with its reason) whatever the id.
      const named =
         requestedVersionId(versionId) !== undefined
            ? await this.resolveRunVersion(
                 environmentName,
                 packageName,
                 versionId,
              )
            : null;
      const m = await this.repository.getMaterializationById(id);
      if (
         !m ||
         m.environmentId !== environmentId ||
         m.packageName !== packageName ||
         (named !== null && m.version !== null && m.version !== named)
      ) {
         throw new MaterializationNotFoundError(
            named !== null
               ? `Materialization ${id} not found for version ${named} of package ${packageName}`
               : `Materialization ${id} not found for package ${packageName}`,
         );
      }
      return m;
   }

   // ==================== CREATE + BUILD ====================

   /**
    * Create a materialization and build it in the background, returning the
    * PENDING record immediately. `buildInstructions` (orchestrated mode) are
    * validated synchronously against the compiled build plan, so a bad
    * instruction is rejected at create time rather than failing the background
    * run.
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
          * The caller-supplied incremental ledger (each entry a
          * `ManifestEntry.ledger` an earlier run reported). When present, even
          * empty, it replaces the local store for the whole run.
          * Orchestrated-only; validated at create time.
          */
         ledger?: LedgerEntry[];
         /**
          * What initiated this run: `ON_DEMAND` (default, manual/API) or
          * `SCHEDULER` (the standalone scheduler). Recorded on the run metadata.
          */
         trigger?: "ON_DEMAND" | "SCHEDULER";
         /**
          * Caller context attached as query metadata to the build's statements.
          * Its `trigger` can express a publish, which `trigger` cannot; its
          * `runId` replaces the materialization id as the grouping id.
          */
         runContext?: components["schemas"]["RunContext"] | null;
         /**
          * The published version to build; omitted or empty, `latest`.
          * Resolved once, here, and carried to the background run, so a
          * `latest` that moves meanwhile never changes what this run builds.
          */
         versionId?: unknown;
      } = {},
   ): Promise<Materialization> {
      const environmentId = await this.resolveEnvironmentId(environmentName);

      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const version = await this.resolveRunVersion(
         environmentName,
         packageName,
         options.versionId,
      );
      const pkg = await environment.getPackage(
         packageName,
         false,
         version !== null ? { versionId: version } : {},
      );
      // Whose tables a version's run builds (see materialization_scope).
      const scope =
         version !== null
            ? (pkg.getPackageMetadata().scope ?? "package")
            : null;

      const buildInstructions = options.buildInstructions;
      const orchestrated = buildInstructions !== undefined;
      const versions = environment.getVersionService();
      const latest =
         version !== null && versions && !orchestrated && scope === "package"
            ? await versions.latestOf(packageName)
            : null;
      if (latest !== null && version !== latest) {
         // A `scope: package` table is the package's, shared by its versions
         // under one self-assigned name, and `latest` is what serves it. An
         // auto-run of another version would rebuild it from that version's
         // definition under `latest`'s feet. With no `latest` yet (explicit
         // promotion, before the first `PUT …/latest`) nothing serves the
         // table, so a version is built before it is promoted.
         throw new PackageVersionError(
            "VERSION_NOT_LATEST",
            `Version ${version} of package ${packageName} is not its latest, and the package's materialization scope is "package": its versions share the package's tables, so an auto-run of this version would rebuild what latest serves. Build latest, give each version tables of its own with "materialization": { "scope": "version" } in publisher.json, or pass buildInstructions to build into tables you name.`,
         );
      }
      if (orchestrated) {
         this.validateInstructions(
            pkg.getBuildPlan(),
            buildInstructions,
            options.ledger,
            options.reseed ?? false,
         );
         if (version !== null) {
            // One table, two writers in one run: it would hold the last
            // one's rows under both definitions.
            const seen = new Set<string>();
            for (const i of buildInstructions) {
               const table = tableIdentity(i.physicalTableName);
               if (seen.has(table)) {
                  throw new BadRequestError(
                     `buildInstructions name table ${i.physicalTableName} more than once; give each source a table of its own.`,
                  );
               }
               seen.add(table);
            }
         }
      }

      // The slot the run holds while it is active. An auto-run of a `scope:
      // package` package writes the tables every version shares, so one runs
      // per package (unversioned packages included). A run that writes only
      // its own version's tables holds that version's slot, so versions build
      // side by side: under `scope: version` its tables carry its version, and
      // with buildInstructions the caller names them, as a host building one
      // version does.
      const lockVersion =
         version !== null && (orchestrated || scope === "version")
            ? version
            : undefined;

      const forceRefresh = options.forceRefresh ?? false;
      const reseed = options.reseed ?? false;
      const trigger = options.trigger ?? "ON_DEMAND";
      const metadata = {
         forceRefresh,
         // Recorded separately: it says whether an incremental source rebuilt
         // or advanced.
         reseed,
         sourceNames: options.sourceNames ?? null,
         mode: orchestrated ? "orchestrated" : "auto",
         trigger,
         // The wire Materialization has no version field, so the version a
         // run built rides its metadata, beside the scope that decided whose
         // tables it builds.
         ...(version !== null ? { versionId: version, scope } : {}),
         // A version's run with instructions writes these, while it is
         // active: no other active run of the package may (see
         // claimTables).
         ...(version !== null && orchestrated
            ? {
                 [WRITES_TABLES_KEY]: [
                    ...new Set(
                       buildInstructions.map((i) => i.physicalTableName),
                    ),
                 ],
              }
            : {}),
      };

      const start = async (): Promise<Materialization> => {
         const active = await this.repository.getActiveMaterialization(
            environmentId,
            packageName,
            lockVersion,
         );
         if (active) {
            throw this.activeConflict(packageName, active.id, active.version);
         }
         if (version !== null && orchestrated) {
            await this.assertTablesFree(
               environmentId,
               packageName,
               tableNamesIn(metadata, WRITES_TABLES_KEY),
               undefined,
               true,
            );
         }

         let created: Materialization;
         try {
            created = await this.repository.createMaterialization(
               environmentId,
               packageName,
               "PENDING",
               metadata,
               // A version's run records its version and slot; a package with
               // no versions passes neither, as before versions.
               ...(version !== null || lockVersion !== undefined
                  ? [{ version, lockVersion }]
                  : []),
            );
         } catch (err) {
            if (err instanceof DuplicateActiveMaterializationError) {
               const winner = await this.repository.getActiveMaterialization(
                  environmentId,
                  packageName,
                  lockVersion,
               );
               throw this.activeConflict(
                  packageName,
                  winner?.id,
                  winner?.version,
               );
            }
            throw err;
         }

         // Registered before the version's lock is released (see below), and
         // released once the run has settled.
         const release =
            version !== null
               ? environment.registerVersionBuild(packageName, version)
               : undefined;
         this.runInBackground(
            created.id,
            (signal) =>
               this.runBuild(
                  created.id,
                  environmentName,
                  packageName,
                  {
                     sourceNames: options.sourceNames,
                     forceRefresh,
                     reseed,
                     buildInstructions,
                     // Orchestrated-only: an auto-run depends on the local
                     // store, which a supplied ledger would replace.
                     ledger: orchestrated ? options.ledger : undefined,
                     referenceManifest: options.referenceManifest,
                     strictUpstreams: options.strictUpstreams,
                     trigger,
                     runContext: options.runContext ?? undefined,
                     version,
                  },
                  signal,
               ),
            release,
         );
         return created;
      };

      if (version === null || !versions) return start();
      // A version's run starts under that version's lock, which an archive
      // also takes: either the archive commits first and this answers 410, or
      // the run is registered first and the archive answers 409
      // VERSION_BUILDING. Archiving reclaims a version's tables, so it must
      // never overlap a run writing them. Then under the package's table
      // lock, so the tables a run with instructions claims are checked and
      // recorded as one step (see claimTables).
      return versions.withVersionLock(packageName, version, async () => {
         await versions.resolve(packageName, version);
         return this.tableLockFor(environmentId, packageName).runExclusive(
            start,
         );
      });
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
         /** The version createMaterialization resolved; null when unversioned. */
         version: string | null;
      },
      signal: AbortSignal,
   ): Promise<void> {
      const orchestrated = opts.buildInstructions !== undefined;
      // Set once this run has recorded the shared tables it rebuilds: from
      // then on, a failure must rebind the other loaded versions too.
      let markedShared = false;
      const mode: MaterializationMode = orchestrated ? "orchestrated" : "auto";
      logger.info("Materialization build started", {
         materializationId: id,
         packageName,
         mode,
      });
      const startedAt = Date.now();

      try {
         // Persisted so the UI can compute a duration.
         await this.repository.updateMaterialization(id, {
            startedAt: new Date(startedAt),
         });

         const environmentId = await this.resolveEnvironmentId(environmentName);
         const environment = await this.environmentStore.getEnvironment(
            environmentName,
            false,
         );
         const versionId = opts.version ?? undefined;
         const pkg = await environment.getPackage(
            packageName,
            false,
            versionId !== undefined ? { versionId } : {},
         );
         // The version whose own runs this run reuses from and builds into
         // (`scope: version`); undefined, the package's shared ones.
         const owned =
            versionId !== undefined ? ownedVersionOf(pkg) : undefined;
         // Whose runs this run reuses from: its own under `scope: version`,
         // the package's shared ones otherwise (see ServingReader).
         const reader: ServingReader =
            versionId !== undefined ? { versionId, owned } : {};

         // A published version's tree never changes, so compiling it needs
         // no lock; an unversioned package's can be replaced under it.
         const compiled =
            versionId !== undefined
               ? await compilePackageBuildPlan(pkg, signal)
               : await environment.withPackageLock(packageName, () =>
                    compilePackageBuildPlan(pkg, signal),
                 );

         // Refuse a `#@ persist` source that Malloy's getBuildPlan() silently
         // dropped (see detectDroppedPersistSources); otherwise the build
         // succeeds with an empty manifest and the source serves live. Scoped to
         // this build's targets so a dropped sibling does not block it.
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
            // `reseed`, never `forceRefresh`: every scheduled fire sets
            // forceRefresh, which would make a schedule all full rebuilds.
            forceRefresh: opts.reseed ?? false,
            now: new Date(startedAt),
            ledger: opts.ledger,
         });

         let instructions: BuildInstruction[];
         let carried: Record<string, ManifestEntry>;
         let derivedRefused: Record<string, RefusedSource> = {};
         let clearedTiers: ReadonlyMap<string, EligibilityTier> | undefined;
         if (orchestrated) {
            instructions = opts.buildInstructions!;
            // Seed with the caller's upstream references so a persist upstream
            // built in a prior run resolves to its table instead of recomputing
            // live.
            carried = this.referenceManifestToEntries(opts.referenceManifest);
            // A reference is an IDENTITY, not a full copy. Upstream entries are
            // filled in DESCENDING precedence from:
            //   1. the `referenceManifest` in this call;
            //   2. the package's BOUND manifest (`manifestLocation`), which lets a
            //      worker that never built the upstream reuse its table;
            //   3. this worker's most recent local manifest.
            // Each fills the storage fields (sourceName, storageDestinationName,
            // schema) a `storage=` upstream's rebind needs. Each step is
            // best-effort: a fetch/read failure leaves the entries as they are.
            await this.seedFromBoundManifest(carried, pkg, instructions);
            if (Object.keys(carried).length > 0) {
               await this.resolveReferencesFromStore(
                  carried,
                  environmentId,
                  packageName,
                  id,
                  reader,
               );
            }
         } else {
            // Skip-if-unchanged: reuse tables from the most recent manifest for
            // sources whose sourceEntityId is unchanged.
            const priorEntries = opts.forceRefresh
               ? {}
               : await this.getMostRecentManifestEntries(
                    environmentId,
                    packageName,
                    id,
                    reader,
                 );
            ({
               instructions,
               carried,
               refused: derivedRefused,
               cleared: clearedTiers,
            } = this.deriveSelfInstructions(
               compiled,
               opts.sourceNames,
               priorEntries,
               incremental,
               owned !== undefined
                  ? versionTableSuffix({
                       environmentName,
                       packageName,
                       version: owned,
                    })
                  : "",
            ));
         }

         // An auto-run of a version of a `scope: package` package rebuilds,
         // source by source, the tables every version of the package reads.
         // Before it does, it claims them, and marks every older run's
         // entries for them superseded: whatever happens to this run, those
         // tables will no longer hold what the older entries say. Then every
         // other loaded version is rebound, so none reads rows this run's
         // definition builds (a version that cannot be rebound is unloaded).
         // A version's run with instructions, likewise: its tables were
         // claimed when it was created, and another version's run may have
         // built one of them from another definition.
         if (
            versionId !== undefined &&
            instructions.length > 0 &&
            (orchestrated || owned === undefined)
         ) {
            if (orchestrated) {
               await this.tableLockFor(environmentId, packageName).runExclusive(
                  () =>
                     this.markSuperseded(
                        environmentId,
                        packageName,
                        id,
                        instructions,
                     ),
               );
            } else {
               await this.claimTables(
                  environmentId,
                  packageName,
                  id,
                  instructions,
               );
            }
            markedShared = true;
            await this.rebindLoadedVersions(
               environmentName,
               packageName,
               versionId,
            );
         } else if (versionId !== undefined && instructions.length > 0) {
            // An auto-run of a `scope: version` version builds tables of its
            // own, named for the version. It claims them all the same: the
            // claim, not the naming, keeps another active run off them, since
            // a run with instructions may name any table. No other run reads
            // them, so nothing is superseded or rebound.
            // It also records where each table lives, which its manifest says
            // only once it commits: the version's archive reclaims the tables
            // of a run that failed or was cancelled part-way from this.
            await this.claimTables(
               environmentId,
               packageName,
               id,
               instructions,
               {
                  supersede: false,
                  targets: writeTargetsFor(pkg.getBuildPlan(), instructions),
               },
            );
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
            // Orchestrated-only; see reclaimStorageTablesFromFailedRun.
            orchestrated ? { environmentId, packageName } : undefined,
            {
               // Optional: a lighter package surface resolves without a
               // package-level layer.
               packageMaterialization: pkg.getMaterializationConfig?.() ?? null,
               context: {
                  queryClass: "materialize",
                  environment: environmentName,
                  package: packageName,
                  // The caller's trigger wins: it can express a publish.
                  trigger:
                     opts.runContext?.trigger ?? opts.trigger?.toLowerCase(),
                  // Always set, so a build's statements group in query history.
                  runId: opts.runContext?.runId ?? id,
               },
            },
            incremental,
            Object.values(derivedRefused),
            clearedTiers,
         );

         const { sourcesBuilt, sourcesFailed, sourcesReused } = tallySources(
            entries,
            failures,
            carried,
         );
         // Auto-run refusals ride the run's free-form metadata, not the manifest
         // a strict-schema caller parses. Orchestrated refusals are in
         // `failures`, where that caller already looks.
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
            // commitManifest replaces the metadata, so the version is set again.
            ...(versionId !== undefined
               ? {
                    versionId,
                    scope: pkg.getPackageMetadata().scope ?? "package",
                 }
               : {}),
         });

         // Auto-run owns distribution; orchestrated leaves it to the caller
         // (manifestLocation).
         if (!orchestrated) {
            await this.autoLoadManifest(
               environment,
               packageName,
               entries,
               versionId,
            );
         }
         if (markedShared && versionId !== undefined) {
            // Every other loaded version is rebound to this run too: each
            // binds only what its own definition built, and serves live what
            // this run rebuilt from a different one (see
            // Package.bindStorageServeBindings).
            await this.rebindLoadedVersions(
               environmentName,
               packageName,
               versionId,
            );
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
         // A run that lost sources is partial: its manifest is missing tables a
         // consumer expected. An auto-run refusal is not a loss: it is a property
         // of the model, and counting it would make every run partial.
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
         if (markedShared && opts.version !== null) {
            // The run may have rebuilt some shared tables before it failed.
            // Its record stays, so rebinding now leaves those tables unbound
            // for every loaded version, as a restart would.
            await this.rebindLoadedVersions(
               environmentName,
               packageName,
               opts.version,
            );
         }
         throw err;
      }
   }

   /**
    * The lock under which a published version's run checks and records the
    * tables it writes, per package. Runs of different versions hold
    * different active slots, so this is what keeps two of them from writing
    * one table at once. In-process: one publisher owns its store.
    */
   private tableLockFor(environmentId: string, packageName: string): Mutex {
      const key = `${environmentId}|${packageName}`;
      let lock = this.tableLocks.get(key);
      if (!lock) {
         lock = new Mutex();
         this.tableLocks.set(key, lock);
      }
      return lock;
   }

   private readonly tableLocks = new Map<string, Mutex>();

   /**
    * Refuse tables another active run of the package writes, and, for a run
    * with instructions, the tables the package's versions share (the
    * newest auto-run's): those are rebuilt only by an auto-run of `latest`,
    * which marks what it makes stale. Called under tableLockFor.
    */
   private async assertTablesFree(
      environmentId: string,
      packageName: string,
      tables: string[],
      excludeId: string | undefined,
      instructed: boolean,
   ): Promise<void> {
      // Compared by what a name names (see tableIdentity), not its spelling.
      const wanted = new Set(tables.map(tableIdentity));
      if (wanted.size === 0) return;
      const runs = await this.repository.listMaterializations(
         environmentId,
         packageName,
      );
      for (const m of runs) {
         if (m.id === excludeId) continue;
         if (m.status !== "PENDING" && m.status !== "MANIFEST_ROWS_READY") {
            continue;
         }
         const clash = tableNamesIn(m.metadata, WRITES_TABLES_KEY).find((t) =>
            wanted.has(tableIdentity(t)),
         );
         if (clash !== undefined) {
            throw new MaterializationConflictError(
               `Table ${clash} is being written by materialization ${m.id}${m.version ? ` of version ${m.version}` : ""} of package ${packageName}; two runs never write one table at once.`,
            );
         }
      }
      if (!instructed) return;
      // Every committed run the package's versions share: neither one
      // version's own nor one version's instructed run. Runs from before the
      // package's first versioned publish count, instructed ones included:
      // every version may serve them.
      const shared = runs
         .filter(
            (m) =>
               !isVersionOwnedRun(m) &&
               (m.metadata?.mode !== "orchestrated" || m.version === null) &&
               m.status === "MANIFEST_FILE_READY",
         )
         .flatMap((m) =>
            Object.values(m.manifest?.entries ?? {}).map(
               (e) => e.physicalTableName,
            ),
         );
      const clash = shared.find((t) => wanted.has(tableIdentity(t)));
      if (clash !== undefined) {
         throw new MaterializationConflictError(
            `Table ${clash} is one the versions of package ${packageName} share, which only an auto-run of its latest version rebuilds; name tables of your own in buildInstructions.`,
         );
      }
   }

   /**
    * Before an auto-run of a version builds the tables in `instructions`:
    * refuse them if another active run writes one, and record them as this
    * run's (WRITES_TABLES_KEY), under the package's table lock. For the
    * shared tables of a `scope: package` version (`supersede`), also mark
    * every other run's entries for them superseded (SUPERSEDED_TABLES_KEY),
    * in the same lock hold.
    */
   private async claimTables(
      environmentId: string,
      packageName: string,
      id: string,
      instructions: BuildInstruction[],
      {
         supersede,
         targets,
      }: { supersede: boolean; targets?: WriteTarget[] } = { supersede: true },
   ): Promise<void> {
      const tables = [...new Set(instructions.map((i) => i.physicalTableName))];
      await this.tableLockFor(environmentId, packageName).runExclusive(
         async () => {
            await this.assertTablesFree(
               environmentId,
               packageName,
               tables,
               id,
               false,
            );
            const run = await this.repository.getMaterializationById(id);
            await this.repository.updateMaterialization(id, {
               metadata: {
                  ...(run?.metadata ?? {}),
                  [WRITES_TABLES_KEY]: tables,
                  ...(targets ? { [WRITES_TARGETS_KEY]: targets } : {}),
               },
            });
            if (!supersede) return;
            await this.markSuperseded(
               environmentId,
               packageName,
               id,
               instructions,
            );
         },
      );
   }

   /**
    * Mark, on every other run of the package, the entries a run about to
    * write `instructions` makes stale: an entry naming one of those tables
    * built from another definition (another content address) no longer says
    * what the table holds, whatever happens to this run. An entry from the
    * same definition still does, and is left alone; so is a run already
    * marked. Called under tableLockFor.
    */
   private async markSuperseded(
      environmentId: string,
      packageName: string,
      id: string,
      instructions: BuildInstruction[],
   ): Promise<void> {
      // Keyed by what a name names (see tableIdentity), not its spelling.
      const rebuilt = new Map<string, Set<string>>();
      for (const i of instructions) {
         const table = tableIdentity(i.physicalTableName);
         const addresses = rebuilt.get(table) ?? new Set<string>();
         addresses.add(i.sourceEntityId);
         rebuilt.set(table, addresses);
      }
      for (const m of await this.repository.listMaterializations(
         environmentId,
         packageName,
      )) {
         if (m.id === id) continue;
         const marked = tableNamesIn(m.metadata, SUPERSEDED_TABLES_KEY);
         const already = new Set(marked.map(tableIdentity));
         const stale = Object.values(m.manifest?.entries ?? {})
            .filter((e) => {
               const table = tableIdentity(e.physicalTableName);
               const addresses = rebuilt.get(table);
               return (
                  addresses !== undefined &&
                  !addresses.has(e.sourceEntityId) &&
                  !already.has(table)
               );
            })
            .map((e) => e.physicalTableName);
         if (stale.length === 0) continue;
         try {
            await this.repository.updateMaterialization(m.id, {
               metadata: {
                  ...(m.metadata ?? {}),
                  [SUPERSEDED_TABLES_KEY]: [...new Set([...marked, ...stale])],
               },
            });
         } catch (err) {
            // Deleted meanwhile: nothing of it is read any more.
            logger.debug("Could not mark a run's entries superseded", {
               materializationId: m.id,
               error: errMessage(err),
            });
         }
      }
   }

   /**
    * Derive auto-run's own build instructions. Each persist source (respecting
    * `sourceNames`) gets a self-assigned physical name and COPY realization,
    * unless its sourceEntityId is unchanged since `priorEntries`, in which case
    * it is carried forward.
    *
    * An INCREMENTAL source is exempt from carry-forward: unchanged SQL says
    * nothing about whether its DATA moved, so carrying it would never reach its
    * delta. The ledger decides instead, and "nothing new" is a cheap skip.
    *
    * A source the eligibility gate refuses is returned in `refused`. It throws
    * instead when `sourceNames` targets it, or when every authored source the
    * run targets was refused. A refused rollup never causes the throw.
    *
    * `cleared` names the tier each source passed the gate for, so the build loop
    * consults that verdict rather than walking the source again.
    */
   private deriveSelfInstructions(
      compiled: CompiledBuildPlan,
      sourceNames: string[] | undefined,
      priorEntries: Record<string, ManifestEntry>,
      incremental?: IncrementalRunContext,
      // Appended to every self-assigned name: a `scope: version` package's
      // version builds into tables of its own (see versionTableSuffix).
      tableSuffix: string = "",
   ): {
      instructions: BuildInstruction[];
      carried: Record<string, ManifestEntry>;
      refused: Record<string, RefusedSource>;
      cleared: Map<string, EligibilityTier>;
   } {
      const include = sourceNames ? new Set(sourceNames) : null;
      const instructions: BuildInstruction[] = [];
      const carried: Record<string, ManifestEntry> = {};
      const refused: Record<string, RefusedSource> = {};
      const refusals: MaterializationEligibilityError[] = [];
      const cleared = new Map<string, EligibilityTier>();
      const seen = new Set<string>();

      for (const graph of compiled.graphs) {
         for (const persistSource of iterGraphSources(
            graph,
            compiled.sources,
         )) {
            if (include && !include.has(persistSource.name)) continue;

            // A source that DECLARES `storage=` must never silently downgrade
            // to a colocated CTAS into its own warehouse when
            // PERSIST_STORAGE_MODE=off: the author did not ask for that, and
            // production grants read-only warehouse access. It serves LIVE and
            // Package.storageWarnings reports it. A plain `#@ persist` is
            // unaffected; colocated is its intent.
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
            // makes its getSQL() throw opaquely.
            //
            // A refused source is SKIPPED and recorded, not thrown for, so one
            // ineligible source does not stop the rest of the package refreshing.
            // The run throws only when nothing else is left to build, or when
            // `sourceNames` asked for exactly this source.
            try {
               if (destination) {
                  assertMaterializationEligible(
                     persistSource,
                     deriveAnnotationFields(persistSource),
                  );
               } else {
                  // Colocated `#@ persist`: just as frozen as a storage build, so
                  // an authorize-gated source materialized here would be served
                  // to every caller. The origin makes a refused rollup name
                  // `#@ preaggregate`, since its synthesized name appears nowhere
                  // in the author's model.
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
                  // A refused rollup never fails the run: the author asked for
                  // nothing they can see.
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
            cleared.set(
               persistSource.sourceID,
               destination ? "storage" : "colocated",
            );

            const sourceEntityId = computeSourceEntityId(
               persistSource,
               compiled.connectionDigests,
            );
            if (seen.has(sourceEntityId)) continue;
            seen.add(sourceEntityId);

            // Self-assign the name from `name=` (or the source name) verbatim
            // for both destinations. No generational decoration is needed: a
            // storage build replaces the table atomically (`CREATE OR REPLACE`,
            // or the partition create/alter/insert trio in ONE transaction, so a
            // rebuild never empties the table it serves). Orchestrated builds
            // use the host-supplied `physicalTableName`. A `scope: version`
            // run's name gains its version's suffix.
            const logicalName = tableSuffix
               ? versionedTableName(
                    selfAssignTableName(persistSource),
                    tableSuffix,
                 )
               : selfAssignTableName(persistSource);

            // The SAME predicate the build's dispatch uses, so the two never
            // disagree about which sources are incremental. A declaration the
            // delta path cannot act on (unsupported dialect, unusable watermark)
            // keeps skip-if-unchanged rather than rebuilding fully every run.
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
            // Carry forward only within the SAME destination: sourceEntityId
            // does not encode it, so a source that changes `storage=` must
            // rebuild rather than reuse a table its serve cannot resolve.
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

      return { instructions, carried, refused, cleared };
   }

   /**
    * Project the caller's upstream reference manifest into the seed entry map
    * `executeInstructedBuild` consumes, keyed by sourceEntityId. The optional
    * `connectionName` lets the seed be dialect-quoted (see
    * {@link quoteSeedTablePath}); without it the seed is unquoted.
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
    * Fill the fields a thin reference cannot carry from this publisher's own
    * most recent manifest (the skip-if-unchanged cache). Caller-supplied fields
    * WIN: the orchestrator is authoritative across a stateless fleet. Mutates
    * `carried`. Best-effort: a lookup failure leaves references as supplied.
    */
   private async resolveReferencesFromStore(
      carried: Record<string, ManifestEntry>,
      environmentId: string,
      packageName: string,
      excludeId: string,
      // Whose runs the references resolve against (see ServingReader).
      reader: ServingReader = {},
   ): Promise<void> {
      let cached: Record<string, ManifestEntry>;
      try {
         cached = await this.getMostRecentManifestEntries(
            environmentId,
            packageName,
            excludeId,
            reader,
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
    * Seed upstream reuse from the package's BOUND manifest (`manifestLocation`),
    * so a worker that never built an upstream can still reuse its table. Adds
    * bound upstreams not already carried and fills gaps in thin references; an
    * explicit `referenceManifest` field always wins. Sources this build produces
    * are skipped. Best-effort: a fetch failure is ignored. Mutates `carried`.
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
      // Storage entries carry their full shape; colocated ones are rebuilt from
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
    * `reader` says whose runs count (see newestServingEntries): a published
    * version's, or the unversioned slot's.
    */
   private async getMostRecentManifestEntries(
      environmentId: string,
      packageName: string,
      excludeId: string,
      reader: ServingReader = {},
   ): Promise<Record<string, ManifestEntry>> {
      const list =
         (await this.repository.listMaterializations(
            environmentId,
            packageName,
            reader.owned !== undefined ? { version: reader.owned } : undefined,
         )) ?? [];
      // Legacy tolerance, removable with `ManifestEntry.error`: manifests
      // from 0.0.245-0.0.246 record failed sources among their entries.
      // Reusing one would never retry the source and would seed a downstream
      // from a table that was never created.
      return Object.fromEntries(
         Object.entries(newestServingEntries(list, reader, excludeId)).filter(
            ([, entry]) => !isLegacyFailedEntry(entry),
         ),
      );
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
            versionId?: string,
         ): Promise<void>;
         bindPackageStorageServeBindings(
            packageName: string,
            entries: Record<string, ManifestEntry>,
            versionId?: string,
         ): Promise<void>;
      },
      packageName: string,
      entries: Record<string, ManifestEntry>,
      // The version the run built; undefined for an unversioned package.
      versionId?: string,
   ): Promise<void> {
      // Bound un-gated: freshness is stamped only on the control plane's wire
      // manifest. A failed source mirrored into `entries` (the
      // `ManifestEntry.error` deprecation window) names the previous
      // generation's real table under auto-run's stable names, so binding it
      // would serve stale rows as fresh; skip it.
      const builtEntries = Object.fromEntries(
         Object.entries(entries).filter(
            ([, entry]) => !isLegacyFailedEntry(entry),
         ),
      );
      const manifestEntries: FreshnessManifest = {};
      for (const [sourceEntityId, entry] of Object.entries(builtEntries)) {
         // Only colocated entries: a storage entry here would substitute a
         // table on the source's OWN connection, where it does not exist.
         // Storage entries serve via the bindings below.
         if (entry.physicalTableName && !entry.storageDestinationName) {
            manifestEntries[sourceEntityId] = {
               tableName: entry.physicalTableName,
               // For dialect quoting at bind (Package.quoteBoundTableNames): the
               // build CREATEd it quoted, so an unquoted read misses on a
               // case-folding engine.
               connectionName: entry.connectionName,
            };
         }
      }
      try {
         await environment.reloadAllModelsForPackage(
            packageName,
            manifestEntries,
            versionId,
         );
         // Storage sources serve cross-connection via the virtual-source
         // transform. No-op without storage= sources.
         await environment.bindPackageStorageServeBindings(
            packageName,
            builtEntries,
            versionId,
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
    * Validate caller-supplied build instructions against the compiled build
    * plan: every instructed sourceEntityId must be planned, and only COPY is
    * supported. Throws when the package declares no persist source.
    *
    * Each caller-supplied ledger entry must name a table this run instructs,
    * name it once, belong to a source declaring incremental refresh, and carry
    * the plan's content address for it. An invalid entry is a 400 here, where
    * the caller can react, rather than a failed background run; the address
    * check fires routinely after a publish or rollback changes a source. An
    * entry for a source being reseeded is not validated: `reseed` is the
    * documented recovery from this 400.
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

      // Instructed tables, keyed like a ledger entry, with the plan's facts
      // about each writing source. Resolved by sourceID, else content address
      // (the build loop's order).
      const instructed = new Map<
         string,
         { sourceEntityId: string; refresh: string | null; reseed: boolean }
      >();
      // The same tables keyed WITHOUT their destination, so a destination-less
      // entry can be recognized as stale.
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
            // An entry without `storageDestinationName` for a table this run
            // builds into storage is STALE, not wrong: the source seeds (the
            // build's index keys on the destination, so it finds no boundary).
            // An entry naming a DIFFERENT destination is refused below.
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
    * resolves it on a case-folding engine. Without a resolvable connection the
    * path binds unquoted; on a case-folding engine the downstream CREATE then
    * fails loudly.
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
         // Only a named-but-absent connection is a real gap; a missing name is
         // the benign older-caller default.
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
      // Used only to reclaim this run's storage tables on failure.
      owner?: { environmentId: string; packageName: string },
      buildMetadata?: BuildQueryMetadata,
      // Undefined leaves every source on the full-rebuild path.
      incremental?: IncrementalRunContext,
      // Auto-run refusals, named in the error of a run whose every instructed
      // source failed so it accounts for every unbuilt source.
      priorRefusals: RefusedSource[] = [],
      // Tiers auto-run already cleared (see deriveSelfInstructions); a listed
      // source is not asserted again.
      clearedTiers?: ReadonlyMap<string, EligibilityTier>,
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
         clearedTiers,
         index,
         // Once per run: it depends only on the plan.
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

         // A run that produced nothing must not report success with errors
         // attached. Thrown inside the try so it takes the same cleanup path as
         // any other total failure.
         if (run.builtSources.length === 0 && run.failedReasons.length > 0) {
            throw new Error(
               [
                  ...run.failedReasons,
                  ...run.refusals.map((e) => e.message),
                  ...priorRefusals.map((r) => r.message),
               ].join("; "),
            );
         }
         // Every instructed source was refused: the refusal is the answer.
         if (run.builtSources.length === 0 && run.refusals.length > 0) {
            throw allSourcesRefusedError(run.refusals);
         }
      } catch (err) {
         // A run that commits no manifest leaves its written tables unreachable
         // to manifest-driven GC, so reclaim them before rethrowing.
         // Best-effort: the build's own failure is what the caller needs.
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
      // Accumulates physical names as sources build so downstreams resolve to
      // them. In strict mode an upstream neither built nor seeded fails the
      // compile (runtime-manifest-strict-miss) instead of recomputing live.
      const manifest = new Manifest();
      manifest.strict = strict;
      const entries: Record<string, ManifestEntry> = {};
      for (const [sourceEntityId, entry] of Object.entries(seedEntries)) {
         if (entry.physicalTableName) {
            // The build Manifest feeds a downstream `FROM` verbatim, so it needs
            // the quoting the table was CREATEd with. `entries` (the committed
            // manifest) keeps the logical, unquoted name.
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

      // The manifest is keyed by the CONTENT sourceEntityId (what Malloy
      // recomputes to resolve upstream references), not the instruction's
      // identity. A sourceID match has not computed it yet.
      const sourceEntityId =
         resolved.sourceEntityId ??
         computeSourceEntityId(persistSource, compiled.connectionDigests);

      // Re-assert (idempotent) so no path into a storage build is ungated.
      if (
         !orchestrated &&
         instruction.destination &&
         getPersistStorageMode() !== "off" &&
         run.clearedTiers?.get(persistSource.sourceID) !== "storage"
      ) {
         assertMaterializationEligible(
            persistSource,
            deriveAnnotationFields(persistSource),
         );
      }

      // One physical table, written once. A base and its extension (`#@
      // persist` is inherited) compile to the same address, and a source used
      // across models appears in several graphs. Only the same-content case is
      // decided here; DIFFERENT definitions on one table were reported (and
      // refused under PERSIST_COLLISION_ENFORCE) by assertNoTableCollisions, so
      // reaching here means the deployment chose to warn and write anyway.
      const target = physicalTargetKey(instruction, graphConnectionName);
      const written = run.writtenTargets.get(target);
      if (written && written.sourceEntityId === sourceEntityId) {
         // Another route to the same table: nothing to build or record.
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
         // An instructed destination that cannot be honored REFUSES, never
         // falls through to a colocated build: that would write the caller's
         // name into the source's own warehouse and answer success.
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
         // A manifest travels without its build plan, so this is the only
         // signal that a table belongs to a rollup in no model file; served as
         // an ordinary binding it would be silently dropped (see
         // preaggregation_serve_bindings).
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
         // Recorded, not logged: only a failed run leaves these behind.
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
      // A refusal fails THIS source, not the run (as in
      // deriveSelfInstructions), and is reported in `failures` because an
      // absent entry reads as "built". A host building from the plan never
      // instructs a refused source, so reaching the catch means the plan and
      // this gate disagreed.
      try {
         // Host-supplied storage instructions are gated too, not trusted.
         // Skipped when the mode is off: the destination refusal owns that.
         if (
            orchestrated &&
            instruction.destination &&
            getPersistStorageMode() !== "off"
         ) {
            assertMaterializationEligible(
               persistSource,
               deriveAnnotationFields(persistSource),
            );
         } else if (!run.clearedTiers?.has(persistSource.sourceID)) {
            // Every other instruction (colocated, or any build while the mode
            // is off): refuse an authorize-gated source however it was
            // instructed, unless its compile-time gate outcome proves the
            // colocated relaxation applies. `else` because the storage gate
            // already runs the same IR walk. A source auto-run cleared passed
            // one of the two gates and is not walked again.
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
         // Keyed by the caller's address: a refused source may have no
         // content address (getSQL throws for a given or free parameter).
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
    * The destination's config for a best-effort table drop, or undefined when it
    * is no longer configured, so a cleanup sweep skips that table instead of
    * aborting and leaving every later table behind. The build path does NOT use
    * this: there an unresolvable destination must fail the run.
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

   /**
    * Drop the storage tables a FAILED run created: the run commits no manifest,
    * and GC reclaims only what a manifest records. Each guard closes a way to
    * destroy live data:
    *
    * - ORCHESTRATED runs only (no `owner` for auto-run). Host-assigned
    *   generational names are unique by construction. The still-referenced check
    *   reads THIS environment only, and a BuildID has no environment input, so
    *   with auto-run's stable names two environments sharing a destination could
    *   resolve to the SAME table and a reclaim could drop one another serves.
    *   Do not widen this until colliding persist targets are refused at
    *   validation time.
    * - Only entries this run CREATED (never a carried-forward one).
    * - Only names no other MANIFEST_FILE_READY run references, the same
    *   destination-and-name check {@link dropMaterializedTables} applies.
    *
    * Colocated failures are left alone: stable names in the customer's own
    * warehouse are overwritten by the next build, and a DROP there is a far
    * larger blast radius for no reclaim.
    */
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
    * The `RunSQLOptions` for one source's build statements. Layers, least
    * specific first: connection default, the model's declaration (via
    * {@link resolveQueryMetadata}), then the run's context naming the source.
    *
    * Fails OPEN: a build must not fail over metadata, so an unresolvable
    * connection contributes no default and a dropped property is logged and
    * metered rather than thrown.
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
      // Absent leaves the full-rebuild path.
      incremental?: IncrementalRunContext,
      // The content address, NOT the caller-assigned
      // `instruction.sourceEntityId`. Recorded on the covered_through ledger and
      // COMPARED on every read, so a boundary is never read against different
      // SQL: a changed source seeds instead of deltaing onto a stale table. It is
      // not part of the ledger key, so a boundary stays findable across versions.
      contentSourceEntityId?: string,
      // The default knows nothing; only test seams rely on it.
      planFacts: ChainedPlanFacts = NO_PLAN_FACTS,
   ): Promise<ManifestEntry> {
      const sourceEntityId = instruction.sourceEntityId;
      const physicalTableName = instruction.physicalTableName;
      const isStorageBuild =
         !!instruction.destination && getPersistStorageMode() !== "off";
      // Warehouse-executed build SQL cannot see a storage upstream's
      // DuckDB/DuckLake table, so storage upstreams are excluded from the build
      // manifest: non-strict they INLINE (recompute from raw); under
      // `strictUpstreams` they are a strict-miss error.
      //
      // A storage build renders this SQL permissively because for it the SQL is
      // only the recompute fallback (stacking on the parent uses a separate
      // DuckDB recompile); rendering strictly would refuse a chained source
      // before parent reuse could run. Strict is enforced below, where a
      // recompute would really happen. A colocated build keeps strict: its
      // buildSQL IS what runs.
      const reducedManifest = manifestExcludingStorage(manifest, builtEntries);
      const buildManifest = isStorageBuild
         ? { ...reducedManifest, strict: false }
         : reducedManifest;
      const buildSQL = persistSource.getSQL({
         buildManifest,
         connectionDigests,
      });

      // Which stored tables this source reads, and whether the substituted
      // manifest holds each, decided once for both tiers. It drives stacking on
      // parents, strict refusal, and the entry's `upstreamReuse`.
      //
      // `_model._modelDef` is compiler-internal. If a compiler bump removes it,
      // the walk finds nothing and the entry carries no `upstreamReuse`: a
      // visible gap, not a wrong answer.
      const lift = authorModelLiftContext(
         persistSource._model?._modelDef,
         readAuthorFile,
      );
      const reached = reachedPersistedSources(
         lift,
         persistSource.name,
         (_name, sourceID) => planFacts.persistSourceIds.has(sourceID),
      );
      // The compiler's reach, rendered twice over the full manifest.
      // Permissively, compared with the build SQL: a difference means a stored
      // table was inlined. Strictly: the one possible miss is a persist source
      // the SQL reads that nothing supplied.
      //
      // These renders only classify. A colocated build logs a failed render and
      // omits `upstreamReuse`; a storage build fails, because stacking on parents
      // needs the answer. Neither runs when nothing was excluded, and the strict
      // render is skipped for a strict colocated build (already strict above).
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
              // The content address: orchestrators may assign their own ids.
              rootAddress: contentSourceEntityId ?? instruction.sourceEntityId,
           })
         : { fields: {}, inManifestOnly: [], missing: [] };

      // Every statement of this build carries the same metadata so query history
      // shows it as one unit of work. Resolved before the storage branch so both
      // paths attribute identically (a `storage=` build applies it itself).
      const runOptions = this.buildRunSQLOptions(
         persistSource,
         environment,
         buildMetadata,
      );

      // `storage=` build into a DuckDB/DuckLake destination via a build-scoped
      // session (never the source or serve connection), capturing the
      // authoritative schema for the serve transform.
      if (isStorageBuild) {
         // An upstream nothing supplied can only be recomputed, which strict
         // forbids. The build SQL was rendered permissively, so the refusal is
         // made here from the strict render (`compilerMissing`).
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
         // Stack on the parent exactly when the build SQL inlined a stored
         // table (the two renders differ). The walk alone must not decide: a
         // declared join the query never reads needs no chained attempt.
         const dependsOnStorageUpstream = sqlInlinesStored;
         // Materialize ONLY the source's PUBLIC columns. `getSQL` projects
         // hidden ones too (`except:`, access modifiers); serving is already
         // bounded by the shape, so this keeps hidden VALUES from sitting at
         // rest in the destination, reachable by direct catalog access. Refuses
         // the build (422) if the public surface can't be determined.
         const publicBuildSQL = projectToPublicColumns(persistSource, buildSQL);
         return this.buildOneSourceIntoStorage({
            persistSource,
            instruction,
            manifest,
            environment,
            publicBuildSQL,
            // Unprojected, for a delta: it projects outermost, around its range
            // predicate, since the projection may not carry the watermark.
            buildSQL,
            builtEntries,
            dependsOnStorageUpstream,
            // Reported when the build does not stack on a parent.
            baseReuse: reuse.fields,
            queryMetadata: runOptions.queryMetadata,
            incremental,
            contentSourceEntityId,
            planFacts,
            lift,
         });
      }

      // Incremental refresh: a source with `refresh="incremental"` and a usable
      // watermark advances by a bounded delta; anything else gets `undefined`
      // and falls through to the CTAS.
      const dialect = persistSource.dialectName;
      const quotedPhysicalPath = quoteTablePath(physicalTableName, dialect);
      // Colocated here, so one dialect is both source and target.
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
      // One narrowing, so the incremental touchpoints below never disagree.
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
               // The CTAS's own SQL, so a delta computes what a rebuild would
               // (see deltaSelect).
               sourceSQL: buildSQL,
            }),
            manifest,
            upstreamReuse: reuse.fields,
         });
         if (applied) return applied;
      }

      const stagingTableName = `${physicalTableName}${stagingSuffix(sourceEntityId)}`;
      // Names arrive logical (unquoted); quote for valid DDL (e.g. a hyphenated
      // BigQuery project id). The manifest echoes the logical name.
      const quotedStaging = quoteTablePath(stagingTableName, dialect);
      const quotedPhysical = quotedPhysicalPath;
      // Not always the bare name: on some dialects a bare rename target moves
      // the table into the session's default container.
      const quotedRenameTarget = quoteRenameTarget(physicalTableName, dialect);

      // Drop the boundary BEFORE the rebuild: a crash mid-rebuild must not leave
      // a boundary for data that no longer exists for the next delta to trust.
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
      // Only the CTAS reads source data, so only its cost is reported.
      // Undefined on backends that do not report one.
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

      // Visible to later downstreams in this run, under the SAME quoted path
      // the CREATE used (pasted into a downstream `FROM` verbatim). The returned
      // entry keeps the logical name.
      manifest.update(sourceEntityId, { tableName: quotedPhysical });

      const durationMs = Math.round(performance.now() - startTime);
      recordSourceBuildDuration(durationMs, "in_warehouse");
      logger.info(`Built materialized source ${persistSource.name}`, {
         physicalTableName,
         durationMs,
      });

      // The table now holds a full snapshot; its boundary turns the NEXT
      // refresh into a delta.
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
         ...ledgerFields(incrementalRefresh?.lineage, seededThrough),
         // Reported for any incremental source that rebuilt, so a full rebuild
         // is distinguishable from a delta.
         ...refreshFields(incrementalRefresh ? "full" : undefined),
         rowCount: null,
         buildDurationMs: durationMs,
         // The recurring warehouse cost of keeping this source materialized.
         queryCostBytes: buildCostBytes ?? null,
         ...reuse.fields,
      };
   }

   /**
    * Materialize a source into a `storage=` destination (DuckDB/DuckLake) via a
    * query-passthrough CTAS on a build-scoped session. The entry records the
    * captured DuckDB schema for the serve transform; `connectionName` names the
    * SOURCE warehouse, `storageDestinationName` where the table lives.
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
      /** Applied by the passthrough itself (see {@link buildSourceIntoStorage}). */
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
       * The `upstreamReuse` fields for a build that does NOT stack on a parent
       * ({@link upstreamReuseFromManifest}).
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
      // From the destination list, never the connection list: an unconfigured
      // destination must fail (422) rather than resolve a same-named connection
      // and write a tenant's own warehouse.
      const destinationConnection =
         environment.getStorageDestination(destinationName);

      // Incremental refresh of a STORED table: the warehouse computes the delta,
      // the destination engine applies it; seed-vs-delta is the shared planner's.
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
      // A CHAINED stored source always rebuilds: an incremental parent's delta
      // can restate rows BELOW this source's frontier, which a half-open range
      // never revisits. Reported under its own reason code.
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
         // Clear the boundary before the rebuild, as the delta path does. A
         // source that becomes chained keeps its name and content address.
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

      // Stack on the parent: read its STORED lake table instead of recomputing
      // from raw, so the downstream is a pure function of the parent's rows. On
      // ineligibility it throws and we fall back to recompute (non-strict) or
      // refuse (`strictUpstreams`).
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
            // Redact: a failed read-write ATTACH or CTAS can echo the SQL,
            // catalog `password=` included, into the run `error` or the log.
            const safeDetail = redactConnectionSecrets(
               errMessage(err),
               sourceConnection,
               destinationConnection,
            );
            // Only a SHAPE failure justifies recomputing from raw. An INFRA
            // failure (attach, CTAS, destination down) would fail the same way
            // against the same destination, so it throws.
            //
            // Under strict, a missing upstream or an uncarried shape would
            // recompute tables the orchestrator pinned, so it is refused. A
            // downstream that reaches the source warehouse has no build over the
            // parents, so strict permits the recompute, and it is reported.
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

      // Recompute from raw: run `buildSQL` (storage upstreams inlined) in the
      // source warehouse and CTAS it into the destination.
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
            // Redact: a federation/passthrough/attach error can echo connection
            // detail (connstrings, service-account JSON). Strip credential VALUES
            // but keep the message legible, since the run `error` is user-visible.
            const safeDetail = redactConnectionSecrets(
               errMessage(err),
               sourceConnection,
               destinationConnection,
            );
            // Whether the warehouse read was already BILLED must survive the
            // rewrap: a scheduled retry re-pays for it every occurrence, unlike a
            // free-to-retry attach failure.
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

      // Build-time servability gate: the serve shape must compile in DuckDB
      // against the captured schema, or the build is refused (422) instead of
      // failing at serve time. Outside the redaction try: its error carries no
      // secrets.
      try {
         await assertStorageServeShapeCompiles({
            destinationName,
            sourceName: persistSource.name,
            virtualHandle: sourceEntityId,
            physicalTableName,
            schema: result.schema,
         });
      } catch (gateErr) {
         // An in-place refresh advanced the LIVE serving table, over a shape
         // a delta cannot change, so it is never dropped. The run still fails,
         // leaving the boundary unrecorded so the next refresh re-applies the
         // same idempotent range.
         if (result.refresh) throw gateErr;
         // No manifest records the just-built table, so GC would never see it:
         // drop it, best-effort. With in-place naming the prior generation is
         // already gone, so the source serves live until a successful build;
         // rollback-safe regeneration needs host-generational names.
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
      // Deltas and rebuilds get separate series; a SKIP did no work and is not
      // timed.
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
            // The whole cost and the warehouse job ids, which the manifest has
            // no room for. Null means nothing was reported, not free (see
            // BuildReadCost).
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
         // A refresh reports the APPLY's duration, not the session setup. A
         // SKIP reports null rather than skewing the series as a fast build.
         buildDurationMs: result.refresh
            ? (result.refresh.durationMs ?? null)
            : durationMs,
         // Incremental coverage and what this run did, as for a colocated
         // table. Absent for a non-incremental source.
         ...ledgerFields(
            lineage,
            result.refresh?.coveredThrough ?? result.seededThrough,
         ),
         ...refreshFields(
            result.refresh?.refresh ?? (lineage ? "full" : undefined),
         ),
         // How stored upstreams were read: a chained attempt answers for
         // itself; otherwise the manifest's answer for the passthrough SQL.
         ...(dependsOnStorageUpstream
            ? {
                 upstreamReuse: result.upstreamReuse ?? "recomputed",
                 ...(upstreamRecomputeReason
                    ? { upstreamRecomputeReason }
                    : {}),
              }
            : baseReuse),
         // SCANNED, not billed, matching the colocated path's runStats
         // (totalBytesProcessed), so the field sums consistently across sources.
         // Null when the read reported nothing: a build with an empty metadata
         // bag, since the label is what makes BigQuery's read report.
         queryCostBytes: result.readCost?.bytesScanned ?? null,
      };
   }

   /**
    * Stack on the parent: build a chained storage source over its upstreams'
    * tables in the SAME destination. Rebinds each to a virtual source with its
    * captured schema, re-declares the downstream over them (text lifted from the
    * author's model), and hands the transient model to
    * {@link buildDownstreamIntoStorage}.
    *
    * Throws (the caller then recomputes or refuses) when there is no
    * same-destination upstream, the definition text can't be lifted, or the
    * transient model doesn't compile.
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
      // Rebind every upstream in THIS destination under every name for its
      // table: a rename (`source: x is daily`) binds to the same handle, as on
      // the serve path. An EXTENSION adds refinements the table lacks, so it is
      // lifted below over its base instead. A parent in another destination is
      // reported missing below.
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
         // A rollup is never an upstream (its synthesized name is in no model
         // file). Filtering it also keeps the legible "no materialized upstream"
         // refusal for a destination holding only rollups.
         .filter((b) => b.origin !== "preaggregate");
      // Each parent re-declares what its source adds to the table (`where:`,
      // fields, joins, views), as the serve shape does; without them a binding
      // reads the table unfiltered.
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
      // Stored tables the downstream reads, directly or transitively. One absent
      // from this destination's parents is named here, before any compile, which
      // would report only "undefined object".
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
         // Say where the table is, not only where it is not.
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
      // A path to the source warehouse (a joined table, a SQL source) has no
      // stored stand-in, so only a recompute can build it. Decided from the
      // model, not the compile, because strict treats "cannot express" and
      // "could not carry" differently.
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
      // Intermediate sources, each carried once everything it names is in the
      // model. An extension of a parent becomes the parent's virtual source plus
      // its refinements. Only sources on this downstream's path, so an
      // uncompilable lift elsewhere cannot fail this build.
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
      // The parents' refinements in the serve shape's tiers
      // (`REFINEMENT_TIER_KINDS`): everything first, then progressively thinner,
      // down to the row-changing `where:` alone. A lower tier is tried only on
      // a compile-time eligibility error, before anything is written. The
      // filter is never thinned, so no tier reads a parent unfiltered.
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
    * Lift `<name> is <def>` of a persist source's declaration from the author's
    * model file by its compiled `location` (the assembler prepends `source: `).
    * Undefined when there is no file-backed location or it can't be sliced.
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
      versionId?: unknown,
   ): Promise<Materialization> {
      const m = await this.getMaterialization(
         environmentName,
         packageName,
         id,
         versionId,
      );

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
    * Delete a terminal materialization record; an active run must be stopped
    * first. Physical-table GC is the caller's, unless `dropTables` asks for a
    * best-effort drop of the run's tables (a failure never blocks the delete).
    */
   async deleteMaterialization(
      environmentName: string,
      packageName: string,
      id: string,
      options: { dropTables?: boolean; versionId?: unknown } = {},
   ): Promise<void> {
      const m = await this.getMaterialization(
         environmentName,
         packageName,
         id,
         options.versionId,
      );

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

      // The deleted run may have been the one serving (and its tables dropped);
      // routing has no run-time fallback, so rebind to the next-latest
      // generation, or clear to serve live.
      await this.rebindServeBindingsAfterDelete(environmentName, packageName);
   }

   /**
    * Re-derive a package's serve routing, both tiers, from its latest remaining
    * successful materialization (mirroring the load-time
    * {@link Environment.rebindServeBindingsFromLocalStore}). Best-effort: a
    * failure leaves the current bindings for a later reload/build to fix.
    * Colocated is rebound regardless of `PERSIST_STORAGE_MODE` (it is not gated
    * by the storage kill switch); storage= only when the tier is not `off`.
    */
   private async rebindServeBindingsAfterDelete(
      environmentName: string,
      packageName: string,
   ): Promise<void> {
      await this.rebindLoadedVersions(environmentName, packageName);
   }

   /**
    * Re-derive serve routing from the store for every loaded slot of a
    * package: its unversioned slot, or each loaded version. A version reads
    * the runs that own its tables (its own under `scope: version`, the
    * package's otherwise; see ownedVersionOf), and one bound to a host
    * manifest is left alone, as on load. `exceptVersion` skips the version a
    * caller has just bound itself. Best-effort per slot.
    */
   private async rebindLoadedVersions(
      environmentName: string,
      packageName: string,
      exceptVersion?: string,
   ): Promise<void> {
      let environment: Awaited<ReturnType<EnvironmentStore["getEnvironment"]>>;
      let environmentId: string;
      let slots: (string | undefined)[];
      try {
         environmentId = await this.resolveEnvironmentId(environmentName);
         environment = await this.environmentStore.getEnvironment(
            environmentName,
            false,
         );
         const loaded = environment.getLoadedVersionIds(packageName);
         const versions = environment.getVersionService();
         slots =
            loaded.length > 0
               ? loaded
               : versions && (await versions.isVersioned(packageName))
                 ? [] // A versioned package with nothing loaded: nothing to rebind.
                 : [undefined];
      } catch (err) {
         logger.warn("Failed to rebind serve bindings", {
            packageName,
            error: errMessage(err),
         });
         return;
      }
      for (const versionId of slots) {
         if (versionId !== undefined && versionId === exceptVersion) continue;
         if (versionId !== undefined) {
            // Read and bound under the version's lock; a version that cannot
            // be rebound is unloaded, and binds from the store when next read.
            await environment.rebindVersionFromStore(
               packageName,
               versionId,
               (reader) =>
                  // "" excludes nothing — a deleted record is already gone.
                  this.getMostRecentManifestEntries(
                     environmentId,
                     packageName,
                     "",
                     reader,
                  ),
            );
            continue;
         }
         try {
            // "" excludes nothing — a deleted record is already gone.
            const entries = await this.getMostRecentManifestEntries(
               environmentId,
               packageName,
               "",
            );
            const { tableNameManifest, storageEntries } = splitManifestEntries(
               entries,
               `rebind (package ${packageName})`,
            );
            // Colocated: re-derive (or clear) regardless of mode.
            await environment.bindPackageColocatedServeManifest(
               packageName,
               tableNameManifest,
               versionId,
            );
            // Storage=: only meaningful when the tier is not off.
            if (getPersistStorageMode() !== "off") {
               await environment.bindPackageStorageServeBindings(
                  packageName,
                  storageEntries,
                  versionId,
               );
            }
         } catch (err) {
            logger.warn(
               "Failed to rebind serve bindings (leaving current bindings; a " +
                  "reload/build will re-derive)",
               { packageName, versionId, error: errMessage(err) },
            );
         }
      }
   }

   /**
    * Best-effort drop of every physical table (and leftover staging table) in
    * this run's manifest. Failures are logged and swallowed so cleanup never
    * blocks deleting the record.
    *
    * A name is dropped only when NO other MANIFEST_FILE_READY run references
    * it: generations of a source share one physical name, so dropping a
    * superseded record's table would take out the one still serving.
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

      // Names another MANIFEST_FILE_READY run still references, whichever
      // version built it. `m` is still in the repo (deleted after this sweep),
      // so it is excluded by id.
      const environmentId = await this.resolveEnvironmentId(environmentName);
      const stillReferenced = await this.tablesStillReferenced(
         environmentId,
         packageName,
         (other) => other.id !== m.id,
      );
      await this.dropManifestEntries(
         environment,
         environmentName,
         packageName,
         m.id,
         Object.values(entries),
         stillReferenced,
         await this.connectionResolverFor(environment, packageName, m.version),
      );
   }

   /**
    * The destination-and-name keys of every table a MANIFEST_FILE_READY run of
    * the package references, among the runs `include` admits.
    */
   private async tablesStillReferenced(
      environmentId: string,
      packageName: string,
      include: (other: Materialization) => boolean,
   ): Promise<Set<string>> {
      const stillReferenced = new Set<string>();
      const others =
         (await this.repository.listMaterializations(
            environmentId,
            packageName,
         )) ?? [];
      for (const other of others) {
         if (!include(other)) continue;
         if (other.status !== "MANIFEST_FILE_READY") continue;
         for (const e of Object.values(other.manifest?.entries ?? {})) {
            const dest = e.storageDestinationName ?? e.connectionName;
            if (dest && e.physicalTableName) {
               stillReferenced.add(tableKey(dest, e.physicalTableName));
            }
         }
      }
      return stillReferenced;
   }

   /**
    * How to reach the connection a run's table was built on. Through the
    * package the build used: its unversioned slot, or its version when that is
    * loaded. A version that is not loaded is never loaded for a drop: its
    * package-local `duckdb` is an in-memory database that went with its
    * unload (undefined: nothing to drop there), and every other connection
    * is the environment's.
    */
   private async connectionResolverFor(
      environment: Awaited<ReturnType<EnvironmentStore["getEnvironment"]>>,
      packageName: string,
      version: string | null,
   ): Promise<(name: string) => Promise<MalloyConnection | undefined>> {
      const pkg =
         version !== null
            ? environment.peekVersion(packageName, version)
            : await environment.getPackage(packageName, false);
      if (pkg) return (name) => pkg.getMalloyConnection(name);
      return async (name) =>
         name === "duckdb" ? undefined : environment.getMalloyConnection(name);
   }

   /**
    * Best-effort drop of the tables a run's manifest entries name, skipping
    * any `stillReferenced` holds. Failures are logged rather than thrown, and
    * returned: the entries whose table may still exist because its drop failed
    * or its destination could not be reached.
    */
   private async dropManifestEntries(
      environment: Awaited<ReturnType<EnvironmentStore["getEnvironment"]>>,
      environmentName: string,
      packageName: string,
      materializationId: string,
      entries: ManifestEntry[],
      stillReferenced: Set<string>,
      connectionFor: (name: string) => Promise<MalloyConnection | undefined>,
   ): Promise<ManifestEntry[]> {
      const m = { id: materializationId };
      const connectionCache = new Map<string, MalloyConnection>();
      const owed: ManifestEntry[] = [];
      for (const entry of entries) {
         const connectionName = entry.connectionName;
         const physicalTableName = entry.physicalTableName;
         if (!connectionName || !physicalTableName) {
            logger.warn("Skipping manifest entry with no connection/table", {
               materializationId: m.id,
               sourceEntityId: entry.sourceEntityId,
            });
            continue;
         }

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

         // A storage= table lives in the destination, not the source warehouse,
         // and needs a READ-WRITE attach (the serve attach is read-only). A
         // failure is logged and the sweep continues.
         if (entry.storageDestinationName) {
            const destinationConnection = this.destinationForCleanup(
               environment,
               entry.storageDestinationName,
            );
            if (!destinationConnection) {
               owed.push(entry);
               continue;
            }
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
               owed.push(entry);
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
               connection = await connectionFor(connectionName);
               if (!connection) {
                  logger.info(
                     "Skipping drop: the table lived in an unloaded version's in-memory duckdb",
                     { materializationId: m.id, physicalTableName },
                  );
                  continue;
               }
               connectionCache.set(connectionName, connection);
            }
            // Quoted as buildOneSource quoted it, so what built also drops.
            const dialect = connection.dialectName;
            // Tagged for attribution like a build, but as `ops`.
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
            // A crash between staging-create and rename can leave this behind.
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
            owed.push(entry);
            recordDropTables("failure", "in_warehouse");
            logger.warn("Failed to drop materialized table on delete", {
               materializationId: m.id,
               physicalTableName,
               connectionName,
               error: errMessage(err),
            });
         }
      }
      return owed;
   }

   /**
    * Reclaim what an archived version alone owns: the tables its auto-runs
    * built under `scope: version` (named for it, see versionTableSuffix).
    * Started by the archive, off the request path, and best-effort
    * throughout.
    *
    * Left alone: a table any other run of the package still references; the
    * tables of an orchestrated run, whose names and lifetime the caller that
    * assigned them owns; and everything under `scope: package`, whose tables
    * the package's versions share. A run that failed or was cancelled before
    * it committed is reclaimed too: its manifest names no table, so the tables
    * it recorded writing before it built are dropped instead
    * (reclaimableEntries). The reclaimed runs are then deleted, so an
    * unarchived version serves live until it is built again rather than
    * binding to tables that are gone. A table that could not be dropped keeps
    * a FAILED record naming it, which the sweep at startup tries again (as
    * does archiving the version again after an unarchive; archiving one that
    * is already archived changes nothing and starts no reclaim).
    *
    * Done under the version's lock, and only while it is still archived, or
    * deleted (no row: a delete that left a table owed): an unarchive, a
    * publish of the version, and a run of it (which is refused while it is
    * archived) take the same lock.
    */
   async reclaimVersionTables(
      environmentName: string,
      packageName: string,
      versionId: string,
   ): Promise<void> {
      try {
         const environment = await this.environmentStore.getEnvironment(
            environmentName,
            false,
         );
         const versions = environment.getVersionService();
         if (!versions) return;
         const environmentId = await this.resolveEnvironmentId(environmentName);
         await versions.withVersionLock(packageName, versionId, async () => {
            const row = await this.repository.getVersion(
               environmentId,
               packageName,
               versionId,
            );
            if (row !== null && row.archiveStatus !== "archive") return;
            await this.reclaimOwnedRuns(
               environment,
               environmentName,
               environmentId,
               packageName,
               versionId,
               row === null ? "deleted" : "archived",
            );
         });
      } catch (err) {
         logger.warn("Failed to reclaim an archived version's tables", {
            environmentName,
            packageName,
            version: versionId,
            error: errMessage(err),
         });
      }
   }

   /**
    * Reclaim what a version being deleted alone owns, as an archive's reclaim
    * does (see reclaimVersionTables), before the delete removes its row.
    * Called by the delete holding the version's lock, so it takes none; a
    * table it could not drop keeps a FAILED record, which the startup sweep
    * tries again once the row is gone. Best-effort: never throws.
    */
   async reclaimDeletingVersion(
      environmentName: string,
      packageName: string,
      versionId: string,
   ): Promise<void> {
      try {
         const environment = await this.environmentStore.getEnvironment(
            environmentName,
            false,
         );
         const environmentId = await this.resolveEnvironmentId(environmentName);
         await this.reclaimOwnedRuns(
            environment,
            environmentName,
            environmentId,
            packageName,
            versionId,
            "deleted",
         );
      } catch (err) {
         logger.warn("Failed to reclaim a deleted version's tables", {
            environmentName,
            packageName,
            version: versionId,
            error: errMessage(err),
         });
      }
   }

   /**
    * Drop the tables of a version's own committed or failed auto-runs that no
    * other run references, and delete those runs; keep a FAILED record naming
    * any table that could not be dropped. Called holding the version's lock,
    * with the version archived or being deleted, so nothing serves those
    * tables or writes them.
    */
   private async reclaimOwnedRuns(
      environment: Awaited<ReturnType<EnvironmentStore["getEnvironment"]>>,
      environmentName: string,
      environmentId: string,
      packageName: string,
      versionId: string,
      cause: "archived" | "deleted",
   ): Promise<void> {
      const runs = (
         await this.repository.listMaterializations(
            environmentId,
            packageName,
            {
               version: versionId,
            },
         )
      ).filter(
         (m) =>
            isVersionOwnedRun(m) &&
            m.metadata?.mode === "auto" &&
            m.status !== "PENDING" &&
            m.status !== "MANIFEST_ROWS_READY",
      );
      if (runs.length === 0) return;
      const reclaimed = new Set(runs.map((m) => m.id));
      const stillReferenced = await this.tablesStillReferenced(
         environmentId,
         packageName,
         (other) => !reclaimed.has(other.id),
      );
      // Never through getPackage: the version is out of service, and its
      // package-local duckdb went with its unload.
      const connectionFor = await this.connectionResolverFor(
         environment,
         packageName,
         versionId,
      );
      for (const m of runs) {
         const owed = await this.dropManifestEntries(
            environment,
            environmentName,
            packageName,
            m.id,
            reclaimableEntries(m),
            stillReferenced,
            connectionFor,
         );
         if (owed.length === 0) {
            await this.repository.deleteMaterialization(m.id);
            continue;
         }
         await this.repository.updateMaterialization(m.id, {
            status: "FAILED",
            error: `Reclaiming ${cause} version ${versionId} could not drop ${owed.length} table(s); the next reclaim retries.`,
            manifest: {
               ...(m.manifest ?? { strict: false }),
               entries: Object.fromEntries(
                  owed.map((e) => [e.sourceEntityId, e]),
               ),
            },
         });
         logger.warn(
            "Kept a reclaimed run's record: some of its tables could not be dropped",
            {
               environmentName,
               packageName,
               version: versionId,
               materializationId: m.id,
               owed: owed.map((e) => e.physicalTableName),
            },
         );
      }
      logger.info(
         `Reclaimed ${cause === "archived" ? "an archived" : "a deleted"} version's tables`,
         {
            environmentName,
            packageName,
            version: versionId,
            runs: runs.length,
         },
      );
   }

   /**
    * Reclaim every archived version that still has runs to reclaim, and every
    * deleted version whose delete left a table owed, across the environments
    * this server holds. Run once at startup: a reclaim is started only by an
    * archive or a delete, so one that a crash or a failed drop cut short would
    * otherwise never run again. Best-effort, one at a time.
    */
   async reclaimArchivedVersions(): Promise<void> {
      for (const environment of this.environmentStore.getLoadedEnvironments()) {
         const environmentName = environment.getEnvironmentName();
         try {
            const environmentId =
               await this.resolveEnvironmentId(environmentName);
            const rows =
               await this.repository.listVersionsByEnvironment(environmentId);
            for (const version of rows) {
               if (version.archiveStatus !== "archive") continue;
               await this.reclaimVersionTables(
                  environmentName,
                  version.packageName,
                  version.versionId,
               );
            }
            // A deleted version has no row: its own runs, still recorded,
            // name it. A package that lost its last version lost its runs
            // with it, so only packages that still have versions are read.
            const held = new Map<string, Set<string>>();
            for (const row of rows) {
               const ids = held.get(row.packageName) ?? new Set<string>();
               ids.add(row.versionId);
               held.set(row.packageName, ids);
            }
            for (const [packageName, ids] of held) {
               const deleted = new Set<string>();
               for (const m of await this.repository.listMaterializations(
                  environmentId,
                  packageName,
               )) {
                  if (
                     isVersionOwnedRun(m) &&
                     m.version !== null &&
                     !ids.has(m.version)
                  ) {
                     deleted.add(m.version);
                  }
               }
               for (const versionId of deleted) {
                  await this.reclaimVersionTables(
                     environmentName,
                     packageName,
                     versionId,
                  );
               }
            }
         } catch (err) {
            logger.warn("Failed to sweep archived versions for reclaim", {
               environmentName,
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
         // Omitted rather than {} on a clean run, matching earlier manifests.
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
      activeVersion?: string | null,
   ): MaterializationConflictError {
      const suffix = activeId
         ? activeVersion
            ? ` (${activeId}, building version ${activeVersion})`
            : ` (${activeId})`
         : "";
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
      // Called once the run has settled and its final status is recorded.
      onSettled?: () => void,
   ): void {
      const abortController = new AbortController();
      this.runningAbortControllers.set(id, abortController);

      run(abortController.signal)
         .catch(async (err) => {
            // Per-source failures arrive already redacted. A whole-run throw has
            // no connection to redact against but can still echo a DSN, so
            // redactPgSecrets (which needs none) is the floor for log and record.
            const message = redactPgSecrets(errMessage(err));
            const cancelled = abortController.signal.aborted;
            const next = cancelled ? "CANCELLED" : "FAILED";
            // A background throw answers no request, so log it (with the stack)
            // or it is invisible in the server's output.
            if (cancelled) {
               // An abort racing a genuine failure records "Cancelled"; this log
               // is then the only place the real error appears.
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
            onSettled?.();
         });
   }

   private resolveEnvironmentId(environmentName: string): Promise<string> {
      return resolveEnvironmentId(this.repository, environmentName);
   }
}
