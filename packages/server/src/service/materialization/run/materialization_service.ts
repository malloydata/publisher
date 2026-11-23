// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The run's front door ({@link MaterializationService}): creates, lists,
 * reads, stops and deletes materialization runs, and starts each build in the
 * background. Its {@link RunCollaborators} are the steps a run takes (the
 * build, a source's build, the storage tier's chained build, the commit), each
 * replaceable from the constructor. Called by the materialization controller,
 * main_server.ts, the scheduler and index.ts; main_server.ts reaches the
 * reclaimer through it.
 *
 * Effects: writes the run store; one active run per package, held by the
 * store's unique index; every table write goes through its collaborators.
 */

import { errorCodeOf } from "../../../error_code";
import { leafKey } from "../domain/physical_table";
import {
   BadRequestError,
   BuildInstructionsInvalidError,
   DestinationNotFoundError,
   BuildPlanUnavailableError,
   ConnectionNotFoundError,
   databaseAccessFailure,
   InvalidArgumentError,
   InvalidStateTransitionError,
   MaterializationConflictError,
   MaterializationNotFoundError,
   PackageVersionError,
   UnconfiguredConnectionError,
} from "../../../errors";
import { quoteTablePath, UnquotableIdentifierError } from "../../quoting";
import { isUsableTableName } from "./manifest_loader";
import { logger } from "../../../logger";
import { getPersistCollisionEnforce } from "../../../config";
import { MaterializationMode } from "../../../materialization_metrics";
import {
   Materialization,
   ResourceRepository,
} from "../../../storage/DatabaseInterface";
import { DuplicateActiveMaterializationError } from "../../../storage/duckdb/MaterializationRepository";
import type { components } from "../../../api";
import { withSpan } from "../../../telemetry";
import {
   parseEnvironmentName,
   parsePackageName,
   VersionRef,
} from "../../identity";
import { resolveEnvironmentId } from "../../resolve_environment";
import {
   ownedVersionOf,
   ServingReader,
   tableNamesIn,
   WRITES_TABLES_KEY,
} from "../../versions/materialization_scope";
import type { MaterializationHost } from "../host";
import { compileRunPlan, type RunPlan, runPlanGateText } from "./run_plan";
import {
   BuildInstruction,
   LedgerEntry,
   MalformedLedgerEntry,
   ManifestEntry,
   ManifestReference,
   RefusedSource,
} from "../domain/types";
import {
   type BuildRunFailedError,
   failedRunManifestOf,
   withFailedRunManifest,
} from "./build_failure_class";
import {
   incrementalRunContext,
   type IncrementalRunContext,
} from "./incremental_build";
import { boundParentReads, incrementalEntriesOf } from "./incremental_upstream";
import {
   INLINE_UNIQUENESS_KEY,
   rollupKeyRecordsIn,
   uncontestedAutoRun,
   UniquenessLedger,
   uniquenessPassesIn,
   withoutUniquenessPasses,
} from "./inline_uniqueness";
import { InstructedBuild, tableSiteContext } from "./instructed_build";
import {
   ManifestCommitter,
   rebindsOwnVersionAfter,
   referenceManifestToEntries,
} from "./manifest_committer";
import {
   type EligibilityTier,
   planRefusalOf,
} from "./materialization_instructed_run";
import { isActiveRun, outcomeFor } from "./run_status";
import { packageRelativeRun, withChainBuildText } from "./build_upstreams";
import { tallySources } from "./run_telemetry";
import type { QueryContext } from "../../query_metadata";
import { packageOwner } from "./table_mark_sql";
import { Reclaimer, RENAMED_TABLES_KEY } from "./reclaimer";
import { RunLifecycle } from "./run_lifecycle";
import {
   buildKindOf,
   buildKindsByAddress,
   reportSourceRefused,
   reportSourceReused,
   RunTelemetry,
   runTriggerOf,
   withRunTelemetry,
} from "./run_telemetry";
import {
   declaredFreshnessIndex,
   pastDeclaredWindow,
   stampDeclaredFreshness,
} from "./freshness_stamp";
import {
   deriveSelfInstructions,
   type RenamedTable,
   validateInstructions,
   writeTargetsFor,
} from "./run_instructions";
import { packageRelativeText } from "../../../package_relative_text";
import { buildSource } from "./source_build";
import { StorageBuildStrategy } from "./storage_build_strategy";
import { TableClaims } from "./table_claims";
import { uncarryLostTables, uncarryUnownedTables } from "./table_ownership";
import { isRollupOrigin } from "../domain/table_plans";

/** What a run is asked to do: createMaterialization's options, resolved. */
export interface RunBuildOptions {
   sourceNames: string[] | undefined;
   forceRefresh: boolean;
   reseed: boolean;
   buildInstructions: BuildInstruction[] | undefined;
   ledger: LedgerEntry[] | undefined;
   /** The caller's ledger entries the controller dropped as malformed. */
   malformedLedger?: MalformedLedgerEntry[];
   referenceManifest: ManifestReference[] | undefined;
   trigger: "on_demand" | "scheduler";
   runContext?: components["schemas"]["RunContext"];
   /** The version createMaterialization resolved; null when unversioned. */
   version: string | null;
}

/**
 * The steps a run takes through the service's collaborators, each one
 * replaceable from the constructor. The real ones reach each other through
 * this set, so replacing one step (one source's build, the storage tier's
 * chained build, a commit) changes it wherever a run takes it.
 */
export interface RunCollaborators {
   /** The background run a create starts: the build, inside its telemetry. */
   runBuild(
      id: string,
      environmentName: string,
      packageName: string,
      opts: RunBuildOptions,
      signal: AbortSignal,
   ): Promise<void>;
   /** An auto-run's instructions (run_instructions.ts). */
   deriveSelfInstructions: typeof deriveSelfInstructions;
   /** The run's incremental context, over the service's ledger store. */
   incrementalRunContext(
      compiled: RunPlan,
      run: Parameters<typeof incrementalRunContext>[1],
   ): IncrementalRunContext | undefined;
   /** The instructed build loop ({@link InstructedBuild.run}). */
   executeInstructedBuild: InstructedBuild["run"];
   /** One source's build ({@link buildSource}). */
   buildOneSource: (
      options: Parameters<typeof buildSource>[0],
   ) => ReturnType<typeof buildSource>;
   /** A chained storage build on its stored parents. */
   buildDownstreamViaParents: StorageBuildStrategy["buildDownstreamViaParents"];
   /** See {@link Reclaimer.reclaimStorageTablesFromFailedRun}. */
   reclaimStorageTablesFromFailedRun: Reclaimer["reclaimStorageTablesFromFailedRun"];
   /** How a reclaim drops one run's entries ({@link Reclaimer.dropManifestEntries}). */
   dropManifestEntries: Reclaimer["dropManifestEntries"];
   /** See {@link ManifestCommitter.commit}. */
   commitManifest: ManifestCommitter["commit"];
   /** See {@link ManifestCommitter.autoLoad}. */
   autoLoadManifest: ManifestCommitter["autoLoad"];
}

/**
 * The collaborators a service is constructed with: some of them, or a
 * function of the real ones that returns some (to wrap a real step).
 */
export type RunCollaboratorOverrides =
   | Partial<RunCollaborators>
   | ((real: RunCollaborators) => Partial<RunCollaborators>);

/** What a create may ask of a run (see MaterializationService.createMaterialization). */
interface CreateMaterializationOptions {
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
   /**
    * The caller-supplied incremental ledger (each entry a
    * `ManifestEntry.ledger` an earlier run reported). When present, even
    * empty, it replaces the local store for the whole run.
    * Orchestrated-only; validated at create time.
    */
   ledger?: LedgerEntry[];
   /**
    * The `ledger` entries dropped as malformed, each by the table it named:
    * its source seeds under `ledger_entry_malformed`. Not validated against
    * the plan, since a dropped entry is as good as absent.
    */
   malformedLedger?: MalformedLedgerEntry[];
   /**
    * What initiated this run: `on_demand` (default, manual/API) or
    * `scheduler` (the standalone scheduler). Recorded on the run metadata.
    */
   trigger?: "on_demand" | "scheduler";
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
   versionId?: VersionRef;
}

/** One run's arguments, as each stage of the build reads them. */
interface RunScope {
   telemetry: RunTelemetry;
   id: string;
   environmentName: string;
   packageName: string;
   opts: RunBuildOptions;
   signal: AbortSignal;
   orchestrated: boolean;
   mode: MaterializationMode;
   startedAt: number;
}

/** The query context every statement of a run carries. */
function runQueryContext(run: RunScope): QueryContext {
   const { id, environmentName, packageName, opts } = run;
   return {
      queryClass: "materialize",
      environment: environmentName,
      package: packageName,
      // The caller's trigger wins: it can express a publish.
      trigger: opts.runContext?.trigger ?? opts.trigger?.toLowerCase(),
      // Always set, so a run's statements group in query history.
      runId: opts.runContext?.runId ?? id,
   };
}

/** What a run's failure handling reads, set as the run goes. */
interface RunState {
   markedShared: boolean;
   packagePath: string | undefined;
   /** The package the run builds, once loaded. */
   target?: RunTarget;
   /**
    * Each instruction whose build started, in order, and whether the author
    * chose its table's name.
    */
   buildsStarted: { instruction: BuildInstruction; authorNamed: boolean }[];
}

/** The package a run builds, loaded and compiled. */
interface RunTarget {
   environmentId: string;
   environment: Awaited<ReturnType<MaterializationHost["getEnvironment"]>>;
   versionId: string | undefined;
   pkg: Awaited<ReturnType<RunTarget["environment"]["getPackage"]>>;
   owned: string | undefined;
   reader: ServingReader;
   compiled: RunPlan;
   /**
    * The sources the run's own plan refused, by sourceID: absent from
    * `compiled`, so an instruction naming one is failed with its refusal.
    */
   planRefusals: Record<string, RefusedSource>;
}

/** A run's instructions, and what deriving them decided. */
interface RunInstructions {
   instructions: BuildInstruction[];
   carried: Record<string, ManifestEntry>;
   refused: Record<string, RefusedSource>;
   cleared: ReadonlyMap<string, EligibilityTier> | undefined;
   renamed: RenamedTable[];
}

/** What the instructed build returned. */
type InstructedBuildResult = Awaited<
   ReturnType<RunCollaborators["executeInstructedBuild"]>
>;

/** What a run's commit leaves for the steps after it. */
interface CommittedRun {
   served: Awaited<ReturnType<RunCollaborators["commitManifest"]>>;
   failures: ReturnType<typeof packageRelativeRun>["failures"];
   sourcesFailed: number;
}

/**
 * How long deleting or unloading a package waits for its aborted runs to
 * settle. A run stops at its next check; a statement already sent to a
 * database that ignores the abort runs to its end first.
 */
const PACKAGE_RUNS_SETTLE_MS = 30_000;

/**
 * Orchestrates single-call materialization builds: the facade the controller,
 * the scheduler and main_server.ts call.
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
 *
 * The work is its collaborators': {@link RunLifecycle} (the run state machine
 * and background runs), {@link TableClaims} (which tables an active run
 * writes), {@link InstructedBuild} and the tier adapters it reaches through
 * {@link SourceBuilder} (the build itself), {@link ManifestCommitter} (commit,
 * bind, read back) and {@link Reclaimer} (giving tables back, reached
 * directly as {@link MaterializationService.reclaimer}). The steps a run
 * takes through them are its {@link RunCollaborators}: the constructor takes
 * any of them in place of the real one, so a spec hands the service a build,
 * a tier or a commit of its own rather than replacing a method.
 */
export class MaterializationService {
   private readonly lifecycle: RunLifecycle;
   private readonly claims: TableClaims;
   private readonly committer: ManifestCommitter;
   /**
    * Giving tables back: the version lifecycle's hooks (main_server.ts) and
    * the startup sweep call it directly.
    */
   readonly reclaimer: Reclaimer;
   /**
    * The tables each unsettled run in this process has started writing, by
    * run, so a retired run's drop keeps a table another run may have
    * rewritten since (Reclaimer.reclaimRetiredRunTables).
    */
   private readonly buildsInFlight = new Map<
      string,
      { environmentName: string; packageName: string; tables: string[] }
   >();
   /**
    * The steps a run takes: the constructor's, else the real ones. Specs
    * also drive a step through it.
    */
   readonly collaborators: RunCollaborators;

   constructor(
      private readonly host: MaterializationHost,
      overrides: RunCollaboratorOverrides = {},
   ) {
      const repository = () => this.repository;
      this.lifecycle = new RunLifecycle(repository);
      this.claims = new TableClaims(repository);
      this.committer = new ManifestCommitter({
         repository,
         host,
         lifecycle: this.lifecycle,
         withTableLock: (environmentId, packageName, fn) =>
            this.claims.withTableLock(environmentId, packageName, fn),
      });
      this.reclaimer = new Reclaimer({
         repository,
         host,
         lifecycle: this.lifecycle,
         dropManifestEntries: (...args) =>
            this.collaborators.dropManifestEntries(...args),
         // The locks a run's start takes for its active-run check, in the
         // same order (see createMaterialization), so a reclaim never
         // decides while a run starts, and never waits on a run's start
         // while holding a lock that start needs.
         withRunLock: (
            environment,
            environmentId,
            packageName,
            versionId,
            fn,
         ) => {
            if (versionId === undefined) {
               return environment.withPackageLock(packageName, fn);
            }
            const versions = environment.getVersionService();
            if (!versions) return fn();
            return versions.withVersionLock(packageName, versionId, () =>
               this.claims.withTableLock(environmentId, packageName, fn),
            );
         },
         withTableLock: (environmentId, packageName, fn) =>
            this.claims.withTableLock(environmentId, packageName, fn),
      });
      const real = this.realCollaborators();
      this.collaborators = {
         ...real,
         ...(typeof overrides === "function" ? overrides(real) : overrides),
      };
   }

   /**
    * The real steps. Each reaches the others through
    * {@link collaborators}, so a replaced step is the one every run takes.
    */
   private realCollaborators(): RunCollaborators {
      // Stateless, so one serves every build.
      const storage = new StorageBuildStrategy({
         buildDownstreamViaParents: (...a) =>
            this.collaborators.buildDownstreamViaParents(...a),
      });
      return {
         runBuild: (...args) => this.runBuild(...args),
         deriveSelfInstructions,
         incrementalRunContext: (compiled, run) =>
            incrementalRunContext(compiled, run, this.repository),
         executeInstructedBuild: (...args) =>
            new InstructedBuild({
               buildOneSource: (...a) =>
                  this.collaborators.buildOneSource(...a),
               reclaimStorageTablesFromFailedRun: (...a) =>
                  this.collaborators.reclaimStorageTablesFromFailedRun(...a),
               tableNotOwned: async (environmentName, packageName, target) => {
                  await this.claims.markHeldElsewhere(
                     await this.resolveEnvironmentId(environmentName),
                     packageName,
                     "",
                     target,
                  );
                  await this.committer.rebindLoadedVersions(
                     environmentName,
                     packageName,
                  );
               },
            }).run(...args),
         buildOneSource: (options) =>
            buildSource(options, (request) => storage.build(request)),
         buildDownstreamViaParents: (...args) =>
            storage.buildDownstreamViaParents(...args),
         reclaimStorageTablesFromFailedRun: (...args) =>
            this.reclaimer.reclaimStorageTablesFromFailedRun(...args),
         dropManifestEntries: (...args) =>
            this.reclaimer.dropManifestEntries(...args),
         commitManifest: (...args) => this.committer.commit(...args),
         autoLoadManifest: (...args) => this.committer.autoLoad(...args),
      };
   }

   private get repository(): ResourceRepository {
      return this.host.storageManager.getRepository();
   }

   // ==================== STATE MACHINE ====================

   /**
    * In-flight runs (the lifecycle's). Unread here: specs read it to wait for
    * or abort a background run.
    */
   protected get runningAbortControllers(): Map<string, AbortController> {
      return this.lifecycle.runningAbortControllers;
   }

   private runInBackground(
      id: string,
      run: (signal: AbortSignal) => Promise<void>,
      // Called once the run has settled and its final status is recorded.
      onSettled?: () => void,
      owner?: { environmentName: string; packageName: string },
   ): void {
      this.lifecycle.runInBackground(id, run, onSettled, owner);
   }

   // ==================== QUERIES ====================

   /**
    * A package's runs, newest first. For a versioned package, the runs of one
    * version (the one named, or `latest`) and the runs from before its first
    * versioned publish, which still drive what a `scope: package` version
    * serves. An unversioned package lists every run.
    */
   async listMaterializations(
      environmentName: string,
      packageName: string,
      options?: { limit?: number; offset?: number; versionId?: VersionRef },
   ): Promise<Materialization[]> {
      const versionId = options?.versionId ?? VersionRef.LATEST;
      const environmentId = await this.resolveEnvironmentId(environmentName);
      // Naming no version of a package that has no `latest` yet (explicit
      // promotion, before the first move) lists every run.
      const versions = (
         await this.host.getEnvironment(
            parseEnvironmentName(environmentName),
            false,
         )
      ).getVersionService();
      const version =
         versionId.requested() === undefined &&
         versions !== null &&
         (await versions.latestOf(packageName)) === null
            ? null
            : await this.resolveRunVersion(
                 environmentName,
                 packageName,
                 versionId,
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
      versionId: VersionRef,
   ): Promise<string | null> {
      const environment = await this.host.getEnvironment(
         parseEnvironmentName(environmentName),
         false,
      );
      const versions = environment.getVersionService();
      if (!versions) {
         const named = versionId.requested();
         if (named !== undefined) {
            throw new PackageVersionError(
               "VERSION_NOT_FOUND",
               `Package ${packageName} has no version ${named}.`,
            );
         }
         return null;
      }
      // A configured package whose boot publish was refused is published
      // again first, so a run resolves the version that publish makes rather
      // than an unversioned slot that does not exist.
      await environment.publishRefusedConfiguredPackage(
         packageName,
         versionId.requested(),
      );
      return (
         (await versions.resolve(packageName, versionId.requested()))
            ?.versionId ?? null
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
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<Materialization> {
      const environmentId = await this.resolveEnvironmentId(environmentName);
      // The version first, so an unknown or archived one says so (404/410,
      // with its reason) whatever the id.
      const named =
         versionId.requested() !== undefined
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
    *
    * The create is registered with the package's runs before its first
    * await, so a delete or unload of the package while it runs aborts it: a
    * create aborted before its run starts records CANCELLED and builds
    * nothing.
    */
   async createMaterialization(
      environmentName: string,
      packageName: string,
      options: CreateMaterializationOptions = {},
   ): Promise<Materialization> {
      const creating = this.lifecycle.beginCreate({
         environmentName,
         packageName,
      });
      try {
         return await this.createRun(
            environmentName,
            packageName,
            options,
            creating.signal,
         );
      } finally {
         creating.done();
      }
   }

   /** {@link createMaterialization}'s body; `retired` aborts it before the run starts. */
   private async createRun(
      environmentName: string,
      packageName: string,
      options: CreateMaterializationOptions,
      retired: AbortSignal,
   ): Promise<Materialization> {
      const {
         environmentId,
         environment,
         version,
         scope,
         versions,
         orchestrated,
         buildInstructions,
      } = await this.resolveRunTarget(environmentName, packageName, options);

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
      const trigger = options.trigger ?? "on_demand";
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
                       buildInstructions!.map((i) => i.physicalTableName),
                    ),
                 ],
              }
            : {}),
      };

      const start = (): Promise<Materialization> =>
         this.startRun({
            environmentId,
            environment,
            environmentName,
            packageName,
            options,
            version,
            lockVersion,
            metadata,
            orchestrated,
            buildInstructions,
            forceRefresh,
            reseed,
            trigger,
            retired,
         });

      // An unversioned package's run is created under the package's lock,
      // which a rebind from the store holds across its read and its bind
      // (Environment.rebindPackageFromStore, and a load's): the read either
      // sees this run active, and so binds no key check of a table it may
      // rewrite (uncontestedAutoRun), or the bind lands first, and this run
      // forgets what it bound before writing.
      if (version === null) {
         return environment.withPackageLock(packageName, start);
      }
      if (!versions) return start();
      // A version's run starts under that version's lock, which an archive
      // also takes: either the archive commits first and this answers 410, or
      // the run is registered first and the archive answers 409
      // VERSION_BUILDING. Archiving reclaims a version's tables, so it must
      // never overlap a run writing them. Then under the package's table
      // lock, so the tables a run with instructions claims are checked and
      // recorded as one step (see claimTables).
      return versions.withVersionLock(packageName, version, async () => {
         await versions.resolve(packageName, version);
         return this.claims.withTableLock(environmentId, packageName, start);
      });
   }

   /**
    * What a create builds: the run's version, its package and scope, refused
    * when an auto-run of a `scope: package` version is not its latest, and
    * the caller's instructions checked against the build plan.
    */
   private async resolveRunTarget(
      environmentName: string,
      packageName: string,
      options: CreateMaterializationOptions,
   ) {
      const environmentId = await this.resolveEnvironmentId(environmentName);

      const environment = await this.host.getEnvironment(
         parseEnvironmentName(environmentName),
         false,
      );
      const version = await this.resolveRunVersion(
         environmentName,
         packageName,
         options.versionId ?? VersionRef.LATEST,
      );
      const pkg = await environment.getPackage(
         parsePackageName(packageName),
         false,
         version !== null ? { versionId: VersionRef.parse(version) } : {},
      );
      // A package that serves an older model than its files (its last reload
      // refused them) is not run: the run would build the files the reload
      // refused. Refused here, a 409, so a schedule does not pile up failed
      // runs; the run itself checks again.
      if (
         version === null &&
         environment.getStaleCompileErrors().has(packageName)
      ) {
         throw new BuildPlanUnavailableError(
            `Package ${packageName} serves an older model than its files: its last reload refused them. Fix the files and reload the package, then run again.`,
         );
      }
      // Whose tables a version's run builds (see materialization_scope).
      const scope =
         version !== null
            ? (pkg.getPackageMetadata().scope ?? "package")
            : null;

      const buildInstructions = options.buildInstructions;
      const orchestrated = buildInstructions !== undefined;
      // A package or version bound to a host manifest serves the host's
      // tables, and a version a host has bound serves its manifest or live
      // (Environment.isHostOwned): a run's own tables are never applied to
      // either (Environment.loadRunManifest), so an auto-run would build
      // tables it cannot serve. Refused before anything is built, as the
      // scheduler skips it; a host building into tables it names
      // (buildInstructions) is what such a package's runs are for.
      if (!orchestrated && environment.isHostOwned(pkg)) {
         const manifestLocation = pkg.getPackageMetadata().manifestLocation;
         const target =
            version !== null
               ? `Version ${version} of package ${packageName}`
               : `Package ${packageName}`;
         throw new MaterializationConflictError(
            manifestLocation
               ? version !== null
                  ? `${target} is bound to a host manifest (${manifestLocation}): its tables are served from a host manifest, so a local run would build tables it cannot serve. Pass buildInstructions to build into tables the host names.`
                  : `${target} is bound to a host manifest (${manifestLocation}): its tables are served from a host manifest, so a local run would build tables it cannot serve. Clear its manifestLocation to build here, or pass buildInstructions to build into tables the host names.`
               : `${target} has been bound or built by a host: it serves the host's manifest or live, never a local run's tables, so a local run would build tables it cannot serve. Pass buildInstructions to build into tables the host names.`,
         );
      }
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
         // Validate against the plan the run will build from: a connection
         // edit re-derives it (build_plan_connection_refresh.spec.ts).
         await pkg.whenBuildPlanCurrent();
         // Never against a plan that failed to derive: after a failed
         // re-derivation the pre-edit plan is gone, and validating against
         // nothing would let stale ledger entries through to the build.
         if (pkg.isBuildPlanUnavailable()) {
            throw new BuildPlanUnavailableError(
               `The build plan of package ${packageName} could not be derived, so the build instructions cannot be checked against it. Reload the package to derive it again.`,
            );
         }
         validateInstructions(
            pkg.getBuildPlan(),
            buildInstructions,
            options.ledger,
            options.reseed ?? false,
         );
         // A stored reference's destination must be one this environment
         // configures: an unknown one would read its table from nowhere.
         for (const ref of options.referenceManifest ?? []) {
            if (ref.storageDestinationName === undefined) continue;
            try {
               environment.getStorageDestination(ref.storageDestinationName);
            } catch (err) {
               if (!(err instanceof DestinationNotFoundError)) throw err;
               throw new BuildInstructionsInvalidError(
                  `Manifest reference for '${ref.sourceEntityId}' names storage destination '${ref.storageDestinationName}', which this environment does not configure`,
               );
            }
         }
         if (version !== null) {
            // One table, two writers in one run: it would hold the last
            // one's rows under both definitions.
            const seen = new Set<string>();
            for (const i of buildInstructions) {
               const table = leafKey(i.physicalTableName);
               if (seen.has(table)) {
                  throw new BuildInstructionsInvalidError(
                     `buildInstructions name table ${i.physicalTableName} more than once; give each source a table of its own.`,
                  );
               }
               seen.add(table);
            }
         }
      }
      return {
         environmentId,
         environment,
         version,
         scope,
         versions,
         orchestrated,
         buildInstructions,
      };
   }

   /**
    * Record the run PENDING, unless another run holds its slot or a table it
    * names, and start its build in the background.
    */
   private async startRun(args: {
      environmentId: string;
      environment: RunTarget["environment"];
      environmentName: string;
      packageName: string;
      options: CreateMaterializationOptions;
      version: string | null;
      lockVersion: string | undefined;
      metadata: Record<string, unknown>;
      orchestrated: boolean;
      buildInstructions: BuildInstruction[] | undefined;
      forceRefresh: boolean;
      reseed: boolean;
      trigger: "on_demand" | "scheduler";
      /** Aborted when the package retired while this create ran. */
      retired: AbortSignal;
   }): Promise<Materialization> {
      const {
         environmentId,
         environment,
         environmentName,
         packageName,
         options,
         version,
         lockVersion,
         metadata,
         orchestrated,
         buildInstructions,
         forceRefresh,
         reseed,
         trigger,
         retired,
      } = args;
      const active = await this.repository.getActiveMaterialization(
         environmentId,
         packageName,
         lockVersion,
      );
      if (active) {
         throw this.lifecycle.activeConflict(
            packageName,
            active.id,
            active.version,
         );
      }
      if (version !== null && orchestrated) {
         await this.claims.assertTablesFree(
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
            // no versions passes neither.
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
            throw this.lifecycle.activeConflict(
               packageName,
               winner?.id,
               winner?.version,
            );
         }
         throw err;
      }
      // The package was deleted or unloaded while the record was written
      // (its abort found no run to stop), or before this start took the
      // package's lock (a create that began after the delete's last abort
      // found the package lock-free, then queued on the lock the delete
      // held): either way none starts. A version's start has already
      // re-resolved the version under its lock.
      if (
         retired.aborted ||
         (version === null && !environment.hasLoadedPackage(packageName))
      ) {
         return this.lifecycle.transition(created.id, "CANCELLED", {
            completedAt: new Date(),
            error: "Cancelled",
         });
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
            withSpan(
               "publisher.materialization.build",
               {
                  "malloy.package": packageName,
                  "publisher.materialization.id": created.id,
                  "publisher.materialization.mode": orchestrated
                     ? "orchestrated"
                     : "auto",
               },
               () =>
                  this.collaborators.runBuild(
                     created.id,
                     environmentName,
                     packageName,
                     {
                        sourceNames: options.sourceNames,
                        forceRefresh,
                        reseed,
                        buildInstructions,
                        // Orchestrated-only: an auto-run depends on the
                        // local store, which a supplied ledger would replace.
                        ledger: orchestrated ? options.ledger : undefined,
                        ...(orchestrated && options.malformedLedger?.length
                           ? { malformedLedger: options.malformedLedger }
                           : {}),
                        referenceManifest: options.referenceManifest,
                        trigger,
                        runContext: options.runContext ?? undefined,
                        version,
                     },
                     signal,
                  ),
            ),
         release,
         { environmentName, packageName },
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
   private runBuild(
      ...args: Parameters<RunCollaborators["runBuild"]>
   ): Promise<void> {
      const [id, , packageName, opts] = args;
      const telemetry = new RunTelemetry(
         id,
         opts.buildInstructions !== undefined ? "orchestrated" : "auto",
         packageName,
      );
      // Every source event of the run, however deep, belongs to this run.
      return withRunTelemetry(telemetry, () =>
         this.buildRun(telemetry, ...args),
      );
   }

   /** {@link runBuild}'s body, inside the run's telemetry. */
   private async buildRun(
      telemetry: RunTelemetry,
      id: string,
      environmentName: string,
      packageName: string,
      opts: RunBuildOptions,
      signal: AbortSignal,
   ): Promise<void> {
      const orchestrated = opts.buildInstructions !== undefined;
      const state: RunState = {
         // Set once this run has recorded the shared tables it rebuilds: from
         // then on, a failure must rebind the other loaded versions too.
         markedShared: false,
         // Where the package is on this server, once known: a whole-run failure
         // names its files package-relative too (see packageRelativeRun).
         packagePath: undefined,
         buildsStarted: [],
      };
      const mode: MaterializationMode = telemetry.mode;
      const startedAt = telemetry.startedAtMs;
      const run: RunScope = {
         telemetry,
         id,
         environmentName,
         packageName,
         opts,
         signal,
         orchestrated,
         mode,
         startedAt,
      };

      try {
         // Persisted so the UI can compute a duration.
         await this.repository.updateMaterialization(id, {
            startedAt: new Date(startedAt),
         });

         const target = await this.loadRunPackage(run, state);
         state.target = target;
         const { environmentId, pkg, compiled } = target;

         // Assembled BEFORE the instructions because skip-if-unchanged consults
         // it: an incremental source is exempt from the carry-forward below.
         const incremental = this.collaborators.incrementalRunContext(
            compiled,
            {
               environmentId,
               packageName,
               materializationId: id,
               // `reseed`, never `forceRefresh`: every scheduled fire sets
               // forceRefresh, which would make a schedule all full rebuilds.
               forceRefresh: opts.reseed ?? false,
               now: new Date(startedAt),
               ledger: opts.ledger,
               ...(opts.malformedLedger
                  ? { malformedLedger: opts.malformedLedger }
                  : {}),
            },
         );
         // What earlier deltas found missing, from the manifest bound now;
         // a delta or skip carries it until a rebuild.
         if (incremental) {
            incremental.priorLateRows = pkg.boundLateRows?.();
            incremental.noteIncrementalCheck = (sourceEntityId, finding) =>
               pkg.noteIncrementalCheck?.(sourceEntityId, finding);
            incremental.boundEntries = pkg.boundIncrementalEntries?.();
            // Standalone only: which build of its parent's table each
            // incremental extension last read, when the local ledger store
            // cannot say. A caller's ledger carries it (incremental_build.ts).
            if (opts.ledger === undefined) {
               incremental.boundParentReads = pkg.boundParentReads?.();
            }
         }

         const derived = await this.deriveRunInstructions(
            run,
            target,
            incremental,
         );
         const { instructions } = derived;
         await this.claimRunTables(run, target, instructions, state);

         telemetry.started(
            runTriggerOf({
               trigger: opts.trigger,
               runContextTrigger: opts.runContext?.trigger,
               orchestrated,
            }),
            instructions.length,
         );
         // Auto-run's refusals never reach the build loop, which counts the
         // rest of the run's sources.
         for (const refusal of Object.values(derived.refused)) {
            reportSourceRefused(
               {
                  kind: buildKindOf(compiled, refusal.sourceID),
                  tier: refusal.tier === "storage" ? "storage" : "database",
                  sourceName: refusal.name,
               },
               refusal.reason,
            );
         }

         // A rollup this run may rewrite in place no longer holds what its
         // bound key check said: forget it before any
         // table is written, so a run that fails part-way never leaves a
         // check bound over rows it did not read. This run's own commit binds
         // what it checks.
         pkg.forgetKeyUniqueness(
            instructions.map((instruction) => instruction.sourceEntityId),
         );

         const built = await this.runInstructedBuild(
            run,
            target,
            derived,
            incremental,
            state,
         );
         const committed = await this.commitRun(run, target, derived, built);
         // The extensions this run rebuilt for want of a record of their
         // parent's build, which the package warns about until a run proves
         // them again.
         if (incremental) {
            pkg.setUnprovenGenerations?.(incremental.unprovenGenerations ?? []);
         }
         await this.finishCommittedRun(run, target, derived, committed, state);
      } catch (err) {
         const outcome = outcomeFor(signal);
         telemetry.finished(outcome, err);
         if (state.packagePath !== undefined && err instanceof Error) {
            err.message = packageRelativeText(err.message, state.packagePath);
            // Each source's reason on the failed run's manifest, as a
            // committed run names them.
            const failed = failedRunManifestOf(err);
            if (failed) {
               // failedRunManifestOf found it on an error that carries one.
               withFailedRunManifest(err as BuildRunFailedError, {
                  failures: packageRelativeRun(
                     { entries: {}, failures: failed.failures, refused: {} },
                     state.packagePath,
                  ).failures,
               });
            }
         }
         if (state.markedShared && opts.version !== null) {
            // The run may have rebuilt some shared tables before it failed.
            // Its record stays, so rebinding now leaves those tables unbound
            // for every loaded version, as a restart would.
            await this.committer.rebindLoadedVersions(
               environmentName,
               packageName,
               opts.version,
            );
         }
         await this.reclaimIfRetired(run, state);
         throw err;
      } finally {
         this.buildsInFlight.delete(id);
      }
   }

   /** Record that `run` started writing `table` (see buildsInFlight). */
   private buildInFlight(run: RunScope, table: string): void {
      const { id, environmentName, packageName } = run;
      let entry = this.buildsInFlight.get(id);
      if (!entry) {
         entry = { environmentName, packageName, tables: [] };
         this.buildsInFlight.set(id, entry);
      }
      entry.tables.push(table);
   }

   /**
    * A run its package's delete or unload aborted gives back, before it
    * settles, the tables its builds may have written: once the package goes
    * nothing serves them and nothing else would reclaim them. An auto-run
    * only: a host's tables are its own. Never throws.
    */
   private async reclaimIfRetired(
      run: RunScope,
      state: RunState,
   ): Promise<void> {
      const { id, environmentName, packageName, orchestrated, signal } = run;
      const target = state.target;
      if (
         orchestrated ||
         !signal.aborted ||
         !this.lifecycle.isRetiring(id) ||
         target === undefined ||
         state.buildsStarted.length === 0
      ) {
         return;
      }
      const { environment, environmentId, pkg } = target;
      // A table at the author's `name=` may have stood before this run
      // wrote it, and a reader outside Malloy may rely on it: kept.
      const kept = state.buildsStarted.filter((b) => b.authorNamed);
      if (kept.length > 0) {
         logger.info(
            "Keeping the tables at an author's name= that a retired run wrote",
            {
               runId: id,
               packageName,
               tables: kept.map((b) => b.instruction.physicalTableName),
            },
         );
      }
      await this.reclaimer.reclaimRetiredRunTables({
         environment,
         environmentName,
         environmentId,
         packageName,
         runId: id,
         targets: writeTargetsFor(
            pkg.getBuildPlan(),
            state.buildsStarted
               .filter((b) => !b.authorNamed)
               .map((b) => b.instruction),
         ),
         // The package is still loaded: a retire stops its runs first.
         connectionFor: (name) => pkg.getMalloyConnection(name),
         inFlightElsewhere: [...this.buildsInFlight]
            .filter(
               ([runId, other]) =>
                  runId !== id &&
                  other.environmentName === environmentName &&
                  other.packageName === packageName,
            )
            .flatMap(([, other]) => other.tables),
      });
   }

   /**
    * The package a run builds, loaded (its version's, or the unversioned
    * one) and compiled; a `#@ persist` source the build walk dropped
    * refuses the run.
    */
   private async loadRunPackage(
      run: RunScope,
      state: RunState,
   ): Promise<RunTarget> {
      const { environmentName, packageName, opts, signal } = run;
      const environmentId = await this.resolveEnvironmentId(environmentName);
      const environment = await this.host.getEnvironment(
         parseEnvironmentName(environmentName),
         false,
      );
      const versionId = opts.version ?? undefined;
      const pkg = await environment.getPackage(
         parsePackageName(packageName),
         false,
         versionId !== undefined
            ? { versionId: VersionRef.parse(versionId) }
            : {},
      );
      state.packagePath = pkg.getPackagePath();
      // The version whose own runs this run reuses from and builds into
      // (`scope: version`); undefined, the package's shared ones.
      const owned = versionId !== undefined ? ownedVersionOf(pkg) : undefined;
      // Whose runs this run reuses from: its own under `scope: version`,
      // the package's shared ones otherwise (see ServingReader).
      const reader: ServingReader =
         versionId !== undefined ? { versionId, owned } : {};

      // A published version's tree never changes, so compiling it needs
      // no lock; an unversioned package's can be replaced under it.
      const { runPlan: compiled, packagePlan } =
         versionId !== undefined
            ? await compileRunPlan(pkg, signal)
            : await environment.withPackageLock(packageName, () =>
                 compileRunPlan(pkg, signal),
              );

      // A `#@ persist` source Malloy's build walk did not plan fails the
      // package's load (`persist-not-materializable`), so a package that
      // reaches a run has none: one source's mistake never refuses the
      // whole run. What can reach one is the files on disk changed since the
      // load: a save the load refused (the package serves its last good
      // model, stale). The run builds from those files, so it is refused
      // until they are fixed, rather than build what the load refused. A
      // version's files never change; only the unversioned package a reload
      // replaces can be stale.
      const stale =
         versionId === undefined &&
         environment.getStaleCompileErrors().has(packageName);
      // The load gate's own text, so the run and the load agree.
      const invalid = runPlanGateText(packagePlan, {
         enforceCollisions: getPersistCollisionEnforce(),
         versionScoped: owned !== undefined,
      });
      if (stale || invalid) {
         throw new BadRequestError(
            `Package ${packageName} is not built: ` +
               (invalid
                  ? `its files carry an invalid materialization annotation, which its load refuses:\n${invalid}`
                  : "its files changed and its last reload refused them, " +
                    "so it serves an older model") +
               `\nFix the files and reload the package, then run again.`,
         );
      }
      return {
         environmentId,
         environment,
         versionId,
         pkg,
         owned,
         reader,
         compiled,
         planRefusals: packagePlan.plan?.refusedSources ?? {},
      };
   }

   /**
    * The run's instructions: the caller's, with the upstream entries they
    * reference, or an auto-run's own, with what it carries forward, refuses
    * and renames.
    */
   private async deriveRunInstructions(
      run: RunScope,
      target: RunTarget,
      incremental: IncrementalRunContext | undefined,
   ): Promise<RunInstructions> {
      const { id, environmentName, packageName, opts, orchestrated } = run;
      const { environmentId, environment, pkg, owned, reader, compiled } =
         target;
      let instructions: BuildInstruction[];
      // Tables built under readable names in place of an earlier run's,
      // reclaimed once this run commits (Reclaimer.reclaimRenamedTables).
      let renamed: RenamedTable[] = [];
      let carried: Record<string, ManifestEntry>;
      let derivedRefused: Record<string, RefusedSource> = {};
      let clearedTiers: ReadonlyMap<string, EligibilityTier> | undefined;
      if (orchestrated) {
         instructions = opts.buildInstructions!;
         // Seed with the caller's upstream references so a persist upstream
         // built in a prior run resolves to its table instead of recomputing
         // live.
         carried = referenceManifestToEntries(opts.referenceManifest);
         // A reference is an IDENTITY, not a full copy. Upstream entries are
         // filled in DESCENDING precedence from:
         //   1. the `referenceManifest` in this call;
         //   2. the package's BOUND manifest (`manifestLocation`), which lets a
         //      worker that never built the upstream reuse its table;
         //   3. this worker's most recent local manifest.
         // Each fills the storage fields (sourceName, storageDestinationName,
         // schema) a `storage=` upstream's rebind needs. Each step is
         // best-effort: a fetch/read failure leaves the entries as they are.
         await this.committer.seedFromBoundManifest(carried, pkg, instructions);
         if (Object.keys(carried).length > 0) {
            await this.committer.resolveReferencesFromStore(
               carried,
               environmentId,
               packageName,
               id,
               reader,
            );
         }
      } else {
         // Skip-if-unchanged: reuse tables from the most recent manifest for
         // sources whose sourceEntityId is unchanged. Read under
         // `forceRefresh` too, where nothing is carried: the entries still
         // decide each table's name (an incremental source keeps the name
         // its ledger is keyed by, a readable name stays put).
         const priorEntries = await this.committer.mostRecentEntries(
            environmentId,
            packageName,
            id,
            reader,
         );
         if (incremental) {
            // Each incremental source's newest entry, over the bound
            // manifest's: a run of some sources binds a manifest naming only
            // those, while an extension's last read and its parent's last
            // build are on the newest entry of each (incremental_upstream.ts).
            const newest = Object.entries(
               await this.committer.newestIncrementalEntries(
                  environmentId,
                  packageName,
                  id,
                  reader,
               ),
            );
            incremental.boundParentReads = new Map([
               ...(incremental.boundParentReads ?? []),
               ...boundParentReads(newest),
            ]);
            incremental.boundEntries = new Map([
               ...(incremental.boundEntries ?? []),
               ...incrementalEntriesOf(newest),
            ]);
         }
         let recheck: Record<string, BuildInstruction>;
         let confirmExists: Record<string, BuildInstruction>;
         const freshness = declaredFreshnessIndex(pkg.getBuildPlan());
         ({
            instructions,
            carried,
            refused: derivedRefused,
            cleared: clearedTiers,
            recheck = {},
            confirmExists = {},
            renamed = [],
         } = this.collaborators.deriveSelfInstructions(
            compiled,
            opts.sourceNames,
            priorEntries,
            incremental,
            {
               environmentName,
               packageName,
               packagePath: pkg.getPackagePath(),
               ...(owned !== undefined ? { ownedVersion: owned } : {}),
            },
            {
               forceRefresh: opts.forceRefresh ?? false,
               pastWindow: (sourceEntityId, prior) =>
                  pastDeclaredWindow(
                     prior,
                     freshness.factsAt(sourceEntityId),
                     new Date(run.startedAt),
                  ),
               upstreams: {
                  windowSecondsAt: (address) =>
                     freshness.factsAt(address)?.freshnessWindowSeconds,
                  upstreamsOf: (address) => freshness.upstreamsOf(address),
                  at: new Date(run.startedAt),
               },
               incompatible:
                  pkg.incompatibleTables?.(priorEntries) ?? new Set(),
            },
         ));
         // A carried table at the author's `name=` is reused only while
         // it is still marked as this package's (table_ownership.ts).
         const siteContext = () =>
            tableSiteContext({
               compiled,
               environment,
               buildMetadata: {
                  packageMaterialization: pkg.getMaterializationConfig(),
                  context: runQueryContext(run),
               },
            });
         if (Object.keys(recheck).length > 0) {
            await uncarryUnownedTables(
               recheck,
               carried,
               instructions,
               packageOwner(environmentName, packageName),
               siteContext(),
            );
         }
         // A carried table in the package's in-memory sandbox is reused
         // only while it still exists: a restart empties the sandbox.
         if (Object.keys(confirmExists).length > 0) {
            await uncarryLostTables(
               confirmExists,
               carried,
               instructions,
               siteContext(),
            );
         }
      }
      return {
         instructions,
         carried,
         refused: derivedRefused,
         cleared: clearedTiers,
         renamed,
      };
   }

   /**
    * Before a version's run writes a table, claim what it writes; for the
    * package's shared tables, also mark older entries superseded and rebind
    * the other loaded versions.
    */
   private async claimRunTables(
      run: RunScope,
      target: RunTarget,
      instructions: BuildInstruction[],
      state: RunState,
   ): Promise<void> {
      const { id, environmentName, packageName, orchestrated } = run;
      const { environmentId, versionId, pkg, owned } = target;
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
            await this.claims.withTableLock(environmentId, packageName, () =>
               this.claims.markSuperseded(
                  environmentId,
                  packageName,
                  id,
                  instructions,
               ),
            );
         } else {
            await this.claims.claimTables(
               environmentId,
               packageName,
               id,
               instructions,
            );
         }
         state.markedShared = true;
         await this.committer.rebindLoadedVersions(
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
         await this.claims.claimTables(
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
   }

   /** The instructed build of the run's sources, with its uniqueness ledger. */
   private async runInstructedBuild(
      run: RunScope,
      target: RunTarget,
      derived: RunInstructions,
      incremental: IncrementalRunContext | undefined,
      state: RunState,
   ): Promise<InstructedBuildResult> {
      const { id, packageName, opts, signal, orchestrated } = run;
      const { environmentId, environment, versionId, pkg, compiled } = target;
      const {
         instructions,
         carried,
         refused: derivedRefused,
         cleared: clearedTiers,
      } = derived;
      // An instruction is checked against the plan the package serves
      // (validateInstructions); the run's own plan may have refused its
      // source since (judged against tables bound after the served plan was
      // derived), and then holds no table for it.
      const refusedAtPlan = planRefusalOf(
         pkg.getBuildPlan()?.sources ?? {},
         target.planRefusals,
      );
      return await this.collaborators.executeInstructedBuild({
         refusedAtPlan,
         compiled,
         environment: withChainBuildText(
            environment,
            pkg,
            // A version's files never change; only the unversioned package
            // a reload replaces can be stale.
            () =>
               versionId === undefined &&
               (environment.getStaleCompileErrors?.().has(packageName) ??
                  false),
         ),
         instructions,
         seedEntries: carried,
         signal,
         // Always strict: a downstream is built over its upstreams' tables
         // or not at all.
         strict: true,
         // Orchestrated-only; see reclaimStorageTablesFromFailedRun.
         owner: orchestrated ? { environmentId, packageName } : undefined,
         buildMetadata: {
            // Optional: a lighter package surface resolves without a
            // package-level layer.
            packageMaterialization: pkg.getMaterializationConfig(),
            context: runQueryContext(run),
         },
         incremental,
         priorRefusals: Object.values(derivedRefused),
         clearedTiers,
         hostInstructed: orchestrated,
         freshness: (() => {
            const index = declaredFreshnessIndex(pkg.getBuildPlan());
            return {
               windowSecondsAt: (address: string) =>
                  index.factsAt(address)?.freshnessWindowSeconds,
               at: new Date(run.startedAt),
            };
         })(),
         // Auto-run only: Publisher owns every write to its tables, so it
         // can tell a joined table that has not changed since a check
         // passed. A host's tables are its own.
         uniqueness: orchestrated
            ? undefined
            : await (async () => {
                 const prior = opts.forceRefresh
                    ? undefined
                    : await this.priorUniquenessMetadata(
                         environmentId,
                         packageName,
                         id,
                      );
                 return new UniquenessLedger(
                    prior ? uniquenessPassesIn(prior) : undefined,
                    id,
                    prior ? rollupKeyRecordsIn(prior) : undefined,
                 );
              })(),
         // Fresh on each ask: a run committed since the run began may name
         // the table.
         packageRuns: async () =>
            (await this.repository.listMaterializations(
               environmentId,
               packageName,
            )) ?? [],
         onBuildStarted: (instruction, authorNamed) => {
            state.buildsStarted.push({ instruction, authorNamed });
            this.buildInFlight(run, instruction.physicalTableName);
         },
      });
   }

   /**
    * Commit the run's manifest, its files named package-relative, and for an
    * auto-run load it into the package before the run reports done.
    */
   private async commitRun(
      run: RunScope,
      target: RunTarget,
      derived: RunInstructions,
      built: InstructedBuildResult,
   ): Promise<CommittedRun> {
      const { id, packageName, opts, signal, orchestrated, mode, startedAt } =
         run;
      const { environment, versionId, pkg } = target;
      const { carried, renamed } = derived;
      let derivedRefused = derived.refused;
      const {
         entries: builtEntries,
         failures: builtFailures,
         sourcesRefused: instructedRefused,
         refusedBesideBuilt: builtRefusedBeside = {},
         uniquenessChecks,
      } = built;

      // What the run records names the package's files package-relative:
      // a compile error in a reason names its file by where the server
      // keeps it.
      const {
         entries,
         failures,
         refused: refusedBesideBuilt,
      } = packageRelativeRun(
         {
            entries: builtEntries,
            failures: builtFailures,
            refused: builtRefusedBeside,
         },
         pkg.getPackagePath(),
      );
      derivedRefused = packageRelativeRun(
         { entries: {}, failures: {}, refused: derivedRefused },
         pkg.getPackagePath(),
      ).refused;

      const { sourcesBuilt, sourcesFailed, sourcesReused } = tallySources(
         entries,
         failures,
         carried,
      );
      // Auto-run refusals ride the run's free-form metadata, not the manifest
      // a strict-schema caller parses. Orchestrated refusals are in
      // `failures`, where that caller already looks, unless an eligible
      // sibling built their table (then in `refusedSources`).
      const sourcesRefused =
         Object.keys(derivedRefused).length + instructedRefused;
      const durationMs = Date.now() - startedAt;
      // A standalone run stamps its tables' declared freshness, as a host
      // stamps the manifest it distributes; a host's run leaves it to the host.
      const committedEntries = orchestrated
         ? entries
         : stampDeclaredFreshness(
              entries,
              declaredFreshnessIndex(pkg.getBuildPlan()),
              new Set(Object.keys(carried)),
              new Date(startedAt),
              built.readFrom,
           );
      // A host built this version: it owns it from now on, as a bind would
      // make it (Environment.isHostOwned), so a clear, reload or restart
      // never binds what this server's store holds for it. Marked before
      // the commit, so no load or settle in between binds this run's own
      // tables from the store.
      if (orchestrated && versionId !== undefined) {
         await environment.markVersionHostOwned(packageName, versionId);
      }
      const served = await this.collaborators.commitManifest(
         id,
         committedEntries,
         failures,
         {
            // Recorded before any is dropped, so a crash between this
            // commit and the drops leaves them for the next run to finish.
            ...(renamed.length > 0 ? { [RENAMED_TABLES_KEY]: renamed } : {}),
            forceRefresh: opts.forceRefresh,
            sourceNames: opts.sourceNames ?? null,
            mode,
            trigger: opts.trigger,
            sourcesBuilt,
            sourcesReused,
            ...(sourcesRefused > 0 ? { sourcesRefused } : {}),
            // Auto-run's refusals, plus instructed sources refused beside a
            // table an eligible sibling built, which are not in `failures`.
            ...(Object.keys(derivedRefused).length +
               Object.keys(refusedBesideBuilt).length >
            0
               ? {
                    refusedSources: {
                       ...derivedRefused,
                       ...refusedBesideBuilt,
                    },
                 }
               : {}),
            durationMs,
            // The inline uniqueness checks: the passes the next run may
            // rely on, and whether each ran or was skipped.
            ...(uniquenessChecks
               ? { [INLINE_UNIQUENESS_KEY]: uniquenessChecks }
               : {}),
            // commitManifest replaces the metadata, so the version is set again.
            ...(versionId !== undefined
               ? {
                    versionId,
                    scope: pkg.getPackageMetadata().scope ?? "package",
                 }
               : {}),
         },
         // Auto-run owns distribution, and loads the manifest into its own
         // package before the run reports MANIFEST_FILE_READY, so "done"
         // means served. Orchestrated leaves it to the caller
         // (manifestLocation). Other loaded versions are rebound after,
         // below: they read the run from the store, which counts it only
         // once it is MANIFEST_FILE_READY.
         orchestrated
            ? undefined
            : (finish) =>
                 this.collaborators.autoLoadManifest(
                    environment,
                    packageName,
                    committedEntries,
                    versionId,
                    uniquenessChecks?.rollupKeys,
                    // A stop that landed during the load did not cut it
                    // short; the run says so.
                    (served) =>
                       finish(
                          signal.aborted
                             ? {
                                  ...served,
                                  metadata: {
                                     ...served.metadata,
                                     stopIgnored: true,
                                  },
                               }
                             : served,
                       ),
                 ),
      );
      return { served, failures, sourcesFailed };
   }

   /**
    * After the commit: rebind the loaded copies of the package, reclaim the
    * tables readable names replaced, count what was carried forward, and
    * record how the run ended.
    */
   private async finishCommittedRun(
      run: RunScope,
      target: RunTarget,
      derived: RunInstructions,
      committed: CommittedRun,
      state: RunState,
   ): Promise<void> {
      const { telemetry, id, environmentName, packageName, orchestrated } = run;
      const { versionId, pkg, reader, compiled } = target;
      const { carried, renamed } = derived;
      const { served, failures, sourcesFailed } = committed;
      // Whether every loaded copy of the package now reads this run: its
      // own load, then the other versions' rebinds below.
      let bound = !orchestrated && served?.autoLoad === "loaded";
      if (
         !orchestrated &&
         versionId !== undefined &&
         rebindsOwnVersionAfter(served)
      ) {
         await this.committer.rebindOwnVersionIfMoved(
            environmentName,
            packageName,
            versionId,
         );
      }
      if (state.markedShared && versionId !== undefined) {
         // Every other loaded version is rebound to this run too: each
         // binds only what its own definition built, and serves live what
         // this run rebuilt from a different one (see
         // Package.bindStorageServeBindings).
         bound =
            (await this.committer.rebindLoadedVersions(
               environmentName,
               packageName,
               versionId,
            )) && bound;
      }
      // Once nothing loaded reads them, the tables readable names replaced
      // (this run's, and any an earlier run could not finish). Only after
      // this run's own load took, and, before any drop, once every loaded
      // copy of the package has rebound from the store: one that did not
      // (a version an earlier run's failed load left on the old table)
      // still reads it.
      // Unbound, nothing is dropped, but the tables it keeps are still
      // listed (Reclaimer.reclaimRenamedTables).
      if (!orchestrated) {
         if (!bound && renamed.length > 0) {
            logger.warn(
               "Keeping the tables readable names replaced: the package did not bind to this run",
               { packageName, tables: renamed.map((t) => t.previousName) },
            );
         }
         const kept = await this.reclaimer.reclaimRenamedTables(
            environmentName,
            packageName,
            id,
            reader,
            bound
               ? () =>
                    this.committer.rebindLoadedVersions(
                       environmentName,
                       packageName,
                    )
               : async () => false,
         );
         if (kept !== undefined) pkg.setKeptTables?.(kept);
      }

      // Carried forward, so never in the build loop: counted once the run
      // has committed them. A host's reference to an address this run
      // rebuilt was counted as built.
      const kindByAddress = buildKindsByAddress(compiled, pkg.getBuildPlan());
      for (const [address, entry] of Object.entries(carried)) {
         if (failures[address] || telemetry.builtAddresses.has(address)) {
            continue;
         }
         reportSourceReused(
            kindByAddress.get(address) ??
               (isRollupOrigin(entry.origin) ? "rollup" : "persist"),
            entry.storageDestinationName ? "storage" : "database",
         );
      }
      // A run that lost sources is partial: its manifest is missing tables a
      // consumer expected. An auto-run refusal is not a loss: it is a property
      // of the model, and counting it would make every run partial.
      const partial = sourcesFailed > 0;
      telemetry.finished(partial ? "partial" : "success");
   }

   // ==================== UNIQUENESS PASSES ====================

   /**
    * The inline uniqueness passes an auto-run may rely on: those the run
    * before it recorded, and only when that run is the one every other run of
    * the package (any version, any status) had finished before it started.
    * Then no Publisher run wrote a table between that run's checks and this
    * run, so a joined table it holds unchanged is the table its check passed
    * over. An orchestrated run before, or any doubt (a run still active, one
    * that overlapped, an unreadable listing), relies on nothing.
    */
   private async priorUniquenessMetadata(
      environmentId: string,
      packageName: string,
      excludeId: string,
   ): Promise<Record<string, unknown> | undefined> {
      // Read right before the build, never from the run's start: a run
      // created during the claim and rebind must make this one rely on
      // nothing.
      let runs: Materialization[];
      try {
         runs = (
            (await this.repository.listMaterializations(
               environmentId,
               packageName,
            )) ?? []
         ).filter((m) => m.id !== excludeId);
      } catch (err) {
         logger.warn(
            "Could not list earlier runs; every inline uniqueness check runs",
            { packageName, code: errorCodeOf(err) },
         );
         return undefined;
      }
      return uncontestedAutoRun(runs)?.metadata ?? undefined;
   }

   /**
    * Drop the inline uniqueness passes every other run of the package records
    * (see {@link withoutUniquenessPasses}), so the next auto-run checks again
    * rather than rely on a pass whose run a deletion made look newest.
    */
   private async forgetUniquenessPasses(
      environmentId: string,
      packageName: string,
      deletedId: string,
   ): Promise<void> {
      // Read fresh, after the drop: each run's metadata is written back
      // whole, so an older copy would undo what another run wrote since.
      const runs =
         (await this.repository.listMaterializations(
            environmentId,
            packageName,
         )) ?? [];
      for (const run of runs) {
         if (run.id === deletedId) continue;
         const metadata = withoutUniquenessPasses(run.metadata);
         if (metadata) {
            await this.repository.updateMaterialization(run.id, { metadata });
         }
      }
   }

   // ==================== CANCELLATION ====================

   /**
    * Abort every run of a package running in this process, and every create
    * of one whose run has not started, for a package being deleted or
    * unloaded: the abort lands before this returns, so call
    * it before the package goes. Resolves once those runs have settled
    * (CANCELLED unless one finished first), or after `settleWithinMs`.
    */
   abortPackageRuns(
      environmentName: string,
      packageName: string,
      settleWithinMs = PACKAGE_RUNS_SETTLE_MS,
   ): Promise<void> {
      return this.lifecycle.abortPackageRuns(
         environmentName,
         packageName,
         settleWithinMs,
      );
   }

   /** Cancel a non-terminal materialization. */
   async stopMaterialization(
      environmentName: string,
      packageName: string,
      id: string,
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<Materialization> {
      const m = await this.getMaterialization(
         environmentName,
         packageName,
         id,
         versionId,
      );

      if (!isActiveRun(m.status)) {
         throw new InvalidStateTransitionError(
            `Materialization ${id} is ${m.status}, cannot stop`,
         );
      }
      // Undefined when the run was aborted in this process: it records
      // CANCELLED itself as it unwinds.
      return (await this.lifecycle.cancel(id)) ?? m;
   }

   /**
    * Delete a terminal materialization record; an active run must be stopped
    * first. Record-only: it never drops a table. Physical-table GC is the
    * caller's, through the connection.
    */
   async deleteMaterialization(
      environmentName: string,
      packageName: string,
      id: string,
      options: { versionId?: VersionRef } = {},
   ): Promise<void> {
      const m = await this.getMaterialization(
         environmentName,
         packageName,
         id,
         options.versionId,
      );

      if (isActiveRun(m.status)) {
         throw new InvalidStateTransitionError(
            `Cannot delete materialization ${id} while it is ${m.status}`,
         );
      }

      // The record is part of what proves a recorded uniqueness pass still
      // holds: once it goes, an older run's pass could read as the newest
      // over a table this run rewrote. Forgotten first, so a failure leaves
      // the record (and the proof) in place.
      await this.forgetUniquenessPasses(m.environmentId, packageName, id);
      await this.repository.deleteMaterialization(id);

      // The deleted run may have been the one serving;
      // routing has no run-time fallback, so rebind to the next-latest
      // generation, or clear to serve live.
      // (Mirrors the load-time Environment.rebindServeBindingsFromLocalStore;
      // best-effort: a failure leaves the bindings for a later reload or build.)
      await this.committer.rebindLoadedVersions(environmentName, packageName);
   }

   /**
    * Drop one database-tier table a materialization of the package built on
    * `connectionName`, at a caller's request (`drop-materialized-table`),
    * when Publisher can prove it is the package's and that nothing uses it
    * (Reclaimer.dropConnectionTable). "absent" when the table is already
    * gone and a run record of the package still names it there.
    *
    * Resolved here, before any lock: the version (404 unknown, 410
    * archived), the run `materializationId` names (404 unless it is the
    * package's), the table name (400 unless every dialect parses it and the
    * connection's quotes it), the package (which may load it) and the
    * connection it reaches by that name (404 otherwise).
    */
   async dropMaterializedTable(
      environmentName: string,
      packageName: string,
      connectionName: string,
      tableName: string,
      options: { materializationId?: string; versionId?: VersionRef } = {},
   ): Promise<"dropped" | "absent"> {
      const versionId = options.versionId ?? VersionRef.LATEST;
      const environmentId = await this.resolveEnvironmentId(environmentName);
      const environment = await this.host.getEnvironment(
         parseEnvironmentName(environmentName),
         false,
      );
      await this.resolveRunVersion(environmentName, packageName, versionId);
      if (options.materializationId !== undefined) {
         await this.getMaterialization(
            environmentName,
            packageName,
            options.materializationId,
            versionId,
         );
      }
      if (!isUsableTableName(tableName)) {
         throw new InvalidArgumentError(
            `tableName '${tableName}' must be a table path with no empty part, no backslash or control character, and only whole quoted parts`,
         );
      }
      const pkg = await environment.getPackage(
         parsePackageName(packageName),
         false,
         { versionId },
      );
      let connection: Awaited<ReturnType<typeof pkg.getMalloyConnection>>;
      try {
         connection = await pkg.getMalloyConnection(connectionName);
      } catch (error) {
         if (error instanceof UnconfiguredConnectionError) {
            throw new ConnectionNotFoundError(
               `Package ${packageName} reaches no connection ${connectionName}`,
            );
         }
         throw databaseAccessFailure(error) ?? error;
      }
      try {
         quoteTablePath(tableName, connection.dialectName);
      } catch (error) {
         if (error instanceof UnquotableIdentifierError) {
            throw new InvalidArgumentError(error.message);
         }
         throw error;
      }
      return this.reclaimer.dropConnectionTable({
         environment,
         environmentName,
         environmentId,
         packageName,
         connectionName,
         connection,
         physicalTableName: tableName,
         ...(options.materializationId !== undefined
            ? { materializationId: options.materializationId }
            : {}),
      });
   }

   private resolveEnvironmentId(environmentName: string): Promise<string> {
      return resolveEnvironmentId(this.repository, environmentName);
   }
}
