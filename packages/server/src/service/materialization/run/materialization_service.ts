// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   BadRequestError,
   BuildPlanUnavailableError,
   InvalidStateTransitionError,
   MaterializationEligibilityError,
   MaterializationNotFoundError,
   PackageVersionError,
} from "../../../errors";
import { logger } from "../../../logger";
import { MaterializationMode } from "../../../materialization_metrics";
import {
   Materialization,
   MaterializationUpdate,
   ResourceRepository,
} from "../../../storage/DatabaseInterface";
import { DuplicateActiveMaterializationError } from "../../../storage/duckdb/MaterializationRepository";
import type { components } from "../../../api";
import { withSpan } from "../../../telemetry";
import { errMessage } from "../../../utils";
import {
   parseEnvironmentName,
   parsePackageName,
   VersionRef,
} from "../../identity";
import { resolveEnvironmentId } from "../../resolve_environment";
import {
   ownedVersionOf,
   ServingReader,
   tableIdentity,
   tableNamesIn,
   packageTableSuffix,
   versionTableSuffix,
   WRITES_TABLES_KEY,
} from "../../versions/materialization_scope";
import type { MaterializationEnvironment, MaterializationHost } from "../host";
import { CompiledBuildPlan } from "../plan/build_plan";
import { compilePackageBuildPlan } from "../plan/build_plan_compile";
import {
   BuildInstruction,
   LedgerEntry,
   ManifestEntry,
   ManifestReference,
   MaterializationStatus,
   RefusedSource,
} from "../domain/types";
import {
   incrementalRunContext,
   type IncrementalRunContext,
} from "./incremental_build";
import {
   INLINE_UNIQUENESS_KEY,
   rollupKeyRecordsIn,
   uncontestedAutoRun,
   UniquenessLedger,
   uniquenessPassesIn,
   withoutUniquenessPasses,
} from "./inline_uniqueness";
import { InstructedBuild, nameViewContext } from "./instructed_build";
import {
   ManifestCommitter,
   referenceManifestToEntries,
} from "./manifest_committer";
import type { EligibilityTier } from "./materialization_instructed_run";
import {
   BuildEnvironment,
   outcomeFor,
   tallySources,
   type WarehouseReach,
} from "./materialization_run_support";
import {
   dropReplacedTablesAside,
   NAME_VIEWS_KEY,
   restoreNameViews,
   type NameViewRecord,
} from "./name_view";
import { nameViewOwner } from "./name_view_sql";
import { Reclaimer } from "./reclaimer";
import { RunLifecycle } from "./run_lifecycle";
import {
   buildKindOf,
   buildKindsByAddress,
   markRunFailureReported,
   reportSourceRefused,
   reportSourceReused,
   RunTelemetry,
   runTriggerOf,
   withRunTelemetry,
} from "./run_telemetry";
import {
   deriveSelfInstructions,
   validateInstructions,
   writeTargetsFor,
} from "./run_instructions";
import { SourceBuilder } from "./source_build";
import {
   StorageBuildStrategy,
   type StorageBuildRequest,
} from "./storage_build_strategy";
import { TableClaims } from "./table_claims";
import { uncarryUnownedTables } from "./table_ownership";

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
 * writes), {@link InstructedBuild} and the tier strategies it reaches through
 * {@link SourceBuilder} (the build itself), {@link ManifestCommitter} (commit,
 * bind, read back) and {@link Reclaimer} (giving tables back). The private
 * methods below that only delegate are the seams this class's specs drive and
 * substitute; a collaborator that calls one goes back through this class.
 */
export class MaterializationService {
   private readonly lifecycle: RunLifecycle;
   private readonly claims: TableClaims;
   private readonly committer: ManifestCommitter;
   private readonly reclaimer: Reclaimer;

   constructor(private readonly host: MaterializationHost) {
      const repository = () => this.repository;
      this.lifecycle = new RunLifecycle(repository);
      this.claims = new TableClaims(repository);
      this.committer = new ManifestCommitter({
         repository,
         host,
         lifecycle: this.lifecycle,
      });
      this.reclaimer = new Reclaimer({
         repository,
         host,
         lifecycle: this.lifecycle,
         dropManifestEntries: (...args) => this.dropManifestEntries(...args),
         reclaimVersionTables: (...args) => this.reclaimVersionTables(...args),
      });
   }

   private get repository(): ResourceRepository {
      return this.host.storageManager.getRepository();
   }

   // ==================== STATE MACHINE ====================

   /** In-flight runs, so they can be cancelled. In-process only. */
   private get runningAbortControllers(): Map<string, AbortController> {
      return this.lifecycle.runningAbortControllers;
   }

   private transition(
      id: string,
      next: MaterializationStatus,
      extra?: Omit<MaterializationUpdate, "status">,
   ): Promise<Materialization> {
      return this.lifecycle.transition(id, next, extra);
   }

   private runInBackground(
      id: string,
      run: (signal: AbortSignal) => Promise<void>,
      // Called once the run has settled and its final status is recorded.
      onSettled?: () => void,
   ): void {
      this.lifecycle.runInBackground(id, run, onSettled);
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
      options?: { limit?: number; offset?: number; versionId?: VersionRef },
   ): Promise<Materialization[]> {
      const versionId = options?.versionId ?? VersionRef.LATEST;
      const environmentId = await this.resolveEnvironmentId(environmentName);
      // Naming no version of a package that has no `latest` yet (explicit
      // promotion, before the first move) lists every run, as before.
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
      await environment.publishRefusedConfiguredPackage?.(
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
          * What a strict build does with a source whose only build recomputes
          * a stored upstream from the warehouse (#6756). Orchestrated-only.
          */
         warehouseReach?: WarehouseReach;
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
         versionId?: VersionRef;
      } = {},
   ): Promise<Materialization> {
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
         // Validate against the plan the run will build from: a connection
         // edit re-derives it (ms2data/service#6145).
         await pkg.whenBuildPlanCurrent?.();
         // Never against a plan that failed to derive: after a failed
         // re-derivation the pre-edit plan is gone, and validating against
         // nothing would let stale ledger entries through to the build.
         if (pkg.isBuildPlanUnavailable?.()) {
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
               throw this.lifecycle.activeConflict(
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
                     this.runBuild(
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
                           referenceManifest: options.referenceManifest,
                           strictUpstreams: options.strictUpstreams,
                           warehouseReach: orchestrated
                              ? options.warehouseReach
                              : undefined,
                           trigger,
                           runContext: options.runContext ?? undefined,
                           version,
                        },
                        signal,
                     ),
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
         return this.claims
            .lockFor(environmentId, packageName)
            .runExclusive(start);
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
   private runBuild(
      ...args: Parameters<MaterializationService["buildRun"]> extends [
         RunTelemetry,
         ...infer Rest,
      ]
         ? Rest
         : never
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
      opts: {
         sourceNames: string[] | undefined;
         forceRefresh: boolean;
         reseed: boolean;
         buildInstructions: BuildInstruction[] | undefined;
         ledger: LedgerEntry[] | undefined;
         referenceManifest: ManifestReference[] | undefined;
         strictUpstreams: boolean | undefined;
         warehouseReach?: WarehouseReach;
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
      const mode: MaterializationMode = telemetry.mode;
      const startedAt = Date.now();

      try {
         // Persisted so the UI can compute a duration.
         await this.repository.updateMaterialization(id, {
            startedAt: new Date(startedAt),
         });

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

         // Refuse a `#@ persist` source that Malloy's build walk silently
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
            await this.committer.seedFromBoundManifest(
               carried,
               pkg,
               instructions,
            );
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
            // sources whose sourceEntityId is unchanged.
            const priorEntries = opts.forceRefresh
               ? {}
               : await this.getMostRecentManifestEntries(
                    environmentId,
                    packageName,
                    id,
                    reader,
                 );
            let recheck: Record<string, BuildInstruction>;
            ({
               instructions,
               carried,
               refused: derivedRefused,
               cleared: clearedTiers,
               recheck = {},
            } = this.deriveSelfInstructions(
               compiled,
               opts.sourceNames,
               priorEntries,
               incremental,
               owned !== undefined
                  ? {
                       versionSuffix: versionTableSuffix({
                          environmentName,
                          packageName,
                          version: owned,
                       }),
                    }
                  : {
                       packageSuffix: packageTableSuffix({
                          environmentName,
                          packageName,
                       }),
                    },
            ));
            // A carried table at the author's `name=` is reused only while
            // it is still marked as this package's (table_ownership.ts).
            if (Object.keys(recheck).length > 0) {
               await uncarryUnownedTables(
                  recheck,
                  carried,
                  instructions,
                  nameViewOwner(environmentName, packageName),
                  nameViewContext({ compiled, environment }),
               );
            }
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
               await this.claims
                  .lockFor(environmentId, packageName)
                  .runExclusive(() =>
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
            markedShared = true;
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
         for (const refusal of Object.values(derivedRefused)) {
            reportSourceRefused(
               buildKindOf(compiled, refusal.sourceID),
               refusal.tier === "storage" ? "storage" : "colocated",
            );
         }

         // A rollup this run may rewrite in place no longer holds what its
         // bound key check said (known limitation 29): forget it before any
         // table is written, so a run that fails part-way never leaves a
         // check bound over rows it did not read. This run's own commit binds
         // what it checks.
         pkg.forgetKeyUniqueness?.(
            instructions.map((instruction) => instruction.sourceEntityId),
         );

         const {
            entries,
            failures,
            sourcesRefused: instructedRefused,
            refusedBesideBuilt = {},
            nameViews = [],
            uniquenessChecks,
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
            orchestrated,
            opts.warehouseReach,
            // Auto-run only: Publisher owns every write to its tables, so it
            // can tell a joined table that has not changed since a check
            // passed. A host's tables are its own.
            orchestrated
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
         );

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
         try {
            await this.commitManifest(id, entries, failures, {
               forceRefresh: opts.forceRefresh,
               sourceNames: opts.sourceNames ?? null,
               mode,
               trigger: opts.trigger,
               sourcesBuilt,
               sourcesReused,
               ...(sourcesRefused > 0 ? { sourcesRefused } : {}),
               // Auto-run's refusals, plus instructed sources refused beside a
               // table an eligible sibling built (ms2data/service#7078), which
               // are not in `failures`.
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
               // The `name=` views this run pointed, for the dropper and the
               // reclaimers to find (name_view.ts).
               ...(nameViews.length > 0 ? { [NAME_VIEWS_KEY]: nameViews } : {}),
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
            });
         } catch (err) {
            // The views were pointed at this run's tables just before the
            // commit; a run that did not commit must not leave them there.
            if (nameViews.length > 0) {
               const restored = await restoreNameViews(
                  nameViews,
                  nameViewContext({ compiled, environment }),
               );
               // What became of each table a view had replaced, on the
               // failed run's record (the failure keeps its metadata), beside
               // the run's other views.
               if (restored.length > 0) {
                  await this.recordRestoredNameViews(id, nameViews, restored);
               }
            }
            throw err;
         }

         // A view that replaced a table this package's own run wrote at its
         // name: the table kept aside until this commit goes, and the older
         // entries for it are retired.
         const replacedTables = nameViews.filter(
            (v) => v.outcome === "replaced_own_table",
         );
         if (replacedTables.length > 0) {
            await dropReplacedTablesAside(
               replacedTables,
               nameViewContext({ compiled, environment }),
            );
            try {
               await this.claims.markReplacedByView(
                  environmentId,
                  packageName,
                  id,
                  replacedTables.map((v) => ({
                     physicalTableName: v.viewName,
                     connectionName: v.connectionName,
                     ...(v.storageDestinationName
                        ? { storageDestinationName: v.storageDestinationName }
                        : {}),
                  })),
               );
            } catch (err) {
               logger.warn(
                  "Could not retire the entries of a table a name= view replaced",
                  { materializationId: id, error: errMessage(err) },
               );
            }
            // Another loaded version may serve the replaced table (a shared
            // run's entry, read through the name that is now this run's
            // view): rebound, it reads its entries less the ones just
            // retired, as after a shared table's rebuild, instead of this
            // run's generation through the view. A run that marked shared
            // tables rebinds them below anyway; this run's own version is
            // its caller's to load (orchestrated) or already loaded.
            if (versionId !== undefined && !markedShared) {
               await this.committer.rebindLoadedVersions(
                  environmentName,
                  packageName,
                  versionId,
               );
            }
         }

         // Auto-run owns distribution; orchestrated leaves it to the caller
         // (manifestLocation).
         if (!orchestrated) {
            await this.autoLoadManifest(
               environment,
               packageName,
               entries,
               versionId,
               uniquenessChecks?.rollupKeys,
            );
         }
         if (markedShared && versionId !== undefined) {
            // Every other loaded version is rebound to this run too: each
            // binds only what its own definition built, and serves live what
            // this run rebuilt from a different one (see
            // Package.bindStorageServeBindings).
            await this.committer.rebindLoadedVersions(
               environmentName,
               packageName,
               versionId,
            );
         }

         // Carried forward, so never in the build loop: counted once the run
         // has committed them. A host's reference to an address this run
         // rebuilt was counted as built.
         const kindByAddress = buildKindsByAddress(
            compiled,
            pkg.getBuildPlan(),
         );
         for (const [address, entry] of Object.entries(carried)) {
            if (failures[address] || telemetry.builtAddresses.has(address)) {
               continue;
            }
            reportSourceReused(
               kindByAddress.get(address) ??
                  (entry.origin === "preaggregate" ? "rollup" : "persist"),
               entry.storageDestinationName ? "storage" : "colocated",
            );
         }
         // A run that lost sources is partial: its manifest is missing tables a
         // consumer expected. An auto-run refusal is not a loss: it is a property
         // of the model, and counting it would make every run partial.
         const partial = sourcesFailed > 0;
         this.lifecycle.recordRun(
            mode,
            partial ? "partial" : "success",
            startedAt,
         );
         telemetry.finished(partial ? "partial" : "success", durationMs);
      } catch (err) {
         const outcome = outcomeFor(err, signal);
         this.lifecycle.recordRun(mode, outcome, startedAt);
         telemetry.finished(outcome, Date.now() - startedAt, err);
         markRunFailureReported(err);
         if (markedShared && opts.version !== null) {
            // The run may have rebuilt some shared tables before it failed.
            // Its record stays, so rebinding now leaves those tables unbound
            // for every loaded version, as a restart would.
            await this.committer.rebindLoadedVersions(
               environmentName,
               packageName,
               opts.version,
            );
         }
         throw err;
      }
   }

   /**
    * Best-effort: record on a run whose commit failed what became of each
    * table one of its `name=` views had replaced ({@link restoreNameViews}).
    */
   private async recordRestoredNameViews(
      id: string,
      nameViews: NameViewRecord[],
      restored: NameViewRecord[],
   ): Promise<void> {
      // Each replacement's record gives way to what became of it; the run's
      // other views keep theirs.
      const pending = [...restored];
      const records = nameViews.map((record) =>
         record.outcome === "replaced_own_table"
            ? (pending.shift() ?? record)
            : record,
      );
      try {
         const current = await this.repository.getMaterializationById(id);
         await this.repository.updateMaterialization(id, {
            metadata: {
               ...(current?.metadata ?? {}),
               [NAME_VIEWS_KEY]: records,
            },
         });
      } catch (err) {
         logger.warn(
            "Could not record the name= tables a failed run put back",
            {
               materializationId: id,
               error: errMessage(err),
            },
         );
      }
   }

   // ==================== BUILD SEAMS ====================

   /** See {@link deriveSelfInstructions} (run_instructions.ts). */
   private deriveSelfInstructions(
      ...args: Parameters<typeof deriveSelfInstructions>
   ): ReturnType<typeof deriveSelfInstructions> {
      return deriveSelfInstructions(...args);
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

   /** See {@link InstructedBuild.run}. */
   private executeInstructedBuild(
      ...args: Parameters<InstructedBuild["run"]>
   ): ReturnType<InstructedBuild["run"]> {
      return new InstructedBuild({
         buildOneSource: (...a) => this.buildOneSource(...a),
         reclaimStorageTablesFromFailedRun: (...a) =>
            this.reclaimStorageTablesFromFailedRun(...a),
         packageRuns: async (environmentName, packageName) =>
            (await this.repository.listMaterializations(
               await this.resolveEnvironmentId(environmentName),
               packageName,
            )) ?? [],
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
      }).run(...args);
   }

   /** See {@link SourceBuilder.build}. */
   private buildOneSource(
      ...args: Parameters<SourceBuilder["build"]>
   ): ReturnType<SourceBuilder["build"]> {
      return new SourceBuilder((request) =>
         this.buildOneSourceIntoStorage(request),
      ).build(...args);
   }

   /** See {@link StorageBuildStrategy.build}. */
   private buildOneSourceIntoStorage(
      params: StorageBuildRequest,
   ): Promise<ManifestEntry> {
      return this.storageStrategy().build(params);
   }

   /** See {@link StorageBuildStrategy.buildDownstreamViaParents}. */
   private buildDownstreamViaParents(
      ...args: Parameters<StorageBuildStrategy["buildDownstreamViaParents"]>
   ): ReturnType<StorageBuildStrategy["buildDownstreamViaParents"]> {
      return this.storageStrategy().buildDownstreamViaParents(...args);
   }

   /**
    * Stateless, so made per call: a build reached through the prototype alone
    * (a spec that never constructs the service) still finds it.
    */
   private storageStrategy(): StorageBuildStrategy {
      return new StorageBuildStrategy({
         buildDownstreamViaParents: (...a) =>
            this.buildDownstreamViaParents(...a),
      });
   }

   /** See {@link Reclaimer.reclaimStorageTablesFromFailedRun}. */
   private reclaimStorageTablesFromFailedRun(
      builtThisRun: ManifestEntry[],
      environment: BuildEnvironment,
      owner: { environmentId: string; packageName: string },
   ): Promise<void> {
      return this.reclaimer.reclaimStorageTablesFromFailedRun(
         builtThisRun,
         environment,
         owner,
      );
   }

   // ==================== MANIFEST SEAMS ====================

   /** See {@link ManifestCommitter.commit}. */
   private commitManifest(
      ...args: Parameters<ManifestCommitter["commit"]>
   ): Promise<void> {
      return this.committer.commit(...args);
   }

   /** See {@link ManifestCommitter.autoLoad}. */
   private autoLoadManifest(
      ...args: Parameters<ManifestCommitter["autoLoad"]>
   ): Promise<void> {
      return this.committer.autoLoad(...args);
   }

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
            { packageName, error: errMessage(err) },
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

   /** See {@link ManifestCommitter.mostRecentEntries}. */
   private getMostRecentManifestEntries(
      ...args: Parameters<ManifestCommitter["mostRecentEntries"]>
   ): Promise<Record<string, ManifestEntry>> {
      return this.committer.mostRecentEntries(...args);
   }

   // ==================== CANCELLATION ====================

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
      options: { dropTables?: boolean; versionId?: VersionRef } = {},
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

      // The record is part of what proves a recorded uniqueness pass still
      // holds: once it goes, an older run's pass could read as the newest
      // over a table this run rewrote. Forgotten first, so a failure leaves
      // the record (and the proof) in place.
      await this.forgetUniquenessPasses(m.environmentId, packageName, id);
      await this.repository.deleteMaterialization(id);

      // The deleted run may have been the one serving (and its tables dropped);
      // routing has no run-time fallback, so rebind to the next-latest
      // generation, or clear to serve live.
      // (Mirrors the load-time Environment.rebindServeBindingsFromLocalStore;
      // best-effort: a failure leaves the bindings for a later reload or build.)
      await this.committer.rebindLoadedVersions(environmentName, packageName);
   }

   // ==================== RECLAIM ====================

   /** See {@link Reclaimer.dropMaterializedTables}. */
   private dropMaterializedTables(
      environmentName: string,
      packageName: string,
      m: Materialization,
   ): Promise<void> {
      return this.reclaimer.dropMaterializedTables(
         environmentName,
         packageName,
         m,
      );
   }

   /** See {@link Reclaimer.dropManifestEntries}. */
   private dropManifestEntries(
      ...args: Parameters<Reclaimer["dropManifestEntries"]>
   ): Promise<ManifestEntry[]> {
      return this.reclaimer.dropManifestEntries(...args);
   }

   /** See {@link Reclaimer.reclaimVersionTables}. */
   reclaimVersionTables(
      environmentName: string,
      packageName: string,
      versionId: string,
   ): Promise<void> {
      return this.reclaimer.reclaimVersionTables(
         environmentName,
         packageName,
         versionId,
      );
   }

   /** See {@link Reclaimer.reclaimOwedNow}. */
   reclaimOwedNow(
      environment: MaterializationEnvironment,
      packageName: string,
      versionId: string,
   ): Promise<void> {
      return this.reclaimer.reclaimOwedNow(environment, packageName, versionId);
   }

   /** See {@link Reclaimer.cleanUpDeletedVersion}. */
   cleanUpDeletedVersion(
      environmentName: string,
      packageName: string,
      versionId: string,
   ): Promise<void> {
      return this.reclaimer.cleanUpDeletedVersion(
         environmentName,
         packageName,
         versionId,
      );
   }

   /** See {@link Reclaimer.reclaimArchivedVersions}. */
   reclaimArchivedVersions(): Promise<void> {
      return this.reclaimer.reclaimArchivedVersions();
   }

   private resolveEnvironmentId(environmentName: string): Promise<string> {
      return resolveEnvironmentId(this.repository, environmentName);
   }
}
