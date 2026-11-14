// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { GivenValue } from "@malloydata/malloy";
import { recordAnnotationFindings } from "../materialization_metrics";
import { type CompiledDocument } from "./compile_document";
import { isUnderPublicDir, reportWritePathProblem } from "./report";
import {
   lazyCounter,
   resetInstrumentsForTesting,
   withSpan,
} from "../telemetry";
import { measurePackageOperation } from "../package_load_metrics";
import { Mutex } from "async-mutex";
import crypto from "crypto";
import * as fs from "fs";
import * as path from "path";
import { components } from "../api";
import {
   INDEX_MODEL_NAME,
   normalizeModelPath,
   README_NAME,
   PACKAGE_INSTALL_RECORDS_DIR,
   MAX_CONCURRENT_DRY_RUNS,
} from "../constants";
import {
   BadRequestError,
   ConnectionNotFoundError,
   DestinationNotFoundError,
   EnvironmentNotFoundError,
   InvalidModelPathError,
   PackageNotFoundError,
   PackageVersionError,
   ServiceUnavailableError,
   WriteRollbackFailedError,
   WriteRolledBackError,
   WriteVerifyError,
   DryRunCapacityError,
   PackageAdmissionRefusedError,
} from "../errors";
import { getPersistStorageMode, isPackageInlineFieldsEnabled } from "../config";
import { logger } from "../logger";
import { redactPgSecrets } from "../pg_helpers";
import {
   mergeConnectionUpdate,
   toPublicConnections,
} from "./connection_public_view";
import { recordManifestBind } from "../materialization_metrics";
import {
   assertSafeEnvironmentPath,
   assertSafePackageName,
   assertSafeRelativeModelPath,
   safeJoinUnderRoot,
} from "../path_safety";
import {
   FreshnessManifest,
   ManifestEntry,
} from "./materialization/domain/types";
import { errMessage } from "../utils";
import {
   buildEnvironmentMalloyConfig,
   deleteDuckLakeConnectionFile,
   EnvironmentMalloyConfig,
   InternalConnection,
} from "./connection";
import {
   getStaticConnectionAttributes,
   storageDestinationRoot,
   processStorageDestinations,
   processStorageDestinationsOrThrow,
   storageDestinationsEqual,
} from "./connection_config";
import {
   fetchManifestEntries,
   splitManifestEntries,
   type FetchedManifest,
} from "./materialization/run/manifest_loader";
import { ApiConnection } from "./model";
import { Package } from "./package";
import { applyLoadGates } from "./package_load_gates";
import { resolvePatchedSettings } from "./package_patch";
import { environmentResource, packageResource } from "../api_resource";
import { parsePackageName, type PackageName, VersionRef } from "./identity";
import {
   PackageLifecycle,
   type PackageInfo,
   type PackageLifecycleEvent,
   PackageLoadTracker,
   type StaleCompileError,
} from "./package_lifecycle";
import type { PackageMemoryGovernor } from "./package_memory_governor";
import {
   compileSource,
   type CompileScope,
   type TaggedLogMessage,
} from "./compile_source";
import { type OverlayFile } from "./request_overlay";
import { destinationSchemaOf } from "./materialization/domain/storage_table_path";

export {
   COMPILE_SCOPES,
   packageRelativeModelPath,
   type CompileScope,
   type TaggedLogMessage,
} from "./compile_source";
import {
   backoffAfter,
   type ConfiguredPublishRetry,
} from "./configured_publish_retry";
import { VersionRegistry, VersionService } from "./versions/version_service";
import { getPackageLoadWorkerCount } from "../package_load/package_load_pool";
import { readManifestVersion, VersionStore } from "./versions/version_store";
import {
   ownedVersionOf,
   ServingReader,
   servingReaderOf,
} from "./versions/materialization_scope";

/**
 * Sibling dirs under `environmentPath` that keep long downloads off the
 * per-package mutex:
 *
 *  - `.staging/<pkg>-<uuid>/` — a download in progress, renamed to the
 *    canonical path under the lock once complete.
 *  - `.retired/<pkg>-<uuid>/` — the previous canonical tree, renamed aside
 *    during a swap or delete and removed after the lock is released.
 *
 * Dot-prefixed so anything that walks an environment for packages never finds
 * a half-downloaded or superseded copy.
 */
export const STAGING_DIR_NAME = ".staging";
const RETIRED_DIR_NAME = ".retired";

// Binding happens before a package is SERVING, so a slow manifest store must
// not block it: past this, the package serves live.
const MANIFEST_FETCH_TIMEOUT_MS = 15_000;
// A version's rebind that answers 503 (its compile workers are busy) is tried
// this many times, waiting this long times the attempt between tries.
const REBIND_ATTEMPTS = 3;
const REBIND_RETRY_MS = 500;

type ApiPackage = components["schemas"]["Package"];
type ApiPackageStatus = components["schemas"]["PackageStatus"];
type ApiPackageStatusEntry = components["schemas"]["PackageStatusEntry"];
type ApiPackageServingState = components["schemas"]["PackageServingState"];
type ApiEnvironment = components["schemas"]["Environment"];
type RetiredConnectionGeneration = {
   label: string;
   releaseConnections: () => Promise<void>;
   timer?: ReturnType<typeof setTimeout>;
};

const RETIRED_CONNECTION_DRAIN_MS = 30_000;

/**
 * The only storage-destination fields any read reports, so a reader never sees
 * warehouse credentials. Also identifies a write entry that only references a
 * stored destination (see {@link Environment.setStorageDestinations}).
 */
const DESTINATION_READ_FIELDS: ReadonlySet<string> = new Set([
   "name",
   "type",
   "resource",
]);

const getQueryAdmissionRejectionsCounter = lazyCounter(
   "publisher_query_admission_rejections_total",
   "Queries rejected with 503 because Environment.assertCanAdmitQuery() observed memory back-pressure",
);
const getPackageAdmissionRejectionsCounter = lazyCounter(
   "publisher_package_admission_rejections_total",
   "Package loads rejected with 503 because Environment.assertCanAdmitNewPackage() observed memory back-pressure",
);

/**
 * Visible for tests; production code never calls this. Resets the
 * lazy caches so a fresh MeterProvider can capture future writes.
 */
export function resetAdmissionTelemetryForTesting(): void {
   resetInstrumentsForTesting();
}

/**
 * Judges a metadata PATCH under the package lock: `body` against `target`,
 * the metadata of the tree it is applied to, and `previous`, that of the
 * package an install replaces (absent for a metadata update, and when nothing
 * was resident). Returns the body to apply, or throws to refuse it.
 */
export type AdmitUpdate = (
   body: ApiPackage,
   target: ApiPackage,
   previous: ApiPackage | undefined,
) => ApiPackage;

/** The publisher.json fields a metadata write may set. */
type PackageManifestUpdate = {
   name: string;
   description?: string;
   location?: string;
   explores?: string[];
   queryableSources?: "declared" | "all";
   manifestLocation?: string | null;
   scope?: ApiPackage["scope"];
   queryMetadata?: ApiPackage["queryMetadata"];
   materialization?: ApiPackage["materialization"];
};

/**
 * The publisher.json to write: the manifest on disk with `metadata` applied,
 * preserving what the caller did not send.
 */
function mergePackageManifest(
   existingManifest: Record<string, unknown>,
   metadata: PackageManifestUpdate,
): Record<string, unknown> {
   const onDiskMaterialization =
      existingManifest.materialization !== null &&
      typeof existingManifest.materialization === "object" &&
      !Array.isArray(existingManifest.materialization)
         ? (existingManifest.materialization as Record<string, unknown>)
         : undefined;

   // Scope has two homes, `materialization.scope` (canonical) and the root
   // (deprecated), and both are written in sync: root-only would conflict
   // with an existing envelope (see resolvePackageScope), and envelope-only
   // would make an older publisher default a `version` package to `package`.
   // The wire materialization block has no `scope`, so an on-disk envelope
   // value survives a materialization PATCH.
   const resolvedScope =
      metadata.scope ??
      (onDiskMaterialization?.scope as ApiPackage["scope"] | undefined) ??
      (existingManifest.scope as ApiPackage["scope"] | undefined);

   // A materialization PATCH replaces the block wholesale (schedule and
   // freshness are the policy being set), but `queryMetadata` is orthogonal:
   // dropping it on omission would silently untag every build statement.
   // Null preserves too, at both wire homes, since some clients serialize
   // unset fields as null; the clear is an empty bag.
   const preservedQueryMetadata =
      metadata.materialization !== undefined &&
      metadata.materialization?.queryMetadata == null &&
      onDiskMaterialization?.queryMetadata !== undefined
         ? { queryMetadata: onDiskMaterialization.queryMetadata }
         : {};

   const materializationBase: Record<string, unknown> | undefined =
      metadata.materialization !== undefined
         ? { ...metadata.materialization, ...preservedQueryMetadata }
         : onDiskMaterialization !== undefined
           ? { ...onDiskMaterialization }
           : undefined;
   const materializationBlock =
      resolvedScope !== undefined
         ? { ...(materializationBase ?? {}), scope: resolvedScope }
         : materializationBase;

   // `queryMetadata` also has two homes, the other way round: the root is
   // canonical, the envelope deprecated, and both are written so an older
   // publisher reading only the envelope still tags. Precedence matches the
   // manifest resolver: top-level field, then the block, then disk. Null
   // means "not provided" throughout; the clear is an empty bag.
   const resolvedQueryMetadata =
      metadata.queryMetadata ??
      (materializationBlock as { queryMetadata?: unknown } | undefined)
         ?.queryMetadata ??
      (existingManifest as { queryMetadata?: unknown }).queryMetadata;

   // Fields are overwritten only when provided. A convention-derived surface
   // is never written: a read-modify-write PATCH echoes the `index.malloy`
   // surface a GET derived, and freezing it into `explores` would leave the
   // package serving nothing once that file is renamed. Recognized as the
   // exact derived value in a manifest that declares no `explores`.
   const echoesDerivedSurface =
      existingManifest.explores === undefined &&
      Array.isArray(metadata.explores) &&
      metadata.explores.length === 1 &&
      metadata.explores[0] === INDEX_MODEL_NAME;

   return {
      ...existingManifest,
      name: metadata.name,
      // An undefined would be dropped by JSON.stringify, erasing it on disk.
      ...(metadata.description !== undefined
         ? { description: metadata.description }
         : {}),
      ...(metadata.explores !== undefined && !echoesDerivedSurface
         ? { explores: metadata.explores }
         : {}),
      ...(metadata.queryableSources !== undefined
         ? { queryableSources: metadata.queryableSources }
         : {}),
      ...(metadata.manifestLocation !== undefined
         ? { manifestLocation: metadata.manifestLocation }
         : {}),
      ...(resolvedScope !== undefined ? { scope: resolvedScope } : {}),
      ...(resolvedQueryMetadata !== undefined
         ? { queryMetadata: resolvedQueryMetadata }
         : {}),
      // Mirrored into the deprecated home only when it already exists or the
      // caller wrote to it, so a manifest never gains the block being
      // migrated away from.
      ...(materializationBlock !== undefined
         ? {
              materialization: {
                 ...materializationBlock,
                 ...(resolvedQueryMetadata !== undefined
                    ? { queryMetadata: resolvedQueryMetadata }
                    : {}),
              },
           }
         : {}),
   };
}

/**
 * How a package listing is built. `includeLoading` lists a first-time load as
 * a bare entry; `operational` builds the `/status` entries: what a reconcile
 * reads (`PackageStatusEntry`). While PUBLISHER_PACKAGE_INLINE_FIELDS is on an
 * entry is the package's full response plus its serving state.
 */
export type PackageListingOptions = {
   includeLoading?: boolean;
   operational?: boolean;
};

/**
 * At most this many build plans re-derive at once after a connection edit:
 * two, and never more than the load pool has workers, so with the default
 * single worker they run one at a time (each at background priority, behind
 * publishes and reloads; see LoadPackageJob.background).
 */
function planRefreshConcurrency(): number {
   return Math.max(1, Math.min(2, getPackageLoadWorkerCount()));
}

/**
 * The connection names an edit changed: added, removed, or with a different
 * digest-relevant configuration. A connection that carries a `fingerprint` is
 * digested by that fingerprint alone (applyConnectionFingerprint in
 * connection.ts), so when both sides carry one only the fingerprints are
 * compared: rotating a secret behind a stable fingerprint moves no address and
 * re-derives nothing. Without a fingerprint the stored config is compared
 * whole (secrets included), since a connector's own digest may read any of it.
 *
 * This assumes the host changes a connection's fingerprint whenever a setting
 * changes what its builds compile to (its project, database or default
 * schema, which decide how Malloy resolves a table name): such a change moves
 * the build SQL, and so the address, under a stable fingerprint, and a
 * fingerprint-only comparison would not re-derive it.
 */
export function changedConnectionNames(
   before: readonly ApiConnection[],
   after: readonly ApiConnection[],
): Set<string> {
   const digestInput = (c: ApiConnection): string =>
      c.fingerprint ? `fingerprint:${c.fingerprint}` : JSON.stringify(c);
   const byName = (list: readonly ApiConnection[]) =>
      new Map(list.map((c) => [c.name ?? "", c]));
   const previous = byName(before);
   const next = byName(after);
   const changed = new Set<string>();
   for (const [name, config] of next) {
      const prior = previous.get(name);
      if (!prior) {
         changed.add(name);
      } else if (prior.fingerprint && config.fingerprint) {
         if (prior.fingerprint !== config.fingerprint) changed.add(name);
      } else if (digestInput(prior) !== digestInput(config)) {
         changed.add(name);
      }
   }
   for (const name of previous.keys()) {
      if (!next.has(name)) changed.add(name);
   }
   changed.delete("");
   return changed;
}

export class Environment {
   private packages: Map<string, Package> = new Map();
   /**
    * Packages whose unversioned files a first versioned publish has moved
    * aside while it compiles the version (see holdingUnversionedReads):
    * getPackage's lock-free fast path does not serve them until it settles.
    */
   private unversionedReadsHeld = new Set<string>();
   /** See {@link setConfiguredPublishRetry}. */
   private readonly configuredPublishRetries = new Map<
      string,
      ConfiguredPublishRetry
   >();
   /** A retry in flight, per package, shared by the callers waiting on it. */
   private readonly configuredPublishRetriesRunning = new Map<
      string,
      Promise<void>
   >();
   /**
    * The version rules for this environment's packages, bound by the
    * EnvironmentStore once the environment has a registry row. Versioned
    * packages never enter `packages`: their loaded versions live in this
    * service's cache, keyed by (package, version).
    */
   private versionService: VersionService<Package> | null = null;
   // Lock ordering: connectionMutex (environment) MUST be acquired before any
   // packageMutex, since connection updates can force package reloads. Never
   // acquire connectionMutex while holding a packageMutex (AB/BA deadlock).
   private packageMutexes = new Map<string, Mutex>();
   /**
    * Each package's phase and the problems it carries (load and mount
    * failures, staleness), changed only through {@link transition}.
    */
   private readonly lifecycle: PackageLifecycle;
   /**
    * Loads, reinstalls and recompiles in flight: what `Package.status.loading`
    * reports, and what {@link awaitPackageLoads} waits on. Independent of the
    * lifecycle phase, because a reload runs while the previous copy serves.
    */
   private readonly loadsInFlight = new PackageLoadTracker();
   /** Bumped by every connection edit that can move content addresses. */
   private connectionGeneration = 0;
   /** The connections the latest edit changed, for the refresh's log lines. */
   private lastChangedConnections: string[] = [];
   /** Packages waiting to re-derive their build plan (see schedulePlanRefresh). */
   private readonly planRefreshQueue = new Set<Package>();
   /** Re-derivations running now, bounded by planRefreshConcurrency(). */
   private planRefreshesRunning = 0;
   /** Queued packages whose plan a failed re-derivation left unavailable. */
   private readonly planRefreshRetrying = new Set<Package>();
   /**
    * Per package, failed re-derivations in a row and the timer of the next
    * retry (backoffAfter: 2s doubling to 60s). Cleared when one succeeds.
    */
   private readonly planRefreshBackoff = new Map<
      Package,
      { failures: number; timer?: ReturnType<typeof setTimeout> }
   >();
   /**
    * Set once this environment is closed (deleted, or the server is shutting
    * down): no re-derivation is queued or retried after it, so a timer cannot
    * fire against a package whose files are gone.
    */
   private planRefreshesClosed = false;
   /**
    * Every environment not yet closed, for {@link cancelAllPlanRefreshes}.
    * Weak, so an environment nobody closes (a test's) can still be collected.
    */
   private static readonly openEnvironments = new Set<WeakRef<Environment>>();
   private readonly openEnvironmentRef = new WeakRef<Environment>(this);
   private malloyConfig: EnvironmentMalloyConfig;
   private connectionMutex = new Mutex();
   private retiredConnectionGenerations =
      new Set<RetiredConnectionGeneration>();
   private apiConnections: ApiConnection[];
   /**
    * Warehouses materialization builds write to and serve from. A separate
    * list, not a flag on {@link apiConnections}, so every connection consumer
    * excludes destinations structurally: they are unresolvable from a model
    * and may share a name with a connection.
    */
   private destinations: ApiConnection[];
   /**
    * Live Malloy connections for {@link destinations}, outside the namespace
    * packages compile against. Rebuilt (retiring old handles) whenever the
    * list is replaced. Always assigned: the constructor calls
    * `setStorageDestinations`, which builds an unbuilt config even for an
    * unchanged empty list.
    */
   private destinationMalloyConfig!: EnvironmentMalloyConfig;
   /**
    * False when a load could not read the stored destinations: the list is
    * then unknown, not empty, and the database sync must not prune rows
    * against it. An explicit set makes it authoritative again.
    */
   private destinationsAuthoritative = true;
   /**
    * The connection counterpart of {@link destinationsAuthoritative}: false
    * when the list is a fallback (the config standing in for a missing
    * directory, or a reload from config) or torn down, so the database sync
    * must not prune stored connections against it. Only a wholesale
    * replacement of the list (an environment create or PATCH carrying
    * `connections`, or a restore from the stored rows) makes it authoritative
    * again. Adding, updating or deleting one connection edits one entry of
    * whatever list is in memory, so it leaves the flag as it was: a fallback
    * list with one connection added is still a fallback.
    */
   private connectionsAuthoritative = true;
   private environmentPath: string;
   private environmentName: string;
   // Resolves a package's latest persisted manifest entries (both tiers), so
   // serve routing is re-established whenever a package (re)loads, not only
   // after a build. Undefined ⇒ no re-bind on load. See
   // {@link rebindServeBindingsFromLocalStore}.
   //
   // `reader` says who reads the runs: a published version (only its own runs
   // under `scope: version`) or the unversioned slot (see ServingReader).
   private storageBindingResolver?: (
      packageName: string,
      reader: ServingReader,
   ) => Promise<Record<string, ManifestEntry>>;
   // Whether an UNVERSIONED package restores its serve bindings from the local
   // store when it loads. Only an environment reloaded from the database at
   // startup does, exactly as before versions existed; one created at runtime
   // (over the API, or from config on its first boot) serves an unversioned
   // package live until a build binds it. A published version always restores
   // its own: the resolver is installed for every environment for them.
   private restoreUnversionedFromLocalStore = false;
   // Materializations running now, per `package@version`: a version is never
   // archived while one builds it (see registerVersionBuild).
   private readonly versionBuilds = new Map<string, number>();
   // What each loaded version last bound from the store, so a settle rebinds
   // only when the store moved since (see settleSharedBindings).
   private readonly storeBindingReads = new WeakMap<Package, string>();
   // The loaded versions already settled once they became findable.
   private readonly settledVersions = new WeakSet<Package>();
   // Called when a version is archived, to reclaim the tables it alone owns.
   // Set by EnvironmentStore (see setVersionArchivedHook).
   private versionArchivedHook:
      | ((packageName: string, versionId: string) => void)
      | null = null;
   // Called once a version's row is deleted, to stop its builds and reclaim
   // the tables it alone owns. Set by EnvironmentStore (see
   // setVersionDeletedHook).
   private versionDeletedHook:
      | ((packageName: string, versionId: string) => void)
      | null = null;
   // Called before a publish creates a version's row, to reclaim what a
   // deleted version of that id still owes. Set by EnvironmentStore (see
   // setOwedReclaimHook).
   private owedReclaimHook:
      | ((packageName: string, versionId: string) => Promise<void>)
      | null = null;
   public metadata: ApiEnvironment;
   // Owner of the RSS back-pressure flag; null makes the admission gates no-ops.
   private memoryGovernor: PackageMemoryGovernor | null = null;
   // Called with each package the moment it enters `this.packages`. Set by
   // EnvironmentStore (see setPackageLoadedHook); null means nobody listens.
   private packageLoadedHook: ((pkg: Package) => void) | null = null;

   /** Absolute path on disk where this environment's package files live. */
   public getEnvironmentPath(): string {
      return this.environmentPath;
   }

   /** This environment's name (the canonical key used by the API/service). */
   public getEnvironmentName(): string {
      return this.environmentName;
   }

   constructor(
      environmentName: string,
      environmentPath: string,
      malloyConfig: EnvironmentMalloyConfig,
      apiConnections: InternalConnection[],
      storageDestinations: ApiConnection[] = [],
   ) {
      // Sanitizer barrier for every downstream `path.join(this.environmentPath, …)`.
      assertSafeEnvironmentPath(environmentPath);
      this.environmentName = environmentName;
      this.environmentPath = environmentPath;
      this.lifecycle = new PackageLifecycle(environmentName);
      Environment.openEnvironments.add(this.openEnvironmentRef);
      this.malloyConfig = malloyConfig;
      this.apiConnections = apiConnections;
      this.destinations = [];
      this.setStorageDestinations(storageDestinations);
      this.metadata = {
         resource: environmentResource(this.environmentName),
         name: this.environmentName,
         location: this.environmentPath,
      };
      void this.reloadEnvironmentMetadata();
   }

   private async writeEnvironmentReadme(readme?: string): Promise<void> {
      if (readme === undefined) return;

      const readmePath = path.join(this.environmentPath, "README.md");

      try {
         await fs.promises.writeFile(readmePath, readme, "utf-8");
         logger.info(
            `Updated README.md for environment ${this.environmentName}`,
         );
      } catch (err) {
         logger.error(`Failed to write README.md`, { error: err });
         throw new Error(`Failed to update environment README`, { cause: err });
      }
   }

   public async update(payload: ApiEnvironment) {
      // First, so an unreadable destination list is refused before anything
      // changes. Absent leaves the list alone; an unreadable shape is
      // rejected rather than read as "keep none"; an explicit [] clears.
      if (payload.storageDestinations !== undefined) {
         this.setStorageDestinations(payload.storageDestinations, {
            rejectInvalid: true,
         });
      }

      if (payload.readme !== undefined) {
         this.metadata.readme = payload.readme;
         await this.writeEnvironmentReadme(payload.readme);
      }

      if (payload.connections) {
         const payloadConnections = payload.connections;
         await this.runConnectionUpdateExclusive(async () => {
            logger.info(
               `Updating ${payloadConnections.length} connections for environment ${this.environmentName}`,
            );
            // The list replaces the stored one wholesale, but reads carry no
            // credentials, so an echoed entry arrives without them. Merging
            // against the stored entry keeps one connection edit in the app
            // from stripping every other connection's credentials.
            const mergedConnections = payloadConnections.map((incoming) => {
               const stored = this.apiConnections.find(
                  (existing) => existing.name === incoming.name,
               );
               return stored
                  ? mergeConnectionUpdate(stored, incoming)
                  : incoming;
            });
            const isUpdateConnectionRequest = true;
            const nextMalloyConfig = buildEnvironmentMalloyConfig(
               mergedConnections,
               this.environmentPath,
               isUpdateConnectionRequest,
            );

            this.updateConnections(nextMalloyConfig);
            // A wholesale replacement: the list is now the desired state.
            this.connectionsAuthoritative = true;

            logger.info(
               `Successfully updated connections for environment ${this.environmentName}`,
               {
                  apiConnections: this.apiConnections.length,
                  internalConnections: this.apiConnections.length,
               },
            );
         });
      }

      return this;
   }

   static async create(
      environmentName: string,
      environmentPath: string,
      connections: ApiConnection[],
      storageDestinations: ApiConnection[] = [],
   ): Promise<Environment> {
      assertSafeEnvironmentPath(environmentPath);
      if (!(await fs.promises.stat(environmentPath))?.isDirectory()) {
         throw new EnvironmentNotFoundError(
            `Environment path ${environmentPath} not found`,
         );
      }

      logger.info(`Creating environment with connection configuration`);
      const malloyConfig = buildEnvironmentMalloyConfig(
         connections,
         environmentPath,
      );

      logger.info(
         `Loaded ${malloyConfig.apiConnections.length} connections for environment ${environmentName}`,
         {
            connections: malloyConfig.apiConnections.map((c) => ({
               name: c.name,
               type: c.type,
            })),
         },
      );

      const environment = new Environment(
         environmentName,
         environmentPath,
         malloyConfig,
         malloyConfig.apiConnections,
         storageDestinations,
      );

      // Best-effort cleanup of orphans from a crashed install or delete. Takes
      // the validated argument, not `this`, which CodeQL treats as tainted.
      await Environment.sweepStaleInstallDirs(environmentPath);

      return environment;
   }

   public async reloadEnvironmentMetadata(): Promise<ApiEnvironment> {
      let readme = "";
      try {
         readme = (
            await fs.promises.readFile(
               safeJoinUnderRoot(this.environmentPath, README_NAME),
            )
         ).toString();
      } catch {
         // No readme.
      }
      this.metadata = {
         ...this.metadata,
         resource: environmentResource(this.environmentName),
         name: this.environmentName,
         readme,
      };
      return this.metadata;
   }

   public async compileSource(
      packageName: string,
      modelName: string,
      source: string | undefined,
      includeSql: boolean = false,
      givens?: Record<string, GivenValue>,
      scope: CompileScope = "append",
      versionId: VersionRef = VersionRef.LATEST,
      overlay?: readonly OverlayFile[],
   ): Promise<{
      problems: TaggedLogMessage[];
      sql?: string;
      document?: CompiledDocument;
   }> {
      return compileSource(
         {
            environmentName: this.environmentName,
            environmentPath: this.environmentPath,
            withPackageLock: (name, fn) => this.withPackageLock(name, fn),
            loadPackageLocked: (name) => this._loadOrGetPackageLocked(name),
            // An unversioned package compiles in its single slot; one with
            // published versions in the version the request names (`latest`
            // when none), resolved through the registry.
            versionedPackage: async (name, version, underLock) => {
               if (!this.versionService) return undefined;
               // See getPackage: a refused configured package is published
               // again before it compiles, outside the lock its first version
               // takes.
               // Only the default resolution: a request naming a version
               // that does not exist answers 404 without publishing.
               if (
                  !underLock &&
                  version.requested() === undefined &&
                  !this.packages.has(name) &&
                  this.configuredPublishRetries.has(name)
               ) {
                  await this.retryConfiguredPublish(name);
               }
               if (
                  this.packages.has(name) &&
                  (underLock || version.requested() === undefined)
               ) {
                  return undefined;
               }
               return (
                  await this.versionService.getLoaded(name, version.requested())
               )?.loaded;
            },
         },
         packageName,
         modelName,
         source,
         includeSql,
         givens,
         scope,
         versionId,
         overlay,
      );
   }

   public listApiConnections(): ApiConnection[] {
      return this.apiConnections;
   }

   public getApiConnection(connectionName: string): ApiConnection {
      const connection = this.apiConnections.find(
         (connection) => connection.name === connectionName,
      );
      if (!connection) {
         throw new ConnectionNotFoundError(
            `Connection ${connectionName} not found`,
         );
      }
      return connection;
   }

   /**
    * Replaces the destination list, re-validating every entry however it
    * arrived. An entry carrying only the fields a read reports is resolved
    * against the stored list, so a read-modify-write keeps configs it was never
    * shown. `rejectInvalid` refuses a bad entry (a request body) instead of
    * dropping it (config or a restored row, which cannot be asked to fix it).
    */
   public setStorageDestinations(
      storageDestinations: ApiConnection[],
      { rejectInvalid = false }: { rejectInvalid?: boolean } = {},
   ): void {
      const previous = this.destinations;
      // Resolved first so a "keep this one" reference validates as the stored
      // destination it names.
      const requested = Array.isArray(storageDestinations)
         ? storageDestinations.map((destination) =>
              this.resolveDestinationReference(destination),
           )
         : storageDestinations;
      // Throws before anything is assigned.
      this.destinations = rejectInvalid
         ? processStorageDestinationsOrThrow(requested)
         : processStorageDestinations(requested);
      this.destinationsAuthoritative = true;

      // An orchestrator re-pushing its desired state on a loop must not
      // re-attach every destination each cycle, so an equal list (ignoring
      // order) is a no-op — but only once a config has been built, since the
      // constructor's call compares two empty lists.
      if (
         this.destinationMalloyConfig &&
         storageDestinationsEqual(previous, this.destinations)
      ) {
         return;
      }

      this.rebuildDestinationMalloyConfig();
      if (previous.length > 0 || this.destinations.length > 0) {
         logger.info(
            `Environment ${this.environmentName} has ${this.destinations.length} storage destination(s)`,
            { destinations: this.destinations.map((d) => d.name) },
         );
      }
   }

   /**
    * Substitutes the stored destination for a config-less entry naming one.
    * Anything else is returned untouched (an unknown reference then fails
    * validation).
    */
   private resolveDestinationReference(
      destination: ApiConnection,
   ): ApiConnection {
      if (!destination || typeof destination !== "object") {
         return destination;
      }
      const carriesOnlyReportedFields = Object.keys(destination).every(
         (field) => DESTINATION_READ_FIELDS.has(field),
      );
      if (!carriesOnlyReportedFields) {
         return destination;
      }
      return (
         this.destinations.find((stored) => stored.name === destination.name) ??
         destination
      );
   }

   /**
    * Give a loaded package the destination connections its serve shapes
    * compile against, resolved live so a list swap needs no reload. A push,
    * not a `Package.create` argument, because a model's own compile config
    * must never contain a destination. Missing it costs routing (queries go
    * live), never correctness; it must run before serve bindings are pushed.
    */
   private attachDestinationServeConfig(_package: Package): void {
      _package.setServeDestinationConfig(() =>
         this.getStorageDestinationMalloyConfig(),
      );
      // Live too: a destination configured after the load clears its
      // publish warning (P13) with no reload.
      _package.setDeploymentFacts({
         destinationNames: () =>
            this.destinationsAuthoritative
               ? this.destinations.flatMap((d) => (d.name ? [d.name] : []))
               : undefined,
         connectionCanHoldTable: (connectionName) => {
            if (!this.connectionsAuthoritative) return undefined;
            const connection = this.apiConnections.find(
               (c) => c.name === connectionName,
            );
            return connection
               ? getStaticConnectionAttributes(connection.type)?.canPersist
               : undefined;
         },
      });
      // Live, like the config: a destination list swap needs no reload.
      _package.setServeDestinationSchema((destinationName) =>
         destinationSchemaOf(
            this.destinations.find((d) => d.name === destinationName),
         ),
      );
   }

   /**
    * (Re)assemble the destination connections, draining the previous
    * generation's handles. A failure is not fatal: the environment serves on,
    * builds refuse and materialized queries go live.
    */
   private rebuildDestinationMalloyConfig(): void {
      const previous = this.destinationMalloyConfig;
      try {
         // Rooted apart from the connections' files so a destination never
         // shares a pooled DuckDB instance with a same-named connection (see
         // STORAGE_DESTINATIONS_DIR); must exist before the first lookup.
         const destinationRoot = storageDestinationRoot(this.environmentPath);
         if (this.destinations.length > 0) {
            fs.mkdirSync(destinationRoot, { recursive: true });
         }
         this.destinationMalloyConfig = buildEnvironmentMalloyConfig(
            this.destinations,
            destinationRoot,
            false,
            { origin: "storage-destination" },
         );
      } catch (error) {
         logger.error(
            `Failed to assemble storage destinations for environment ${this.environmentName}; serving without them`,
            { error },
         );
         this.destinationMalloyConfig = buildEnvironmentMalloyConfig(
            [],
            storageDestinationRoot(this.environmentPath),
            false,
            { origin: "storage-destination" },
         );
      }
      if (previous && previous !== this.destinationMalloyConfig) {
         this.retireConnectionGeneration(
            `environment ${this.environmentName} destinations`,
            () => previous.releaseConnections(),
         );
         // Memoized serve shapes hold the retired generation's connections but
         // are keyed on the binding set, which this does not change; left in
         // place, every routed query would quietly fall back to live.
         this.invalidateServeShapes();
      }
   }

   /**
    * After a connection edit, re-derive the build plan of every loaded package
    * and loaded version whose persist sources build on a changed connection
    * (see {@link Package.markBuildPlanStale}): a content address folds in its
    * connection's digest, so the edit can move it. A package whose plan does
    * not touch a changed connection is left alone; one whose plan is
    * unavailable is re-derived, since its connections are unknown.
    */
   private refreshBuildPlans(changed: ReadonlySet<string>): void {
      if (changed.size === 0) return;
      this.connectionGeneration++;
      this.lastChangedConnections = [...changed].sort();
      const loaded = [
         ...this.packages.values(),
         ...(this.versionService?.cache.entries() ?? []).map(([, , v]) => v),
      ];
      for (const _package of loaded) {
         if (Environment.planUsesAny(_package, changed)) {
            this.schedulePlanRefresh(_package);
         }
      }
   }

   private static planUsesAny(
      _package: Package,
      changed: ReadonlySet<string>,
   ): boolean {
      const names = _package.planConnectionNames();
      if (names === null) return true;
      for (const name of names) if (changed.has(name)) return true;
      return false;
   }

   /**
    * Mark `_package`'s plan stale at once, so readers wait for it, and queue
    * its re-derivation. Queued once however many edits land before it starts
    * (it reads the connections as they are when it runs), and run at most
    * PLAN_REFRESH_CONCURRENCY at a time, so one edit never recompiles every
    * package together.
    */
   private schedulePlanRefresh(
      _package: Package,
      changedConnections: Iterable<string> = this.lastChangedConnections,
   ): void {
      if (this.planRefreshesClosed) return;
      _package.markBuildPlanStale(changedConnections);
      // This edit's refresh supersedes a pending retry.
      const backoff = this.planRefreshBackoff.get(_package);
      if (backoff?.timer) clearTimeout(backoff.timer);
      if (backoff) backoff.timer = undefined;
      if (this.planRefreshQueue.has(_package)) return;
      this.planRefreshQueue.add(_package);
      this.pumpPlanRefreshes();
   }

   /**
    * After a failed re-derivation, retry it with backoff: a transient failure
    * (a load-worker timeout or crash) must not leave the package refusing runs
    * until someone reloads it. A retry does not mark the plan stale, so
    * readers get the 409 at once rather than wait; it stops once the package
    * is reloaded, replaced or deleted (it is then not a loaded copy).
    */
   private schedulePlanRetry(_package: Package): void {
      if (this.planRefreshesClosed) return;
      const backoff = this.planRefreshBackoff.get(_package) ?? { failures: 0 };
      backoff.failures++;
      if (backoff.timer) clearTimeout(backoff.timer);
      backoff.timer = setTimeout(() => {
         backoff.timer = undefined;
         if (this.planRefreshesClosed) return;
         if (!this.isLoadedCopy(_package)) {
            this.planRefreshBackoff.delete(_package);
            return;
         }
         if (!_package.isBuildPlanUnavailable()) return;
         if (this.planRefreshQueue.has(_package)) return;
         this.planRefreshRetrying.add(_package);
         this.planRefreshQueue.add(_package);
         this.pumpPlanRefreshes();
      }, backoffAfter(backoff.failures));
      backoff.timer.unref?.();
      this.planRefreshBackoff.set(_package, backoff);
   }

   /** Whether `_package` is the copy this environment serves (unversioned or a cached version). */
   private isLoadedCopy(_package: Package): boolean {
      if (this.packages.get(_package.getPackageName()) === _package)
         return true;
      return (this.versionService?.cache.entries() ?? []).some(
         ([, , version]) => version === _package,
      );
   }

   private pumpPlanRefreshes(): void {
      if (this.planRefreshesClosed) return;
      while (
         this.planRefreshesRunning < planRefreshConcurrency() &&
         this.planRefreshQueue.size > 0
      ) {
         const [next] = this.planRefreshQueue;
         this.planRefreshQueue.delete(next);
         const retryFailed = this.planRefreshRetrying.delete(next);
         this.planRefreshesRunning++;
         const connections = this.lastChangedConnections;
         next
            .refreshBuildPlan({ retryFailed })
            .then((derived) => {
               if (derived) {
                  const backoff = this.planRefreshBackoff.get(next);
                  if (backoff?.timer) clearTimeout(backoff.timer);
                  this.planRefreshBackoff.delete(next);
                  return;
               }
               if (!next.lastBuildPlanRefreshFailedTransiently()) {
                  // The plan compute itself failed (a model that no longer
                  // compiles, a connection that does not resolve): it fails
                  // the same way until the model or a connection changes, and
                  // either re-derives it, so no retry loop recompiles it.
                  const backoff = this.planRefreshBackoff.get(next);
                  if (backoff?.timer) clearTimeout(backoff.timer);
                  this.planRefreshBackoff.delete(next);
                  logger.warn(
                     "Build plan could not be re-derived after a connection change; run creation answers 409 until the next connection edit or a reload",
                     {
                        environmentName: this.environmentName,
                        packageName: next.getPackageName(),
                        versionId: next.getVersionId(),
                        changedConnections: connections,
                     },
                  );
                  return;
               }
               this.schedulePlanRetry(next);
               logger.warn(
                  "Build plan could not be re-derived after a connection change (the load pool failed); run creation answers 409 and the re-derivation is retried with backoff",
                  {
                     environmentName: this.environmentName,
                     packageName: next.getPackageName(),
                     versionId: next.getVersionId(),
                     changedConnections: connections,
                     failures: this.planRefreshBackoff.get(next)?.failures,
                  },
               );
            })
            .catch((err: unknown) => {
               // Not expected (refreshBuildPlan resolves false on failure),
               // but an unexpected throw retries the same way.
               this.schedulePlanRetry(next);
               logger.warn(
                  "Build plan re-derivation after a connection change failed",
                  {
                     environmentName: this.environmentName,
                     packageName: next.getPackageName(),
                     changedConnections: connections,
                     error: errMessage(err),
                  },
               );
            })
            .finally(() => {
               this.planRefreshesRunning--;
               this.pumpPlanRefreshes();
            });
      }
   }

   /** Drop every loaded model's memoized serve shape; the next query recompiles. */
   private invalidateServeShapes(): void {
      for (const _package of this.packages.values()) {
         _package.invalidateServeShapes();
      }
      // Loaded versions too: they compiled against the destinations that just
      // changed.
      for (const [, , version] of this.versionService?.cache.entries() ?? []) {
         version.invalidateServeShapes();
      }
   }

   /**
    * The connections a serve shape may compile against. Never the same object
    * as {@link getEnvironmentMalloyConfig}: the two resolve disjoint name sets.
    */
   public getStorageDestinationMalloyConfig() {
      return this.destinationMalloyConfig.malloyConfig;
   }

   /** The stored destinations could not be read; see {@link destinationsAuthoritative}. */
   public markStorageDestinationsUnknown(): void {
      this.destinationsAuthoritative = false;
   }

   /** See {@link destinationsAuthoritative}. */
   public hasAuthoritativeStorageDestinations(): boolean {
      return this.destinationsAuthoritative;
   }

   /** The connection list is a best guess; see {@link connectionsAuthoritative}. */
   public markConnectionsUnknown(): void {
      this.connectionsAuthoritative = false;
   }

   /** See {@link connectionsAuthoritative}. */
   public hasAuthoritativeConnections(): boolean {
      return this.connectionsAuthoritative;
   }

   /**
    * The destinations with their configs. Deliberately exposed by no
    * controller: a config carries warehouse credentials.
    */
   public listStorageDestinations(): ApiConnection[] {
      return this.destinations;
   }

   public hasStorageDestination(destinationName: string): boolean {
      return this.destinations.some(
         (destination) => destination.name === destinationName,
      );
   }

   /**
    * Resolves a storage destination by name. Never falls back to the
    * connection list: an unknown destination must fail rather than write into
    * a same-named connection, the tenant's own warehouse.
    */
   public getStorageDestination(destinationName: string): ApiConnection {
      const destination = this.destinations.find(
         (destination) => destination.name === destinationName,
      );
      if (!destination) {
         throw new DestinationNotFoundError(
            `Storage destination ${destinationName} not found`,
         );
      }
      return destination;
   }

   public async getMalloyConnection(connectionName: string) {
      return this.malloyConfig.malloyConfig.connections.lookupConnection(
         connectionName,
      );
   }

   public getEnvironmentMalloyConfig() {
      return this.malloyConfig.malloyConfig;
   }

   public async runConnectionUpdateExclusive<T>(
      fn: () => Promise<T>,
   ): Promise<T> {
      return this.connectionMutex.runExclusive(fn);
   }

   private retireConnectionGeneration(
      label: string,
      releaseConnections: () => Promise<void>,
   ): void {
      const generation: RetiredConnectionGeneration = {
         label,
         releaseConnections,
      };
      generation.timer = setTimeout(() => {
         void this.releaseRetiredConnectionGeneration(generation);
      }, RETIRED_CONNECTION_DRAIN_MS);
      (
         generation.timer as ReturnType<typeof setTimeout> & {
            unref?: () => void;
         }
      ).unref?.();
      this.retiredConnectionGenerations.add(generation);
   }

   private async releaseRetiredConnectionGeneration(
      generation: RetiredConnectionGeneration,
   ): Promise<void> {
      if (!this.retiredConnectionGenerations.delete(generation)) return;

      if (generation.timer) {
         clearTimeout(generation.timer);
      }

      try {
         await generation.releaseConnections();
      } catch (error) {
         logger.error(
            `Error releasing retired connection generation ${generation.label}`,
            { error },
         );
      }
   }

   private async releaseAllRetiredConnectionGenerations(): Promise<void> {
      await Promise.all(
         [...this.retiredConnectionGenerations].map((generation) =>
            this.releaseRetiredConnectionGeneration(generation),
         ),
      );
   }

   /**
    * Snapshot of the packages loaded in memory; never triggers a load, so the
    * materialization scheduler sweeps only what something else loaded.
    * Published versions are swept separately (VersionService.activeVersions),
    * and the scheduler loads a version when its schedule comes due.
    */
   public getLoadedPackages(): Package[] {
      return [...this.packages.values()];
   }

   /**
    * Whether a compiled copy of `name` is in memory: its single slot, or any
    * of its versions. Never loads anything.
    */
   public hasLoadedPackage(name: string): boolean {
      if (this.packages.has(name)) return true;
      return (
         (this.versionService?.cache.loadedVersionsOf(name).length ?? 0) > 0
      );
   }

   /**
    * The packages this environment holds, as the API describes them.
    *
    * A resident copy is described without its lock, so a reloading package is
    * listed with `status.loading` (its previous copy still serves) rather than
    * vanishing. A registered package that is not resident is loaded here.
    *
    * A first-time load has nothing to describe and is listed (name and
    * `status` only) only under `includeLoading`, for orchestrators; otherwise
    * a listed package always means one that can serve here.
    */
   public async listPackages(
      options: PackageListingOptions = {},
   ): Promise<ApiPackage[]> {
      logger.debug("Listing packages", {
         environmentPath: this.environmentPath,
      });
      try {
         const names = new Set<string>([
            ...this.lifecycle.registeredNames(),
            ...(options.includeLoading ? this.loadsInFlight.names() : []),
         ]);
         const packageMetadata: Array<ApiPackage | undefined> =
            await Promise.all(
               Array.from(names).map(async (packageName) => {
                  const phase = this.lifecycle.phaseOf(packageName);
                  if (phase === "unloading") {
                     return undefined;
                  }
                  try {
                     const resident = this.packages.get(packageName);
                     // A versioned package is listed as its latest; every other
                     // package from its single slot below.
                     if (
                        resident === undefined &&
                        (await this.versionService?.isVersioned(packageName))
                     ) {
                        // `/status` lists each version instead (see
                        // listVersionEntries), so it skips loading the latest.
                        if (options.operational) return undefined;
                        return await this.listedVersionedPackage(packageName);
                     }
                     // A slim `/status` entry reads only the binding, so it skips
                     // the package's metadata and the warnings computed with it.
                     const slim =
                        options.operational === true &&
                        !isPackageInlineFieldsEnabled();
                     let metadata: ApiPackage | undefined;
                     let loaded: Package | undefined;
                     if (resident !== undefined) {
                        loaded = resident;
                        if (slim) return this.statusEntry(packageName, loaded);
                        metadata = resident.getPackageMetadata();
                     } else if (this.loadsInFlight.has(packageName)) {
                        if (!options.includeLoading) {
                           return undefined;
                        }
                        metadata = { name: packageName };
                     } else if (phase === "loading") {
                        // An interrupted load: reloading it from this read path
                        // would repeat the work on every listing.
                        return undefined;
                     } else if (phase === "absent" || phase === "failed") {
                        // Listed only as a load in flight, which has since failed
                        // (one that succeeded is resident or versioned): its
                        // caller has the error, and a read never loads it.
                        return undefined;
                     } else {
                        loaded = await this.getPackage(
                           parsePackageName(packageName),
                           false,
                        );
                        if (slim) return this.statusEntry(packageName, loaded);
                        metadata = loaded.getPackageMetadata();
                     }
                     metadata.name = packageName;
                     if (
                        options.operational ||
                        isPackageInlineFieldsEnabled()
                     ) {
                        metadata.status =
                           this.describePackageStatus(packageName);
                     }
                     return metadata;
                  } catch (error) {
                     logger.error(
                        `Failed to load package: ${packageName} due to : ${error}`,
                     );
                     // Redacted because getStatus returns it in a response body,
                     // and a Postgres/DuckLake ATTACH failure carries the
                     // connection string (keyword `password=` or URI userinfo).
                     this.transition(packageName, {
                        type: "recordLoadError",
                        message: redactPgSecrets(
                           error instanceof Error
                              ? error.message
                              : String(error),
                        ),
                     });
                     return undefined;
                  }
               }),
            );
         return packageMetadata.filter(
            (metadata): metadata is ApiPackage => metadata !== undefined,
         );
      } catch (error) {
         logger.error("Error listing packages", { error });
         throw error;
      }
   }

   /**
    * Where `packageName` was installed from, recorded outside the package
    * directory (see {@link PACKAGE_INSTALL_RECORDS_DIR}).
    */
   private installRecordPath(packageName: string): string {
      return safeJoinUnderRoot(
         this.environmentPath,
         PACKAGE_INSTALL_RECORDS_DIR,
         `${packageName}.json`,
      );
   }

   /**
    * Resolve once no load, reinstall or recompile of the package is in flight,
    * for a caller that must decide against the copy that will be resident.
    */
   public async awaitPackageLoads(packageName: string): Promise<void> {
      await this.loadsInFlight.awaitSettled(packageName);
   }

   /**
    * The `Package.status` the API reports: whether a compiled copy serves, and
    * whether a load is in progress. Lock-free, so it answers during the load.
    * With `versionId`, whether that published version serves.
    */
   public describePackageStatus(
      packageName: string,
      versionId?: string | null,
   ): ApiPackageStatus {
      const loadingSince = this.loadsInFlight.since(packageName);
      // A versioned package serves through its loaded versions.
      const versions = this.versionService;
      const servingVersions = versions
         ? versions.cache.loadedVersionsOf(packageName)
         : [];
      const serving = versionId
         ? servingVersions.includes(versionId)
         : this.packages.has(packageName) || servingVersions.length > 0;
      return {
         serving,
         loading: loadingSince !== undefined,
         ...(loadingSince !== undefined
            ? { loadingSince: new Date(loadingSince).toISOString() }
            : {}),
      };
   }

   /**
    * A package's slim `GET /status` entry: what a reconcile reads, without the
    * package's metadata (see PackageStatusEntry). `loaded` is the copy whose
    * binding it reports, when one is in memory.
    */
   private statusEntry(
      packageName: string,
      loaded: Package | undefined,
      versionId?: string,
   ): ApiPackageStatusEntry {
      const binding = loaded?.getOperationalFields();
      return {
         name: packageName,
         resource: packageResource(this.environmentName, packageName),
         ...(versionId !== undefined && { versionId }),
         status: this.describePackageStatus(packageName, versionId),
         ...(binding && {
            manifestBindingStatus: binding.manifestBindingStatus,
            boundManifestUri: binding.boundManifestUri,
         }),
      };
   }

   /**
    * `GET …/packages/{packageName}/status`: how this server holds the package,
    * or the named version of it (its `latest` when none is named), right now.
    * Never loads anything: a copy that is not in memory reports
    * `status.serving` false and none of the load fields. 404 for a package
    * this environment does not know.
    */
   public async describeServingState(
      packageName: string,
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<ApiPackageServingState> {
      assertSafePackageName(packageName);
      const resource = packageResource(this.environmentName, packageName);
      const versions = this.versionService;
      if (
         versions &&
         (versionId.requested() !== undefined ||
            (!this.packages.has(packageName) &&
               (await versions.isVersioned(packageName))))
      ) {
         let named = versionId.requested();
         if (named === undefined) {
            const latest = await versions.latestOf(packageName);
            if (latest === null) {
               return {
                  name: packageName,
                  resource,
                  versionId: null,
                  status: this.describePackageStatus(packageName),
               };
            }
            named = latest;
         }
         // 404 for a version this package lacks.
         const row = await versions.getVersion(packageName, named);
         const loaded = versions.cache.peek(packageName, row.versionId);
         return {
            name: packageName,
            resource,
            versionId: row.versionId,
            loaded: loaded !== undefined,
            archiveStatus: row.archiveStatus,
            status: this.describePackageStatus(packageName, row.versionId),
            ...loaded?.getOperationalFields(),
            ...servingVerdicts(loaded),
         };
      }
      const resident = this.packages.get(packageName);
      if (
         resident === undefined &&
         !this.lifecycle.isRegistered(packageName) &&
         !this.loadsInFlight.has(packageName)
      ) {
         throw new PackageNotFoundError(
            `Package ${packageName} not found in environment ${this.environmentName}`,
         );
      }
      return {
         name: packageName,
         resource,
         status: this.describePackageStatus(packageName),
         ...resident?.getOperationalFields(),
         ...servingVerdicts(resident),
      };
   }

   /**
    * Run `fn` with the package marked as loading (see `loadsInFlight`). Every
    * path that compiles a copy runs inside this, from acceptance (download
    * included) until it settles or rolls back.
    */
   private trackPackageLoad<T>(
      packageName: string,
      fn: () => Promise<T>,
   ): Promise<T> {
      // A load or publish that a connection edit overtook derived its plan
      // under the old connections: re-derive it, and the package's other
      // loaded copies, once it lands (see refreshBuildPlans).
      const generation = this.connectionGeneration;
      return this.loadsInFlight.track(packageName, fn).then((result) => {
         if (this.connectionGeneration !== generation) {
            const changed = new Set(this.lastChangedConnections);
            const loaded = [
               ...(result instanceof Package ? [result] : []),
               ...[this.packages.get(packageName)],
               ...(this.versionService?.cache.entries() ?? [])
                  .filter(([name]) => name === packageName)
                  .map(([, , version]) => version),
            ];
            for (const _package of new Set(loaded)) {
               if (_package && Environment.planUsesAny(_package, changed)) {
                  this.schedulePlanRefresh(_package);
               }
            }
         }
         return result;
      });
   }

   /**
    * One mutex per package name, never replaced: replacing would let two
    * loads race on the canonical directory. `deletePackage` keeps the entry
    * so a re-install serializes against straggling readers, so the map grows
    * with distinct names ever served; there is no sweep for unbounded
    * create/delete of unique names.
    */
   private getOrCreatePackageMutex(packageName: string): Mutex {
      let packageMutex = this.packageMutexes.get(packageName);
      if (packageMutex === undefined) {
         packageMutex = new Mutex();
         this.packageMutexes.set(packageName, packageMutex);
      }
      return packageMutex;
   }

   /**
    * Run `fn` holding the per-package mutex, which every path that mutates or
    * reads `{environmentPath}/{packageName}/` must go through (lock ordering:
    * see `packageMutexes`). Not reentrant: inside `fn`, use the `_xxxLocked`
    * variants or it deadlocks.
    */
   public async withPackageLock<T>(
      packageName: string,
      fn: () => Promise<T>,
   ): Promise<T> {
      assertSafePackageName(packageName);
      return this.getOrCreatePackageMutex(packageName).runExclusive(fn);
   }

   /**
    * Read a model file's text, or undefined when there is none. The caller
    * holds the package lock.
    */
   private async _readModelFileLocked(
      target: string,
   ): Promise<string | undefined> {
      try {
         return await fs.promises.readFile(target, "utf8");
      } catch (error) {
         const code = (error as NodeJS.ErrnoException).code;
         // ENOTDIR: a directory on the path is a file, so nothing is here;
         // resolving the directories refuses it (`_resolveModelFileDirLocked`).
         if (code !== "ENOENT" && code !== "ENOTDIR") throw error;
         return undefined;
      }
   }

   /**
    * Settle where a write to `target` really leads, creating nothing, and
    * return the missing directories, deepest first. Caller holds the lock.
    *
    * The target is confined lexically already, but a directory can be a
    * symlink that `mkdir` and the write would follow, so the nearest existing
    * directory (and the file, if a link) must resolve inside the package root
    * and outside `public/` (also reachable by case on a case-insensitive FS).
    */
   private async _resolveModelFileDirLocked(
      packageRoot: string,
      target: string,
   ): Promise<string[]> {
      const missing: string[] = [];
      let dir = path.dirname(target);
      while (dir.startsWith(packageRoot)) {
         const stat = await fs.promises.stat(dir).catch((error) => {
            const code = (error as NodeJS.ErrnoException).code;
            // ENOTDIR: an ancestor is a file, found as the walk goes up.
            if (code === "ENOENT" || code === "ENOTDIR") return undefined;
            throw error;
         });
         if (stat === undefined) {
            missing.push(dir);
            dir = path.dirname(dir);
            continue;
         }
         if (!stat.isDirectory())
            throw new InvalidModelPathError(
               `Invalid model path: \`${path.relative(packageRoot, dir).split(path.sep).join("/")}\` ` +
                  `is a file in the package, not a directory.`,
            );
         break;
      }
      const [realRoot, realExisting, realTarget] = await Promise.all([
         fs.promises.realpath(packageRoot),
         fs.promises.realpath(dir).catch(() => undefined),
         // The file itself may be a link; where it leads is checked too.
         missing.length > 0
            ? undefined
            : fs.promises.realpath(target).catch(() => undefined),
      ]);
      for (const real of [realExisting, realTarget]) {
         if (real === undefined) continue;
         if (real !== realRoot && !real.startsWith(`${realRoot}${path.sep}`)) {
            throw new InvalidModelPathError(
               `Invalid model path: it resolves outside the package.`,
            );
         }
         if (
            isUnderPublicDir(
               path.relative(realRoot, real).split(path.sep).join("/"),
            )
         ) {
            throw new InvalidModelPathError(
               `Invalid model path: it resolves into \`public/\`, which is served ` +
                  `as raw static files.`,
            );
         }
      }
      if (realExisting === undefined) {
         throw new InvalidModelPathError(
            `Invalid model path: it resolves outside the package.`,
         );
      }
      return missing;
   }

   /** Make the directories {@link _resolveModelFileDirLocked} found missing; returned deepest first, to take back. */
   private async _createModelFileDirsLocked(
      missing: readonly string[],
   ): Promise<string[]> {
      // Shallowest first to create; returned deepest first to remove.
      const created: string[] = [];
      try {
         for (const next of [...missing].reverse()) {
            await fs.promises.mkdir(next);
            created.unshift(next);
         }
      } catch (error) {
         await this._removeCreatedDirsLocked(created);
         throw error;
      }
      return created;
   }

   /** Take back directories a create made, deepest first; only empty ones go. */
   private async _removeCreatedDirsLocked(dirs: string[]): Promise<void> {
      for (const dir of dirs) {
         await fs.promises.rmdir(dir).catch(() => undefined);
      }
   }

   /**
    * Put text at a model path atomically (temp file renamed over the target),
    * so a reader or a crash sees the old text or the new. Caller holds the
    * lock; the directory exists.
    */
   private async _writeModelFileLocked(
      target: string,
      source: string,
   ): Promise<void> {
      const temporary = `${target}.${crypto.randomUUID()}.tmp`;
      try {
         await fs.promises.writeFile(temporary, source, "utf8");
         await fs.promises.rename(temporary, target);
      } catch (error) {
         await fs.promises.rm(temporary, { force: true });
         throw error;
      }
   }

   /**
    * Write one model file and reload its package as one step that either
    * happens or does not.
    *
    * The precondition, write, reload, verification and any restore share one
    * hold of the package lock; as separate locked calls, a second writer could
    * land between write and reload (served under this caller's success) or
    * between a failed reload and the restore (which would revert THEIR write).
    *
    * `check(current, loaded)` refuses by throwing, so two racing saves cannot
    * both pass. `verify(reloaded)` refuses by throwing too; the previous text
    * is then restored (a new file removed), the package reloaded, and
    * {@link WriteRolledBackError} raised, even if that reload fails. If the
    * restore itself fails, {@link WriteRollbackFailedError}: the package may
    * serve the new text.
    *
    * The real path is resolved before the file is read, so nothing outside
    * the package is read through a link. Compile before calling: the lock is
    * not reentrant.
    */
   public async writeModelFileTransactional<T>(
      packageName: string,
      modelPath: string,
      source: string,
      check: (current: string | undefined, loaded: Package | undefined) => void,
      verify: (reloaded: Package) => Promise<T>,
   ): Promise<{ previous: string | undefined; verified: T }> {
      assertSafePackageName(packageName);
      assertSafeRelativeModelPath(modelPath);
      const pathProblem = reportWritePathProblem(modelPath);
      if (pathProblem) throw new InvalidModelPathError(pathProblem);
      return this.withPackageLock(packageName, async () => {
         // A published version takes no in-place write; checked under the
         // lock, so a first versioned publish cannot commit between the
         // caller's own check and the write.
         await this.assertNotVersionedLocked(
            packageName,
            this.packages.get(packageName),
         );
         const target = safeJoinUnderRoot(
            this.environmentPath,
            packageName,
            modelPath,
         );
         const missing = await this._resolveModelFileDirLocked(
            safeJoinUnderRoot(this.environmentPath, packageName),
            target,
         );
         const previous = await this._readModelFileLocked(target);
         check(previous, this.packages.get(packageName));
         const createdDirs = await this._createModelFileDirsLocked(missing);
         try {
            await this._writeModelFileLocked(target, source);
         } catch (error) {
            // No reload to undo: only a new file and its directories.
            if (previous === undefined) {
               await fs.promises.rm(target, { force: true });
               await this._removeCreatedDirsLocked(createdDirs);
            }
            throw error;
         }
         try {
            const reloaded = await this._loadOrGetPackageLocked(
               packageName,
               true,
            );
            return { previous, verified: await verify(reloaded) };
         } catch (error) {
            try {
               if (previous !== undefined)
                  await this._writeModelFileLocked(target, previous);
               else {
                  await fs.promises.rm(target, { force: true });
                  await this._removeCreatedDirsLocked(createdDirs);
               }
            } catch (restoreError) {
               // The only log line for this write; callers leave it to here.
               logger.error("Report write rollback failed", {
                  packageName,
                  model: modelPath,
                  error: errMessage(error),
                  restoreError: errMessage(restoreError),
               });
               throw new WriteRollbackFailedError(
                  `The package did not reload with the new \`${modelPath}\`, ` +
                     `and putting the previous text back failed too, so the ` +
                     `package may be serving the new text or none. Reload the ` +
                     `package, and check \`${modelPath}\`.`,
                  { cause: restoreError },
               );
            }
            // The previous text is on disk, so this is a rollback even if the
            // reload fails.
            let reloadError: unknown;
            try {
               await this._loadOrGetPackageLocked(packageName, true);
            } catch (e) {
               reloadError = e;
            }
            logger.warn("Report write rolled back", {
               packageName,
               model: modelPath,
               error: errMessage(error),
               ...(reloadError === undefined
                  ? {}
                  : { reloadError: errMessage(reloadError) }),
            });
            // Only a refusal worded for the caller is echoed; anything else can carry a server path.
            const reason =
               error instanceof WriteVerifyError ? error.message : undefined;
            throw new WriteRolledBackError(
               `The package did not reload with the new \`${modelPath}\`` +
                  `${reason ? ` (${reason})` : ""}, so the previous text was ` +
                  (reloadError === undefined
                     ? `put back and nothing changed.`
                     : `put back, but the package did not reload it and may ` +
                       `need a reload.`),
               { cause: error },
            );
         }
      });
   }

   private allocateStagingPath(packageName: string): string {
      return safeJoinUnderRoot(
         this.environmentPath,
         STAGING_DIR_NAME,
         `${packageName}-${crypto.randomUUID()}`,
      );
   }

   private allocateRetiredPath(packageName: string): string {
      return safeJoinUnderRoot(
         this.environmentPath,
         RETIRED_DIR_NAME,
         `${packageName}-${crypto.randomUUID()}`,
      );
   }

   /**
    * Best-effort sweep of `.staging/` and `.retired/` left by a previous run.
    * Safe at startup only: `installPackage`, `deletePackage`, a dry run and the
    * version store write there while serving, so a sweep while serving would
    * delete a tree mid-load.
    * Static so the path derives from the validated argument rather than
    * `this`, whose fields CodeQL's path-injection query treats as tainted.
    */
   public static async sweepStaleInstallDirs(
      environmentPath: string,
   ): Promise<void> {
      assertSafeEnvironmentPath(environmentPath);
      for (const dirName of [STAGING_DIR_NAME, RETIRED_DIR_NAME]) {
         const dir = safeJoinUnderRoot(environmentPath, dirName);
         // Sanitizer barriers in the shape CodeQL's `js/path-injection`
         // query recognises.
         if (dir.indexOf("..") !== -1) continue;
         if (path.basename(dir) !== dirName) continue;
         try {
            await fs.promises.rm(dir, { recursive: true, force: true });
         } catch (err) {
            logger.warn(`Failed to sweep stale ${dirName} dir at ${dir}`, {
               error: err,
            });
         }
      }
   }

   /**
    * Attach (or detach with `null`) the process-wide memory governor that
    * gates new package allocations.
    */
   public setMemoryGovernor(governor: PackageMemoryGovernor | null): void {
      this.memoryGovernor = governor;
   }

   /**
    * Attach (or detach with `null`) a callback run each time a package enters
    * the package map (boot, add, install, reload). It must only schedule work.
    */
   public setPackageLoadedHook(hook: ((pkg: Package) => void) | null): void {
      this.packageLoadedHook = hook;
   }

   /**
    * Called straight after each `this.packages.set`. A throwing hook is
    * logged and swallowed: an observer must never fail the load.
    */
   private notifyPackageLoaded(pkg: Package): void {
      try {
         this.packageLoadedHook?.(pkg);
      } catch (error) {
         logger.warn("Package-loaded hook failed", {
            environmentName: this.environmentName,
            packageName: pkg.getPackageName(),
            error: error instanceof Error ? error.message : String(error),
         });
      }
   }

   /** See {@link storageBindingResolver}. */
   public setStorageBindingResolver(
      resolver: (
         packageName: string,
         reader: ServingReader,
      ) => Promise<Record<string, ManifestEntry>>,
   ): void {
      this.storageBindingResolver = resolver;
   }

   /**
    * Let unversioned packages restore their serve bindings from the local
    * store on load, as versions always do. Called for an environment reloaded
    * from the database at startup, the one place the restore ran before
    * versions existed (see {@link restoreUnversionedFromLocalStore}).
    */
   public restoreUnversionedBindingsFromLocalStore(): void {
      this.restoreUnversionedFromLocalStore = true;
   }

   /**
    * Attach (or detach with `null`) the callback run when a version is
    * archived, which reclaims the tables that version alone owns. It must
    * only schedule work, and must not throw.
    */
   public setVersionArchivedHook(
      hook: ((packageName: string, versionId: string) => void) | null,
   ): void {
      this.versionArchivedHook = hook;
   }

   /**
    * Attach (or detach with `null`) the callback run once a version is
    * deleted, which stops its builds and reclaims the tables that version
    * alone owns. It must only schedule work, and must not throw.
    */
   public setVersionDeletedHook(
      hook: ((packageName: string, versionId: string) => void) | null,
   ): void {
      this.versionDeletedHook = hook;
   }

   /**
    * Set the callback a publish runs before it creates a version's row,
    * which reclaims what a deleted version of the same id still owes when
    * the delete's background cleanup has not run yet. It must not throw.
    */
   public setOwedReclaimHook(
      hook: ((packageName: string, versionId: string) => Promise<void>) | null,
   ): void {
      this.owedReclaimHook = hook;
   }

   /**
    * Resolves once no materialization of the version is running: at once
    * when none is, else when the last one settles.
    */
   public versionBuildsSettled(
      packageName: string,
      versionId: string,
   ): Promise<void> {
      const k = `${packageName}@${versionId}`;
      if (!this.versionBuilds.has(k)) return Promise.resolve();
      return new Promise((resolve) => {
         const waiting = this.versionBuildWaiters.get(k) ?? [];
         waiting.push(resolve);
         this.versionBuildWaiters.set(k, waiting);
      });
   }

   // Waiting on a version's builds to settle (see versionBuildsSettled).
   private readonly versionBuildWaiters = new Map<string, Array<() => void>>();

   /**
    * Record that a materialization of a version is running, until the
    * returned release is called. Called holding the version's lock, so an
    * archive (which takes the same lock) either refuses because the build is
    * running, or commits before the build starts and the build is refused.
    */
   public registerVersionBuild(
      packageName: string,
      versionId: string,
   ): () => void {
      const k = `${packageName}@${versionId}`;
      this.versionBuilds.set(k, (this.versionBuilds.get(k) ?? 0) + 1);
      let released = false;
      return () => {
         if (released) return;
         released = true;
         const left = (this.versionBuilds.get(k) ?? 1) - 1;
         if (left > 0) {
            this.versionBuilds.set(k, left);
            return;
         }
         this.versionBuilds.delete(k);
         // Copies taken out of service while the build ran (see
         // takeOutOfService): the build is done with them now.
         for (const copy of this.pendingVersionReleases.get(k) ?? []) {
            this.retireConnectionGeneration(
               `package ${packageName} version ${versionId}`,
               () => copy.getMalloyConfig().shutdown("close"),
            );
         }
         this.pendingVersionReleases.delete(k);
         // A reclaim of a deleted version waits for its builds to settle.
         for (const resolve of this.versionBuildWaiters.get(k) ?? []) {
            resolve();
         }
         this.versionBuildWaiters.delete(k);
      };
   }

   // Loaded copies of a version taken out of service while a build of it ran,
   // released once the build settles (see takeOutOfService).
   private readonly pendingVersionReleases = new Map<string, Package[]>();

   /** A loaded version, without loading it. */
   public peekVersion(
      packageName: string,
      versionId: string,
   ): Package | undefined {
      return this.versionService?.cache.peek(packageName, versionId);
   }

   /** The versions of a package loaded now. */
   public getLoadedVersionIds(packageName: string): string[] {
      return this.versionService?.cache.loadedVersionsOf(packageName) ?? [];
   }

   /**
    * Run `fn` on a loaded version, holding that version's lock, so it never
    * overlaps a manifest rebind or an archive of it. A version that is not
    * loaded is skipped (undefined): it binds from the store when it loads.
    */
   private async withLoadedVersion<T>(
      packageName: string,
      versionId: string,
      what: string,
      fn: (pkg: Package) => Promise<T>,
   ): Promise<T | undefined> {
      const versions = this.versionService;
      if (!versions) return undefined;
      return versions.withVersionLock(packageName, versionId, async () => {
         const pkg = versions.cache.peek(packageName, versionId);
         if (!pkg) {
            logger.debug(`Cannot ${what}: the version is not loaded`, {
               packageName,
               versionId,
            });
            return undefined;
         }
         return fn(pkg);
      });
   }

   /**
    * Re-establish a package's serve routing from its latest persisted
    * materialization when it (re)loads. Serve bindings live in memory, so
    * without this a restart serves materialized sources live until the next
    * build. Best-effort: a lookup failure leaves the package serving live.
    *
    * Both tiers come from the same manifest ({@link splitManifestEntries}):
    *  - colocated → {@link Package.bindColocatedServeManifest}, a per-query
    *    override with no recompile; restored regardless of
    *    `PERSIST_STORAGE_MODE`, which does not gate a plain `#@ persist`.
    *  - storage= → {@link Package.bindStorageServeBindings}; only when the
    *    storage tier is not `off`.
    *
    * Skipped when the package has a `manifestLocation`: the host's manifest is
    * then authoritative, and this rebind (which runs after
    * {@link bindManifestIfConfigured}) would overwrite it with a possibly
    * staler local generation.
    */
   private async rebindServeBindingsFromLocalStore(
      pkg: Package,
   ): Promise<void> {
      if (!this.storageBindingResolver) return;
      if (
         pkg.getVersionId() === undefined &&
         !this.restoreUnversionedFromLocalStore
      ) {
         return;
      }
      if (pkg.getPackageMetadata().manifestLocation) return;
      const packageName = pkg.getPackageName();
      try {
         const rawEntries = await this.storageBindingResolver(
            packageName,
            servingReaderOf(pkg),
         );
         if (pkg.getVersionId() !== undefined) {
            this.storeBindingReads.set(pkg, JSON.stringify(rawEntries));
         }
         if (Object.keys(rawEntries).length === 0) return;
         const { tableNameManifest, storageEntries } = splitManifestEntries(
            rawEntries,
            `local store (package ${packageName})`,
            true,
         );
         if (Object.keys(tableNameManifest).length > 0) {
            pkg.bindColocatedServeManifest(tableNameManifest);
         }
         if (
            getPersistStorageMode() !== "off" &&
            Object.keys(storageEntries).length > 0
         ) {
            pkg.bindStorageServeBindings(storageEntries);
         }
      } catch (err) {
         logger.warn(
            "Failed to rebind serve bindings from local store on load",
            {
               packageName,
               error: err instanceof Error ? err.message : String(err),
            },
         );
      }
   }

   /**
    * Choke point for every path that allocates a new package into the map
    * (cache-miss load, reload, add): 503 under memory back-pressure.
    * `allowAdmission` is an opt-out for a caller that cannot tolerate 503s;
    * none sets it today.
    */
   private assertCanAdmitNewPackage(
      packageName: string,
      reason: string,
      allowAdmission: boolean,
   ): void {
      if (allowAdmission) return;
      if (!this.memoryGovernor?.isBackpressured()) return;
      // Counted before throwing, since callers may swallow the error.
      getPackageAdmissionRejectionsCounter().add(1, {
         environment: this.environmentName,
         reason,
      });
      throw new PackageAdmissionRefusedError(
         `Publisher is under memory pressure and cannot ${reason} (package "${packageName}", environment "${this.environmentName}"). Retry after the server's memory usage drops below the low-water mark (PUBLISHER_MEMORY_LOW_WATER_FRACTION of PUBLISHER_MAX_MEMORY_BYTES), or raise PUBLISHER_MAX_MEMORY_BYTES if you have headroom.`,
      );
   }

   /**
    * 503 for a query under memory back-pressure, called by every query path
    * before it runs. {@link assertCanAdmitNewPackage} alone leaves loaded
    * packages queryable; together they mean "no new work of any kind".
    */
   public assertCanAdmitQuery(): void {
      if (!this.memoryGovernor?.isBackpressured()) return;
      // Counted before throwing, since callers may swallow the error.
      getQueryAdmissionRejectionsCounter().add(1, {
         environment: this.environmentName,
      });
      throw new ServiceUnavailableError(
         `Publisher is under memory pressure and cannot accept new queries (environment "${this.environmentName}"). Retry after the server's memory usage drops below the low-water mark (PUBLISHER_MEMORY_LOW_WATER_FRACTION of PUBLISHER_MAX_MEMORY_BYTES), or raise PUBLISHER_MAX_MEMORY_BYTES if you have headroom.`,
      );
   }

   /**
    * The package instance currently being served under `name`, or undefined.
    * Never loads from disk, so a caller can ask "is this still served?"
    * without bringing back a package that was unloaded or deleted.
    */
   public peekPackage(name: string): Package | undefined {
      const unversioned = this.packages.get(name);
      if (unversioned !== undefined) return unversioned;
      // A versioned package by name means its loaded `latest`.
      return this.versionService?.peekLatestLoaded(name);
   }

   /** The version rules for this environment's packages, once bound. */
   public getVersionService(): VersionService<Package> | null {
      return this.versionService;
   }

   /**
    * Refuse to treat a versioned package as a single slot, on every path that
    * loads, installs or adds one from `<pkg>/`. That folder holds versions,
    * not a package: loading it fails, and installing over it would delete
    * them. Called holding the package lock, so a first versioned publish
    * cannot commit between the check and what the caller does next.
    */
   private async assertNotVersionedLocked(
      packageName: string,
      resident: Package | undefined,
   ): Promise<void> {
      if (
         resident === undefined &&
         this.versionService &&
         (await this.versionService.isVersioned(packageName))
      ) {
         throw new PackageVersionError(
            "PACKAGE_IS_VERSIONED",
            `Package ${packageName} has published versions, which are immutable; it is read by version and changed by publishing a new one.`,
         );
      }
   }

   /**
    * A loaded package's metadata as a response carries it. For a published
    * version that adds the package's current `latestVersion` (it moves, so it
    * is read now, not at load) and the version's own serving status, and the
    * description is the package's own when a request set one (the version's
    * publisher.json description otherwise).
    */
   public async describePackage(pkg: Package): Promise<ApiPackage> {
      const metadata = pkg.getPackageMetadata();
      const versionId = metadata.versionId;
      if (!this.versionService || !versionId) {
         return metadata;
      }
      const packageName = pkg.getPackageName();
      const description =
         await this.versionService.packageDescriptionOf(packageName);
      return {
         ...metadata,
         ...(description !== null ? { description } : {}),
         latestVersion: await this.versionService.latestOf(packageName),
         // Serving state (`GET …/status`), on a package response only while
         // PUBLISHER_PACKAGE_INLINE_FIELDS is on.
         ...(isPackageInlineFieldsEnabled() && {
            status: this.describePackageStatus(packageName, versionId),
         }),
      };
   }

   /**
    * A versioned package's entry in a package listing: its `latest`
    * version's metadata, or a bare entry with no version while it has no
    * `latest` yet (explicit promotion, before the first move).
    */
   private async listedVersionedPackage(
      packageName: string,
   ): Promise<ApiPackage> {
      const latest = await this.versionService!.latestOf(packageName);
      if (latest === null) {
         return {
            name: packageName,
            resource: packageResource(this.environmentName, packageName),
            versionId: null,
            latestVersion: null,
            ...(isPackageInlineFieldsEnabled() && {
               status: this.describePackageStatus(packageName),
            }),
         };
      }
      const loaded = await this.getPackage(
         parsePackageName(packageName),
         false,
         {
            versionId: VersionRef.parse(latest),
         },
      );
      return { ...(await this.describePackage(loaded)), name: packageName };
   }

   /**
    * For `/status`: every version each versioned package here holds, archived
    * and unloaded ones included, so a reconcile that reads it never mistakes
    * a version that is not in memory for one that is missing. A loaded
    * version carries its full metadata (manifest binding included); one that
    * is not carries its name, version, `latestVersion`, `archiveStatus` and
    * status. Empty when no package here has versions.
    *
    * With `includeLoading`, a version being published that has no row yet (a
    * version new to its package, before its commit) is listed too, as
    * loading: an orchestrator that dispatched the publish can tell it from an
    * absence by its `versionId`.
    */
   public async listVersionEntries(
      options: { includeLoading?: boolean } = {},
   ): Promise<Map<string, ApiPackage[]>> {
      const entries = new Map<string, ApiPackage[]>();
      if (!this.versionService) return entries;
      const versions = this.versionService;
      // Before the rows: a publish that commits while they are read is then
      // in both, and listed once, from its row.
      const publishing = options.includeLoading
         ? versions.publishingVersions()
         : [];
      const rows = await versions.listAllVersions();
      for (const row of rows) {
         const loaded = versions.cache.peek(row.packageName, row.versionId);
         // Slim unless PUBLISHER_PACKAGE_INLINE_FIELDS is on: what a reconcile
         // reads, not the version's metadata (see PackageStatusEntry).
         const base: ApiPackage = !isPackageInlineFieldsEnabled()
            ? this.statusEntry(row.packageName, loaded, row.versionId)
            : loaded
              ? loaded.getPackageMetadata()
              : {
                   name: row.packageName,
                   resource: packageResource(
                      this.environmentName,
                      row.packageName,
                   ),
                   versionId: row.versionId,
                };
         const list = entries.get(row.packageName) ?? [];
         list.push({
            ...base,
            name: row.packageName,
            versionId: row.versionId,
            latestVersion: row.latest,
            archiveStatus: row.archiveStatus,
            loaded: loaded !== undefined,
            status: this.describePackageStatus(row.packageName, row.versionId),
         });
         entries.set(row.packageName, list);
      }
      for (const { packageName, versionId } of publishing) {
         // An unversioned copy still serving keeps its package entry until
         // the first version commits.
         if (this.packages.has(packageName)) continue;
         const list = entries.get(packageName) ?? [];
         if (list.some((entry) => entry.versionId === versionId)) continue;
         list.push({
            name: packageName,
            resource: packageResource(this.environmentName, packageName),
            versionId,
            latestVersion: list[0]?.latestVersion ?? null,
            loaded: false,
            status: this.describePackageStatus(packageName, versionId),
         });
         entries.set(packageName, list);
      }
      return entries;
   }

   /**
    * Give this environment its version rules, backed by the registry rows of
    * `environmentId`. Idempotent per id. `hooks` are what only the store
    * knows: how to fetch a location, and how to write the package's row.
    */
   public bindVersions(
      registry: VersionRegistry,
      environmentId: string,
      hooks: {
         downloaderFor(
            packageName: string,
            location: string,
         ): (stagingPath: string) => Promise<void>;
         ensurePackageRecord(
            packageName: string,
            description: string | null | undefined,
         ): Promise<boolean>;
         removePackageRecord(packageName: string): Promise<void>;
         /** A package went with its last version: drop what is kept on it. */
         packageRemoved?(packageName: string): void;
      },
   ): VersionService<Package> {
      if (this.versionService && this.boundEnvironmentId === environmentId) {
         return this.versionService;
      }
      this.boundEnvironmentId = environmentId;
      this.versionService = new VersionService<Package>(
         registry,
         environmentId,
         new VersionStore(this.environmentPath),
         {
            loadVersion: (packageName, versionPath, version) =>
               this.trackPackageLoad(packageName, () =>
                  this.loadVersionPackage(packageName, versionPath, version),
               ),
            trackPublish: (packageName, fn) =>
               this.trackPackageLoad(packageName, fn),
            releaseVersion: (packageName, versionId, loaded) =>
               this.retireConnectionGeneration(
                  `package ${packageName} version ${versionId}`,
                  () => loaded.getMalloyConfig().shutdown("close"),
               ),
            bindVersionManifest: (loaded, manifestPath) =>
               this.bindVersionManifest(loaded, manifestPath),
            downloaderFor: hooks.downloaderFor,
            admit: (packageName, reason) =>
               this.assertCanAdmitNewPackage(packageName, reason, false),
            withPackageLock: (packageName, fn) =>
               this.withPackageLock(packageName, fn),
            holdingUnversionedReads: async (packageName, fn) => {
               this.unversionedReadsHeld.add(packageName);
               try {
                  return await fn();
               } finally {
                  this.unversionedReadsHeld.delete(packageName);
               }
            },
            isWatchMounted: async (packageName) => {
               const stat = await fs.promises
                  .lstat(safeJoinUnderRoot(this.environmentPath, packageName))
                  .catch(() => undefined);
               return stat?.isSymbolicLink() === true;
            },
            retireUnversioned: (packageName) => {
               const unversioned = this.packages.get(packageName);
               if (!unversioned) return;
               this.packages.delete(packageName);
               this.retireConnectionGeneration(`package ${packageName}`, () =>
                  unversioned.getMalloyConfig().shutdown("close"),
               );
            },
            ensurePackageRecord: hooks.ensurePackageRecord,
            removePackageRecord: hooks.removePackageRecord,
            boundManifestOf: (loaded) =>
               loaded.getPackageMetadata().manifestLocation ?? null,
            isVersionBuilding: (packageName, versionId) =>
               this.versionBuilds.has(`${packageName}@${versionId}`),
            onVersionArchived: (packageName, versionId) =>
               this.versionArchivedHook?.(packageName, versionId),
            onVersionDeleted: (packageName, versionId) =>
               this.versionDeletedHook?.(packageName, versionId),
            reclaimOwedBy: async (packageName, versionId) => {
               await this.owedReclaimHook?.(packageName, versionId);
            },
            packageRemoved: (packageName) => {
               this.clearPackageLoadFailure(packageName);
               this.transition(packageName, { type: "unregister" });
               hooks.packageRemoved?.(packageName);
            },
            onVersionLoaded: (packageName, loaded, isLatest) => {
               this.transition(packageName, { type: "versionServing" });
               void this.settleSharedBindings(packageName, loaded);
               // The package's load failure, and the retrieval index (keyed
               // by package name), both belong to the version a request
               // without a version reaches: only `latest` clears or feeds them.
               if (isLatest) {
                  this.clearPackageLoadFailure(packageName);
                  this.notifyPackageLoaded(loaded);
               }
            },
         },
      );
      return this.versionService;
   }

   private boundEnvironmentId: string | null = null;

   /**
    * Compile one published version from its own folder. Its files are never
    * removed on a failed load: they belong to the registry row (a failed
    * publish removes its own placement).
    */
   private async loadVersionPackage(
      packageName: string,
      versionPath: string,
      version: {
         versionId: string;
         manifestPath: string | null;
         sourceLocation: string | null;
      },
   ): Promise<Package> {
      const loaded = await Package.create(
         this.environmentName,
         packageName,
         versionPath,
         () => this.malloyConfig.malloyConfig,
      );
      this.attachDestinationServeConfig(loaded);
      // The version's serving state lives on its registry row, not in its
      // immutable publisher.json: where it came from, which version it is,
      // and the manifest it is bound to.
      loaded.setPackageMetadata({
         ...loaded.getPackageMetadata(),
         versionId: version.versionId,
         ...(version.sourceLocation
            ? { location: version.sourceLocation }
            : {}),
         manifestLocation: version.manifestPath,
      });
      await this.bindManifestIfConfigured(loaded);
      await this.rebindServeBindingsFromLocalStore(loaded);
      return loaded;
   }

   /**
    * Read a version's shared bindings from the store again, once, when it
    * first becomes findable. An auto-run of a `scope: package` version marks
    * the shared tables it is about to rebuild, then rebinds the versions it
    * finds loaded. A version that read the store before that mark and became
    * findable after that rebind was missed by both, and would serve a table
    * the run is rebuilding. Read here, after it is findable, the store has the
    * mark, or the run's later rebinds find the version. A version that owns
    * its tables shares none.
    */
   private async settleSharedBindings(
      packageName: string,
      loaded: Package,
   ): Promise<void> {
      const versionId = loaded.getVersionId();
      if (versionId === undefined || ownedVersionOf(loaded) !== undefined) {
         return;
      }
      if (this.settledVersions.has(loaded)) return;
      this.settledVersions.add(loaded);
      const resolver = this.storageBindingResolver;
      if (!resolver) return;
      await this.rebindVersionFromStore(
         packageName,
         versionId,
         (reader) => resolver(packageName, reader),
         { onlyIfMoved: true },
      );
   }

   /**
    * Read a loaded version's tables from the store and bind them, holding the
    * version's lock, so two of these never apply out of the order they read
    * in. `read` answers for the version as a reader (see ServingReader);
    * `onlyIfMoved` skips a bind when the store says what this version last
    * bound from it. A version bound to a host manifest is the host's.
    *
    * A version whose rebind fails is taken out of service at once: it would
    * otherwise keep a binding the store no longer vouches for, possibly a
    * table being rebuilt from another definition. It loads again, from the
    * store, when next read. A failure that says to retry (503: the compile
    * workers are busy) is retried first, waiting outside the lock. A version
    * with a build of its own running is taken out of the cache too, but its
    * connections, which that build is using, are released only once the
    * build settles. Returns whether the rebind failed.
    */
   public async rebindVersionFromStore(
      packageName: string,
      versionId: string,
      read: (reader: ServingReader) => Promise<Record<string, ManifestEntry>>,
      options: { onlyIfMoved?: boolean } = {},
   ): Promise<boolean> {
      const versions = this.versionService;
      if (!versions) return false;
      let unloaded: "now" | "after its build" | undefined;
      try {
         for (let attempt = 1; ; attempt++) {
            const outcome = await this.trackPackageLoad(packageName, () =>
               this.withLoadedVersion(
                  packageName,
                  versionId,
                  "rebind it from the store",
                  async (pkg): Promise<"done" | "retry"> => {
                     if (pkg.getPackageMetadata().manifestLocation) {
                        return "done";
                     }
                     try {
                        const entries = await read(servingReaderOf(pkg));
                        const signature = JSON.stringify(entries);
                        if (
                           options.onlyIfMoved &&
                           this.storeBindingReads.get(pkg) === signature
                        ) {
                           return "done";
                        }
                        const { tableNameManifest, storageEntries } =
                           splitManifestEntries(
                              entries,
                              `local store (package ${packageName} version ${versionId})`,
                           );
                        if (
                           Object.keys(tableNameManifest).length > 0 ||
                           pkg.hasBoundTableNameManifest()
                        ) {
                           await pkg.reloadAllModels(tableNameManifest);
                        }
                        if (getPersistStorageMode() !== "off") {
                           pkg.bindStorageServeBindings(storageEntries);
                        }
                        this.storeBindingReads.set(pkg, signature);
                        return "done";
                     } catch (error) {
                        if (
                           error instanceof ServiceUnavailableError &&
                           attempt < REBIND_ATTEMPTS
                        ) {
                           return "retry";
                        }
                        unloaded = this.takeOutOfService(
                           packageName,
                           versionId,
                        );
                        throw error;
                     }
                  },
               ),
            );
            if (outcome !== "retry") return false;
            // Waited out of the lock, so the version's other changes go on.
            await new Promise((resolve) =>
               setTimeout(
                  resolve,
                  REBIND_RETRY_MS * attempt + Math.random() * REBIND_RETRY_MS,
               ),
            );
         }
      } catch (error) {
         logger.warn(
            unloaded === "now"
               ? "Unloaded a version whose serve bindings could not be re-read; it loads again from the store when next read"
               : unloaded === "after its build"
                 ? "Took a version whose serve bindings could not be re-read out of service; it loads again from the store when next read, and the copy its running build holds is released once that build settles"
                 : "Could not rebind a version's serve bindings",
            {
               packageName,
               versionId,
               error: error instanceof Error ? error.message : String(error),
            },
         );
         return true;
      }
   }

   /**
    * Take a loaded version out of service now. Unloaded outright, unless a
    * build of it is running: then it leaves the cache now (the next read
    * loads it afresh) and the copy the build holds is released once the
    * build settles (see registerVersionBuild).
    */
   private takeOutOfService(
      packageName: string,
      versionId: string,
   ): "now" | "after its build" {
      const versions = this.versionService!;
      const k = `${packageName}@${versionId}`;
      if (!this.versionBuilds.has(k)) {
         versions.cache.evict(packageName, versionId);
         return "now";
      }
      const detached = versions.cache.detach(packageName, versionId);
      if (detached) {
         const held = this.pendingVersionReleases.get(k) ?? [];
         held.push(detached);
         this.pendingVersionReleases.set(k, held);
      }
      return "after its build";
   }

   /** Rebind a loaded version to a build manifest, or revert it to live. */
   private async bindVersionManifest(
      loaded: Package,
      manifestPath: string | null,
   ): Promise<void> {
      const packageName = loaded.getPackageName();
      const wasBound =
         loaded.hasBoundTableNameManifest() || loaded.hasStorageServeBindings();
      const previous = loaded.getPackageMetadata().manifestLocation ?? null;
      loaded.setPackageMetadata({
         ...loaded.getPackageMetadata(),
         manifestLocation: manifestPath,
      });
      try {
         if (manifestPath) {
            await this.trackPackageLoad(packageName, () =>
               this.bindManifest(loaded, manifestPath),
            );
         } else if (wasBound) {
            await this.trackPackageLoad(packageName, async () => {
               await loaded.reloadAllModels({});
               loaded.bindStorageServeBindings({});
            });
         }
      } catch (error) {
         // The models still use the previous binding, so the metadata says so.
         loaded.setPackageMetadata({
            ...loaded.getPackageMetadata(),
            manifestLocation: previous,
         });
         throw error;
      }
   }

   public async getPackage(
      packageName: PackageName,
      reload: boolean = false,
      /**
       * `versionId`: which version to read; absent means `latest`. A
       * malformed one is refused (400) only when this environment has
       * versions to consult.
       */
      options: { allowAdmission?: boolean; versionId?: VersionRef } = {},
   ): Promise<Package> {
      // The sink-side check (path_safety.ts); the brand says it already ran.
      assertSafePackageName(packageName);
      // A published version is immutable, so it has nothing to reload: a
      // versioned package answers with the named version (or `latest`) as it
      // is, resolved from its registry records, never the disk. An unversioned
      // package already loaded here skips the lookup unless the request names
      // a version (`latest` names none), which it does not have (404).
      if (this.versionService) {
         const requested = options.versionId?.requested();
         if (requested !== undefined || !this.packages.has(packageName)) {
            const versioned = await this.versionService.getLoaded(
               packageName,
               requested,
            );
            if (versioned) return versioned.loaded;
            // A configured package whose boot publish was refused: publish it
            // again now, which serves it or answers why it still cannot. Only
            // for the default resolution: a request naming a version that does
            // not exist answers 404 without publishing anything.
            if (
               requested === undefined &&
               !this.packages.has(packageName) &&
               this.configuredPublishRetries.has(packageName)
            ) {
               await this.retryConfiguredPublish(packageName);
               const published = await this.versionService.getLoaded(
                  packageName,
                  requested,
               );
               if (published) return published.loaded;
            }
         }
      }
      // Lock-free fast path: `Package` references are immutable, and
      // disk-reading methods take the lock themselves.
      //
      // INVARIANT: code reachable from a fast-path Package (MCP query tools,
      // Model.getModel()) must stay in-memory; any new disk I/O against the
      // canonical tree needs `withPackageLock`, or it races install/delete.
      const _package = this.packages.get(packageName);
      if (_package !== undefined && !reload) {
         if (!this.unversionedReadsHeld.has(packageName)) return _package;
         // The package's first versioned publish has moved this copy's files
         // aside while it compiles. Wait for it (it holds the package lock),
         // then serve what it left: its version, or this copy put back.
         await this.withPackageLock(packageName, async () => {});
         return this.getPackage(packageName, reload, options);
      }

      this.assertCanAdmitNewPackage(
         packageName,
         reload ? "reload a package" : "load a package",
         options.allowAdmission === true,
      );

      return this.withPackageLock(packageName, async () => {
         // Re-checked under the lock: a first versioned publish
         // may have committed while this waited, and its folder now holds
         // versions, not a package.
         if (this.versionService && !this.packages.has(packageName)) {
            const versioned = await this.versionService.getLoaded(
               packageName,
               options.versionId?.requested(),
            );
            if (versioned) return versioned.loaded;
         }
         return this._loadOrGetPackageLocked(packageName, reload);
      });
   }

   /** Load (or reload) a package from disk. Caller holds the package mutex. */
   private async _loadOrGetPackageLocked(
      packageName: string,
      reload: boolean = false,
   ): Promise<Package> {
      const existingPackage = this.packages.get(packageName);
      if (existingPackage !== undefined && !reload) {
         return existingPackage;
      }
      await this.assertNotVersionedLocked(packageName, existingPackage);

      const load = () => this.loadPackageLocked(packageName, existingPackage);
      if (!reload) return load();
      return withSpan(
         "publisher.package.reload",
         {
            "malloy.environment": this.environmentName,
            "malloy.package": packageName,
         },
         () => measurePackageOperation("reload", load),
      );
   }

   /** Create the package from disk and serve it. Caller holds the package mutex. */
   private loadPackageLocked(
      packageName: string,
      existingPackage: Package | undefined,
   ): Promise<Package> {
      return this.trackPackageLoad(packageName, async () => {
         this.transition(packageName, { type: "beginLoad" });

         try {
            logger.debug(`Loading package ${packageName}...`);
            const packagePath = safeJoinUnderRoot(
               this.environmentPath,
               packageName,
            );
            const _package = await Package.create(
               this.environmentName,
               packageName,
               packagePath,
               () => this.malloyConfig.malloyConfig,
            );
            this.attachDestinationServeConfig(_package);
            await this.commitLoadedPackage(packageName, _package, {
               bindServeState: true,
               replaces: existingPackage,
               reportSurfaceChange: true,
            });
            logger.debug(`Successfully loaded package ${packageName}`);

            return _package;
         } catch (error) {
            logger.error(`Failed to load package ${packageName}`, { error });
            if (existingPackage !== undefined) {
               // A failed reload keeps serving the last good model (replaced
               // only on success) rather than evicting the package. Recorded
               // here so every reload caller (watcher, MCP, REST) reports the
               // staleness to /status; cleared on the next successful load.
               this.transition(packageName, {
                  type: "reloadFailed",
                  stale: {
                     message: redactPgSecrets(
                        error instanceof Error ? error.message : String(error),
                     ),
                     failedAt: new Date().toISOString(),
                  },
               });
            } else {
               this.packages.delete(packageName);
               this.transition(packageName, { type: "unregister" });
            }
            throw error;
         }
      });
   }

   /**
    * Make a freshly created package the served copy: bind its serve state,
    * retire the copy it replaces, register it and mark it serving. Caller
    * holds the package lock. `reportSurfaceChange` warns when the replaced
    * copy published a surface this one lacks; it runs after the bind, which
    * may reload the models in place.
    */
   private async commitLoadedPackage(
      packageName: string,
      pkg: Package,
      options: {
         bindServeState: boolean;
         replaces?: Package;
         reportSurfaceChange?: boolean;
      },
   ): Promise<void> {
      const { replaces } = options;
      if (options.bindServeState) {
         await this.bindManifestIfConfigured(pkg);
         await this.rebindServeBindingsFromLocalStore(pkg);
      }
      if (replaces) {
         this.retireConnectionGeneration(`package ${packageName}`, () =>
            replaces.getMalloyConfig().shutdown("close"),
         );
         if (options.reportSurfaceChange) {
            pkg.noteSurfaceChangeFrom(replaces.getPackageMetadata().explores);
         }
      }
      this.packages.set(packageName, pkg);
      // Counted here, when a loaded package is accepted and served: once per
      // load, after every gate and publish check, so a refused publish and a
      // PATCH that re-runs the gates count nothing.
      recordAnnotationFindings(pkg.annotationFindingWarnings());
      this.notifyPackageLoaded(pkg);
      // Serving, and a package that failed earlier is fixed without a restart.
      this.transition(packageName, { type: "loaded" });
   }

   public async addPackage(
      packageName: string,
      options: { allowAdmission?: boolean } = {},
   ) {
      assertSafePackageName(packageName);
      const packagePath = safeJoinUnderRoot(this.environmentPath, packageName);
      if (
         !(await fs.promises
            .access(packagePath)
            .then(() => true)
            .catch(() => false)) ||
         !(await fs.promises.stat(packagePath))?.isDirectory()
      ) {
         throw new PackageNotFoundError(`Package ${packageName} not found`);
      }
      // After the existence check, so a permanent 404 is not masked as a
      // retryable 503.
      this.assertCanAdmitNewPackage(
         packageName,
         "add a new package",
         options.allowAdmission === true,
      );
      logger.info(
         `Adding package ${packageName} to environment ${this.environmentName}`,
         {
            packagePath,
         },
      );

      return this.withPackageLock(packageName, () =>
         this._addPackageLocked(packageName),
      );
   }

   private async _addPackageLocked(
      packageName: string,
   ): Promise<Package | undefined> {
      const packagePath = safeJoinUnderRoot(this.environmentPath, packageName);
      const existingPackage = this.packages.get(packageName);
      if (existingPackage !== undefined) {
         return existingPackage;
      }
      await this.assertNotVersionedLocked(packageName, existingPackage);

      return this.trackPackageLoad(packageName, async () => {
         this.transition(packageName, { type: "beginLoad" });
         try {
            const addedPackage = await Package.create(
               this.environmentName,
               packageName,
               packagePath,
               () => this.malloyConfig.malloyConfig,
            );
            this.attachDestinationServeConfig(addedPackage);
            await this.commitLoadedPackage(packageName, addedPackage, {
               bindServeState: false,
            });
         } catch (error) {
            logger.error("Error adding package", { error });
            this.transition(packageName, { type: "unregister" });
            throw error;
         }
         return this.packages.get(packageName);
      });
   }

   /**
    * Replace a package on disk via stage-and-swap, then load it.
    *
    *  - Phase 1 (no lock): `downloader(stagingPath)` fills `.staging/…`, so
    *    lock holders never queue behind a long download.
    *  - Phase 2 (lock held): rename the canonical tree to `.retired/…`,
    *    staging into place, and `Package.create` it.
    *  - Phase 3 (after release): drain the old connections and remove the
    *    retired tree.
    *
    * On failure the staging dir is removed and the old tree renamed back.
    */
   public async installPackage(
      packageName: string,
      downloader: (stagingPath: string) => Promise<void>,
      validate?: (pkg: Package) => string | undefined,
      options: {
         allowAdmission?: boolean;
         /**
          * Metadata applied inside the install's lock hold. Its `location` is
          * recorded as where the package was installed from (only installs
          * record one).
          */
         update?: ApiPackage;
         /**
          * Judges `update` under the lock, against the tree just loaded and
          * the package it replaces, as the lock holder sees them: returns the
          * body to apply, or throws to refuse it (the swap rolls back).
          */
         admit?: AdmitUpdate;
      } = {},
   ): Promise<Package> {
      assertSafePackageName(packageName);
      // The largest allocation a package can ask for (a reinstall holds two
      // copies until the swap), so gate it before the download.
      this.assertCanAdmitNewPackage(
         packageName,
         this.packages.has(packageName)
            ? "reinstall a package"
            : "install a package",
         options.allowAdmission === true,
      );
      return measurePackageOperation("install", () =>
         this.trackPackageLoad(packageName, () =>
            this._installPackageTracked(
               packageName,
               downloader,
               validate,
               options,
            ),
         ),
      );
   }

   /**
    * Load the tree `downloader` writes as `packageName` and run the publish
    * checks on it, exactly as a publish of it would, then discard it: nothing
    * is installed, swapped or recorded, and the package already served under
    * the name is untouched. Returns what `describe` reads from the loaded
    * package, before its connections are released.
    *
    * `readVersion` (versioning on) also reads the version the tree's
    * publisher.json declares, refusing a missing or invalid one with the
    * publish's own 400; `describe` receives it. Version lifecycle checks (an
    * existing version, `latest`) are not part of it.
    */
   public async dryRunInstall<T>(
      packageName: string,
      downloader: (stagingPath: string) => Promise<void>,
      options: {
         validate?: (pkg: Package) => string | undefined;
         readVersion: boolean;
         describe: (
            pkg: Package,
            versionId: string | undefined,
         ) => T | Promise<T>;
      },
   ): Promise<T> {
      assertSafePackageName(packageName);
      this.assertCanAdmitNewPackage(packageName, "dry-run a package", false);
      if (Environment.dryRunsInFlight >= MAX_CONCURRENT_DRY_RUNS) {
         throw new DryRunCapacityError(
            `This server is already running ${MAX_CONCURRENT_DRY_RUNS} dry-run publishes, as many as it runs at once; retry shortly or ask another server.`,
         );
      }
      Environment.dryRunsInFlight++;
      try {
         return await this.dryRunInstallAdmitted(
            packageName,
            downloader,
            options,
         );
      } finally {
         Environment.dryRunsInFlight--;
      }
   }

   /**
    * Dry runs running on this server, across environments: each holds a whole
    * download and load, so they are capped together.
    */
   private static dryRunsInFlight = 0;

   private async dryRunInstallAdmitted<T>(
      packageName: string,
      downloader: (stagingPath: string) => Promise<void>,
      options: {
         validate?: (pkg: Package) => string | undefined;
         readVersion: boolean;
         describe: (
            pkg: Package,
            versionId: string | undefined,
         ) => T | Promise<T>;
      },
   ): Promise<T> {
      const stagingPath = await this.stageDownload(packageName, downloader);
      try {
         // Admitted again before the load: the download can be long, and
         // memory may have filled meanwhile.
         this.assertCanAdmitNewPackage(packageName, "dry-run a package", false);
         const versionId = options.readVersion
            ? (await readManifestVersion(stagingPath)).versionId
            : undefined;
         const pkg = await this.loadStagedTree(
            packageName,
            stagingPath,
            options.validate,
            { dryRun: true },
         );
         try {
            // Awaited here: a preview compiles against the staged files.
            return await options.describe(pkg, versionId);
         } finally {
            // Never served, so nothing is in flight on its connections.
            await Environment.releasePackageConnections(
               pkg,
               `dry run of ${packageName}`,
            );
         }
      } finally {
         await fs.promises
            .rm(stagingPath, { recursive: true, force: true })
            .catch(() => {});
      }
   }

   /**
    * A fresh staging folder holding what `downloader` wrote. A failed download
    * removes the folder and rethrows.
    */
   private async stageDownload(
      packageName: string,
      downloader: (stagingPath: string) => Promise<void>,
   ): Promise<string> {
      const stagingPath = this.allocateStagingPath(packageName);
      await fs.promises.mkdir(path.dirname(stagingPath), { recursive: true });

      logger.debug("install.phase1.download.started", {
         environmentName: this.environmentName,
         packageName,
         stagingPath,
      });
      const downloadStartedAt = performance.now();
      try {
         await downloader(stagingPath);
      } catch (err) {
         await fs.promises
            .rm(stagingPath, { recursive: true, force: true })
            .catch(() => {});
         throw err;
      }
      logger.debug("install.phase1.download.completed", {
         environmentName: this.environmentName,
         packageName,
         durationMs: performance.now() - downloadStartedAt,
      });
      return stagingPath;
   }

   /**
    * Load the freshly staged tree at `treePath` as `packageName` and run the
    * publish checks on it: the one load a publish and a dry run share, so they
    * cannot drift. A load `validate` refuses has its connections released
    * before the refusal is thrown, since no caller ever holds it.
    */
   private async loadStagedTree(
      packageName: string,
      treePath: string,
      validate: ((pkg: Package) => string | undefined) | undefined,
      options: { dryRun: boolean },
   ): Promise<Package> {
      const pkg = await Package.create(
         this.environmentName,
         packageName,
         treePath,
         () => this.malloyConfig.malloyConfig,
         // Freshly staged, so a failed load's half-built tree is ours to
         // remove.
         true,
         { dryRun: options.dryRun },
      );
      this.attachDestinationServeConfig(pkg);
      // Publish, update and dry run only (reload passes no validator). A
      // validator refuses by returning why (400) or by throwing its own error.
      let validationMsg: string | undefined;
      try {
         validationMsg = validate?.(pkg);
      } catch (error) {
         await Environment.releasePackageConnections(
            pkg,
            `refused load of ${packageName}`,
         );
         throw error;
      }
      if (validationMsg) {
         await Environment.releasePackageConnections(
            pkg,
            `refused load of ${packageName}`,
         );
         throw new BadRequestError(validationMsg);
      }
      return pkg;
   }

   /** Close a package's own connections now; for a package nothing serves. */
   private static async releasePackageConnections(
      pkg: Package,
      label: string,
   ): Promise<void> {
      try {
         await pkg.getMalloyConfig().shutdown("close");
      } catch (error) {
         logger.error(`Error releasing the connections of ${label}`, {
            error,
         });
      }
   }

   private async _installPackageTracked(
      packageName: string,
      downloader: (stagingPath: string) => Promise<void>,
      validate: ((pkg: Package) => string | undefined) | undefined,
      options: { update?: ApiPackage; admit?: AdmitUpdate },
   ): Promise<Package> {
      const stagingPath = await this.stageDownload(packageName, downloader);

      return this.withPackageLock(packageName, async () => {
         try {
            await this.assertNotVersionedLocked(
               packageName,
               this.packages.get(packageName),
            );
         } catch (err) {
            await fs.promises
               .rm(stagingPath, { recursive: true, force: true })
               .catch(() => {});
            throw err;
         }
         logger.debug("install.phase2.swap.started", {
            environmentName: this.environmentName,
            packageName,
         });
         const canonicalPath = safeJoinUnderRoot(
            this.environmentPath,
            packageName,
         );
         let retiredPath: string | undefined;

         const oldPackage = this.packages.get(packageName);
         const oldExistsOnDisk = await fs.promises
            .access(canonicalPath)
            .then(() => true)
            .catch(() => false);

         if (oldExistsOnDisk) {
            retiredPath = this.allocateRetiredPath(packageName);
            await fs.promises.mkdir(path.dirname(retiredPath), {
               recursive: true,
            });
            await fs.promises.rename(canonicalPath, retiredPath);
            logger.debug("install.phase2.retired_old", {
               environmentName: this.environmentName,
               packageName,
               retiredPath,
            });
         }

         // The body as admitted under this lock hold, against the tree just
         // loaded and the package it replaces: after the validator, so a
         // request the publish checks refuse is answered 400 first.
         let update = options.update;
         const validateAndAdmit = (pkg: Package): string | undefined => {
            const refused = validate?.(pkg);
            if (refused) return refused;
            if (update !== undefined && options.admit) {
               update = options.admit(
                  update,
                  pkg.getPackageMetadata(),
                  oldPackage?.getPackageMetadata(),
               );
            }
            return undefined;
         };

         let newPackage: Package;
         try {
            await fs.promises.rename(stagingPath, canonicalPath);

            this.transition(packageName, { type: "beginLoad" });
            // A refusal is thrown inside the try so the swap rolls back.
            newPackage = await this.loadStagedTree(
               packageName,
               canonicalPath,
               validateAndAdmit,
               { dryRun: false },
            );
            // The body's listing decides what the gates judge, so a refusal
            // here rolls the swap back rather than 400ing a tree already live.
            const gateMsg = update
               ? await this.gatesRefuseUnder(newPackage, update)
               : "";
            if (gateMsg) {
               throw new BadRequestError(gateMsg);
            }
            logger.debug("install.phase2.committed", {
               environmentName: this.environmentName,
               packageName,
               canonicalPath,
            });
         } catch (err) {
            await this.rollbackInstallSwap(packageName, err, {
               canonicalPath,
               retiredPath,
               stagingPath,
               oldPackage,
            });
            throw err;
         }

         // Outside the rollback window: an unfetchable manifest must not undo
         // a successful install (it serves live instead).
         await this.commitLoadedPackage(packageName, newPackage, {
            bindServeState: true,
            replaces: oldPackage,
         });

         if (retiredPath) {
            this.scheduleRetiredTreeCleanup(packageName, retiredPath);
         }

         // Same lock hold: as a separate step, a delete queued behind the
         // swap could run first and the update would 404 for completed work.
         if (update !== undefined) {
            await this._updatePackageLocked(packageName, update, {
               recordLocation: true,
               regated: true,
            });
         }

         return newPackage;
      });
   }

   /**
    * Undo a failed Phase 2 swap: wipe the canonical path, put the retired tree
    * back if one was moved aside, and leave the package maps agreeing with
    * what is now on disk. The caller rethrows `err`.
    */
   private async rollbackInstallSwap(
      packageName: string,
      err: unknown,
      {
         canonicalPath,
         retiredPath,
         stagingPath,
         oldPackage,
      }: {
         canonicalPath: string;
         retiredPath: string | undefined;
         stagingPath: string;
         oldPackage: Package | undefined;
      },
   ): Promise<void> {
      // Best-effort wipe (often already gone) so the rename-back has a clean
      // destination.
      await fs.promises
         .rm(canonicalPath, { recursive: true, force: true })
         .catch(() => {});
      let restored = false;
      if (retiredPath) {
         try {
            await fs.promises.rename(retiredPath, canonicalPath);
            restored = true;
         } catch (restoreErr) {
            logger.error(
               "Failed to restore retired package after install rollback",
               {
                  error: restoreErr,
                  retiredPath,
                  canonicalPath,
               },
            );
         }
      }
      await fs.promises
         .rm(stagingPath, { recursive: true, force: true })
         .catch(() => {});
      if (oldPackage && restored) {
         // The old package still serves; unregistering it would hide it from
         // listings (which enumerate registered packages) until a restart.
         this.transition(packageName, { type: "restored" });
      } else {
         // Nothing to fall back to, or the restore failed: the cached package
         // no longer matches disk, so drop its registration and the copy.
         this.transition(packageName, { type: "unregister" });
         if (oldPackage) {
            // Retire before dropping, or its native handles are never
            // released; drained rather than shut down inline because queries
            // that already hold this Package are not covered by the lock.
            this.retireConnectionGeneration(`package ${packageName}`, () =>
               oldPackage.getMalloyConfig().shutdown("close"),
            );
         }
         this.packages.delete(packageName);
      }
      logger.debug("install.phase2.rollback", {
         environmentName: this.environmentName,
         packageName,
         restored,
         errorName: err instanceof Error ? err.name : "Unknown",
      });
   }

   /** Phase 3: remove the retired tree once the lock is released. */
   private scheduleRetiredTreeCleanup(
      packageName: string,
      retiredPath: string,
   ): void {
      setImmediate(() => {
         logger.debug("install.phase3.retired_cleanup", {
            environmentName: this.environmentName,
            packageName,
            retiredPath,
         });
         void fs.promises
            .rm(retiredPath, { recursive: true, force: true })
            .catch((err) => {
               logger.warn(
                  `Failed to clean up retired package directory ${retiredPath}`,
                  { error: err },
               );
            });
      });
   }

   /**
    * Reload every model in a package against a build manifest, under the
    * package mutex.
    *
    * Every materialization run lands here, so the recompile (~1.1 MB of RSS
    * never reclaimed) is skipped when there is nothing to substitute now and
    * nothing was before: a `tableName` manifest is resolved at compile time,
    * and a package that dropped its last colocated entry must still recompile
    * to revert it.
    */
   public async reloadAllModelsForPackage(
      packageName: string,
      manifest: FreshnessManifest,
      // A published version of the package; undefined, its unversioned slot.
      versionId?: string,
   ): Promise<void> {
      assertSafePackageName(packageName);
      if (versionId !== undefined) {
         await this.trackPackageLoad(packageName, () =>
            this.withLoadedVersion(
               packageName,
               versionId,
               "reload its models",
               async (pkg) => {
                  const has = Object.keys(manifest).length > 0;
                  if (!has && !pkg.hasBoundTableNameManifest()) return;
                  await pkg.reloadAllModels(manifest);
               },
            ),
         );
         return;
      }
      return this.trackPackageLoad(packageName, () =>
         this.withPackageLock(packageName, async () => {
            const pkg = this.packages.get(packageName);
            if (!pkg) {
               throw new PackageNotFoundError(
                  `Package ${packageName} is not loaded`,
               );
            }
            const has = Object.keys(manifest).length > 0;
            const had = pkg.hasBoundTableNameManifest();
            if (!has && !had) return;
            await pkg.reloadAllModels(manifest);
         }),
      );
   }

   /**
    * Bind a package's `storage=` serve bindings from a build's full manifest
    * entries, routing those sources through the virtual-source transform.
    * Best-effort: logged no-op if the package isn't loaded.
    */
   public async bindPackageStorageServeBindings(
      packageName: string,
      entries: Record<string, ManifestEntry>,
      // A published version of the package; undefined, its unversioned slot.
      versionId?: string,
   ): Promise<void> {
      assertSafePackageName(packageName);
      if (versionId !== undefined) {
         await this.withLoadedVersion(
            packageName,
            versionId,
            "bind storage serve bindings",
            async (pkg) => {
               // Host-authoritative, as below.
               if (pkg.getPackageMetadata().manifestLocation) return;
               pkg.bindStorageServeBindings(entries);
            },
         );
         return;
      }
      return this.withPackageLock(packageName, async () => {
         const pkg = this.packages.get(packageName);
         if (!pkg) {
            logger.warn(
               "Cannot bind storage serve bindings: package not loaded",
               { packageName },
            );
            return;
         }
         // With a manifestLocation the host is authoritative; this local-store
         // path would clobber its bindings with a possibly staler generation.
         if (pkg.getPackageMetadata().manifestLocation) {
            logger.debug(
               "Skipping local-store storage serve binding: manifestLocation " +
                  "is bound (host authoritative)",
               { packageName },
            );
            return;
         }
         pkg.bindStorageServeBindings(entries);
      });
   }

   /**
    * The colocated analogue of {@link bindPackageStorageServeBindings}, used by
    * the post-delete rebind so a reclaimed table is not left routed. An empty
    * manifest reverts to live; skipped under a `manifestLocation`.
    *
    * Unlike the load-time rebind's per-query overlay, these models have the
    * substitution baked in by a build's auto-load, so this must recompile —
    * skipped when there is nothing to substitute or clear.
    */
   public async bindPackageColocatedServeManifest(
      packageName: string,
      entries: FreshnessManifest,
      // A published version of the package; undefined, its unversioned slot.
      versionId?: string,
   ): Promise<void> {
      assertSafePackageName(packageName);
      if (versionId !== undefined) {
         await this.trackPackageLoad(packageName, () =>
            this.withLoadedVersion(
               packageName,
               versionId,
               "bind a colocated serve manifest",
               async (pkg) => {
                  // Host-authoritative, as below.
                  if (pkg.getPackageMetadata().manifestLocation) return;
                  const hasColocated = Object.keys(entries).length > 0;
                  if (hasColocated || pkg.hasBoundTableNameManifest()) {
                     await pkg.reloadAllModels(entries);
                  }
               },
            ),
         );
         return;
      }
      // A recompile, so reported as loading while it runs (see trackPackageLoad).
      return this.trackPackageLoad(packageName, () =>
         this.withPackageLock(packageName, async () => {
            const pkg = this.packages.get(packageName);
            if (!pkg) {
               logger.warn(
                  "Cannot bind colocated serve manifest: package not loaded",
                  { packageName },
               );
               return;
            }
            if (pkg.getPackageMetadata().manifestLocation) {
               logger.debug(
                  "Skipping local-store colocated serve binding: manifestLocation " +
                     "is bound (host authoritative)",
                  { packageName },
               );
               return;
            }
            const hasColocated = Object.keys(entries).length > 0;
            const hadColocated = pkg.hasBoundTableNameManifest();
            if (hasColocated || hadColocated) {
               await pkg.reloadAllModels(entries);
            }
         }),
      );
   }

   /**
    * If the package declares a `manifestLocation`, fetch the host's build
    * manifest and bind it. Best-effort: a failure leaves it serving live.
    * Caller holds the package lock.
    */
   private async bindManifestIfConfigured(pkg: Package): Promise<void> {
      const manifestLocation = pkg.getPackageMetadata().manifestLocation;
      if (!manifestLocation) {
         return;
      }
      await this.bindManifest(pkg, manifestLocation);
   }

   /** Fetch + bind a specific manifest URI onto an already-loaded package. */
   private async bindManifest(
      pkg: Package,
      manifestLocation: string,
   ): Promise<void> {
      const packageName = pkg.getPackageName();
      try {
         // Timed out to live, since binding runs before the package serves.
         const { tableNameManifest, storageEntries } =
            await this.fetchManifestEntriesWithTimeout(manifestLocation);

         // Storage entries bind without a recompile. Bind when there are any
         // or were any, so vanished entries drop their old bindings. Take both
         // `had*` reads before either tier applies, since each bind overwrites
         // what its guard reads.
         const hasStorage = Object.keys(storageEntries).length > 0;
         const hadStorage = pkg.hasStorageServeBindings();

         // Colocated substitution is resolved at compile time, so it needs a
         // recompile, skipped when there is nothing to substitute or clear.
         const hasColocated = Object.keys(tableNameManifest).length > 0;
         const hadColocated = pkg.hasBoundTableNameManifest();

         // Recompile first: it is the only step that can throw, and the catch
         // reports "no manifest applied", which would be false if the new
         // storage bindings were already routing.
         if (hasColocated || hadColocated) {
            await pkg.reloadAllModels(tableNameManifest);
         }

         if (hasStorage || hadStorage) {
            pkg.bindStorageServeBindings(storageEntries);
         }

         // Unconditionally bound, even for a manifest that binds nothing:
         // reporting `unbound` would be permanent drift to a caller that
         // rebinds on anything but `bound`. Entry counts distinguish empty.
         pkg.markManifestBound(manifestLocation);
         recordManifestBind("success");
         logger.info("Bound build manifest to package", {
            environmentName: this.environmentName,
            packageName,
            manifestLocation,
            tableNameEntryCount: Object.keys(tableNameManifest).length,
            storageEntryCount: Object.keys(storageEntries).length,
            recompiled: hasColocated || hadColocated,
         });
      } catch (err) {
         pkg.markManifestBindFailed();
         const timedOut =
            err instanceof Error && err.message.includes("Timed out after");
         recordManifestBind(timedOut ? "timeout" : "failure");
         logger.warn("Failed to bind build manifest; serving live", {
            environmentName: this.environmentName,
            packageName,
            manifestLocation,
            timedOut,
            error: err instanceof Error ? err.message : String(err),
         });
      }
   }

   /** Fetch manifest entries, rejecting after {@link MANIFEST_FETCH_TIMEOUT_MS}. */
   private async fetchManifestEntriesWithTimeout(
      manifestLocation: string,
   ): Promise<FetchedManifest> {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const timeout = new Promise<never>((_, reject) => {
         timer = setTimeout(
            () =>
               reject(
                  new Error(
                     `Timed out after ${MANIFEST_FETCH_TIMEOUT_MS}ms fetching manifest ${manifestLocation}`,
                  ),
               ),
            MANIFEST_FETCH_TIMEOUT_MS,
         );
      });
      try {
         return await Promise.race([
            fetchManifestEntries(manifestLocation),
            timeout,
         ]);
      } finally {
         if (timer) {
            clearTimeout(timer);
         }
      }
   }

   /** Read a model's source text under the package mutex. */
   public async getModelFileText(
      packageName: string,
      modelPath: string,
   ): Promise<string> {
      assertSafePackageName(packageName);
      assertSafeRelativeModelPath(modelPath);
      return this.withPackageLock(packageName, async () => {
         const pkg = this.packages.get(packageName);
         if (!pkg) {
            throw new PackageNotFoundError(
               `Package ${packageName} is not loaded`,
            );
         }
         return pkg.getModelFileText(modelPath);
      });
   }

   private async writePackageManifest(
      packageName: string,
      metadata: PackageManifestUpdate,
   ): Promise<void> {
      const packagePath = safeJoinUnderRoot(this.environmentPath, packageName);
      const manifestPath = safeJoinUnderRoot(packagePath, "publisher.json");

      try {
         let existingManifest: Record<string, unknown> = {};
         try {
            const content = await fs.promises.readFile(manifestPath, "utf-8");
            existingManifest = JSON.parse(content);
         } catch (_err) {
            logger.warn(`Could not read manifest for ${packageName}`);
         }

         const updatedManifest = mergePackageManifest(
            existingManifest,
            metadata,
         );

         await fs.promises.writeFile(
            manifestPath,
            JSON.stringify(updatedManifest, null, 2),
            "utf-8",
         );
         // Recorded outside the package directory so nothing the package's
         // content carries is ever read as its install location. Never
         // cleared from here.
         if (metadata.location !== undefined && metadata.location !== "") {
            const recordPath = this.installRecordPath(packageName);
            await fs.promises.mkdir(path.dirname(recordPath), {
               recursive: true,
            });
            await fs.promises.writeFile(
               recordPath,
               JSON.stringify({ location: metadata.location }, null, 2),
               "utf-8",
            );
         }

         logger.info(`Updated publisher.json for ${packageName}`);
      } catch (error) {
         logger.error(`Failed to update publisher.json`, { error });
         throw new Error(`Failed to update package manifest`, { cause: error });
      }
   }

   public async updatePackage(
      packageName: string,
      body: ApiPackage,
      options: {
         /**
          * Judges `body` under the lock, against the metadata the update
          * applies to (`previous` is absent): returns the body to apply, or
          * throws to refuse it with nothing changed.
          */
         admit?: AdmitUpdate;
      } = {},
   ) {
      assertSafePackageName(packageName);
      // An install downloads before taking the lock, so a PATCH arriving then
      // would land on the copy about to be swapped away. Wait for loads in
      // flight and apply to whatever is resident afterwards (or 404).
      await this.awaitPackageLoads(packageName);
      return this.withPackageLock(packageName, () =>
         this._updatePackageLocked(packageName, body, {
            recordLocation: false,
            admit: options.admit,
         }),
      );
   }

   /**
    * The load gates' refusal for a package under the listing (`explores`,
    * `queryableSources`) a body would give it, empty when the body leaves the
    * listing as it is or the gates pass. The package keeps its own listing.
    */
   private async gatesRefuseUnder(
      pkg: Package,
      body: ApiPackage,
   ): Promise<string> {
      const existing = pkg.getPackageMetadata();
      const { explores, queryableSources } = resolvePatchedSettings(
         body,
         existing,
      );
      const listing = (e?: string[], q?: string) =>
         JSON.stringify([e ?? null, q ?? "declared"]);
      if (
         listing(explores, queryableSources) ===
         listing(existing.explores, existing.queryableSources)
      ) {
         return "";
      }
      try {
         await applyLoadGates(pkg, { explores, queryableSources });
         return "";
      } catch (error) {
         if (error instanceof BadRequestError) return error.message;
         throw error;
      }
   }

   /** Apply a metadata PATCH to a loaded package. Caller holds the package mutex. */
   private async _updatePackageLocked(
      packageName: string,
      requested: ApiPackage,
      options: {
         /** See updatePackage: judged here, against what this apply reads. */
         admit?: AdmitUpdate;
         /**
          * Record the body's `location` as the install location. True only
          * for an install; a PATCH naming a new location is a reinstall.
          */
         recordLocation: boolean;
         /** The install already ran the gates under this body's listing, inside its swap. */
         regated?: boolean;
      },
   ) {
      const _package = this.packages.get(packageName);
      if (!_package) {
         throw new PackageNotFoundError(`Package ${packageName} not found`);
      }
      // `setPackageMetadata` replaces the whole object, so omitted fields
      // carry over; a wiped `explores` would silently serve every model.
      const existing = _package.getPackageMetadata();
      // Under the lock and against this metadata, so a PATCH racing another
      // is judged against what the first one left, not what it read before.
      const body = options.admit
         ? options.admit(requested, existing, undefined)
         : requested;
      // Normalized as the worker normalizes on-disk explores, so publish and
      // reload agree.
      const normalizedExplores = body.explores?.map(normalizeModelPath);
      // An explicit null clears it, reverting to live.
      const manifestLocation =
         body.manifestLocation !== undefined
            ? body.manifestLocation
            : existing.manifestLocation;
      // The settings a reload would lose resolve in one place, which the
      // refusal of a change a reload discards reads too. `scope` and
      // `materialization` apply when provided and are otherwise preserved; a
      // schedule change re-arms the scheduler on its next tick. Null counts
      // as omitted, so a client serializing unset fields as null neither
      // trips the policy gate below (failing an orchestrated run) nor resets
      // the policy. `manifestLocation` differs: null there clears.
      const scopeProvided = body.scope != null;
      const materializationProvided = body.materialization != null;
      const editingPolicy = scopeProvided || materializationProvided;
      const {
         description,
         explores,
         queryableSources,
         scope,
         queryMetadata,
         materialization,
      } = resolvePatchedSettings(body, existing);
      // Judged before the listing goes live, so a query meanwhile is served
      // under the old one. Only a change to what is queryable re-runs them.
      const gateMsg = options.regated
         ? ""
         : await this.gatesRefuseUnder(_package, body);
      // Omitted (or null) fields carry over, so a rebind PATCH keeps the
      // `location` a reload reinstalls from and the `resource` an
      // orchestrator identifies the package by. An empty string is a value.
      _package.setPackageMetadata({
         name: body.name != null ? body.name : existing.name,
         description,
         resource: body.resource != null ? body.resource : existing.resource,
         location:
            options.recordLocation && body.location != null
               ? body.location
               : existing.location,
         explores,
         queryableSources,
         manifestLocation,
         materialization,
         queryMetadata,
         scope,
      });

      // Strict-reject like publish, restoring the prior metadata first so a
      // bad update neither persists nor changes the served surface. Each check
      // runs only when the body touches its fields, so a PATCH is never newly
      // rejected for a warning the load tolerated (and a post-swap update
      // never fails, leaving the new tree serving unrecorded).
      const policyMsg = editingPolicy
         ? _package.formatInvalidPersistencePolicy()
         : "";
      const exploresMsg =
         normalizedExplores !== undefined
            ? _package.formatInvalidExplores()
            : "";
      const invalidMsg = [exploresMsg, policyMsg, gateMsg]
         .filter(Boolean)
         .join("\n");
      if (invalidMsg) {
         _package.setPackageMetadata(existing);
         throw new BadRequestError(invalidMsg);
      }
      // After the checks, so a refused PATCH leaves the name as it was.
      if (body.name) {
         _package.setName(body.name);
      }

      await this.writePackageManifest(packageName, {
         name: packageName,
         description: body.description ?? undefined,
         location: options.recordLocation
            ? (body.location ?? undefined)
            : undefined,
         explores: normalizedExplores,
         queryableSources: body.queryableSources ?? undefined,
         manifestLocation: body.manifestLocation,
         scope: scopeProvided ? body.scope : undefined,
         queryMetadata: body.queryMetadata ?? undefined,
         materialization: materializationProvided
            ? body.materialization
            : undefined,
      });

      // Apply a manifestLocation change now: a URI rebinds, null/empty
      // reverts to live.
      const revertsBinding =
         body.manifestLocation !== undefined &&
         !body.manifestLocation &&
         (_package.hasBoundTableNameManifest() ||
            _package.hasStorageServeBindings());
      if (body.manifestLocation) {
         // May recompile, so reported as loading.
         await this.trackPackageLoad(packageName, () =>
            this.bindManifest(_package, body.manifestLocation as string),
         );
      } else if (revertsBinding) {
         // Drop both tiers so nothing still routes to a materialized table.
         await this.trackPackageLoad(packageName, async () => {
            await _package.reloadAllModels({});
            _package.bindStorageServeBindings({});
         });
      } else {
         // The surface may have changed with no file changing.
         await _package.relintReports();
      }

      return _package.getPackageMetadata();
   }

   /**
    * The package's registration: LOADING, SERVING (configured or loaded) or
    * UNLOADING, with when it was registered. Undefined when it is not.
    */
   public getPackageStatus(packageName: string): PackageInfo | undefined {
      return this.lifecycle.statusOf(packageName);
   }

   /**
    * Register a package configured for this environment (from the config file
    * or the database) before it is loaded: it is listed, and its first
    * listing or request loads it.
    */
   public registerConfiguredPackage(packageName: string): void {
      this.transition(packageName, { type: "configure" });
   }

   /**
    * Packages actually serving: registered packages minus failed ones.
    * Disjoint from getFailedPackages() so the readiness line never
    * double-counts a configured package whose location never mounted.
    * Triggers no load.
    */
   public getServingPackageCount(): number {
      return this.lifecycle.servingCount();
   }

   /**
    * Record why a configured package's location never mounted. Wins over a
    * load failure in {@link getFailedPackages}: an unmounted package is still
    * registered, so its lazy load later fails on a missing manifest, which
    * would hide a typo'd `location`.
    */
   public setPackageMountError(packageName: string, message: string): void {
      this.transition(packageName, { type: "recordMountError", message });
   }

   /**
    * How to publish a configured package again after its boot-time publish
    * was refused. A refused publish places nothing, so without it the package
    * had no files at all: `/compile` could not report why it failed, a
    * failure that clears on its own (a table created after boot) could never
    * recover, and every request answered a misleading "manifest does not
    * exist". Its next access runs this instead: a publish that now succeeds
    * serves the version, and one that still fails answers its own error.
    * `fingerprint` reads the location's model files cheaply, so a change to
    * them retries at once whatever the backoff (configured_publish_retry.ts).
    */
   public setConfiguredPublishRetry(
      packageName: string,
      publish: () => Promise<unknown>,
      fingerprint?: () => Promise<string | undefined>,
   ): void {
      this.configuredPublishRetries.set(packageName, {
         publish,
         fingerprint,
         failures: 0,
         notBefore: 0,
      });
   }

   /**
    * Record a configured package whose boot publish was refused and that can
    * NOT be published again: a remote location, whose mount is removed after
    * boot. Its reads answer the boot publish's own error (as a local package's
    * do after a failed retry) instead of "Package manifest … does not exist",
    * and never publish; it waits for the next `--init`.
    */
   public setConfiguredPublishRefused(
      packageName: string,
      error: unknown,
   ): void {
      this.configuredPublishRetries.set(packageName, {
         publish: () => Promise.reject(error),
         failures: 1,
         notBefore: Number.POSITIVE_INFINITY,
         lastError: error,
      });
   }

   /**
    * {@link retryConfiguredPublish} for callers that resolve a version before
    * they read the package (a materialization run). Resolves at once when the
    * package is loaded, has no refused publish to retry, or the caller names a
    * version (only the default resolution publishes).
    */
   public async publishRefusedConfiguredPackage(
      packageName: string,
      requestedVersion?: string,
   ): Promise<void> {
      if (requestedVersion !== undefined) return;
      if (this.packages.has(packageName)) return;
      if (!this.configuredPublishRetries.has(packageName)) return;
      if (this.versionService) {
         const loaded = await this.versionService.getLoaded(packageName);
         if (loaded) return;
      }
      await this.retryConfiguredPublish(packageName);
   }

   /**
    * Run a refused configured package's publish again (see
    * {@link setConfiguredPublishRetry}). One run at a time per package; the
    * callers waiting on it share its outcome. Between failures it backs off
    * (configured_publish_retry.ts), answering the last failure without
    * publishing unless the package's files changed. Holds no package lock: a
    * package's first version takes that lock itself, and a delete waits for a
    * running retry before it takes the lock (see deletePackage). Resolves when
    * there is nothing to retry or the retry published a version; rejects with
    * the publish's own error otherwise.
    */
   private async retryConfiguredPublish(packageName: string): Promise<void> {
      const entry = this.configuredPublishRetries.get(packageName);
      if (!entry) return;
      let running = this.configuredPublishRetriesRunning.get(packageName);
      if (!running) {
         const fingerprint = await entry.fingerprint?.();
         // Re-read after the await: another caller may have started one.
         running = this.configuredPublishRetriesRunning.get(packageName);
         if (!running) {
            const changed =
               fingerprint !== undefined &&
               fingerprint !== entry.lastFingerprint;
            if (Date.now() < entry.notBefore && !changed) {
               throw entry.lastError;
            }
            running = this.runConfiguredPublish(
               packageName,
               entry,
               fingerprint,
            );
            this.configuredPublishRetriesRunning.set(packageName, running);
         }
      }
      await running;
   }

   private async runConfiguredPublish(
      packageName: string,
      entry: ConfiguredPublishRetry,
      fingerprint: string | undefined,
   ): Promise<void> {
      try {
         await entry.publish();
         if (this.configuredPublishRetries.get(packageName) === entry) {
            this.configuredPublishRetries.delete(packageName);
            this.transition(packageName, { type: "clearProblems" });
         }
      } catch (error) {
         entry.failures += 1;
         entry.notBefore = Date.now() + backoffAfter(entry.failures);
         entry.lastError = error;
         entry.lastFingerprint = fingerprint;
         if (this.configuredPublishRetries.get(packageName) === entry) {
            // The location mounted; its package did not load. A load error,
            // so /status names the real cause, replacing the boot record.
            this.transition(packageName, { type: "clearProblems" });
            this.transition(packageName, {
               type: "recordLoadError",
               message: redactPgSecrets(
                  error instanceof Error ? error.message : String(error),
               ),
            });
         }
         throw error;
      } finally {
         this.configuredPublishRetriesRunning.delete(packageName);
      }
   }

   /**
    * Record a runtime add that failed before the package could serve, for
    * /status. Skipped while the name is registered (a failed re-install
    * rolled back, so nothing failed). Names are the caller's, so the record
    * is capped, oldest dropped first; boot-time mount errors are not capped.
    */
   public recordPackageAddFailure(packageName: string, message: string): void {
      this.lifecycle.recordAddFailure(packageName, message);
   }

   /** Forget any recorded failure for a package, whatever its cause. */
   private clearPackageLoadFailure(packageName: string): void {
      this.configuredPublishRetries.delete(packageName);
      this.transition(packageName, { type: "clearProblems" });
   }

   /**
    * Packages configured for, or added to, this environment that did not load,
    * and why.
    */
   public getFailedPackages(): ReadonlyMap<string, string> {
      return this.lifecycle.failures();
   }

   /**
    * SERVING packages whose most recent reload failed to compile: the served
    * model is stale relative to the files on disk. Disjoint from
    * {@link getFailedPackages} in practice (a successful load clears the
    * record; a failed first load records a load failure instead).
    */
   public getStaleCompileErrors(): ReadonlyMap<string, StaleCompileError> {
      return this.lifecycle.staleRecords();
   }

   /** Every change to a package's phase or problems goes through here. */
   private transition(packageName: string, event: PackageLifecycleEvent): void {
      this.lifecycle.apply(packageName, event);
   }

   public async deletePackage(
      packageName: string,
      options: {
         /**
          * The package had published versions, and the caller has already
          * removed their rows.
          */
         versioned?: boolean;
         /**
          * Remove the package's rows, holding the package's lock, so a first
          * versioned publish cannot commit between the delete's decision and
          * the rows going. For a package with versions, before anything else
          * goes: once the rows are gone no request can resolve a version, so
          * none can fetch a version's files again while they are removed. For
          * one without, after it is unloaded, as before.
          */
         removeRecords?: () => Promise<void>;
      } = {},
   ): Promise<void> {
      assertSafePackageName(packageName);
      // A refused configured package's retry: forgotten first, so none starts
      // from here on, then awaited, so one already publishing lands before
      // this delete removes it rather than after (which would bring the
      // package back). Outside the lock, which that publish takes itself.
      this.configuredPublishRetries.delete(packageName);
      await this.configuredPublishRetriesRunning
         .get(packageName)
         ?.catch(() => {});
      // The versions a delete of a versioned package removed. Each is then
      // cleaned up as deleting that version alone is: its own running
      // auto-runs stopped and its own tables reclaimed, in the background.
      let deletedVersions: string[] = [];
      await this.withPackageLock(packageName, async () => {
         // Before the early return: a failed package is never in `packages`,
         // and a stale entry would report a loadError for an unconfigured one.
         this.clearPackageLoadFailure(packageName);

         const _package = this.packages.get(packageName);
         if (!_package) {
            // A versioned package: every version goes, loaded or loading, with
            // the folder that holds them all. Only one: the folder of a
            // package that merely failed to load is kept, as before.
            const versions = this.versionService;
            const versioned =
               versions !== null &&
               (options.versioned === true ||
                  versions.cache.loadedVersionsOf(packageName).length > 0 ||
                  (await versions.isVersioned(packageName)));
            if (versioned && !options.versioned) {
               deletedVersions = (
                  await versions!.listVersions(packageName)
               ).versions.map((version) => version.versionId);
               await options.removeRecords?.();
            }
            if (versioned && this.versionService) {
               this.versionService.forgetPackage(packageName);
               await new VersionStore(this.environmentPath).removePackage(
                  packageName,
               );
               this.transition(packageName, { type: "unregister" });
            } else {
               // Registered but never loaded (configured at boot, not yet
               // listed): it goes as a loaded one does, registration and
               // folder, or the next listing loads and serves it again. One
               // whose load failed is already unregistered, and keeps its
               // folder as before.
               let retiredPath: string | null = null;
               if (this.lifecycle.phaseOf(packageName) === "configured") {
                  retiredPath = await this.retirePackageDirectory(packageName);
                  this.transition(packageName, { type: "unregister" });
                  await fs.promises
                     .rm(this.installRecordPath(packageName), { force: true })
                     .catch(() => {});
               }
               // Not loaded, and no versions: its rows go under the lock too,
               // so a first versioned publish cannot land between.
               await options.removeRecords?.();
               if (retiredPath) this.removeRetiredInBackground(retiredPath);
            }
            return;
         }
         // A guard for anything that bypasses the package lock.
         if (this.lifecycle.phaseOf(packageName) === "loading") {
            logger.error("Package loading. Can't unload.", {
               environmentName: this.environmentName,
               packageName,
            });
            throw new Error(
               "Package loading. Can't unload. " +
                  this.environmentName +
                  " " +
                  packageName,
            );
         }
         this.transition(packageName, { type: "beginUnload" });

         // Drained so in-flight queries finish before the handle is released.
         this.retireConnectionGeneration(`package ${packageName}`, () =>
            _package.getMalloyConfig().shutdown("close"),
         );

         const retiredPath = await this.retirePackageDirectory(packageName);

         this.packages.delete(packageName);
         this.transition(packageName, { type: "unregister" });
         await fs.promises
            .rm(this.installRecordPath(packageName), { force: true })
            .catch(() => {});
         // Its rows, still under the lock: a first versioned publish that
         // waited for it must find the package gone, rows and all.
         await options.removeRecords?.();

         if (retiredPath) this.removeRetiredInBackground(retiredPath);
      });
      // The delete is committed; cleaning up must not turn it into a failure.
      for (const versionId of deletedVersions) {
         try {
            this.versionDeletedHook?.(packageName, versionId);
         } catch (error) {
            logger.warn("Could not start reclaiming a deleted version", {
               environmentName: this.environmentName,
               packageName,
               versionId,
               error,
            });
         }
      }
   }

   /**
    * Rename a deleted package's directory aside, under the caller's lock, so
    * the slow recursive remove can be deferred. Null when there was nothing
    * to rename (a configured package whose location never mounted) or the
    * rename failed.
    */
   private async retirePackageDirectory(
      packageName: string,
   ): Promise<string | null> {
      const canonicalPath = safeJoinUnderRoot(
         this.environmentPath,
         packageName,
      );
      const retiredPath = this.allocateRetiredPath(packageName);
      try {
         await fs.promises.mkdir(path.dirname(retiredPath), {
            recursive: true,
         });
         await fs.promises.rename(canonicalPath, retiredPath);
         return retiredPath;
      } catch (err) {
         if ((err as NodeJS.ErrnoException).code !== "ENOENT") {
            logger.error(
               "Error renaming package directory to retired during unload",
               {
                  error: err,
                  environmentName: this.environmentName,
                  packageName,
               },
            );
         }
         return null;
      }
   }

   private removeRetiredInBackground(retiredPath: string): void {
      setImmediate(() => {
         void fs.promises
            .rm(retiredPath, { recursive: true, force: true })
            .catch((err) => {
               logger.warn(
                  `Failed to clean up retired package directory ${retiredPath}`,
                  { error: err },
               );
            });
      });
   }

   /**
    * Evict a package from memory without touching its directory: rolls back
    * a no-location `addPackage` of a user-owned directory, which must not be
    * deleted. Its connections are still drained and closed.
    */
   public async unloadPackage(packageName: string): Promise<void> {
      assertSafePackageName(packageName);
      return this.withPackageLock(packageName, async () => {
         const _package = this.packages.get(packageName);
         if (!_package) {
            return;
         }
         this.transition(packageName, { type: "beginUnload" });
         this.retireConnectionGeneration(`package ${packageName}`, () =>
            _package.getMalloyConfig().shutdown("close"),
         );
         // Eviction and clearing a recorded failure belong together.
         this.clearPackageLoadFailure(packageName);
         this.packages.delete(packageName);
         this.transition(packageName, { type: "unregister" });
      });
   }

   public updateConnections(
      malloyConfig: EnvironmentMalloyConfig,
      _apiConnections?: ApiConnection[],
      afterPreviousRelease?: () => Promise<void>,
   ): void {
      const previousMalloyConfig = this.malloyConfig;
      this.malloyConfig = malloyConfig;
      const previousConnections = this.apiConnections;
      this.apiConnections = malloyConfig.apiConnections;
      if (previousMalloyConfig !== malloyConfig) {
         this.refreshBuildPlans(
            changedConnectionNames(previousConnections, this.apiConnections),
         );
      }
      // Deliberately leaves connectionsAuthoritative alone: ConnectionService
      // calls this for a single-connection edit, which does not make a
      // fallback list the desired state. See {@link connectionsAuthoritative}.

      if (previousMalloyConfig !== malloyConfig) {
         this.retireConnectionGeneration(
            `environment ${this.environmentName}`,
            async () => {
               await previousMalloyConfig.releaseConnections();
               await afterPreviousRelease?.();
            },
         );
      } else {
         void afterPreviousRelease?.();
      }
   }

   public async deleteConnection(connectionName: string): Promise<void> {
      const index = this.apiConnections.findIndex(
         (conn) => conn.name === connectionName,
      );

      if (index !== -1) {
         this.apiConnections.splice(index, 1);
      }

      if (index !== -1) {
         logger.info(
            `Removed connection ${connectionName} from environment ${this.environmentName}`,
         );
      } else {
         logger.warn(
            `Connection ${connectionName} not found in environment ${this.environmentName}`,
         );
      }
   }

   /**
    * Stop every pending and future build-plan re-derivation and retry for this
    * environment. Called when it is closed; a re-derivation already running
    * finishes, but nothing is queued or retried after it.
    */
   public cancelPlanRefreshes(): void {
      this.planRefreshesClosed = true;
      for (const backoff of this.planRefreshBackoff.values()) {
         if (backoff.timer) clearTimeout(backoff.timer);
      }
      this.planRefreshBackoff.clear();
      this.planRefreshQueue.clear();
      this.planRefreshRetrying.clear();
      Environment.openEnvironments.delete(this.openEnvironmentRef);
   }

   /**
    * {@link cancelPlanRefreshes} for every open environment: the server's
    * graceful shutdown, so no retry timer fires while it exits.
    */
   public static cancelAllPlanRefreshes(): void {
      for (const ref of [...Environment.openEnvironments]) {
         const environment = ref.deref();
         if (environment) environment.cancelPlanRefreshes();
         else Environment.openEnvironments.delete(ref);
      }
   }

   public async closeAllConnections(): Promise<void> {
      this.cancelPlanRefreshes();
      // Package configs (each with its own sandbox `duckdb`) first, or hard
      // unload leaks their handles.
      const packageReleases = await Promise.allSettled(
         Array.from(this.packages.values(), (pkg) =>
            pkg.getMalloyConfig().shutdown("close"),
         ),
      );
      for (const result of packageReleases) {
         if (result.status === "rejected") {
            logger.error(
               `Error closing package connections for environment ${this.environmentName}`,
               { error: result.reason },
            );
         }
      }
      this.packages.clear();
      this.lifecycle.unregisterAll();
      // Every loaded version too: each holds its own MalloyConfig. Evicting retires their connections,
      // which the release of retired generations below then closes at once.
      this.versionService?.cache.evictAll();

      try {
         await this.malloyConfig.releaseConnections();
      } catch (error) {
         logger.error(
            `Error closing connections for environment ${this.environmentName}`,
            { error },
         );
      }

      try {
         await this.destinationMalloyConfig.releaseConnections();
      } catch (error) {
         // `{ error }` alone serializes an Error to `{}`.
         logger.error(
            `Error closing storage destinations for environment ${this.environmentName}`,
            { error: error instanceof Error ? error.message : String(error) },
         );
      }

      this.destinations = [];
      // Torn down: the empty list is not authoritative to prune against.
      this.destinationsAuthoritative = false;
      await this.releaseAllRetiredConnectionGenerations();

      this.apiConnections = [];
      // Torn down: as for destinations, not a list to prune against.
      this.connectionsAuthoritative = false;

      logger.info(
         `Closed all connections for environment ${this.environmentName}`,
      );
   }

   public async serialize(
      options: PackageListingOptions = {},
   ): Promise<ApiEnvironment> {
      return {
         ...this.metadata,
         // Credentials stay server-side; listApiConnections() is the internal
         // view that still carries them.
         connections: toPublicConnections(this.listApiConnections()),
         // Name and type only, so an orchestrator can see which destinations
         // a worker holds.
         storageDestinations: this.destinations.map(({ name, type }) => ({
            name,
            type,
         })),
         packages: await this.listPackages(options),
      };
   }

   public async deleteDuckDBConnection(connectionName: string): Promise<void> {
      const duckdbPath = path.join(
         this.environmentPath,
         `${connectionName}.duckdb`,
      );
      try {
         await fs.promises.rm(duckdbPath, { force: true });
         logger.info(
            `Removed DuckDB connection file ${connectionName} from environment ${this.environmentName}`,
         );
      } catch (error) {
         logger.error(
            `Failed to remove DuckDB connection file ${connectionName} from environment ${this.environmentName}`,
            { error },
         );
      }
   }

   public async deleteDuckLakeConnection(
      connectionName: string,
   ): Promise<void> {
      await deleteDuckLakeConnectionFile(connectionName, this.environmentPath);
      logger.info(
         `Removed DuckLake connection ${connectionName} from environment ${this.environmentName}`,
      );
   }
}

/**
 * Extracts the preamble from a Malloy model file — the leading block of
 * `##!` pragmas, `import` statements, blank lines, and comments that appear
 * before any `source:`, `query:`, or `run:` definition. This allows a
 * submitted query to inherit the model's import context.
 */
export async function extractPreamble(modelPath: string): Promise<string> {
   try {
      const content = await fs.promises.readFile(modelPath, "utf8");
      return extractPreambleFromSource(content);
   } catch {
      // Unreadable: the compile surfaces any import errors.
      return "";
   }
}

/**
 * Extracts the preamble from Malloy source text. Exported for testing.
 */
export function extractPreambleFromSource(content: string): string {
   const lines = content.split("\n");
   const preambleLines: string[] = [];

   for (const line of lines) {
      const trimmed = line.trim();
      if (
         trimmed.startsWith("source:") ||
         trimmed.startsWith("query:") ||
         trimmed.startsWith("run:")
      ) {
         break;
      }
      preambleLines.push(line);
   }

   return preambleLines.join("\n").trimEnd();
}

/**
 * A loaded package's serve verdicts, for its serving state; nothing when it is
 * not loaded or materializes nothing.
 */
function servingVerdicts(pkg: Package | undefined): {
   serveVerdicts?: ReturnType<Package["getServeVerdicts"]>;
} {
   if (!pkg) return {};
   try {
      const verdicts = pkg.getServeVerdicts();
      return verdicts.length > 0 ? { serveVerdicts: verdicts } : {};
   } catch (error) {
      logger.warn("Could not compute serve verdicts", {
         error: error instanceof Error ? error.message : String(error),
      });
      return {};
   }
}
