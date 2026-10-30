// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { GivenValue } from "@malloydata/malloy";
import { type CompiledDocument } from "./compile_document";
import { isUnderPublicDir, reportWritePathProblem } from "./report";
import { lazyCounter, resetInstrumentsForTesting } from "../telemetry";
import { Mutex } from "async-mutex";
import crypto from "crypto";
import * as fs from "fs";
import * as path from "path";
import { components } from "../api";
import {
   API_PREFIX,
   INDEX_MODEL_NAME,
   normalizeModelPath,
   README_NAME,
   PACKAGE_INSTALL_RECORDS_DIR,
} from "../constants";
import {
   BadRequestError,
   ConnectionNotFoundError,
   DestinationNotFoundError,
   EnvironmentNotFoundError,
   InvalidModelPathError,
   PackageNotFoundError,
   ServiceUnavailableError,
   WriteRollbackFailedError,
   WriteRolledBackError,
   WriteVerifyError,
   PackageAdmissionRefusedError,
} from "../errors";
import { getPersistStorageMode } from "../config";
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
import { FreshnessManifest, ManifestEntry } from "../storage/DatabaseInterface";
import { errMessage } from "../utils";
import {
   buildEnvironmentMalloyConfig,
   deleteDuckLakeConnectionFile,
   EnvironmentMalloyConfig,
   InternalConnection,
} from "./connection";
import {
   storageDestinationRoot,
   processStorageDestinations,
   processStorageDestinationsOrThrow,
   storageDestinationsEqual,
} from "./connection_config";
import {
   fetchManifestEntries,
   splitManifestEntries,
   type FetchedManifest,
} from "./manifest_loader";
import { ApiConnection } from "./model";
import { Package } from "./package";
import type { PackageMemoryGovernor } from "./package_memory_governor";
import {
   compileSource,
   type CompileScope,
   type TaggedLogMessage,
} from "./compile_source";
import { destinationSchemaOf } from "./storage_table_path";

export {
   COMPILE_SCOPES,
   packageRelativeModelPath,
   type CompileScope,
   type TaggedLogMessage,
} from "./compile_source";

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
const STAGING_DIR_NAME = ".staging";
const RETIRED_DIR_NAME = ".retired";

// Binding happens before a package is SERVING, so a slow manifest store must
// not block it: past this, the package serves live.
const MANIFEST_FETCH_TIMEOUT_MS = 15_000;

export enum PackageStatus {
   LOADING = "loading",
   SERVING = "serving",
   UNLOADING = "unloading",
}

interface PackageInfo {
   name: string;
   loadTimestamp: number;
   status: PackageStatus;
}

type ApiPackage = components["schemas"]["Package"];
type ApiPackageStatus = NonNullable<ApiPackage["status"]>;
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

/** Cap on runtime add failures kept per environment for /status. */
const MAX_RECORDED_ADD_FAILURES = 100;

export class Environment {
   private packages: Map<string, Package> = new Map();
   // Lock ordering: connectionMutex (environment) MUST be acquired before any
   // packageMutex, since connection updates can force package reloads. Never
   // acquire connectionMutex while holding a packageMutex (AB/BA deadlock).
   private packageMutexes = new Map<string, Mutex>();
   private packageStatuses: Map<string, PackageInfo> = new Map();
   /**
    * Packages with a load, reinstall or recompile in progress, with when the
    * first began; what `Package.status.loading` reports. Separate from
    * `packageStatuses` ("is a copy registered to serve") because a reload runs
    * while the previous copy keeps serving. Reference-counted so a nested
    * operation stays marked until the outermost finishes.
    */
   private loadsInFlight: Map<
      string,
      {
         count: number;
         since: number;
         /** Resolves once every load sharing this entry has finished. */
         settled: Promise<void>;
         settle: () => void;
      }
   > = new Map();
   /**
    * Configured packages that failed to load, with the reason. The only
    * lasting record: the failing load deletes the `packageStatuses` entry, so
    * `listPackages` forgets the package was configured.
    */
   private failedPackages: Map<string, string> = new Map();
   /**
    * Why a configured package never reached the disk. Wins over
    * {@link failedPackages}: an unmounted package is still seeded SERVING, so
    * its lazy load later fails on a missing manifest, which would hide a
    * typo'd `location`.
    */
   private mountErrors: Map<string, string> = new Map();
   /** Runtime add failures recorded in {@link mountErrors}, oldest first. */
   private recordedAddFailures: string[] = [];
   /**
    * Why a SERVING package's latest reload failed to compile. Not a failure:
    * the last good model keeps serving, so this records staleness, reported
    * by /status as a `stale: true` loadErrors item rather than only on stderr.
    */
   private staleCompileErrors: Map<
      string,
      { message: string; failedAt: string }
   > = new Map();
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
   private environmentPath: string;
   private environmentName: string;
   // Resolves a package's latest persisted manifest entries (both tiers), so
   // serve routing is re-established whenever a package (re)loads, not only
   // after a build. Undefined ⇒ no re-bind on load. See
   // {@link rebindServeBindingsFromLocalStore}.
   private storageBindingResolver?: (
      packageName: string,
   ) => Promise<Record<string, ManifestEntry>>;
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
      this.malloyConfig = malloyConfig;
      this.apiConnections = apiConnections;
      this.destinations = [];
      this.setStorageDestinations(storageDestinations);
      this.metadata = {
         resource: `${API_PREFIX}/environments/${this.environmentName}`,
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
         resource: `${API_PREFIX}/environments/${this.environmentName}`,
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
         },
         packageName,
         modelName,
         source,
         includeSql,
         givens,
         scope,
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

   /** Drop every loaded model's memoized serve shape; the next query recompiles. */
   private invalidateServeShapes(): void {
      for (const _package of this.packages.values()) {
         _package.invalidateServeShapes();
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
    */
   public getLoadedPackages(): Package[] {
      return [...this.packages.values()];
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
      options: { includeLoading?: boolean } = {},
   ): Promise<ApiPackage[]> {
      logger.debug("Listing packages", {
         environmentPath: this.environmentPath,
      });
      try {
         const names = new Set<string>([
            ...this.packageStatuses.keys(),
            ...(options.includeLoading ? this.loadsInFlight.keys() : []),
         ]);
         const packageMetadata = await Promise.all(
            Array.from(names).map(async (packageName) => {
               const registered = this.packageStatuses.get(packageName)?.status;
               if (registered === PackageStatus.UNLOADING) {
                  return undefined;
               }
               try {
                  const resident = this.packages.get(packageName);
                  let metadata: ApiPackage | undefined;
                  if (resident !== undefined) {
                     metadata = resident.getPackageMetadata();
                  } else if (this.loadsInFlight.has(packageName)) {
                     if (!options.includeLoading) {
                        return undefined;
                     }
                     metadata = { name: packageName };
                  } else if (registered === PackageStatus.LOADING) {
                     // An interrupted load: reloading it from this read path
                     // would repeat the work on every listing.
                     return undefined;
                  } else {
                     metadata = (
                        await this.getPackage(packageName, false)
                     ).getPackageMetadata();
                  }
                  metadata.name = packageName;
                  metadata.status = this.describePackageStatus(packageName);
                  return metadata;
               } catch (error) {
                  logger.error(
                     `Failed to load package: ${packageName} due to : ${error}`,
                  );
                  // Redacted because getStatus returns it in a response body,
                  // and a Postgres/DuckLake ATTACH failure carries the
                  // connection string (keyword `password=` or URI userinfo).
                  this.failedPackages.set(
                     packageName,
                     redactPgSecrets(
                        error instanceof Error ? error.message : String(error),
                     ),
                  );
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
      let entry = this.loadsInFlight.get(packageName);
      while (entry !== undefined) {
         await entry.settled;
         entry = this.loadsInFlight.get(packageName);
      }
   }

   /**
    * The `Package.status` the API reports: whether a compiled copy serves, and
    * whether a load is in progress. Lock-free, so it answers during the load.
    */
   public describePackageStatus(packageName: string): ApiPackageStatus {
      const inFlight = this.loadsInFlight.get(packageName);
      return {
         serving: this.packages.has(packageName),
         loading: inFlight !== undefined,
         ...(inFlight !== undefined
            ? { loadingSince: new Date(inFlight.since).toISOString() }
            : {}),
      };
   }

   /**
    * Run `fn` with the package marked as loading (see `loadsInFlight`). Every
    * path that compiles a copy runs inside this, from acceptance (download
    * included) until it settles or rolls back.
    */
   private async trackPackageLoad<T>(
      packageName: string,
      fn: () => Promise<T>,
   ): Promise<T> {
      const current = this.loadsInFlight.get(packageName);
      let settle = current?.settle;
      const settled =
         current?.settled ??
         new Promise<void>((resolve) => {
            settle = resolve;
         });
      this.loadsInFlight.set(packageName, {
         count: (current?.count ?? 0) + 1,
         since: current?.since ?? Date.now(),
         settled,
         settle: settle!,
      });
      try {
         return await fn();
      } finally {
         const entry = this.loadsInFlight.get(packageName);
         if (entry !== undefined && entry.count > 1) {
            entry.count -= 1;
         } else {
            this.loadsInFlight.delete(packageName);
            entry?.settle();
         }
      }
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
    * Safe at startup: only `installPackage` / `deletePackage` use them.
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
      resolver: (packageName: string) => Promise<Record<string, ManifestEntry>>,
   ): void {
      this.storageBindingResolver = resolver;
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
      if (pkg.getPackageMetadata().manifestLocation) return;
      const packageName = pkg.getPackageName();
      try {
         const rawEntries = await this.storageBindingResolver(packageName);
         if (Object.keys(rawEntries).length === 0) return;
         const { tableNameManifest, storageEntries } = splitManifestEntries(
            rawEntries,
            `local store (package ${packageName})`,
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
      return this.packages.get(name);
   }

   public async getPackage(
      packageName: string,
      reload: boolean = false,
      options: { allowAdmission?: boolean } = {},
   ): Promise<Package> {
      assertSafePackageName(packageName);
      // Lock-free fast path: `Package` references are immutable, and
      // disk-reading methods take the lock themselves.
      //
      // INVARIANT: code reachable from a fast-path Package (MCP query tools,
      // Model.getModel()) must stay in-memory; any new disk I/O against the
      // canonical tree needs `withPackageLock`, or it races install/delete.
      const _package = this.packages.get(packageName);
      if (_package !== undefined && !reload) {
         return _package;
      }

      this.assertCanAdmitNewPackage(
         packageName,
         reload ? "reload a package" : "load a package",
         options.allowAdmission === true,
      );

      return this.withPackageLock(packageName, () =>
         this._loadOrGetPackageLocked(packageName, reload),
      );
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

      return this.trackPackageLoad(packageName, async () => {
         this.setPackageStatus(packageName, PackageStatus.LOADING);

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
            if (existingPackage !== undefined && reload) {
               // A failed reload keeps serving the last good model (replaced
               // only on success) rather than evicting the package.
               this.setPackageStatus(packageName, PackageStatus.SERVING);
               // Recorded here so every reload caller (watcher, MCP, REST)
               // reports the staleness to /status; cleared on the next
               // successful load.
               this.staleCompileErrors.set(packageName, {
                  message: redactPgSecrets(
                     error instanceof Error ? error.message : String(error),
                  ),
                  failedAt: new Date().toISOString(),
               });
            } else {
               this.packages.delete(packageName);
               this.packageStatuses.delete(packageName);
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
      this.notifyPackageLoaded(pkg);
      this.setPackageStatus(packageName, PackageStatus.SERVING);
      // A package that failed earlier can be fixed without a restart.
      this.clearPackageLoadFailure(packageName);
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

      return this.trackPackageLoad(packageName, async () => {
         this.setPackageStatus(packageName, PackageStatus.LOADING);
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
            this.deletePackageStatus(packageName);
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
      return this.trackPackageLoad(packageName, () =>
         this._installPackageTracked(
            packageName,
            downloader,
            validate,
            options,
         ),
      );
   }

   private async _installPackageTracked(
      packageName: string,
      downloader: (stagingPath: string) => Promise<void>,
      validate: ((pkg: Package) => string | undefined) | undefined,
      options: { update?: ApiPackage },
   ): Promise<Package> {
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

      return this.withPackageLock(packageName, async () => {
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

         let newPackage: Package;
         try {
            await fs.promises.rename(stagingPath, canonicalPath);

            this.setPackageStatus(packageName, PackageStatus.LOADING);
            newPackage = await Package.create(
               this.environmentName,
               packageName,
               canonicalPath,
               () => this.malloyConfig.malloyConfig,
               // Freshly staged, so a failed load's half-built tree is ours to
               // remove.
               true,
            );
            this.attachDestinationServeConfig(newPackage);
            // Publish/update only (reload passes no validator). Thrown inside
            // the try so the swap rolls back.
            const validationMsg = validate?.(newPackage);
            if (validationMsg) {
               throw new BadRequestError(validationMsg);
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
         if (options.update !== undefined) {
            await this._updatePackageLocked(packageName, options.update, {
               recordLocation: true,
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
         // The old package still serves; deleting its status would hide it
         // from listings (which enumerate packageStatuses) until a restart.
         this.setPackageStatus(packageName, PackageStatus.SERVING);
      } else {
         // Nothing to fall back to, or the restore failed: the cached package
         // no longer matches disk, so drop it from both maps.
         this.deletePackageStatus(packageName);
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
   ): Promise<void> {
      assertSafePackageName(packageName);
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
   ): Promise<void> {
      assertSafePackageName(packageName);
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
   ): Promise<void> {
      assertSafePackageName(packageName);
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

   public async updatePackage(packageName: string, body: ApiPackage) {
      assertSafePackageName(packageName);
      // An install downloads before taking the lock, so a PATCH arriving then
      // would land on the copy about to be swapped away. Wait for loads in
      // flight and apply to whatever is resident afterwards (or 404).
      await this.awaitPackageLoads(packageName);
      return this.withPackageLock(packageName, () =>
         this._updatePackageLocked(packageName, body, {
            recordLocation: false,
         }),
      );
   }

   /** Apply a metadata PATCH to a loaded package. Caller holds the package mutex. */
   private async _updatePackageLocked(
      packageName: string,
      body: ApiPackage,
      options: {
         /**
          * Record the body's `location` as the install location. True only
          * for an install; a PATCH naming a new location is a reinstall.
          */
         recordLocation: boolean;
      },
   ) {
      const _package = this.packages.get(packageName);
      if (!_package) {
         throw new PackageNotFoundError(`Package ${packageName} not found`);
      }
      if (body.name) {
         _package.setName(body.name);
      }
      // `setPackageMetadata` replaces the whole object, so omitted fields
      // carry over; a wiped `explores` would silently serve every model.
      const existing = _package.getPackageMetadata();
      // Normalized as the worker normalizes on-disk explores, so publish and
      // reload agree.
      const normalizedExplores = body.explores?.map(normalizeModelPath);
      const explores =
         normalizedExplores !== undefined
            ? normalizedExplores
            : existing.explores;
      const queryableSources =
         body.queryableSources != null
            ? body.queryableSources
            : existing.queryableSources;
      // An explicit null clears it, reverting to live.
      const manifestLocation =
         body.manifestLocation !== undefined
            ? body.manifestLocation
            : existing.manifestLocation;
      // `scope` and `materialization` apply when provided and are otherwise
      // preserved; a schedule change re-arms the scheduler on its next tick.
      // Null counts as omitted, so a client serializing unset fields as null
      // neither trips the policy gate below (failing an orchestrated run) nor
      // resets the policy. `manifestLocation` differs: null there clears.
      const scopeProvided = body.scope != null;
      const materializationProvided = body.materialization != null;
      const editingPolicy = scopeProvided || materializationProvided;
      const scope = scopeProvided ? body.scope : existing.scope;
      // Preserved the same way (null = omitted; the clear is an empty bag),
      // resolved once across both wire homes and written to both: the serve
      // path reads the canonical field and the build path the block, so they
      // must not disagree.
      const queryMetadata =
         body.queryMetadata ??
         body.materialization?.queryMetadata ??
         existing.queryMetadata ??
         existing.materialization?.queryMetadata ??
         null;
      const materializationBase = materializationProvided
         ? body.materialization
         : existing.materialization;
      const materialization =
         materializationBase || queryMetadata !== null
            ? { ...(materializationBase ?? {}), queryMetadata }
            : materializationBase;
      // Omitted (or null) fields carry over, so a rebind PATCH keeps the
      // `location` a reload reinstalls from and the `resource` an
      // orchestrator identifies the package by. An empty string is a value.
      _package.setPackageMetadata({
         name: body.name != null ? body.name : existing.name,
         description:
            body.description != null ? body.description : existing.description,
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
      const invalidMsg = [exploresMsg, policyMsg].filter(Boolean).join("\n");
      if (invalidMsg) {
         _package.setPackageMetadata(existing);
         throw new BadRequestError(invalidMsg);
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

   public getPackageStatus(packageName: string): PackageInfo | undefined {
      return this.packageStatuses.get(packageName);
   }

   /**
    * Packages actually serving: registered statuses minus failed ones.
    * Disjoint from getFailedPackages() so the readiness line never
    * double-counts a package seeded SERVING at boot. Triggers no load.
    */
   public getServingPackageCount(): number {
      const failed = this.getFailedPackages();
      let serving = 0;
      for (const name of this.packageStatuses.keys()) {
         if (!failed.has(name)) serving += 1;
      }
      return serving;
   }

   /** Record why a configured package's location never mounted (see {@link mountErrors}). */
   public setPackageMountError(packageName: string, message: string): void {
      this.mountErrors.set(packageName, message);
   }

   /**
    * Record a runtime add that failed before the package could serve, for
    * /status. Skipped while the name has any status (a failed re-install
    * rolled back, so nothing failed). Names are the caller's, so the record
    * is capped at MAX_RECORDED_ADD_FAILURES, oldest dropped first; boot-time
    * mount errors are not capped.
    */
   public recordPackageAddFailure(packageName: string, message: string): void {
      if (this.packageStatuses.has(packageName)) return;
      if (!this.mountErrors.has(packageName)) {
         this.recordedAddFailures.push(packageName);
         while (this.recordedAddFailures.length > MAX_RECORDED_ADD_FAILURES) {
            const evicted = this.recordedAddFailures.shift();
            if (evicted !== undefined) this.mountErrors.delete(evicted);
         }
      }
      this.mountErrors.set(packageName, message);
   }

   /** Forget any recorded failure for a package, whatever its cause. */
   private clearPackageLoadFailure(packageName: string): void {
      this.failedPackages.delete(packageName);
      this.mountErrors.delete(packageName);
      const recorded = this.recordedAddFailures.indexOf(packageName);
      if (recorded !== -1) this.recordedAddFailures.splice(recorded, 1);
      this.staleCompileErrors.delete(packageName);
   }

   /**
    * Packages configured for, or added to, this environment that did not load,
    * and why.
    */
   public getFailedPackages(): ReadonlyMap<string, string> {
      if (this.mountErrors.size === 0) return this.failedPackages;
      // Mount errors last, so the specific cause overwrites the generic
      // manifest error that the un-mounted package produces on its lazy load.
      return new Map([...this.failedPackages, ...this.mountErrors]);
   }

   /**
    * SERVING packages whose most recent reload failed to compile: the served
    * model is stale relative to the files on disk. Disjoint from
    * {@link getFailedPackages} by construction (a successful load clears the
    * entry; a failed first load lands in failedPackages instead).
    */
   public getStaleCompileErrors(): ReadonlyMap<
      string,
      { message: string; failedAt: string }
   > {
      return this.staleCompileErrors;
   }

   public setPackageStatus(packageName: string, status: PackageStatus): void {
      const currentStatus = this.packageStatuses.get(packageName);
      this.packageStatuses.set(packageName, {
         name: packageName,
         loadTimestamp: currentStatus?.loadTimestamp || Date.now(),
         status: status,
      });
   }

   public deletePackageStatus(packageName: string): void {
      this.packageStatuses.delete(packageName);
   }

   public async deletePackage(packageName: string): Promise<void> {
      assertSafePackageName(packageName);
      return this.withPackageLock(packageName, async () => {
         // Before the early return: a failed package is never in `packages`,
         // and a stale entry would report a loadError for an unconfigured one.
         this.clearPackageLoadFailure(packageName);

         const _package = this.packages.get(packageName);
         if (!_package) {
            return;
         }
         const packageStatus = this.packageStatuses.get(packageName);

         // A guard for anything that bypasses the package lock.
         if (packageStatus?.status === PackageStatus.LOADING) {
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
         } else if (packageStatus?.status === PackageStatus.SERVING) {
            this.setPackageStatus(packageName, PackageStatus.UNLOADING);
         }

         // Drained so in-flight queries finish before the handle is released.
         this.retireConnectionGeneration(`package ${packageName}`, () =>
            _package.getMalloyConfig().shutdown("close"),
         );

         // Rename aside under the lock; the slow recursive remove is deferred.
         const canonicalPath = safeJoinUnderRoot(
            this.environmentPath,
            packageName,
         );
         const retiredPath = this.allocateRetiredPath(packageName);
         let renamed = false;
         try {
            await fs.promises.mkdir(path.dirname(retiredPath), {
               recursive: true,
            });
            await fs.promises.rename(canonicalPath, retiredPath);
            renamed = true;
         } catch (err) {
            logger.error(
               "Error renaming package directory to retired during unload",
               {
                  error: err,
                  environmentName: this.environmentName,
                  packageName,
               },
            );
         }

         this.packages.delete(packageName);
         this.packageStatuses.delete(packageName);
         await fs.promises
            .rm(this.installRecordPath(packageName), { force: true })
            .catch(() => {});

         if (renamed) {
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
         if (
            this.packageStatuses.get(packageName)?.status ===
            PackageStatus.SERVING
         ) {
            this.setPackageStatus(packageName, PackageStatus.UNLOADING);
         }
         this.retireConnectionGeneration(`package ${packageName}`, () =>
            _package.getMalloyConfig().shutdown("close"),
         );
         // Eviction and clearing a recorded failure belong together.
         this.clearPackageLoadFailure(packageName);
         this.packages.delete(packageName);
         this.packageStatuses.delete(packageName);
      });
   }

   public updateConnections(
      malloyConfig: EnvironmentMalloyConfig,
      _apiConnections?: ApiConnection[],
      afterPreviousRelease?: () => Promise<void>,
   ): void {
      const previousMalloyConfig = this.malloyConfig;
      this.malloyConfig = malloyConfig;
      this.apiConnections = malloyConfig.apiConnections;

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

   public async closeAllConnections(): Promise<void> {
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
      this.packageStatuses.clear();

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

      logger.info(
         `Closed all connections for environment ${this.environmentName}`,
      );
   }

   public async serialize(
      options: { includeLoading?: boolean } = {},
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
