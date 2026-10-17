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

export {
   COMPILE_SCOPES,
   packageRelativeModelPath,
   type CompileScope,
   type TaggedLogMessage,
} from "./compile_source";

/**
 * Sibling dirs under `environmentPath` used by the install/delete pipeline so
 * that long downloads do not hold the per-package mutex.
 *
 *  - `.staging/<pkg>-<uuid>/` — a download in progress. Renamed to the
 *    canonical path under the lock once complete.
 *  - `.retired/<pkg>-<uuid>/` — the previous canonical tree, atomically
 *    renamed out of the way during a swap or delete. `fs.rm`'d asynchronously
 *    after the lock is released.
 *
 * Both hold PACKAGE TREES, in the same directory the canonical ones live in, so
 * both are dot-prefixed: anything that enumerates an environment looking for
 * packages must not find a half-downloaded or already-superseded copy. No such
 * enumeration exists today — `listPackages` reads the registered names and this
 * sweep removes these two paths by name — so the prefix is what keeps that true
 * for whatever walks the directory next, rather than a guard on a live path.
 */
const STAGING_DIR_NAME = ".staging";
const RETIRED_DIR_NAME = ".retired";

// How long to wait for a control-plane manifest fetch during (re)bind before
// giving up and serving live. Binding happens before a package is marked
// SERVING, so an unreachable/slow manifest store must not block the package.
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
 * The only fields of a storage destination any read reports, so an
 * operator or orchestrator can see which destinations a worker holds without
 * being handed their warehouse credentials. Also the test for a write entry that
 * is a reference to a stored destination rather than a new config for it — see
 * {@link Environment.setStorageDestinations}.
 */
const DESTINATION_READ_FIELDS: ReadonlySet<string> = new Set([
   "name",
   "type",
   "resource",
]);

/**
 * Admission-rejection counters. The environment name is a label so
 * dashboards can identify hot environments without grepping logs.
 */
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

   // Scope has two homes: `materialization.scope` (canonical) and the
   // manifest root (deprecated). The server writes BOTH, in sync, for as
   // long as the root form is supported:
   //
   //  - writing only the root would author a manifest this build's loader
   //    refuses, since a root that disagrees with an existing envelope is a
   //    conflict (see resolvePackageScope);
   //  - writing only the envelope would silently downgrade a package read
   //    by an older publisher, which knows only the root and would default
   //    to `package` — cross-version table reuse for a package declared
   //    `version`.
   //
   // A caller can only express scope through the top-level `scope` field
   // (the wire materialization block has no `scope`), so an envelope value
   // already on disk is preserved rather than dropped by a materialization
   // PATCH that says nothing about it.
   const resolvedScope =
      metadata.scope ??
      (onDiskMaterialization?.scope as ApiPackage["scope"] | undefined) ??
      (existingManifest.scope as ApiPackage["scope"] | undefined);

   // A materialization PATCH replaces the block wholesale, which is right
   // for schedule and freshness — they are the policy the caller is
   // setting, and they are mutually exclusive with each other.
   // `queryMetadata` is orthogonal to both: a client setting a schedule
   // has no reason to re-send the package's tags, and dropping them
   // silently untags every statement the package's builds issue. So it is
   // preserved on omission, like `scope` above.
   //
   // A NULL is preserve too, not a clear — one rule at both wire homes.
   // Null used to clear here while null on the canonical field preserved,
   // which protected a client that serializes unset fields as null only
   // if it had already migrated off this home. The clear is an EMPTY bag,
   // which is unambiguous and which no such client emits by accident.
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

   // `queryMetadata` has two homes as well, migrating the opposite way to
   // `scope`: the manifest ROOT is canonical and the envelope is
   // deprecated. Same dual-write rule and the same reason — writing only
   // the root would leave an older publisher, which reads only the
   // envelope, serving untagged statements for a package that asked to be
   // tagged.
   //
   // A caller can express tags at either wire home, so precedence follows
   // the same order the manifest resolver uses: the canonical top-level
   // field, then the deprecated block, then what is already on disk. A
   // client that has not migrated keeps working; one that has is not
   // overruled by a block it did not send.
   //
   // A null anywhere in this chain is "not provided", never a clear — the
   // one rule at both homes (see preservedQueryMetadata). Clearing is an
   // empty bag, which parses to "no tags" and reaches both homes like any
   // other value.
   const resolvedQueryMetadata =
      metadata.queryMetadata ??
      (materializationBlock as { queryMetadata?: unknown } | undefined)
         ?.queryMetadata ??
      (existingManifest as { queryMetadata?: unknown }).queryMetadata;

   // Update with new metadata. `explores`/`queryableSources` are only
   // overwritten when the caller explicitly provides them; otherwise the
   // existing on-disk value is preserved via the spread (an undefined here
   // must not erase it).
   // A convention-derived surface is never written. A GET of an
   // index.malloy-curated package echoes the surface the server derived,
   // so an ordinary read-modify-write PATCH -- one that meant to change
   // only the description -- hands that derived value back, and writing
   // it would freeze a surface that tracks the file into a key that does
   // not. The two behave identically until the file is renamed or
   // replaced: the convention then follows it, while the frozen key names
   // a model that no longer exists, and the package silently lists and
   // serves nothing. Recognizable because it is the exact value the
   // convention produces, in a manifest that declares no `explores` (a
   // surface naming a file that is not there was already rejected
   // upstream by formatInvalidExplores, so the file exists). Declaring it
   // by hand buys nothing the convention does not already give.
   const echoesDerivedSurface =
      existingManifest.explores === undefined &&
      Array.isArray(metadata.explores) &&
      metadata.explores.length === 1 &&
      metadata.explores[0] === INDEX_MODEL_NAME;

   return {
      ...existingManifest,
      name: metadata.name,
      // Only when provided: an undefined here is dropped by
      // JSON.stringify and so would erase the description on disk, which
      // a PATCH that did not mention it never meant.
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
      // Mirrored into the deprecated home too, so a package tagged
      // through the canonical field still reads as tagged on a publisher
      // that only knows the envelope. Only when that home already exists
      // or the caller wrote to it: mirroring unconditionally introduced a
      // `materialization` block into a manifest that never had one, which
      // is the shape being migrated AWAY from, appearing in a file whose
      // author only ever used the canonical field.
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
   // packageMutex. Connection updates may invalidate cached package
   // MalloyConfigs and force reloads, so the environment lock is the outer one.
   // Never acquire connectionMutex while holding a packageMutex — that's the
   // AB/BA deadlock path.
   private packageMutexes = new Map<string, Mutex>();
   private packageStatuses: Map<string, PackageInfo> = new Map();
   /**
    * Packages with a load, reinstall or recompile in progress here, keyed by
    * name, with when the first in-flight operation began. This is what
    * `Package.status.loading` reports. It is kept apart from
    * `packageStatuses`, whose LOADING/SERVING/UNLOADING answers "is a copy
    * registered to serve", because the two are independent: a reload of a
    * serving package is in flight here while the previous copy stays in
    * `packages` and keeps answering queries. Entries are reference-counted so
    * a nested operation (a reinstall that then rebinds a manifest) stays
    * marked until the outermost one finishes.
    */
   private loadsInFlight: Map<
      string,
      {
         count: number;
         since: number;
         /**
          * Resolves once every load counted here has finished, however each
          * ended. Loads of one package started independently (a reinstall
          * racing a reload, a lazy load joined by an install) share the entry,
          * so a waiter is released only when the last of them is done.
          */
         settled: Promise<void>;
         settle: () => void;
      }
   > = new Map();
   /**
    * Configured packages that failed to load, keyed by name, with the reason.
    *
    * A load failure is not fatal: the package is omitted and its siblings serve
    * on. It is also observable exactly once, because the failing load deletes
    * the `packageStatuses` entry that `listPackages` enumerates, so the next
    * listing no longer knows the package was ever configured. That makes this
    * the only lasting record. Read by EnvironmentStore.getStatus.
    */
   private failedPackages: Map<string, string> = new Map();
   /**
    * Why a configured package never reached the disk, keyed by package name.
    *
    * Separate from {@link failedPackages} because it is the more specific
    * answer and has to win. A package whose location failed to mount is still
    * seeded SERVING by addEnvironment, so its later lazy load fails on the
    * manifest that was never copied and `listPackages` records that instead.
    * Reporting "Package manifest ... does not exist." for what was really a
    * typo'd `location` sends the reader hunting in the wrong place.
    */
   private mountErrors: Map<string, string> = new Map();
   /** Runtime add failures recorded in {@link mountErrors}, oldest first. */
   private recordedAddFailures: string[] = [];
   /**
    * Why a SERVING package's most recent reload failed to compile, keyed by
    * package name.
    *
    * Separate from {@link failedPackages} because the package is NOT failed:
    * a failed reload keeps the last good compiled model serving (see
    * {@link _loadOrGetPackageLocked}), so `getFailedPackages()` and the
    * serving counters must not include it. What this records is staleness:
    * the model answering queries is older than the files on disk. Without it
    * a watch-mode recompile failure is visible only on stderr, and /status
    * keeps reporting a healthy server while queries answer from the previous
    * model. Read by EnvironmentStore.getStatus, which reports each entry as a
    * loadErrors item with `stale: true`.
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
    * Warehouses materialization builds write to and materialized queries are
    * served from. Disjoint from {@link apiConnections}: a destination is not a
    * connection, so it is absent from `listApiConnections`, unresolvable by name
    * from a user's model, and free to share a name with a connection without
    * colliding with it. Two lists rather than one flagged list so that every
    * existing connection consumer excludes destinations structurally, instead of
    * each having to remember a filter.
    */
   private destinations: ApiConnection[];
   /**
    * The live Malloy connections for {@link destinations}, assembled exactly the
    * way the user-facing ones are but from the destination list — so a
    * materialization serve shape can reach a destination while nothing in the
    * namespace a package compiles against can. Rebuilt whenever the list is
    * replaced, retiring the previous generation's handles.
    *
    * Always assigned by the time anything can read it: the constructor sets the
    * destination list unconditionally, and `setStorageDestinations` treats an
    * unbuilt config as a reason to build even when the list has not changed —
    * which is what makes the assertion here true for an environment with no
    * destinations at all.
    */
   private destinationMalloyConfig!: EnvironmentMalloyConfig;
   /**
    * Whether {@link destinations} is the authoritative set for this environment —
    * i.e. safe to reconcile the stored rows against.
    *
    * False when a load could not READ the stored destinations. The list is then
    * "unknown", not "empty", and the two are not interchangeable: the database
    * sync prunes rows the list does not hold, so treating a failed read as an
    * empty list turns a transient error into permanently deleted registrations.
    * An explicit set (config or API) makes it authoritative again.
    */
   private destinationsAuthoritative = true;
   private environmentPath: string;
   private environmentName: string;
   // Resolves a package's latest persisted materialization manifest entries
   // (the full map — colocated tableName entries AND `storage=` cross-connection
   // entries), so serve routing for BOTH tiers is re-established when a package
   // (re)loads — e.g. after a worker restart — instead of only when a build's
   // auto-load runs. Injected by the EnvironmentStore, which owns the
   // materialization repository. Undefined ⇒ no re-bind on load (routing then
   // depends on a fresh build, the old behavior). See
   // {@link rebindServeBindingsFromLocalStore}.
   private storageBindingResolver?: (
      packageName: string,
   ) => Promise<Record<string, ManifestEntry>>;
   public metadata: ApiEnvironment;
   // The shared memory governor that consults process RSS. Optional —
   // when null the gate is a no-op and the environment behaves exactly
   // like it did before the governor was introduced. Set by
   // EnvironmentStore.setMemoryGovernor at server start so we keep the
   // governor as the single owner of the back-pressure boolean.
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
      // Sanitizer barrier: every downstream `path.join(this.environmentPath,
      // …)` site (including the static `sweepStaleInstallDirs` sweep) gets a
      // value that has cleared an allowlist check at the gate.
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
      // Ahead of the readme write, so a body carrying a destination list we
      // cannot read is refused before this method has changed anything.
      //
      // Absent means "leave alone", so a caller updating only the readme or the
      // connections cannot blank the destination list by omission. Anything else
      // present is acted on, including a shape we cannot read: the list replaces
      // what is stored, so "we did not understand this" must not resolve to "then
      // keep none of them". An explicit empty list is a different thing and does
      // clear it.
      if (payload.storageDestinations !== undefined) {
         this.setStorageDestinations(payload.storageDestinations, {
            rejectInvalid: true,
         });
      }

      if (payload.readme !== undefined) {
         this.metadata.readme = payload.readme;
         await this.writeEnvironmentReadme(payload.readme);
      }

      // Handle connections update
      // TODO: Update environment connections should have its own API endpoint
      if (payload.connections) {
         const payloadConnections = payload.connections;
         await this.runConnectionUpdateExclusive(async () => {
            logger.info(
               `Updating ${payloadConnections.length} connections for environment ${this.environmentName}`,
            );
            // This list replaces the stored one wholesale, and responses no
            // longer carry credentials, so an entry echoed back from a read
            // arrives without one. Merge each entry against what is stored, the
            // same "an entry that names a stored one and carries no config
            // keeps it" rule storageDestinations already documents. Without
            // this, adding or deleting ANY connection through the app strips
            // the credentials of every other connection in the environment,
            // because the app sends the whole list back. An entry with no
            // stored counterpart is new and passes through as sent.
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

      // Best-effort: a previous run may have crashed mid-install or
      // mid-delete and left orphan dirs under .staging/ or .retired/.
      // Run against the validated constructor argument so the sink path
      // here does NOT route through `this` (which CodeQL conservatively
      // treats as tainted because other methods on this class touch
      // request-derived `packageName` values).
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
         // Readme not found, so we'll just return an empty string
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
    * Replaces the destination list. Every entry is re-validated here, so this is
    * also the barrier that keeps an unvalidated destination off the environment
    * however it arrived — config file or request body.
    *
    * An entry that carries nothing but the fields a read reports is resolved
    * against the stored list first, so a read-modify-write of the environment
    * keeps the configs it was never shown.
    *
    * `rejectInvalid` picks what an entry we cannot use means. A config file or a
    * restored row is a source that cannot be asked to fix it, so the default
    * drops the entry and keeps serving. A request body can be refused, and is:
    * see {@link processStorageDestinationsOrThrow}.
    */
   public setStorageDestinations(
      storageDestinations: ApiConnection[],
      { rejectInvalid = false }: { rejectInvalid?: boolean } = {},
   ): void {
      const previous = this.destinations;
      // Resolved before validation so an entry that legitimately carries no
      // config of its own — the "keep this one" reference — is validated as the
      // stored destination it names, not as the bare reference.
      const requested = Array.isArray(storageDestinations)
         ? storageDestinations.map((destination) =>
              this.resolveDestinationReference(destination),
           )
         : storageDestinations;
      // Throws before anything is assigned, so a refused update leaves the
      // environment exactly as it was.
      this.destinations = rejectInvalid
         ? processStorageDestinationsOrThrow(requested)
         : processStorageDestinations(requested);
      // An explicit set makes the list authoritative again: whatever could not be
      // read before, this is now the set the store should be reconciled to.
      this.destinationsAuthoritative = true;

      // Nothing to swap when the resolved list describes the same destinations.
      // An orchestrator that reconciles by re-pushing its whole desired state on
      // a loop would otherwise re-attach every destination on every cycle and
      // drop the serve shapes compiled against them, so the comparison ignores
      // list order and config key order, neither of which changes what a
      // destination is.
      //
      // "Same as before" only means there is nothing to do once something has
      // been built. On the constructor's call both lists are empty for every
      // environment with no destinations, so skipping on equality alone would
      // leave the config unassigned for the common case, not the rare one.
      if (
         this.destinationMalloyConfig &&
         storageDestinationsEqual(previous, this.destinations)
      ) {
         return;
      }

      this.rebuildDestinationMalloyConfig();
      // Quiet for the overwhelmingly common case of an environment with no
      // destinations at all, loud for every transition that matters, including
      // one that empties the list.
      if (previous.length > 0 || this.destinations.length > 0) {
         logger.info(
            `Environment ${this.environmentName} has ${this.destinations.length} storage destination(s)`,
            { destinations: this.destinations.map((d) => d.name) },
         );
      }
   }

   /**
    * Substitutes the stored destination for an entry that names one and carries
    * no config of its own. Anything carrying a config is returned untouched and
    * replaces what is stored; a reference to a name that is not stored is
    * returned untouched too, and then fails validation like any config-less
    * entry.
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
    * Give a freshly loaded package the connections its materialization serve
    * shapes compile against — this environment's destinations, resolved live so a
    * destination-list swap propagates without a package reload.
    *
    * Deliberately a push rather than a `Package.create` argument: the package
    * config a model compiles against must never contain a destination, so this is
    * the only route by which one reaches a compile at all, and it feeds only the
    * synthetic serve shape. Missing it costs serve routing (queries fall back to
    * live), never correctness — so it runs before serve bindings are pushed,
    * which is what routing actually requires.
    */
   private attachDestinationServeConfig(_package: Package): void {
      _package.setServeDestinationConfig(() =>
         this.getStorageDestinationMalloyConfig(),
      );
   }

   /**
    * (Re)assemble the destination connections after the list changed, draining
    * the previous generation's handles on the same delay a connection-generation
    * swap uses.
    *
    * A failure here is not fatal to the environment: destinations are an add-on,
    * and an environment that cannot assemble them still serves its packages —
    * builds refuse and materialized queries fall back to live.
    */
   private rebuildDestinationMalloyConfig(): void {
      const previous = this.destinationMalloyConfig;
      try {
         // Rooted apart from the connections' files so a destination can never
         // share a pooled DuckDB instance with a connection of the same name —
         // see STORAGE_DESTINATIONS_DIR. Created here because the
         // directory has to exist before the first lookup opens a database in it.
         const destinationRoot = storageDestinationRoot(this.environmentPath);
         if (this.destinations.length > 0) {
            fs.mkdirSync(destinationRoot, { recursive: true });
         }
         this.destinationMalloyConfig = buildEnvironmentMalloyConfig(
            this.destinations,
            destinationRoot,
         );
      } catch (error) {
         logger.error(
            `Failed to assemble storage destinations for environment ${this.environmentName}; serving without them`,
            { error },
         );
         this.destinationMalloyConfig = buildEnvironmentMalloyConfig(
            [],
            storageDestinationRoot(this.environmentPath),
         );
      }
      if (previous && previous !== this.destinationMalloyConfig) {
         this.retireConnectionGeneration(
            `environment ${this.environmentName} destinations`,
            () => previous.releaseConnections(),
         );
         // A loaded model memoizes the serve shape it compiled, and that shape
         // holds the connections of the generation just retired — which are
         // released once the drain elapses. The memo is keyed on the BINDING set,
         // so a destination change does not change the key and the stale shape
         // would be reused until the package reloaded: every routed query for it
         // failing over to live, permanently and quietly. Dropped here, after the
         // swap, so the next query recompiles against the config now installed.
         this.invalidateServeShapes();
      }
   }

   /**
    * Drop every loaded model's memoized materialization serve shape. Cheap: the
    * next routed query recompiles one, and a package with no `storage=` bindings
    * has none to drop.
    */
   private invalidateServeShapes(): void {
      for (const _package of this.packages.values()) {
         _package.invalidateServeShapes();
      }
   }

   /**
    * The connections a materialization serve shape may compile against. Separate
    * from {@link getEnvironmentMalloyConfig} — which is what a package's models
    * fall through to — so the two compiles resolve disjoint name sets. Handing
    * out the same object for both is exactly the mistake this split exists to
    * make impossible.
    */
   public getStorageDestinationMalloyConfig() {
      return this.destinationMalloyConfig.malloyConfig;
   }

   /**
    * Records that this environment's stored destinations could not be read, so
    * {@link listStorageDestinations} is a fallback rather than the
    * authoritative set. Callers that reconcile storage must not prune against it.
    */
   public markStorageDestinationsUnknown(): void {
      this.destinationsAuthoritative = false;
   }

   /** See {@link destinationsAuthoritative}. */
   public hasAuthoritativeStorageDestinations(): boolean {
      return this.destinationsAuthoritative;
   }

   /**
    * The destinations configured for this environment, with their configs.
    * Deliberately not exposed by any controller: a destination config carries
    * warehouse credentials and the destination has no endpoint of its own, so
    * nothing can fetch or probe one.
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
    * connection list: a `storage=` build naming a destination that is not
    * configured must fail rather than write into a same-named connection, which
    * would be the tenant's own warehouse.
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
    * Snapshot of the packages currently loaded in memory (does not trigger a
    * load or reload). Used by the standalone materialization scheduler to sweep
    * only already-loaded packages — a not-yet-loaded package is simply not
    * scheduled until something else loads it, so the scheduler never forces a
    * load of its own.
    */
   public getLoadedPackages(): Package[] {
      return [...this.packages.values()];
   }

   /**
    * The packages this environment holds, as the API describes them.
    *
    * A package whose compiled copy is resident is described from that copy
    * without taking its lock, so a reload in progress (which holds the lock
    * and has flipped the registered status to LOADING) is listed with
    * `status.loading` rather than hidden: the previous copy is still what
    * answers queries, and a listing that omitted it would read as the package
    * having left this server. A registered package that is not resident is
    * loaded here, as before, which is what brings an environment's configured
    * packages into memory on first listing.
    *
    * A package that is loading for the first time has nothing compiled to
    * describe. It is listed, as its name and `status` alone, only when
    * `includeLoading` is set; `GET /status?includeLoading=true` sets it so an
    * orchestrator that reads `status` can tell a load it dispatched from a
    * package that is absent. Every other listing leaves it out: a listed
    * package has always meant one that can serve here, and a consumer that
    * reads the listing that way would otherwise route to a copy still
    * downloading or compiling.
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
                     // Registered as loading with nothing in flight: a load that
                     // was interrupted before it settled. Nothing here can be
                     // described, and loading it from this read path would
                     // repeat the interrupted work on every listing.
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
                  // Directory did not contain a valid package.json file -- therefore, it's not a package.
                  // Or it timed out
                  // Redact before this reaches getStatus: compiling a model
                  // resolves the package's connections, so a Postgres/DuckLake
                  // ATTACH failure surfaces here carrying the connection string
                  // (connection.ts builds it, and redacts it before logging for
                  // the same reason). A log line was the old destination; this
                  // one is an HTTP response body.
                  //
                  // redactPgSecrets covers both keyword-form `password=` (what
                  // buildPgConnectionString emits) and URI userinfo
                  // (`postgres://user:pass@host`), so a URL-form connectionString
                  // supplied verbatim in config is redacted here too.
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
    * The `Package.status` the API reports for a package here: whether a
    * compiled copy is resident to answer queries, and whether a load,
    * reinstall or recompile is in progress. Read without the package lock, so
    * it answers during the operations it describes.
    */
   /**
    * The server's record of where `packageName` was installed from, kept
    * outside the package directory (see {@link PACKAGE_INSTALL_RECORDS_DIR}).
    * Joined under the environment root the way every other path built from a
    * package name here is, so a name that escaped validation cannot name a
    * file elsewhere.
    */
   private installRecordPath(packageName: string): string {
      return safeJoinUnderRoot(
         this.environmentPath,
         PACKAGE_INSTALL_RECORDS_DIR,
         `${packageName}.json`,
      );
   }

   /**
    * Resolve once no load, reinstall or recompile of the package is in flight
    * here. A caller that must decide against the copy that will be resident
    * (a PATCH comparing its `location` with the installed one) waits here
    * first, so it reads the outcome of the install rather than the copy the
    * install is about to replace, or has just failed to.
    */
   public async awaitPackageLoads(packageName: string): Promise<void> {
      let entry = this.loadsInFlight.get(packageName);
      while (entry !== undefined) {
         await entry.settled;
         entry = this.loadsInFlight.get(packageName);
      }
   }

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
    * Run `fn` with the package marked as loading here for its duration (see
    * `loadsInFlight`). Every path that allocates a new compiled copy of a
    * package, or recompiles the one it has, runs inside this, so
    * `Package.status.loading` is true from the moment the work is accepted,
    * download included, until it has settled or rolled back.
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
    * One mutex per package name; never replace after create — replacing
    * would allow two loads of the same package to run in parallel and
    * race on the canonical directory.
    *
    * `deletePackage` intentionally leaves the entry behind: a
    * subsequent re-install must serialize against any straggling
    * readers from the deleted generation that are still inside
    * `withPackageLock`. The map therefore grows by the count of
    * *distinct* package names the environment has ever served, not by
    * install churn, so for the publisher's expected workload
    * (config-declared packages, occasional ad-hoc additions) this is
    * bounded in practice. Long-lived deployments that create and
    * delete unique package names indefinitely would need an explicit
    * sweep; we'll add one if/when that pattern appears.
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
    * Run `fn` while holding the per-package mutex. This is the single
    * synchronization primitive that protects a package directory: every
    * code path that mutates `{environmentPath}/{packageName}/` or reads
    * from disk under it must serialize through this lock. See the lock
    * ordering note above the `packageMutexes` field for the wider
    * invariant.
    *
    * `async-mutex` is **not reentrant** — `fn` must not call any other
    * method that calls `withPackageLock` on the same package, or it will
    * deadlock. Use the `_xxxLocked` variants below in that case.
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
    * return the directories it would need, deepest first. The caller holds the
    * package lock.
    *
    * The target is already confined lexically (`safeJoinUnderRoot`), but a
    * directory in the package can be a symlink elsewhere, which `mkdir` and the
    * write would follow. So the nearest existing directory (and the file, if it
    * is a link) is resolved and must lie inside the resolved package root and
    * outside `public/`, which a symlink or a differently cased name on a
    * case-insensitive file system would otherwise reach. A path through an
    * existing file is refused too.
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
    * Put text at a model path, atomically: a sibling temporary file is renamed
    * over the target, so a reader never sees a half-written file and a crash
    * leaves either the old text or the new. The caller holds the package lock,
    * and the directory exists: {@link _createModelFileDirsLocked} made it for a
    * new file, and a restore writes where the previous text was.
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
    * Write one model file and reload the package behind it, as one step that
    * either happens or does not.
    *
    * Everything here — the precondition, the write, the reload, the caller's
    * check on the reloaded package, and the restore when that check fails —
    * runs inside a single hold of the package lock. That is the point of the
    * method. Doing it as separate locked calls leaves two windows open: a
    * second writer can land between the write and the reload, so the reload
    * serves their text under this caller's success; and it can land between a
    * failed reload and the restore, so the restore reverts THEIR write instead
    * of this one. Both end with the package serving something nobody asked
    * for, and neither is visible to the caller that lost.
    *
    * `check` refuses by throwing, and runs against the file's current text
    * (undefined when there is none) and the package as loaded before the
    * write (undefined when it is not) — so two saves racing on one file cannot
    * both pass their precondition. `verify` runs against the reloaded package
    * and likewise refuses by throwing; a refusal puts the previous text back
    * (or removes the file, when it is new), reloads again, and raises
    * {@link WriteRolledBackError}. When putting it back fails too, that is
    * logged as an error and raised as {@link WriteRollbackFailedError}: the
    * package may then serve the new text, so the caller must not read it as
    * "nothing changed". A restore that lands but whose reload fails is still a
    * rollback — the previous text is on disk — and its message says the
    * package may need a reload.
    *
    * Where the path really leads (through any link, into the package and out
    * of `public/`) is settled before the file is read, so nothing outside the
    * package is ever read through it.
    *
    * Anything that must NOT be under the lock — compiling above all — belongs
    * before this call: the lock is not reentrant, and compiling the proposed
    * text does not depend on what is on disk.
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
            // Nothing was served from the new text, so there is no reload to
            // undo: only a new file and the directories made for it.
            if (previous === undefined) {
               await fs.promises.rm(target, { force: true });
               await this._removeCreatedDirsLocked(createdDirs);
            }
            throw error;
         }
         try {
            // The locked form, because this whole callback already holds the
            // mutex that `getPackage` would take.
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
                  // The directories this create made, emptied by the removal.
                  await this._removeCreatedDirsLocked(createdDirs);
               }
            } catch (restoreError) {
               // The one log line for this write: the route and the error
               // mapping leave both rollback outcomes to this method.
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
            // The previous text is on disk again, so this is a rollback even
            // when reloading it fails: what is left is a reload, not an edit.
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
    * Best-effort sweep of `.staging/` and `.retired/` left over from a
    * previous run (crash, OOM, etc). Safe because both dirs are managed
    * exclusively by `installPackage` / `deletePackage`; no in-flight
    * operation in this process can be using them yet.
    *
    * Static + path-as-parameter on purpose: the sink path here must
    * derive from the validated factory argument, not from `this`,
    * because CodeQL's path-injection query conservatively treats every
    * field on this class as tainted (other methods on the same class
    * receive request-derived `packageName` values).
    */
   public static async sweepStaleInstallDirs(
      environmentPath: string,
   ): Promise<void> {
      assertSafeEnvironmentPath(environmentPath);
      for (const dirName of [STAGING_DIR_NAME, RETIRED_DIR_NAME]) {
         const dir = safeJoinUnderRoot(environmentPath, dirName);
         // Inline sanitizer barriers in the precise shape CodeQL's
         // `js/path-injection` query recognises (regex-test +
         // `indexOf("..") !== -1` guard) so the sink right below is
         // covered even when the call chain feeding `environmentPath`
         // is taint-tracked from an HTTP request handler.
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
    * Attach (or detach with `null`) the memory governor that gates new
    * package allocations. The single instance is owned by the
    * EnvironmentStore and propagated to every Environment so the
    * back-pressure decision is process-wide.
    */
   public setMemoryGovernor(governor: PackageMemoryGovernor | null): void {
      this.memoryGovernor = governor;
   }

   /**
    * Attach (or detach with `null`) a callback run each time a package
    * enters this environment's package map: at boot, on add, on install, and
    * on reload. The callback must only schedule work; see
    * {@link notifyPackageLoaded}.
    */
   public setPackageLoadedHook(hook: ((pkg: Package) => void) | null): void {
      this.packageLoadedHook = hook;
   }

   /**
    * Tell the hook a package is now served. Called straight after each
    * `this.packages.set`. A throwing hook is logged and swallowed: an
    * observer of the load must never fail it.
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

   /**
    * Inject the resolver that fetches a package's latest persisted
    * materialization entries — both tiers (see {@link storageBindingResolver}).
    */
   public setStorageBindingResolver(
      resolver: (packageName: string) => Promise<Record<string, ManifestEntry>>,
   ): void {
      this.storageBindingResolver = resolver;
   }

   /**
    * Re-establish a package's serve routing from its latest persisted
    * materialization when it (re)loads, so serving survives a restart — both
    * tiers, not just one. Serve bindings are otherwise in-memory, set only by a
    * build's post-run auto-load, so a worker restart silently reverted a
    * materialized source to serving live until the next build. Runs beside
    * {@link bindManifestIfConfigured} — the same "bind serve state on load" step.
    * Best-effort: a lookup failure logs and leaves the package serving live (a
    * subsequent build will bind it).
    *
    * Both tiers are restored from the SAME persisted manifest, split by
    * {@link splitManifestEntries}, mirroring {@link bindManifest}:
    *  - **colocated** (same-connection) → {@link Package.bindColocatedServeManifest},
    *    applied as a per-query `buildManifest` override at serve time (no
    *    recompile — the load-time compile already produced flag-carrying models).
    *    Independent of `PERSIST_STORAGE_MODE`: colocated is the v0 path and is
    *    not gated by the storage kill switch (a plain `#@ persist` materializes
    *    and serves even when the tier is `off`), so its routing is restored
    *    regardless of mode.
    *  - **storage=** (cross-connection) → {@link Package.bindStorageServeBindings},
    *    the virtual-source transform. Restored only when the tier is not `off`:
    *    storage serve routing requires the tier on, so an off deployment skips it.
    *
    * Skipped entirely when the package has a bound `manifestLocation`: the two
    * binding producers (this local store and the host's fetched manifest, applied
    * by {@link bindManifest}) are mutually exclusive by manifest presence, and the
    * host is authoritative when one is set. An orchestrated publisher STILL
    * persists its own local materialization records (the host triggers builds
    * through it), so without this guard the local rebind — which runs AFTER
    * {@link bindManifestIfConfigured} on load — would overwrite the host's
    * bindings with a possibly-staler local generation.
    */
   private async rebindServeBindingsFromLocalStore(
      pkg: Package,
   ): Promise<void> {
      if (!this.storageBindingResolver) return;
      // Host-authoritative: a bound manifestLocation means bindManifest already
      // supplied both tiers' serve bindings from the host's manifest; the local
      // store must not clobber them (mutually-exclusive binding sources).
      if (pkg.getPackageMetadata().manifestLocation) return;
      const packageName = pkg.getPackageName();
      try {
         const rawEntries = await this.storageBindingResolver(packageName);
         if (Object.keys(rawEntries).length === 0) return;
         const { tableNameManifest, storageEntries } = splitManifestEntries(
            rawEntries,
            `local store (package ${packageName})`,
         );
         // Colocated: restore regardless of PERSIST_STORAGE_MODE (v0 path, not
         // gated by the storage kill switch).
         if (Object.keys(tableNameManifest).length > 0) {
            pkg.bindColocatedServeManifest(tableNameManifest);
         }
         // Storage=: only meaningful when the tier is not off (serve routing to
         // the external store requires it). Ships dark otherwise.
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
    * Choke-point check called from every code path that would allocate
    * a *new* package into the in-memory map (lazy load on cache miss,
    * explicit reload, `addPackage`). Throws HTTP 503 when the governor
    * is back-pressured; cheap no-op when the governor is unset or
    * happy.
    *
    * `allowAdmission` is the documented opt-out for read paths that
    * genuinely cannot tolerate 503s. None of the current callers set
    * it; the parameter exists so a future caller (e.g. a
    * health/warmup probe) can self-document its bypass intent.
    */
   private assertCanAdmitNewPackage(
      packageName: string,
      reason: string,
      allowAdmission: boolean,
   ): void {
      if (allowAdmission) return;
      if (!this.memoryGovernor?.isBackpressured()) return;
      // Increment *before* throwing so the metric ticks even on
      // the not-uncommon "caught and swallowed" path. The label
      // shape mirrors `assertCanAdmitQuery` so a dashboard panel
      // can sum both rejection kinds by environment.
      getPackageAdmissionRejectionsCounter().add(1, {
         environment: this.environmentName,
         reason,
      });
      throw new PackageAdmissionRefusedError(
         `Publisher is under memory pressure and cannot ${reason} (package "${packageName}", environment "${this.environmentName}"). Retry after the server's memory usage drops below the low-water mark (PUBLISHER_MEMORY_LOW_WATER_FRACTION of PUBLISHER_MAX_MEMORY_BYTES), or raise PUBLISHER_MAX_MEMORY_BYTES if you have headroom.`,
      );
   }

   /**
    * Reject incoming queries with HTTP 503 when the memory governor
    * has tripped its high-water mark. Used by every query controller
    * (connection SQL, model query, notebook cell, MCP `execute_query`)
    * to shed load before the query runs — complementing
    * {@link assertCanAdmitNewPackage}, which only fires on cache-miss
    * package loads and so leaves already-loaded packages fully
    * queryable under pressure. With this in place, "back-pressured"
    * means "no new work of any kind" until the governor's low-water
    * mark is crossed.
    *
    * Cheap O(1) boolean read; no allocation when happy.
    */
   public assertCanAdmitQuery(): void {
      if (!this.memoryGovernor?.isBackpressured()) return;
      // Tick first so the counter reflects every rejection even
      // when the controller's catch block swallows the error (e.g.
      // an MCP tool surfaces it as a content payload rather than
      // letting it bubble to the HTTP error mapper).
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
      // Fast-path: serve from cache without acquiring the lock. Safe because
      // `Package` references are immutable; the disk-reading methods that
      // actually need protection (compileSource, getModelFileText,
      // reloadAllModelsForPackage, ...) acquire the lock themselves.
      //
      // INVARIANT: callers that consume the returned Package on the fast
      // path (notably the MCP query tools and Model.getModel()) must
      // remain in-memory only. If any code reachable from a `Package`
      // method ever grows new disk I/O against the canonical tree, that
      // path needs to be bracketed by `withPackageLock`; otherwise a
      // concurrent install/delete will race against an unlocked reader.
      const _package = this.packages.get(packageName);
      if (_package !== undefined && !reload) {
         return _package;
      }

      // We are either reloading or about to lazy-load on a cache miss
      // — both allocate a new package. This is the single choke point
      // for admission control; controllers no longer need their own
      // back-pressure check.
      this.assertCanAdmitNewPackage(
         packageName,
         reload ? "reload a package" : "load a package",
         options.allowAdmission === true,
      );

      return this.withPackageLock(packageName, () =>
         this._loadOrGetPackageLocked(packageName, reload),
      );
   }

   /**
    * Load (or reload) a package from its canonical disk location. Assumes
    * the caller holds the per-package mutex (via {@link withPackageLock}).
    *
    * Used by {@link getPackage} and by {@link compileSource} so the
    * cache-miss path doesn't re-enter the mutex.
    */
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
               // A failed RELOAD must not take down a package that is already
               // serving. The compiled model in `packages` is still the last good
               // one (it is only replaced on success), so keep serving it and let
               // the caller surface the error instead of evicting the package and
               // leaving the environment with nothing to answer from.
               this.setPackageStatus(packageName, PackageStatus.SERVING);
               // Serving the last good model is right, but it must not be silent:
               // this is the only record that the served model is now older than
               // the files on disk, and it is what makes a failed watch-mode
               // recompile visible to /status at all (the watch controller only
               // logs to stderr). Cleared on the next successful load via
               // clearPackageLoadFailure. Recording here, not in the watch
               // controller, covers every reload caller: the chokidar watcher,
               // MCP reload_package, and REST ?reload=true.
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
    * retire the copy it replaces, register it and mark it serving. The caller
    * holds the per-package lock.
    *
    * `bindServeState: false` (the add path) skips both the manifest bind and
    * the local-store serve-binding rebind. `reportSurfaceChange` (the reload
    * path) warns when the replaced copy published a surface this one lacks;
    * it runs after the bind, which may itself reload the models in place.
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
      // It loaded, so any earlier failure is stale. A package that failed at
      // boot can be fixed on disk and reloaded, re-added or republished
      // without a restart.
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
      // 404 takes precedence over 503 so a permanent "you forgot to
      // upload the package" failure isn't masked as a transient
      // "retry later" — the gate runs after the existence check.
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
            // The add path does not bind serve state; see commitLoadedPackage.
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
    *  - Phase 1 (no lock): run `downloader(stagingPath)`, writing the new
    *    content into a fresh sibling dir at `.staging/<pkg>-<uuid>/`. This
    *    is where multi-second downloads (git clone, GCS pull, ...) happen.
    *  - Phase 2 (lock held): atomically rename any existing canonical tree
    *    out to `.retired/<pkg>-<uuid>/`, rename staging into the canonical
    *    path, and run `Package.create` against the canonical path.
    *  - Phase 3 (after lock release): retire the old package's connections
    *    via the existing 30s drain and `fs.rm` the retired tree.
    *
    * Concurrent compiles / `getModelFileText` / `reloadAllModels` calls
    * take the same mutex and so are mutually exclusive with the Phase 2
    * swap, but they never queue behind a long Phase 1 download.
    *
    * On failure (Phase 1 download or Phase 2 `Package.create`), the staging
    * dir is removed and — if we already renamed the old tree aside — the
    * old tree is renamed back so the canonical path is restored.
    */
   public async installPackage(
      packageName: string,
      downloader: (stagingPath: string) => Promise<void>,
      validate?: (pkg: Package) => string | undefined,
      options: {
         allowAdmission?: boolean;
         /**
          * Metadata to apply to the installed copy inside the install's own lock
          * hold. The `location` it carries is recorded as where the package was
          * installed from; an install is the only path that records one.
          */
         update?: ApiPackage;
      } = {},
   ): Promise<Package> {
      assertSafePackageName(packageName);
      // An install allocates a whole new compiled copy, and for a reinstall
      // holds it beside the copy still serving until the swap, so it is the
      // largest single allocation a package can ask for. It is gated before
      // the download, the same way a lazy load and an add are gated, so a
      // server under memory back-pressure refuses it with a 503 the caller
      // can retry elsewhere rather than taking on the work and being killed.
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
               // This tree was just staged into place by the rename above, so a
               // failed load leaves a half-built directory that is ours to
               // remove; the rollback below restores the previous one.
               true,
            );
            this.attachDestinationServeConfig(newPackage);
            // Strict-reject hook (publish/update only — reload passes no
            // validator and stays fail-safe). Throw INSIDE the try so the
            // catch below rolls the swap back: the just-installed tree is
            // wiped and the retired tree (if any) is restored, so a rejected
            // publish/update never leaves the bad tree served on disk.
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

         // Best-effort manifest bind happens after the swap commits, outside the
         // rollback window: a manifest that can't be fetched must not undo an
         // otherwise-successful install (the package serves live instead).
         await this.commitLoadedPackage(packageName, newPackage, {
            bindServeState: true,
            replaces: oldPackage,
         });

         if (retiredPath) {
            this.scheduleRetiredTreeCleanup(packageName, retiredPath);
         }

         // Metadata the caller sent with the install (a publish's
         // `manifestLocation`, an update's whole body) is applied under this
         // same lock hold. Applied as a second, separately locked step, a
         // delete queued behind the swap ran first and the update then found
         // no package and answered 404 for work that had completed.
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
      // Rollback: clobber whatever (partial) content sits at canonical
      // — Package.create's own failure-cleanup may have already rm'd
      // the directory, so the most common outcome here is ENOENT.
      // `force: true` plus the `.catch(() => {})` make this a
      // best-effort wipe whose only job is to leave the rename-back
      // below a clean destination. Then put the old tree back if we
      // moved one aside.
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
         // The rollback put the old tree back and the previous package is
         // still in `this.packages` (it is only replaced on success
         // below), so it is genuinely still serving. Deleting its status
         // would strand it: listPackages enumerates packageStatuses, so
         // the package would answer getPackage while being invisible to
         // listings and discovery until a restart.
         this.setPackageStatus(packageName, PackageStatus.SERVING);
      } else {
         // Either there was nothing to fall back to (a first install), or
         // the restore did not happen: the rename-back threw, or the old
         // tree was never on disk to retire. The canonical path is then
         // missing or still holds the rejected content, so the cached
         // package no longer matches disk and must not be advertised as
         // serving. Drop both, and keep the two maps agreeing.
         this.deletePackageStatus(packageName);
         if (oldPackage) {
            // Retire before dropping it, the same way every other eviction
            // here does: once it leaves this.packages, closeAllConnections
            // can no longer reach its MalloyConfig, so its native handles
            // would never be released. Retire rather than shut down
            // inline, because withPackageLock does not cover queries that
            // already took this Package from an earlier getPackage; the
            // drain lets those finish first.
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
    * Reload every model in a package against the supplied build manifest,
    * holding the per-package mutex for the duration of the disk reads.
    * Replaces direct `Package.reloadAllModels` calls from outside
    * `Environment`.
    *
    * Skips the recompile when there is nothing to substitute now AND nothing was
    * substituted before — the same condition {@link bindManifest} applies to a
    * pure-storage manifest flip, and for the same reason: a same-connection
    * `tableName` manifest is resolved at COMPILE time, so an empty one over an
    * already-empty one changes nothing a recompile could express. The previous
    * state has to be checked too, because a package that just dropped its last
    * colocated entry still has to recompile to revert the substitution.
    *
    * This matters because every materialization run lands here. Without the
    * guard a `storage=`-only package — or one with no persist sources at all —
    * paid a full package recompile per run: measured at ~1.1MB of RSS per run on
    * the production image, never reclaimed, against a `main` build that reclaims.
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
    * Bind a package's `storage=` serve bindings from a build's FULL manifest
    * entries (carrying `storageDestinationName` + captured `schema`), so a query
    * against a materialized-into-storage source can be routed through the
    * virtual-source serve transform. Distinct from
    * {@link reloadAllModelsForPackage}, which binds the tableName-only manifest
    * for same-connection persistence. No-op (logged) if the package isn't
    * loaded — best-effort, like the post-build auto-load itself.
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
         // Host-authoritative: when a manifestLocation is bound, the host's
         // manifest is the sole source of storage serve bindings (see
         // bindManifest + rebindServeBindingsFromLocalStore' matching guard). This
         // method is the LOCAL-store binding path (post-build auto-load and the
         // post-delete rebind); on an orchestrated deployment the publisher
         // still writes its own local materialization records, so without this
         // guard a build's auto-load or a routine retire of a superseded record
         // would clobber the host's bindings with a possibly-staler local
         // generation. Standalone (no manifestLocation) is unaffected.
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
    * Re-establish a package's COLOCATED (same-connection) serve routing from a
    * FreshnessManifest, holding the package lock — the colocated analogue of
    * {@link bindPackageStorageServeBindings}, used by the post-delete rebind so a
    * reclaimed colocated table is not left routed. No-op (logged) if the package
    * isn't loaded, and skipped when a `manifestLocation` is bound (host
    * authoritative, same as the storage variant). An empty manifest reverts to
    * serving live.
    *
    * Unlike the storage tier — and unlike the load-time colocated rebind
    * ({@link Package.bindColocatedServeManifest}, which restores routing onto
    * FRESHLY-compiled models with a per-query overlay, no recompile) — this path
    * runs against models that a build's auto-load already recompiled with the
    * substitution BAKED IN. Clearing the per-query overlay would not strip a
    * baked substitution, so reverting/rebinding here requires a recompile
    * ({@link Package.reloadAllModels}), exactly as {@link bindManifest} does for
    * colocated. Skipped when there is nothing to substitute AND nothing was
    * previously substituted to clear (no needless recompile for a storage-only or
    * never-materialized package).
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
    * If the freshly-loaded package declares a `manifestLocation`, fetch the
    * control-plane-computed build manifest and rebind its models so persist
    * references resolve to the materialized tables. Best-effort: a fetch/bind
    * failure logs a warning and leaves the package serving live (the models are
    * already loaded without a manifest). Callers must hold the package lock —
    * this rebinds `pkg` in place rather than re-entering {@link withPackageLock}.
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
         // Bind runs before a package is marked SERVING, so a slow/unreachable
         // manifest store must not block serving indefinitely — bound is the
         // intended state, live is the degraded fallback. Race the fetch against
         // a timeout and fall back to live on either failure.
         const { tableNameManifest, storageEntries } =
            await this.fetchManifestEntriesWithTimeout(manifestLocation);

         // Tier split. Storage entries (cross-connection) bind as serve bindings
         // WITHOUT a recompile — they apply to the already-compiled models via
         // Model.setServeBindings. The host is the authoritative producer here,
         // so this also supersedes the local-store rebind (see
         // rebindServeBindingsFromLocalStore, which no-ops when manifestLocation
         // is set).
         // Bind whenever there ARE storage entries OR there WERE (bindStorage-
         // ServeBindings with the now-empty set clears them): a manifest whose
         // storage entries vanished must drop the old bindings, not leave them
         // routing at a table the host no longer vouches for. Mirrors the
         // hadColocated guard below.
         //
         // Each bind overwrites the state its own guard reads, so take both
         // `had*` reads before either tier applies.
         const hasStorage = Object.keys(storageEntries).length > 0;
         const hadStorage = pkg.hasStorageServeBindings();

         // colocated entries drive the same-connection tableName substitution, which
         // is resolved at COMPILE time — so they require a reloadAllModels
         // recompile (v0's existing cost). Skip the recompile for a pure-storage
         // manifest flip: only when there is nothing to substitute AND nothing
         // previously substituted to clear (otherwise a package that just dropped
         // its last colocated entry must still recompile to revert it).
         const hasColocated = Object.keys(tableNameManifest).length > 0;
         const hadColocated = pkg.hasBoundTableNameManifest();

         // Recompile FIRST. `reloadAllModels` is the only step here that can throw
         // (a compile error, or the worker pool being unavailable), and the catch
         // below reports `live_fallback` — "no manifest applied". Binding storage
         // entries before it would leave the NEW manifest's cross-connection
         // bindings installed and routing while the package reports that nothing
         // was applied and `boundManifestUri` still names the OLD manifest: a
         // caller reading the status as "not serving from manifest tables" would be
         // wrong about tables this package is actively serving from.
         if (hasColocated || hadColocated) {
            await pkg.reloadAllModels(tableNameManifest);
         }

         // Then storage. Ordering between the two is otherwise immaterial:
         // `reloadAllModels` re-pushes whatever bindings are current onto the fresh
         // model set, and `bindStorageServeBindings` pushes its own, so the new set
         // lands either way.
         if (hasStorage || hadStorage) {
            pkg.bindStorageServeBindings(storageEntries);
         }

         // Both tiers have applied by here, so the package is bound regardless of
         // which one carried entries — a pure-`storage=` manifest binds without a
         // colocated entry to count, and deriving the status from that count
         // alone reports `unbound` after a bind that fully succeeded.
         //
         // This is deliberately unconditional, and supersedes the status
         // `recordManifestBinding` derived if the recompile ran: a manifest that
         // binds nothing (empty, or every entry skipped) is still a manifest that
         // was fetched and applied, and reporting it `unbound` would make it
         // permanent drift to a caller that rebinds on anything but `bound`.
         // `manifestEntryCount` and `storageServeBindings` are what distinguish
         // "bound and serving" from "bound and empty".
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

   /**
    * Fetch manifest entries, rejecting if the fetch exceeds
    * {@link MANIFEST_FETCH_TIMEOUT_MS}. Keeps {@link bindManifest}'s bind-before-
    * serve guarantee from stalling on an unreachable manifest store.
    */
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

   /**
    * Read a model's source text from disk, holding the per-package mutex
    * so the read is serialized against {@link installPackage} /
    * {@link deletePackage} / {@link updatePackage}.
    */
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
         // Read existing manifest
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

         // Write back to file
         await fs.promises.writeFile(
            manifestPath,
            JSON.stringify(updatedManifest, null, 2),
            "utf-8",
         );
         // The install location lives in the server's own record outside the
         // package directory, so an in-place reload and a restart know where
         // the package came from, and nothing the package's content carries
         // (a `location` in publisher.json, a file of this name in a downloaded
         // tree) is ever read as one. Written only when an install supplies
         // it; never cleared from here.
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
      // An install downloads before it takes the package lock. A PATCH that
      // arrives then finds the lock free and, during a reinstall, the previous
      // copy resident; applied at once it would land on that copy, and what
      // it wrote would be swapped away a moment later, or kept by a rollback
      // as if the new content had arrived. So a PATCH waits for every load in
      // flight and is applied to whichever copy is resident afterwards. If
      // nothing is, the lookup below answers 404 as for any package that is
      // not here.
      await this.awaitPackageLoads(packageName);
      return this.withPackageLock(packageName, () =>
         this._updatePackageLocked(packageName, body, {
            recordLocation: false,
         }),
      );
   }

   /**
    * Apply a metadata PATCH to a loaded package. Assumes the caller holds the
    * per-package mutex: {@link updatePackage} takes it for a standalone PATCH,
    * and {@link installPackage} calls this inside its own hold so an install
    * and the metadata that came with it land as one operation.
    */
   private async _updatePackageLocked(
      packageName: string,
      body: ApiPackage,
      options: {
         /**
          * Whether the body's `location` is recorded as where the package was
          * installed from. True only for the metadata an install applies to the
          * copy it just installed: a metadata PATCH never changes it, because a
          * PATCH naming a different location is a reinstall, decided before it
          * gets here.
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
      // Preserve `explores` across a metadata PATCH. `setPackageMetadata`
      // replaces the whole object, so a name/description-only update must
      // carry the existing discovery surface through — otherwise the
      // in-memory `explores` is wiped and `listModels()` silently starts
      // serving every model until the next reload. When the body explicitly
      // carries `explores`, honor the new set instead.
      const existing = _package.getPackageMetadata();
      // Normalize API-body explores through the same helper the worker uses
      // for on-disk explores, so `["./index.malloy"]` / backslash paths
      // validate and persist identically regardless of input channel (no
      // misleading publish-time 400, no publish-vs-reload divergence).
      const normalizedExplores = body.explores?.map(normalizeModelPath);
      const explores =
         normalizedExplores !== undefined
            ? normalizedExplores
            : existing.explores;
      const queryableSources =
         body.queryableSources != null
            ? body.queryableSources
            : existing.queryableSources;
      // Preserve the existing manifestLocation unless the body explicitly
      // sets it (including to null, which clears it and reverts to live).
      const manifestLocation =
         body.manifestLocation !== undefined
            ? body.manifestLocation
            : existing.manifestLocation;
      // Persist `scope` and `materialization` (the schedule cron) are
      // editable via the API — both are writable in the schema. When the
      // body carries a value, apply it; otherwise preserve the
      // manifest-derived one (a name/description-only PATCH must not wipe
      // them, and the control plane must not misread the gap as a removal).
      // Changing the schedule re-arms the standalone scheduler on its next
      // tick — no reload needed.
      //
      // A *null* scope/materialization is treated the same as omitted
      // (preserve), NOT a wipe: the control plane's post-build rebind PATCH
      // carries only name/location/manifestLocation, but a client that
      // serializes unset fields as explicit null must not thereby trip the
      // policy gate below (a rejection there fails the orchestrated run) or
      // reset the persisted policy. `manifestLocation` is deliberately
      // different — null there means "clear" (revert to live), which the
      // caller's orchestrated build path relies on.
      const scopeProvided = body.scope != null;
      const materializationProvided = body.materialization != null;
      const editingPolicy = scopeProvided || materializationProvided;
      const scope = scopeProvided ? body.scope : existing.scope;
      // Preserved unless provided, for the same reason as scope: this
      // replaces the whole metadata object, so omitting it would make a
      // name/description-only PATCH silently untag every query the package
      // emits until the next reload. A null is treated as omitted for the
      // same reason as scope — a client that serializes unset fields as null
      // must not thereby untag a package. Clearing is an empty bag, which no
      // such client produces by accident.
      //
      // Resolved ONCE across both wire homes and then written to BOTH, the
      // way writePackageManifest resolves the file. Setting them
      // independently left whichever home the caller did not send holding a
      // stale bag, and the two are read by different paths: the serve path
      // takes the canonical field through getDeclaredQueryMetadata, the build
      // path takes the block. A migrated client PATCHing only the canonical
      // field therefore tagged its served queries with the new bag and its
      // builds with the old one, and getPackageMetadata returned the two
      // homes contradicting each other on a schema that promises both.
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
      // `setPackageMetadata` replaces the whole object, so every field the
      // body omits is carried through from the existing metadata, the way
      // `explores` and `manifestLocation` below already are. A PATCH that
      // names only a new `manifestLocation` (the post-build rebind) must not
      // drop the `location` the package was installed from, which is what a
      // later reload reinstalls from, or the `resource` the orchestrator
      // identifies the package by. A null counts as omitted, as it does for
      // `scope` above: a client that serializes unset fields as null must not
      // blank them. An empty string is a value, and clears a description.
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

      // Strict-reject, symmetric with the publish path
      // (package.controller.addPackage): validate the resulting explores
      // against the live model set and restore the prior metadata before
      // rejecting, so a bad update neither persists nor mutates the served
      // surface. When the body edits the persistence policy (scope /
      // materialization), also enforce the same scope/schedule/freshness/cron
      // rules a publish enforces — but only then, so a description-only PATCH
      // on a package with a pre-existing (load-tolerated) policy warning is
      // not newly rejected. Cron validity is one of these rules
      // (persistencePolicyWarnings Rule 4), so publish, PATCH, load, and the
      // scheduler all enforce it identically.
      const policyMsg = editingPolicy
         ? _package.formatInvalidPersistencePolicy()
         : "";
      // The explores check is gated the same way: a body that does not touch
      // `explores` (a rebind, the location an install records, a reload) must
      // not fail after the swap on a surface the load itself only warned
      // about, leaving the new tree serving with no record and no binding.
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
         // Only write when explicitly provided (non-null): mirrors the
         // null-as-absent rule above, so a rebind PATCH neither wipes the
         // persisted policy nor writes a stray `scope: null`.
         scope: scopeProvided ? body.scope : undefined,
         queryMetadata: body.queryMetadata ?? undefined,
         materialization: materializationProvided
            ? body.materialization
            : undefined,
      });

      // When the body changes manifestLocation, apply it now so the new
      // binding takes effect without a separate reload: a URI rebinds models
      // to the materialized tables; null/empty reverts the package to live.
      const revertsBinding =
         body.manifestLocation !== undefined &&
         !body.manifestLocation &&
         (_package.hasBoundTableNameManifest() ||
            _package.hasStorageServeBindings());
      if (body.manifestLocation) {
         // A rebind may recompile the package (colocated manifest entries
         // resolve at compile time), so it is reported as loading while it
         // runs, like every other recompile.
         await this.trackPackageLoad(packageName, () =>
            this.bindManifest(_package, body.manifestLocation as string),
         );
      } else if (revertsBinding) {
         // Revert to live: drop the colocated tableName substitution AND the
         // cross-connection storage serve bindings the prior bindManifest
         // applied, so no query still routes to a materialized table after
         // the operator explicitly cleared the manifest. A package with
         // nothing bound has nothing to revert, so a null from a client that
         // serializes unset fields does not recompile it.
         await this.trackPackageLoad(packageName, async () => {
            await _package.reloadAllModels({});
            _package.bindStorageServeBindings({});
         });
      } else {
         // The surface may have changed with no file changing, so the tile
         // findings are re-checked against it. (A manifest rebind above
         // reloads, which re-discovers and re-lints on its own.)
         await _package.relintReports();
      }

      return _package.getPackageMetadata();
   }

   public getPackageStatus(packageName: string): PackageInfo | undefined {
      return this.packageStatuses.get(packageName);
   }

   /**
    * Packages this environment is actually serving: registered statuses minus
    * any recorded as failed or un-mounted. Disjoint from getFailedPackages()
    * by construction, so the readiness line's packages= and load_errors=
    * cannot double-count a package that is seeded SERVING at boot and only
    * pruned later by a side-effect load (which a transient DB or memory-
    * pressure error can skip). Cheap: no package load is triggered.
    */
   public getServingPackageCount(): number {
      const failed = this.getFailedPackages();
      let serving = 0;
      for (const name of this.packageStatuses.keys()) {
         if (!failed.has(name)) serving += 1;
      }
      return serving;
   }

   /**
    * Record why a configured package's location never mounted, so /status can
    * name the real cause instead of the missing-manifest fallout it produces.
    * Called by EnvironmentStore right after the environment is created.
    */
   public setPackageMountError(packageName: string, message: string): void {
      this.mountErrors.set(packageName, message);
   }

   /**
    * Record a runtime add that failed before the package could serve, so
    * /status reports it the way it reports a configured package whose location
    * never mounted. Skipped while the name has any status: a failed re-install
    * rolls back to the previous tree, which is not a failed package. Cleared
    * like every other load failure, by a later successful add or install of
    * the name, or by deleting it.
    *
    * Names are the caller's, so the record is bounded: past
    * MAX_RECORDED_ADD_FAILURES the oldest recorded add failure is dropped.
    * Boot-time mount errors are not subject to the cap.
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
         // Clear the load failure before the early return, not after it. A
         // package that failed to load is not in `packages` (the load catch
         // evicts it), so deleting it takes the early return every time, while
         // the controller still drops its config row. Leaving the entry would
         // make getStatus report a loadError for a package that is no longer
         // configured, which is the one thing that channel must not do.
         this.clearPackageLoadFailure(packageName);

         const _package = this.packages.get(packageName);
         if (!_package) {
            return;
         }
         const packageStatus = this.packageStatuses.get(packageName);

         // The mutex now serializes load/install/compile against delete, so
         // the LOADING-state guard is mostly vestigial — left in place for
         // backwards-compatible error messaging in case anything bypasses
         // the lock.
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

         // Retire the package's connections via the existing 30s drain so
         // any in-flight queries that already acquired a connection finish
         // before the underlying duckdb handle is released.
         this.retireConnectionGeneration(`package ${packageName}`, () =>
            _package.getMalloyConfig().shutdown("close"),
         );

         // Atomically rename the canonical tree out of the way so no reader
         // can stat into it after the lock is released. The actual fs.rm is
         // deferred to setImmediate to keep the lock-hold time at one
         // rename rather than a (potentially slow) recursive remove.
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
    * Evict a package from the in-memory caches WITHOUT touching its on-disk
    * directory — the non-destructive counterpart to {@link deletePackage}.
    *
    * Used to roll back a no-location `addPackage` (which registers a
    * *pre-existing*, user-owned directory) when post-load validation rejects
    * it: deleting the tree there would destroy content the publisher never
    * created. This still drains and closes the connections the just-created
    * `Package` opened, so the duckdb handle isn't leaked.
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
         // Same 30s connection drain as deletePackage — just no fs rename/rm.
         this.retireConnectionGeneration(`package ${packageName}`, () =>
            _package.getMalloyConfig().shutdown("close"),
         );
         // Same reason deletePackage clears: a recorded failure describes a
         // package that is serving or configured, and after this it is neither.
         // Today no entry can survive to here (the only caller evicts a package
         // that addPackage just created, and addPackage clears on success), but
         // the eviction and the clear belong together whoever calls next.
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
      // Release the package-scoped MalloyConfigs (each holds the package's own
      // sandbox `duckdb` connection) before tearing down the environment config
      // they wrap. Without this, hard unload leaks per-package DuckDB handles.
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
         // `{ error }` alone serializes an Error to `{}`, which is what a reader
         // of this line gets told. Carry the message so a shutdown failure is
         // diagnosable from the log rather than only from a debugger.
         logger.error(
            `Error closing storage destinations for environment ${this.environmentName}`,
            { error: error instanceof Error ? error.message : String(error) },
         );
      }

      this.destinations = [];
      // Torn down, so the empty list above describes nothing rather than
      // describing an environment with no destinations — anything that reconciled
      // storage against it from here would be pruning on no information.
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
         // Credentials stay server-side, the same rule storageDestinations
         // below already followed. listApiConnections() is the internal view
         // and still carries them, because compiling and connecting need them.
         connections: toPublicConnections(this.listApiConnections()),
         // Name and type only. This is what the status endpoint reports, so it
         // is how an operator or orchestrator confirms which destinations a
         // worker picked up; the configs behind them stay server-side.
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
      // If the model file can't be read, return empty preamble
      // and let the compilation surface any import errors naturally.
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
      // Stop at the first source/query/run definition
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
