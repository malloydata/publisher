// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { localPackageFingerprint } from "./configured_publish_retry";
import { Mutex } from "async-mutex";
import crypto from "crypto";
import * as fs from "fs";
import * as path from "path";
import { components } from "../api";
import {
   getProcessedPublisherConfig,
   getPublisherConfigDir,
   getModelPageMode,
   getUnresolvedPublisherConfigPath,
   getVersionPromotionMode,
   isVersioningEnabled,
   isMcpIncludeHiddenFilesAndSources,
   isPublisherConfigFrozen,
   ProcessedEnvironment,
   ProcessedPublisherConfig,
} from "../config";
import { settings } from "../settings";
import {
   API_PREFIX,
   PUBLISHER_CONFIG_NAME,
   PUBLISHER_DATA_DIR,
} from "../constants";
import {
   ConnectionSaveError,
   EnvironmentNotFoundError,
   FrozenConfigError,
   messageWithFilesystemCause,
   PackageNotFoundError,
   PublisherConfigError,
} from "../errors";
import { getOperationalState, markNotReady, markReady } from "../health";
import { formatDuration, logger } from "../logger";
import { redactPgSecrets } from "../pg_helpers";
import { lazyCounter } from "../telemetry";
import { assertSafePackageName, safeJoinUnderRoot } from "../path_safety";
import {
   type ConnectionRecord,
   type EnvironmentRecord as DbEnvironment,
   type ResourceRepository,
} from "../storage/DatabaseInterface";
import { StorageConfig, StorageManager } from "../storage/StorageManager";
import { extractErrorDataFromError, validateAzureUrl } from "./db_utils";
import {
   Environment,
   STAGING_DIR_NAME,
   type PackageListingOptions,
} from "./environment";
import { formatPublishRejections } from "./publish_checks";
import type { Package } from "./package";
import { environmentResource } from "../api_resource";
import { type EnvironmentName, parseEnvironmentName } from "./identity";
import { VersionStore } from "./versions/version_store";
import { storageBindingResolverFor } from "./versions/materialization_scope";
import type { PackageMemoryGovernor } from "./package_memory_governor";
import { SERVER_VERSION } from "../version";
import type {
   ConnectionAdmin,
   EnvironmentAdmin,
   EnvironmentPeek,
   LoadedEnvironmentsReader,
   McpExposureReader,
   PackageAdmin,
   ServerStatusBody,
   StatusOptions,
   StatusReader,
} from "./environment_ports";
import {
   isGitHubURL,
   isLocalPath,
   PackageSources,
   type PackageSourceClients,
   parseGitHubUrl,
   resolvePackageLocation,
} from "./package_sources";
type ApiEnvironment = components["schemas"]["Environment"];
type ApiConnection = components["schemas"]["Connection"];
type LoadError = NonNullable<
   components["schemas"]["ServerStatus"]["loadErrors"]
>[number];

/**
 * The connection rows one sync could not bring in line with memory. Each was
 * logged where it failed; this is what the caller surfaces afterwards.
 */
interface ConnectionSaveFailures {
   /** Connections whose row could not be written: lost on a restart. */
   unsaved: string[];
   /** Removed connections whose row could not be deleted: back on a restart. */
   unremoved: string[];
   /** The stored rows could not be listed, so none were pruned. */
   unlisted: boolean;
}

function hasConnectionSaveFailures(failures: ConnectionSaveFailures): boolean {
   return (
      failures.unsaved.length > 0 ||
      failures.unremoved.length > 0 ||
      failures.unlisted
   );
}

/**
 * The answer to a sync whose connection rows did not all save. Composed here,
 * from connection names only: the storage error itself is logged, not sent.
 */
function connectionSaveError(
   environmentName: string,
   failures: ConnectionSaveFailures,
): ConnectionSaveError {
   const quote = (names: string[]) => names.map((n) => `"${n}"`).join(", ");
   const problems: string[] = [];
   if (failures.unsaved.length > 0) {
      problems.push(
         `could not save connection(s) ${quote(failures.unsaved)}, which serve now but will be missing after a restart`,
      );
   }
   if (failures.unremoved.length > 0) {
      problems.push(
         `could not delete removed connection(s) ${quote(failures.unremoved)}, which will return after a restart`,
      );
   }
   if (failures.unlisted) {
      problems.push(
         "could not read its stored connections, so any connection this change removed will return after a restart",
      );
   }
   return new ConnectionSaveError(
      `Environment "${environmentName}" was updated, but the server ${problems.join("; and ")}. Retry the change to save them.`,
      [...failures.unsaved, ...failures.unremoved],
   );
}

/**
 * What triggered an environment's sync to the database: a `request` that
 * changed it (create, PATCH), the `boot` load, or a read that loaded it
 * (`load`) or reloaded it (`reload`) from config. Only a `request` fails when
 * a connection row does not save (see addEnvironmentToDatabase).
 */
type EnvironmentSyncPath = "request" | "boot" | "load" | "reload";

/**
 * Environment syncs whose connection rows did not all save. Counted because
 * anywhere but a request (at boot, or on a read that loaded the environment)
 * nothing else reports it off-box: the environment loads and serves.
 */
const connectionSaveFailureCounter = lazyCounter(
   "publisher_connection_save_failures_total",
   "Environment syncs to the database whose connection rows did not all save, so the connections in memory will differ after a restart. Label: path ('request'|'boot'|'load'|'reload').",
);

function validateEnvironmentAzureUrls(environment: ApiEnvironment): void {
   for (const conn of environment.connections || []) {
      if (conn.type !== "duckdb") continue;
      for (const db of conn.duckdbConnection?.attachedDatabases || []) {
         if (db.type !== "azure" || !db.azureConnection) continue;
         const { authType, sasUrl, fileUrl } = db.azureConnection;
         if (authType === "sas_token" && sasUrl) {
            validateAzureUrl(sasUrl, `"${db.name}" sasUrl`);
         } else if (authType === "service_principal" && fileUrl) {
            validateAzureUrl(fileUrl, `"${db.name}" fileUrl`);
         }
      }
   }
}

// Clear a prior mount target before (re)mounting. A symlink is unlinked, never
// rm'd recursively, which could traverse into and damage the linked source.
// Both mount modes call this so toggling watch mode across runs cannot leave
// an entry that breaks the next mount.
async function clearMountTarget(targetPath: string): Promise<void> {
   try {
      const stats = await fs.promises.lstat(targetPath);
      if (stats.isDirectory() && !stats.isSymbolicLink()) {
         await fs.promises.rm(targetPath, { recursive: true, force: true });
      } else {
         await fs.promises.unlink(targetPath);
      }
   } catch {
      // Nothing there, fine.
   }
}

/** `0.0.0.0`/`::` bind every interface; a script needs a host it can dial. */
function displayHost(host: string): string {
   if (host === "0.0.0.0" || host === "::") {
      return "localhost";
   }
   // An IPv6 literal needs brackets to be dialable inside a URL.
   return host.includes(":") ? `[${host}]` : host;
}

/**
 * The one-line machine-readable boot signal scripts grep for instead of
 * polling `/api/v0/status`; the counts say how much of the configured world
 * actually loaded. Host and port defaults mirror server.ts's.
 */
export function formatReadinessLine(counts: {
   environments: number;
   packages: number;
   loadErrors: number;
}): string {
   const host = displayHost(settings.get("host"));
   const port = settings.get("port");
   const mcpPort = settings.get("mcp.port");
   return (
      `PUBLISHER_READY url=http://${host}:${port} ` +
      `mcp=http://${host}:${mcpPort} ` +
      `environments=${counts.environments} packages=${counts.packages} ` +
      `load_errors=${counts.loadErrors}`
   );
}

/**
 * The store-level callbacks main_server installs, each handed to every
 * Environment bound to that environment. Set once at server start.
 */
interface EnvironmentHooks {
   packageLoaded: ((environmentName: string, pkg: Package) => void) | null;
   versionArchived:
      | ((
           environmentName: string,
           packageName: string,
           versionId: string,
        ) => void)
      | null;
   versionDeleted:
      | ((
           environmentName: string,
           packageName: string,
           versionId: string,
        ) => void)
      | null;
   /**
    * Takes the Environment itself: a configured package publishes while the
    * store is still initializing, when a lookup by name would wait for that
    * very initialization.
    */
   owedReclaim:
      | ((
           environment: Environment,
           packageName: string,
           versionId: string,
        ) => Promise<void>)
      | null;
   /**
    * A package left the store (deleted, or replaced by a publish that dropped
    * it): retrieval drops what it derived from it. Fired by the store itself,
    * not handed to an Environment. Best effort: a rejection is logged.
    */
   packageDeleted:
      | ((environmentName: string, packageName: string) => Promise<void>)
      | null;
   /** An environment was deleted; like {@link packageDeleted}, for all of it. */
   environmentDeleted: ((environmentName: string) => Promise<void>) | null;
}

/** The hooks the store fires itself rather than handing to an Environment. */
type StoreLevelHook = "packageDeleted" | "environmentDeleted";
type EnvironmentLevelHook = Exclude<keyof EnvironmentHooks, StoreLevelHook>;

/** How each hook is handed to one Environment; `null` detaches it. */
const ATTACH_HOOK: {
   [K in EnvironmentLevelHook]: (
      env: Environment,
      hook: EnvironmentHooks[K],
   ) => void;
} = {
   packageLoaded: (env, hook) =>
      env.setPackageLoadedHook(
         hook && ((pkg) => hook(env.getEnvironmentName(), pkg)),
      ),
   versionArchived: (env, hook) =>
      env.setVersionArchivedHook(
         hook &&
            ((packageName, versionId) =>
               hook(env.getEnvironmentName(), packageName, versionId)),
      ),
   versionDeleted: (env, hook) =>
      env.setVersionDeletedHook(
         hook &&
            ((packageName, versionId) =>
               hook(env.getEnvironmentName(), packageName, versionId)),
      ),
   owedReclaim: (env, hook) =>
      env.setOwedReclaimHook(
         hook &&
            ((packageName, versionId) => hook(env, packageName, versionId)),
      ),
};

/** Picked up when an Environment's version registry is bound. */
const VERSION_HOOKS = [
   "versionArchived",
   "versionDeleted",
   "owedReclaim",
] as const;

/**
 * Seams for tests: replace the metadata store, or the git, GCS or S3 client
 * the package sources use.
 */
export interface EnvironmentStoreDeps extends PackageSourceClients {
   storageManager?: StorageManager;
}

export class EnvironmentStore
   implements
      EnvironmentAdmin,
      EnvironmentPeek,
      PackageAdmin,
      ConnectionAdmin,
      McpExposureReader,
      LoadedEnvironmentsReader,
      StatusReader
{
   public serverRootPath: string;
   private environments: Map<string, Environment> = new Map();
   /**
    * Environments that were configured but did not load, keyed by name: the
    * only record that one is missing rather than never asked for, since a
    * skipped environment never reaches `this.environments`. Surfaced on
    * `getStatus`.
    */
   private failedEnvironments = new Map<string, string>();
   /**
    * Why this server booted empty because no config was found (set by
    * logUnconfiguredNotice). Reported as `emptyReason` only while no
    * environment exists. A startup snapshot: a config written later is not
    * read until a restart.
    */
   private unconfiguredNotice: string | null = null;
   /**
    * Why initialization failed. The server stays up but not ready, so without
    * this `/status` reads as still starting. Reported as `initError`.
    */
   private initError: string | null = null;
   private environmentMutexes = new Map<string, Mutex>();
   /**
    * Read in the constructor, not after storage opens: write paths check it
    * before they wait for initialization, so a write during a slow first
    * boot (or after a failed one) must already see `frozenConfig`.
    */
   public publisherConfigIsFrozen: boolean;
   /**
    * Read once: the MCP server registers its tools per request, and must not
    * re-parse the config each time.
    */
   public readonly mcpIncludeHiddenFilesAndSources: boolean;
   /** Settles when initialization has finished; see {@link finishedInitialization}. */
   private readonly initialization: Promise<void>;
   private settleInitialization!: (outcome: Promise<void>) => void;
   private started = false;
   private isInitialized: boolean = false;
   public storageManager: StorageManager;
   /** Brings a package location (git, GCS, S3, local, zip) to disk. */
   private packageSources: PackageSources;
   private memoryGovernor: PackageMemoryGovernor | null = null;
   private hooks: EnvironmentHooks = {
      packageLoaded: null,
      versionArchived: null,
      versionDeleted: null,
      owedReclaim: null,
      packageDeleted: null,
      environmentDeleted: null,
   };

   /**
    * Environments (from `PUBLISHER_WATCH`) whose package directories under
    * `publisher_data/<env>/` are symlinks to the local sources rather than
    * copies, so source edits live-reload. The default copy keeps source and
    * served package decoupled. Remote locations always copy.
    */
   private inPlaceEnvs = new Set<string>();

   /**
    * Does no I/O: the metadata store is opened and the configured
    * environments loaded by {@link start}, which the composition root calls.
    */
   constructor(serverRootPath: string, deps: EnvironmentStoreDeps = {}) {
      this.serverRootPath = serverRootPath;
      this.packageSources = new PackageSources(deps, (location) =>
         this.resolveLocalPath(location),
      );
      this.mcpIncludeHiddenFilesAndSources =
         isMcpIncludeHiddenFilesAndSources();
      this.publisherConfigIsFrozen = isPublisherConfigFrozen();

      for (const envName of settings.get("watchEnvironments")) {
         this.inPlaceEnvs.add(envName);
      }

      const storageConfig: StorageConfig = {
         type: "duckdb",
         duckdb: {
            path: path.join(serverRootPath, "publisher.db"),
         },
      };
      this.storageManager =
         deps.storageManager ?? new StorageManager(storageConfig);

      this.initialization = new Promise<void>((resolve) => {
         this.settleInitialization = resolve;
      });
   }

   /**
    * Settles when initialization has finished, successfully or not (a failure
    * is recorded, not thrown). Reading it starts the store if nothing has
    * yet, so a store built outside the composition root initializes on first
    * use instead of leaving every lookup pending. The root still calls
    * {@link start} itself, after installing its hooks.
    */
   public get finishedInitialization(): Promise<void> {
      return this.start();
   }

   /**
    * Open the metadata store and load the configured (or persisted)
    * environments. Idempotent: a later call returns the first one's
    * promise. Never rejects; a failure is logged,
    * printed as `PUBLISHER_INIT_FAILED`, and reported on `/status`.
    */
   public start(): Promise<void> {
      if (!this.started) {
         this.started = true;
         this.settleInitialization(this.initialize());
      }
      return this.initialization;
   }

   /** True if this env should mount package dirs in place (symlinks). */
   public isInPlace(environmentName: string): boolean {
      return this.inPlaceEnvs.has(environmentName);
   }

   /** Add an env to in-place mode at runtime. Caller is responsible for
    *  triggering a re-mount if the env was already loaded with copies. */
   public markInPlace(environmentName: string): void {
      this.inPlaceEnvs.add(environmentName);
   }

   /**
    * Attach (or detach with `null`) the shared {@link PackageMemoryGovernor},
    * for existing and future Environments, so back-pressure is process-wide.
    */
   public setMemoryGovernor(governor: PackageMemoryGovernor | null): void {
      this.memoryGovernor = governor;
      for (const env of this.environments.values()) {
         env.setMemoryGovernor(governor);
      }
   }

   /**
    * Attach (or detach with `null`) a callback run for every package that
    * enters an Environment's package map: at boot, on add, on install and on
    * reload. It must only schedule work, because a restart fires it for
    * every package at once.
    */
   public setPackageLoadedHook(hook: EnvironmentHooks["packageLoaded"]): void {
      this.setHook("packageLoaded", hook);
   }

   /**
    * Attach (or detach with `null`) the callback run when a version is
    * archived, which reclaims the tables that version alone owns.
    */
   public setVersionArchivedHook(
      hook: EnvironmentHooks["versionArchived"],
   ): void {
      this.setHook("versionArchived", hook);
   }

   /**
    * Attach (or detach with `null`) the callback run once a version is
    * deleted, which stops its builds and reclaims the tables that version
    * alone owns, in the background.
    */
   public setVersionDeletedHook(
      hook: EnvironmentHooks["versionDeleted"],
   ): void {
      this.setHook("versionDeleted", hook);
   }

   /**
    * Attach (or detach with `null`) the callback a publish runs before it
    * creates a version's row, which reclaims what a deleted version of the
    * same id still owes.
    */
   public setOwedReclaimHook(hook: EnvironmentHooks["owedReclaim"]): void {
      this.setHook("owedReclaim", hook);
   }

   /**
    * Attach (or detach with `null`) the callback run when a package is
    * deleted, which drops what retrieval derived from it (its embeddings and
    * search-index rows).
    */
   public setPackageDeletedHook(
      hook: EnvironmentHooks["packageDeleted"],
   ): void {
      this.hooks.packageDeleted = hook;
   }

   /** Environment-wide variant of {@link setPackageDeletedHook}. */
   public setEnvironmentDeletedHook(
      hook: EnvironmentHooks["environmentDeleted"],
   ): void {
      this.hooks.environmentDeleted = hook;
   }

   /**
    * Remember `hook` and hand it to every Environment already here. One
    * created later gets it when it is attached ({@link attachHooks}).
    */
   private setHook<K extends EnvironmentLevelHook>(
      name: K,
      hook: EnvironmentHooks[K],
   ): void {
      this.hooks[name] = hook;
      for (const env of this.environments.values()) {
         this.attachHooks(env, [name]);
      }
   }

   private attachHooks(
      env: Environment,
      names: readonly EnvironmentLevelHook[],
   ): void {
      for (const name of names) {
         ATTACH_HOOK[name](env, this.hooks[name] as never);
      }
   }

   /** The environments in memory, without touching the database or loading. */
   public getLoadedEnvironments(): Environment[] {
      return [...this.environments.values()];
   }

   private async addConfiguredEnvironment(environment: ProcessedEnvironment) {
      // With package versioning on, a configured package loads as a publish of
      // its location, exactly as `POST .../packages` with that location
      // publishes it: as the version its publisher.json declares, by the same
      // checks. A watched environment mounts its packages' sources in place,
      // which are no immutable versions, so it loads them as before.
      const asVersions =
         isVersioningEnabled() && !this.inPlaceEnvs.has(environment.name);
      try {
         const added = await this.addEnvironment(
            {
               name: environment.name,
               resource: environmentResource(environment.name),
               connections: environment.connections,
               // Only asserted when the config declares destinations: an empty
               // list is an instruction to clear, and this call reaches
               // `Environment.update` for an environment that is already loaded.
               ...(environment.storageDestinations.length
                  ? {
                       storageDestinations: environment.storageDestinations,
                    }
                  : {}),
               packages: asVersions ? [] : environment.packages,
            },
            true,
            { configuredPackagesPublished: asVersions },
         );
         if (asVersions) {
            await this.publishConfiguredPackages(added, environment.packages);
         }
      } catch (error) {
         this.recordEnvironmentInitializationFailure(environment.name, error);
      }
   }

   /**
    * Publish each configured package as the version its publisher.json
    * declares (see addConfiguredEnvironment). A local location (a directory
    * or a .zip) is published straight from where it is, so it is copied
    * once. The remote ones (GitHub, gs://, s3://) are mounted first, by the
    * loop an unversioned environment mounts its packages with
    * (loadEnvironmentIntoDisk), into a folder under the environment's
    * `.staging`: a location holding several packages is downloaded once, with
    * labelled progress, and one that does not mount is recorded as that
    * package's load error. Each is then published from its mount. A server
    * stopped mid-mount leaves that folder behind, and the next boot's sweep of
    * `.staging` removes it (Environment.sweepStaleInstallDirs), whether that
    * boot reads the config or restores from the database. A package that
    * cannot be published (no version, a refused check, a conflict) is recorded
    * as its load error too, and the others still load.
    */
   private async publishConfiguredPackages(
      environment: Environment,
      packages: ProcessedEnvironment["packages"],
   ): Promise<void> {
      if (packages.length === 0) return;
      const environmentName = environment.getEnvironmentName();
      const versions = environment.getVersionService();
      const remote = packages.filter((p) => !isLocalPath(p.location));
      // Dot-prefixed, so no package's own staging (`<pkg>-<uuid>`) shares it.
      const mountRoot = safeJoinUnderRoot(
         environment.getEnvironmentPath(),
         STAGING_DIR_NAME,
         `.configured-${crypto.randomUUID()}`,
      );
      try {
         let mountErrors: ReadonlyMap<string, string> = new Map();
         if (remote.length > 0) {
            try {
               ({ mountErrors } = await this.loadEnvironmentIntoDisk(
                  environmentName,
                  remote,
                  mountRoot,
               ));
            } catch (error) {
               // Nothing could be staged (the environment's folder refused
               // the staging folder, say): each remote package is a load
               // error, as a location that does not mount is, and the
               // environment serves.
               const failed = new Map<string, string>();
               this.recordMountFailure(
                  error,
                  mountRoot,
                  remote.map((p) => p.name),
                  failed,
               );
               mountErrors = failed;
            }
         }
         for (const configured of packages) {
            const mountError = mountErrors.get(configured.name);
            if (mountError !== undefined) {
               environment.setPackageMountError(configured.name, mountError);
               continue;
            }
            try {
               if (!versions) {
                  throw new Error(
                     "package versions are not available in this environment",
                  );
               }
               // A relative or `~/` location is the config's, resolved against
               // its folder (resolveLocalPath), so a later boot that restores
               // the version from its recorded source location finds the same
               // folder.
               const local = isLocalPath(configured.location);
               const sourceLocation = local
                  ? this.resolveLocalPath(configured.location)
                  : configured.location;
               const fetchFrom = local
                  ? sourceLocation
                  : safeJoinUnderRoot(mountRoot, configured.name);
               const publish = () =>
                  versions.publish(
                     configured.name,
                     (stagingPath) =>
                        this.downloadPackageInto(
                           environmentName,
                           configured.name,
                           fetchFrom,
                           stagingPath,
                        ),
                     {
                        sourceLocation,
                        promotion: getVersionPromotionMode(),
                        validate: (loaded) => formatPublishRejections(loaded),
                     },
                  );
               try {
                  await publish();
               } catch (error) {
                  // A refused publish places nothing. A local location can be
                  // read again, so the package's next access publishes it
                  // again (Environment.setConfiguredPublishRetry); a remote
                  // one's mount is removed below, so its reads answer this
                  // error until the next --init.
                  if (local) {
                     environment.setConfiguredPublishRetry(
                        configured.name,
                        publish,
                        () => localPackageFingerprint(sourceLocation),
                     );
                  } else {
                     environment.setConfiguredPublishRefused(
                        configured.name,
                        error,
                     );
                  }
                  throw error;
               }
            } catch (error) {
               const message = redactPgSecrets(
                  error instanceof Error ? error.message : String(error),
               );
               logger.error(
                  `Failed to publish configured package "${configured.name}" as a version; skipping it`,
                  {
                     environmentName,
                     packageName: configured.name,
                     error: message,
                  },
               );
               environment.setPackageMountError(configured.name, message);
            }
         }
      } finally {
         await fs.promises
            .rm(mountRoot, { recursive: true, force: true })
            .catch((error) =>
               logger.warn("Could not remove a package staging folder", {
                  environmentName,
                  stagingRoot: mountRoot,
                  error,
               }),
            );
      }
   }

   /**
    * Log a skipped environment AND remember why, for getStatus. Both boot
    * paths (config and database restore) funnel through here so neither drops
    * failures silently.
    */
   private recordEnvironmentInitializationFailure(
      environmentName: string | undefined,
      error: unknown,
   ) {
      const label = environmentName ? ` "${environmentName}"` : "";
      logger.error(
         `Error initializing environment${label}; skipping environment`,
         extractErrorDataFromError(error),
      );
      // Only record an environment that is genuinely absent. A throw from the
      // tail of setup can land here after the environment is live; recording
      // it would contradict `environments` and never be cleared.
      if (environmentName && !this.environments.has(environmentName)) {
         // Redacted: bound for an HTTP response body.
         this.failedEnvironments.set(
            environmentName,
            redactPgSecrets(
               error instanceof Error ? error.message : String(error),
            ),
         );
      }
   }

   /**
    * Rebuild one environment the database holds: its connections,
    * destinations and seeded package statuses, or, when its directory has
    * gone, re-add it from the config. A failure is recorded and the
    * environment skipped.
    */
   private async restoreEnvironmentFromDatabase(
      dbEnvironment: DbEnvironment,
      environmentManifest: ProcessedPublisherConfig,
      repository: ResourceRepository,
   ) {
      try {
         const environmentExists = await fs.promises
            .access(dbEnvironment.path)
            .then(() => true)
            .catch(() => false);

         const environmentConfig = environmentManifest.environments.find(
            (p) => p.name === dbEnvironment.name,
         );

         if (!environmentExists) {
            if (environmentConfig) {
               const environmentInstance = await this.addEnvironment(
                  {
                     name: environmentConfig.name,
                     resource: environmentResource(environmentConfig.name),
                     connections: environmentConfig.connections,
                     storageDestinations: environmentConfig.storageDestinations,
                     packages: environmentConfig.packages,
                  },
                  true,
                  // The config is only a fallback for a missing directory:
                  // its empty destination list must not prune destinations
                  // registered over the API, nor its connections the ones
                  // registered over the API.
                  {
                     storageDestinationsAreFallback: true,
                     connectionsAreFallback: true,
                  },
               );

               await repository.updateEnvironment(dbEnvironment.id, {
                  path: environmentInstance.metadata.location,
               });

               return environmentInstance.listPackages();
            } else {
               logger.error(
                  `Environment "${dbEnvironment.name}" not found in config and files missing`,
               );
               return;
            }
         }

         const connections = await repository.listConnections(dbEnvironment.id);

         // Destinations from the database too, so one registered over the API
         // survives a restart; the config seeds only an environment the
         // database holds none for. Not fatal: a missing destination only
         // makes its materialized queries serve live.
         let destinations = environmentConfig?.storageDestinations;
         let destinationsRead = true;
         try {
            const stored = await repository.listStorageDestinations(
               dbEnvironment.id,
            );
            if (stored.length) {
               destinations = stored.map((row) => ({
                  name: row.name,
                  type: row.type as ApiConnection["type"],
                  ...row.config,
               }));
            }
         } catch (error) {
            destinationsRead = false;
            logger.error(
               `Error reading storage destinations for "${dbEnvironment.name}"; continuing without the stored ones`,
               { error },
            );
         }

         const environmentInstance = await Environment.create(
            dbEnvironment.name,
            dbEnvironment.path,
            connections.map((conn) => ({
               name: conn.name,
               type: conn.type,
               resource: `${API_PREFIX}/connections/${conn.name}`,
               ...conn.config,
            })),
            destinations,
         );
         // A fallback, not the set of record: see
         // markStorageDestinationsUnknown.
         if (!destinationsRead) {
            environmentInstance.markStorageDestinationsUnknown();
         }
         environmentInstance.setMemoryGovernor(this.memoryGovernor);
         this.attachHooks(environmentInstance, ["packageLoaded"]);
         // Its version rules, with the storage-binding resolver every package
         // here restores its serve bindings through.
         await this.bindVersionRegistry(environmentInstance, dbEnvironment.id);
         // Reloaded from the database: its unversioned packages restore their
         // serve bindings from the local store on load, so serving survives a
         // restart. The only path that did so before versions existed;
         // environments created at runtime still do not.
         environmentInstance.restoreUnversionedBindingsFromLocalStore();

         // Registered as configured, not serving: none is loaded yet, and
         // the first listing or request loads each.
         const packages = await repository.listPackages(dbEnvironment.id);
         packages.forEach((pkg) => {
            environmentInstance.registerConfiguredPackage(pkg.name);
         });

         this.environments.set(dbEnvironment.name, environmentInstance);

         return environmentInstance.listPackages();
      } catch (error) {
         this.recordEnvironmentInitializationFailure(dbEnvironment.name, error);
      }
   }

   private async initialize() {
      const reInit = settings.get("initializeStorage");
      const initialTime = performance.now();

      try {
         await this.storageManager.initialize(reInit);

         // Re-read from the same settings the constructor read, so a store
         // built before its settings were installed (as tests do) still
         // starts with the value in force.
         this.publisherConfigIsFrozen = isPublisherConfigFrozen();

         const environmentManifest =
            await EnvironmentStore.reloadEnvironmentManifest(
               this.serverRootPath,
            );

         await this.cleanupAndCreatePublisherPath();

         const repository = this.storageManager.getRepository();

         // What was ASKED FOR, which may differ from what loaded.
         let declaredEnvironments = environmentManifest.environments.length;

         let existingEnvironments: DbEnvironment[] = [];
         if (!reInit) {
            existingEnvironments = await repository.listEnvironments();
            declaredEnvironments += existingEnvironments.length;
         }

         if (existingEnvironments.length > 0) {
            await Promise.all(
               existingEnvironments.map((dbEnvironment) =>
                  this.restoreEnvironmentFromDatabase(
                     dbEnvironment,
                     environmentManifest,
                     repository,
                  ),
               ),
            );
         } else {
            // From the config: always on --init, else when the database is
            // empty.
            await Promise.all(
               environmentManifest.environments.map(async (environment) => {
                  await this.addConfiguredEnvironment(environment);
               }),
            );
         }

         this.isInitialized = true;
         markReady();
         const initializationDuration = performance.now() - initialTime;
         logger.info(
            `Environment store successfully initialized in ${formatDuration(initializationDuration)}`,
         );
         this.logUnconfiguredNotice(declaredEnvironments);
         this.emitReadinessLine();
      } catch (error) {
         markNotReady();
         // Redacted because initialization errors can carry connection strings.
         this.initError = redactPgSecrets(messageWithFilesystemCause(error));
         const errorData = extractErrorDataFromError(error);
         logger.error("Error initializing environment store", errorData);
         try {
            // The failure counterpart of PUBLISHER_READY, so a waiting script
            // fails fast. Redacted for the same reason as above.
            process.stderr.write(
               `PUBLISHER_INIT_FAILED error=${JSON.stringify(
                  redactPgSecrets(
                     error instanceof Error ? error.message : String(error),
                  ),
               )}\n`,
            );
         } catch {
            // A failed stderr write must not mask the logged error above.
         }
      }
   }

   /**
    * Say why an empty server is empty, once, beside the readiness line:
    * otherwise an operator who forgot to mount a config sees a healthy server
    * and an empty catalog, with no output naming the path checked.
    *
    * `info`, not `warn`: booting unconfigured and creating environments over
    * the API is a supported mode. Gated on environments DECLARED, not loaded:
    * a database environment whose directory is gone loads nothing, and
    * blaming the config file for it would be wrong. With no config,
    * `frozenConfig` is false, so runtime creation is always available here.
    */
   private logUnconfiguredNotice(declaredEnvironments: number): void {
      if (declaredEnvironments > 0) {
         return;
      }
      const checked = getUnresolvedPublisherConfigPath(this.serverRootPath);
      if (!checked) {
         return;
      }
      if (checked.explicit) {
         // getPublisherConfig already logged this; only /status needs it.
         this.unconfiguredNotice =
            `Serving with no environments: the --config path ${checked.path} was not found when the server started. ` +
            `Fix the path and restart. Environments can also be created at runtime through the API.`;
         return;
      }
      this.unconfiguredNotice =
         `Serving with no environments: no ${PUBLISHER_CONFIG_NAME} was found at ${checked.path} when the server started. ` +
         `Create one there (in Docker, mount it at that path) and restart, or pass --config <path>. ` +
         `Environments can also be created at runtime through the API.`;
      logger.info(this.unconfiguredNotice);
   }

   /**
    * Printed exactly once, right after markReady(), so it fires at the same
    * moment as every other readiness signal and never for a failed boot.
    */
   private emitReadinessLine(): void {
      let packages = 0;
      let loadErrors = this.failedEnvironments.size;
      for (const environment of this.environments.values()) {
         packages += environment.getServingPackageCount();
         loadErrors += environment.getFailedPackages().size;
      }
      try {
         process.stderr.write(
            formatReadinessLine({
               environments: this.environments.size,
               packages,
               loadErrors,
            }) + "\n",
         );
      } catch {
         // A failed stderr write must not fail a boot that is already
         // serving.
      }
   }

   /**
    * Give an environment its version rules, backed by its registry rows.
    * Idempotent. The first bind of an environment also settles what a crash
    * may have left on disk, before anything of it loads: version folders no
    * row owns, and unversioned trees a first versioned publish moved aside.
    */
   private async bindVersionRegistry(
      environment: Environment,
      environmentId: string,
   ): Promise<void> {
      const repository = this.storageManager.getRepository();
      const environmentName = environment.getEnvironmentName();
      const alreadyBound = environment.getVersionService() !== null;
      // Re-establish serve routing when a package loads, from its latest
      // successful materialization — so serving survives a restart, not only
      // a fresh build. The full entry map is returned; the environment splits
      // it by tier (colocated + storage=) in rebindServeBindingsFromLocalStore.
      // A version of a `scope: version` package reads only its own runs;
      // everything else reads the package's shared runs.
      environment.setStorageBindingResolver(
         storageBindingResolverFor(repository, environmentId),
      );
      this.attachHooks(environment, VERSION_HOOKS);
      environment.bindVersions(repository, environmentId, {
         downloaderFor: (packageName, location) => (stagingPath) =>
            this.downloadPackageInto(
               environmentName,
               packageName,
               location,
               stagingPath,
            ),
         ensurePackageRecord: async (packageName, description) => {
            const existing = await repository.getPackageByName(
               environmentId,
               packageName,
            );
            if (!existing) {
               await repository.createPackage({
                  environmentId,
                  name: packageName,
                  description: description ?? undefined,
                  manifestPath: "",
                  metadata: {},
               });
               return true;
            }
            if (
               description !== undefined &&
               description !== (existing.description ?? null)
            ) {
               await repository.updatePackage(existing.id, { description });
            }
            return false;
         },
         removePackageRecord: async (packageName) => {
            const existing = await repository.getPackageByName(
               environmentId,
               packageName,
            );
            // The row only: what is keyed by the package's name (an
            // unversioned package's runs and ledger) predates this publish.
            if (existing) await repository.deletePackageRecord(existing.id);
         },
         packageRemoved: (packageName) =>
            this.cleanupPackageEmbeddings(environmentName, packageName),
      });
      if (alreadyBound) return;
      try {
         const owned = new Map<string, Set<string>>();
         for (const version of await repository.listVersionsByEnvironment(
            environmentId,
         )) {
            const dirs = owned.get(version.packageName) ?? new Set<string>();
            dirs.add(version.dirName);
            owned.set(version.packageName, dirs);
         }
         await new VersionStore(environment.getEnvironmentPath()).cleanup(
            owned,
         );
      } catch (error) {
         logger.warn("Could not clean up package version folders", {
            environmentName,
            error,
         });
      }
   }

   /**
    * Fetch a package location into `targetPath`: a GitHub URL, a gs:// or
    * s3:// prefix (or .zip), or a local directory or .zip. Callers pass a
    * staging folder, so a long download holds no lock.
    */
   public async downloadPackageInto(
      environmentName: string,
      packageName: string,
      packageLocation: string,
      targetPath: string,
   ): Promise<void> {
      await this.packageSources.fetchForPublish(
         environmentName,
         packageName,
         packageLocation,
         targetPath,
      );
   }

   /**
    * Mirror an environment into the database. Every step runs even when an
    * earlier one could not save a connection row, because memory has already
    * changed and a skipped package sync would bring the environment back with
    * no packages after a restart. A connection that did not save is surfaced
    * only after all of them. Only a `request` that changed the environment
    * (a create or PATCH) fails, with a {@link ConnectionSaveError} naming it.
    * At `boot`, and on a `load` or `reload` from config that a read triggered,
    * the environment still loads and the failure is logged and counted: at
    * boot nothing is waiting for an answer, and a read changed nothing a
    * retry of it could save.
    */
   public async addEnvironmentToDatabase(
      environment: Environment,
      { syncPath = "request" }: { syncPath?: EnvironmentSyncPath } = {},
   ): Promise<void> {
      if (!environment) {
         logger.error("Cannot sync: environment is null or undefined");
         return;
      }

      const environmentName = environment.metadata?.name;
      if (!environmentName) {
         throw new Error("Environment name is required but not found");
      }

      const repository = this.storageManager.getRepository();

      const dbEnvironment = await this.addEnvironmentMetadata(
         environment,
         repository,
      );
      // Its version rules as soon as it has a row: the environment is already
      // served, and a publish to it needs them. A sync below that fails must
      // not leave it without them.
      await this.bindVersionRegistry(environment, dbEnvironment.id);

      const connectionFailures = await this.addConnections(
         environment,
         dbEnvironment.id,
         repository,
      );
      await this.syncStorageDestinations(
         environment,
         dbEnvironment.id,
         repository,
      );
      await this.addPackages(environment, dbEnvironment.id, repository);

      if (!hasConnectionSaveFailures(connectionFailures)) {
         logger.info(`Synced environment "${environmentName}" to database`);
         return;
      }
      connectionSaveFailureCounter().add(1, {
         path: syncPath,
      });
      const error = connectionSaveError(environmentName, connectionFailures);
      if (syncPath === "request") throw error;
      logger.error(
         `Loaded environment "${environmentName}", but its connections did not all save to the database`,
         {
            environmentName,
            connections: error.connectionNames,
            message: error.message,
         },
      );
   }

   public async deleteEnvironmentFromDatabase(
      environmentName: string,
   ): Promise<void> {
      // Before the metadata lookup: see deletePackageFromDatabase.
      this.cleanupEnvironmentEmbeddings(environmentName);

      const repository = this.storageManager.getRepository();

      const dbEnvironment =
         await repository.getEnvironmentByName(environmentName);

      if (!dbEnvironment) {
         logger.error(`Environment "${environmentName}" not found in database`);
         return;
      }

      // Cascades to connections and packages.
      await repository.deleteEnvironment(dbEnvironment.id);
      logger.info(`Deleted environment "${environmentName}" from database`);
   }

   private async addEnvironmentMetadata(
      environment: Environment,
      repository: ReturnType<typeof this.storageManager.getRepository>,
   ): Promise<{ id: string; name: string }> {
      const environmentName = environment.metadata?.name;
      if (!environmentName) {
         throw new Error("Environment name is required but not found");
      }
      const environmentPath = environment.metadata?.location || "";
      const environmentDescription = environment.metadata?.readme;

      const environmentData = {
         name: environmentName,
         path: environmentPath,
         description: environmentDescription,
         metadata: environment.metadata || {},
      };
      const existingEnvironment =
         await repository.getEnvironmentByName(environmentName);

      let dbEnvironment: { id: string; name: string };
      if (existingEnvironment) {
         const updateData = {
            description: environmentDescription,
            metadata: environment.metadata || {},
         };

         await repository.updateEnvironment(existingEnvironment.id, updateData);
         dbEnvironment = { id: existingEnvironment.id, name: environmentName };
      } else {
         dbEnvironment = await repository.createEnvironment(environmentData);
      }

      return dbEnvironment;
   }

   private async addPackages(
      environment: Environment,
      environmentId: string,
      repository: ReturnType<typeof this.storageManager.getRepository>,
   ): Promise<void> {
      const packages = await environment.listPackages();

      for (const pkg of packages) {
         if (!pkg.name) {
            logger.warn("Skipping package with undefined name");
            continue;
         }
         const name = pkg.name;
         // Under the package's lock, so a first versioned publish (which
         // takes it) cannot commit between the check and the write.
         await environment.withPackageLock(name, async () => {
            if (await this.isVersionedPackage(environment, name)) return;
            await this.addPackage(pkg, environmentId, repository);
         });
      }
   }

   /**
    * Whether a package has published versions. Its row is the version
    * registry's (its `latest` pointer, and the description a request set),
    * so a sync from the listing, which shows its latest version, must not
    * write over it.
    */
   private async isVersionedPackage(
      environment: Environment,
      packageName: string,
   ): Promise<boolean> {
      const versions = environment.getVersionService();
      return versions !== null && (await versions.isVersioned(packageName));
   }

   private async addPackage(
      pkg: components["schemas"]["Package"],
      environmentId: string,
      repository: ReturnType<typeof this.storageManager.getRepository>,
   ): Promise<void> {
      const pkgs = pkg as {
         name: string;
         description?: string;
         manifestPath?: string;
         metadata?: Record<string, unknown>;
      };

      const packageData = {
         environmentId,
         name: pkgs.name,
         description: pkgs.description ?? undefined,
         manifestPath: pkgs.manifestPath ?? "",
         metadata: pkgs.metadata ?? {},
      };

      try {
         await repository.createPackage(packageData);
         logger.info(`Synced package: ${pkg.name}`);
      } catch (err: unknown) {
         const error = err as Error;
         if (
            error.message?.includes("UNIQUE") ||
            error.message?.includes("Constraint")
         ) {
            await this.updatePackage(
               pkgs.name,
               environmentId,
               packageData,
               repository,
            );
         } else {
            logger.warn(`Failed to sync package ${pkg.name}:`, error.message);
         }
      }
   }

   private async updatePackage(
      packageName: string,
      environmentId: string,
      packageData: {
         description: string | undefined;
         manifestPath: string;
         metadata: Record<string, unknown>;
      },
      repository: ReturnType<typeof this.storageManager.getRepository>,
   ): Promise<void> {
      const existingPackage = await repository.getPackageByName(
         environmentId,
         packageName,
      );

      if (existingPackage) {
         await repository.updatePackage(existingPackage.id, packageData);
         logger.info(`Updated existing package: ${packageName}`);
      }
   }

   /**
    * Mirror an environment's connection list into the database so it survives
    * a restart. The list has replace semantics (an environment PATCH replaces
    * it wholesale), so rows the environment no longer holds are deleted too:
    * a left-behind row would resurrect a removed connection on the next load,
    * which restores connections from these rows.
    *
    * A failed row write does not stop the others, nor the rest of the sync:
    * the in-memory list has already changed, so each step still mirrors what
    * it can. The failures come back for the caller to surface once every
    * step has run (see {@link addEnvironmentToDatabase}).
    *
    * Runs under the environment's connection lock, which ConnectionService
    * holds while it writes a connection's row and then its in-memory entry.
    * Unlocked, a connection added between this snapshot of the list and the
    * listing of stored rows would have its fresh row pruned: it would serve
    * until the restart and then be gone. No caller holds the lock here, and
    * it is taken before any package lock, per the order Environment declares.
    */
   private async addConnections(
      environment: Environment,
      environmentId: string,
      repository: ReturnType<typeof this.storageManager.getRepository>,
   ): Promise<ConnectionSaveFailures> {
      return environment.runConnectionUpdateExclusive(() =>
         this.reconcileConnectionRows(environment, environmentId, repository),
      );
   }

   private async reconcileConnectionRows(
      environment: Environment,
      environmentId: string,
      repository: ReturnType<typeof this.storageManager.getRepository>,
   ): Promise<ConnectionSaveFailures> {
      const environmentName = environment.metadata?.name;
      const failures: ConnectionSaveFailures = {
         unsaved: [],
         unremoved: [],
         unlisted: false,
      };
      const kept = new Set<string>();
      for (const conn of environment.listApiConnections()) {
         if (!conn.name) {
            logger.warn("Skipping connection with undefined name");
            continue;
         }
         kept.add(conn.name);
         try {
            const existingConn = await repository.getConnectionByName(
               environmentId,
               conn.name,
            );
            if (existingConn) {
               await this.updateConnection(conn, environmentId, repository);
            } else {
               await this.addConnection(conn, environmentId, repository);
            }
         } catch (err: unknown) {
            logger.error(
               `Could not save connection "${conn.name}" of environment "${environmentName}"`,
               extractErrorDataFromError(err),
            );
            failures.unsaved.push(conn.name);
         }
      }

      // Prune only against a list that IS the desired state, as for
      // destinations: a fallback list (the config standing in for a missing
      // directory) would delete every connection registered over the API.
      if (!environment.hasAuthoritativeConnections()) {
         logger.warn(
            `Not reconciling stored connections for "${environmentName}": the in-memory list is a fallback, not the desired state`,
         );
         return failures;
      }

      let stored: Awaited<ReturnType<typeof repository.listConnections>>;
      try {
         stored = await repository.listConnections(environmentId);
      } catch (err: unknown) {
         logger.error(
            `Could not list the stored connections of environment "${environmentName}", so none it removed were deleted`,
            extractErrorDataFromError(err),
         );
         failures.unlisted = true;
         return failures;
      }
      for (const row of stored) {
         if (kept.has(row.name)) continue;
         try {
            await repository.deleteConnection(row.id);
            logger.info(
               `Removed connection ${row.name} from environment ${environmentName}`,
            );
         } catch (err: unknown) {
            logger.error(
               `Could not delete removed connection "${row.name}" of environment "${environmentName}"`,
               extractErrorDataFromError(err),
            );
            failures.unremoved.push(row.name);
         }
      }
      return failures;
   }

   /**
    * Mirror an environment's destination list into the database so it
    * survives a restart. Like {@link addConnections} it also deletes rows the
    * environment no longer holds: the list has replace semantics, so a
    * left-behind row would resurrect a removed destination on the next load.
    */
   private async syncStorageDestinations(
      environment: Environment,
      environmentId: string,
      repository: ReturnType<typeof this.storageManager.getRepository>,
   ): Promise<void> {
      try {
         const destinations = environment.listStorageDestinations();
         const kept = new Set<string>();

         for (const destination of destinations) {
            if (!destination.name || !destination.type) {
               continue;
            }
            kept.add(destination.name);
            await repository.upsertStorageDestination({
               environmentId,
               name: destination.name,
               type: destination.type,
               config: destination,
            });
         }

         // Prune only against a list that IS the desired state. After a failed
         // read the list is unknown, and pruning would permanently delete every
         // registration over a transient error.
         if (!environment.hasAuthoritativeStorageDestinations()) {
            logger.warn(
               `Not reconciling stored storage destinations for "${environment.metadata?.name}": this environment's destinations were never read, so the in-memory list is not the desired state`,
            );
            return;
         }

         const stored = await repository.listStorageDestinations(environmentId);
         for (const row of stored) {
            if (!kept.has(row.name)) {
               await repository.deleteStorageDestination(row.id);
               logger.info(
                  `Removed storage destination ${row.name} from environment ${environment.metadata?.name}`,
               );
            }
         }
      } catch (err: unknown) {
         const error = err as Error;
         logger.error(
            `Error syncing storage destinations for "${environment.metadata?.name}":`,
            error,
         );
      }
   }

   public async addConnection(
      conn: ApiConnection,
      environmentId: string,
      repository: ResourceRepository,
   ): Promise<void> {
      if (!conn.name) {
         logger.warn("Skipping connection with undefined name");
         return;
      }

      const connectionData = {
         environmentId,
         name: conn.name,
         type: conn.type as ConnectionRecord["type"],
         config: conn,
      };

      try {
         await repository.createConnection(connectionData);
         logger.info(`Created connection: ${conn.name}`);
      } catch (err: unknown) {
         const error = err as Error;
         logger.error(`Failed to create connection ${conn.name}:`, error);
         throw error;
      }
   }

   public async updateConnection(
      conn: ApiConnection,
      environmentId: string,
      repository: ResourceRepository,
   ): Promise<void> {
      if (!conn.name) {
         throw new Error("Connection name is required for update");
      }

      const existingConn = await repository.getConnectionByName(
         environmentId,
         conn.name,
      );

      if (!existingConn) {
         logger.error(`Connection "${conn.name}" not found in environment`);
      }

      const connectionData = {
         type: conn.type as ConnectionRecord["type"],
         config: conn,
      };

      try {
         if (existingConn) {
            await repository.updateConnection(existingConn.id, connectionData);
         }
         logger.info(`Updated connection: ${conn.name}`);
      } catch (err: unknown) {
         const error = err as Error;
         logger.error(`Failed to update connection ${conn.name}:`, error);
         throw error;
      }
   }

   public async addPackageToDatabase(
      environmentName: string,
      packageName: string,
   ): Promise<void> {
      const environment = await this.getEnvironment(
         parseEnvironmentName(environmentName),
         false,
      );
      const repository = this.storageManager.getRepository();

      const dbEnvironment =
         await repository.getEnvironmentByName(environmentName);

      if (!dbEnvironment) {
         logger.error(`Environment "${environmentName}" not found in database`);
         throw new Error(
            `Environment "${environmentName}" not found in database`,
         );
      }

      const packages = await environment.listPackages();
      const pkg = packages.find((p) => p.name === packageName);

      if (!pkg) {
         logger.warn(`Package "${packageName}" not found in environment`);
         return;
      }
      // Under the package's lock, as addPackages does, so a first versioned
      // publish (which takes it) cannot commit between the check and the
      // write and have its row written over.
      const synced = await environment.withPackageLock(
         packageName,
         async () => {
            if (await this.isVersionedPackage(environment, packageName)) {
               return false;
            }
            await this.addPackage(pkg, dbEnvironment.id, repository);
            return true;
         },
      );
      if (synced) logger.info(`Synced package "${packageName}" to database`);
   }

   public async deletePackageFromDatabase(
      environmentName: string,
      packageName: string,
   ): Promise<void> {
      // Before the metadata lookups: embeddings are written off the
      // in-memory environment, which can outlive a lost metadata row,
      // so the cleanup must not sit behind the missing-row early return.
      this.cleanupPackageEmbeddings(environmentName, packageName);

      const repository = this.storageManager.getRepository();

      const dbEnvironment =
         await repository.getEnvironmentByName(environmentName);

      if (!dbEnvironment) {
         logger.error(`Environment "${environmentName}" not found in database`);
         return;
      }

      const existingPackage = await repository.getPackageByName(
         dbEnvironment.id,
         packageName,
      );

      if (existingPackage) {
         await repository.deletePackage(existingPackage.id);
         logger.info(`Deleted package "${packageName}" from database`);
      }
   }

   /**
    * Best-effort, non-blocking: fire the packageDeleted hook, which drops a
    * deleted package's cached embeddings so churn does not grow publisher.db
    * forever. Not awaited: it queues behind the package's sync mutex, which a
    * bulk embed can hold for minutes. An orphaned row is inert, so failure is
    * logged, never propagated.
    */
   public cleanupPackageEmbeddings(
      environmentName: string,
      packageName: string,
   ): void {
      const hook = this.hooks.packageDeleted;
      if (!hook) return;
      try {
         hook(environmentName, packageName).catch((error: unknown) => {
            logger.warn("Failed to clean up entity embeddings for package", {
               environmentName,
               packageName,
               error: error instanceof Error ? error.message : String(error),
            });
         });
      } catch (error) {
         logger.warn("Failed to clean up entity embeddings for package", {
            environmentName,
            packageName,
            error: error instanceof Error ? error.message : String(error),
         });
      }
   }

   /** Environment-wide variant of {@link cleanupPackageEmbeddings}. */
   private cleanupEnvironmentEmbeddings(environmentName: string): void {
      const hook = this.hooks.environmentDeleted;
      if (!hook) return;
      try {
         hook(environmentName).catch((error: unknown) => {
            logger.warn(
               "Failed to clean up entity embeddings for environment",
               {
                  environmentName,
                  error: error instanceof Error ? error.message : String(error),
               },
            );
         });
      } catch (error) {
         logger.warn("Failed to clean up entity embeddings for environment", {
            environmentName,
            error: error instanceof Error ? error.message : String(error),
         });
      }
   }

   private async cleanupAndCreatePublisherPath() {
      const reInit = settings.get("initializeStorage");

      // mkdir rather than stat: Bun on Windows resolves fs.promises.stat to
      // undefined instead of throwing.
      await fs.promises.mkdir(this.serverRootPath, { recursive: true });

      if (reInit) {
         const uploadDocsPath = path.join(
            this.serverRootPath,
            PUBLISHER_DATA_DIR,
         );
         logger.info(
            `Reinitialization mode: Cleaning up upload documents path ${uploadDocsPath}`,
         );
         try {
            await fs.promises.rm(uploadDocsPath, {
               recursive: true,
               force: true,
            });
         } catch (error) {
            if ((error as NodeJS.ErrnoException).code === "EACCES") {
               logger.warn(
                  `Permission denied, skipping cleanup of upload documents path ${uploadDocsPath}`,
               );
            } else if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
               throw error;
            }
         }
      } else {
         logger.info(`Using existing publisher path`);
      }

      const uploadDocsPath = path.join(this.serverRootPath, PUBLISHER_DATA_DIR);
      await fs.promises.mkdir(uploadDocsPath, { recursive: true });
   }

   public async listEnvironments(
      skipInitializationCheck: boolean = false,
      options: PackageListingOptions = {},
   ) {
      if (!skipInitializationCheck) {
         await this.finishedInitialization;
      }
      return Promise.all(
         Array.from(this.environments.values()).map((environment) =>
            environment.serialize(options),
         ),
      );
   }

   public async getStatus(
      options: StatusOptions = {},
   ): Promise<ServerStatusBody> {
      // Back-pressure reads as "throttled" so the control plane can stop
      // routing to this worker; "draining" takes precedence.
      const baseState = getOperationalState();
      const operationalState = (
         baseState !== "draining" &&
         (this.memoryGovernor?.isBackpressured() ?? false)
            ? "throttled"
            : baseState
      ) as ServerStatusBody["operationalState"];

      const status: ServerStatusBody = {
         timestamp: Date.now(),
         environments: [],
         initialized: this.isInitialized,
         frozenConfig: isPublisherConfigFrozen(),
         operationalState,
         version: SERVER_VERSION,
      };
      // Whether a publish from a location publishes versions, so an
      // orchestrator mid-rollout can tell. A server from before versions sends
      // no field, which means "off".
      status.packageVersioning = isVersioningEnabled() ? "on" : "off";
      status.versionPromotion = getVersionPromotionMode();
      // The Console's model page. Absent, as from a server that predates the
      // setting, reads as "explorer".
      status.modelPage = getModelPageMode();

      // `/status` entries carry each package's serving state; a listing does not.
      const environments = await this.listEnvironments(true, {
         ...options,
         operational: true,
      });

      await Promise.all(
         environments.map(async (environment) => {
            try {
               // A versioned package is listed once per version it holds (each
               // with its own versionId), not once as its latest.
               const versionEntries =
                  (await this.environments
                     .get(environment.name ?? "")
                     ?.listVersionEntries(options)) ?? new Map();
               const packages =
                  versionEntries.size === 0
                     ? environment.packages
                     : [
                          ...(environment.packages ?? []).filter(
                             (p) => !versionEntries.has(p.name ?? ""),
                          ),
                          ...[...versionEntries.values()].flat(),
                       ];
               environment.packages = packages;
               const connections = environment.connections;

               logger.debug(`Environment ${environment.name} status:`, {
                  connectionsCount: environment.connections?.length || 0,
                  packagesCount: packages?.length || 0,
               });

               const _connections = connections?.map((connection) => {
                  return {
                     ...connection,
                     attributes: undefined,
                  };
               });

               const _environment = {
                  ...environment,
                  connections: _connections,
                  // Every listed entry carries its name (the listing sets it).
                  packages:
                     packages as components["schemas"]["PackageStatusEntry"][],
               };
               environment.connections = _connections;
               status.environments.push(_environment);
            } catch (error) {
               logger.error("Error listing packages and connections", {
                  error,
               });
               throw new Error(
                  "Error listing packages and connections: " + error,
               );
            }
         }),
      );

      // Collected AFTER listEnvironments: serialize() loads packages, which is
      // what records their failures.
      const loadErrors: LoadError[] = [];
      for (const [environmentName, message] of this.failedEnvironments) {
         loadErrors.push({ environment: environmentName, message });
      }
      for (const [environmentName, environment] of this.environments) {
         for (const [packageName, message] of environment.getFailedPackages()) {
            loadErrors.push({
               environment: environmentName,
               package: packageName,
               message,
            });
         }
         // Stale packages are SERVING, but from a model older than the files on
         // disk because the latest reload failed to compile. Without this a
         // broken save is invisible off-box.
         for (const [
            packageName,
            stale,
         ] of environment.getStaleCompileErrors()) {
            loadErrors.push({
               environment: environmentName,
               package: packageName,
               message: stale.message,
               stale: true,
               failedAt: stale.failedAt,
            });
         }
      }
      // Absent, not empty, when nothing failed.
      if (loadErrors.length > 0) {
         status.loadErrors = loadErrors;
      }
      // The one place a /status poller learns the server found no config.
      if (this.unconfiguredNotice && status.environments.length === 0) {
         status.emptyReason = this.unconfiguredNotice;
      }
      if (this.initError) {
         status.initError = this.initError;
      }

      return status;
   }

   /**
    * The environment currently held in memory under `name`, or undefined.
    * Never loads, so it cannot bring back an environment that was removed.
    */
   public peekEnvironment(name: string): Environment | undefined {
      return this.environments.get(name);
   }

   public async getEnvironment(
      environmentName: EnvironmentName,
      reload: boolean = false,
   ): Promise<Environment> {
      await this.finishedInitialization;
      // The sink-side check (path_safety.ts); the brand says it already ran.
      assertSafePackageName(environmentName);

      const environment = this.environments.get(environmentName);
      if (environment !== undefined && !reload) {
         return environment;
      }

      // Serialize so concurrent requests do not create the environment twice.
      let environmentMutex = this.environmentMutexes.get(environmentName);
      if (environmentMutex?.isLocked()) {
         await environmentMutex.waitForUnlock();
         const existingEnvironment = this.environments.get(environmentName);
         if (existingEnvironment && !reload) {
            return existingEnvironment;
         }
      }
      environmentMutex = new Mutex();
      this.environmentMutexes.set(environmentName, environmentMutex);

      return environmentMutex.runExclusive(async () => {
         const existingEnvironment = this.environments.get(environmentName);
         if (existingEnvironment !== undefined && !reload) {
            return existingEnvironment;
         }

         const environmentManifest =
            await EnvironmentStore.reloadEnvironmentManifest(
               this.serverRootPath,
            );
         const environmentConfig = environmentManifest.environments.find(
            (e) => e.name === environmentName,
         );
         const environmentPath =
            existingEnvironment?.metadata.location ||
            environmentConfig?.packages[0]?.location;
         if (!environmentPath) {
            throw new EnvironmentNotFoundError(
               `Environment "${environmentName}" could not be resolved to a path.`,
               {
                  environmentName,
                  // The loaded set, which is what list_packages shows.
                  availableEnvironments: Array.from(
                     this.environments.keys(),
                  ).sort(),
               },
            );
         }
         return await this.addEnvironment(
            {
               name: environmentName,
               resource: environmentResource(environmentName),
               connections: environmentConfig?.connections || [],
               // Only asserted when the config declares destinations: an empty
               // list reads as "clear them", wiping API-registered ones on reload.
               ...(environmentConfig?.storageDestinations?.length
                  ? {
                       storageDestinations:
                          environmentConfig.storageDestinations,
                    }
                  : {}),
            },
            false,
            // The config's list (empty for an environment created over the
            // API) is not a request to delete stored connections.
            {
               connectionsAreFallback: true,
               // A read: a connection row that does not save is logged and
               // counted, not answered with a 500 the caller cannot act on.
               syncPath: reload ? "reload" : "load",
            },
         );
      });
   }

   /**
    * `storageDestinationsAreFallback` marks the payload's destinations as a best
    * guess, so the database sync must not prune against them. It is a parameter
    * because this method syncs before it returns.
    *
    * `syncPath` decides how a connection row that did not save is surfaced
    * (see {@link addEnvironmentToDatabase}). It defaults from
    * `skipInitialization`, which only the boot paths (the config load and the
    * database restore) set: `boot` there, where the environment must still
    * load, and `request`, a failed request, otherwise. A read that loads or
    * reloads from config passes its own.
    */
   public async addEnvironment(
      environment: ApiEnvironment,
      skipInitialization: boolean = false,
      {
         storageDestinationsAreFallback = false,
         // The same for connections: the sync must not prune stored rows
         // against the payload's list.
         connectionsAreFallback = false,
         // The config's packages are published as versions once the
         // environment exists (see addConfiguredEnvironment), not loaded here.
         configuredPackagesPublished = false,
         syncPath = (skipInitialization
            ? "boot"
            : "request") as EnvironmentSyncPath,
      } = {},
   ) {
      if (!skipInitialization) {
         await this.finishedInitialization;
      }
      if (!skipInitialization && this.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }
      assertSafePackageName(environment.name);
      const environmentName = environment.name;
      for (const _package of environment.packages || []) {
         assertSafePackageName(_package.name);
      }
      const existingEnvironment = this.environments.get(environmentName);
      if (existingEnvironment) {
         const updatedEnvironment =
            await existingEnvironment.update(environment);
         this.environments.set(environmentName, updatedEnvironment);
         if (storageDestinationsAreFallback) {
            updatedEnvironment.markStorageDestinationsUnknown();
         }
         if (connectionsAreFallback) {
            updatedEnvironment.markConnectionsUnknown();
         }
         await this.addEnvironmentToDatabase(updatedEnvironment, {
            syncPath,
         });
         return updatedEnvironment;
      }
      const environmentManifest =
         await EnvironmentStore.reloadEnvironmentManifest(this.serverRootPath);
      const environmentConfig = environmentManifest.environments.find(
         (e) => e.name === environmentName,
      );
      const hasPackages =
         (environment?.packages && environment.packages.length > 0) ||
         (!configuredPackagesPublished &&
            environmentConfig?.packages &&
            environmentConfig.packages.length > 0);
      let absoluteEnvironmentPath: string;
      let mountErrors: ReadonlyMap<string, string> = new Map();
      if (hasPackages) {
         const packagesToProcess =
            environment?.packages || environmentConfig?.packages || [];
         const loaded = await this.loadEnvironmentIntoDisk(
            environmentName,
            packagesToProcess,
         );
         absoluteEnvironmentPath = loaded.path;
         mountErrors = loaded.mountErrors;
      } else {
         absoluteEnvironmentPath = await this.scaffoldEnvironment(environment);
      }
      const newEnvironment = await Environment.create(
         environmentName,
         absoluteEnvironmentPath,
         environment.connections || [],
         environment.storageDestinations || [],
      );
      newEnvironment.setMemoryGovernor(this.memoryGovernor);
      this.attachHooks(newEnvironment, ["packageLoaded"]);

      if (!newEnvironment.metadata) newEnvironment.metadata = {};
      newEnvironment.metadata.location = absoluteEnvironmentPath;

      // Attach before registering the packages below: those packages are
      // configured but un-mounted, and this is the only record of why.
      for (const [packageName, message] of mountErrors) {
         newEnvironment.setPackageMountError(packageName, message);
      }

      this.environments.set(environmentName, newEnvironment);
      // It loaded, so any earlier failure for this name is stale.
      this.failedEnvironments.delete(environmentName);

      environment?.packages?.forEach((_package) => {
         if (_package.name) {
            newEnvironment.registerConfiguredPackage(_package.name);
         }
      });

      if (storageDestinationsAreFallback) {
         newEnvironment.markStorageDestinationsUnknown();
      }
      if (connectionsAreFallback) {
         newEnvironment.markConnectionsUnknown();
      }
      await this.addEnvironmentToDatabase(newEnvironment, {
         syncPath,
      });

      return newEnvironment;
   }

   public async updateEnvironment(environment: ApiEnvironment) {
      await this.finishedInitialization;
      if (this.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }
      validateEnvironmentAzureUrls(environment);
      assertSafePackageName(environment.name);
      const environmentName = environment.name;
      for (const _package of environment.packages || []) {
         assertSafePackageName(_package.name);
      }
      const existingEnvironment = this.environments.get(environmentName);
      if (!existingEnvironment) {
         throw new EnvironmentNotFoundError(
            `Environment ${environmentName} not found`,
         );
      }
      const updatedEnvironment = await existingEnvironment.update(environment);
      this.environments.set(environmentName, updatedEnvironment);
      await this.addEnvironmentToDatabase(updatedEnvironment);
      return updatedEnvironment;
   }

   public async deleteEnvironment(
      environmentName: string,
   ): Promise<Environment | undefined> {
      await this.finishedInitialization;
      if (this.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }
      assertSafePackageName(environmentName);
      const environment = this.environments.get(environmentName);
      if (!environment) {
         return;
      }

      const environmentPath = environment.metadata?.location;

      await environment.closeAllConnections();

      this.environments.delete(environmentName);
      await this.deleteEnvironmentFromDatabase(environmentName);
      if (environmentPath) {
         try {
            await fs.promises.rm(environmentPath, {
               recursive: true,
               force: true,
            });
            logger.info(`Deleted environment directory: ${environmentPath}`);
         } catch (err) {
            logger.error("Error removing environment directory", {
               error: err,
            });
         }
      }

      return environment;
   }

   public static async reloadEnvironmentManifest(
      serverRootPath: string,
   ): Promise<ProcessedPublisherConfig> {
      try {
         return getProcessedPublisherConfig(serverRootPath);
      } catch (error) {
         if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
            // A config Publisher cannot honour (malformed JSON, a rejected
            // shape, an unset `${VAR}`) is raised, not absorbed into an empty
            // manifest, which would boot "serving" with nothing and no
            // loadErrors entry to say why. At boot it becomes
            // PUBLISHER_INIT_FAILED; a serving server is affected only on an
            // explicit reload, which answers with the reason.
            throw new PublisherConfigError(PUBLISHER_CONFIG_NAME, error);
         } else {
            // No publisher.config.json: one environment per directory.
            try {
               const entries = await fs.promises.readdir(serverRootPath, {
                  withFileTypes: true,
               });
               const environments: ProcessedEnvironment[] = [];
               for (const entry of entries) {
                  if (entry.isDirectory()) {
                     environments.push({
                        name: entry.name,
                        packages: [
                           {
                              name: entry.name,
                              location: `./${entry.name}` as const,
                           },
                        ],
                        connections: [],
                        storageDestinations: [],
                     });
                  }
               }
               return { environments };
            } catch (lsError) {
               // An Error's fields are not enumerable; `{ error }` logs {}.
               logger.error(`Error listing directories in ${serverRootPath}`, {
                  error:
                     lsError instanceof Error
                        ? lsError.message
                        : String(lsError),
               });
               return { environments: [] };
            }
         }
      }
   }

   private async scaffoldEnvironment(environment: ApiEnvironment) {
      assertSafePackageName(environment.name);
      const environmentName = environment.name;
      const absoluteEnvironmentPath = safeJoinUnderRoot(
         this.serverRootPath,
         PUBLISHER_DATA_DIR,
         environmentName,
      );
      await fs.promises.mkdir(absoluteEnvironmentPath, { recursive: true });
      if (environment.readme) {
         await fs.promises.writeFile(
            safeJoinUnderRoot(absoluteEnvironmentPath, "README.md"),
            environment.readme,
         );
      }
      return absoluteEnvironmentPath;
   }

   /**
    * Absolute on-disk path for a local package `location` (including one
    * POSTed at runtime), anchored at the active config's directory, else the
    * server root.
    */
   private resolveLocalPath(location: string): string {
      return resolvePackageLocation(
         location,
         getPublisherConfigDir(this.serverRootPath) ?? this.serverRootPath,
      );
   }

   /**
    * Isolate a location failure to the packages it affects instead of
    * aborting the environment: each one named here is left un-mounted and
    * reported in /status loadErrors, while siblings serve.
    *
    * The message is kept because the later lazy-load error ("Package manifest
    * ... does not exist.") would point at publisher_data/ instead of the bad
    * location. Redacted because it reaches an HTTP response body.
    */
   private recordMountFailure(
      error: unknown,
      location: string,
      packageNames: string[],
      mountErrors: Map<string, string>,
   ): void {
      logger.error(
         `Failed to download or mount location "${location}"`,
         extractErrorDataFromError(error),
      );
      const message = redactPgSecrets(messageWithFilesystemCause(error));
      for (const packageName of packageNames) {
         mountErrors.set(packageName, message);
      }
   }

   /**
    * Mount an environment's packages, each at `<targetRoot>/<name>`:
    * `targetRoot` is the environment's own folder when its packages are served
    * from where they are mounted, or a staging folder they are published from
    * (publishConfiguredPackages).
    */
   private async loadEnvironmentIntoDisk(
      environmentName: string,
      packages: ApiEnvironment["packages"],
      targetRoot?: string,
   ) {
      assertSafePackageName(environmentName);
      const absoluteTargetPath =
         targetRoot ??
         safeJoinUnderRoot(
            this.serverRootPath,
            PUBLISHER_DATA_DIR,
            environmentName,
         );
      const mountErrors = new Map<string, string>();

      await fs.promises.mkdir(absoluteTargetPath, { recursive: true });

      if (!packages || packages.length === 0) {
         throw new PackageNotFoundError(
            `No packages found for environment ${environmentName}`,
         );
      }

      // Grouped by location so each is downloaded once.
      const locationGroups = new Map<
         string,
         Array<{ name: string; location: string }>
      >();

      for (const _package of packages) {
         if (!_package.name) {
            throw new PackageNotFoundError(`Package has no name specified`);
         }

         if (!_package.location) {
            throw new PackageNotFoundError(
               `Package ${_package.name} has no location specified`,
            );
         }

         // GitHub locations group by repository, in HTTPS form.
         let locationKey = _package.location;
         if (isGitHubURL(_package.location)) {
            const githubInfo = parseGitHubUrl(_package.location);
            if (githubInfo) {
               locationKey = `https://github.com/${githubInfo.owner}/${githubInfo.repoName}`;
            }
         }

         if (!locationGroups.has(locationKey)) {
            locationGroups.set(locationKey, []);
         }
         locationGroups.get(locationKey)!.push({
            name: _package.name,
            location: _package.location,
         });
      }

      const totalPackages = packages.length;
      let mountedCount = 0;
      for (const [groupedLocation, packagesForLocation] of locationGroups) {
         // A short hash keeps paths under Windows MAX_PATH.
         const locationHash = crypto
            .createHash("sha256")
            .update(groupedLocation)
            .digest("hex")
            .substring(0, 16);
         const tempDownloadPath = safeJoinUnderRoot(
            absoluteTargetPath,
            `.temp_${locationHash}`,
         );
         await fs.promises.mkdir(tempDownloadPath, { recursive: true });
         logger.info(`Created temporary directory: ${tempDownloadPath}`);
         // Two failure scopes: a failed download loses every package at this
         // location, while a failed extract must not strand its siblings.
         let downloaded = true;
         try {
            await this.downloadOrMountLocation(
               groupedLocation,
               tempDownloadPath,
               environmentName,
               "shared",
               packagesForLocation.map((p) => p.name),
            );
         } catch (error) {
            downloaded = false;
            this.recordMountFailure(
               error,
               groupedLocation,
               packagesForLocation.map((p) => p.name),
               mountErrors,
            );
         }
         for (const _package of downloaded ? packagesForLocation : []) {
            // Declared out here so the catch cleans up exactly this path.
            // Never re-derive it from the name there: a package called "."
            // would hand the cleanup the whole environment directory.
            let absolutePackagePath: string | undefined;
            try {
               const packageDir = _package.name;
               assertSafePackageName(packageDir);
               absolutePackagePath = safeJoinUnderRoot(
                  absoluteTargetPath,
                  packageDir,
               );
               const isLocalArchive =
                  isLocalPath(_package.location) &&
                  _package.location.endsWith(".zip");
               let sourcePath: string;
               if (isGitHubURL(_package.location)) {
                  const githubInfo = parseGitHubUrl(_package.location);
                  if (githubInfo && githubInfo.packagePath) {
                     // The subdirectory after /tree/<branch>/.
                     const subPathMatch =
                        _package.location.match(/\/tree\/[^/]+\/(.+)$/);
                     if (subPathMatch) {
                        sourcePath = safeJoinUnderRoot(
                           tempDownloadPath,
                           subPathMatch[1],
                        );
                     } else {
                        // The repo itself is the package.
                        sourcePath = tempDownloadPath;
                     }
                  } else {
                     sourcePath = tempDownloadPath;
                  }
               } else {
                  if (isLocalArchive) {
                     // The archive was extracted into the download dir.
                     sourcePath = tempDownloadPath;
                  } else if (isLocalPath(_package.location)) {
                     // Resolved as in `downloadOrMountLocation`, so a relative
                     // location can reach the in-place mount branch.
                     sourcePath = this.resolveLocalPath(_package.location);
                  } else {
                     sourcePath = safeJoinUnderRoot(
                        tempDownloadPath,
                        groupedLocation,
                     );
                  }
               }

               const sourceExists = await fs.promises
                  .access(sourcePath)
                  .then(() => true)
                  .catch(() => false);

               if (sourceExists) {
                  // Watch mode links a local-dir package in place so source
                  // edits live-reload. Not an archive: its extracted copy is
                  // removed after this loop.
                  const isInPlace =
                     this.inPlaceEnvs.has(environmentName) &&
                     isLocalPath(_package.location) &&
                     !isLocalArchive;
                  if (isInPlace) {
                     await clearMountTarget(absolutePackagePath);
                     const absoluteSourcePath = path.resolve(sourcePath);
                     // On Windows a "dir" symlink needs elevation and a
                     // junction does not; chokidar watches through both.
                     const linkType =
                        process.platform === "win32" ? "junction" : "dir";
                     try {
                        await fs.promises.symlink(
                           absoluteSourcePath,
                           absolutePackagePath,
                           linkType,
                        );
                        logger.info(
                           `In-place mount (watch mode): linked package "${packageDir}" -> "${absoluteSourcePath}" (${++mountedCount}/${totalPackages})`,
                        );
                     } catch (linkError) {
                        // Degrade to a copy (e.g. locked-down Windows denying
                        // even junctions): it serves, without live reload.
                        const code =
                           (linkError as NodeJS.ErrnoException)?.code ??
                           String(linkError);
                        logger.warn(
                           `In-place mount failed for package "${packageDir}" (${code}); falling back to a copy. Source-edit live reload is disabled for this package.`,
                        );
                        await clearMountTarget(absolutePackagePath);
                        await fs.promises.mkdir(absolutePackagePath, {
                           recursive: true,
                        });
                        await fs.promises.cp(sourcePath, absolutePackagePath, {
                           recursive: true,
                        });
                        logger.info(
                           `Copied package "${packageDir}" (${++mountedCount}/${totalPackages})`,
                        );
                     }
                  } else {
                     if (
                        this.inPlaceEnvs.has(environmentName) &&
                        !isLocalPath(_package.location)
                     ) {
                        logger.warn(
                           `Watch mode: package "${packageDir}" has remote location "${_package.location}" — falling back to copy. Source-edit live reload won't work for this package; clone the source locally and use a local-dir location to enable it.`,
                        );
                     } else if (
                        this.inPlaceEnvs.has(environmentName) &&
                        isLocalArchive
                     ) {
                        logger.warn(
                           `Watch mode: package "${packageDir}" is an archive "${_package.location}" — extracted and copied. Source-edit live reload won't work for this package; use an unpacked local-dir location to enable it.`,
                        );
                     }
                     // Clear a stale target first (e.g. a symlink from a prior
                     // --watch-env run), or fs.cp throws onto it.
                     await clearMountTarget(absolutePackagePath);
                     await fs.promises.mkdir(absolutePackagePath, {
                        recursive: true,
                     });
                     await fs.promises.cp(sourcePath, absolutePackagePath, {
                        recursive: true,
                     });
                     logger.info(
                        `Extracted package "${packageDir}" from ${groupedLocation.startsWith("https://github.com/") && _package.location.includes("/tree/") ? "GitHub subdirectory" : "shared download"} (${++mountedCount}/${totalPackages})`,
                     );
                  }
               } else {
                  // No source subdirectory: the whole download is the package.
                  await clearMountTarget(absolutePackagePath);
                  await fs.promises.mkdir(absolutePackagePath, {
                     recursive: true,
                  });
                  await fs.promises.cp(tempDownloadPath, absolutePackagePath, {
                     recursive: true,
                  });
                  logger.info(
                     `Copied entire download as package "${packageDir}" (${++mountedCount}/${totalPackages})`,
                  );
               }
            } catch (error) {
               this.recordMountFailure(
                  error,
                  groupedLocation,
                  [_package.name],
                  mountErrors,
               );
               // Leave nothing behind: a partial copy can hold the manifest
               // but not the data, load cleanly, clear the error just
               // recorded, and serve incomplete content.
               if (absolutePackagePath) {
                  await clearMountTarget(absolutePackagePath);
               }
            }
         }
         try {
            await fs.promises.rm(tempDownloadPath, {
               recursive: true,
               force: true,
            });
         } catch (error) {
            logger.warn(
               `Failed to clean up temporary download directory "${tempDownloadPath}"`,
               {
                  error,
               },
            );
         }
      }

      return { path: absoluteTargetPath, mountErrors };
   }

   private async downloadOrMountLocation(
      location: string,
      targetPath: string,
      environmentName: string,
      packageName: string,
      packageNames?: string[],
   ) {
      const selected = this.packageSources.forConfiguredLocation(
         location,
         targetPath,
      );
      if (!selected) {
         const errorMsg = `Invalid package path: "${location}". Must be an absolute mounted path or a GCS/S3/GitHub URI.`;
         logger.error(errorMsg, { environmentName, location });
         throw new PackageNotFoundError(errorMsg);
      }
      try {
         logger.info(selected.starting);
         await selected.source.fetch(selected.location, targetPath, {
            environmentName,
            packageName,
            progress: { environmentName, packageNames },
         });
      } catch (error) {
         const errorData = extractErrorDataFromError(error);
         logger.error(
            `Failed to ${selected.action} "${selected.subject}"`,
            errorData,
         );
         throw new PackageNotFoundError(
            `Failed to ${selected.action}: ${selected.subject}`,
            { cause: error },
         );
      }
   }
}
