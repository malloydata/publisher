// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { GetObjectCommand, S3 } from "@aws-sdk/client-s3";
import { Storage } from "@google-cloud/storage";
import { Mutex } from "async-mutex";
import crypto from "crypto";
import extract from "extract-zip";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { simpleGit, type SimpleGitProgressEvent } from "simple-git";
import { Writable } from "stream";
import { components } from "../api";
import {
   getProcessedPublisherConfig,
   getPublisherConfigDir,
   getUnresolvedPublisherConfigPath,
   getVersionPromotionMode,
   isVersioningEnabled,
   isMcpIncludeHiddenFilesAndSources,
   isPublisherConfigFrozen,
   ProcessedEnvironment,
   ProcessedPublisherConfig,
} from "../config";
import {
   API_PREFIX,
   PUBLISHER_CONFIG_NAME,
   PUBLISHER_DATA_DIR,
} from "../constants";
import {
   BadRequestError,
   EnvironmentNotFoundError,
   FrozenConfigError,
   messageWithFilesystemCause,
   PackageNotFoundError,
   PublisherConfigError,
} from "../errors";
import { getOperationalState, markNotReady, markReady } from "../health";
import { formatDuration, logger } from "../logger";
import {
   deleteEnvironmentEmbeddings,
   deletePackageEmbeddings,
} from "../mcp/tools/embedding_index";
import { redactPgSecrets } from "../pg_helpers";
import {
   assertSafeEnvironmentPath,
   assertSafePackageName,
   safeJoinUnderRoot,
} from "../path_safety";
import {
   Connection,
   type Environment as DbEnvironment,
   type ResourceRepository,
} from "../storage/DatabaseInterface";
import { StorageConfig, StorageManager } from "../storage/StorageManager";
import { extractErrorDataFromError, validateAzureUrl } from "./db_utils";
import { Environment, PackageStatus } from "./environment";
import { formatPublishRejections } from "./publish_checks";
import { assertGoogleCredentialsIsNotADirectory } from "./google_credentials";
import type { Package } from "./package";
import { VersionStore } from "./versions/version_store";
import { storageBindingResolverFor } from "./versions/materialization_scope";
import type { PackageMemoryGovernor } from "./package_memory_governor";
import { SERVER_VERSION } from "../version";
type ApiEnvironment = components["schemas"]["Environment"];
type ApiConnection = components["schemas"]["Connection"];
type LoadError = NonNullable<
   components["schemas"]["ServerStatus"]["loadErrors"]
>[number];

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

/**
 * Absolute on-disk path for a local package `location`.
 *
 * `~/` (POSIX form only) expands to the home directory. Anything still
 * relative resolves against `anchorDir`, the directory holding the active
 * config, so a config and the packages it points at can be moved or
 * committed together. Absolute locations are returned untouched.
 */
export function resolvePackageLocation(
   location: string,
   anchorDir: string,
   homeDir?: string,
): string {
   let expanded = location;
   if (location.startsWith("~/")) {
      // Lazily: os.homedir() can throw in a distroless container, and a
      // location without `~` must never fail on that.
      const home = homeDir ?? os.homedir();
      if (!home) {
         throw new Error(
            `Cannot expand "~" in location "${location}": home directory is not set`,
         );
      }
      expanded = path.join(home, location.slice(2));
   }
   return path.isAbsolute(expanded) ? expanded : path.join(anchorDir, expanded);
}

/**
 * Options for every package git clone. Shallow, because only the default
 * branch's working tree is served and a reload re-clones, so history is pure
 * download cost.
 */
export const GIT_CLONE_OPTIONS: Record<string, string | number | null> = {
   "--depth": 1,
   "--single-branch": null,
};

/**
 * Drop git progress lines from a failed clone's error message (simple-git
 * forces `--progress` when a progress handler is set), since the message
 * reaches API error responses and logs. Returns the input rather than an
 * empty error when every line matched.
 */
export function stripGitProgressNoise(message: string): string {
   const kept = message
      .split(/\r\n|\r|\n/)
      .filter(
         (line) =>
            !/\d+% \(\d+\/\d+\)/.test(line) &&
            // Also matches the "Error: Cloning into ..." shape V8 seeds
            // into err.stack from the message's first line.
            !/^(\w+: )?Cloning into /.test(line.trim()),
      )
      .join("\n")
      .trim();
   return kept.length > 0 ? kept : message.trim();
}

/** e.g. `[examples] cloning malloydata/publisher (storefront, +2 more)`. */
export function cloneProgressLabel(
   repo: string,
   context?: { environmentName?: string; packageNames?: string[] },
): string {
   const env = context?.environmentName ? `[${context.environmentName}] ` : "";
   const names = context?.packageNames ?? [];
   const shown = names.length > 4 ? names.slice(0, 3) : names;
   const more = names.length - shown.length;
   const pkgs =
      shown.length > 0
         ? ` (${shown.join(", ")}${more > 0 ? `, +${more} more` : ""})`
         : "";
   return `${env}cloning ${repo}${pkgs}`;
}

/**
 * Streams git clone progress to stderr, not winston (whose level can hide it)
 * or stdout (which an MCP stdio transport would own). A TTY gets one line
 * rewritten in place; anything else gets a line per stage and per 25-point
 * step.
 */
export class CloneProgressReporter {
   private lastStage: string | undefined;
   private lastMilestone = -1;
   private wroteInPlace = false;
   private finished = false;
   /**
    * One reporter at a time owns a TTY's in-place line: environments load
    * concurrently at boot, and two reporters `\r`-rewriting the same line
    * would overwrite each other. Later reporters fall back to milestone
    * lines until the owner finishes. Keyed by stream so tests with fake
    * streams cannot collide.
    */
   private static ttyOwners = new WeakMap<object, CloneProgressReporter>();

   constructor(
      private readonly label: string,
      private readonly out: NodeJS.WriteStream = process.stderr,
   ) {}

   onProgress(event: SimpleGitProgressEvent): void {
      if (this.finished) {
         // A late event after done() must not re-claim the TTY line the
         // finishing newline just released.
         return;
      }
      const { stage, progress, processed, total } = event;
      const counts = total > 0 ? ` (${processed}/${total})` : "";
      const text = `${this.label}: ${stage} ${progress}%${counts}`;
      if (this.out.isTTY === true && this.claimTtyLine()) {
         // Clamped to the terminal width, since a wrapped line breaks the \r
         // rewrite; the label is shortened first to keep the percentage
         // visible. \x1b[K clears the tail of a longer previous line.
         const budget = Math.max(1, (this.out.columns || 80) - 1);
         const suffix = `: ${stage} ${progress}%${counts}`;
         let line: string;
         if (text.length <= budget) {
            line = text;
         } else if (suffix.length + 4 <= budget) {
            const labelBudget = budget - suffix.length;
            line = `${this.label.slice(0, labelBudget - 3)}...${suffix}`;
         } else {
            line = text.slice(0, budget);
         }
         this.out.write(`\r${line}\x1b[K`);
         this.wroteInPlace = true;
         return;
      }
      const milestone = Math.floor(progress / 25);
      if (stage !== this.lastStage || milestone < this.lastMilestone) {
         // A new stage, or a percentage restart inside one reported stage:
         // git's server-side counting and compressing phases both arrive as
         // stage "remote:", so a drop in progress marks the next sub-phase.
         this.lastStage = stage;
         this.lastMilestone = milestone;
      } else if (milestone > this.lastMilestone) {
         this.lastMilestone = milestone;
      } else {
         return;
      }
      if (this.out.isTTY === true) {
         // A non-owner on a TTY: the cursor may sit mid-line on the owner's
         // in-place output, so take over the row cleanly and scroll it.
         this.out.write(`\r${text}\x1b[K\n`);
         return;
      }
      this.out.write(`${text}\n`);
   }

   done(): void {
      this.finished = true;
      if (this.wroteInPlace) {
         this.out.write("\n");
      }
      if (CloneProgressReporter.ttyOwners.get(this.out) === this) {
         CloneProgressReporter.ttyOwners.delete(this.out);
      }
   }

   private claimTtyLine(): boolean {
      const owner = CloneProgressReporter.ttyOwners.get(this.out);
      if (owner === undefined) {
         CloneProgressReporter.ttyOwners.set(this.out, this);
         return true;
      }
      return owner === this;
   }
}

/**
 * The last `/segment` of the run of non-empty path segments at the start of
 * `afterRepo`, or undefined (the capture semantics of a `(\/[^/]+)*` group).
 */
function lastGitHubPathSegment(afterRepo: string): string | undefined {
   if (!afterRepo.startsWith("/")) return undefined;
   const run: string[] = [];
   for (const seg of afterRepo.slice(1).split("/")) {
      if (seg === "") break;
      run.push(seg);
   }
   return run.length > 0 ? `/${run[run.length - 1]}` : undefined;
}

/**
 * Parse a GitHub package location into owner, repo, and (optionally) the
 * subdirectory path segment.
 *
 * String ops rather than a `(\/[^/]+)*` regex, which backtracks
 * polynomially (ReDoS) on a user-supplied location. `packagePath` is only the
 * LAST path segment after the repo, a quirk callers rely on;
 * environment_store.spec.ts fuzzes this against the reference regexes.
 */
export function parseGitHubUrl(
   githubUrl: string,
): { owner: string; repoName: string; packagePath?: string } | null {
   // HTTPS: https://github.com/owner/repo/tree/branch/subdir; unanchored.
   const httpsMatch = /github\.com\/([^/]+)\/([^/]+)/.exec(githubUrl);
   if (httpsMatch) {
      const afterRepo = githubUrl.slice(
         httpsMatch.index + httpsMatch[0].length,
      );
      return {
         owner: httpsMatch[1],
         repoName: httpsMatch[2],
         packagePath: lastGitHubPathSegment(afterRepo),
      };
   }

   // SSH: git@github.com:owner/repo(.git)?(/subdir)*, unanchored at the start
   // but $-anchored: a trailing slash or empty segment fails the parse.
   const SSH_PREFIX = "git@github.com:";
   const sshAt = githubUrl.indexOf(SSH_PREFIX);
   if (sshAt !== -1) {
      const rest = githubUrl.slice(sshAt + SSH_PREFIX.length);
      const firstSlash = rest.indexOf("/");
      if (firstSlash > 0) {
         const owner = rest.slice(0, firstSlash);
         const afterOwner = rest.slice(firstSlash + 1);
         const secondSlash = afterOwner.indexOf("/");
         const repoSeg =
            secondSlash === -1 ? afterOwner : afterOwner.slice(0, secondSlash);
         const afterRepo =
            secondSlash === -1 ? "" : afterOwner.slice(secondSlash);
         // A repo segment containing whitespace is not a GitHub URL.
         if (repoSeg.length > 0 && !/\s/.test(repoSeg)) {
            const repoName =
               repoSeg.length > 4 && repoSeg.endsWith(".git")
                  ? repoSeg.slice(0, -4)
                  : repoSeg;
            if (afterRepo === "") {
               return { owner, repoName };
            }
            const segments = afterRepo.slice(1).split("/");
            // $-anchored: every trailing segment must be non-empty.
            if (!segments.includes("")) {
               return {
                  owner,
                  repoName,
                  packagePath: `/${segments[segments.length - 1]}`,
               };
            }
         }
      }
   }

   return null;
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
   const host = displayHost(process.env.PUBLISHER_HOST || "0.0.0.0");
   const port = Number(process.env.PUBLISHER_PORT || 4000);
   const mcpPort = Number(process.env.MCP_PORT || 4040);
   return (
      `PUBLISHER_READY url=http://${host}:${port} ` +
      `mcp=http://${host}:${mcpPort} ` +
      `environments=${counts.environments} packages=${counts.packages} ` +
      `load_errors=${counts.loadErrors}`
   );
}

export class EnvironmentStore {
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
   public publisherConfigIsFrozen: boolean;
   /**
    * Read once: the MCP server registers its tools per request, and must not
    * re-parse the config each time.
    */
   public readonly mcpIncludeHiddenFilesAndSources: boolean;
   public finishedInitialization: Promise<void>;
   private isInitialized: boolean = false;
   public storageManager: StorageManager;
   private s3Client = new S3({
      followRegionRedirects: true,
   });
   private gcsClient: Storage;
   private memoryGovernor: PackageMemoryGovernor | null = null;
   private packageLoadedHook:
      | ((environmentName: string, pkg: Package) => void)
      | null = null;
   // Called when a version is archived. Set once at server start; each
   // Environment picks it up when its version registry is bound.
   private versionArchivedHook:
      | ((
           environmentName: string,
           packageName: string,
           versionId: string,
        ) => void)
      | null = null;
   // Called once a version is deleted; picked up like versionArchivedHook.
   private versionDeletedHook:
      | ((
           environmentName: string,
           packageName: string,
           versionId: string,
        ) => void)
      | null = null;

   /**
    * Environments (from `PUBLISHER_WATCH`) whose package directories under
    * `publisher_data/<env>/` are symlinks to the local sources rather than
    * copies, so source edits live-reload. The default copy keeps source and
    * served package decoupled. Remote locations always copy.
    */
   private inPlaceEnvs = new Set<string>();

   constructor(serverRootPath: string) {
      this.serverRootPath = serverRootPath;
      this.mcpIncludeHiddenFilesAndSources =
         isMcpIncludeHiddenFilesAndSources(serverRootPath);
      this.gcsClient = new Storage();

      const watchEnvList = (process.env.PUBLISHER_WATCH || "")
         .split(",")
         .map((s) => s.trim())
         .filter((s) => s.length > 0);
      for (const envName of watchEnvList) this.inPlaceEnvs.add(envName);

      const storageConfig: StorageConfig = {
         type: "duckdb",
         duckdb: {
            path: path.join(serverRootPath, "publisher.db"),
         },
      };
      this.storageManager = new StorageManager(storageConfig);

      this.finishedInitialization = this.initialize();
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
    * every package at once. Remembered, so Environments created after this
    * call also use it.
    */
   public setPackageLoadedHook(
      hook: ((environmentName: string, pkg: Package) => void) | null,
   ): void {
      this.packageLoadedHook = hook;
      for (const env of this.environments.values()) {
         this.attachPackageLoadedHook(env);
      }
   }

   /**
    * Attach (or detach with `null`) the callback run when a version is
    * archived, which reclaims the tables that version alone owns. Remembered,
    * so Environments bound after this call also use it.
    */
   public setVersionArchivedHook(
      hook:
         | ((
              environmentName: string,
              packageName: string,
              versionId: string,
           ) => void)
         | null,
   ): void {
      this.versionArchivedHook = hook;
      for (const env of this.environments.values()) {
         this.attachVersionArchivedHook(env);
      }
   }

   private attachVersionArchivedHook(env: Environment): void {
      const hook = this.versionArchivedHook;
      env.setVersionArchivedHook(
         hook
            ? (packageName, versionId) =>
                 hook(env.getEnvironmentName(), packageName, versionId)
            : null,
      );
   }

   /**
    * Attach (or detach with `null`) the callback run once a version is
    * deleted, which stops its builds and reclaims the tables that version
    * alone owns, in the background. Remembered, so Environments bound after
    * this call also use it.
    */
   public setVersionDeletedHook(
      hook:
         | ((
              environmentName: string,
              packageName: string,
              versionId: string,
           ) => void)
         | null,
   ): void {
      this.versionDeletedHook = hook;
      for (const env of this.environments.values()) {
         this.attachVersionDeletedHook(env);
      }
   }

   private attachVersionDeletedHook(env: Environment): void {
      const hook = this.versionDeletedHook;
      env.setVersionDeletedHook(
         hook
            ? (packageName, versionId) =>
                 hook(env.getEnvironmentName(), packageName, versionId)
            : null,
      );
   }

   private attachPackageLoadedHook(env: Environment): void {
      const hook = this.packageLoadedHook;
      env.setPackageLoadedHook(
         hook ? (pkg) => hook(env.getEnvironmentName(), pkg) : null,
      );
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
               resource: `${API_PREFIX}/environments/${environment.name}`,
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
    * declares (see addConfiguredEnvironment). One that cannot be published
    * (no version, a refused check, a conflict) is recorded as that package's
    * load error, as a configured package that does not mount is, and the
    * others still load.
    */
   private async publishConfiguredPackages(
      environment: Environment,
      packages: ProcessedEnvironment["packages"],
   ): Promise<void> {
      const environmentName = environment.getEnvironmentName();
      const versions = environment.getVersionService();
      for (const configured of packages) {
         try {
            if (!versions) {
               throw new Error(
                  "package versions are not available in this environment",
               );
            }
            await versions.publish(
               configured.name,
               (stagingPath) =>
                  this.downloadPackageInto(
                     environmentName,
                     configured.name,
                     configured.location,
                     stagingPath,
                  ),
               {
                  sourceLocation: configured.location,
                  promotion: getVersionPromotionMode(this.serverRootPath),
                  validate: (loaded) => formatPublishRejections(loaded),
               },
            );
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
                     resource: `${API_PREFIX}/environments/${environmentConfig.name}`,
                     connections: environmentConfig.connections,
                     storageDestinations: environmentConfig.storageDestinations,
                     packages: environmentConfig.packages,
                  },
                  true,
                  // The config is only a fallback for a missing directory:
                  // its empty destination list must not prune destinations
                  // registered over the API.
                  { storageDestinationsAreFallback: true },
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
         this.attachPackageLoadedHook(environmentInstance);
         // Its version rules, with the storage-binding resolver every package
         // here restores its serve bindings through.
         await this.bindVersionRegistry(environmentInstance, dbEnvironment.id);
         // Reloaded from the database: its unversioned packages restore their
         // serve bindings from the local store on load, so serving survives a
         // restart. The only path that did so before versions existed;
         // environments created at runtime still do not.
         environmentInstance.restoreUnversionedBindingsFromLocalStore();

         const packages = await repository.listPackages(dbEnvironment.id);
         packages.forEach((pkg) => {
            environmentInstance.setPackageStatus(
               pkg.name,
               PackageStatus.SERVING,
            );
         });

         this.environments.set(dbEnvironment.name, environmentInstance);

         return environmentInstance.listPackages();
      } catch (error) {
         this.recordEnvironmentInitializationFailure(dbEnvironment.name, error);
      }
   }

   private async initialize() {
      const reInit = process.env.INITIALIZE_STORAGE === "true";
      const initialTime = performance.now();

      try {
         await this.storageManager.initialize(reInit);

         // Read both versioning settings now, so a value outside the set fails
         // initialization naming the setting, rather than surfacing at the first
         // publish that reads it.
         isVersioningEnabled();
         getVersionPromotionMode(this.serverRootPath);

         this.publisherConfigIsFrozen = isPublisherConfigFrozen(
            this.serverRootPath,
         );

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
      this.attachVersionArchivedHook(environment);
      this.attachVersionDeletedHook(environment);
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
      const isCompressedFile = packageLocation.endsWith(".zip");
      if (
         packageLocation.startsWith("https://") ||
         packageLocation.startsWith("git@")
      ) {
         await this.downloadGitHubDirectory(packageLocation, targetPath);
      } else if (packageLocation.startsWith("gs://")) {
         await this.downloadGcsDirectory(
            packageLocation,
            environmentName,
            targetPath,
            isCompressedFile,
         );
      } else if (packageLocation.startsWith("s3://")) {
         await this.downloadS3Directory(
            packageLocation,
            environmentName,
            targetPath,
            isCompressedFile,
         );
      }

      if (packageLocation.startsWith("/") || path.isAbsolute(packageLocation)) {
         // Absolute paths from the publisher.config could be placed outside of
         // /etc/publisher, so they are mounted in the right place.
         // `path.isAbsolute` is what catches a Windows drive-letter path
         // (`D:\pkgs\sales`), which no other branch claims either; without it
         // the install stages nothing and fails with a bare ENOENT rename.
         // Same pairing as isLocalPath.
         await this.mountLocalDirectory(
            packageLocation,
            targetPath,
            environmentName,
            packageName,
         );
      }
   }

   public async addEnvironmentToDatabase(
      environment: Environment,
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

      await this.addConnections(environment, dbEnvironment.id, repository);
      await this.syncStorageDestinations(
         environment,
         dbEnvironment.id,
         repository,
      );
      await this.addPackages(environment, dbEnvironment.id, repository);

      logger.info(`Synced environment "${environmentName}" to database`);
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

   private async addConnections(
      environment: Environment,
      environmentId: string,
      repository: ReturnType<typeof this.storageManager.getRepository>,
   ): Promise<void> {
      try {
         const connections = environment.listApiConnections();
         for (const conn of connections) {
            if (!conn.name) {
               logger.warn("Skipping connection with undefined name");
               continue;
            }

            const existingConn = await repository.getConnectionByName(
               environmentId,
               conn.name,
            );

            if (existingConn) {
               await this.updateConnection(conn, environmentId, repository);
            } else {
               await this.addConnection(conn, environmentId, repository);
            }
         }
      } catch (err: unknown) {
         const error = err as Error;
         const environmentName = environment.metadata?.name;
         logger.error(
            `Error syncing connections for "${environmentName}":`,
            error,
         );
      }
   }

   /**
    * Mirror an environment's destination list into the database so it
    * survives a restart. Unlike {@link addConnections} it also deletes rows the
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
      conn: ReturnType<Environment["listApiConnections"]>[number],
      environmentId: string,
      repository: ReturnType<typeof this.storageManager.getRepository>,
   ): Promise<void> {
      if (!conn.name) {
         logger.warn("Skipping connection with undefined name");
         return;
      }

      const connectionData = {
         environmentId,
         name: conn.name,
         type: conn.type as Connection["type"],
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
      conn: ReturnType<Environment["listApiConnections"]>[number],
      environmentId: string,
      repository: ReturnType<typeof this.storageManager.getRepository>,
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
         type: conn.type as Connection["type"],
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
      const environment = await this.getEnvironment(environmentName, false);
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
    * Best-effort, non-blocking: drop a deleted package's cached embeddings so
    * churn does not grow publisher.db forever. Not awaited: it queues behind
    * the package's sync mutex, which a bulk embed can hold for minutes. An
    * orphaned row is inert, so failure is logged, never propagated.
    */
   public cleanupPackageEmbeddings(
      environmentName: string,
      packageName: string,
   ): void {
      try {
         deletePackageEmbeddings(
            this.storageManager.getDuckDbConnection(),
            environmentName,
            packageName,
         ).catch((error: unknown) => {
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
      try {
         deleteEnvironmentEmbeddings(
            this.storageManager.getDuckDbConnection(),
            environmentName,
         ).catch((error: unknown) => {
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
      const reInit = process.env.INITIALIZE_STORAGE === "true";

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
      options: { includeLoading?: boolean } = {},
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

   public async getStatus(options: { includeLoading?: boolean } = {}) {
      // Back-pressure reads as "throttled" so the control plane can stop
      // routing to this worker; "draining" takes precedence.
      const baseState = getOperationalState();
      const operationalState = (
         baseState !== "draining" &&
         (this.memoryGovernor?.isBackpressured() ?? false)
            ? "throttled"
            : baseState
      ) as components["schemas"]["ServerStatus"]["operationalState"];

      const status: {
         timestamp: number;
         environments: Array<components["schemas"]["Environment"]>;
         initialized: boolean;
         frozenConfig: boolean;
         operationalState: components["schemas"]["ServerStatus"]["operationalState"];
         version: string;
         packageVersioning?: components["schemas"]["ServerStatus"]["packageVersioning"];
         versionPromotion?: components["schemas"]["ServerStatus"]["versionPromotion"];
         emptyReason?: string;
         initError?: string;
         loadErrors?: LoadError[];
      } = {
         timestamp: Date.now(),
         environments: [],
         initialized: this.isInitialized,
         frozenConfig: isPublisherConfigFrozen(this.serverRootPath),
         operationalState,
         version: SERVER_VERSION,
      };
      // Whether a publish from a location publishes versions, so an
      // orchestrator mid-rollout can tell. A server from before versions sends
      // no field, which means "off".
      status.packageVersioning = versioningEnabledForStatus() ? "on" : "off";
      // A malformed value failed initialization, which this reports.
      try {
         status.versionPromotion = getVersionPromotionMode(this.serverRootPath);
      } catch {
         // Left absent.
      }

      const environments = await this.listEnvironments(true, options);

      await Promise.all(
         environments.map(async (environment) => {
            try {
               // A versioned package is listed once per version it holds (each
               // with its own versionId), not once as its latest.
               const versionEntries =
                  (await this.environments
                     .get(environment.name ?? "")
                     ?.listVersionEntries()) ?? new Map();
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
      environmentName: string,
      reload: boolean = false,
   ): Promise<Environment> {
      await this.finishedInitialization;
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
         return await this.addEnvironment({
            name: environmentName,
            resource: `${API_PREFIX}/environments/${environmentName}`,
            connections: environmentConfig?.connections || [],
            // Only asserted when the config declares destinations: an empty
            // list reads as "clear them", wiping API-registered ones on reload.
            ...(environmentConfig?.storageDestinations?.length
               ? {
                    storageDestinations: environmentConfig.storageDestinations,
                 }
               : {}),
         });
      });
   }

   /**
    * `storageDestinationsAreFallback` marks the payload's destinations as a best
    * guess, so the database sync must not prune against them. It is a parameter
    * because this method syncs before it returns.
    */
   public async addEnvironment(
      environment: ApiEnvironment,
      skipInitialization: boolean = false,
      {
         storageDestinationsAreFallback = false,
         // The config's packages are published as versions once the
         // environment exists (see addConfiguredEnvironment), not loaded here.
         configuredPackagesPublished = false,
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
         await this.addEnvironmentToDatabase(updatedEnvironment);
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
      this.attachPackageLoadedHook(newEnvironment);

      if (!newEnvironment.metadata) newEnvironment.metadata = {};
      newEnvironment.metadata.location = absoluteEnvironmentPath;

      // Attach before the SERVING seeding below: those packages are configured
      // but un-mounted, and this is the only record of why.
      for (const [packageName, message] of mountErrors) {
         newEnvironment.setPackageMountError(packageName, message);
      }

      this.environments.set(environmentName, newEnvironment);
      // It loaded, so any earlier failure for this name is stale.
      this.failedEnvironments.delete(environmentName);

      environment?.packages?.forEach((_package) => {
         if (_package.name) {
            newEnvironment.setPackageStatus(
               _package.name,
               PackageStatus.SERVING,
            );
         }
      });

      if (storageDestinationsAreFallback) {
         newEnvironment.markStorageDestinationsUnknown();
      }
      await this.addEnvironmentToDatabase(newEnvironment);

      return newEnvironment;
   }

   public async unzipEnvironment(absoluteEnvironmentPath: string) {
      assertSafeEnvironmentPath(absoluteEnvironmentPath);
      // Extracts to a sibling directory named for the archive. The check is in
      // the one shape CodeQL's js/path-injection accepts as a barrier (an
      // unconditional startsWith on the resolved value it later sinks).
      const archiveDir = path.resolve(path.dirname(absoluteEnvironmentPath));
      const unzippedEnvironmentPath = path.resolve(
         archiveDir,
         path.basename(absoluteEnvironmentPath, ".zip"),
      );
      if (!unzippedEnvironmentPath.startsWith(archiveDir + path.sep)) {
         throw new BadRequestError(
            `Refusing to unzip "${absoluteEnvironmentPath}": target escapes its directory`,
         );
      }
      await this.extractZipInto(
         absoluteEnvironmentPath,
         unzippedEnvironmentPath,
      );
      return unzippedEnvironmentPath;
   }

   /**
    * Extract `archivePath` into `targetDir`, replacing whatever is there.
    *
    * `targetDir` is removed first, so it must be a directory the server owns.
    * Never pass a path derived from an operator-supplied location: a sibling of
    * an operator's zip is the operator's directory, not ours.
    */
   private async extractZipInto(archivePath: string, targetDir: string) {
      const startedAt = Date.now();
      logger.info(`Detected zip file at "${archivePath}". Unzipping...`);
      await fs.promises.rm(targetDir, {
         recursive: true,
         force: true,
      });
      await fs.promises.mkdir(targetDir, { recursive: true });

      // Stream-extract off the main thread (yauzl via extract-zip): a
      // synchronous inflate of a large archive blocks the event loop long
      // enough to fail Kubernetes liveness probes.
      let entryCount = 0;
      let totalUncompressedBytes = 0;
      try {
         await extract(archivePath, {
            dir: path.resolve(targetDir),
            onEntry: (entry) => {
               // extract-zip 2.0.1 never validates a symlink entry's target, so a
               // crafted archive can write outside `dir` (CVE-2026-19693,
               // CVE-2026-56876, no upstream fix). Throwing here cancels the extract.
               if (
                  ((entry.externalFileAttributes >>> 16) & 0o170000) ===
                  0o120000
               ) {
                  throw new BadRequestError(
                     `Refusing to unzip "${archivePath}": entry "${entry.fileName}" is a symbolic link`,
                  );
               }
               entryCount += 1;
               totalUncompressedBytes += entry.uncompressedSize ?? 0;
            },
         });
      } catch (error) {
         // Leave no partial extract behind for a later load to pick up.
         await fs.promises.rm(targetDir, {
            recursive: true,
            force: true,
         });
         throw error;
      }

      const mib = (totalUncompressedBytes / (1024 * 1024)).toFixed(1);
      logger.info(
         `Unzipped "${archivePath}" -> "${targetDir}" ` +
            `(${entryCount} entries, ${mib} MiB uncompressed) in ` +
            `${formatDuration(Date.now() - startedAt)}`,
      );
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
               return { frozenConfig: false, environments };
            } catch (lsError) {
               // An Error's fields are not enumerable; `{ error }` logs {}.
               logger.error(`Error listing directories in ${serverRootPath}`, {
                  error:
                     lsError instanceof Error
                        ? lsError.message
                        : String(lsError),
               });
               return { frozenConfig: false, environments: [] };
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

   private isLocalPath(location: string) {
      return (
         location.startsWith("./") ||
         location.startsWith("../") ||
         location.startsWith("~/") ||
         location.startsWith("/") ||
         path.isAbsolute(location)
      );
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

   private isGitHubURL(location: string) {
      return (
         location.startsWith("https://github.com/") ||
         location.startsWith("git@github.com:")
      );
   }

   private isGCSURL(location: string) {
      return location.startsWith("gs://");
   }

   private isS3URL(location: string) {
      return location.startsWith("s3://");
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

   private async loadEnvironmentIntoDisk(
      environmentName: string,
      packages: ApiEnvironment["packages"],
   ) {
      assertSafePackageName(environmentName);
      const absoluteTargetPath = safeJoinUnderRoot(
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
         if (this.isGitHubURL(_package.location)) {
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
                  this.isLocalPath(_package.location) &&
                  _package.location.endsWith(".zip");
               let sourcePath: string;
               if (this.isGitHubURL(_package.location)) {
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
                  } else if (this.isLocalPath(_package.location)) {
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
                     this.isLocalPath(_package.location) &&
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
                        !this.isLocalPath(_package.location)
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
      const loader = this.locationLoader(
         location,
         targetPath,
         environmentName,
         packageName,
         packageNames,
      );
      if (!loader) {
         const errorMsg = `Invalid package path: "${location}". Must be an absolute mounted path or a GCS/S3/GitHub URI.`;
         logger.error(errorMsg, { environmentName, location });
         throw new PackageNotFoundError(errorMsg);
      }
      try {
         logger.info(loader.starting);
         await loader.load();
      } catch (error) {
         const errorData = extractErrorDataFromError(error);
         logger.error(
            `Failed to ${loader.action} "${loader.subject}"`,
            errorData,
         );
         throw new PackageNotFoundError(
            `Failed to ${loader.action}: ${loader.subject}`,
            { cause: error },
         );
      }
   }

   /**
    * How to bring `location` to `targetPath`, chosen by its scheme: GCS,
    * GitHub, S3, then an absolute or relative local path. Undefined for any
    * other form.
    */
   private locationLoader(
      location: string,
      targetPath: string,
      environmentName: string,
      packageName: string,
      packageNames?: string[],
   ):
      | {
           /** What failed, for the log line and the 404. */
           action: string;
           subject: string;
           starting: string;
           load: () => Promise<void>;
        }
      | undefined {
      const isCompressedFile = location.endsWith(".zip");
      if (this.isGCSURL(location)) {
         return {
            action: "download GCS directory",
            subject: location,
            starting: `Downloading GCS directory from "${location}" to "${targetPath}"`,
            load: () =>
               this.downloadGcsDirectory(
                  location,
                  environmentName,
                  targetPath,
                  isCompressedFile,
               ),
         };
      }

      if (this.isGitHubURL(location)) {
         return {
            action: "clone GitHub repository",
            subject: location,
            starting: `Cloning GitHub repository from "${location}" to "${targetPath}"`,
            load: () =>
               this.downloadGitHubDirectory(location, targetPath, {
                  environmentName,
                  packageNames,
               }),
         };
      }

      if (this.isS3URL(location)) {
         return {
            action: "download S3 directory",
            subject: location,
            starting: `Downloading S3 directory from "${location}" to "${targetPath}"`,
            load: () =>
               this.downloadS3Directory(
                  location,
                  environmentName,
                  targetPath,
                  isCompressedFile,
               ),
         };
      }

      if (this.isLocalPath(location)) {
         const packagePath: string = this.resolveLocalPath(location);
         return {
            action: "mount local directory",
            subject: packagePath,
            starting: `Mounting local directory at "${packagePath}" to "${targetPath}"`,
            load: () =>
               this.mountLocalDirectory(
                  packagePath,
                  targetPath,
                  environmentName,
                  packageName,
               ),
         };
      }

      return undefined;
   }

   public async mountLocalDirectory(
      environmentPath: string,
      absoluteTargetPath: string,
      environmentName: string,
      packageName: string,
   ) {
      // `environmentPath` is the operator-supplied mount source and may
      // legitimately be a relative path that resolves outside the
      // server root; only the target is asserted.
      assertSafeEnvironmentPath(absoluteTargetPath);
      const notFound = () =>
         new PackageNotFoundError(
            `Package ${packageName} for environment ${environmentName} not found in "${environmentPath}"`,
         );
      const source = await fs.promises.stat(environmentPath).catch((error) => {
         if ((error as NodeJS.ErrnoException).code === "ENOENT") {
            throw notFound();
         }
         throw error;
      });
      if (environmentPath.endsWith(".zip") && source.isFile()) {
         // Extract straight into the server-owned target: nothing is written
         // beside the archive, so a read-only mount works.
         await this.extractZipInto(environmentPath, absoluteTargetPath);
      } else if (source.isDirectory()) {
         await fs.promises.rm(absoluteTargetPath, {
            recursive: true,
            force: true,
         });
         await fs.promises.mkdir(absoluteTargetPath, { recursive: true });
         await fs.promises.cp(environmentPath, absoluteTargetPath, {
            recursive: true,
         });
      } else {
         throw notFound();
      }
   }

   async downloadGcsDirectory(
      gcsPath: string,
      environmentName: string,
      absoluteDirPath: string,
      isCompressedFile: boolean,
   ) {
      assertSafeEnvironmentPath(absoluteDirPath);
      assertGoogleCredentialsIsNotADirectory();
      const trimmedPath = gcsPath.slice(5);
      const [bucketName, ...prefixParts] = trimmedPath.split("/");
      const prefix = prefixParts.join("/");
      const [files] = await this.gcsClient.bucket(bucketName).getFiles({
         prefix,
      });
      if (files.length === 0) {
         throw new EnvironmentNotFoundError(
            `Environment ${environmentName} not found in ${gcsPath}`,
         );
      }
      if (!isCompressedFile) {
         await fs.promises.rm(absoluteDirPath, {
            recursive: true,
            force: true,
         });
         await fs.promises.mkdir(absoluteDirPath, { recursive: true });
      } else {
         absoluteDirPath = `${absoluteDirPath}.zip`;
      }
      await Promise.all(
         files.map(async (file) => {
            // Strip a leading `/` (prefix without trailing slash), which
            // `safeJoinUnderRoot` would reject as absolute.
            const relativeFilePath = file.name
               .replace(prefix, "")
               .replace(/^\/+/, "");
            const absoluteFilePath = isCompressedFile
               ? absoluteDirPath
               : safeJoinUnderRoot(absoluteDirPath, relativeFilePath);
            if (file.name.endsWith("/")) {
               return;
            }
            await fs.promises.mkdir(path.dirname(absoluteFilePath), {
               recursive: true,
            });
            return fs.promises.writeFile(
               absoluteFilePath,
               await file.download(),
            );
         }),
      );
      if (isCompressedFile) {
         await this.unzipEnvironment(absoluteDirPath);
      }
      logger.info(`Downloaded GCS directory ${gcsPath} to ${absoluteDirPath}`);
   }

   async downloadS3Directory(
      s3Path: string,
      environmentName: string,
      absoluteDirPath: string,
      isCompressedFile: boolean = false,
   ) {
      assertSafeEnvironmentPath(absoluteDirPath);
      const trimmedPath = s3Path.slice(5);
      const [bucketName, ...prefixParts] = trimmedPath.split("/");
      const prefix = prefixParts.join("/");

      if (isCompressedFile) {
         const zipFilePath = `${absoluteDirPath}.zip`;
         await fs.promises.mkdir(path.dirname(zipFilePath), {
            recursive: true,
         });

         const command = new GetObjectCommand({
            Bucket: bucketName,
            Key: prefix,
         });
         const item = await this.s3Client.send(command);
         if (!item.Body) {
            throw new EnvironmentNotFoundError(
               `Environment ${environmentName} not found in ${s3Path}`,
            );
         }
         const file = fs.createWriteStream(zipFilePath);
         item.Body.transformToWebStream().pipeTo(Writable.toWeb(file));
         await new Promise<void>((resolve, reject) => {
            file.on("error", reject);
            file.on("finish", resolve);
         });

         await this.unzipEnvironment(zipFilePath);
         logger.info(`Downloaded S3 zip file ${s3Path} to ${absoluteDirPath}`);
         return;
      }

      const objects = await this.s3Client.listObjectsV2({
         Bucket: bucketName,
         Prefix: prefix,
      });
      await fs.promises.rm(absoluteDirPath, { recursive: true, force: true });
      await fs.promises.mkdir(absoluteDirPath, { recursive: true });

      if (!objects.Contents || objects.Contents.length === 0) {
         throw new EnvironmentNotFoundError(
            `Environment ${environmentName} not found in ${s3Path}`,
         );
      }
      await Promise.all(
         objects.Contents?.map(async (object) => {
            const key = object.Key;
            if (!key) {
               return;
            }
            // Strip a leading `/`, as for GCS above.
            const relativeFilePath = key
               .replace(prefix, "")
               .replace(/^\/+/, "");
            if (!relativeFilePath || relativeFilePath.endsWith("/")) {
               return;
            }
            const absoluteFilePath = safeJoinUnderRoot(
               absoluteDirPath,
               relativeFilePath,
            );
            await fs.promises.mkdir(path.dirname(absoluteFilePath), {
               recursive: true,
            });
            const command = new GetObjectCommand({
               Bucket: bucketName,
               Key: key,
            });
            const item = await this.s3Client.send(command);
            if (!item.Body) {
               return;
            }
            const file = fs.createWriteStream(absoluteFilePath);
            item.Body.transformToWebStream().pipeTo(Writable.toWeb(file));
            await new Promise<void>((resolve, reject) => {
               file.on("error", reject);
               file.on("finish", resolve);
            });
         }),
      );
      logger.info(`Downloaded S3 directory ${s3Path} to ${absoluteDirPath}`);
   }

   async downloadGitHubDirectory(
      githubUrl: string,
      absoluteDirPath: string,
      progressContext?: { environmentName?: string; packageNames?: string[] },
   ) {
      assertSafeEnvironmentPath(absoluteDirPath);
      // Clone the whole repo, then hoist the package subdirectory (if any).
      const githubInfo = parseGitHubUrl(githubUrl);
      if (!githubInfo) {
         throw new Error(`Invalid GitHub URL: ${githubUrl}`);
      }
      const { owner, repoName, packagePath } = githubInfo;
      // `packagePath` is captured with a leading `/`; strip it so the
      // value is a relative segment usable with `safeJoinUnderRoot`.
      const cleanPackagePath = (
         packagePath?.replace("/tree/main", "") || ""
      ).replace(/^\/+/, "");

      await fs.promises.rm(absoluteDirPath, {
         recursive: true,
         force: true,
      });
      await fs.promises.mkdir(absoluteDirPath, { recursive: true });
      const repoUrl = `https://github.com/${owner}/${repoName}`;
      const reporter = new CloneProgressReporter(
         cloneProgressLabel(`${owner}/${repoName}`, progressContext),
      );

      await new Promise<void>((resolve, reject) => {
         simpleGit({
            progress: (event) => reporter.onProgress(event),
         }).clone(repoUrl, absoluteDirPath, GIT_CLONE_OPTIONS, (err) => {
            reporter.done();
            if (err) {
               err.message = stripGitProgressNoise(err.message);
               if (err.stack) {
                  // V8 captures the message into the stack at construction,
                  // so the spew survives there unless stripped too.
                  err.stack = stripGitProgressNoise(err.stack);
               }
               const errorData = extractErrorDataFromError(err);
               logger.error(
                  `Failed to clone GitHub repository "${repoUrl}"`,
                  errorData,
               );
               reject(err);
               return;
            }
            resolve();
         });
      });

      if (!cleanPackagePath) {
         logger.info(
            `Successfully cloned entire repository to: ${absoluteDirPath}`,
         );
         return;
      }

      // Replace the clone with the subdirectory's contents, e.g.
      // <dir>/imdb/publisher.json becomes <dir>/publisher.json.
      const packageFullPath = safeJoinUnderRoot(
         absoluteDirPath,
         cleanPackagePath,
      );

      const packageExists = await fs.promises
         .access(packageFullPath)
         .then(() => true)
         .catch(() => false);

      if (!packageExists) {
         throw new Error(
            `Package path "${cleanPackagePath}" does not exist in the cloned repository.`,
         );
      }

      const dirContents = await fs.promises.readdir(absoluteDirPath);
      for (const entry of dirContents) {
         if (entry !== cleanPackagePath.replace(/^\/+/, "").split("/")[0]) {
            await fs.promises.rm(safeJoinUnderRoot(absoluteDirPath, entry), {
               recursive: true,
               force: true,
            });
         }
      }

      const packageContents = await fs.promises.readdir(packageFullPath);
      for (const entry of packageContents) {
         await fs.promises.rename(
            safeJoinUnderRoot(packageFullPath, entry),
            safeJoinUnderRoot(absoluteDirPath, entry),
         );
      }

      await fs.promises.rm(packageFullPath, { recursive: true, force: true });
   }
}

/**
 * The versioning flag for /status, which must answer even when the flag's
 * value is malformed: that failed initialization, and /status is where the
 * failure is reported.
 */
function versioningEnabledForStatus(): boolean {
   try {
      return isVersioningEnabled();
   } catch {
      return false;
   }
}
