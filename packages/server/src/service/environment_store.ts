import { GetObjectCommand, S3 } from "@aws-sdk/client-s3";
import { Storage } from "@google-cloud/storage";
import { Mutex } from "async-mutex";
import crypto from "crypto";
import extract from "extract-zip";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import simpleGit, { type SimpleGitProgressEvent } from "simple-git";
import { Writable } from "stream";
import { components } from "../api";
import {
   getProcessedPublisherConfig,
   getPublisherConfigDir,
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
   PackageNotFoundError,
} from "../errors";
import { getOperationalState, markNotReady, markReady } from "../health";
import { formatDuration, logger } from "../logger";
import { redactPgSecrets } from "../pg_helpers";
import {
   assertSafeEnvironmentPath,
   assertSafePackageName,
   safeJoinUnderRoot,
} from "../path_safety";
import { Connection } from "../storage/DatabaseInterface";
import { StorageConfig, StorageManager } from "../storage/StorageManager";
import { Environment, PackageStatus } from "./environment";
import type { PackageMemoryGovernor } from "./package_memory_governor";
type ApiEnvironment = components["schemas"]["Environment"];
type LoadError = NonNullable<
   components["schemas"]["ServerStatus"]["loadErrors"]
>[number];

const AZURE_SUPPORTED_SCHEMES = ["https://", "http://", "abfss://", "az://"];
const AZURE_DATA_EXTENSIONS = [
   ".parquet",
   ".csv",
   ".json",
   ".jsonl",
   ".ndjson",
];

function validateAzureUrl(url: string, fieldName: string): void {
   if (!AZURE_SUPPORTED_SCHEMES.some((s) => url.startsWith(s))) {
      throw new BadRequestError(
         `Azure ${fieldName} must use one of: ${AZURE_SUPPORTED_SCHEMES.join(", ")}`,
      );
   }
   const pathWithoutQuery = url.split("?")[0];
   const stars = (pathWithoutQuery.match(/\*/g) || []).length;

   if (stars === 0) {
      const lower = pathWithoutQuery.toLowerCase();
      if (!AZURE_DATA_EXTENSIONS.some((ext) => lower.endsWith(ext))) {
         throw new BadRequestError(
            `Azure ${fieldName}: a single-file URL must end with a data file extension (${AZURE_DATA_EXTENSIONS.join(", ")})`,
         );
      }
   } else if (pathWithoutQuery.endsWith("**")) {
      // recursive — valid
      // includes all data files in the container and all subdirectories
   } else {
      const lastSegment = pathWithoutQuery.split("/").pop() || "";
      if (stars !== 1 || !lastSegment.startsWith("*")) {
         throw new BadRequestError(
            `Azure ${fieldName}: only three URL patterns are supported:\n` +
               `  • Single file:    path/file.parquet\n` +
               `  • Directory glob: path/*.ext  (direct children only)\n` +
               `  • Recursive:      path/**     (includes all data files in the container and all subdirectories)\n` +
               `Multi-level globs such as "sub_dir/*/*.parquet" are not supported.`,
         );
      }
   }
}

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

// Safely clear a prior mount target before (re)mounting at `targetPath`.
// Distinguishes symlinks from real dirs: fs.rm's recursive mode could otherwise
// traverse into a symlinked source and damage it, so unlink symlinks and rm real
// dirs. Both the in-place (symlink) and copy mount paths call this, so toggling
// watch mode across runs against the same publisher_data can't leave a stale
// entry that breaks the next mount (mkdir is a no-op on a symlink and fs.cp then
// throws ERR_FS_CP_DIR_TO_NON_DIR).
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
      // Resolved lazily, inside the tilde branch only: os.homedir() can throw
      // where HOME is unset and there is no passwd entry for the uid (e.g. a
      // distroless container with runAsUser), and a `./sales` or absolute
      // location must never fail on that.
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
 * Options for every package git clone. Packages are served from the default
 * branch's working tree only (no ref is ever checked out afterwards, and no
 * later git operation touches the clone: a reload deletes and re-clones), so
 * history is pure download cost. Exported for tests.
 */
export const GIT_CLONE_OPTIONS: Record<string, string | number | null> = {
   "--depth": 1,
   "--single-branch": null,
};

/**
 * With a progress handler configured, simple-git forces `--progress`, and a
 * failed clone's error message then carries every progress line git wrote to
 * stderr. Those messages surface in API error responses on the add/update
 * package path and in the failure logs (the boot path's `/status` loadErrors
 * carries a location-level summary instead), so drop the noise and keep the
 * lines that say what went wrong. Returns the input when everything matched,
 * rather than an empty error.
 */
export function stripGitProgressNoise(message: string): string {
   const kept = message
      .split(/\r\n|\r|\n/)
      .filter(
         (line) =>
            !/\d+% \(\d+\/\d+\)/.test(line) &&
            !/^Cloning into /.test(line.trim()),
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
 * Streams git clone progress to stderr: off winston (whose format changes
 * under OTEL and whose level can hide info lines) and off stdout (which an
 * MCP stdio transport would own), and it is where git itself reports
 * progress. A TTY gets one line rewritten in place; anything else (CI logs)
 * gets a line per stage and per 25-point step, so a slow clone shows life
 * without flooding. Exported for tests.
 */
export class CloneProgressReporter {
   private lastStage: string | undefined;
   private lastMilestone = -1;
   private wroteInPlace = false;
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
      const { stage, progress, processed, total } = event;
      const counts = total > 0 ? ` (${processed}/${total})` : "";
      const text = `${this.label}: ${stage} ${progress}%${counts}`;
      if (this.out.isTTY === true && this.claimTtyLine()) {
         // Clamped to the terminal width: an auto-wrapped line breaks the
         // \r rewrite (git clamps its own progress output for the same
         // reason). The moving part sits at the end, so shorten the label
         // and keep the stage/percentage visible; only when even the
         // suffix cannot fit does a plain head slice apply. \x1b[K clears
         // the tail of a longer previous line.
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
      this.out.write(`${text}\n`);
   }

   done(): void {
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

/** `0.0.0.0`/`::` bind every interface; a script needs a host it can dial. */
function displayHost(host: string): string {
   if (host === "0.0.0.0" || host === "::") {
      return "localhost";
   }
   // An IPv6 literal needs brackets to be dialable inside a URL.
   return host.includes(":") ? `[${host}]` : host;
}

/**
 * The one-line machine-readable boot signal. Scripts grep for the
 * `PUBLISHER_READY` prefix instead of polling `/api/v0/status`, and the
 * counts carry what `operationalState: "serving"` alone does not say: how
 * much of the configured world actually loaded. Host and ports mirror
 * server.ts's use-site defaults; parseArgs writes the flag values into
 * process.env before the store is constructed. Exported for tests.
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
    * Environments that were configured but did not load, keyed by name.
    *
    * A load failure is not fatal: the environment is skipped and the server
    * still reports `serving`. Nothing else remembers it, because the throw
    * happens before the environment reaches `this.environments` or the
    * database, so this is the only record that a configured environment is
    * missing rather than never asked for. Surfaced on `getStatus`.
    */
   private failedEnvironments = new Map<string, string>();
   private environmentMutexes = new Map<string, Mutex>();
   public publisherConfigIsFrozen: boolean;
   public finishedInitialization: Promise<void>;
   private isInitialized: boolean = false;
   public storageManager: StorageManager;
   private s3Client = new S3({
      followRegionRedirects: true,
   });
   private gcsClient: Storage;
   // Shared by every Environment so the back-pressure decision is
   // process-wide. Set once at server start via setMemoryGovernor;
   // new Environments pick it up at construction.
   private memoryGovernor: PackageMemoryGovernor | null = null;

   /**
    * Set of environment names that should be loaded "in place" — i.e. the
    * package directories under `publisher_data/<envName>/` are symlinks to
    * the original local source directories instead of recursive copies.
    *
    * In-place mode powers the `npm run dev`-style live-reload story: edits
    * to your source repo are visible to Publisher and trigger watch events
    * that fan out via SSE to embedded HTML pages. Production mode (the
    * default) keeps the safe copy semantics so the source repo and the
    * served package are decoupled.
    *
    * Populated from the `PUBLISHER_WATCH` env var (comma-separated env
    * names) at construction; can also be augmented via `markInPlace()` if
    * a future feature wants to flip an environment to dev mode at runtime.
    *
    * Only LOCAL-DIR package locations are eligible for symlinking; remote
    * sources (GitHub, GCS, S3) always copy regardless.
    */
   private inPlaceEnvs = new Set<string>();

   constructor(serverRootPath: string) {
      this.serverRootPath = serverRootPath;
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
    * Attach (or detach with `null`) the shared {@link PackageMemoryGovernor}.
    * Propagated to every Environment so the back-pressure decision is
    * process-wide, and remembered so any Environment created *after*
    * this call also picks it up at construction.
    */
   public setMemoryGovernor(governor: PackageMemoryGovernor | null): void {
      this.memoryGovernor = governor;
      for (const env of this.environments.values()) {
         env.setMemoryGovernor(governor);
      }
   }

   /**
    * Snapshot of the environments currently held in memory. Used by the
    * standalone materialization scheduler to sweep loaded packages without
    * touching the database or triggering loads.
    */
   public getLoadedEnvironments(): Environment[] {
      return [...this.environments.values()];
   }

   private async addConfiguredEnvironment(environment: ProcessedEnvironment) {
      try {
         await this.addEnvironment(
            {
               name: environment.name,
               resource: `${API_PREFIX}/environments/${environment.name}`,
               connections: environment.connections,
               packages: environment.packages,
            },
            true,
         );
      } catch (error) {
         this.recordEnvironmentInitializationFailure(environment.name, error);
      }
   }

   /**
    * Log a skipped environment AND remember why, for getStatus to report.
    *
    * Both boot paths funnel through here: the config path
    * ({@link addConfiguredEnvironment}) and the database-restore path taken by
    * every restart against an existing publisher.db. Keeping the record here
    * rather than in the callers is deliberate: recording it in only one of them
    * would leave the other silently dropping failures, which is the bug this is
    * meant to fix.
    */
   private recordEnvironmentInitializationFailure(
      environmentName: string | undefined,
      error: unknown,
   ) {
      const label = environmentName ? ` "${environmentName}"` : "";
      logger.error(
         `Error initializing environment${label}; skipping environment`,
         this.extractErrorDataFromError(error),
      );
      // Only record an environment that is genuinely not there. An environment
      // reaches `this.environments` before the rest of its setup (the database
      // sync, its package listing) is done, and a throw from that tail lands
      // here while the environment is live and serving. Reporting it would
      // contradict `environments`, which still lists it, and a loadErrors entry
      // with no `package` means the whole environment was skipped. It also could
      // never be cleared: the clear below only runs when an environment is
      // created, so a live one would carry the entry until restart.
      if (environmentName && !this.environments.has(environmentName)) {
         // Redacted because this is bound for an HTTP response body, not just a
         // log line; see the same call in environment.ts.
         this.failedEnvironments.set(
            environmentName,
            redactPgSecrets(
               error instanceof Error ? error.message : String(error),
            ),
         );
      }
   }

   private async initialize() {
      const reInit = process.env.INITIALIZE_STORAGE === "true";
      const initialTime = performance.now();

      try {
         await this.storageManager.initialize(reInit);

         this.publisherConfigIsFrozen = isPublisherConfigFrozen(
            this.serverRootPath,
         );

         const environmentManifest =
            await EnvironmentStore.reloadEnvironmentManifest(
               this.serverRootPath,
            );

         await this.cleanupAndCreatePublisherPath();

         const repository = this.storageManager.getRepository();

         if (reInit) {
            // Load environments from config file
            await Promise.all(
               environmentManifest.environments.map(async (environment) => {
                  await this.addConfiguredEnvironment(environment);
               }),
            );
         } else {
            // Load existing environments from database
            const existingEnvironments = await repository.listEnvironments();

            if (existingEnvironments.length > 0) {
               // Load environments from database
               await Promise.all(
                  existingEnvironments.map(async (dbEnvironment) => {
                     try {
                        // Check if environment files exist on disk
                        const environmentExists = await fs.promises
                           .access(dbEnvironment.path)
                           .then(() => true)
                           .catch(() => false);

                        if (!environmentExists) {
                           // Try to find in config and reload
                           const environmentConfig =
                              environmentManifest.environments.find(
                                 (p) => p.name === dbEnvironment.name,
                              );

                           if (environmentConfig) {
                              const environmentInstance =
                                 await this.addEnvironment(
                                    {
                                       name: environmentConfig.name,
                                       resource: `${API_PREFIX}/environments/${environmentConfig.name}`,
                                       connections:
                                          environmentConfig.connections,
                                       packages: environmentConfig.packages,
                                    },
                                    true,
                                 );

                              // Update database with new path
                              await repository.updateEnvironment(
                                 dbEnvironment.id,
                                 {
                                    path: environmentInstance.metadata.location,
                                 },
                              );

                              return environmentInstance.listPackages();
                           } else {
                              logger.error(
                                 `Environment "${dbEnvironment.name}" not found in config and files missing`,
                              );
                              return;
                           }
                        }

                        // Get connections from database
                        const connections = await repository.listConnections(
                           dbEnvironment.id,
                        );

                        const environmentInstance = await Environment.create(
                           dbEnvironment.name,
                           dbEnvironment.path,
                           connections.map((conn) => ({
                              name: conn.name,
                              type: conn.type,
                              resource: `${API_PREFIX}/connections/${conn.name}`,
                              ...conn.config,
                           })),
                        );
                        environmentInstance.setMemoryGovernor(
                           this.memoryGovernor,
                        );

                        // Get packages from database
                        const packages = await repository.listPackages(
                           dbEnvironment.id,
                        );
                        packages.forEach((pkg) => {
                           environmentInstance.setPackageStatus(
                              pkg.name,
                              PackageStatus.SERVING,
                           );
                        });

                        this.environments.set(
                           dbEnvironment.name,
                           environmentInstance,
                        );

                        return environmentInstance.listPackages();
                     } catch (error) {
                        this.recordEnvironmentInitializationFailure(
                           dbEnvironment.name,
                           error,
                        );
                     }
                  }),
               );
            } else {
               // Fallback to config file if database is empty
               await Promise.all(
                  environmentManifest.environments.map(async (environment) => {
                     await this.addConfiguredEnvironment(environment);
                  }),
               );
            }
         }

         this.isInitialized = true;
         markReady();
         const initializationDuration = performance.now() - initialTime;
         logger.info(
            `Environment store successfully initialized in ${formatDuration(initializationDuration)}`,
         );
         this.emitReadinessLine();
      } catch (error) {
         markNotReady();
         const errorData = this.extractErrorDataFromError(error);
         logger.error("Error initializing environment store", errorData);
      }
   }

   /**
    * Printed exactly once, from initialize()'s success tail: markReady() has
    * just flipped /api/v0/status to "serving", and both HTTP listeners bound
    * long before the downloads finished, so this is the same moment every
    * existing readiness consumer (CI status polls, MCP /health/readiness)
    * fires on. The catch above swallows a failed boot after markNotReady(),
    * so the line never prints for a server that is not actually serving.
    */
   private emitReadinessLine(): void {
      let packages = 0;
      let loadErrors = this.failedEnvironments.size;
      for (const environment of this.environments.values()) {
         packages += environment.getRegisteredPackageCount();
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
         // A dead stderr pipe must not fail a boot that is already serving.
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

      // Sync environment metadata
      const dbEnvironment = await this.addEnvironmentMetadata(
         environment,
         repository,
      );

      // Sync connections
      await this.addConnections(environment, dbEnvironment.id, repository);

      // Sync packages
      await this.addPackages(environment, dbEnvironment.id, repository);

      logger.info(`Synced environment "${environmentName}" to database`);
   }

   public async deleteEnvironmentFromDatabase(
      environmentName: string,
   ): Promise<void> {
      const repository = this.storageManager.getRepository();

      // Get the environment from database
      const dbEnvironment =
         await repository.getEnvironmentByName(environmentName);

      if (!dbEnvironment) {
         logger.error(`Environment "${environmentName}" not found in database`);
         return;
      }

      // Delete the environment (this will cascade delete connections and packages)
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

      // Sync each package
      for (const pkg of packages) {
         if (!pkg.name) {
            logger.warn("Skipping package with undefined name");
            continue;
         }

         await this.addPackage(pkg, environmentId, repository);
      }
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
         // Add/update connections
         for (const conn of connections) {
            if (!conn.name) {
               logger.warn("Skipping connection with undefined name");
               continue;
            }

            // Check if connection exists
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

      // Get the environment ID from database
      const dbEnvironment =
         await repository.getEnvironmentByName(environmentName);

      if (!dbEnvironment) {
         logger.error(`Environment "${environmentName}" not found in database`);
         throw new Error(
            `Environment "${environmentName}" not found in database`,
         );
      }

      // Get the package from the environment
      const packages = await environment.listPackages();
      const pkg = packages.find((p) => p.name === packageName);

      if (!pkg) {
         logger.warn(`Package "${packageName}" not found in environment`);
         return;
      }

      // Sync the specific package
      await this.addPackage(pkg, dbEnvironment.id, repository);
      logger.info(`Synced package "${packageName}" to database`);
   }

   /**
    * Delete a package from the database
    */
   public async deletePackageFromDatabase(
      environmentName: string,
      packageName: string,
   ): Promise<void> {
      const repository = this.storageManager.getRepository();

      // Get the environment ID from database
      const dbEnvironment =
         await repository.getEnvironmentByName(environmentName);

      if (!dbEnvironment) {
         logger.error(`Environment "${environmentName}" not found in database`);
         return;
      }

      // Find and delete the package
      const existingPackage = await repository.getPackageByName(
         dbEnvironment.id,
         packageName,
      );

      if (existingPackage) {
         await repository.deletePackage(existingPackage.id);
         logger.info(`Deleted package "${packageName}" from database`);
      }
   }

   private async cleanupAndCreatePublisherPath() {
      const reInit = process.env.INITIALIZE_STORAGE === "true";

      // Ensure serverRootPath exists as a directory (mkdir recursive is a
      // no-op when the directory already exists and avoids a Bun-on-Windows
      // bug where fs.promises.stat resolves to undefined instead of throwing)
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
               // Ignore if directory doesn't exist
               throw error;
            }
         }
      } else {
         logger.info(`Using existing publisher path`);
      }

      const uploadDocsPath = path.join(this.serverRootPath, PUBLISHER_DATA_DIR);
      await fs.promises.mkdir(uploadDocsPath, { recursive: true });
   }

   public async listEnvironments(skipInitializationCheck: boolean = false) {
      if (!skipInitializationCheck) {
         await this.finishedInitialization;
      }
      return Promise.all(
         Array.from(this.environments.values()).map((environment) =>
            environment.serialize(),
         ),
      );
   }

   public async getStatus() {
      // Surface the memory governor's back-pressure as a "throttled"
      // operational state so the control plane can stop routing new package
      // loads/queries to a throttled worker. Draining takes precedence: a
      // shutting-down server should keep reporting "draining". When the
      // governor is disabled (no PUBLISHER_MAX_MEMORY_BYTES) this never fires.
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
         loadErrors?: LoadError[];
      } = {
         timestamp: Date.now(),
         environments: [],
         initialized: this.isInitialized,
         frozenConfig: isPublisherConfigFrozen(this.serverRootPath),
         operationalState,
      };

      const environments = await this.listEnvironments(true);

      await Promise.all(
         environments.map(async (environment) => {
            try {
               const packages = environment.packages;
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

      // Collected AFTER listEnvironments, not before: serialize() awaits
      // listPackages(), which is what loads an environment's packages and so
      // records their failures. Reading the maps at the top of this method risks
      // reading them before the failures they report have been recorded.
      //
      // A read-time overlay over live state, like "throttled" above, so there is
      // no fourth stored status to keep in sync.
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
      }
      // Left unset when nothing failed, so the key is absent from the response
      // and a healthy server's status body is byte-for-byte what it was before
      // this field existed.
      if (loadErrors.length > 0) {
         status.loadErrors = loadErrors;
      }

      return status;
   }

   public async getEnvironment(
      environmentName: string,
      reload: boolean = false,
   ): Promise<Environment> {
      await this.finishedInitialization;
      assertSafePackageName(environmentName);

      // Check if environment is already loaded first
      const environment = this.environments.get(environmentName);
      if (environment !== undefined && !reload) {
         return environment;
      }

      // We need to acquire the mutex to prevent concurrent requests from creating the
      // environment multiple times.
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
         // Double-check after acquiring mutex
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
            );
         }
         return await this.addEnvironment({
            name: environmentName,
            resource: `${API_PREFIX}/environments/${environmentName}`,
            connections: environmentConfig?.connections || [],
         });
      });
   }

   public async addEnvironment(
      environment: ApiEnvironment,
      skipInitialization: boolean = false,
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
      // Check if environment already exists and update it instead of creating a new one
      const existingEnvironment = this.environments.get(environmentName);
      if (existingEnvironment) {
         const updatedEnvironment =
            await existingEnvironment.update(environment);
         this.environments.set(environmentName, updatedEnvironment);
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
         (environmentConfig?.packages && environmentConfig.packages.length > 0);
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
         if (absoluteEnvironmentPath.endsWith(".zip")) {
            absoluteEnvironmentPath = await this.unzipEnvironment(
               absoluteEnvironmentPath,
            );
         }
      } else {
         absoluteEnvironmentPath = await this.scaffoldEnvironment(environment);
      }
      const newEnvironment = await Environment.create(
         environmentName,
         absoluteEnvironmentPath,
         environment.connections || [],
      );
      newEnvironment.setMemoryGovernor(this.memoryGovernor);

      if (!newEnvironment.metadata) newEnvironment.metadata = {};
      newEnvironment.metadata.location = absoluteEnvironmentPath;

      // Attach before the SERVING seeding below: those packages are configured
      // but un-mounted, and this is the only record of why.
      for (const [packageName, message] of mountErrors) {
         newEnvironment.setPackageMountError(packageName, message);
      }

      this.environments.set(environmentName, newEnvironment);
      // It loaded, so any earlier failure for this name is stale. A boot
      // failure can be followed by a successful POST or lazy load of the same
      // environment, and a status that keeps reporting the old error would lie
      // in the other direction.
      this.failedEnvironments.delete(environmentName);

      environment?.packages?.forEach((_package) => {
         if (_package.name) {
            newEnvironment.setPackageStatus(
               _package.name,
               PackageStatus.SERVING,
            );
         }
      });

      await this.addEnvironmentToDatabase(newEnvironment);

      return newEnvironment;
   }

   public async unzipEnvironment(absoluteEnvironmentPath: string) {
      assertSafeEnvironmentPath(absoluteEnvironmentPath);
      const startedAt = Date.now();
      logger.info(
         `Detected zip file at "${absoluteEnvironmentPath}". Unzipping...`,
      );
      const unzippedEnvironmentPath = absoluteEnvironmentPath.replace(
         ".zip",
         "",
      );
      await fs.promises.rm(unzippedEnvironmentPath, {
         recursive: true,
         force: true,
      });
      await fs.promises.mkdir(unzippedEnvironmentPath, { recursive: true });

      // Stream-extract via yauzl (wrapped by extract-zip). Each entry's
      // inflate and write are dispatched to the libuv thread pool, so the
      // main event loop stays responsive even for very large archives.
      // The previous adm-zip path used fs.readFileSync + zlib.inflateRawSync
      // on the main thread, which parked the loop long enough on multi-
      // hundred-MB packages to fail Kubernetes liveness probes mid-extract.
      let entryCount = 0;
      let totalUncompressedBytes = 0;
      await extract(absoluteEnvironmentPath, {
         dir: path.resolve(unzippedEnvironmentPath),
         onEntry: (entry) => {
            entryCount += 1;
            totalUncompressedBytes += entry.uncompressedSize ?? 0;
         },
      });

      const mib = (totalUncompressedBytes / (1024 * 1024)).toFixed(1);
      logger.info(
         `Unzipped "${absoluteEnvironmentPath}" -> "${unzippedEnvironmentPath}" ` +
            `(${entryCount} entries, ${mib} MiB uncompressed) in ` +
            `${formatDuration(Date.now() - startedAt)}`,
      );

      return unzippedEnvironmentPath;
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

      // Close all connections before removing the environment
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
            logger.error(
               `Error reading ${PUBLISHER_CONFIG_NAME}. Generating from directory`,
               { error },
            );
            return { frozenConfig: false, environments: [] };
         } else {
            // If publisher.config.json is missing, generate the manifest from directories
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
                     });
                  }
               }
               return { frozenConfig: false, environments };
            } catch (lsError) {
               logger.error(`Error listing directories in ${serverRootPath}`, {
                  error: lsError,
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
    * Absolute on-disk path for a local package `location`, anchored at the
    * directory holding the active config. That covers locations POSTed at
    * runtime too, which no config declares. When there is no config worth
    * anchoring to, `getPublisherConfigDir` returns null (it owns the cases) and
    * the server root is the only meaningful base, which is also what this did
    * before the anchor moved.
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
    * Isolate a location failure to the packages it actually affects instead of
    * aborting the whole environment. The packages that mounted still serve;
    * each one named here is left un-mounted, so its configured status entry
    * (set in addEnvironment) resolves to a per-package load failure that
    * getFailedPackages() -> /status loadErrors reports. A bad location now
    * behaves like a bad manifest: those packages dropped, siblings unaffected.
    *
    * The message is kept rather than letting the lazy load speak for it. An
    * un-mounted package fails later on the manifest that was never copied, and
    * "Package manifest ... does not exist." points at publisher_data/ instead
    * of the location that is actually wrong. Redacted because it reaches an
    * HTTP response body.
    */
   private recordMountFailure(
      error: unknown,
      location: string,
      packageNames: string[],
      mountErrors: Map<string, string>,
   ): void {
      logger.error(
         `Failed to download or mount location "${location}"`,
         this.extractErrorDataFromError(error),
      );
      const message = redactPgSecrets(
         error instanceof Error ? error.message : String(error),
      );
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
      // Why each package's location failed, carried out to the Environment so
      // /status can report the real cause rather than the missing manifest it
      // leaves behind.
      const mountErrors = new Map<string, string>();

      await fs.promises.mkdir(absoluteTargetPath, { recursive: true });

      if (!packages || packages.length === 0) {
         throw new PackageNotFoundError(
            `No packages found for environment ${environmentName}`,
         );
      }

      // Group packages by location to optimize downloads
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

         // For GitHub URLs, group by base repository URL to optimize downloads
         let locationKey = _package.location;
         if (this.isGitHubURL(_package.location)) {
            const githubInfo = this.parseGitHubUrl(_package.location);
            if (githubInfo) {
               // Always use HTTPS format for grouping to ensure consistency
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

      // Processing by each unique location
      for (const [groupedLocation, packagesForLocation] of locationGroups) {
         // Use a hash instead of base64 to keep paths short (Windows MAX_PATH limit)
         // This works for both local and remote paths
         const locationHash = crypto
            .createHash("sha256")
            .update(groupedLocation)
            .digest("hex")
            .substring(0, 16); // Use first 16 chars for shorter paths
         const tempDownloadPath = safeJoinUnderRoot(
            absoluteTargetPath,
            `.temp_${locationHash}`,
         );
         await fs.promises.mkdir(tempDownloadPath, { recursive: true });
         logger.info(`Created temporary directory: ${tempDownloadPath}`);
         // Two failure scopes, deliberately separate. The download is shared by
         // every package at this location, so losing it loses all of them. An
         // extract is per package, so one bad sibling must not strand the ones
         // after it in the loop.
         let downloaded = true;
         try {
            // Use the existing download method for all locations
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
         // Extract each package from the downloaded content
         for (const _package of downloaded ? packagesForLocation : []) {
            // Declared out here so the catch can clean up the exact path this
            // iteration created. Never re-derive it from the package name
            // there: the name may be why we are in the catch at all, and
            // safeJoinUnderRoot permits a name that resolves to the root, so a
            // package called "." would hand the cleanup the whole environment
            // directory. Undefined means nothing was created yet.
            let absolutePackagePath: string | undefined;
            try {
               const packageDir = _package.name;
               assertSafePackageName(packageDir);
               absolutePackagePath = safeJoinUnderRoot(
                  absoluteTargetPath,
                  packageDir,
               );
               // For GitHub URLs, extract the subdirectory path from the original location
               let sourcePath: string;
               if (this.isGitHubURL(_package.location)) {
                  const githubInfo = this.parseGitHubUrl(_package.location);
                  if (githubInfo && githubInfo.packagePath) {
                     // Extract subdirectory from the original GitHub URL
                     // Handle both /tree/main/subdir and /tree/branch/subdir cases
                     const subPathMatch =
                        _package.location.match(/\/tree\/[^/]+\/(.+)$/);
                     if (subPathMatch) {
                        sourcePath = safeJoinUnderRoot(
                           tempDownloadPath,
                           subPathMatch[1],
                        );
                     } else {
                        // If no subdirectory after /tree/branch, the repo itself is the package
                        sourcePath = tempDownloadPath;
                     }
                  } else {
                     // No packagePath means the repo itself is the package
                     sourcePath = tempDownloadPath;
                  }
               } else {
                  // For non-GitHub locations, use package name
                  if (this.isLocalPath(_package.location)) {
                     // Same resolution rule as `downloadOrMountLocation`.
                     // Without this step the existing-source check below
                     // falls through for any relative location, and the
                     // in-place mount branch is unreachable.
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
                  // In-place mount path (watch-mode-only, local-dir packages
                  // only). Replace the recursive copy with a symlink so that
                  // edits to the source repo are visible to Publisher and
                  // can trigger live-reload events.
                  //
                  // Carefully: we MUST distinguish symlinks from real dirs
                  // when removing the prior content, because `fs.rm`'s
                  // recursive mode could in principle traverse into a
                  // symlinked source dir and damage it. We lstat first and
                  // call `unlink` for symlinks, `rm` for real directories.
                  const isInPlace =
                     this.inPlaceEnvs.has(environmentName) &&
                     this.isLocalPath(_package.location);
                  if (isInPlace) {
                     await clearMountTarget(absolutePackagePath);
                     const absoluteSourcePath = path.resolve(sourcePath);
                     // On Windows a "dir" symlink needs elevation
                     // (SeCreateSymbolicLinkPrivilege — admin or Developer
                     // Mode); a junction does not and works for absolute
                     // directory targets, so prefer it there. chokidar watches
                     // through both.
                     const linkType =
                        process.platform === "win32" ? "junction" : "dir";
                     try {
                        await fs.promises.symlink(
                           absoluteSourcePath,
                           absolutePackagePath,
                           linkType,
                        );
                        logger.info(
                           `In-place mount (watch mode): linked package "${packageDir}" -> "${absoluteSourcePath}"`,
                        );
                     } catch (linkError) {
                        // Degrade gracefully instead of failing the env load:
                        // copy the package so it still serves. Live reload
                        // won't work for it (source edits aren't seen), but the
                        // environment comes up. Hit e.g. on locked-down Windows
                        // where even junction creation is denied.
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
                     }
                  } else {
                     if (
                        this.inPlaceEnvs.has(environmentName) &&
                        !this.isLocalPath(_package.location)
                     ) {
                        logger.warn(
                           `Watch mode: package "${packageDir}" has remote location "${_package.location}" — falling back to copy. Source-edit live reload won't work for this package; clone the source locally and use a local-dir location to enable it.`,
                        );
                     }
                     // Copy the specific directory. Clear any stale mount target
                     // first (e.g. a symlink left by a prior --watch-env run), or
                     // fs.cp would throw ERR_FS_CP_DIR_TO_NON_DIR onto it.
                     await clearMountTarget(absolutePackagePath);
                     await fs.promises.mkdir(absolutePackagePath, {
                        recursive: true,
                     });
                     await fs.promises.cp(sourcePath, absolutePackagePath, {
                        recursive: true,
                     });
                     logger.info(
                        `Extracted package "${packageDir}" from ${groupedLocation.startsWith("https://github.com/") && _package.location.includes("/tree/") ? "GitHub subdirectory" : "shared download"}`,
                     );
                  }
               } else {
                  // If source doesn't exist, copy the entire download as the package
                  await clearMountTarget(absolutePackagePath);
                  await fs.promises.mkdir(absolutePackagePath, {
                     recursive: true,
                  });
                  await fs.promises.cp(tempDownloadPath, absolutePackagePath, {
                     recursive: true,
                  });
                  logger.info(
                     `Copied entire download as package "${packageDir}"`,
                  );
               }
            } catch (error) {
               this.recordMountFailure(
                  error,
                  groupedLocation,
                  [_package.name],
                  mountErrors,
               );
               // Leave nothing behind. A copy that threw part way through can
               // leave a tree holding the manifest and models but not the data,
               // which loads cleanly and therefore clears the very error
               // recorded above, so the package serves incomplete content and
               // /status says nothing. Continuing past the failure is what
               // makes this reachable for more than one package per location.
               if (absolutePackagePath) {
                  await clearMountTarget(absolutePackagePath);
               }
            }
         }
         try {
            // Clean up temporary download directory
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
      const isCompressedFile = location.endsWith(".zip");
      // Handle GCS paths
      if (this.isGCSURL(location)) {
         try {
            logger.info(
               `Downloading GCS directory from "${location}" to "${targetPath}"`,
            );
            await this.downloadGcsDirectory(
               location,
               environmentName,
               targetPath,
               isCompressedFile,
            );
            return;
         } catch (error) {
            const errorData = this.extractErrorDataFromError(error);
            logger.error(
               `Failed to download GCS directory "${location}"`,
               errorData,
            );
            throw new PackageNotFoundError(
               `Failed to download GCS directory: ${location}`,
            );
         }
      }

      // Handle GitHub URLs
      if (this.isGitHubURL(location)) {
         try {
            logger.info(
               `Cloning GitHub repository from "${location}" to "${targetPath}"`,
            );
            await this.downloadGitHubDirectory(location, targetPath, {
               environmentName,
               packageNames,
            });
            return;
         } catch (error) {
            const errorData = this.extractErrorDataFromError(error);
            logger.error(
               `Failed to clone GitHub repository "${location}"`,
               errorData,
            );
            throw new PackageNotFoundError(
               `Failed to clone GitHub repository: ${location}`,
            );
         }
      }

      // Handle S3 paths
      if (this.isS3URL(location)) {
         try {
            logger.info(
               `Downloading S3 directory from "${location}" to "${targetPath}"`,
            );
            await this.downloadS3Directory(
               location,
               environmentName,
               targetPath,
               isCompressedFile,
            );
            return;
         } catch (error) {
            const errorData = this.extractErrorDataFromError(error);
            logger.error(
               `Failed to download S3 directory "${location}"`,
               errorData,
            );
            throw new PackageNotFoundError(
               `Failed to download S3 directory: ${location}`,
            );
         }
      }

      // Handle absolute and relative paths
      if (this.isLocalPath(location)) {
         const packagePath: string = this.resolveLocalPath(location);
         try {
            logger.info(
               `Mounting local directory at "${packagePath}" to "${targetPath}"`,
            );
            await this.mountLocalDirectory(
               packagePath,
               targetPath,
               environmentName,
               packageName,
            );
            return;
         } catch (error) {
            const errorData = this.extractErrorDataFromError(error);
            logger.error(
               `Failed to mount local directory "${packagePath}"`,
               errorData,
            );
            throw new PackageNotFoundError(
               `Failed to mount local directory: ${packagePath}`,
            );
         }
      }

      // If we get here, the path format is not supported
      const errorMsg = `Invalid package path: "${location}". Must be an absolute mounted path or a GCS/S3/GitHub URI.`;
      logger.error(errorMsg, { environmentName, location });
      throw new PackageNotFoundError(errorMsg);
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
      if (environmentPath.endsWith(".zip")) {
         environmentPath = await this.unzipEnvironment(environmentPath);
      }
      const environmentDirExists =
         (await fs.promises.stat(environmentPath))?.isDirectory() ?? false;
      if (environmentDirExists) {
         await fs.promises.rm(absoluteTargetPath, {
            recursive: true,
            force: true,
         });
         await fs.promises.mkdir(absoluteTargetPath, { recursive: true });
         await fs.promises.cp(environmentPath, absoluteTargetPath, {
            recursive: true,
         });
      } else {
         throw new PackageNotFoundError(
            `Package ${packageName} for environment ${environmentName} not found in "${environmentPath}"`,
         );
      }
   }

   async downloadGcsDirectory(
      gcsPath: string,
      environmentName: string,
      absoluteDirPath: string,
      isCompressedFile: boolean,
   ) {
      assertSafeEnvironmentPath(absoluteDirPath);
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
            // Strip leading `/` left over from prefix removal when the
            // GCS prefix lacked a trailing slash — otherwise
            // `safeJoinUnderRoot` treats it as absolute and rejects.
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
         // Download the single zip file
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

         // Extract the zip file
         await this.unzipEnvironment(zipFilePath);
         logger.info(`Downloaded S3 zip file ${s3Path} to ${absoluteDirPath}`);
         return;
      }

      // Original behavior: download directory contents
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
            // Strip leading `/` left over from prefix removal when the
            // S3 prefix lacked a trailing slash — otherwise
            // `safeJoinUnderRoot` treats it as absolute and rejects.
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

   private parseGitHubUrl(
      githubUrl: string,
   ): { owner: string; repoName: string; packagePath?: string } | null {
      // Handle HTTPS format: https://github.com/owner/repo/tree/branch/subdir
      const httpsRegex =
         /github\.com\/(?<owner>[^/]+)\/(?<repoName>[^/]+)(?<packagePath>\/[^/]+)*/;
      const httpsMatch = githubUrl.match(httpsRegex);
      if (httpsMatch) {
         const { owner, repoName, packagePath } = httpsMatch.groups!;
         return { owner, repoName, packagePath };
      }

      // Handle SSH format: git@github.com:owner/repo.git or git@github.com:owner/repo
      const sshRegex =
         /git@github\.com:(?<owner>[^/]+)\/(?<repoName>[^/\s]+?)(?:\.git)?(?<packagePath>\/[^/]+)*$/;
      const sshMatch = githubUrl.match(sshRegex);
      if (sshMatch) {
         const { owner, repoName, packagePath } = sshMatch.groups!;
         return { owner, repoName, packagePath };
      }

      return null;
   }

   async downloadGitHubDirectory(
      githubUrl: string,
      absoluteDirPath: string,
      progressContext?: { environmentName?: string; packageNames?: string[] },
   ) {
      assertSafeEnvironmentPath(absoluteDirPath);
      // First we'll clone the repo without the additional path
      // E.g. we're removing `/tree/main/imdb` from https://github.com/credibledata/malloy-samples/tree/main/imdb
      const githubInfo = this.parseGitHubUrl(githubUrl);
      if (!githubInfo) {
         throw new Error(`Invalid GitHub URL: ${githubUrl}`);
      }
      const { owner, repoName, packagePath } = githubInfo;
      // `packagePath` is captured with a leading `/`; strip it so the
      // value is a relative segment usable with `safeJoinUnderRoot`.
      const cleanPackagePath = (
         packagePath?.replace("/tree/main", "") || ""
      ).replace(/^\/+/, "");

      // We'll make sure whatever was in absoluteDirPath is removed,
      // so we have a nice a clean directory where we can clone the repo
      await fs.promises.rm(absoluteDirPath, {
         recursive: true,
         force: true,
      });
      await fs.promises.mkdir(absoluteDirPath, { recursive: true });
      const repoUrl = `https://github.com/${owner}/${repoName}`;
      const reporter = new CloneProgressReporter(
         cloneProgressLabel(`${owner}/${repoName}`, progressContext),
      );

      // We'll clone the repo into absoluteDirPath
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
               const errorData = this.extractErrorDataFromError(err);
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

      // If there's no specific package path, we're done (for grouped downloads)
      if (!cleanPackagePath) {
         logger.info(
            `Successfully cloned entire repository to: ${absoluteDirPath}`,
         );
         return;
      }

      // For single package downloads, extract the specific subdirectory
      // After cloning, we'll replace all contents of absoluteDirPath with the contents of absoluteDirPath/cleanPackagePath
      // E.g. we're moving /var/publisher/asd123/imdb/publisher.json into /var/publisher/asd123/publisher.json

      // Remove all contents of absoluteDirPath (/var/publisher/asd123)
      // except for the cleanPackagePath directory (/var/publisher/asd123/imdb)
      const packageFullPath = safeJoinUnderRoot(
         absoluteDirPath,
         cleanPackagePath,
      );

      // Check if the cleanPackagePath (/var/publisher/asd123/imdb) exists
      const packageExists = await fs.promises
         .access(packageFullPath)
         .then(() => true)
         .catch(() => false);

      if (!packageExists) {
         throw new Error(
            `Package path "${cleanPackagePath}" does not exist in the cloned repository.`,
         );
      }

      // Remove everything in absoluteDirPath (/var/publisher/asd123)
      const dirContents = await fs.promises.readdir(absoluteDirPath);
      for (const entry of dirContents) {
         // Don't remove the cleanPackagePath directory itself (/var/publisher/asd123/imdb)
         if (entry !== cleanPackagePath.replace(/^\/+/, "").split("/")[0]) {
            await fs.promises.rm(safeJoinUnderRoot(absoluteDirPath, entry), {
               recursive: true,
               force: true,
            });
         }
      }

      // Now, move the contents of packageFullPath (/var/publisher/asd123/imdb) up to absoluteDirPath (/var/publisher/asd123)
      const packageContents = await fs.promises.readdir(packageFullPath);
      for (const entry of packageContents) {
         await fs.promises.rename(
            safeJoinUnderRoot(packageFullPath, entry),
            safeJoinUnderRoot(absoluteDirPath, entry),
         );
      }

      // Remove the now-empty cleanPackagePath directory (/var/publisher/asd123/imdb)
      await fs.promises.rm(packageFullPath, { recursive: true, force: true });

      // https://github.com/credibledata/malloy-samples/imdb/publisher.json -> ${absoluteDirPath}/publisher.json
   }

   private extractErrorDataFromError(error: unknown): {
      error: string;
      stack?: string;
      task?: unknown;
   } {
      const errorMessage =
         error instanceof Error ? error.message : String(error);
      const errorData: { error: string; stack?: string; task?: unknown } = {
         error: errorMessage,
      };
      if (error instanceof Error && logger.level === "debug") {
         errorData.stack = error.stack;
      }
      if (error && typeof error === "object" && "task" in error) {
         errorData.task = (error as { task?: unknown }).task;
      }
      return errorData;
   }
}
