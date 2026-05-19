import type { LogMessage } from "@malloydata/malloy";
import { MalloyError, Runtime } from "@malloydata/malloy";
import { Mutex } from "async-mutex";
import crypto from "crypto";
import * as fs from "fs";
import * as path from "path";
import { components } from "../api";
import { API_PREFIX, README_NAME } from "../constants";
import {
   ConnectionNotFoundError,
   EnvironmentNotFoundError,
   PackageNotFoundError,
   ServiceUnavailableError,
} from "../errors";
import { logger } from "../logger";
import {
   assertSafeEnvironmentPath,
   assertSafePackageName,
   assertSafeRelativeModelPath,
   safeJoinUnderRoot,
} from "../path_safety";
import { BuildManifest } from "../storage/DatabaseInterface";
import { URL_READER } from "../utils";
import {
   buildEnvironmentMalloyConfig,
   deleteDuckLakeConnectionFile,
   EnvironmentMalloyConfig,
   InternalConnection,
} from "./connection";
import { ApiConnection } from "./model";
import { Package } from "./package";
import type { PackageMemoryGovernor } from "./package_memory_governor";

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
 * Both names start with a `.` so the package walkers (which use
 * {@link ignoreDotfiles}) skip them.
 */
const STAGING_DIR_NAME = ".staging";
const RETIRED_DIR_NAME = ".retired";

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
type ApiEnvironment = components["schemas"]["Environment"];
type RetiredConnectionGeneration = {
   label: string;
   releaseConnections: () => Promise<void>;
   timer?: ReturnType<typeof setTimeout>;
};

const RETIRED_CONNECTION_DRAIN_MS = 30_000;

export class Environment {
   private packages: Map<string, Package> = new Map();
   // Lock ordering: connectionMutex (environment) MUST be acquired before any
   // packageMutex. Connection updates may invalidate cached package
   // MalloyConfigs and force reloads, so the environment lock is the outer one.
   // Never acquire connectionMutex while holding a packageMutex — that's the
   // AB/BA deadlock path.
   private packageMutexes = new Map<string, Mutex>();
   private packageStatuses: Map<string, PackageInfo> = new Map();
   private malloyConfig: EnvironmentMalloyConfig;
   private connectionMutex = new Mutex();
   private retiredConnectionGenerations =
      new Set<RetiredConnectionGeneration>();
   private apiConnections: ApiConnection[];
   private environmentPath: string;
   private environmentName: string;
   public metadata: ApiEnvironment;
   // The shared memory governor that consults process RSS. Optional —
   // when null the gate is a no-op and the environment behaves exactly
   // like it did before the governor was introduced. Set by
   // EnvironmentStore.setMemoryGovernor at server start so we keep the
   // governor as the single owner of the back-pressure boolean.
   private memoryGovernor: PackageMemoryGovernor | null = null;

   constructor(
      environmentName: string,
      environmentPath: string,
      malloyConfig: EnvironmentMalloyConfig,
      apiConnections: InternalConnection[],
   ) {
      // Sanitizer barrier: every downstream `path.join(this.environmentPath,
      // …)` site (including the static `sweepStaleInstallDirs` sweep) gets a
      // value that has cleared an allowlist check at the gate.
      assertSafeEnvironmentPath(environmentPath);
      this.environmentName = environmentName;
      this.environmentPath = environmentPath;
      this.malloyConfig = malloyConfig;
      this.apiConnections = apiConnections;
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
         throw new Error(`Failed to update environment README`);
      }
   }

   public async update(payload: ApiEnvironment) {
      if (payload.readme !== undefined) {
         this.metadata.readme = payload.readme;
         await this.writeEnvironmentReadme(payload.readme);
      }

      if (payload.materializationStorage !== undefined) {
         this.metadata.materializationStorage = payload.materializationStorage;
      }

      // Handle connections update
      // TODO: Update environment connections should have its own API endpoint
      if (payload.connections) {
         const payloadConnections = payload.connections;
         await this.runConnectionUpdateExclusive(async () => {
            logger.info(
               `Updating ${payloadConnections.length} connections for environment ${this.environmentName}`,
            );
            const isUpdateConnectionRequest = true;
            const nextMalloyConfig = buildEnvironmentMalloyConfig(
               payloadConnections,
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
   ): Promise<Environment> {
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
            apiConnections: malloyConfig.apiConnections,
         },
      );

      const environment = new Environment(
         environmentName,
         environmentPath,
         malloyConfig,
         malloyConfig.apiConnections,
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
               path.join(this.environmentPath, README_NAME),
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
      source: string,
      includeSql: boolean = false,
   ): Promise<{ problems: LogMessage[]; sql?: string }> {
      assertSafePackageName(packageName);
      assertSafeRelativeModelPath(modelName);
      // Hold the per-package mutex for the duration of every disk read —
      // both the explicit `fs.readFile(modelPath)` below and the implicit
      // import resolution that `runtime.loadModel` does through the URL
      // reader. This is mutually exclusive with `installPackage`'s Phase 2
      // rename swap and with `deletePackage`'s rename-to-retired, so a
      // compile can never observe a half-rewritten tree. The slow Phase 1
      // download happens outside this lock, so a multi-second clone does
      // not block compiles.
      return this.withPackageLock(packageName, async () => {
         // Sanitized join: input segments are allowlisted above; the
         // resolve-and-contain check here is the secondary guard CodeQL's
         // path-injection sanitizer recognises.
         const modelPath = safeJoinUnderRoot(
            this.environmentPath,
            packageName,
            modelName,
         );
         // Place the virtual file in the model's directory so relative imports resolve correctly.
         const modelDir = path.dirname(modelPath);
         const virtualUri = `file://${path.join(modelDir, "__compile_check.malloy")}`;
         const virtualUrl = new URL(virtualUri);

         // Read the full model file so the submitted source inherits the model's
         // complete namespace — imports, source definitions, queries, etc.
         let modelContent = "";
         try {
            modelContent = await fs.promises.readFile(modelPath, "utf8");
         } catch {
            // If the model file can't be read, proceed with empty content
            // and let compilation surface any errors naturally.
         }
         const fullSource = modelContent
            ? `${modelContent}\n${source}`
            : source;

         // Create a URL Reader that serves the source string for the virtual file,
         // but falls back to the disk for everything else (imports).
         const interceptingReader = {
            readURL: async (url: URL) => {
               if (url.toString() === virtualUri) {
                  return fullSource;
               }
               return URL_READER.readURL(url);
            },
         };

         // Use the locked variant — we already hold the per-package mutex.
         const pkg = await this._loadOrGetPackageLocked(packageName);

         // Initialize Runtime with the package's active MalloyConfig so compile
         // checks see the same package-scoped duckdb as execution. This runtime
         // borrows the package config; the package/environment lifecycle owns release.
         const runtime = new Runtime({
            urlReader: interceptingReader,
            config: pkg.getMalloyConfig(),
         });

         // Attempt to compile
         try {
            const modelMaterializer = runtime.loadModel(virtualUrl);
            const model = await modelMaterializer.getModel();

            // If includeSql is requested and compilation succeeded, attempt to extract SQL
            let sql: string | undefined;
            if (includeSql) {
               try {
                  const queryMaterializer = modelMaterializer.loadFinalQuery();
                  sql = await queryMaterializer.getSQL();
               } catch {
                  // Source may not contain a runnable query (e.g. only source definitions),
                  // in which case we simply omit the sql field.
               }
            }

            // If successful, return any non-fatal warnings
            return { problems: model.problems, sql };
         } catch (error) {
            // If parsing/compilation fails, return the errors
            if (error instanceof MalloyError) {
               return { problems: error.problems };
            }
            // If it's a system error (e.g. file not found), throw it up
            throw error;
         }
      });
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

   public async listPackages(): Promise<ApiPackage[]> {
      logger.debug("Listing packages", {
         environmentPath: this.environmentPath,
      });
      try {
         const packageMetadata = await Promise.all(
            Array.from(this.packageStatuses.keys()).map(async (packageName) => {
               try {
                  const packageMetadata = (
                     this.packageStatuses.get(packageName)?.status ===
                     PackageStatus.LOADING
                        ? undefined
                        : await this.getPackage(packageName, false)
                  )?.getPackageMetadata();
                  if (packageMetadata) {
                     packageMetadata.name = packageName;
                  }
                  return packageMetadata;
               } catch (error) {
                  logger.error(
                     `Failed to load package: ${packageName} due to : ${error}`,
                  );
                  // Directory did not contain a valid package.json file -- therefore, it's not a package.
                  // Or it timed out
                  return undefined;
               }
            }),
         );
         // Get rid of undefined entries (i.e, directories without publisher.json files).
         const filteredMetadata = packageMetadata.filter(
            (metadata) => metadata,
         ) as ApiPackage[];

         // Filter out packages that are being unloaded
         const finalMetadata = filteredMetadata.filter((metadata) => {
            const packageStatus = this.packageStatuses.get(metadata.name || "");
            return packageStatus?.status !== PackageStatus.UNLOADING;
         });

         return finalMetadata;
      } catch (error) {
         logger.error("Error listing packages", { error });
         console.error(error);
         throw error;
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
      throw new ServiceUnavailableError(
         `Publisher is under memory pressure and cannot ${reason} (package "${packageName}", environment "${this.environmentName}"). Retry after the server's memory usage drops below the configured low-water mark.`,
      );
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
      // path (notably MCP resource handlers and Model.getModel()) must
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
         if (existingPackage !== undefined && reload) {
            this.retireConnectionGeneration(`package ${packageName}`, () =>
               existingPackage.getMalloyConfig().releaseConnections(),
            );
         }
         this.packages.set(packageName, _package);
         this.setPackageStatus(packageName, PackageStatus.SERVING);
         logger.debug(`Successfully loaded package ${packageName}`);

         return _package;
      } catch (error) {
         logger.error(`Failed to load package ${packageName}`, { error });
         this.packages.delete(packageName);
         this.packageStatuses.delete(packageName);
         throw error;
      }
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
            malloyConfig: this.malloyConfig.malloyConfig,
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

      this.setPackageStatus(packageName, PackageStatus.LOADING);
      try {
         this.packages.set(
            packageName,
            await Package.create(
               this.environmentName,
               packageName,
               packagePath,
               () => this.malloyConfig.malloyConfig,
            ),
         );
      } catch (error) {
         logger.error("Error adding package", { error });
         this.deletePackageStatus(packageName);
         throw error;
      }
      this.setPackageStatus(packageName, PackageStatus.SERVING);
      return this.packages.get(packageName);
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
   ): Promise<Package> {
      assertSafePackageName(packageName);
      const stagingPath = this.allocateStagingPath(packageName);
      await fs.promises.mkdir(path.dirname(stagingPath), { recursive: true });

      try {
         await downloader(stagingPath);
      } catch (err) {
         await fs.promises
            .rm(stagingPath, { recursive: true, force: true })
            .catch(() => {});
         throw err;
      }

      return this.withPackageLock(packageName, async () => {
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
            );
         } catch (err) {
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
            if (retiredPath) {
               try {
                  await fs.promises.rename(retiredPath, canonicalPath);
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
            this.deletePackageStatus(packageName);
            throw err;
         }

         this.packages.set(packageName, newPackage);
         this.setPackageStatus(packageName, PackageStatus.SERVING);

         if (oldPackage) {
            this.retireConnectionGeneration(`package ${packageName}`, () =>
               oldPackage.getMalloyConfig().releaseConnections(),
            );
         }

         if (retiredPath) {
            const pathToClean = retiredPath;
            setImmediate(() => {
               void fs.promises
                  .rm(pathToClean, { recursive: true, force: true })
                  .catch((err) => {
                     logger.warn(
                        `Failed to clean up retired package directory ${pathToClean}`,
                        { error: err },
                     );
                  });
            });
         }

         return newPackage;
      });
   }

   /**
    * Reload every model in a package against the supplied build manifest,
    * holding the per-package mutex for the duration of the disk reads.
    * Replaces direct `Package.reloadAllModels` calls from outside
    * `Environment`.
    */
   public async reloadAllModelsForPackage(
      packageName: string,
      manifest: BuildManifest["entries"],
   ): Promise<void> {
      assertSafePackageName(packageName);
      return this.withPackageLock(packageName, async () => {
         const pkg = this.packages.get(packageName);
         if (!pkg) {
            throw new PackageNotFoundError(
               `Package ${packageName} is not loaded`,
            );
         }
         await pkg.reloadAllModels(manifest);
      });
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
      metadata: { name: string; description?: string },
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

         // Update with new metadata
         const updatedManifest = {
            ...existingManifest,
            name: metadata.name,
            description: metadata.description,
         };

         // Write back to file
         await fs.promises.writeFile(
            manifestPath,
            JSON.stringify(updatedManifest, null, 2),
            "utf-8",
         );

         logger.info(`Updated publisher.json for ${packageName}`);
      } catch (error) {
         logger.error(`Failed to update publisher.json`, { error });
         throw new Error(`Failed to update package manifest`);
      }
   }

   public async updatePackage(packageName: string, body: ApiPackage) {
      assertSafePackageName(packageName);
      return this.withPackageLock(packageName, async () => {
         const _package = this.packages.get(packageName);
         if (!_package) {
            throw new PackageNotFoundError(`Package ${packageName} not found`);
         }
         if (body.name) {
            _package.setName(body.name);
         }
         _package.setPackageMetadata({
            name: body.name,
            description: body.description,
            resource: body.resource,
            location: body.location,
         });

         await this.writePackageManifest(packageName, {
            name: packageName,
            description: body.description,
         });

         return _package.getPackageMetadata();
      });
   }

   public getPackageStatus(packageName: string): PackageInfo | undefined {
      return this.packageStatuses.get(packageName);
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
            _package.getMalloyConfig().releaseConnections(),
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
            pkg.getMalloyConfig().releaseConnections(),
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
      await this.releaseAllRetiredConnectionGenerations();

      this.apiConnections = [];

      logger.info(
         `Closed all connections for environment ${this.environmentName}`,
      );
   }

   public async serialize(): Promise<ApiEnvironment> {
      return {
         ...this.metadata,
         connections: this.listApiConnections(),
         packages: await this.listPackages(),
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
