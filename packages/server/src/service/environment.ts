import type { LogMessage } from "@malloydata/malloy";
import { MalloyError, Runtime } from "@malloydata/malloy";
import { Mutex } from "async-mutex";
import * as fs from "fs";
import * as path from "path";
import { components } from "../api";
import { API_PREFIX, README_NAME } from "../constants";
import {
   ConnectionNotFoundError,
   EnvironmentNotFoundError,
   PackageNotFoundError,
} from "../errors";
import { logger } from "../logger";
import { ResourceRepository } from "../storage/DatabaseInterface";
import { URL_READER } from "../utils";
import {
   buildEnvironmentMalloyConfig,
   deleteDuckLakeConnectionFile,
   EnvironmentMalloyConfig,
   InternalConnection,
} from "./connection";
import { ApiConnection } from "./model";
import { Package } from "./package";

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
   /**
    * Compiled-package cache, keyed by the package's *current on-disk
    * directory*. Two writes for the same package name produce two distinct
    * directory paths (UUID-scoped versioned dirs) and therefore two distinct
    * cache entries; the package row in the DB carries the indirection from
    * name → path. Entries are evicted by `PackageSweeper` once their
    * directory is no longer referenced and the quiescence window has
    * elapsed, so an in-flight reader holding a `Package` for an older path
    * can keep using it until the sweeper retires both the cache entry and
    * the directory together.
    */
   private packagesByPath: Map<string, Package> = new Map();
   /**
    * Promise-dedup for concurrent first-loads of the same path. Two readers
    * arriving simultaneously after a swap both miss `packagesByPath`; without
    * this map they would each call `Package.create` and one would leak its
    * MalloyConfig into the cache only to be overwritten. With it, the second
    * arrival simply awaits the first arrival's promise.
    */
   private loadsByPath: Map<string, Promise<Package>> = new Map();
   private packageStatuses: Map<string, PackageInfo> = new Map();
   private malloyConfig: EnvironmentMalloyConfig;
   private connectionMutex = new Mutex();
   private retiredConnectionGenerations =
      new Set<RetiredConnectionGeneration>();
   private apiConnections: ApiConnection[];
   private environmentPath: string;
   private environmentName: string;
   /**
    * Optional repository injection. When present, package paths are resolved
    * via the database and the controller's UUID-versioned write path is in
    * effect. When absent (unit tests, callers that construct Environment
    * without the storage layer) the resolver falls back to the deterministic
    * legacy path `<environmentPath>/<packageName>`.
    */
   private repository?: ResourceRepository;
   private cachedEnvironmentDbId?: string;
   public metadata: ApiEnvironment;

   constructor(
      environmentName: string,
      environmentPath: string,
      malloyConfig: EnvironmentMalloyConfig,
      apiConnections: InternalConnection[],
      repository?: ResourceRepository,
   ) {
      this.environmentName = environmentName;
      this.environmentPath = environmentPath;
      this.malloyConfig = malloyConfig;
      this.apiConnections = apiConnections;
      this.repository = repository;
      this.metadata = {
         resource: `${API_PREFIX}/environments/${this.environmentName}`,
         name: this.environmentName,
         location: this.environmentPath,
      };
      void this.reloadEnvironmentMetadata();
   }

   public getEnvironmentName(): string {
      return this.environmentName;
   }

   public getEnvironmentPath(): string {
      return this.environmentPath;
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
      repository?: ResourceRepository,
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
         repository,
      );

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
      // Resolve the package against its current on-disk directory (the
      // versioned UUID dir, or the legacy `<env>/<pkg>` fallback) so the
      // virtual file lives under the same root that `getPackage` will use
      // for compilation. Computing it from `<env>/<pkg>` directly would
      // break any package that has been re-downloaded since startup.
      const pkg = await this.getPackage(packageName);
      const packagePath = pkg.getPackagePath();

      // Place the virtual file in the model's directory so relative imports resolve correctly.
      const modelDir = path.dirname(path.join(packagePath, modelName));
      const virtualUri = `file://${path.join(modelDir, "__compile_check.malloy")}`;
      const virtualUrl = new URL(virtualUri);

      // Read the full model file so the submitted source inherits the model's
      // complete namespace — imports, source definitions, queries, etc.
      const modelPath = path.join(packagePath, modelName);
      let modelContent = "";
      try {
         modelContent = await fs.promises.readFile(modelPath, "utf8");
      } catch {
         // If the model file can't be read, proceed with empty content
         // and let compilation surface any errors naturally.
      }
      const fullSource = modelContent ? `${modelContent}\n${source}` : source;

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
         const packageNames = await this.listPackageNames();
         const packageMetadata = await Promise.all(
            packageNames.map(async (packageName) => {
               try {
                  const packageMetadata = (
                     await this.getPackage(packageName)
                  )?.getPackageMetadata();
                  if (packageMetadata) {
                     packageMetadata.name = packageName;
                  }
                  return packageMetadata;
               } catch (error) {
                  logger.error(
                     `Failed to load package: ${packageName} due to : ${error}`,
                  );
                  // Directory did not contain a valid publisher.json — therefore,
                  // it's not a package; or it timed out.
                  return undefined;
               }
            }),
         );

         const filteredMetadata = packageMetadata.filter(
            (metadata) => metadata,
         ) as ApiPackage[];

         // Hide packages that are mid-unload (deletePackage already retired
         // their connections; their cache entry may briefly survive until the
         // sweeper claims the directory).
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
    * Source of truth for "what packages exist in this environment".
    *
    * Returns the union of:
    *   1. Names of every row in `packages` for this environment, when a
    *      repository was injected. This is the durable source of truth in
    *      production.
    *   2. Keys of `packageStatuses`. This catches the bootstrap window
    *      where the environment row exists but `addPackages` has not yet
    *      written package rows — the names live in `packageStatuses` until
    *      that sync completes — and also unit-test fixtures that construct
    *      `Environment` without a repository at all.
    *
    * Names that are mid-unload (UNLOADING) are excluded. The downstream
    * `listPackages` loops `getPackage` over each remaining name, which
    * tolerates a name appearing only in `packageStatuses` (legacy fallback
    * resolver) or only in the database (full DB-driven path) interchangeably.
    */
   private async listPackageNames(): Promise<string[]> {
      const names = new Set<string>();
      for (const [name, info] of this.packageStatuses) {
         if (info.status === PackageStatus.UNLOADING) continue;
         names.add(name);
      }
      if (this.repository) {
         const dbId = await this.getEnvironmentDbId();
         if (dbId) {
            const rows = await this.repository.listPackages(dbId);
            for (const row of rows) names.add(row.name);
         }
      }
      return Array.from(names);
   }

   private async getEnvironmentDbId(): Promise<string | undefined> {
      if (this.cachedEnvironmentDbId) return this.cachedEnvironmentDbId;
      if (!this.repository) return undefined;
      const dbEnv = await this.repository.getEnvironmentByName(
         this.environmentName,
      );
      this.cachedEnvironmentDbId = dbEnv?.id;
      return this.cachedEnvironmentDbId;
   }

   /**
    * Resolve a package name to its current physical directory. Reads the
    * `manifest_path` column written by `swapPackageDirectory`. Falls back to
    * `<environmentPath>/<packageName>` for legacy / test callers that do
    * not go through the repository — that fallback only succeeds when the
    * directory actually exists, so an unknown package still surfaces as
    * "not found".
    */
   public async resolvePackagePath(
      packageName: string,
   ): Promise<string | undefined> {
      if (this.repository) {
         const dbId = await this.getEnvironmentDbId();
         if (dbId) {
            const pkg = await this.repository.getPackageByName(
               dbId,
               packageName,
            );
            if (pkg?.manifestPath) return pkg.manifestPath;
         }
      }
      const legacy = path.join(this.environmentPath, packageName);
      try {
         const stat = await fs.promises.stat(legacy);
         if (stat.isDirectory()) return legacy;
      } catch {
         // Falls through to undefined.
      }
      return undefined;
   }

   /**
    * Look up the package's current directory and return the cached compiled
    * `Package`, loading it on first access. Concurrent first-loads of the
    * same path are deduped through `loadsByPath` so the cost is paid once
    * even under a request storm.
    *
    * The `_reloadDeprecated` argument is preserved for source compatibility
    * with the old API. Reloads are now driven by the controller writing a
    * fresh directory and calling `swapPackageDirectory`; that pivots the
    * resolver to a new path, which automatically misses the cache and
    * triggers a fresh load with no extra logic here.
    */
   public async getPackage(
      packageName: string,
      _reloadDeprecated: boolean = false,
   ): Promise<Package> {
      const packagePath = await this.resolvePackagePath(packageName);
      if (!packagePath) {
         throw new PackageNotFoundError(`Package ${packageName} not found`);
      }
      return this.loadPackageAtPath(packageName, packagePath);
   }

   private async loadPackageAtPath(
      packageName: string,
      packagePath: string,
   ): Promise<Package> {
      const cached = this.packagesByPath.get(packagePath);
      if (cached !== undefined) return cached;

      const inflight = this.loadsByPath.get(packagePath);
      if (inflight) return inflight;

      this.setPackageStatus(packageName, PackageStatus.LOADING);
      const promise = Package.create(
         this.environmentName,
         packageName,
         packagePath,
         () => this.malloyConfig.malloyConfig,
      )
         .then((pkg) => {
            this.packagesByPath.set(packagePath, pkg);
            this.setPackageStatus(packageName, PackageStatus.SERVING);
            return pkg;
         })
         .catch((error) => {
            // Roll status forward only on success; failed loads leave no
            // entry in `packagesByPath` so the next caller will retry.
            this.deletePackageStatus(packageName);
            throw error;
         })
         .finally(() => {
            this.loadsByPath.delete(packagePath);
         });

      this.loadsByPath.set(packagePath, promise);
      return promise;
   }

   /**
    * Drop the cached `Package` for `packagePath` and retire the connections
    * it owns. Called by `PackageSweeper` immediately before the directory is
    * `rm -rf`'d so we never observe a `Package` whose backing files are
    * gone.
    */
   public evictPackageAtPath(packagePath: string): void {
      const pkg = this.packagesByPath.get(packagePath);
      if (!pkg) return;
      this.packagesByPath.delete(packagePath);
      this.retireConnectionGeneration(`package-evict ${packagePath}`, () =>
         pkg.getMalloyConfig().releaseConnections(),
      );
   }

   /**
    * Update the in-memory metadata for a package after the controller has
    * persisted the same change to the database. The directory swap (if
    * any) is the controller's responsibility; this method only refreshes
    * what the cached `Package` reports as its metadata so reads coming
    * back from the cache reflect the latest description / location until
    * the next directory swap repaves it.
    */
   public async updatePackage(
      packageName: string,
      body: ApiPackage,
   ): Promise<ApiPackage> {
      const pkg = await this.getPackage(packageName);
      if (body.name) {
         pkg.setName(body.name);
      }
      pkg.setPackageMetadata({
         name: body.name,
         description: body.description,
         resource: body.resource,
         location: body.location,
      });
      return pkg.getPackageMetadata();
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

   /**
    * Tear down the in-memory state for a package. Returns the directory
    * the package was bound to so the controller can hand it to the sweeper
    * for deferred deletion. The actual `rm -rf` does NOT happen here:
    * dropping the directory while in-flight readers still hold a reference
    * to the cached `Package` would yank files out from under their lazy
    * imports. The sweeper owns that step and waits out the quiescence
    * window first.
    */
   public async deletePackage(packageName: string): Promise<string | undefined> {
      const packageStatus = this.packageStatuses.get(packageName);
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
      }
      this.setPackageStatus(packageName, PackageStatus.UNLOADING);

      const packagePath = await this.resolvePackagePath(packageName);
      if (packagePath) {
         this.evictPackageAtPath(packagePath);
      }

      this.packageStatuses.delete(packageName);
      return packagePath;
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
         Array.from(this.packagesByPath.values(), (pkg) =>
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
      this.packagesByPath.clear();
      this.loadsByPath.clear();
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
