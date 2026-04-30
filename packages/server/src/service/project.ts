import type { LogMessage } from "@malloydata/malloy";
import { MalloyError, Runtime } from "@malloydata/malloy";
import { Mutex } from "async-mutex";
import * as fs from "fs";
import * as path from "path";
import { components } from "../api";
import { API_PREFIX, README_NAME } from "../constants";
import {
   ConnectionNotFoundError,
   PackageNotFoundError,
   ProjectNotFoundError,
} from "../errors";
import { logger } from "../logger";
import { URL_READER } from "../utils";
import {
   buildProjectMalloyConfig,
   deleteDuckLakeConnectionFile,
   InternalConnection,
   ProjectMalloyConfig,
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
type ApiProject = components["schemas"]["Project"];
type RetiredConnectionGeneration = {
   label: string;
   releaseConnections: () => Promise<void>;
   timer?: ReturnType<typeof setTimeout>;
};

const RETIRED_CONNECTION_DRAIN_MS = 30_000;

export class Project {
   private packages: Map<string, Package> = new Map();
   // Lock ordering: connectionMutex (project) MUST be acquired before any
   // packageMutex. Connection updates may invalidate cached package
   // MalloyConfigs and force reloads, so the project lock is the outer one.
   // Never acquire connectionMutex while holding a packageMutex — that's the
   // AB/BA deadlock path.
   private packageMutexes = new Map<string, Mutex>();
   private packageStatuses: Map<string, PackageInfo> = new Map();
   private malloyConfig: ProjectMalloyConfig;
   private connectionMutex = new Mutex();
   private retiredConnectionGenerations =
      new Set<RetiredConnectionGeneration>();
   private apiConnections: ApiConnection[];
   private projectPath: string;
   private projectName: string;
   public metadata: ApiProject;

   constructor(
      projectName: string,
      projectPath: string,
      malloyConfig: ProjectMalloyConfig,
      apiConnections: InternalConnection[],
   ) {
      this.projectName = projectName;
      this.projectPath = projectPath;
      this.malloyConfig = malloyConfig;
      this.apiConnections = apiConnections;
      this.metadata = {
         resource: `${API_PREFIX}/projects/${this.projectName}`,
         name: this.projectName,
         location: this.projectPath,
      };
      void this.reloadProjectMetadata();
   }

   private async writeProjectReadme(readme?: string): Promise<void> {
      if (readme === undefined) return;

      const readmePath = path.join(this.projectPath, "README.md");

      try {
         await fs.promises.writeFile(readmePath, readme, "utf-8");
         logger.info(`Updated README.md for project ${this.projectName}`);
      } catch (err) {
         logger.error(`Failed to write README.md`, { error: err });
         throw new Error(`Failed to update project README`);
      }
   }

   public async update(payload: ApiProject) {
      if (payload.readme !== undefined) {
         this.metadata.readme = payload.readme;
         await this.writeProjectReadme(payload.readme);
      }

      if (payload.materializationStorage !== undefined) {
         this.metadata.materializationStorage = payload.materializationStorage;
      }

      // Handle connections update
      // TODO: Update project connections should have its own API endpoint
      if (payload.connections) {
         const payloadConnections = payload.connections;
         await this.runConnectionUpdateExclusive(async () => {
            logger.info(
               `Updating ${payloadConnections.length} connections for project ${this.projectName}`,
            );
            const isUpdateConnectionRequest = true;
            const nextMalloyConfig = buildProjectMalloyConfig(
               payloadConnections,
               this.projectPath,
               isUpdateConnectionRequest,
            );

            this.updateConnections(nextMalloyConfig);

            logger.info(
               `Successfully updated connections for project ${this.projectName}`,
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
      projectName: string,
      projectPath: string,
      connections: ApiConnection[],
   ): Promise<Project> {
      if (!(await fs.promises.stat(projectPath))?.isDirectory()) {
         throw new ProjectNotFoundError(
            `Project path ${projectPath} not found`,
         );
      }

      logger.info(`Creating project with connection configuration`);
      const malloyConfig = buildProjectMalloyConfig(connections, projectPath);

      logger.info(
         `Loaded ${malloyConfig.apiConnections.length} connections for project ${projectName}`,
         {
            apiConnections: malloyConfig.apiConnections,
         },
      );

      const project = new Project(
         projectName,
         projectPath,
         malloyConfig,
         malloyConfig.apiConnections,
      );

      return project;
   }

   public async reloadProjectMetadata(): Promise<ApiProject> {
      let readme = "";
      try {
         readme = (
            await fs.promises.readFile(path.join(this.projectPath, README_NAME))
         ).toString();
      } catch {
         // Readme not found, so we'll just return an empty string
      }
      this.metadata = {
         ...this.metadata,
         resource: `${API_PREFIX}/projects/${this.projectName}`,
         name: this.projectName,
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
      // Place the virtual file in the model's directory so relative imports resolve correctly.
      const modelDir = path.dirname(
         path.join(this.projectPath, packageName, modelName),
      );
      const virtualUri = `file://${path.join(modelDir, "__compile_check.malloy")}`;
      const virtualUrl = new URL(virtualUri);

      // Read the full model file so the submitted source inherits the model's
      // complete namespace — imports, source definitions, queries, etc.
      const modelPath = path.join(this.projectPath, packageName, modelName);
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

      const pkg = await this.getPackage(packageName);

      // Initialize Runtime with the package's active MalloyConfig so compile
      // checks see the same package-scoped duckdb as execution. This runtime
      // borrows the package config; the package/project lifecycle owns release.
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

   public getProjectMalloyConfig() {
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
      logger.debug("Listing packages", { projectPath: this.projectPath });
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

   public async getPackage(
      packageName: string,
      reload: boolean = false,
   ): Promise<Package> {
      // Check if package is already loaded first
      const _package = this.packages.get(packageName);
      if (_package !== undefined && !reload) {
         return _package;
      }

      // We need to acquire the mutex to prevent a thundering herd of requests from creating the
      // package multiple times.
      let packageMutex = this.packageMutexes.get(packageName);
      if (packageMutex?.isLocked()) {
         logger.debug(
            `Package ${packageName} is being loaded, waiting for unlock...`,
         );
         await packageMutex.waitForUnlock();
         logger.debug(`Package ${packageName} unlocked`);
         const existingPackage = this.packages.get(packageName);
         if (existingPackage) {
            logger.debug(`Package ${packageName} loaded by another request`);
            return existingPackage;
         }
         // If package still doesn't exist after unlock, it might have failed to load
         // Continue to try loading it ourselves
      }
      packageMutex = new Mutex();
      this.packageMutexes.set(packageName, packageMutex);

      return packageMutex.runExclusive(async () => {
         // Double-check after acquiring mutex
         const existingPackage = this.packages.get(packageName);
         if (existingPackage !== undefined && !reload) {
            return existingPackage;
         }

         // Set package status to loading
         this.setPackageStatus(packageName, PackageStatus.LOADING);

         try {
            logger.debug(`Loading package ${packageName}...`);
            const packagePath = path.join(this.projectPath, packageName);
            const _package = await Package.create(
               this.projectName,
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

            // Set package status to serving
            this.setPackageStatus(packageName, PackageStatus.SERVING);
            logger.debug(`Successfully loaded package ${packageName}`);

            return _package;
         } catch (error) {
            logger.error(`Failed to load package ${packageName}`, { error });
            // Clean up on error - mutex will be automatically released by runExclusive
            this.packages.delete(packageName);
            this.packageStatuses.delete(packageName);
            throw error;
         }
         // Mutex is automatically released here by runExclusive
      });
   }

   public async addPackage(packageName: string) {
      const packagePath = path.join(this.projectPath, packageName);
      if (
         !(await fs.promises
            .access(packagePath)
            .then(() => true)
            .catch(() => false)) ||
         !(await fs.promises.stat(packagePath))?.isDirectory()
      ) {
         throw new PackageNotFoundError(`Package ${packageName} not found`);
      }
      logger.info(
         `Adding package ${packageName} to project ${this.projectName}`,
         {
            packagePath,
            malloyConfig: this.malloyConfig.malloyConfig,
         },
      );
      this.setPackageStatus(packageName, PackageStatus.LOADING);
      try {
         this.packages.set(
            packageName,
            await Package.create(
               this.projectName,
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

   private async writePackageManifest(
      packageName: string,
      metadata: { name: string; description?: string },
   ): Promise<void> {
      const packagePath = path.join(this.projectPath, packageName);
      const manifestPath = path.join(packagePath, "publisher.json");

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
      const _package = this.packages.get(packageName);
      if (!_package) {
         return;
      }
      const packageStatus = this.packageStatuses.get(packageName);

      if (packageStatus?.status === PackageStatus.LOADING) {
         logger.error("Package loading. Can't unload.", {
            projectName: this.projectName,
            packageName,
         });
         throw new Error(
            "Package loading. Can't unload. " +
               this.projectName +
               " " +
               packageName,
         );
      } else if (packageStatus?.status === PackageStatus.SERVING) {
         this.setPackageStatus(packageName, PackageStatus.UNLOADING);
      }

      await _package.getMalloyConfig().releaseConnections();

      try {
         await fs.promises.rm(path.join(this.projectPath, packageName), {
            recursive: true,
            force: true,
         });
      } catch (err) {
         logger.error(
            "Error removing package directory while unloading package",
            { error: err, projectName: this.projectName, packageName },
         );
      }

      // Remove from internal tracking
      this.packages.delete(packageName);
      this.packageStatuses.delete(packageName);
   }

   public updateConnections(
      malloyConfig: ProjectMalloyConfig,
      _apiConnections?: ApiConnection[],
      afterPreviousRelease?: () => Promise<void>,
   ): void {
      const previousMalloyConfig = this.malloyConfig;
      this.malloyConfig = malloyConfig;
      this.apiConnections = malloyConfig.apiConnections;

      if (previousMalloyConfig !== malloyConfig) {
         this.retireConnectionGeneration(
            `project ${this.projectName}`,
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
            `Removed connection ${connectionName} from project ${this.projectName}`,
         );
      } else {
         logger.warn(
            `Connection ${connectionName} not found in project ${this.projectName}`,
         );
      }
   }

   public async closeAllConnections(): Promise<void> {
      // Release the package-scoped MalloyConfigs (each holds the package's own
      // sandbox `duckdb` connection) before tearing down the project config
      // they wrap. Without this, hard unload leaks per-package DuckDB handles.
      const packageReleases = await Promise.allSettled(
         Array.from(this.packages.values(), (pkg) =>
            pkg.getMalloyConfig().releaseConnections(),
         ),
      );
      for (const result of packageReleases) {
         if (result.status === "rejected") {
            logger.error(
               `Error closing package connections for project ${this.projectName}`,
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
            `Error closing connections for project ${this.projectName}`,
            { error },
         );
      }
      await this.releaseAllRetiredConnectionGenerations();

      this.apiConnections = [];

      logger.info(`Closed all connections for project ${this.projectName}`);
   }

   public async serialize(): Promise<ApiProject> {
      return {
         ...this.metadata,
         connections: this.listApiConnections(),
         packages: await this.listPackages(),
      };
   }

   public async deleteDuckDBConnection(connectionName: string): Promise<void> {
      const duckdbPath = path.join(
         this.projectPath,
         `${connectionName}.duckdb`,
      );
      try {
         await fs.promises.rm(duckdbPath, { force: true });
         logger.info(
            `Removed DuckDB connection file ${connectionName} from project ${this.projectName}`,
         );
      } catch (error) {
         logger.error(
            `Failed to remove DuckDB connection file ${connectionName} from project ${this.projectName}`,
            { error },
         );
      }
   }

   public async deleteDuckLakeConnection(
      connectionName: string,
   ): Promise<void> {
      await deleteDuckLakeConnectionFile(connectionName, this.projectPath);
      logger.info(
         `Removed DuckLake connection ${connectionName} from project ${this.projectName}`,
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
