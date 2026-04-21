import { GetObjectCommand, S3 } from "@aws-sdk/client-s3";
import { Storage } from "@google-cloud/storage";
import AdmZip from "adm-zip";
import { Mutex } from "async-mutex";
import crypto from "crypto";
import * as fs from "fs";
import * as path from "path";
import simpleGit from "simple-git";
import { Writable } from "stream";
import { components } from "../api";
import {
   getProcessedPublisherConfig,
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
import { Connection } from "../storage/DatabaseInterface";
import { StorageConfig, StorageManager } from "../storage/StorageManager";
import { Environment, PackageStatus } from "./environment";
type ApiEnvironment = components["schemas"]["Environment"];

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

export class EnvironmentStore {
   public serverRootPath: string;
   private environments: Map<string, Environment> = new Map();
   private environmentMutexes = new Map<string, Mutex>();
   public publisherConfigIsFrozen: boolean;
   public finishedInitialization: Promise<void>;
   private isInitialized: boolean = false;
   public storageManager: StorageManager;
   private s3Client = new S3({
      followRegionRedirects: true,
   });
   private gcsClient: Storage;

   constructor(serverRootPath: string) {
      this.serverRootPath = serverRootPath;
      this.gcsClient = new Storage();

      const storageConfig: StorageConfig = {
         type: "duckdb",
         duckdb: {
            path: path.join(serverRootPath, "publisher.db"),
         },
      };
      this.storageManager = new StorageManager(storageConfig);

      this.finishedInitialization = this.initialize();
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
                  await this.addEnvironment(
                     {
                        name: environment.name,
                        resource: `${API_PREFIX}/environments/${environment.name}`,
                        connections: environment.connections,
                        packages: environment.packages,
                     },
                     true,
                  );
               }),
            );
         } else {
            // Load existing environments from database
            const existingProjects = await repository.listEnvironments();

            if (existingProjects.length > 0) {
               // Load environments from database
               await Promise.all(
                  existingProjects.map(async (dbEnvironment) => {
                     // Check if environment files exist on disk
                     const environmentExists = await fs.promises
                        .access(dbEnvironment.path)
                        .then(() => true)
                        .catch(() => false);

                     if (!environmentExists) {
                        // Try to find in config and reload
                        const environmentConfig =
                           environmentManifest.environments.find(
                              (e) => e.name === dbEnvironment.name,
                           );

                        if (environmentConfig) {
                           const environmentInstance =
                              await this.addEnvironment(
                                 {
                                    name: environmentConfig.name,
                                    resource: `${API_PREFIX}/environments/${environmentConfig.name}`,
                                    connections: environmentConfig.connections,
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

                     this.environments.set(
                        dbEnvironment.name,
                        environmentInstance,
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

                     return environmentInstance.listPackages();
                  }),
               );
            } else {
               // Fallback to config file if database is empty
               await Promise.all(
                  environmentManifest.environments.map(async (environment) => {
                     await this.addEnvironment(
                        {
                           name: environment.name,
                           resource: `${API_PREFIX}/environments/${environment.name}`,
                           connections: environment.connections,
                           packages: environment.packages,
                        },
                        true,
                     );
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
      } catch (error) {
         markNotReady();
         const errorData = this.extractErrorDataFromError(error);
         logger.error("Error initializing environment store", errorData);
         process.exit(1);
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
      const existingProject =
         await repository.getEnvironmentByName(environmentName);

      let dbEnvironment: { id: string; name: string };
      if (existingProject) {
         const updateData = {
            description: environmentDescription,
            metadata: environment.metadata || {},
         };

         await repository.updateEnvironment(existingProject.id, updateData);
         dbEnvironment = { id: existingProject.id, name: environmentName };
      } else {
         dbEnvironment = await repository.createEnvironment(environmentData);
      }

      // Initialize DuckLake manifest storage if configured on the environment.
      const materializationStorage = environment.metadata
         ?.materializationStorage as
         | { catalogUrl?: string; dataPath?: string }
         | undefined;
      if (
         materializationStorage?.catalogUrl &&
         materializationStorage?.dataPath
      ) {
         await this.storageManager.initializeDuckLakeForEnvironment(
            dbEnvironment.id,
            {
               catalogUrl: materializationStorage.catalogUrl,
               dataPath: materializationStorage.dataPath,
            },
         );
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
            logger.error(`Failed to sync package ${pkg.name}:`, error);
            throw error;
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
         throw error;
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
      const status = {
         timestamp: Date.now(),
         environments: [] as Array<components["schemas"]["Environment"]>,
         initialized: this.isInitialized,
         frozenConfig: isPublisherConfigFrozen(this.serverRootPath),
         operationalState:
            getOperationalState() as components["schemas"]["ServerStatus"]["operationalState"],
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
      return status;
   }

   public async getEnvironment(
      environmentName: string,
      reload: boolean = false,
   ): Promise<Environment> {
      await this.finishedInitialization;

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
      const environmentName = environment.name;
      if (!environmentName) {
         throw new Error("Environment name is required");
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
      if (hasPackages) {
         const packagesToProcess =
            environment?.packages || environmentConfig?.packages || [];
         absoluteEnvironmentPath = await this.loadEnvironmentIntoDisk(
            environmentName,
            packagesToProcess,
         );
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

      if (!newEnvironment.metadata) newEnvironment.metadata = {};
      newEnvironment.metadata.location = absoluteEnvironmentPath;

      this.environments.set(environmentName, newEnvironment);

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

      const zip = new AdmZip(absoluteEnvironmentPath);
      zip.extractAllTo(unzippedEnvironmentPath, true);

      return unzippedEnvironmentPath;
   }

   public async updateEnvironment(environment: ApiEnvironment) {
      await this.finishedInitialization;
      if (this.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }
      validateEnvironmentAzureUrls(environment);
      const environmentName = environment.name;
      if (!environmentName) {
         throw new Error("Environment name is required");
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
      const environment = this.environments.get(environmentName);
      if (!environment) {
         return;
      }

      const environmentPath = environment.metadata?.location;

      // Close all connections before removing the environment
      environment.closeAllConnections();

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
      const environmentName = environment.name;
      if (!environmentName) {
         throw new Error("Environment name is required");
      }
      const absoluteEnvironmentPath = `${this.serverRootPath}/${PUBLISHER_DATA_DIR}/${environmentName}`;
      await fs.promises.mkdir(absoluteEnvironmentPath, { recursive: true });
      if (environment.readme) {
         await fs.promises.writeFile(
            path.join(absoluteEnvironmentPath, "README.md"),
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

   private async loadEnvironmentIntoDisk(
      environmentName: string,
      packages: ApiEnvironment["packages"],
   ) {
      const absoluteTargetPath = `${this.serverRootPath}/${PUBLISHER_DATA_DIR}/${environmentName}`;

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
         const tempDownloadPath = `${absoluteTargetPath}/.temp_${locationHash}`;
         await fs.promises.mkdir(tempDownloadPath, { recursive: true });
         logger.info(`Created temporary directory: ${tempDownloadPath}`);
         try {
            // Use the existing download method for all locations
            await this.downloadOrMountLocation(
               groupedLocation,
               tempDownloadPath,
               environmentName,
               "shared",
            );
            // Extract each package from the downloaded content
            for (const _package of packagesForLocation) {
               const packageDir = _package.name;
               const absolutePackagePath = `${absoluteTargetPath}/${packageDir}`;
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
                        sourcePath = path.join(
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
                     sourcePath = _package.location;
                  } else {
                     sourcePath = path.join(tempDownloadPath, groupedLocation);
                  }
               }

               const sourceExists = await fs.promises
                  .access(sourcePath)
                  .then(() => true)
                  .catch(() => false);

               if (sourceExists) {
                  // Copy the specific directory
                  await fs.promises.mkdir(absolutePackagePath, {
                     recursive: true,
                  });
                  await fs.promises.cp(sourcePath, absolutePackagePath, {
                     recursive: true,
                  });
                  logger.info(
                     `Extracted package "${packageDir}" from ${groupedLocation.startsWith("https://github.com/") && _package.location.includes("/tree/") ? "GitHub subdirectory" : "shared download"}`,
                  );
               } else {
                  // If source doesn't exist, copy the entire download as the package
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
            }
         } catch (error) {
            const errorData = this.extractErrorDataFromError(error);
            logger.error(
               `Failed to download or mount location "${groupedLocation}"`,
               errorData,
            );
            throw new PackageNotFoundError(
               `Failed to download or mount location: ${groupedLocation}`,
            );
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

      return absoluteTargetPath;
   }

   private async downloadOrMountLocation(
      location: string,
      targetPath: string,
      environmentName: string,
      packageName: string,
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
            await this.downloadGitHubDirectory(location, targetPath);
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
         const packagePath: string = path.isAbsolute(location)
            ? location
            : path.join(this.serverRootPath, location);
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
            const relativeFilePath = file.name.replace(prefix, "");
            const absoluteFilePath = isCompressedFile
               ? absoluteDirPath
               : path.join(absoluteDirPath, relativeFilePath);
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
            const relativeFilePath = key.replace(prefix, "");
            if (!relativeFilePath || relativeFilePath.endsWith("/")) {
               return;
            }
            const absoluteFilePath = path.join(
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

   async downloadGitHubDirectory(githubUrl: string, absoluteDirPath: string) {
      // First we'll clone the repo without the additional path
      // E.g. we're removing `/tree/main/imdb` from https://github.com/credibledata/malloy-samples/tree/main/imdb
      const githubInfo = this.parseGitHubUrl(githubUrl);
      if (!githubInfo) {
         throw new Error(`Invalid GitHub URL: ${githubUrl}`);
      }
      const { owner, repoName, packagePath } = githubInfo;
      const cleanPackagePath = packagePath?.replace("/tree/main", "") || "";

      // We'll make sure whatever was in absoluteDirPath is removed,
      // so we have a nice a clean directory where we can clone the repo
      await fs.promises.rm(absoluteDirPath, {
         recursive: true,
         force: true,
      });
      await fs.promises.mkdir(absoluteDirPath, { recursive: true });
      const repoUrl = `https://github.com/${owner}/${repoName}`;

      // We'll clone the repo into absoluteDirPath
      await new Promise<void>((resolve, reject) => {
         simpleGit().clone(repoUrl, absoluteDirPath, {}, (err) => {
            if (err) {
               const errorData = this.extractErrorDataFromError(err);
               logger.error(
                  `Failed to clone GitHub repository "${repoUrl}"`,
                  errorData,
               );
               reject(err);
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
      const packageFullPath = path.join(absoluteDirPath, cleanPackagePath);

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
            await fs.promises.rm(path.join(absoluteDirPath, entry), {
               recursive: true,
               force: true,
            });
         }
      }

      // Now, move the contents of packageFullPath (/var/publisher/asd123/imdb) up to absoluteDirPath (/var/publisher/asd123)
      const packageContents = await fs.promises.readdir(packageFullPath);
      for (const entry of packageContents) {
         await fs.promises.rename(
            path.join(packageFullPath, entry),
            path.join(absoluteDirPath, entry),
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
