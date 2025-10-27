import { BaseConnection } from "@malloydata/malloy/connection";
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
import { createProjectConnections, InternalConnection } from "./connection";
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

export class Project {
   private packages: Map<string, Package> = new Map();
   private packageMutexes = new Map<string, Mutex>();
   private packageStatuses: Map<string, PackageInfo> = new Map();
   private malloyConnections: Map<string, BaseConnection>;
   private apiConnections: ApiConnection[];
   private projectPath: string;
   private projectName: string;
   public metadata: ApiProject;

   constructor(
      projectName: string,
      projectPath: string,
      malloyConnections: Map<string, BaseConnection>,
      apiConnections: InternalConnection[],
   ) {
      this.projectName = projectName;
      this.projectPath = projectPath;
      this.malloyConnections = malloyConnections;
      this.apiConnections = apiConnections;
      this.metadata = {
         resource: `${API_PREFIX}/projects/${this.projectName}`,
         name: this.projectName,
         location: this.projectPath,
      };
      void this.reloadProjectMetadata();
   }

   public async update(payload: ApiProject) {
      if (payload.readme !== undefined) {
         this.metadata.readme = payload.readme;
      }

      // Handle connections update
      // TODO: Update project connections should have its own API endpoint
      if (payload.connections) {
         logger.info(
            `Updating ${payload.connections.length} connections for project ${this.projectName}`,
         );

         // Reload connections with full config
         const { malloyConnections, apiConnections } =
            await createProjectConnections(
               payload.connections,
               this.projectPath,
            );

         // Update the project's connection maps
         this.malloyConnections = malloyConnections;
         this.apiConnections = apiConnections;

         logger.info(
            `Successfully updated connections for project ${this.projectName}`,
            {
               malloyConnections: malloyConnections.size,
               apiConnections: apiConnections.length,
               internalConnections: apiConnections.length,
            },
         );
      }

      return this;
   }

   static async create(
      projectName: string,
      projectPath: string,
      connections: ApiConnection[],
   ): Promise<Project> {
      if (!(await fs.promises.stat(projectPath)).isDirectory()) {
         throw new ProjectNotFoundError(
            `Project path ${projectPath} not found`,
         );
      }

      logger.info(`Creating project with connection configuration`);
      const { malloyConnections, apiConnections } =
         await createProjectConnections(connections, projectPath);

      logger.info(
         `Loaded ${malloyConnections.size + apiConnections.length} connections for project ${projectName}`,
         {
            malloyConnections,
            apiConnections,
         },
      );

      return new Project(
         projectName,
         projectPath,
         malloyConnections,
         apiConnections,
      );
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
         resource: `${API_PREFIX}/projects/${this.projectName}`,
         name: this.projectName,
         readme: readme,
      };
      return this.metadata;
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

   public getMalloyConnection(connectionName: string): BaseConnection {
      const connection = this.malloyConnections.get(connectionName);
      if (!connection) {
         throw new ConnectionNotFoundError(
            `Connection ${connectionName} not found`,
         );
      }
      return connection;
   }

   public async listPackages(): Promise<ApiPackage[]> {
      logger.info("Listing packages", { projectPath: this.projectPath });
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
         await packageMutex.waitForUnlock();
         const existingPackage = this.packages.get(packageName);
         if (existingPackage) {
            return existingPackage;
         }
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
            const _package = await Package.create(
               this.projectName,
               packageName,
               path.join(this.projectPath, packageName),
               this.malloyConnections,
               this.apiConnections,
            );
            this.packages.set(packageName, _package);

            // Set package status to serving
            this.setPackageStatus(packageName, PackageStatus.SERVING);

            return _package;
         } catch (error) {
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
         !(await fs.promises.stat(packagePath)).isDirectory()
      ) {
         throw new PackageNotFoundError(`Package ${packageName} not found`);
      }
      logger.info(
         `Adding package ${packageName} to project ${this.projectName}`,
         {
            packagePath,
            malloyConnections: this.malloyConnections,
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
               this.malloyConnections,
               this.apiConnections,
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

   public async serialize(): Promise<ApiProject> {
      return {
         ...this.metadata,
         connections: this.listApiConnections(),
         packages: await this.listPackages(),
      };
   }
}
