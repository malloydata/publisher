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
   ProcessedProject,
   ProcessedPublisherConfig,
} from "../config";
import {
   API_PREFIX,
   PUBLISHER_CONFIG_NAME,
   PUBLISHER_DATA_DIR,
} from "../constants";
import {
   FrozenConfigError,
   PackageNotFoundError,
   ProjectNotFoundError,
} from "../errors";
import { logger } from "../logger";
import { Connection } from "../storage/DatabaseInterface";
import { StorageConfig, StorageManager } from "../storage/StorageManager";
import { PackageStatus, Project } from "./project";
type ApiProject = components["schemas"]["Project"];

export class ProjectStore {
   public serverRootPath: string;
   private projects: Map<string, Project> = new Map();
   private projectMutexes = new Map<string, Mutex>();
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

         const projectManifest = await ProjectStore.reloadProjectManifest(
            this.serverRootPath,
         );

         await this.cleanupAndCreatePublisherPath();

         const repository = this.storageManager.getRepository();

         if (reInit) {
            // Load projects from config file
            await Promise.all(
               projectManifest.projects.map(async (project) => {
                  await this.addProject(
                     {
                        name: project.name,
                        resource: `${API_PREFIX}/projects/${project.name}`,
                        connections: project.connections,
                        packages: project.packages,
                     },
                     true,
                  );
               }),
            );
         } else {
            // Load existing projects from database
            const existingProjects = await repository.listProjects();

            if (existingProjects.length > 0) {
               // Load projects from database
               await Promise.all(
                  existingProjects.map(async (dbProject) => {
                     // Check if project files exist on disk
                     const projectExists = await fs.promises
                        .access(dbProject.path)
                        .then(() => true)
                        .catch(() => false);

                     if (!projectExists) {
                        // Try to find in config and reload
                        const projectConfig = projectManifest.projects.find(
                           (p) => p.name === dbProject.name,
                        );

                        if (projectConfig) {
                           const projectInstance = await this.addProject(
                              {
                                 name: projectConfig.name,
                                 resource: `${API_PREFIX}/projects/${projectConfig.name}`,
                                 connections: projectConfig.connections,
                                 packages: projectConfig.packages,
                              },
                              true,
                           );

                           // Update database with new path
                           await repository.updateProject(dbProject.id, {
                              path: projectInstance.metadata.location,
                           });

                           return projectInstance.listPackages();
                        } else {
                           logger.error(
                              `Project "${dbProject.name}" not found in config and files missing`,
                           );
                           return;
                        }
                     }

                     // Get connections from database
                     const connections = await repository.listConnections(
                        dbProject.id,
                     );

                     const projectInstance = await Project.create(
                        dbProject.name,
                        dbProject.path,
                        connections.map((conn) => ({
                           name: conn.name,
                           type: conn.type,
                           resource: `${API_PREFIX}/connections/${conn.name}`,
                           ...conn.config,
                        })),
                     );

                     this.projects.set(dbProject.name, projectInstance);

                     // Get packages from database
                     const packages = await repository.listPackages(
                        dbProject.id,
                     );
                     packages.forEach((pkg) => {
                        projectInstance.setPackageStatus(
                           pkg.name,
                           PackageStatus.SERVING,
                        );
                     });

                     return projectInstance.listPackages();
                  }),
               );
            } else {
               // Fallback to config file if database is empty
               await Promise.all(
                  projectManifest.projects.map(async (project) => {
                     await this.addProject(
                        {
                           name: project.name,
                           resource: `${API_PREFIX}/projects/${project.name}`,
                           connections: project.connections,
                           packages: project.packages,
                        },
                        true,
                     );
                  }),
               );
            }
         }

         this.isInitialized = true;
         logger.info(
            `Project store successfully initialized in ${performance.now() - initialTime}ms`,
         );
      } catch (error) {
         logger.error("Error initializing project store", { error });
         process.exit(1);
      }
   }

   public async addProjectToDatabase(project: Project): Promise<void> {
      if (!project) {
         logger.error("Cannot sync: project is null or undefined");
         return;
      }

      const projectName = project.metadata?.name;
      if (!projectName) {
         throw new Error("Project name is required but not found");
      }

      const repository = this.storageManager.getRepository();

      // Sync project metadata
      const dbProject = await this.addProjectMetadata(project, repository);

      // Sync connections
      await this.addConnections(project, dbProject.id, repository);

      // Sync packages
      await this.addPackages(project, dbProject.id, repository);

      logger.info(`Synced project "${projectName}" to database`);
   }

   public async deleteProjectFromDatabase(projectName: string): Promise<void> {
      const repository = this.storageManager.getRepository();

      // Get the project from database
      const dbProject = await repository.getProjectByName(projectName);

      if (!dbProject) {
         logger.error(`Project "${projectName}" not found in database`);
         return;
      }

      // Delete the project (this will cascade delete connections and packages)
      await repository.deleteProject(dbProject.id);
      logger.info(`Deleted project "${projectName}" from database`);
   }

   private async addProjectMetadata(
      project: Project,
      repository: ReturnType<typeof this.storageManager.getRepository>,
   ): Promise<{ id: string; name: string }> {
      const projectName = project.metadata?.name;
      if (!projectName) {
         throw new Error("Project name is required but not found");
      }
      const projectPath = project.metadata?.location || "";
      const projectDescription = project.metadata?.readme;

      const projectData = {
         name: projectName,
         path: projectPath,
         description: projectDescription,
         metadata: project.metadata || {},
      };
      const existingProject = await repository.getProjectByName(projectName);

      if (existingProject) {
         const updateData = {
            description: projectDescription,
            metadata: project.metadata || {},
         };

         await repository.updateProject(existingProject.id, updateData);
         return { id: existingProject.id, name: projectName };
      } else {
         return await repository.createProject(projectData);
      }
   }

   private async addPackages(
      project: Project,
      projectId: string,
      repository: ReturnType<typeof this.storageManager.getRepository>,
   ): Promise<void> {
      const packages = await project.listPackages();

      // Sync each package
      for (const pkg of packages) {
         if (!pkg.name) {
            logger.warn("Skipping package with undefined name");
            continue;
         }

         await this.addPackage(pkg, projectId, repository);
      }
   }

   private async addPackage(
      pkg: components["schemas"]["Package"],
      projectId: string,
      repository: ReturnType<typeof this.storageManager.getRepository>,
   ): Promise<void> {
      const pkgs = pkg as {
         name: string;
         description?: string;
         manifestPath?: string;
         metadata?: Record<string, unknown>;
      };

      const packageData = {
         projectId,
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
               projectId,
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
      projectId: string,
      packageData: {
         description: string | undefined;
         manifestPath: string;
         metadata: Record<string, unknown>;
      },
      repository: ReturnType<typeof this.storageManager.getRepository>,
   ): Promise<void> {
      const existingPackage = await repository.getPackageByName(
         projectId,
         packageName,
      );

      if (existingPackage) {
         await repository.updatePackage(existingPackage.id, packageData);
         logger.info(`Updated existing package: ${packageName}`);
      }
   }

   private async addConnections(
      project: Project,
      projectId: string,
      repository: ReturnType<typeof this.storageManager.getRepository>,
   ): Promise<void> {
      try {
         const connections = project.listApiConnections();
         // Add/update connections
         for (const conn of connections) {
            if (!conn.name) {
               logger.warn("Skipping connection with undefined name");
               continue;
            }

            // Check if connection exists
            const existingConn = await repository.getConnectionByName(
               projectId,
               conn.name,
            );

            if (existingConn) {
               await this.updateConnection(conn, projectId, repository);
            } else {
               await this.addConnection(conn, projectId, repository);
            }
         }
      } catch (err: unknown) {
         const error = err as Error;
         const projectName = project.metadata?.name;
         logger.error(`Error syncing connections for "${projectName}":`, error);
      }
   }

   public async addConnection(
      conn: ReturnType<Project["listApiConnections"]>[number],
      projectId: string,
      repository: ReturnType<typeof this.storageManager.getRepository>,
   ): Promise<void> {
      if (!conn.name) {
         logger.warn("Skipping connection with undefined name");
         return;
      }

      const connectionData = {
         projectId,
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
      conn: ReturnType<Project["listApiConnections"]>[number],
      projectId: string,
      repository: ReturnType<typeof this.storageManager.getRepository>,
   ): Promise<void> {
      if (!conn.name) {
         throw new Error("Connection name is required for update");
      }

      const existingConn = await repository.getConnectionByName(
         projectId,
         conn.name,
      );

      if (!existingConn) {
         logger.error(`Connection "${conn.name}" not found in project`);
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
      projectName: string,
      packageName: string,
   ): Promise<void> {
      const project = await this.getProject(projectName, false);
      const repository = this.storageManager.getRepository();

      // Get the project ID from database
      const dbProject = await repository.getProjectByName(projectName);

      if (!dbProject) {
         logger.error(`Project "${projectName}" not found in database`);
         throw new Error(`Project "${projectName}" not found in database`);
      }

      // Get the package from the project
      const packages = await project.listPackages();
      const pkg = packages.find((p) => p.name === packageName);

      if (!pkg) {
         logger.warn(`Package "${packageName}" not found in project`);
         return;
      }

      // Sync the specific package
      await this.addPackage(pkg, dbProject.id, repository);
      logger.info(`Synced package "${packageName}" to database`);
   }

   /**
    * Delete a package from the database
    */
   public async deletePackageFromDatabase(
      projectName: string,
      packageName: string,
   ): Promise<void> {
      const repository = this.storageManager.getRepository();

      // Get the project ID from database
      const dbProject = await repository.getProjectByName(projectName);

      if (!dbProject) {
         logger.error(`Project "${projectName}" not found in database`);
         return;
      }

      // Find and delete the package
      const existingPackage = await repository.getPackageByName(
         dbProject.id,
         packageName,
      );

      if (existingPackage) {
         await repository.deletePackage(existingPackage.id);
         logger.info(`Deleted package "${packageName}" from database`);
      }
   }

   private async cleanupAndCreatePublisherPath() {
      const reInit = process.env.INITIALIZE_STORAGE === "true";

      if (reInit) {
         const uploadDocsPath = path.join(
            this.serverRootPath,
            PUBLISHER_DATA_DIR,
         );
         logger.info(
            `Re init: Cleaning up upload documents path ${uploadDocsPath}`,
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

   public async listProjects(skipInitializationCheck: boolean = false) {
      if (!skipInitializationCheck) {
         await this.finishedInitialization;
      }
      return Promise.all(
         Array.from(this.projects.values()).map((project) =>
            project.serialize(),
         ),
      );
   }

   public async getStatus() {
      const status = {
         timestamp: Date.now(),
         projects: [] as Array<components["schemas"]["Project"]>,
         initialized: this.isInitialized,
         frozenConfig: isPublisherConfigFrozen(this.serverRootPath),
      };

      const projects = await this.listProjects(true);

      await Promise.all(
         projects.map(async (project) => {
            try {
               const packages = project.packages;
               const connections = project.connections;

               logger.info(`Project ${project.name} status:`, {
                  connectionsCount: project.connections?.length || 0,
                  packagesCount: packages?.length || 0,
               });

               const _connections = connections?.map((connection) => {
                  return {
                     ...connection,
                     attributes: undefined,
                  };
               });

               const _project = {
                  ...project,
                  connections: _connections,
               };
               project.connections = _connections;
               status.projects.push(_project);
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

   public async getProject(
      projectName: string,
      reload: boolean = false,
   ): Promise<Project> {
      await this.finishedInitialization;

      // Check if project is already loaded first
      const project = this.projects.get(projectName);
      if (project !== undefined && !reload) {
         return project;
      }

      // We need to acquire the mutex to prevent concurrent requests from creating the
      // project multiple times.
      let projectMutex = this.projectMutexes.get(projectName);
      if (projectMutex?.isLocked()) {
         await projectMutex.waitForUnlock();
         const existingProject = this.projects.get(projectName);
         if (existingProject && !reload) {
            return existingProject;
         }
      }
      projectMutex = new Mutex();
      this.projectMutexes.set(projectName, projectMutex);

      return projectMutex.runExclusive(async () => {
         // Double-check after acquiring mutex
         const existingProject = this.projects.get(projectName);
         if (existingProject !== undefined && !reload) {
            return existingProject;
         }

         const projectManifest = await ProjectStore.reloadProjectManifest(
            this.serverRootPath,
         );
         const projectConfig = projectManifest.projects.find(
            (p) => p.name === projectName,
         );
         const projectPath =
            existingProject?.metadata.location ||
            projectConfig?.packages[0]?.location;
         if (!projectPath) {
            throw new ProjectNotFoundError(
               `Project "${projectName}" could not be resolved to a path.`,
            );
         }
         return await this.addProject({
            name: projectName,
            resource: `${API_PREFIX}/projects/${projectName}`,
            connections: projectConfig?.connections || [],
         });
      });
   }

   public async addProject(
      project: ApiProject,
      skipInitialization: boolean = false,
   ) {
      if (!skipInitialization) {
         await this.finishedInitialization;
      }
      if (!skipInitialization && this.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }
      const projectName = project.name;
      if (!projectName) {
         throw new Error("Project name is required");
      }
      // Check if project already exists and update it instead of creating a new one
      const existingProject = this.projects.get(projectName);
      if (existingProject) {
         const updatedProject = await existingProject.update(project);
         this.projects.set(projectName, updatedProject);
         await this.addProjectToDatabase(updatedProject);
         return updatedProject;
      }
      const projectManifest = await ProjectStore.reloadProjectManifest(
         this.serverRootPath,
      );
      const projectConfig = projectManifest.projects.find(
         (p) => p.name === projectName,
      );
      const hasPackages =
         (project?.packages && project.packages.length > 0) ||
         (projectConfig?.packages && projectConfig.packages.length > 0);
      let absoluteProjectPath: string;
      if (hasPackages) {
         const packagesToProcess =
            project?.packages || projectConfig?.packages || [];
         absoluteProjectPath = await this.loadProjectIntoDisk(
            projectName,
            packagesToProcess,
         );
         if (absoluteProjectPath.endsWith(".zip")) {
            absoluteProjectPath = await this.unzipProject(absoluteProjectPath);
         }
      } else {
         absoluteProjectPath = await this.scaffoldProject(project);
      }
      const newProject = await Project.create(
         projectName,
         absoluteProjectPath,
         project.connections || [],
      );

      if (!newProject.metadata) newProject.metadata = {};
      newProject.metadata.location = absoluteProjectPath;

      this.projects.set(projectName, newProject);

      project?.packages?.forEach((_package) => {
         if (_package.name) {
            newProject.setPackageStatus(_package.name, PackageStatus.SERVING);
         }
      });

      await this.addProjectToDatabase(newProject);

      return newProject;
   }

   public async unzipProject(absoluteProjectPath: string) {
      logger.info(
         `Detected zip file at "${absoluteProjectPath}". Unzipping...`,
      );
      const unzippedProjectPath = absoluteProjectPath.replace(".zip", "");
      await fs.promises.rm(unzippedProjectPath, {
         recursive: true,
         force: true,
      });
      await fs.promises.mkdir(unzippedProjectPath, { recursive: true });

      const zip = new AdmZip(absoluteProjectPath);
      zip.extractAllTo(unzippedProjectPath, true);

      return unzippedProjectPath;
   }

   public async updateProject(project: ApiProject) {
      await this.finishedInitialization;
      if (this.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }
      const projectName = project.name;
      if (!projectName) {
         throw new Error("Project name is required");
      }
      const existingProject = this.projects.get(projectName);
      if (!existingProject) {
         throw new ProjectNotFoundError(`Project ${projectName} not found`);
      }
      const updatedProject = await existingProject.update(project);
      this.projects.set(projectName, updatedProject);
      await this.addProjectToDatabase(updatedProject);
      return updatedProject;
   }

   public async deleteProject(
      projectName: string,
   ): Promise<Project | undefined> {
      await this.finishedInitialization;
      if (this.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }
      const project = this.projects.get(projectName);
      if (!project) {
         return;
      }

      const projectPath = project.metadata?.location;
      this.projects.delete(projectName);
      await this.deleteProjectFromDatabase(projectName);
      if (projectPath) {
         try {
            await fs.promises.rm(projectPath, { recursive: true, force: true });
            logger.info(`Deleted project directory: ${projectPath}`);
         } catch (err) {
            logger.error("Error removing project directory", { error: err });
         }
      }

      return project;
   }

   public static async reloadProjectManifest(
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
            return { frozenConfig: false, projects: [] };
         } else {
            // If publisher.config.json is missing, generate the manifest from directories
            try {
               const entries = await fs.promises.readdir(serverRootPath, {
                  withFileTypes: true,
               });
               const projects: ProcessedProject[] = [];
               for (const entry of entries) {
                  if (entry.isDirectory()) {
                     projects.push({
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
               return { frozenConfig: false, projects };
            } catch (lsError) {
               logger.error(`Error listing directories in ${serverRootPath}`, {
                  error: lsError,
               });
               return { frozenConfig: false, projects: [] };
            }
         }
      }
   }

   private async scaffoldProject(project: ApiProject) {
      const projectName = project.name;
      if (!projectName) {
         throw new Error("Project name is required");
      }
      const absoluteProjectPath = `${this.serverRootPath}/${PUBLISHER_DATA_DIR}/${projectName}`;
      await fs.promises.mkdir(absoluteProjectPath, { recursive: true });
      if (project.readme) {
         await fs.promises.writeFile(
            path.join(absoluteProjectPath, "README.md"),
            project.readme,
         );
      }
      return absoluteProjectPath;
   }

   private isLocalPath(location: string) {
      return (
         location.startsWith("./") ||
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

   private async loadProjectIntoDisk(
      projectName: string,
      packages: ApiProject["packages"],
   ) {
      const absoluteTargetPath = `${this.serverRootPath}/${PUBLISHER_DATA_DIR}/${projectName}`;

      await fs.promises.mkdir(absoluteTargetPath, { recursive: true });

      if (!packages || packages.length === 0) {
         throw new PackageNotFoundError(
            `No packages found for project ${projectName}`,
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
               projectName,
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
            logger.error(
               `Failed to download or mount location "${groupedLocation}"`,
               {
                  error,
               },
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
      projectName: string,
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
               projectName,
               targetPath,
               isCompressedFile,
            );
            return;
         } catch (error) {
            logger.error(`Failed to download GCS directory "${location}"`, {
               error,
            });
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
            logger.error(`Failed to clone GitHub repository "${location}"`, {
               error,
            });
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
            await this.downloadS3Directory(location, projectName, targetPath);
            return;
         } catch (error) {
            logger.error(`Failed to download S3 directory "${location}"`, {
               error,
            });
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
               projectName,
               packageName,
            );
            return;
         } catch (error) {
            logger.error(`Failed to mount local directory "${packagePath}"`, {
               error,
            });
            throw new PackageNotFoundError(
               `Failed to mount local directory: ${packagePath}`,
            );
         }
      }

      // If we get here, the path format is not supported
      const errorMsg = `Invalid package path: "${location}". Must be an absolute mounted path or a GCS/S3/GitHub URI.`;
      logger.error(errorMsg, { projectName, location });
      throw new PackageNotFoundError(errorMsg);
   }

   public async mountLocalDirectory(
      projectPath: string,
      absoluteTargetPath: string,
      projectName: string,
      packageName: string,
   ) {
      if (projectPath.endsWith(".zip")) {
         projectPath = await this.unzipProject(projectPath);
      }
      const projectDirExists = (
         await fs.promises.stat(projectPath)
      ).isDirectory();
      if (projectDirExists) {
         await fs.promises.rm(absoluteTargetPath, {
            recursive: true,
            force: true,
         });
         await fs.promises.mkdir(absoluteTargetPath, { recursive: true });
         await fs.promises.cp(projectPath, absoluteTargetPath, {
            recursive: true,
         });
      } else {
         throw new PackageNotFoundError(
            `Package ${packageName} for project ${projectName} not found in "${projectPath}"`,
         );
      }
   }

   async downloadGcsDirectory(
      gcsPath: string,
      projectName: string,
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
         throw new ProjectNotFoundError(
            `Project ${projectName} not found in ${gcsPath}`,
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
         await this.unzipProject(absoluteDirPath);
      }
      logger.info(`Downloaded GCS directory ${gcsPath} to ${absoluteDirPath}`);
   }

   async downloadS3Directory(
      s3Path: string,
      projectName: string,
      absoluteDirPath: string,
   ) {
      const trimmedPath = s3Path.slice(5);
      const [bucketName, ...prefixParts] = trimmedPath.split("/");
      const prefix = prefixParts.join("/");
      const objects = await this.s3Client.listObjectsV2({
         Bucket: bucketName,
         Prefix: prefix,
      });
      await fs.promises.rm(absoluteDirPath, { recursive: true, force: true });
      await fs.promises.mkdir(absoluteDirPath, { recursive: true });

      if (!objects.Contents || objects.Contents.length === 0) {
         throw new ProjectNotFoundError(
            `Project ${projectName} not found in ${s3Path}`,
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
               console.error(err);
               logger.error(`Failed to clone GitHub repository "${repoUrl}"`, {
                  error: err,
               });
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
}
