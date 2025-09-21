import { GetObjectCommand, S3 } from "@aws-sdk/client-s3";
import { Storage } from "@google-cloud/storage";
import AdmZip from "adm-zip";
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
import { API_PREFIX, PUBLISHER_CONFIG_NAME, publisherPath } from "../constants";
import {
   FrozenConfigError,
   PackageNotFoundError,
   ProjectNotFoundError,
} from "../errors";
import { logger } from "../logger";
import { PackageStatus, Project } from "./project";
type ApiProject = components["schemas"]["Project"];

export class ProjectStore {
   public serverRootPath: string;
   private projects: Map<string, Project> = new Map();
   public publisherConfigIsFrozen: boolean;
   public finishedInitialization: Promise<void>;
   private isInitialized: boolean = false;
   private s3Client = new S3({
      followRegionRedirects: true,
   });
   private gcsClient: Storage;

   constructor(serverRootPath: string) {
      this.serverRootPath = serverRootPath;
      this.gcsClient = new Storage();

      this.finishedInitialization = this.initialize();
   }

   private async initialize() {
      const initialTime = performance.now();
      try {
         this.publisherConfigIsFrozen = isPublisherConfigFrozen(
            this.serverRootPath,
         );
         const projectManifest = await ProjectStore.reloadProjectManifest(
            this.serverRootPath,
         );
         await this.cleanupAndCreatePublisherPath();
         logger.info(`Initializing project store.`);
         await Promise.all(
            projectManifest.projects.map(async (project) => {
               logger.info(`Adding project "${project.name}"`);
               const projectInstance = await this.addProject(
                  {
                     name: project.name,
                     resource: `${API_PREFIX}/projects/${project.name}`,
                     connections: project.connections,
                  },
                  true,
               );
               return projectInstance.listPackages();
            }),
         );
         this.isInitialized = true;
         logger.info(
            `Project store successfully initialized in ${performance.now() - initialTime}ms`,
         );
      } catch (error) {
         logger.error("Error initializing project store", { error });
         console.error(error);
         process.exit(1);
      }
   }

   private async cleanupAndCreatePublisherPath() {
      logger.info(`Cleaning up publisher path ${publisherPath}`);
      try {
         await fs.promises.rm(publisherPath, { recursive: true, force: true });
      } catch (error) {
         if ((error as NodeJS.ErrnoException).code === "EACCES") {
            logger.warn(
               `Permission denied, skipping cleanup of publisher path ${publisherPath}`,
            );
         } else {
            throw error;
         }
      }
      await fs.promises.mkdir(publisherPath, { recursive: true });
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
      let project = this.projects.get(projectName);
      if (project === undefined || reload) {
         const projectManifest = await ProjectStore.reloadProjectManifest(
            this.serverRootPath,
         );
         const projectConfig = projectManifest.projects.find(
            (p) => p.name === projectName,
         );
         const projectPath =
            project?.metadata.location || projectConfig?.packages[0]?.location;
         if (!projectPath) {
            throw new ProjectNotFoundError(
               `Project "${projectName}" could not be resolved to a path.`,
            );
         }
         project = await this.addProject({
            name: projectName,
            resource: `${API_PREFIX}/projects/${projectName}`,
            connections: projectConfig?.connections || [],
         });
      }
      return project;
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
         logger.info(`Project ${projectName} already exists, updating it`);
         const updatedProject = await existingProject.update(project);
         this.projects.set(projectName, updatedProject);
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
         this.serverRootPath,
      );
      this.projects.set(projectName, newProject);
      projectConfig?.packages.forEach((_package) => {
         newProject.setPackageStatus(_package.name, PackageStatus.SERVING);
      });
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
      return updatedProject;
   }

   public async deleteProject(projectName: string) {
      await this.finishedInitialization;
      if (this.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }
      const project = this.projects.get(projectName);
      if (!project) {
         return;
      }
      this.projects.delete(projectName);
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
      const absoluteProjectPath = `${publisherPath}/${projectName}`;
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
      projectPath: string,
      packages: ApiProject["packages"],
   ) {
      const absoluteTargetPath = `${publisherPath}/${projectPath}`;

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
         // Create a temporary directory for the shared download
         const tempDownloadPath = `${absoluteTargetPath}/.temp_${Buffer.from(
            groupedLocation,
         )
            .toString("base64")
            .replace(/[^a-zA-Z0-9]/g, "")}`;
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
