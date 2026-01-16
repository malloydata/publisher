import fs from "fs";
import path from "path";
import { components } from "./api";
import { API_PREFIX, PUBLISHER_CONFIG_NAME } from "./constants";
import { logger } from "./logger";

type FilesystemPath = `./${string}` | `../${string}` | `/${string}`;
type GcsPath = `gs://${string}`;
type ApiConnection = components["schemas"]["Connection"];

export type Package = {
   name: string;
   location: FilesystemPath | GcsPath;
};

export type Connection = {
   name: string;
   type: string;
};

export type Project = {
   name: string;
   packages: Package[];
   connections?: Connection[];
};

export type PublisherConfig = {
   frozenConfig: boolean;
   projects: Project[];
};

export type ProcessedProject = {
   name: string;
   packages: Package[];
   connections: ApiConnection[];
};

export type ProcessedPublisherConfig = {
   frozenConfig: boolean;
   projects: ProcessedProject[];
};

export const getPublisherConfig = (serverRoot: string): PublisherConfig => {
   const publisherConfigPath = path.join(serverRoot, PUBLISHER_CONFIG_NAME);
   if (!fs.existsSync(publisherConfigPath)) {
      return {
         frozenConfig: false,
         projects: [],
      };
   }

   let rawConfig: unknown;
   try {
      const fileContent = fs.readFileSync(publisherConfigPath, "utf8");
      rawConfig = JSON.parse(fileContent);
   } catch (error) {
      logger.error(
         `Failed to parse ${PUBLISHER_CONFIG_NAME}. Using default empty config.`,
         { error },
      );
      return {
         frozenConfig: false,
         projects: [],
      };
   }

   if (
      rawConfig &&
      typeof rawConfig === "object" &&
      "projects" in rawConfig &&
      rawConfig.projects &&
      typeof rawConfig.projects === "object" &&
      !Array.isArray(rawConfig.projects)
   ) {
      logger.error(
         `Invalid ${PUBLISHER_CONFIG_NAME}: projects must be an array. Using default empty config.`,
      );
      return {
         frozenConfig: false,
         projects: [],
      };
   }

   // Ensure projects is an array
   let projects: unknown[] = [];
   if (
      rawConfig &&
      typeof rawConfig === "object" &&
      "projects" in rawConfig &&
      Array.isArray((rawConfig as { projects: unknown }).projects)
   ) {
      projects = (rawConfig as { projects: unknown[] }).projects;
   }

   let frozenConfig = false;
   if (
      rawConfig &&
      typeof rawConfig === "object" &&
      "frozenConfig" in rawConfig
   ) {
      frozenConfig = Boolean(
         (rawConfig as { frozenConfig: unknown }).frozenConfig,
      );
   }

   return {
      frozenConfig,
      projects,
   } as PublisherConfig;
};

export const isPublisherConfigFrozen = (serverRoot: string) => {
   try {
      const publisherConfig = getPublisherConfig(serverRoot);
      return Boolean(publisherConfig.frozenConfig);
   } catch (error) {
      logger.error(
         `Error checking if ${PUBLISHER_CONFIG_NAME} is frozen. Defaulting to false.`,
         { error },
      );
      return false;
   }
};

export const getConnectionsFromPublisherConfig = (
   serverRoot: string,
   projectName: string,
): Connection[] => {
   try {
      const publisherConfig = getPublisherConfig(serverRoot);
      if (!Array.isArray(publisherConfig.projects)) {
         return [];
      }
      const project = publisherConfig.projects.find(
         (p) => p && p.name === projectName,
      );
      return Array.isArray(project?.connections) ? project.connections : [];
   } catch (error) {
      logger.error(
         `Error getting connections for project "${projectName}" from ${PUBLISHER_CONFIG_NAME}`,
         { error },
      );
      return [];
   }
};

export const convertConnectionsToApiConnections = (
   connections: Connection[],
): ApiConnection[] => {
   if (!Array.isArray(connections)) {
      return [];
   }

   return connections
      .filter((conn) => {
         if (!conn || typeof conn !== "object") {
            return false;
         }
         if (!conn.name || typeof conn.name !== "string") {
            logger.warn(
               `Invalid connection: missing or invalid "name" field. Skipping.`,
               { connection: conn },
            );
            return false;
         }
         if (!conn.type || typeof conn.type !== "string") {
            logger.warn(
               `Invalid connection "${conn.name}": missing or invalid "type" field. Skipping.`,
            );
            return false;
         }
         return true;
      })
      .map((conn) => ({
         ...conn,
         name: conn.name,
         type: conn.type as ApiConnection["type"],
         resource: `${API_PREFIX}/connections/${conn.name}`,
      }));
};

export const getProcessedPublisherConfig = (
   serverRoot: string,
): ProcessedPublisherConfig => {
   const rawConfig = getPublisherConfig(serverRoot);

   // Ensure projects is an array
   if (!Array.isArray(rawConfig.projects)) {
      logger.warn(
         `Invalid ${PUBLISHER_CONFIG_NAME}: projects must be an array. Using empty array.`,
      );
      return {
         frozenConfig: rawConfig.frozenConfig ?? false,
         projects: [],
      };
   }

   // Filter and validate projects, skipping invalid ones
   const validProjects: ProcessedProject[] = [];
   for (const project of rawConfig.projects) {
      if (!project || typeof project !== "object") {
         logger.warn(
            `Invalid project in ${PUBLISHER_CONFIG_NAME}: project must be an object. Skipping.`,
         );
         continue;
      }

      if (!project.name || typeof project.name !== "string") {
         logger.warn(
            `Invalid project in ${PUBLISHER_CONFIG_NAME}: missing or invalid "name" field. Skipping project.`,
            { project },
         );
         continue;
      }

      if (!Array.isArray(project.packages)) {
         logger.warn(
            `Invalid project "${project.name}" in ${PUBLISHER_CONFIG_NAME}: missing or invalid "packages" field (must be an array). Skipping project.`,
         );
         continue;
      }

      // Validate packages have required fields
      const validPackages = project.packages.filter((pkg) => {
         if (!pkg || typeof pkg !== "object") {
            logger.warn(
               `Invalid package in project "${project.name}": package must be an object. Skipping.`,
            );
            return false;
         }
         if (!pkg.name || typeof pkg.name !== "string") {
            logger.warn(
               `Invalid package in project "${project.name}": missing or invalid "name" field. Skipping.`,
            );
            return false;
         }
         if (!pkg.location || typeof pkg.location !== "string") {
            logger.warn(
               `Invalid package "${pkg.name}" in project "${project.name}": missing or invalid "location" field. Skipping.`,
            );
            return false;
         }
         return true;
      });

      if (validPackages.length === 0) {
         logger.warn(
            `Project "${project.name}" has no valid packages. Skipping project.`,
         );
         continue;
      }

      validProjects.push({
         name: project.name,
         packages: validPackages,
         connections: convertConnectionsToApiConnections(
            project.connections || [],
         ),
      });
   }

   return {
      frozenConfig: rawConfig.frozenConfig ?? false,
      projects: validProjects,
   };
};
