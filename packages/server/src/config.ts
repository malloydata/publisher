import fs from "fs";
import path from "path";
import { components } from "./api";
import { API_PREFIX, PUBLISHER_CONFIG_NAME } from "./constants";

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
   const rawConfig = JSON.parse(fs.readFileSync(publisherConfigPath, "utf8"));

   if (
      rawConfig.projects &&
      typeof rawConfig.projects === "object" &&
      !Array.isArray(rawConfig.projects)
   ) {
      throw new Error(
         "Config has changed. Please update your config to the new format.",
      );
   }

   return rawConfig as PublisherConfig;
};

export const isPublisherConfigFrozen = (serverRoot: string) => {
   const publisherConfig = getPublisherConfig(serverRoot);
   return Boolean(publisherConfig.frozenConfig);
};

export const getConnectionsFromPublisherConfig = (
   serverRoot: string,
   projectName: string,
): Connection[] => {
   const publisherConfig = getPublisherConfig(serverRoot);
   const project = publisherConfig.projects.find((p) => p.name === projectName);
   return project?.connections || [];
};

export const convertConnectionsToApiConnections = (
   connections: Connection[],
): ApiConnection[] => {
   return connections.map((conn) => ({
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
   return {
      frozenConfig: rawConfig.frozenConfig,
      projects: rawConfig.projects.map((project) => ({
         name: project.name,
         packages: project.packages,
         connections: convertConnectionsToApiConnections(
            project.connections || [],
         ),
      })),
   };
};
