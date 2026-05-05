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

export type Environment = {
   name: string;
   packages: Package[];
   connections?: Connection[];
};

export type PublisherConfig = {
   frozenConfig: boolean;
   environments: Environment[];
};

export type ProcessedEnvironment = {
   name: string;
   packages: Package[];
   connections: ApiConnection[];
};

export type ProcessedPublisherConfig = {
   frozenConfig: boolean;
   environments: ProcessedEnvironment[];
};

function substituteEnvVars(value: string): string {
   const envVarPattern = /\$\{([A-Z_][A-Z0-9_]*)\}/g;

   return value.replace(envVarPattern, (_match, varName) => {
      const envValue = process.env[varName];

      if (envValue !== undefined) {
         return envValue;
      }

      throw new Error(
         `Environment variable '\${${varName}}' is not set in configuration file`,
      );
   });
}

function processConfigValue(value: unknown): unknown {
   if (typeof value === "string") {
      return substituteEnvVars(value);
   }

   if (Array.isArray(value)) {
      return value.map((item) => processConfigValue(item));
   }

   if (value !== null && typeof value === "object") {
      const result: Record<string, unknown> = {};
      for (const [key, val] of Object.entries(value)) {
         result[key] = processConfigValue(val);
      }
      return result;
   }

   return value;
}

export const getPublisherConfig = (serverRoot: string): PublisherConfig => {
   const publisherConfigPath = path.join(serverRoot, PUBLISHER_CONFIG_NAME);
   if (!fs.existsSync(publisherConfigPath)) {
      return {
         frozenConfig: false,
         environments: [],
      };
   }

   let rawConfig: unknown;
   try {
      const fileContent = fs.readFileSync(publisherConfigPath, "utf8");
      rawConfig = JSON.parse(fileContent);
   } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      logger.error(
         `Failed to parse ${publisherConfigPath}: ${message}. Using default empty config.`,
         {
            path: publisherConfigPath,
            error: message,
            stack: error instanceof Error ? error.stack : undefined,
         },
      );
      return {
         frozenConfig: false,
         environments: [],
      };
   }

   // Process environment variables in config values
   const processedConfig = processConfigValue(rawConfig);

   if (
      processedConfig &&
      typeof processedConfig === "object" &&
      "environments" in processedConfig &&
      processedConfig.environments &&
      typeof processedConfig.environments === "object" &&
      !Array.isArray(processedConfig.environments)
   ) {
      logger.error(
         `Invalid ${PUBLISHER_CONFIG_NAME}: the "environments" field must be a JSON array. Using default empty config.`,
      );
      return {
         frozenConfig: false,
         environments: [],
      };
   }

   // Ensure environments is an array
   let environments: unknown[] = [];
   if (
      processedConfig &&
      typeof processedConfig === "object" &&
      "environments" in processedConfig &&
      Array.isArray((processedConfig as { environments: unknown }).environments)
   ) {
      environments = (processedConfig as { environments: unknown[] })
         .environments;
   }

   let frozenConfig = false;
   if (
      processedConfig &&
      typeof processedConfig === "object" &&
      "frozenConfig" in processedConfig
   ) {
      frozenConfig = Boolean(
         (processedConfig as { frozenConfig: unknown }).frozenConfig,
      );
   }

   return {
      frozenConfig,
      environments,
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
   environmentName: string,
): Connection[] => {
   try {
      const publisherConfig = getPublisherConfig(serverRoot);
      if (!Array.isArray(publisherConfig.environments)) {
         return [];
      }
      const environment = publisherConfig.environments.find(
         (e) => e && e.name === environmentName,
      );
      return Array.isArray(environment?.connections)
         ? environment.connections
         : [];
   } catch (error) {
      logger.error(
         `Error getting connections for environment "${environmentName}" from ${PUBLISHER_CONFIG_NAME}`,
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

   // Ensure environments is an array
   if (!Array.isArray(rawConfig.environments)) {
      logger.warn(
         `Invalid ${PUBLISHER_CONFIG_NAME}: the "environments" field must be a JSON array. Using empty array.`,
      );
      return {
         frozenConfig: rawConfig.frozenConfig ?? false,
         environments: [],
      };
   }

   // Filter and validate environments, skipping invalid ones
   const validEnvironments: ProcessedEnvironment[] = [];
   for (const environment of rawConfig.environments) {
      if (!environment || typeof environment !== "object") {
         logger.warn(
            `Invalid environment in ${PUBLISHER_CONFIG_NAME}: entry must be an object. Skipping.`,
         );
         continue;
      }

      if (!environment.name || typeof environment.name !== "string") {
         logger.warn(
            `Invalid environment in ${PUBLISHER_CONFIG_NAME}: missing or invalid "name" field. Skipping entry.`,
            { environment },
         );
         continue;
      }

      if (!Array.isArray(environment.packages)) {
         logger.warn(
            `Invalid environment "${environment.name}" in ${PUBLISHER_CONFIG_NAME}: missing or invalid "packages" field (must be an array). Skipping entry.`,
         );
         continue;
      }

      // Validate packages have required fields
      const validPackages = environment.packages.filter((pkg) => {
         if (!pkg || typeof pkg !== "object") {
            logger.warn(
               `Invalid package in environment "${environment.name}": package must be an object. Skipping.`,
            );
            return false;
         }
         if (!pkg.name || typeof pkg.name !== "string") {
            logger.warn(
               `Invalid package in environment "${environment.name}": missing or invalid "name" field. Skipping.`,
            );
            return false;
         }
         if (!pkg.location || typeof pkg.location !== "string") {
            logger.warn(
               `Invalid package "${pkg.name}" in environment "${environment.name}": missing or invalid "location" field. Skipping.`,
            );
            return false;
         }
         return true;
      });

      if (validPackages.length === 0) {
         logger.warn(
            `Environment "${environment.name}" has no valid packages. Skipping entry.`,
         );
         continue;
      }

      validEnvironments.push({
         name: environment.name,
         packages: validPackages,
         connections: convertConnectionsToApiConnections(
            environment.connections || [],
         ),
      });
   }

   return {
      frozenConfig: rawConfig.frozenConfig ?? false,
      environments: validEnvironments,
   };
};
