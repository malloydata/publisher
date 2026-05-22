import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { components } from "./api";
import {
   API_PREFIX,
   DEFAULT_MAX_QUERY_ROWS,
   PUBLISHER_CONFIG_NAME,
} from "./constants";
import { logger } from "./logger";

/**
 * Path to the publisher.config.json file shipped inside the published
 * package. Used as a last-resort fallback so `npx @malloy-publisher/server`
 * with no args still boots with the DuckDB-only sample packages.
 *
 * The file is copied next to the running module by `build.ts` at production
 * build time. In a source/dev checkout it lives alongside this file.
 */
const BUNDLED_DEFAULT_CONFIG_PATH = path.join(
   path.dirname(fileURLToPath(import.meta.url)),
   "default-publisher.config.json",
);

/**
 * Decide which `publisher.config.json` to read.
 *
 * Precedence:
 *   1. `--config <path>` (surfaced via `process.env.PUBLISHER_CONFIG_PATH`)
 *   2. `<serverRoot>/publisher.config.json`
 *   3. The bundled default shipped inside the package — ONLY when
 *      `process.env.PUBLISHER_USE_BUNDLED_DEFAULT === "true"`. server.ts
 *      sets that flag when the user passed neither `--server_root` nor
 *      `--config`, so `npx @malloy-publisher/server` with zero args
 *      boots into something usable. Callers that construct an
 *      EnvironmentStore programmatically (tests, embeds) don't get
 *      surprise filesystem fallbacks they didn't ask for.
 *
 * Returns `null` if step 1 was requested but the file doesn't exist —
 * that's an explicit user mistake and the caller should surface it as
 * an error rather than silently falling back.
 */
function resolvePublisherConfigPath(serverRoot: string): {
   path: string;
   isBundledDefault: boolean;
} | null {
   const explicitPath = process.env.PUBLISHER_CONFIG_PATH;
   if (explicitPath && explicitPath.length > 0) {
      if (!fs.existsSync(explicitPath)) {
         return null;
      }
      return { path: explicitPath, isBundledDefault: false };
   }

   const serverRootPath = path.join(serverRoot, PUBLISHER_CONFIG_NAME);
   if (fs.existsSync(serverRootPath)) {
      return { path: serverRootPath, isBundledDefault: false };
   }

   if (
      process.env.PUBLISHER_USE_BUNDLED_DEFAULT === "true" &&
      fs.existsSync(BUNDLED_DEFAULT_CONFIG_PATH)
   ) {
      return { path: BUNDLED_DEFAULT_CONFIG_PATH, isBundledDefault: true };
   }

   return null;
}

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

/**
 * Tunables for {@link PackageMemoryGovernor}. All values are sourced
 * from environment variables at startup; see {@link getMemoryGovernorConfig}
 * for parsing and defaults.
 *
 * The governor is admission control only: it polls process RSS on
 * `checkIntervalMs` and toggles a single `isBackpressured` flag using
 * a low/high-water hysteresis band. It does NOT evict, unload, or
 * interrupt already-loaded packages — recovery is left to the kernel
 * reclaiming pages as in-flight traffic completes.
 */
export interface MemoryGovernorConfig {
   /** Hard ceiling for process RSS in bytes (the OOM-relevant figure). */
   maxMemoryBytes: number;
   /** Fraction of `maxMemoryBytes` at which the governor activates back-pressure (new package loads start returning HTTP 503). Must be in (0, 1) and strictly greater than `lowWaterFraction`. */
   highWaterFraction: number;
   /** Fraction of `maxMemoryBytes` at which the governor clears back-pressure (new package loads admitted again). Must be in (0, 1) and strictly less than `highWaterFraction`; the gap is the hysteresis band that prevents flap. */
   lowWaterFraction: number;
   /** Polling cadence for the RSS sampler, in milliseconds. */
   checkIntervalMs: number;
   /** When true, RSS crossings flip the back-pressure flag. When false, the governor still samples and emits metrics but never rejects requests — useful for a monitoring-only rollout before enabling the 503 behaviour. */
   backpressureEnabled: boolean;
}

const DEFAULT_HIGH_WATER_FRACTION = 0.8;
const DEFAULT_LOW_WATER_FRACTION = 0.7;
const DEFAULT_CHECK_INTERVAL_MS = 5_000;
const MIN_CHECK_INTERVAL_MS = 100;

function parseIntEnv(name: string): number | undefined {
   const raw = process.env[name];
   if (raw === undefined || raw.trim() === "") return undefined;
   const value = Number.parseInt(raw, 10);
   if (!Number.isFinite(value) || String(value) !== raw.trim()) {
      throw new Error(
         `Invalid value for ${name}: expected a base-10 integer, got "${raw}"`,
      );
   }
   return value;
}

function parseFloatEnv(name: string): number | undefined {
   const raw = process.env[name];
   if (raw === undefined || raw.trim() === "") return undefined;
   const value = Number.parseFloat(raw);
   if (!Number.isFinite(value)) {
      throw new Error(
         `Invalid value for ${name}: expected a finite number, got "${raw}"`,
      );
   }
   return value;
}

function parseBoolEnv(name: string): boolean | undefined {
   const raw = process.env[name];
   if (raw === undefined || raw.trim() === "") return undefined;
   const normalised = raw.trim().toLowerCase();
   if (["1", "true", "yes", "on"].includes(normalised)) return true;
   if (["0", "false", "no", "off"].includes(normalised)) return false;
   throw new Error(
      `Invalid value for ${name}: expected a boolean (true/false), got "${raw}"`,
   );
}

/**
 * Parse memory-governor settings from environment variables and return
 * either a fully-validated config or `null` when the feature is
 * disabled. The feature is disabled iff `PUBLISHER_MAX_MEMORY_BYTES`
 * is unset or set to `0`.
 *
 * Throws at startup on malformed input so a typo in a k8s manifest
 * surfaces as a loud failure rather than silently disabling the cap.
 */
export const getMemoryGovernorConfig = (): MemoryGovernorConfig | null => {
   const maxMemoryBytes = parseIntEnv("PUBLISHER_MAX_MEMORY_BYTES");
   if (maxMemoryBytes === undefined || maxMemoryBytes === 0) {
      return null;
   }
   if (maxMemoryBytes < 0) {
      throw new Error(
         `PUBLISHER_MAX_MEMORY_BYTES must be a positive integer (got ${maxMemoryBytes})`,
      );
   }

   const highWaterFraction =
      parseFloatEnv("PUBLISHER_MEMORY_HIGH_WATER_FRACTION") ??
      DEFAULT_HIGH_WATER_FRACTION;
   const lowWaterFraction =
      parseFloatEnv("PUBLISHER_MEMORY_LOW_WATER_FRACTION") ??
      DEFAULT_LOW_WATER_FRACTION;
   const checkIntervalMs =
      parseIntEnv("PUBLISHER_MEMORY_CHECK_INTERVAL_MS") ??
      DEFAULT_CHECK_INTERVAL_MS;
   const backpressureEnabled =
      parseBoolEnv("PUBLISHER_MEMORY_BACKPRESSURE") ?? true;

   if (highWaterFraction <= 0 || highWaterFraction >= 1) {
      throw new Error(
         `PUBLISHER_MEMORY_HIGH_WATER_FRACTION must be in (0, 1) (got ${highWaterFraction})`,
      );
   }
   if (lowWaterFraction <= 0 || lowWaterFraction >= 1) {
      throw new Error(
         `PUBLISHER_MEMORY_LOW_WATER_FRACTION must be in (0, 1) (got ${lowWaterFraction})`,
      );
   }
   if (lowWaterFraction >= highWaterFraction) {
      throw new Error(
         `PUBLISHER_MEMORY_LOW_WATER_FRACTION (${lowWaterFraction}) must be strictly less than PUBLISHER_MEMORY_HIGH_WATER_FRACTION (${highWaterFraction})`,
      );
   }
   if (checkIntervalMs < MIN_CHECK_INTERVAL_MS) {
      throw new Error(
         `PUBLISHER_MEMORY_CHECK_INTERVAL_MS must be >= ${MIN_CHECK_INTERVAL_MS} (got ${checkIntervalMs})`,
      );
   }

   return {
      maxMemoryBytes,
      highWaterFraction,
      lowWaterFraction,
      checkIntervalMs,
      backpressureEnabled,
   };
};

/**
 * Resolve the row cap applied to ad-hoc connection SQL queries.
 * Reads `PUBLISHER_MAX_QUERY_ROWS`; falls back to
 * {@link DEFAULT_MAX_QUERY_ROWS} when unset or empty.
 *
 * Throws at startup on malformed input (matching the loud-failure
 * stance of {@link getMemoryGovernorConfig}) so a typo in a k8s
 * manifest surfaces immediately instead of silently disabling the
 * cap. A value of `0` is accepted and disables wrapping entirely;
 * use it only when you intend to opt out of the row cap (e.g. when
 * Step 2's byte budget is the only thing you want enforcing the
 * bound).
 */
export const getMaxQueryRows = (): number => {
   const raw = parseIntEnv("PUBLISHER_MAX_QUERY_ROWS");
   if (raw === undefined) return DEFAULT_MAX_QUERY_ROWS;
   if (raw < 0) {
      throw new Error(
         `PUBLISHER_MAX_QUERY_ROWS must be a non-negative integer (got ${raw})`,
      );
   }
   return raw;
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
   const resolved = resolvePublisherConfigPath(serverRoot);
   if (!resolved) {
      if (
         process.env.PUBLISHER_CONFIG_PATH &&
         process.env.PUBLISHER_CONFIG_PATH.length > 0
      ) {
         // Explicit --config was given but the path didn't exist. Loud
         // failure here so a typo in the flag doesn't silently boot the
         // server with an empty environment list.
         logger.error(
            `--config path not found: ${process.env.PUBLISHER_CONFIG_PATH}. Using default empty config.`,
         );
      }
      return {
         frozenConfig: false,
         environments: [],
      };
   }
   const publisherConfigPath = resolved.path;
   if (resolved.isBundledDefault) {
      logger.info(
         `No publisher.config.json found at ${path.join(serverRoot, PUBLISHER_CONFIG_NAME)}; falling back to bundled DuckDB-only default. Pass --config <path> or place a config in the server root to override.`,
      );
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

   // TODO: Remove this during projects cleanup
   // Back-compat: the top-level key was renamed `projects` → `environments`.
   // If a config still uses the old key, accept it once with a deprecation
   // warning so existing on-disk configs don't silently parse as empty.
   if (
      processedConfig &&
      typeof processedConfig === "object" &&
      !("environments" in processedConfig) &&
      "projects" in processedConfig
   ) {
      logger.warn(
         `${PUBLISHER_CONFIG_NAME} uses deprecated "projects" key; rename to "environments".`,
      );
      (processedConfig as Record<string, unknown>).environments = (
         processedConfig as Record<string, unknown>
      ).projects;
   }

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
