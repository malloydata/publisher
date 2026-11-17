// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Env } from "./settings";
import { derivedEnvName } from "./setting";

/**
 * The variables step 3 of the configuration design removed, each with the
 * key of the setting that replaced it. Nothing reads them. A deployment that
 * still sets one gets that setting's default, which for some is the
 * permissive one (`S3_CREDENTIAL_CHAIN_POLICY=off` is `any` again), so startup
 * warns about each that is set. The negated ones say the opposite of their
 * setting.
 */
export const REMOVED_VARIABLES: Readonly<
   Record<string, { key: string; negated?: true }>
> = {
   SERVER_ROOT: { key: "root" },
   PUBLISHER_CONFIG_PATH: { key: "config" },
   INITIALIZE_STORAGE: { key: "initializeStorage" },
   PUBLISHER_WATCH: { key: "watchEnvironments" },
   MCP_PORT: { key: "mcp.port" },
   MCP_HOST: { key: "mcp.host" },
   MCP_CORS_ORIGINS: { key: "mcp.corsOrigins" },
   PUBLISHER_NO_MCP_CONFIG: { key: "mcp.configFileEnabled", negated: true },
   MCP_APP_ORIGIN: { key: "mcp.appOrigin" },
   PUBLISHER_MAX_QUERY_ROWS: { key: "query.maxRows" },
   PUBLISHER_MAX_RESPONSE_BYTES: { key: "query.maxResponseBytes" },
   PUBLISHER_DEFAULT_QUERY_ROW_LIMIT: { key: "query.defaultRowLimit" },
   PUBLISHER_MAX_CONCURRENT_QUERIES: { key: "query.maxConcurrent" },
   PUBLISHER_QUERY_METADATA: { key: "query.metadataEnabled" },
   PUBLISHER_MAX_MEMORY_BYTES: { key: "memory.maxBytes" },
   PUBLISHER_MEMORY_BACKPRESSURE: { key: "memory.backpressureEnabled" },
   PACKAGE_LOAD_WORKERS: { key: "packageLoad.workers" },
   PACKAGE_LOAD_JOB_TIMEOUT_MS: { key: "packageLoad.jobTimeoutMs" },
   PACKAGE_LOAD_SCHEMA_CACHE_BYTES: { key: "packageLoad.schemaCacheBytes" },
   EXTENSION_FETCH_POLICY: { key: "duckdb.extensionFetchPolicy" },
   PERSIST_STORAGE_MODE: { key: "persist.storageMode" },
   PUBLISHER_MATERIALIZE: { key: "materialize.enabled" },
   PERSIST_COLLISION_ENFORCE: { key: "persist.collisionEnforcementEnabled" },
   PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER: {
      key: "materialization.schedulerEnabled",
   },
   S3_CREDENTIAL_CHAIN_POLICY: { key: "allow.s3CredentialChain" },
   PUBLISHER_BYPASS_AUTHORIZE_SECRET: { key: "authorize.bypassSecret" },
   LOG_LEVEL: { key: "logging.level" },
   DISABLE_RESPONSE_LOGGING: {
      key: "logging.responseBodiesEnabled",
      negated: true,
   },
   SHUTDOWN_DRAIN_DURATION_SECONDS: { key: "shutdown.drainDurationSeconds" },
   SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS: {
      key: "shutdown.gracefulCloseTimeoutSeconds",
   },
   EMBEDDING_API_KEY: { key: "retrieval.embedding.apiKey" },
   EMBEDDING_MODEL: { key: "retrieval.embedding.model" },
   EMBEDDING_API_BASE: { key: "retrieval.embedding.baseUrl" },
   EMBEDDING_DIMENSIONS: { key: "retrieval.embedding.dimensions" },
   EMBEDDING_MIN_SIMILARITY: { key: "retrieval.embedding.minSimilarity" },
   EMBEDDING_INDEX_CONNECTION_SCHEMA: {
      key: "retrieval.embedding.connectionSchemaIndexingEnabled",
   },
   LLM_API_KEY: { key: "retrieval.llm.apiKey" },
};

/**
 * A warning for each removed variable `env` (this process's by default) sets,
 * naming the variable that replaced it. Blank is unset, as for every setting.
 */
export function removedVariableWarnings(env: Env = process.env): string[] {
   return Object.entries(REMOVED_VARIABLES).flatMap(
      ([name, { key, negated }]) =>
         env[name] === undefined || env[name]!.trim() === ""
            ? []
            : [
                 `${name} is set, but this release no longer reads it: set ${derivedEnvName(key)} (${key}) instead${negated ? ", which says the opposite" : ""}.`,
              ],
   );
}
