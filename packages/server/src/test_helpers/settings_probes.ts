// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   BYPASS_AUTHORIZE_HEADER,
   readBypassAuthorize,
} from "../authorize_bypass_header";
import {
   assertDuckDBResourceConfig,
   getDefaultQueryRowLimit,
   getDuckDBMemoryLimit,
   getDuckDBTempDirectory,
   getDuckLakeRowGroupSizeBytes,
   getDuckLakeTargetFileSizeBytes,
   getEmbeddingConfig,
   getExtensionFetchPolicy,
   getInstanceTheme,
   getLlmSettings,
   getMaterializationSchedulerConfig,
   getMaxConcurrentQueries,
   getMaxQueryRows,
   getMaxResponseBytes,
   getMcpCorsOrigins,
   getMemoryGovernorConfig,
   getPartitionedWriteFlushThreshold,
   getPersistCollisionEnforce,
   getPersistStorageMode,
   getPostgresPoolMax,
   getPublisherConfig,
   getQueryMetadataMode,
   getQueryTimeoutMs,
   getRetrievalConfig,
   getS3CredentialChainPolicy,
   getServerApiSettings,
   getUnresolvedPublisherConfigPath,
   isDuckDBMemoryLimitDisabled,
   isDuckdbSetupSqlAllowed,
   isMcpIncludeHiddenFilesAndSources,
   isPublisherConfigFrozen,
   parseBoolEnv,
   schemaEmbeddingEnabled,
} from "../config";
import { parseFrameAncestors } from "../frame_ancestors";
import { mcpConfigEnabled } from "../mcp_config";
import {
   getPackageLoadSchemaCacheBytes,
   getPackageLoadWorkerCount,
} from "../package_load/package_load_pool";
import { parseRateLimit } from "../rate_limit";
import { resolveAdminHost, resolveAdminPort } from "../routes/admin";
import { resolveMcpHost } from "../routes/mcp";
import type { RetrievalLlmConfig } from "../retrieval_config";
import { catalogAdminConfigFromEnv } from "../service/catalog_admin";
import { formatReadinessLine } from "../service/environment_store";
import { allowUnverifiedHostKey } from "../service/proxy";
import type { SettingsContext } from "./settings_env";

/**
 * How the contract table observes each setting: the code that reads it today,
 * called the way the server calls it. Where today's read is an expression
 * inline in a module that cannot be imported in a test (main_server.ts starts
 * listeners), the probe repeats that expression and names where it lives.
 *
 * The table's rows (contract.spec.ts) say what each case gives. These probes
 * are the only part that follows the code as reads move into the registry.
 */
export const PROBES: Record<string, (context: SettingsContext) => unknown> = {
   frozenConfig: ({ serverRoot }) => isPublisherConfigFrozen(serverRoot),
   servingApiEnabled: ({ serverRoot }) =>
      getServerApiSettings(serverRoot).servingApiEnabled,
   adminApiEnabled: ({ serverRoot }) =>
      getServerApiSettings(serverRoot).adminApiEnabled,
   port: () => readiness().port,
   // main_server.ts:117
   host: () => process.env.PUBLISHER_HOST || "0.0.0.0",
   rateLimit: () => parseRateLimit(process.env.PUBLISHER_RATE_LIMIT) ?? 0,
   frameAncestors: () =>
      parseFrameAncestors(process.env.PUBLISHER_FRAME_ANCESTORS),
   theme: ({ serverRoot }) => getInstanceTheme(serverRoot),
   environments: ({ serverRoot }) =>
      getPublisherConfig(serverRoot).environments.map(
         (environment) => environment.name,
      ),

   // main_server.ts:146 and server.ts:24, which resolve it against the cwd.
   root: () => process.env.SERVER_ROOT || ".",
   config: ({ serverRoot }) => {
      const unresolved = getUnresolvedPublisherConfigPath(serverRoot);
      return unresolved?.explicit ? unresolved.path : undefined;
   },
   // No config in the root and none named, so the bundled one is used or none.
   useBundledDefault: ({ serverRoot }) =>
      getUnresolvedPublisherConfigPath(serverRoot) === null,
   // environment_store.ts:679 and :1247
   initializeStorage: () => process.env.INITIALIZE_STORAGE === "true",
   // environment_store.ts:432 and main_server.ts:416
   watchEnvironments: () =>
      (process.env.PUBLISHER_WATCH || "")
         .split(",")
         .map((name) => name.trim())
         .filter((name) => name.length > 0),

   "mcp.port": () => readiness().mcpPort,
   "mcp.host": () => resolveMcpHost(process.env),
   "mcp.corsOrigins": () => getMcpCorsOrigins(),
   "mcp.configFileEnabled": () => mcpConfigEnabled(),
   "mcp.includeHiddenFilesAndSources": ({ serverRoot }) =>
      isMcpIncludeHiddenFilesAndSources(serverRoot),
   // mcp/tools/get_context_tool.ts:517 (traceToLog)
   "mcp.trace": () =>
      process.env.PUBLISHER_MCP_TRACE?.trim().toLowerCase() === "retrieval",

   "admin.port": () => resolveAdminPort(process.env),
   "admin.host": () => resolveAdminHost(process.env),
   "admin.catalogDatabase": () =>
      catalogAdminConfigFromEnv(process.env)?.catalogDatabase ?? null,
   "admin.storage.provider": adminStorage,
   "admin.storage.keyId": adminStorage,
   "admin.storage.secret": adminStorage,
   "admin.storage.region": adminStorage,
   "admin.storage.endpoint": adminStorage,

   "query.maxRows": () => getMaxQueryRows(),
   "query.maxResponseBytes": () => getMaxResponseBytes(),
   "query.defaultRowLimit": () => getDefaultQueryRowLimit(),
   "query.timeoutMs": () => getQueryTimeoutMs(),
   "query.maxConcurrent": () => getMaxConcurrentQueries(),
   "query.metadataEnabled": () => getQueryMetadataMode(),
   "postgres.poolMax": () => getPostgresPoolMax(),
   "memory.maxBytes": () => getMemoryGovernorConfig()?.maxMemoryBytes ?? null,
   "memory.highWaterFraction": () =>
      getMemoryGovernorConfig()?.highWaterFraction,
   "memory.lowWaterFraction": () => getMemoryGovernorConfig()?.lowWaterFraction,
   "memory.checkIntervalMs": () => getMemoryGovernorConfig()?.checkIntervalMs,
   "memory.backpressureEnabled": () =>
      getMemoryGovernorConfig()?.backpressureEnabled,
   "packageLoad.workers": () => getPackageLoadWorkerCount(),
   // package_load/package_load_pool.ts:116, read once when the module loads.
   "packageLoad.jobTimeoutMs": () => {
      const raw = process.env.PACKAGE_LOAD_JOB_TIMEOUT_MS;
      if (raw === undefined) return 120_000;
      const parsed = Number.parseInt(raw, 10);
      if (!Number.isFinite(parsed) || parsed <= 0) return 120_000;
      return parsed;
   },
   "packageLoad.schemaCacheBytes": () => getPackageLoadSchemaCacheBytes(),

   "duckdb.extensionFetchPolicy": () => getExtensionFetchPolicy(),
   "duckdb.memoryLimit": () => {
      assertDuckDBResourceConfig();
      return isDuckDBMemoryLimitDisabled() ? "off" : getDuckDBMemoryLimit();
   },
   "duckdb.tempDirectory": () => getDuckDBTempDirectory(),
   "duckdb.partitionedWriteFlushThreshold": () => {
      assertDuckDBResourceConfig();
      return getPartitionedWriteFlushThreshold();
   },
   "ducklake.rowGroupSizeBytes": () => {
      assertDuckDBResourceConfig();
      return getDuckLakeRowGroupSizeBytes();
   },
   "ducklake.targetFileSizeBytes": () => {
      assertDuckDBResourceConfig();
      return getDuckLakeTargetFileSizeBytes();
   },
   "persist.storageMode": () => getPersistStorageMode(),
   "persist.collisionEnforcementEnabled": () => getPersistCollisionEnforce(),
   "materialization.schedulerEnabled": () =>
      getMaterializationSchedulerConfig() !== null,
   "materialization.schedulerIntervalMs": () =>
      getMaterializationSchedulerConfig()?.tickIntervalMs,
   "materialization.schedulerMaxFiresPerTick": () =>
      getMaterializationSchedulerConfig()?.maxFiresPerTick,

   // service/connection_config.ts:830 (validatePublisherShape)
   "allow.proxyConnections": () =>
      process.env.PUBLISHER_ALLOW_PROXY_CONNECTIONS === "true",
   "allow.duckdbSetupSql": () => isDuckdbSetupSqlAllowed(),
   // Read twice today: leniently where a tunnel connects (proxy.ts), and
   // strictly when a connection's config loads (connection_config.ts:548).
   "allow.unverifiedSshHostKey": () => ({
      atConnect: allowUnverifiedHostKey(),
      atConfigLoad: acceptedOrRefused(() =>
         parseBoolEnv("PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY"),
      ),
   }),
   "allow.s3CredentialChain": () => getS3CredentialChainPolicy(),
   "authorize.bypassSecret": () =>
      readBypassAuthorize({
         headers: { [BYPASS_AUTHORIZE_HEADER]: "s3cret" },
      }) === true,
   // logger.ts:12 (getLogLevel), read once when the logger loads.
   "logging.level": () => {
      const valid = ["error", "warn", "info", "verbose", "debug", "silly"];
      const raw = process.env.LOG_LEVEL;
      return raw && valid.includes(raw.toLowerCase())
         ? raw.toLowerCase()
         : "debug";
   },
   // logger.ts:64, read once when the logger loads.
   "logging.responseBodiesEnabled": () =>
      !(
         process.env.DISABLE_RESPONSE_LOGGING === "true" ||
         process.env.DISABLE_RESPONSE_LOGGING === "1"
      ),
   // main_server.ts:137 and admin_server.ts:61
   "shutdown.drainDurationSeconds": () =>
      Number(process.env.SHUTDOWN_DRAIN_DURATION_SECONDS || 0),
   // main_server.ts:140 and admin_server.ts:62
   "shutdown.gracefulCloseTimeoutSeconds": () =>
      Number(process.env.SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS || 0),

   "retrieval.embedding.apiKey": (context) =>
      embedding(context)?.apiKey ?? null,
   "retrieval.embedding.model": (context) => embedding(context)?.model,
   "retrieval.embedding.baseUrl": (context) => embedding(context)?.baseUrl,
   "retrieval.embedding.dimensions": (context) =>
      embedding(context)?.dimensions,
   "retrieval.embedding.minSimilarity": (context) =>
      embedding(context)?.minSimilarity,
   "retrieval.embedding.connectionSchemaIndexingEnabled": () =>
      schemaEmbeddingEnabled(),
   "retrieval.llm.apiKey": () =>
      getLlmSettings({ provider: "openai", model: "gpt" } as RetrievalLlmConfig)
         ?.apiKey ?? null,
};

/** The ports the readiness line reports, which repeat main_server.ts's reads. */
function readiness(): { port: number; mcpPort: number } {
   const line = formatReadinessLine({
      environments: 0,
      packages: 0,
      loadErrors: 0,
   });
   const port = /url=http:\/\/\S*:([^:\s]*) /.exec(line)?.[1];
   const mcpPort = /mcp=http:\/\/\S*:([^:\s]*) /.exec(line)?.[1];
   return { port: Number(port), mcpPort: Number(mcpPort) };
}

function adminStorage(): unknown {
   return catalogAdminConfigFromEnv(process.env)?.storage;
}

/** The embedding settings as main_server.ts resolves them at boot. */
function embedding({ serverRoot }: SettingsContext) {
   return getEmbeddingConfig(getRetrievalConfig(serverRoot)?.embedding);
}

/** Whether a read that throws on a bad value accepts this one. */
function acceptedOrRefused(read: () => unknown): "accepted" | "refused" {
   try {
      read();
      return "accepted";
   } catch {
      return "refused";
   }
}
