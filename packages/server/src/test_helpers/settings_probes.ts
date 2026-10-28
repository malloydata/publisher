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
   schemaEmbeddingEnabled,
} from "../config";
import { parseFrameAncestors } from "../frame_ancestors";
import { mcpConfigEnabled } from "../mcp_config";
import {
   getPackageLoadSchemaCacheBytes,
   getPackageLoadWorkerCount,
} from "../package_load/package_load_pool";
import type { RetrievalLlmConfig } from "../retrieval_config";
import { catalogAdminConfig } from "../service/catalog_admin";
import { formatReadinessLine } from "../service/environment_store";
import { allowUnverifiedHostKey } from "../service/proxy";
import { settings } from "../settings";
import type { SettingsContext } from "./settings_env";

/**
 * How the contract table observes each setting: the code that reads it,
 * called the way the server calls it. Where the read is inline in a module
 * that cannot be imported in a test (main_server.ts starts listeners), the
 * probe is the setting itself, which that module reads.
 *
 * The table's rows (contract.spec.ts) say what each case gives. These probes
 * are the only part that follows the code as reads move into the registry.
 */
export const PROBES: Record<string, (context: SettingsContext) => unknown> = {
   frozenConfig: () => isPublisherConfigFrozen(),
   servingApiEnabled: () => getServerApiSettings().servingApiEnabled,
   adminApiEnabled: () => getServerApiSettings().adminApiEnabled,
   port: () => readiness().port,
   host: () => settings.get("host"),
   rateLimit: () => settings.get("rateLimit"),
   frameAncestors: () => parseFrameAncestors(settings.get("frameAncestors")),
   theme: ({ serverRoot }) => getInstanceTheme(serverRoot),
   environments: ({ serverRoot }) =>
      getPublisherConfig(serverRoot).environments.map(
         (environment) => environment.name,
      ),

   root: () => settings.get("root"),
   config: ({ serverRoot }) => {
      const unresolved = getUnresolvedPublisherConfigPath(serverRoot);
      return unresolved?.explicit ? unresolved.path : undefined;
   },
   // No config in the root and none named, so the bundled one is used or none.
   useBundledDefault: ({ serverRoot }) =>
      getUnresolvedPublisherConfigPath(serverRoot) === null,
   initializeStorage: () => settings.get("initializeStorage"),
   watchEnvironments: () => settings.get("watchEnvironments"),

   "mcp.port": () => readiness().mcpPort,
   "mcp.host": () => settings.get("mcp.host"),
   "mcp.corsOrigins": () => getMcpCorsOrigins(),
   "mcp.configFileEnabled": () => mcpConfigEnabled(),
   "mcp.includeHiddenFilesAndSources": () =>
      isMcpIncludeHiddenFilesAndSources(),
   "mcp.trace": () => settings.get("mcp.trace") === "retrieval",

   "admin.port": () => settings.get("admin.port") ?? null,
   "admin.host": () => settings.get("admin.host"),
   "admin.catalogDatabase": () => catalogAdminConfig()?.catalogDatabase ?? null,
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
   "packageLoad.jobTimeoutMs": () => settings.get("packageLoad.jobTimeoutMs"),
   "packageLoad.schemaCacheBytes": () => getPackageLoadSchemaCacheBytes(),

   "duckdb.extensionFetchPolicy": () => getExtensionFetchPolicy(),
   "duckdb.memoryLimit": () => {
      assertDuckDBResourceConfig();
      return isDuckDBMemoryLimitDisabled() ? "off" : getDuckDBMemoryLimit();
   },
   "duckdb.tempDirectory": () => {
      assertDuckDBResourceConfig();
      return getDuckDBTempDirectory();
   },
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

   "allow.proxyConnections": () => settings.get("allow.proxyConnections"),
   "allow.duckdbSetupSql": () => isDuckdbSetupSqlAllowed(),
   // Read twice today: leniently where a tunnel connects (proxy.ts), and
   // strictly when a connection's config loads (connection_config.ts:548).
   "allow.unverifiedSshHostKey": () => ({
      atConnect: allowUnverifiedHostKey(),
      atConfigLoad: acceptedOrRefused(() =>
         settings.get("allow.unverifiedSshHostKey"),
      ),
   }),
   "allow.s3CredentialChain": () => getS3CredentialChainPolicy(),
   "authorize.bypassSecret": () =>
      readBypassAuthorize({
         headers: { [BYPASS_AUTHORIZE_HEADER]: "s3cret" },
      }) === true,
   "logging.level": () => settings.get("logging.level"),
   "logging.responseBodiesEnabled": () =>
      settings.get("logging.responseBodiesEnabled"),
   "shutdown.drainDurationSeconds": () =>
      settings.get("shutdown.drainDurationSeconds"),
   "shutdown.gracefulCloseTimeoutSeconds": () =>
      settings.get("shutdown.gracefulCloseTimeoutSeconds"),

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
   return catalogAdminConfig()?.storage;
}

/**
 * The embedding settings as main_server.ts resolves them at boot, where a
 * rejected `retrieval` block leaves retrieval on its defaults.
 */
function embedding({ serverRoot }: SettingsContext) {
   let block: ReturnType<typeof getRetrievalConfig>;
   try {
      block = getRetrievalConfig(serverRoot);
   } catch {
      block = undefined;
   }
   return getEmbeddingConfig(block?.embedding);
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
