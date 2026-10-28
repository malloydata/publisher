// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The main API and MCP: what `server.ts` starts when `servingApiEnabled` is on,
// after it has checked the Node version, applied the command line and checked
// which APIs the config enables. The admin listener starts here too when the
// config leaves `adminApiEnabled` on and its port is set.
import type { AddressInfo } from "net";
import express from "express";
import bodyParser from "body-parser";
import cors from "cors";
import * as http from "http";
import { createProxyMiddleware } from "http-proxy-middleware";
import * as path from "path";
import { fileURLToPath } from "url";
import { CompileController } from "./controller/compile.controller";
import { ConnectionController } from "./controller/connection.controller";
import { DatabaseController } from "./controller/database.controller";
import { MaterializationController } from "./controller/materialization.controller";
import { ModelController } from "./controller/model.controller";
import { PackageController } from "./controller/package.controller";
import { QueryController } from "./controller/query.controller";
import { ReportController } from "./controller/report.controller";
import { SkillController } from "./controller/skill.controller";
import { StorageDestinationController } from "./controller/storage_destination.controller";
import { ThemeController } from "./controller/theme.controller";
import { WatchModeController } from "./controller/watch-mode.controller";
import {
   drainingGuard,
   registerHealthEndpoints,
   registerSignalHandlers,
} from "./health";
import "./instrumentation";
import {
   getPrometheusMetricsHandler,
   httpMetricsMiddleware,
} from "./instrumentation";
import { logger, loggerMiddleware } from "./logger";
import {
   assertDuckDBResourceConfig,
   getDuckDBMemoryLimit,
   getDuckDBTempDirectory,
   DEFAULT_SEMANTIC_INDEX_MAX_ENTITIES,
   getEmbeddingSettings,
   getExtensionFetchPolicy,
   getLlmSettings,
   getMaterializationSchedulerConfig,
   getMemoryGovernorConfig,
   isDuckDBMemoryLimitDisabled,
   getPersistCollisionEnforce,
   getPersistStorageMode,
   getQueryMetadataMode,
   embeddingStartupNotices,
   getRetrievalConfig,
} from "./config";
import { setRetrievalConfig } from "./retrieval_config";
import { checkHeapConfiguration } from "./heap_check";
import { setMaxEmbeddedEntities } from "./mcp/tools/embedding_index";
import { startPackageEmbeddingSync } from "./mcp/tools/get_context_tool";
import {
   addCommand,
   ensureMcpConfig,
   logMcpConfigOutcome,
   MCP_CONFIG_FILENAME,
   mcpConfigEnabled,
   mcpEndpoint,
   resolveBoundPort,
   resolveClientHost,
} from "./mcp_config";
import { observePackageStates } from "./package_state_metrics";
import { registerLegacyRoutes } from "./server-old";
import { EnvironmentStore } from "./service/environment_store";
import { MaterializationScheduler } from "./service/materialization_scheduler";
import { MaterializationService } from "./service/materialization_service";
import { PackageMemoryGovernor } from "./service/package_memory_governor";
import { ThemeStore } from "./service/theme_store";
import { registerApiRoutes } from "./routes/api";
import { unhandledErrorMiddleware } from "./routes/json_route";
import {
   registerDataAppListRoute,
   registerDataAppStaticRoutes,
} from "./routes/data_apps";
import { createAdminApp } from "./routes/admin";
import { CatalogAdmin, catalogAdminConfig } from "./service/catalog_admin";
import { createMcpApp } from "./routes/mcp";
import { registerSpaFallback } from "./routes/spa";
import { rateLimitMiddleware } from "./rate_limit";
import { frameAncestorsMiddleware } from "./frame_ancestors";
import { externalEnv, settings } from "./settings";
import { acceptBundledExperimentalDialects } from "./service/experimental_dialects";

// These getters throw on an invalid value. Read once at boot so a typo fails
// startup instead of later requests: package metadata, publishes, and (for the
// query-metadata mode) every query and build.
getPersistStorageMode();
getPersistCollisionEnforce();
getQueryMetadataMode();

// Before any compile on this thread: a connection-test, an MCP compile or a
// package load on the main thread would otherwise refuse a SQL Server model.
acceptBundledExperimentalDialects();

const PUBLISHER_PORT = settings.get("port");
const PUBLISHER_HOST = settings.get("host");
const MCP_PORT = settings.get("mcp.port");
// The MCP endpoint is unauthenticated and exposes every tool, so it binds
// loopback by default; `mcp.host`'s default follows an explicit `host` (so
// `--host` moves both listeners).
const MCP_HOST = settings.get("mcp.host");
// The admin listener (routes/admin.ts) is off unless a port is given. Read
// here so a malformed port fails startup before anything binds.
const ADMIN_PORT = settings.get("admin.port") ?? null;
const ADMIN_HOST = settings.get("admin.host");
// The catalog database the admin API finds catalogs in, read here so a malformed
// storage setting fails startup. Null leaves the catalog routes answering 503.
const ADMIN_CATALOG_CONFIG = ADMIN_PORT === null ? null : catalogAdminConfig();
const ADMIN_CATALOGS = ADMIN_CATALOG_CONFIG
   ? new CatalogAdmin(ADMIN_CATALOG_CONFIG)
   : null;
// Resolved at module scope, not in the listen callback: it throws on a typo,
// and a throw there would be an uncaughtException after both ports are bound.
const MCP_CONFIG_ENABLED = mcpConfigEnabled();
const SHUTDOWN_DRAIN_DURATION_SECONDS = settings.get(
   "shutdown.drainDurationSeconds",
);
const SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS = settings.get(
   "shutdown.gracefulCloseTimeoutSeconds",
);
// The app directory sits next to this bundled file, under ESM and under npx.
const __filename_esm = fileURLToPath(import.meta.url);
const ROOT = path.join(path.dirname(__filename_esm), "app");
const SERVER_ROOT = path.resolve(process.cwd(), settings.get("root"));
const API_PREFIX = "/api/v0";
const isDevelopment = externalEnv("NODE_ENV") === "development";

export const app = express();
app.use(loggerMiddleware);
app.use(httpMetricsMiddleware);
// Ahead of every early-answering middleware (the rate limiter's 429 included),
// so the frame-ancestors policy is on every response.
app.use(frameAncestorsMiddleware(settings.get("frameAncestors")));
// Health probes come before every middleware they do not need, such as
// static-file serving, which would stat the filesystem first.
registerHealthEndpoints(app);
// Opt-in (`rateLimit`) and mounted before every route; probes and /metrics
// are exempt inside the middleware.
app.use(rateLimitMiddleware(settings.get("rateLimit")));
// Advisory only: warns when --max-old-space-size looks low for the default
// row/byte caps, so operators check that before chasing OOMKills.
checkHeapConfiguration();
export const environmentStore = new EnvironmentStore(SERVER_ROOT);
const watchModeController = new WatchModeController(environmentStore);
const connectionController = new ConnectionController(environmentStore);
const modelController = new ModelController(environmentStore);
// Validated at boot so a typo fails startup rather than the first DuckDB
// query, which matters most to an operator relying on `local-only` for a
// no-network guarantee. The log records the posture the server booted with.
logger.info(`DuckDB extension-fetch policy: ${getExtensionFetchPolicy()}`);
// Validated (and the spill directory created) at boot: the health probes never
// touch DuckDB, so a bad limit or directory would otherwise leave a pod
// reporting ready while every query failed. `SET temp_directory` accepts a
// missing directory and fails only at the first spill.
assertDuckDBResourceConfig();
const duckDBMemoryLimit = getDuckDBMemoryLimit();
if (duckDBMemoryLimit === undefined && !isDuckDBMemoryLimitDisabled()) {
   // Warned rather than defaulted: no flat value suits every container size,
   // so the operator picks it.
   logger.warn(
      "PUBLISHER_DUCKDB_MEMORY_LIMIT is unset: every DuckDB instance in this " +
         "process sizes its memory_limit from the container independently, so " +
         "their combined budget exceeds it and the process can be OOM-killed " +
         "while each instance believes it is within budget. See " +
         "docs/configuration.md.",
   );
} else {
   logger.info(
      `DuckDB session limits: memory_limit=${duckDBMemoryLimit ?? "off (explicitly disabled)"} ` +
         `temp_directory=${getDuckDBTempDirectory() ?? "<duckdb default>"}`,
   );
}
// Retrieval and embedding settings are resolved once at boot, so a bad value
// fails loudly with its fix rather than on the first question. The provider
// getters read them back from module state. Logs name the host, never the key.
//
// A config file that cannot be read or parsed has already failed startup in
// server.ts. What can still fail here must not kill the process: an unset
// `${VAR}` in the environments, which the environment store reports as
// `initError` / PUBLISHER_INIT_FAILED with the server kept up so /status can
// name it, and a rejected `retrieval` block, which leaves retrieval at its
// defaults. So this read logs and carries on.
let retrievalConfig: ReturnType<typeof getRetrievalConfig>;
try {
   retrievalConfig = getRetrievalConfig(SERVER_ROOT);
} catch (error) {
   retrievalConfig = undefined;
   logger.error(
      `Could not read the retrieval settings from publisher.config.json; retrieval runs with its defaults until this is fixed and the server restarted. ${
         error instanceof Error ? error.message : String(error)
      }`,
   );
}
setRetrievalConfig(retrievalConfig);
const embeddingConfig = getEmbeddingSettings(retrievalConfig?.embedding);
if (embeddingConfig) {
   logger.info(
      `Semantic get_context enabled: ${embeddingConfig.provider} model ${embeddingConfig.model}` +
         (embeddingConfig.baseUrl
            ? ` at ${new URL(embeddingConfig.baseUrl).host}`
            : ""),
   );
}
for (const notice of embeddingStartupNotices(retrievalConfig?.embedding)) {
   logger[notice.level](notice.message);
}
const llmSettings = getLlmSettings(retrievalConfig?.llm);
if (llmSettings) {
   logger.info(
      `Retrieval LLM enabled: ${llmSettings.provider} model ${llmSettings.model}`,
   );
} else if (retrievalConfig?.llm) {
   logger.warn(
      `retrieval.llm names provider "${retrievalConfig.llm.provider}" but LLM_API_KEY is not set, so every LLM feature is off. ` +
         `Fix: set LLM_API_KEY in the server's environment.`,
   );
}
const semanticIndexMaxEntities =
   retrievalConfig?.indexing?.maxEntities ??
   DEFAULT_SEMANTIC_INDEX_MAX_ENTITIES;
setMaxEmbeddedEntities(semanticIndexMaxEntities);
if (embeddingConfig) {
   logger.info(
      `Semantic index entity cap: ${semanticIndexMaxEntities} (retrieval.indexing.maxEntities)`,
   );
}
const memoryGovernorConfig = getMemoryGovernorConfig();
const memoryGovernor = memoryGovernorConfig
   ? new PackageMemoryGovernor(memoryGovernorConfig)
   : null;
memoryGovernor?.start();
environmentStore.setMemoryGovernor(memoryGovernor);
observePackageStates(environmentStore);
// Build the semantic index as each package loads, not on the first question.
// This only queues work; with no embedding provider the job returns at once.
environmentStore.setPackageLoadedHook((environmentName, pkg) =>
   startPackageEmbeddingSync(environmentStore, environmentName, pkg),
);
const packageController = new PackageController(environmentStore);
const reportController = new ReportController(environmentStore);
const skillController = new SkillController(environmentStore);
const databaseController = new DatabaseController(environmentStore);
const queryController = new QueryController(environmentStore);
const compileController = new CompileController(environmentStore);
const materializationService = new MaterializationService(environmentStore);
const storageDestinationController = new StorageDestinationController(
   environmentStore,
);
const materializationController = new MaterializationController(
   materializationService,
);
/**
 * Start the standalone materialization scheduler, or return null when
 * `PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER` is off (the default, so an
 * orchestrated deployment never runs it). A function so tests can drive the
 * real env-var path; the module-level call below runs once at import.
 */
export function startMaterializationSchedulerFromEnv(
   store: EnvironmentStore,
   service: MaterializationService,
): MaterializationScheduler | null {
   const config = getMaterializationSchedulerConfig();
   if (!config) return null;
   const scheduler = new MaterializationScheduler(store, service, config);
   scheduler.start();
   return scheduler;
}

// The sweep timer is unref'd, so it never keeps the process alive.
startMaterializationSchedulerFromEnv(environmentStore, materializationService);
const themeStore = new ThemeStore(environmentStore.storageManager, SERVER_ROOT);
const themeController = new ThemeController(themeStore);

export const mcpApp = createMcpApp(environmentStore);

// The data-app static routes come before the SPA catch-all and (in dev) the
// Vite proxy.
registerDataAppStaticRoutes(
   app,
   environmentStore,
   path.join(path.dirname(__filename_esm), "runtime", "publisher.js"),
);

// In development the Console is proxied to the Vite dev server instead.
if (!isDevelopment) {
   app.use("/", express.static(ROOT));
   app.use("/api-doc.html", express.static(path.join(ROOT, "api-doc.html")));
} else {
   app.use(`${API_PREFIX}`, loggerMiddleware);
   app.use(
      createProxyMiddleware({
         target: "http://localhost:5173",
         changeOrigin: true,
         ws: true,
         pathFilter: (path) =>
            !path.startsWith("/api/") &&
            !path.startsWith("/metrics") &&
            !path.startsWith("/health"),
      }),
   );
}

app.use(
   cors({
      origin: "http://localhost:5173",
      credentials: true,
   }),
);

app.use(bodyParser.json({ limit: "1mb" }));

try {
   const metricsHandler = getPrometheusMetricsHandler();
   app.get("/metrics", metricsHandler);
   logger.info("Prometheus metrics endpoint registered at /metrics");
} catch (error) {
   logger.warn("Failed to register Prometheus metrics endpoint", { error });
}

// After the health endpoints, before every other route.
app.use(drainingGuard);

// Registered after CORS, the body parser and the draining guard, so
// cross-origin SDK consumers and authenticated requests both work.
registerDataAppListRoute(app, API_PREFIX, environmentStore);

registerApiRoutes(app, API_PREFIX, {
   environmentStore,
   themeStore,
   themeController,
   watchModeController,
   connectionController,
   packageController,
   modelController,
   skillController,
   reportController,
   databaseController,
   queryController,
   compileController,
   materializationController,
   storageDestinationController,
});

// Legacy `/projects/...` aliases; must precede the SPA catch-all.
registerLegacyRoutes(app, {
   environmentStore,
   connectionController,
   modelController,
   packageController,
   databaseController,
   queryController,
   compileController,
   materializationController,
});

if (!isDevelopment) {
   registerSpaFallback(app, API_PREFIX, ROOT, environmentStore);
}

app.use(unhandledErrorMiddleware);

// Constructed eagerly so a misconfigured PACKAGE_LOAD_WORKERS (e.g. 0) fails
// the boot rather than the first package load, possibly hours later.
{
   const { getPackageLoadPool } = await import(
      "./package_load/package_load_pool"
   );
   getPackageLoadPool();
}

const mainServer = http.createServer({ maxHeaderSize: 262144 }, app);

mainServer.timeout = 600000;
mainServer.keepAliveTimeout = 600000;
mainServer.headersTimeout = 600000;

// The .mcp.json write waits on this, so a process whose REST port fails to
// bind never leaves a .mcp.json pointing at a server about to exit.
let resolveRestBound: () => void = () => {};
const restBound = new Promise<void>((resolve) => {
   resolveRestBound = resolve;
});

mainServer.listen(PUBLISHER_PORT, PUBLISHER_HOST, async () => {
   resolveRestBound();
   const address = mainServer.address() as AddressInfo;
   logger.info(
      `Publisher server listening at http://${address.address}:${address.port}`,
   );
   if (isDevelopment) {
      logger.info(
         "Running in development mode - proxying to React dev server at http://localhost:5173",
      );
   }
   // `--watch-env` / PUBLISHER_WATCH: those envs' packages are mounted in
   // place (see `loadEnvironmentIntoDisk`), so the watcher sees source edits.
   const watchEnvList = settings.get("watchEnvironments");
   if (watchEnvList.length > 0) {
      // WatchModeController holds one watcher, so only the first env
      // auto-reloads; the rest are still mounted in place.
      if (watchEnvList.length > 1) {
         logger.warn(
            `Multiple watch environments requested (${watchEnvList.join(
               ", ",
            )}); watch mode auto-reloads one at a time. Watching "${
               watchEnvList[0]
            }". The others are mounted in place (their source is live) but will not auto-reload. Pass a single --watch-env (or one PUBLISHER_WATCH value) to silence this.`,
         );
      }
      const envName = watchEnvList[0];
      try {
         await environmentStore.finishedInitialization;
         await watchModeController.ensureWatching(envName);
         logger.info(
            `Watch mode active for environment "${envName}" (in-place mount, source-edit live reload).`,
         );
      } catch (error) {
         logger.error(
            `Failed to start watch mode for environment "${envName}"`,
            { error },
         );
      }
   }
});
const mcpServer = mcpApp.listen(
   MCP_PORT,
   MCP_HOST,
   function (this: import("net").Server) {
      // Read back rather than reusing MCP_PORT: `--mcp_port 0` binds an
      // ephemeral port.
      const boundPort = resolveBoundPort(this.address(), MCP_PORT);
      // The bind address, deliberately not resolveClientHost: the docs tell
      // readers this line is the address really bound, which is how they
      // catch a mistyped host falling back to 0.0.0.0.
      const bound = this.address();
      const boundHost =
         typeof bound === "object" && bound ? bound.address : MCP_HOST;
      logger.info(
         `MCP server listening at http://${boundHost.includes(":") ? `[${boundHost}]` : boundHost}:${boundPort}`,
      );
      // Checked before process.cwd(), which can throw: someone who turned the
      // feature off should not get a warning about it.
      if (MCP_CONFIG_ENABLED) {
         // Deferred until REST has also bound: the file is never rewritten
         // once created, so one pointing at a process that then died on
         // EADDRINUSE would break the next agent session.
         const boundAddress = this.address();
         void restBound.then(() => {
            // ensureMcpConfig cannot throw, but its arguments can
            // (process.cwd() raises ENOENT once the directory is removed), so
            // they are built inside the try.
            try {
               // Not `localhost`: it resolves to both loopback families while
               // the server binds one, so another process could get the traffic.
               const endpoint = mcpEndpoint(
                  resolveClientHost(boundAddress, MCP_HOST),
                  boundPort,
               );
               // cwd, not server_root: the file is for whoever opens an agent here.
               logMcpConfigOutcome(
                  ensureMcpConfig({
                     dir: process.cwd(),
                     endpoint,
                     requestedPort: MCP_PORT,
                     boundPort,
                  }),
               );
            } catch (error) {
               logger.info(
                  `Could not set up ${MCP_CONFIG_FILENAME} (${error instanceof Error ? error.message : String(error)}). To connect an agent, run: ${addCommand(mcpEndpoint(resolveClientHost(boundAddress, MCP_HOST), boundPort))}`,
               );
            }
         });
      }
   },
);

// One actionable line and a clean exit instead of an uncaught-'error' crash
// dump. The sibling is closed because the listens race: the other port may
// already be bound, and half a server must not linger.
function fatalListenError(
   label: string,
   requestedPort: number,
   flag: string,
   sibling: http.Server,
): (error: NodeJS.ErrnoException) => void {
   return (error) => {
      if (error.code === "EADDRINUSE") {
         logger.error(`Port ${requestedPort} in use; pass ${flag} <n>`);
      } else {
         logger.error(
            `${label} listener failed on port ${requestedPort}: ${error.message}`,
         );
      }
      try {
         sibling.close();
      } catch {
         // Best effort: the process is exiting either way.
      }
      process.exit(1);
   };
}
// Safe to attach now: 'error' is emitted on a later tick, never from listen().
mainServer.on(
   "error",
   fatalListenError("REST", PUBLISHER_PORT, "--port", mcpServer),
);
mcpServer.on(
   "error",
   fatalListenError("MCP", MCP_PORT, "--mcp_port", mainServer),
);

mcpServer.timeout = 600000;
mcpServer.keepAliveTimeout = 600000;
mcpServer.headersTimeout = 600000;

// The admin API (admin-api-doc.yaml): the DuckLake catalogs in the configured
// catalog database, and controls of this process an operator uses. A file load
// or a snapshot delete can run for minutes, hence the main API's long timeouts.
const adminServer =
   ADMIN_PORT === null
      ? null
      : createAdminApp(ADMIN_CATALOGS, watchModeController).listen(
           ADMIN_PORT,
           ADMIN_HOST,
           function (this: import("net").Server) {
              const bound = this.address();
              const host =
                 typeof bound === "object" && bound
                    ? bound.address
                    : ADMIN_HOST;
              logger.info(
                 `Admin server listening at http://${host.includes(":") ? `[${host}]` : host}:${resolveBoundPort(bound, ADMIN_PORT)}`,
              );
           },
        );
if (adminServer) {
   adminServer.on(
      "error",
      fatalListenError("Admin", ADMIN_PORT!, "--admin_port", mainServer),
   );
   adminServer.timeout = 600000;
   adminServer.keepAliveTimeout = 600000;
   adminServer.headersTimeout = 600000;
}

registerSignalHandlers(
   mainServer,
   mcpServer,
   SHUTDOWN_DRAIN_DURATION_SECONDS,
   SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS,
   adminServer ? [adminServer] : [],
);
