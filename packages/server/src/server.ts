// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Refuse to run on an unsupported Node. Imported first, and importing nothing
// but node:fs itself, so the check pulls no application code into the graph.
// It does not run before that graph: ESM evaluates every import ahead of this
// module's body, and the bundler inlines this entry last, so the call below
// runs after each dependency's top-level code. What it does precede is
// everything this process chooses to do.
import { assertSupportedNodeVersion } from "./node_version_check";
import type { AddressInfo } from "net";
import express from "express";
import bodyParser from "body-parser";
import cors from "cors";
import * as http from "http";
import { createProxyMiddleware } from "http-proxy-middleware";
import * as path from "path";
import { fileURLToPath } from "url";
import { applyCliArgs } from "./cli_args";
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
import { createMcpApp, resolveMcpHost } from "./routes/mcp";
import { registerSpaFallback } from "./routes/spa";
import {
   RATE_LIMIT_ENV,
   parseRateLimit,
   rateLimitMiddleware,
} from "./rate_limit";
import {
   FRAME_ANCESTORS_ENV,
   frameAncestorsMiddleware,
} from "./frame_ancestors";
import { acceptBundledExperimentalDialects } from "./service/experimental_dialects";

// The first statement this module runs. On an unsupported Node this exits
// non-zero here, before any argument parsing, any storage init, and any
// listener. The floor is a support policy (see node_version_check.ts): the
// failure that exposed it surfaced only on the first query, as a 500 naming
// neither Node nor a version, on a server whose boot log read completely
// healthy. Bun is exempt, or the Docker image and `start:dev` would refuse to
// boot.
assertSupportedNodeVersion();

applyCliArgs(process.argv.slice(2));

// Fail fast at boot on a malformed PERSIST_STORAGE_MODE. The getter throws on an
// invalid value; without this it would only throw lazily inside a per-request
// package-metadata call (storageWarnings), on every deployment regardless of
// whether the tier is used. Reading it once here surfaces a typo at startup.
getPersistStorageMode();
// Same for PERSIST_COLLISION_ENFORCE, whose only other caller is the publish
// path — so a typo would otherwise surface as a failed publish request rather
// than a failed boot.
getPersistCollisionEnforce();

// Same hazard, wider blast radius: getQueryMetadataMode() throws on an invalid
// value and is read while resolving EVERY statement, so a typo'd off switch
// ("false", "0", "disabled") would boot clean and then fail every query and
// every build — the one thing the metadata path promises never to do.
getQueryMetadataMode();

// Before any compile on this thread: a connection-test, an MCP compile or a
// package load on the main thread would otherwise refuse a SQL Server model.
acceptBundledExperimentalDialects();

const PUBLISHER_PORT = Number(process.env.PUBLISHER_PORT || 4000);
const PUBLISHER_HOST = process.env.PUBLISHER_HOST || "0.0.0.0";
const MCP_PORT = Number(process.env.MCP_PORT || 4040);
// The MCP endpoint is unauthenticated and exposes every MCP tool, so it binds
// LOOPBACK by default while the REST port keeps its own PUBLISHER_HOST default.
// Precedence: MCP_HOST, then an explicit PUBLISHER_HOST (so `--host` still moves
// both listeners together for an operator who asked for a wider bind), then
// loopback. Widening this is opt-in and belongs behind an authenticating
// gateway.
const MCP_HOST = resolveMcpHost(process.env);
// Resolved here rather than in the listen callback: parseBoolEnv throws on a
// typo, which is the convention for flags in this server, but a throw inside a
// listen callback is an uncaughtException that kills a server which has already
// bound both ports. At module scope it is an ordinary startup failure.
const MCP_CONFIG_ENABLED = mcpConfigEnabled();
const SHUTDOWN_DRAIN_DURATION_SECONDS = Number(
   process.env.SHUTDOWN_DRAIN_DURATION_SECONDS || 0,
);
const SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS = Number(
   process.env.SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS || 0,
);
// Find the app directory relative to this bundled server file.
// Works under both ESM (import.meta.url) and when invoked via NPX.
const __filename_esm = fileURLToPath(import.meta.url);
const ROOT = path.join(path.dirname(__filename_esm), "app");
const SERVER_ROOT = path.resolve(process.cwd(), process.env.SERVER_ROOT || ".");
const API_PREFIX = "/api/v0";
const isDevelopment = process.env["NODE_ENV"] === "development";

export const app = express();
app.use(loggerMiddleware);
app.use(httpMetricsMiddleware);
// Opt-in per-client rate limiting (PUBLISHER_RATE_LIMIT). Mounted before any
// route so the static-file, query, and SPA-fallback handlers are all behind
// it; probes and /metrics are exempt inside the middleware.
// Ahead of the rate limiter, so a 429 carries the policy too. That response is
// JSON and nobody is tricked into clicking one, so this is not a live
// clickjacking path -- but "every document" should be literally true rather than
// true of everything except the responses one middleware happens to answer
// early, and the next early-answering middleware may not be JSON.
app.use(frameAncestorsMiddleware(process.env[FRAME_ANCESTORS_ENV]));
// Health endpoints (Kubernetes liveness/readiness) before every middleware a
// probe does not need: the rate limiter exempts them anyway, and static-file
// serving would stat the filesystem first. A liveness probe that queues behind
// a busy event loop should cost nothing more once it runs.
registerHealthEndpoints(app);
app.use(rateLimitMiddleware(parseRateLimit(process.env[RATE_LIMIT_ENV])));
// Probe the V8 heap ceiling once at startup and warn if it's below
// the recommended floor. The row/byte caps from Steps 1–3 still
// bound per-request memory; this is a "your --max-old-space-size
// looks low for the default caps" advisory so operators don't
// chase OOMKills before checking the obvious config.
checkHeapConfiguration();
export const environmentStore = new EnvironmentStore(SERVER_ROOT);
const watchModeController = new WatchModeController(environmentStore);
const connectionController = new ConnectionController(environmentStore);
const modelController = new ModelController(environmentStore);
// PackageMemoryGovernor is opt-in via PUBLISHER_MAX_MEMORY_BYTES.
// When set, it polls process RSS and flips an `isBackpressured` flag
// that Environment.getPackage / addPackage consult before allocating
// any new package — the server responds with HTTP 503 instead of
// OOM-killing the pod.
// Validate the DuckDB extension-fetch policy at boot so an unrecognised value
// (e.g. a k8s-manifest typo) fails the boot loudly here, rather than surfacing
// on the first query that resolves a DuckDB connection — which matters most for
// an operator relying on `local-only` for a no-network guarantee. Logging the
// resolved policy also records the posture the server booted with.
logger.info(`DuckDB extension-fetch policy: ${getExtensionFetchPolicy()}`);
// Validated and materialized here, not on the first session that opens one:
// `/health` and `/health/readiness` never touch DuckDB, so a malformed limit or
// an uncreatable spill directory would leave the pod reporting ready while every
// query and package load failed. Also creates the directory, since
// `SET temp_directory` accepts one that does not exist and only fails at the
// first spill.
assertDuckDBResourceConfig();
const duckDBMemoryLimit = getDuckDBMemoryLimit();
if (duckDBMemoryLimit === undefined && !isDuckDBMemoryLimitDisabled()) {
   // Warned rather than defaulted. A flat value that suits one container size
   // badly constrains another, so the safe value is the operator's to pick — but
   // an operator who never reads a release note would otherwise have no way to
   // learn that this process runs several DuckDB instances which each size
   // themselves against the whole container independently.
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
// Resolve the embedding config at boot so a malformed EMBEDDING_API_BASE /
// EMBEDDING_DIMENSIONS fails loudly at startup (getEmbeddingConfig throws),
// matching the sibling getters above, rather than surfacing as a warn on the
// first getContext call that reaches tier 4 — or never. Logs the posture the
// server booted with; the host only, never the key.
//
// The `retrieval` block of publisher.config.json is read here, once, so an
// invalid value is reported with its fix rather than surfacing on the first
// question. The provider getters read it back from module state.
//
// A config the server cannot read or parse must not kill the process: the
// environment store reads the same file when it initializes, records the cause
// as `initError`, prints PUBLISHER_INIT_FAILED and keeps the server up so
// /status can name it. This read therefore logs and carries on with no
// retrieval settings; the store reports the same failure right after.
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
// The entity cap for the semantic index.
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
// Start the semantic index building as each package loads, rather than on the
// first question. This only queues work (see startPackageEmbeddingSync); with
// no embedding provider the queued job returns at once.
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
 * Construct and start the standalone materialization scheduler from environment
 * config, or return null when the feature is disabled
 * (`PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER` unset/false — the default, so an
 * orchestrated deployment never runs it). Extracted from the module body so a
 * test can drive the real env-var → {@link getMaterializationSchedulerConfig} →
 * construct → `start()`/`unref()` → timer path an operator uses; the
 * module-level singleton below is armed once at import and can't be re-created
 * per test.
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

// Standalone materialization scheduler: opt-in via
// PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER (default off, so an orchestrated
// deployment — whose control plane drives materialization — never runs it). The
// sweep timer is `unref`'d, so it never keeps the process alive on shutdown.
startMaterializationSchedulerFromEnv(environmentStore, materializationService);
const themeStore = new ThemeStore(environmentStore.storageManager, SERVER_ROOT);
const themeController = new ThemeController(themeStore, SERVER_ROOT);

export const mcpApp = createMcpApp(environmentStore);

// The data-app static routes come before the SPA catch-all and (in dev) the
// Vite proxy.
registerDataAppStaticRoutes(
   app,
   environmentStore,
   path.join(path.dirname(__filename_esm), "runtime", "publisher.js"),
);

// Only serve static files in production mode
// Otherwise we proxy to the React dev server
if (!isDevelopment) {
   app.use("/", express.static(ROOT));
   app.use("/api-doc.html", express.static(path.join(ROOT, "api-doc.html")));
} else {
   // In development mode, proxy requests to React dev server
   // Handle API routes first
   app.use(`${API_PREFIX}`, loggerMiddleware);

   // Proxy everything else to Vite
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

// Set body-parser JSON limit to 1Mb (default: 100kb)
app.use(bodyParser.json({ limit: "1mb" }));

// Register Prometheus metrics endpoint
try {
   const metricsHandler = getPrometheusMetricsHandler();
   app.get("/metrics", metricsHandler);
   logger.info("Prometheus metrics endpoint registered at /metrics");
} catch (error) {
   logger.warn("Failed to register Prometheus metrics endpoint", { error });
}

// Register draining guard middleware - must be after health endpoints but before other routes
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

// Register legacy `/projects/...` routes for backwards compatibility with
// clients that haven't migrated to `/environments/...` yet. Must be added
// before the SPA catch-all below.
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

// Eagerly construct the package-load worker pool so we fail fast at
// boot if PACKAGE_LOAD_WORKERS is misconfigured (e.g. set to 0, the
// removed in-process fallback). Surfacing the bad config here is much
// friendlier than surfacing it on the first package load, which could
// be hours after start.
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

// Resolved from the REST listen callback. The .mcp.json write below waits on
// it, so a process whose REST port fails (the listener that dies is the one
// bound SECOND to a busy port pair) can never leave a fresh .mcp.json behind
// pointing at a server that is about to exit.
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
   // If `--watch-env <name>` (or PUBLISHER_WATCH=name1,name2) was passed,
   // wait for env initialization to settle, then start watch mode for each
   // named env. Packages in those envs are already mounted in-place via the
   // EnvironmentStore in-place path (see `loadEnvironmentIntoDisk`), so the
   // chokidar watcher will see edits to your source repo and fan them out
   // to any connected SSE clients.
   const watchEnvList = (process.env.PUBLISHER_WATCH || "")
      .split(",")
      .map((s) => s.trim())
      .filter((s) => s.length > 0);
   if (watchEnvList.length > 0) {
      // The watcher tracks exactly one env at a time (`WatchModeController`
      // holds a single chokidar instance). Every env in PUBLISHER_WATCH is
      // still mounted in place (live source) by the EnvironmentStore, but only
      // the first is watched, so the others do not auto-reload.
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
      // Read back rather than reusing MCP_PORT, which is only what was requested.
      // `--mcp_port 0` asks for any free port, and under bun a non-numeric value
      // binds an ephemeral one too, so the requested value can be 0 or NaN while
      // a real port is listening. The listening line uses it as well, which is
      // why it no longer reads `http://127.0.0.1:0`.
      const boundPort = resolveBoundPort(this.address(), MCP_PORT);
      // The BIND address, bracketed when it is an IPv6 literal so the URL
      // parses. Deliberately not resolveClientHost: create-malloy-package's
      // README and AGENTS template both tell readers these two listening lines
      // are "the addresses it really bound", and use them to catch a mistyped
      // --hostt that silently falls back to 0.0.0.0. Mapping the wildcard to
      // loopback here would confirm the mistake instead of revealing it. The
      // dialable form belongs in .mcp.json and in the advice, not here.
      const bound = this.address();
      const boundHost =
         typeof bound === "object" && bound ? bound.address : MCP_HOST;
      logger.info(
         `MCP server listening at http://${boundHost.includes(":") ? `[${boundHost}]` : boundHost}:${boundPort}`,
      );
      // Checked before process.cwd(), which can throw: someone who turned the
      // feature off should not get a warning about it.
      if (MCP_CONFIG_ENABLED) {
         // Deferred until the REST listener has ALSO bound. Both listens are
         // issued back-to-back, so this callback can run while the REST port is
         // about to fail EADDRINUSE; writing here used to leave a .mcp.json
         // pointing at a process that died moments later, and the next boot on
         // fresh ports skips the rewrite (create-never-edit), so the stale file
         // silently broke the NEXT agent session.
         const boundAddress = this.address();
         void restBound.then(() => {
            // ensureMcpConfig cannot throw, but its arguments can: process.cwd()
            // raises ENOENT once the working directory has been removed. A throw
            // here would be an unhandled rejection on a server that has already
            // bound both ports. Everything the call needs is built inside the
            // try for that reason, including the endpoint: it is the newest and
            // least-exercised code in this block.
            try {
               // The host an agent should dial, which is NOT `localhost`: that name
               // resolves to both loopback families while the server binds only one,
               // so another local process can hold the same port on the other family
               // and receive the agent's traffic instead.
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

// One actionable line and a clean exit for a listener that cannot bind,
// instead of the raw uncaught-'error' crash dump (a ~40-line stack trace with
// os.loadavg and memoryUsage for what is usually just a busy port). Closing
// the sibling listener matters beyond tidiness: the two listens race, so the
// OTHER port may already be bound and half a server must not linger.
// (Line comments, not a JSDoc, and no star-slash sequence anywhere in them:
// authorize_bypass_header.spec.ts strips block comments from this file with a
// regex that pairs a route string's slash-star with the next closer, so a
// block comment after the route table swallows the getQuery call it asserts
// on.)
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
// Attached after both servers exist (an 'error' event is emitted on a later
// tick, never synchronously out of listen(), so nothing is missed).
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

registerSignalHandlers(
   mainServer,
   mcpServer,
   SHUTDOWN_DRAIN_DURATION_SECONDS,
   SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS,
);
