// Pre-load the instrumentation module; the instrumentation module must be loaded before the other imports.
import "./instrumentation";
import {
   getPrometheusMetricsHandler,
   httpMetricsMiddleware,
} from "./instrumentation";

import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import * as bodyParser from "body-parser";
import cors from "cors";
import express from "express";
import * as http from "http";
import { createProxyMiddleware } from "http-proxy-middleware";
import { AddressInfo } from "net";
import * as path from "path";
import { CompileController } from "./controller/compile.controller";
import { ConnectionController } from "./controller/connection.controller";
import { DatabaseController } from "./controller/database.controller";
import { ModelController } from "./controller/model.controller";
import { PackageController } from "./controller/package.controller";
import { QueryController } from "./controller/query.controller";
import { WatchModeController } from "./controller/watch-mode.controller";
import {
   BadRequestError,
   internalErrorToHttpError,
   NotImplementedError,
} from "./errors";
import {
   drainingGuard,
   registerHealthEndpoints,
   registerSignalHandlers,
} from "./health";
import { logger, loggerMiddleware } from "./logger";

import { ManifestController } from "./controller/manifest.controller";
import { MaterializationController } from "./controller/materialization.controller";
import { initializeMcpServer } from "./mcp/server";
import { ManifestService } from "./service/manifest_service";
import { MaterializationService } from "./service/materialization_service";
import { EnvironmentStore } from "./service/environment_store";

/** Normalize an Express query param into a string[] or undefined. */
export function normalizeQueryArray(value: unknown): string[] | undefined {
   if (value === undefined || value === null) return undefined;
   if (Array.isArray(value)) return value.map(String);
   return [String(value)];
}

// Parse command line arguments
function parseArgs() {
   const args = process.argv.slice(2);
   for (let i = 0; i < args.length; i++) {
      const arg = args[i];
      if (arg === "--port" && args[i + 1]) {
         process.env.PUBLISHER_PORT = args[i + 1];
         i++;
      } else if (arg === "--host" && args[i + 1]) {
         process.env.PUBLISHER_HOST = args[i + 1];
         i++;
      } else if (arg === "--server_root" && args[i + 1]) {
         process.env.SERVER_ROOT = args[i + 1];
         i++;
      } else if (arg === "--mcp_port" && args[i + 1]) {
         process.env.MCP_PORT = args[i + 1];
         i++;
      } else if (arg === "--shutdown_drain_duration_seconds" && args[i + 1]) {
         process.env.SHUTDOWN_DRAIN_DURATION_SECONDS = args[i + 1];
         i++;
      } else if (
         arg === "--shutdown_graceful_close_timeout_seconds" &&
         args[i + 1]
      ) {
         process.env.SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS = args[i + 1];
         i++;
      } else if (arg === "--init") {
         process.env.INITIALIZE_STORAGE = "true";
      } else if (arg === "--help" || arg === "-h") {
         console.log("Malloy Publisher Server");
         console.log("");
         console.log("Usage: malloy-publisher [options]");
         console.log("");
         console.log("Options:");
         console.log(
            "  --port <number>        Port to run the server on (default: 4000)",
         );
         console.log(
            "  --host <string>        Host to bind the server to (default: localhost)",
         );
         console.log(
            "  --server_root <path>   Root directory to serve files from (default: .)",
         );
         console.log(
            "  --mcp_port <number>    Port for MCP server (default: 4040)",
         );
         console.log(
            "  --shutdown_drain_duration_seconds <number>  Time in seconds to keep service in draining state before closing servers (default: 0)",
         );
         console.log(
            "  --shutdown_graceful_close_timeout_seconds <number>  Time in seconds to wait after closing servers before exit (default: 0)",
         );
         console.log(
            "  --init                 Initialize the storage (default: false)",
         );
         console.log("  --help, -h             Show this help message");
         process.exit(0);
      }
   }
}

// Parse CLI arguments before setting up constants
parseArgs();

const PUBLISHER_PORT = Number(process.env.PUBLISHER_PORT || 4000);
const PUBLISHER_HOST = process.env.PUBLISHER_HOST || "0.0.0.0";
const MCP_PORT = Number(process.env.MCP_PORT || 4040);
const MCP_ENDPOINT = "/mcp";
const SHUTDOWN_DRAIN_DURATION_SECONDS = Number(
   process.env.SHUTDOWN_DRAIN_DURATION_SECONDS || 0,
);
const SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS = Number(
   process.env.SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS || 0,
);
// Find the app directory - handle NPX vs local execution
let ROOT: string;
if (require.main) {
   // Use the main module's directory (works for NPX and direct execution)
   ROOT = path.join(path.dirname(require.main.filename), "app");
} else {
   // Fallback to current script directory
   ROOT = path.join(path.dirname(process.argv[1] || __filename), "app");
}
const SERVER_ROOT = path.resolve(process.cwd(), process.env.SERVER_ROOT || ".");
const API_PREFIX = "/api/v0";
const isDevelopment = process.env["NODE_ENV"] === "development";

export const app = express();
app.use(loggerMiddleware);
app.use(httpMetricsMiddleware);
const environmentStore = new EnvironmentStore(SERVER_ROOT);
const manifestService = new ManifestService(environmentStore);
const watchModeController = new WatchModeController(environmentStore);
const connectionController = new ConnectionController(environmentStore);
const modelController = new ModelController(environmentStore);
const packageController = new PackageController(
   environmentStore,
   manifestService,
);
const databaseController = new DatabaseController(environmentStore);
const queryController = new QueryController(environmentStore);
const compileController = new CompileController(environmentStore);
const materializationService = new MaterializationService(
   environmentStore,
   manifestService,
);
const materializationController = new MaterializationController(
   materializationService,
);
const manifestController = new ManifestController(
   environmentStore,
   manifestService,
);

export const mcpApp = express();

// Register health endpoints on mcpApp (for E2E tests)
registerHealthEndpoints(mcpApp);

mcpApp.use(MCP_ENDPOINT, express.json());
mcpApp.use(MCP_ENDPOINT, cors());

mcpApp.all(MCP_ENDPOINT, async (req, res) => {
   logger.info(`[MCP Debug] Handling ${req.method} (Stateless)`);

   try {
      if (req.method === "POST") {
         const transport = new StreamableHTTPServerTransport({
            sessionIdGenerator: undefined,
         });

         transport.onclose = () => {
            logger.info(
               `[MCP Transport Info] Stateless transport closed for a request.`,
            );
         };
         transport.onerror = (err: Error) => {
            logger.error(`[MCP Transport Error] Stateless transport error:`, {
               error: err,
            });
         };

         const requestMcpServer = initializeMcpServer(environmentStore);
         await requestMcpServer.connect(transport);

         res.on("close", () => {
            logger.info(
               "[MCP Transport Info] Response closed, cleaning up stateless transport.",
            );
            transport.close().catch((err) => {
               logger.error(
                  "[MCP Transport Error] Error closing stateless transport on response close:",
                  { error: err },
               );
            });
         });

         await transport.handleRequest(req, res, req.body);
      } else if (req.method === "GET" || req.method === "DELETE") {
         logger.warn(
            `[MCP Transport Warn] Method Not Allowed in Stateless Mode: ${req.method}`,
         );
         res.setHeader("Allow", "POST");
         res.status(405).json({
            jsonrpc: "2.0",
            error: {
               code: -32601,
               message: "Method Not Allowed in Stateless Mode",
            },
            id: null,
         });
         return;
      } else {
         logger.warn(`[MCP Transport Warn] Method Not Allowed: ${req.method}`);
         res.setHeader("Allow", "POST");
         res.status(405).json({
            jsonrpc: "2.0",
            error: { code: -32601, message: "Method Not Allowed" },
            id: null,
         });
         return;
      }
   } catch (error) {
      logger.error(
         `[MCP Transport Error] Unhandled error in ${req.method} handler (Stateless):`,
         { error },
      );
      if (!res.headersSent) {
         res.status(500).json({
            jsonrpc: "2.0",
            error: { code: -32603, message: "Internal server error" },
            id:
               typeof req.body === "object" &&
               req.body !== null &&
               "id" in req.body
                  ? req.body.id
                  : null,
         });
      }
   }
});

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

const setVersionIdError = (res: express.Response) => {
   const { json, status } = internalErrorToHttpError(
      new NotImplementedError("Version IDs not implemented."),
   );
   res.status(status).json(json);
};

app.use(
   cors({
      origin: "http://localhost:5173",
      credentials: true,
   }),
);
app.use(bodyParser.json());

// Register health check endpoints on main app:
// - Required for production/Kubernetes monitoring (main server on PUBLISHER_PORT)
registerHealthEndpoints(app);

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

app.get(`${API_PREFIX}/status`, async (_req, res) => {
   try {
      const status = await environmentStore.getStatus();
      res.status(200).json(status);
   } catch (error) {
      logger.error("Error getting status", { error });
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

app.get(`${API_PREFIX}/watch-mode/status`, watchModeController.getWatchStatus);
app.post(`${API_PREFIX}/watch-mode/start`, watchModeController.startWatching);
app.post(`${API_PREFIX}/watch-mode/stop`, watchModeController.stopWatchMode);

app.get(`${API_PREFIX}/environments`, async (_req, res) => {
   try {
      res.status(200).json(await environmentStore.listEnvironments());
   } catch (error) {
      logger.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

app.post(`${API_PREFIX}/environments`, async (req, res) => {
   try {
      logger.info("Adding environment", { body: req.body });
      const environment = await environmentStore.addEnvironment(req.body);
      res.status(200).json(await environment.serialize());
   } catch (error) {
      logger.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

app.get(`${API_PREFIX}/environments/:environmentName`, async (req, res) => {
   try {
      const environment = await environmentStore.getEnvironment(
         req.params.environmentName,
         req.query.reload === "true",
      );
      res.status(200).json(await environment.serialize());
   } catch (error) {
      logger.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

app.patch(`${API_PREFIX}/environments/:environmentName`, async (req, res) => {
   try {
      const environment = await environmentStore.updateEnvironment(req.body);
      res.status(200).json(await environment.serialize());
   } catch (error) {
      logger.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

app.delete(`${API_PREFIX}/environments/:environmentName`, async (req, res) => {
   try {
      const environment = await environmentStore.deleteEnvironment(
         req.params.environmentName,
      );
      res.status(200).json(await environment?.serialize());
   } catch (error) {
      logger.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

app.get(
   `${API_PREFIX}/environments/:environmentName/connections`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.listConnections(
               req.params.environmentName,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/connections/:connectionName`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.getConnection(
               req.params.environmentName,
               req.params.connectionName,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.post(
   `${API_PREFIX}/environments/:environmentName/connections/:connectionName`,
   async (req, res) => {
      try {
         const result = await connectionController.addConnection(
            req.params.environmentName,
            req.params.connectionName,
            req.body,
         );
         res.status(201).json(result);
      } catch (error) {
         logger.error("Error creating connection", { error });
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.patch(
   `${API_PREFIX}/environments/:environmentName/connections/:connectionName`,
   async (req, res) => {
      try {
         const result = await connectionController.updateConnection(
            req.params.environmentName,
            req.params.connectionName,
            req.body,
         );
         res.status(200).json(result);
      } catch (error) {
         logger.error("Error updating connection", { error });
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.delete(
   `${API_PREFIX}/environments/:environmentName/connections/:connectionName`,
   async (req, res) => {
      try {
         const result = await connectionController.deleteConnection(
            req.params.environmentName,
            req.params.connectionName,
         );
         res.status(200).json(result);
      } catch (error) {
         logger.error("Error deleting connection", { error });
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.post(`${API_PREFIX}/connections/test`, async (req, res) => {
   try {
      const connectionStatus =
         await connectionController.testConnectionConfiguration(req.body);
      res.status(200).json(connectionStatus);
   } catch (error) {
      logger.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

app.get(
   `${API_PREFIX}/environments/:environmentName/connections/:connectionName/schemas`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.listSchemas(
               req.params.environmentName,
               req.params.connectionName,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/connections/:connectionName/schemas/:schemaName/tables`,
   async (req, res) => {
      logger.info("req.params", { params: req.params });
      try {
         const results = await connectionController.listTables(
            req.params.environmentName,
            req.params.connectionName,
            req.params.schemaName,
            normalizeQueryArray(req.query.tableNames),
         );
         res.status(200).json(results);
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/connections/:connectionName/schemas/:schemaName/tables/:tablePath`,
   async (req, res) => {
      logger.info("req.params", { params: req.params });
      try {
         const results = await connectionController.getTable(
            req.params.environmentName,
            req.params.connectionName,
            req.params.schemaName,
            req.params.tablePath,
         );
         res.status(200).json(results);
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

/**
 * @deprecated Use /environments/:environmentName/connections/:connectionName/sqlSource POST method instead
 */
app.get(
   `${API_PREFIX}/environments/:environmentName/connections/:connectionName/sqlSource`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.getConnectionSqlSource(
               req.params.environmentName,
               req.params.connectionName,
               req.query.sqlStatement as string,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.post(
   `${API_PREFIX}/environments/:environmentName/connections/:connectionName/sqlSource`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.getConnectionSqlSource(
               req.params.environmentName,
               req.params.connectionName,
               req.body.sqlStatement as string,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

/**
 * @deprecated Use /environments/:environmentName/connections/:connectionName/queryData POST method instead
 */
app.get(
   `${API_PREFIX}/environments/:environmentName/connections/:connectionName/queryData`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.getConnectionQueryData(
               req.params.environmentName,
               req.params.connectionName,
               req.query.sqlStatement as string,
               req.query.options as string,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.post(
   `${API_PREFIX}/environments/:environmentName/connections/:connectionName/sqlQuery`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.getConnectionQueryData(
               req.params.environmentName,
               req.params.connectionName,
               req.body.sqlStatement as string,
               req.query.options as string,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

/**
 * @deprecated Use /environments/:environmentName/connections/:connectionName/temporaryTable POST method instead
 */
app.get(
   `${API_PREFIX}/environments/:environmentName/connections/:connectionName/temporaryTable`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.getConnectionTemporaryTable(
               req.params.environmentName,
               req.params.connectionName,
               req.query.sqlStatement as string,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.post(
   `${API_PREFIX}/environments/:environmentName/connections/:connectionName/sqlTemporaryTable`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.getConnectionTemporaryTable(
               req.params.environmentName,
               req.params.connectionName,
               req.body.sqlStatement as string,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/packages`,
   async (req, res) => {
      if (req.query.versionId) {
         setVersionIdError(res);
         return;
      }

      try {
         res.status(200).json(
            await packageController.listPackages(req.params.environmentName),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.post(
   `${API_PREFIX}/environments/:environmentName/packages`,
   async (req, res) => {
      try {
         const autoLoadManifest = req.query.autoLoadManifest === "true";
         const _package = await packageController.addPackage(
            req.params.environmentName,
            req.body,
            { autoLoadManifest },
         );
         res.status(200).json(_package?.getPackageMetadata());
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName`,
   async (req, res) => {
      if (req.query.versionId) {
         setVersionIdError(res);
         return;
      }

      try {
         res.status(200).json(
            await packageController.getPackage(
               req.params.environmentName,
               req.params.packageName,
               req.query.reload === "true",
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.patch(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName`,
   async (req, res) => {
      try {
         res.status(200).json(
            await packageController.updatePackage(
               req.params.environmentName,
               req.params.packageName,
               req.body,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.delete(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName`,
   async (req, res) => {
      try {
         res.status(200).json(
            await packageController.deletePackage(
               req.params.environmentName,
               req.params.packageName,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/models`,
   async (req, res) => {
      if (req.query.versionId) {
         setVersionIdError(res);
         return;
      }

      try {
         res.status(200).json(
            await modelController.listModels(
               req.params.environmentName,
               req.params.packageName,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/models/*?`,
   async (req, res) => {
      if (req.query.versionId) {
         setVersionIdError(res);
         return;
      }

      try {
         // Express stores wildcard matches in params['0']
         const modelPath = (req.params as Record<string, string>)["0"];
         res.status(200).json(
            await modelController.getModel(
               req.params.environmentName,
               req.params.packageName,
               modelPath,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/notebooks`,
   async (req, res) => {
      if (req.query.versionId) {
         setVersionIdError(res);
         return;
      }

      try {
         res.status(200).json(
            await modelController.listNotebooks(
               req.params.environmentName,
               req.params.packageName,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

// Execute notebook cell route must come BEFORE the general get notebook route
// to avoid the wildcard matching incorrectly
app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/notebooks/*/cells/:cellIndex`,
   async (req, res) => {
      if (req.query.versionId) {
         setVersionIdError(res);
         return;
      }

      try {
         const cellIndex = parseInt(req.params.cellIndex, 10);
         if (isNaN(cellIndex)) {
            res.status(400).json({
               error: "Invalid cell index",
            });
            return;
         }

         // Express stores wildcard matches in params['0']
         const notebookPath = (req.params as Record<string, string>)["0"];

         // Parse optional filter_params (JSON query string) and bypass_filters
         let filterParams: Record<string, string | string[]> | undefined;
         if (typeof req.query.filter_params === "string") {
            try {
               filterParams = JSON.parse(req.query.filter_params);
            } catch {
               res.status(400).json({
                  error: "Invalid filter_params: must be valid JSON",
               });
               return;
            }
         }
         const bypassFilters =
            req.query.bypass_filters === "true" ? true : undefined;

         res.status(200).json(
            await modelController.executeNotebookCell(
               req.params.environmentName,
               req.params.packageName,
               notebookPath,
               cellIndex,
               filterParams,
               bypassFilters,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/notebooks/*?`,
   async (req, res) => {
      if (req.query.versionId) {
         setVersionIdError(res);
         return;
      }

      try {
         // Express stores wildcard matches in params['0']
         const notebookPath = (req.params as Record<string, string>)["0"];
         res.status(200).json(
            await modelController.getNotebook(
               req.params.environmentName,
               req.params.packageName,
               notebookPath,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.post(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/models/*?/query`,
   async (req, res) => {
      if (req.body.versionId) {
         setVersionIdError(res);
         return;
      }

      try {
         // Express stores wildcard matches in params['0']
         const modelPath = (req.params as Record<string, string>)["0"];
         res.status(200).json(
            await queryController.getQuery(
               req.params.environmentName,
               req.params.packageName,
               modelPath,
               req.body.sourceName as string,
               req.body.queryName as string,
               req.body.query as string,
               req.body.compactJson === true,
               (req.body.filterParams ?? req.body.sourceFilters) as
                  | Record<string, string | string[]>
                  | undefined,
               req.body.bypassFilters === true ? true : undefined,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/databases`,
   async (req, res) => {
      if (req.query.versionId) {
         setVersionIdError(res);
         return;
      }

      try {
         res.status(200).json(
            await databaseController.listDatabases(
               req.params.environmentName,
               req.params.packageName,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.post(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/models/:modelName/compile`,
   async (req, res) => {
      try {
         const result = await compileController.compile(
            req.params.environmentName,
            req.params.packageName,
            req.params.modelName,
            req.body.source,
            req.body.includeSql === true,
         );
         res.status(200).json(result);
      } catch (error) {
         logger.error("Compilation error", { error });
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

// ==================== MATERIALIZATION ROUTES ====================

app.post(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/materializations`,
   async (req, res) => {
      try {
         const build = await materializationController.createMaterialization(
            req.params.environmentName,
            req.params.packageName,
            req.body || {},
         );
         res.status(201).json(build);
      } catch (error) {
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/materializations`,
   async (req, res) => {
      try {
         const limit = req.query.limit
            ? parseInt(req.query.limit as string, 10)
            : undefined;
         const offset = req.query.offset
            ? parseInt(req.query.offset as string, 10)
            : undefined;
         const builds = await materializationController.listMaterializations(
            req.params.environmentName,
            req.params.packageName,
            { limit, offset },
         );
         res.status(200).json(builds);
      } catch (error) {
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/materializations/:materializationId`,
   async (req, res) => {
      try {
         const build = await materializationController.getMaterialization(
            req.params.environmentName,
            req.params.packageName,
            req.params.materializationId,
         );
         res.status(200).json(build);
      } catch (error) {
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.post(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/materializations/teardown`,
   async (req, res) => {
      try {
         const result = await materializationController.teardownPackage(
            req.params.environmentName,
            req.params.packageName,
            req.body || {},
         );
         res.status(200).json(result);
      } catch (error) {
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.post(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/materializations/:materializationId`,
   async (req, res) => {
      try {
         const action = req.query.action;
         if (action === "start") {
            const build = await materializationController.startMaterialization(
               req.params.environmentName,
               req.params.packageName,
               req.params.materializationId,
            );
            res.status(202).json(build);
         } else if (action === "stop") {
            const build = await materializationController.stopMaterialization(
               req.params.environmentName,
               req.params.packageName,
               req.params.materializationId,
            );
            res.status(200).json(build);
         } else {
            throw new BadRequestError(
               `Unsupported action '${String(action ?? "")}'. Expected 'start' or 'stop'.`,
            );
         }
      } catch (error) {
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.delete(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/materializations/:materializationId`,
   async (req, res) => {
      try {
         await materializationController.deleteMaterialization(
            req.params.environmentName,
            req.params.packageName,
            req.params.materializationId,
         );
         res.status(204).send();
      } catch (error) {
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

// ==================== MANIFEST ROUTES ====================

app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/manifest`,
   async (req, res) => {
      try {
         const manifest = await manifestController.getManifest(
            req.params.environmentName,
            req.params.packageName,
         );
         res.status(200).json(manifest);
      } catch (error) {
         logger.error("Get manifest error", { error });
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.post(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/manifest`,
   async (req, res) => {
      try {
         const action = req.query.action;
         if (action === "reload") {
            const manifest = await manifestController.reloadManifest(
               req.params.environmentName,
               req.params.packageName,
            );
            res.status(200).json(manifest);
         } else {
            throw new BadRequestError(
               `Unsupported action '${String(action ?? "")}'. Expected 'reload'.`,
            );
         }
      } catch (error) {
         logger.error("Manifest action error", { error });
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

// Modify the catch-all route to only serve index.html in production
if (!isDevelopment) {
   app.get("*", (_req, res) => res.sendFile(path.resolve(ROOT, "index.html")));
}

app.use(
   (
      err: Error,
      _req: express.Request,
      res: express.Response,
      _next: express.NextFunction,
   ) => {
      logger.error("Unhandled error:", err);
      const { json, status } = internalErrorToHttpError(err);
      res.status(status).json(json);
   },
);

const mainServer = http.createServer({ maxHeaderSize: 262144 }, app);

mainServer.timeout = 600000;
mainServer.keepAliveTimeout = 600000;
mainServer.headersTimeout = 600000;

mainServer.listen(PUBLISHER_PORT, PUBLISHER_HOST, () => {
   const address = mainServer.address() as AddressInfo;
   logger.info(
      `Publisher server listening at http://${address.address}:${address.port}`,
   );
   if (isDevelopment) {
      logger.info(
         "Running in development mode - proxying to React dev server at http://localhost:5173",
      );
   }
});
const mcpServer = mcpApp.listen(MCP_PORT, PUBLISHER_HOST, () => {
   logger.info(`MCP server listening at http://${PUBLISHER_HOST}:${MCP_PORT}`);
});

mcpServer.timeout = 600000;
mcpServer.keepAliveTimeout = 600000;
mcpServer.headersTimeout = 600000;

registerSignalHandlers(
   mainServer,
   mcpServer,
   SHUTDOWN_DRAIN_DURATION_SECONDS,
   SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS,
);
