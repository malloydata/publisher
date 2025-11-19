import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import * as bodyParser from "body-parser";
import cors from "cors";
import express from "express";
import * as http from "http";
import { createProxyMiddleware } from "http-proxy-middleware";
import { AddressInfo } from "net";
import * as path from "path";
import { ConnectionController } from "./controller/connection.controller";
import { DatabaseController } from "./controller/database.controller";
import { ModelController } from "./controller/model.controller";
import { PackageController } from "./controller/package.controller";
import { QueryController } from "./controller/query.controller";
import { WatchModeController } from "./controller/watch-mode.controller";
import { internalErrorToHttpError, NotImplementedError } from "./errors";
import { logger, loggerMiddleware } from "./logger";
import { initializeMcpServer } from "./mcp/server";
import { ProjectStore } from "./service/project_store";

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

const app = express();
app.use(loggerMiddleware);

const projectStore = new ProjectStore(SERVER_ROOT);
const watchModeController = new WatchModeController(projectStore);
const connectionController = new ConnectionController(projectStore);
const modelController = new ModelController(projectStore);
const packageController = new PackageController(projectStore);
const databaseController = new DatabaseController(projectStore);
const queryController = new QueryController(projectStore);

export const mcpApp = express();

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

         const requestMcpServer = initializeMcpServer(projectStore);
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
         pathFilter: (path) => !path.startsWith("/api/"),
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

app.get(`${API_PREFIX}/status`, async (_req, res) => {
   try {
      const status = await projectStore.getStatus();
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

app.get(`${API_PREFIX}/projects`, async (_req, res) => {
   try {
      res.status(200).json(await projectStore.listProjects());
   } catch (error) {
      logger.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

app.post(`${API_PREFIX}/projects`, async (req, res) => {
   try {
      logger.info("Adding project", { body: req.body });
      const project = await projectStore.addProject(req.body);
      res.status(200).json(await project.serialize());
   } catch (error) {
      logger.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

app.get(`${API_PREFIX}/projects/:projectName`, async (req, res) => {
   try {
      const project = await projectStore.getProject(
         req.params.projectName,
         req.query.reload === "true",
      );
      res.status(200).json(await project.serialize());
   } catch (error) {
      logger.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

app.patch(`${API_PREFIX}/projects/:projectName`, async (req, res) => {
   try {
      const project = await projectStore.updateProject(req.body);
      res.status(200).json(await project.serialize());
   } catch (error) {
      logger.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

app.delete(`${API_PREFIX}/projects/:projectName`, async (req, res) => {
   try {
      const project = await projectStore.deleteProject(req.params.projectName);
      res.status(200).json(await project?.serialize());
   } catch (error) {
      logger.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

app.get(`${API_PREFIX}/projects/:projectName/connections`, async (req, res) => {
   try {
      res.status(200).json(
         await connectionController.listConnections(req.params.projectName),
      );
   } catch (error) {
      logger.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

app.get(
   `${API_PREFIX}/projects/:projectName/connections/:connectionName`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.getConnection(
               req.params.projectName,
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
   `${API_PREFIX}/projects/:projectName/connections/:connectionName/schemas`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.listSchemas(
               req.params.projectName,
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
   `${API_PREFIX}/projects/:projectName/connections/:connectionName/schemas/:schemaName/tables`,
   async (req, res) => {
      logger.info("req.params", { params: req.params });
      try {
         const results = await connectionController.listTables(
            req.params.projectName,
            req.params.connectionName,
            req.params.schemaName,
         );
         logger.info("results", { results });
         res.status(200).json(results);
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/projects/:projectName/connections/:connectionName/schemas/:schemaName/tables/:tablePath`,
   async (req, res) => {
      logger.info("req.params", { params: req.params });
      try {
         const results = await connectionController.getTable(
            req.params.projectName,
            req.params.connectionName,
            req.params.schemaName,
            req.params.tablePath,
         );
         logger.info("results", { results });
         res.status(200).json(results);
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

/**
 * @deprecated Use /projects/:projectName/connections/:connectionName/sqlSource POST method instead
 */
app.get(
   `${API_PREFIX}/projects/:projectName/connections/:connectionName/sqlSource`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.getConnectionSqlSource(
               req.params.projectName,
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
   `${API_PREFIX}/projects/:projectName/connections/:connectionName/sqlSource`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.getConnectionSqlSource(
               req.params.projectName,
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
   `${API_PREFIX}/projects/:projectName/connections/:connectionName/tableSource`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.getConnectionTableSource(
               req.params.projectName,
               req.params.connectionName,
               req.query.tableKey as string,
               req.query.tablePath as string,
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
 * @deprecated Use /projects/:projectName/connections/:connectionName/queryData POST method instead
 */
app.get(
   `${API_PREFIX}/projects/:projectName/connections/:connectionName/queryData`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.getConnectionQueryData(
               req.params.projectName,
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
   `${API_PREFIX}/projects/:projectName/connections/:connectionName/sqlQuery`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.getConnectionQueryData(
               req.params.projectName,
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
 * @deprecated Use /projects/:projectName/connections/:connectionName/temporaryTable POST method instead
 */
app.get(
   `${API_PREFIX}/projects/:projectName/connections/:connectionName/temporaryTable`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.getConnectionTemporaryTable(
               req.params.projectName,
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
   `${API_PREFIX}/projects/:projectName/connections/:connectionName/sqlTemporaryTable`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.getConnectionTemporaryTable(
               req.params.projectName,
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

app.get(`${API_PREFIX}/projects/:projectName/packages`, async (req, res) => {
   if (req.query.versionId) {
      setVersionIdError(res);
      return;
   }

   try {
      res.status(200).json(
         await packageController.listPackages(req.params.projectName),
      );
   } catch (error) {
      logger.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

app.post(`${API_PREFIX}/projects/:projectName/packages`, async (req, res) => {
   try {
      const _package = await packageController.addPackage(
         req.params.projectName,
         req.body,
      );
      res.status(200).json(_package?.getPackageMetadata());
   } catch (error) {
      logger.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

app.get(
   `${API_PREFIX}/projects/:projectName/packages/:packageName`,
   async (req, res) => {
      if (req.query.versionId) {
         setVersionIdError(res);
         return;
      }

      try {
         res.status(200).json(
            await packageController.getPackage(
               req.params.projectName,
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
   `${API_PREFIX}/projects/:projectName/packages/:packageName`,
   async (req, res) => {
      try {
         res.status(200).json(
            await packageController.updatePackage(
               req.params.projectName,
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
   `${API_PREFIX}/projects/:projectName/packages/:packageName`,
   async (req, res) => {
      try {
         res.status(200).json(
            await packageController.deletePackage(
               req.params.projectName,
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
   `${API_PREFIX}/projects/:projectName/packages/:packageName/models`,
   async (req, res) => {
      if (req.query.versionId) {
         setVersionIdError(res);
         return;
      }

      try {
         res.status(200).json(
            await modelController.listModels(
               req.params.projectName,
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
   `${API_PREFIX}/projects/:projectName/packages/:packageName/models/*?`,
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
               req.params.projectName,
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
   `${API_PREFIX}/projects/:projectName/packages/:packageName/notebooks`,
   async (req, res) => {
      if (req.query.versionId) {
         setVersionIdError(res);
         return;
      }

      try {
         res.status(200).json(
            await modelController.listNotebooks(
               req.params.projectName,
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
   `${API_PREFIX}/projects/:projectName/packages/:packageName/notebooks/*/cells/:cellIndex`,
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

         res.status(200).json(
            await modelController.executeNotebookCell(
               req.params.projectName,
               req.params.packageName,
               notebookPath,
               cellIndex,
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
   `${API_PREFIX}/projects/:projectName/packages/:packageName/notebooks/*?`,
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
               req.params.projectName,
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
   `${API_PREFIX}/projects/:projectName/packages/:packageName/models/*?/query`,
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
               req.params.projectName,
               req.params.packageName,
               modelPath,
               req.body.sourceName as string,
               req.body.queryName as string,
               req.body.query as string,
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
   `${API_PREFIX}/projects/:projectName/packages/:packageName/databases`,
   async (req, res) => {
      if (req.query.versionId) {
         setVersionIdError(res);
         return;
      }

      try {
         res.status(200).json(
            await databaseController.listDatabases(
               req.params.projectName,
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

const mainServer = http.createServer(app);

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
