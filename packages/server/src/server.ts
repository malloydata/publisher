import express, { Router } from "express";
import * as http from "http";
import { AddressInfo } from "net";
import * as path from "path";
import morgan from "morgan";
import * as bodyParser from "body-parser";
import { getWorkingDirectory } from "./utils";
import cors from "cors";
import * as fs from "fs";
import { internalErrorToHttpError, NotImplementedError } from "./errors";
import { PackageService } from "./service/package.service";
import { initializeMcpServer } from "./mcp/server";
import { SSEServerTransport } from "@modelcontextprotocol/sdk/server/sse.js";
import { AboutController } from "./controller/about.controller";
import { ModelController } from "./controller/model.controller";
import { PackageController } from "./controller/package.controller";
import { QueryController } from "./controller/query.controller";
import { ScheduleController } from "./controller/schedule.controller";
import { Server, createServer } from "http";

// Main app for REST API
const app = express();
app.use(morgan("tiny"));

const PUBLISHER_PORT = Number(process.env.PUBLISHER_PORT || 4000);
const PUBLISHER_HOST = process.env.PUBLISHER_HOST || "localhost";
const MCP_PORT = Number(process.env.MCP_PORT || 4001);
const ROOT = path.join(__dirname, "../../app/dist/");
const API_PREFIX = "/api/v0";
const PROJECT_NAME = "home";

const packageService = new PackageService();
const aboutController = new AboutController();
const modelController = new ModelController(packageService);
const packageController = new PackageController(packageService);
const queryController = new QueryController(packageService);
const scheduleController = new ScheduleController(packageService);

// Initialize MCP server and transport
const mcpServer = initializeMcpServer(packageService);

// Separate Express app for MCP
const mcpApp = express();
let mcpTransport: SSEServerTransport | null = null;

// Mount SSE endpoints directly
mcpApp.get('/', async (req, res) => {
  mcpTransport = new SSEServerTransport('/messages', res);
  await mcpServer.connect(mcpTransport);
});

mcpApp.post('/messages', async (req, res) => {
  if (mcpTransport) {
    await mcpTransport.handlePostMessage(req, res);
  } else {
    res.status(400).send(JSON.stringify({ error: 'No active SSE connection' }));
  }
});

// --- Static file serving (Keep before API routers) ---
app.use("/", express.static(path.join(ROOT, "/")));
app.use("/api-doc.html", express.static(path.join(ROOT, "/api-doc.html")));

if (!fs.existsSync(getWorkingDirectory())) {
   throw Error(
      "Server working directory does not exist: " + getWorkingDirectory(),
   );
}

const setVersionIdError = (res: express.Response) => {
   const { json, status } = internalErrorToHttpError(
      new NotImplementedError("Version IDs not implemented."),
   );
   res.status(status).json(json);
};

const setProjectNameError = (res: express.Response) => {
   const { json, status } = internalErrorToHttpError(
      new NotImplementedError(
         "Project names other than 'default' not implemented.",
      ),
   );
   res.status(status).json(json);
};

// --- REST API Router Definition ---
const restApiRouter = Router();

restApiRouter.use(cors());

restApiRouter.get('/projects', async (_req, res) => {
   try {
      res.status(200).json([{ name: PROJECT_NAME }]);
   } catch (error) {
      console.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

restApiRouter.get('/projects/:projectName/about', async (req, res) => {
   if (req.params.projectName !== PROJECT_NAME) {
      setProjectNameError(res);
      return;
   }

   try {
      res.status(200).json(await aboutController.getAbout());
   } catch (error) {
      console.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

restApiRouter.get('/projects/:projectName/packages', async (req, res) => {
   if (req.params.projectName !== PROJECT_NAME) {
      setProjectNameError(res);
      return;
   }
   if (req.query.versionId) {
      setVersionIdError(res);
      return;
   }

   try {
      res.status(200).json(await packageController.listPackages());
   } catch (error) {
      console.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

restApiRouter.get('/projects/:projectName/packages/:packageName', async (req, res) => {
   if (req.params.projectName !== PROJECT_NAME) {
      setProjectNameError(res);
      return;
   }
   if (req.query.versionId) {
      setVersionIdError(res);
      return;
   }

   try {
      const pkg = await packageService.getPackage(req.params.packageName);
      res.status(200).json(
         pkg.getPackageMetadata()
      );
   } catch (error) {
      console.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

restApiRouter.get('/projects/:projectName/packages/:packageName/models', async (req, res) => {
   if (req.params.projectName !== PROJECT_NAME) {
      setProjectNameError(res);
      return;
   }
   if (req.query.versionId) {
      setVersionIdError(res);
      return;
   }

   try {
      res.status(200).json(
         await modelController.listModels(req.params.packageName),
      );
   } catch (error) {
      console.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

restApiRouter.get('/projects/:projectName/packages/:packageName/models/*?', async (req, res) => {
   if (req.params.projectName !== PROJECT_NAME) {
      setProjectNameError(res);
      return;
   }
   if (req.query.versionId) {
      setVersionIdError(res);
      return;
   }

   try {
      const zero = 0 as unknown;
      res.status(200).json(
         await modelController.getModel(
            req.params.packageName,
            req.params[zero as keyof typeof req.params],
         ),
      );
   } catch (error) {
      console.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

restApiRouter.get('/projects/:projectName/packages/:packageName/queryResults/*?', async (req, res) => {
   if (req.params.projectName !== PROJECT_NAME) {
      setProjectNameError(res);
      return;
   }
   if (req.query.versionId) {
      setVersionIdError(res);
      return;
   }

   try {
      const zero = 0 as unknown;
      res.status(200).json(
         await queryController.getQuery(
            req.params.packageName,
            req.params[zero as keyof typeof req.params],
            req.query.sourceName as string,
            req.query.queryName as string,
            req.query.query as string,
         ),
      );
   } catch (error) {
      console.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

restApiRouter.get('/projects/:projectName/packages/:packageName/schedules', async (req, res) => {
   if (req.params.projectName !== PROJECT_NAME) {
      setProjectNameError(res);
      return;
   }
   if (req.query.versionId) {
      setVersionIdError(res);
      return;
   }

   try {
      res.status(200).json(await scheduleController.listSchedules(req.params.packageName));
   } catch (error) {
      console.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

restApiRouter.get(
   '/projects/:projectName/packages/:packageName/databases',
   async (req, res) => {
      if (req.params.projectName !== PROJECT_NAME) {
         setProjectNameError(res);
         return;
      }
      if (req.query.versionId) {
         setVersionIdError(res);
         return;
      }

      try {
         const pkg = await packageService.getPackage(req.params.packageName);
         res.status(200).json(
            pkg.listDatabases()
         );
      } catch (error) {
         console.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

// Mount REST API router
app.use(API_PREFIX, restApiRouter);

app.get("*", (_req: express.Request, res: express.Response) => res.sendFile(path.resolve(ROOT, "index.html")));

app.use((err: Error, _req: express.Request, res: express.Response) => {
  console.error("Unhandled error:", err);
  const { json, status } = internalErrorToHttpError(err);
  res.status(status).json(json);
});

// Start main server
const mainServer = app.listen(PUBLISHER_PORT, PUBLISHER_HOST, () => {
  console.log(`Publisher server listening at http://${PUBLISHER_HOST}:${PUBLISHER_PORT}`);
});

// Start MCP server on its own port
const mcpHttpServer = mcpApp.listen(MCP_PORT, PUBLISHER_HOST, () => {
  console.log(`MCP server listening at http://${PUBLISHER_HOST}:${MCP_PORT}`);
});

export { mainServer as httpServer, mcpHttpServer, app, mcpApp };
