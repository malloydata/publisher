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

const app = express();
app.use(morgan("tiny"));

const PUBLISHER_PORT = Number(process.env.PUBLISHER_PORT || 4000);
const PUBLISHER_HOST = process.env.PUBLISHER_HOST || "localhost";
const ROOT = path.join(__dirname, "../../app/dist/");
const API_PREFIX = "/api/v0";
const PROJECT_NAME = "home";

const packageService = new PackageService();
const aboutController = new AboutController();
const modelController = new ModelController(packageService);
const packageController = new PackageController(packageService);
const queryController = new QueryController(packageService);
const scheduleController = new ScheduleController(packageService);

const mcpServer = initializeMcpServer(packageService);

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

// --- MCP Router Definition ---
// Create a separate Express app for MCP to avoid middleware interference
const mcpApp = express();

// No middleware is applied to mcpApp to ensure raw access to request/response

// Single transport instance for the MCP server
let mcpTransport: SSEServerTransport | null = null;

mcpApp.get('/sse', async (req, res) => {
   console.log(`SSE connection initiated from ${req.ip}`);
   try {
      // If there's an existing transport, close it first
      if (mcpTransport) {
         await mcpTransport.close();
      }

      const messagePath = `${API_PREFIX}/mcp/messages`;
      mcpTransport = new SSEServerTransport(messagePath, res);
      
      // Handle connection cleanup
      res.on("close", async () => {
         console.log(`SSE connection closed`);
         mcpTransport = null;
      });

      await mcpServer.connect(mcpTransport);
      console.log(`MCP Server connected to transport`);
   } catch (error) {
      console.error("Error setting up SSE connection:", error);
      if (!res.headersSent) {
         res.status(500).send("Failed to establish SSE connection");
      }
   }
});

mcpApp.post('/messages', async (req, res) => {
   if (!mcpTransport) {
      return res.status(400).send('No active SSE connection');
   }

   try {
      await mcpTransport.handlePostMessage(req, res);
      console.log(`Successfully handled POST message`);
   } catch (error) {
      console.error("Error handling POST message:", error);
      if (!res.headersSent) {
         res.status(500).send("Error handling message");
      }
   }
});

// --- Mount Routers (Keep AFTER definitions) ---
// Mount MCP app BEFORE REST router to avoid bodyParser conflict
app.use(`${API_PREFIX}/mcp`, mcpApp);
app.use(API_PREFIX, restApiRouter);

app.get("*", (_req: express.Request, res: express.Response) => res.sendFile(path.resolve(ROOT, "index.html")));

app.use((err: Error, _req: express.Request, res: express.Response) => {
  console.error("Unhandled error:", err);
  const { json, status } = internalErrorToHttpError(err);
  res.status(status).json(json);
});

const httpServer = http.createServer(app);

if (require.main === module) {
   httpServer.listen(PUBLISHER_PORT, PUBLISHER_HOST, () => {
      const address = httpServer.address() as AddressInfo;
      console.log(
         `Server is running at http://${address.address}:${address.port}`,
      );
   });
}

export { httpServer, app };
