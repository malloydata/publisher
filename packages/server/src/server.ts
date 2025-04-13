import express, { Router } from "express";
import * as http from "http";
import { AddressInfo } from "net";
import * as path from "path";
// import morgan from "morgan"; // Comment out
import * as bodyParser from "body-parser";
import { getWorkingDirectory } from "./utils";
// import cors from "cors"; // Comment out
import * as fs from "fs";
import { internalErrorToHttpError, NotImplementedError } from "./errors";
import { PackageService } from "./service/package.service";
import { initializeMcpServer } from "./mcp/server";
import { SSEServerTransport } from "@modelcontextprotocol/sdk/server/sse.js";

const app = express();
// app.use(morgan("tiny")); // Comment out

const PUBLISHER_PORT = Number(process.env.PUBLISHER_PORT || 4000);
const PUBLISHER_HOST = process.env.PUBLISHER_HOST || "localhost";
const ROOT = path.join(__dirname, "../../app/dist/");
const API_PREFIX = "/api/v0";

const packageService = new PackageService();

// Initialize MCP Server
const mcpServer = initializeMcpServer(packageService);

// Transport management object as in the example
const transports: {[sessionId: string]: SSEServerTransport} = {};

// app.use(cors()); // Comment out
app.use("/", express.static(path.join(ROOT, "/")));
app.use("/api-doc.html", express.static(path.join(ROOT, "/api-doc.html")));

// Validate working directory exists or throw an error and fail to startup.
if (!fs.existsSync(getWorkingDirectory())) {
   throw Error(
      "Server working directory does not exist: " + getWorkingDirectory(),
   );
}

// --- MCP Router --- 
const mcpRouter = Router();

// Remove body parser from MCP router
// mcpRouter.use(bodyParser.json());

// No body parsing middleware needed here if transport handles raw stream

mcpRouter.get('/sse', async (req, res) => {
   console.log(`SSE connection initiated from ${req.ip}`);
   try {
      const messagePath = `${API_PREFIX}/mcp/messages`; // Use absolute path for client
      const transport = new SSEServerTransport(messagePath, res);
      transports[transport.sessionId] = transport;
      console.log(`Transport created for session: ${transport.sessionId}`);

      res.on("close", () => {
         console.log(`SSE connection closed for session: ${transport.sessionId}`);
         delete transports[transport.sessionId];
      });

      await mcpServer.connect(transport);
      console.log(`MCP Server connected to transport for session: ${transport.sessionId}`);
   } catch (error) {
      console.error("Error setting up SSE connection:", error);
      if (!res.headersSent) {
         res.status(500).send("Failed to establish SSE connection");
      }
   }
});

mcpRouter.post('/messages', async (req, res) => {
   const sessionId = req.query.sessionId as string;
   console.log(`Received POST message for session: ${sessionId}`);
   if (!sessionId) {
      return res.status(400).send('sessionId query parameter is required');
   }

   const transport = transports[sessionId];
   if (transport) {
      console.log(`Found transport for session ${sessionId}, handling message.`);
      console.log(`Stream readable before handlePostMessage?: ${req.readable}`);
      try {
         await transport.handlePostMessage(req, res);
         console.log(`Successfully handled POST message for session ${sessionId}`);
      } catch (error) {
         console.error(`Error handling POST message for session ${sessionId}:`, error);
         if (!res.headersSent) {
            res.status(500).send('Internal Server Error handling message');
         }
      }
   } else {
      console.warn(`No transport found for session ${sessionId}.`);
      res.status(400).send(`No active SSE connection found for session ${sessionId}`);
   }
});

// --- Mount Routers ---
app.use(`${API_PREFIX}/mcp`, mcpRouter);

// --- Fallback and Error Handling ---
app.get("*", (_req, res) => res.sendFile(path.resolve(ROOT, "index.html")));

// Error handling middleware (must be last)
app.use((err: Error, _req: express.Request, res: express.Response) => {
  console.error("Unhandled error:", err);
  const { json, status } = internalErrorToHttpError(err);
  res.status(status).json(json);
});

// --- Server Startup ---
const httpServer = http.createServer(app);

if (require.main === module) {
   httpServer.listen(PUBLISHER_PORT, PUBLISHER_HOST, () => {
      const address = httpServer.address() as AddressInfo;
      console.log(
         `Server is running at http://${address.address}:${address.port}`,
      );
   });
}

// Export for testing
export { httpServer, app };
