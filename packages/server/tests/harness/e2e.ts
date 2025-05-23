import http from "http";
import { URL } from "url";
import path from "path";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import type {
  Request,
  Notification,
  Result,
} from "@modelcontextprotocol/sdk/types.js";

/**
 * E2E environment descriptor returned by {@link startE2E}.
 */
export interface E2EEnv {
  httpServer: http.Server;
  serverUrl: string;
  mcpClient: Client<Request, Notification, Result>;
}

let originalServerRoot: string | undefined;

/**
 * Spin-up the real Express app (via mcp/server.ts), expose its base URL and
 * return a fully-connected MCP client. The caller **must** invoke {@link stop}
 * once its suite finishes.
 */
export async function startE2E(): Promise<E2EEnv & { stop(): Promise<void> }> {
  //--------------------------------------------------------------------------
  // 1.  Set SERVER_ROOT so ProjectStore loader finds publisher.config.json
  //--------------------------------------------------------------------------
  originalServerRoot = process.env.SERVER_ROOT;
  const serverPackageDir = path.resolve(__dirname, "../../../"); // packages/server
  process.env.SERVER_ROOT = serverPackageDir;

  //--------------------------------------------------------------------------
  // 2.  Dynamically import the configured Express app AFTER env var mutation
  //--------------------------------------------------------------------------
  const { mcpApp } = await import("../../src/server");

  //--------------------------------------------------------------------------
  // 3.  Start HTTP server on a predictable high port (4042 by default)
  //--------------------------------------------------------------------------
  const TEST_PORT = Number(process.env.MCP_PORT || 4040) + 2;

  const httpServer: http.Server = await new Promise<http.Server>((resolve, reject) => {
    const srv = http
      .createServer(mcpApp)
      .listen(TEST_PORT, "127.0.0.1", () => resolve(srv));

    srv.on("error", (err: NodeJS.ErrnoException) => {
      /* c8 ignore next 3 */
      console.error("[E2E] server listen error", err);
      reject(err);
    });
  });

  const serverUrl = `http://127.0.0.1:${TEST_PORT}`;

  //--------------------------------------------------------------------------
  // 4.  Connect MCP client over HTTP streaming bridge
  //--------------------------------------------------------------------------
  const mcpClient = new Client<Request, Notification, Result>({
    name: "mcp-e2e-client",
    version: "1.0",
  });

  const transport = new StreamableHTTPClientTransport(
    new URL(`${serverUrl}/mcp`),
  );

  await mcpClient.connect(transport);

  //--------------------------------------------------------------------------
  // 5.  Return env + graceful stop helper
  //--------------------------------------------------------------------------
  const stop = async (): Promise<void> => {
    try {
      await mcpClient.close();
    } finally {
      httpServer.closeAllConnections?.();
      await new Promise<void>((r) => httpServer.close(() => r()));
      // Restore SERVER_ROOT
      if (originalServerRoot === undefined) delete process.env.SERVER_ROOT;
      else process.env.SERVER_ROOT = originalServerRoot;
    }
  };

  return { httpServer, serverUrl, mcpClient, stop };
} 