// Use explicit relative path due to potential resolution issues
import { Server } from "../../../../node_modules/@modelcontextprotocol/sdk/dist/esm/server";
import { ServerCapabilities } from "../../../../node_modules/@modelcontextprotocol/sdk/dist/esm/types"; // Capabilities likely in shared types

// Basic server info - can be enhanced later
export const testServerInfo = { // Export for testing
  name: "malloy-publisher-mcp-server",
  version: "0.0.1", // TODO: Sync with package.json version?
  displayName: "Malloy Publisher MCP Server",
  description: "Provides access to Malloy models and query execution via MCP.",
};

// Define initial capabilities - resources and tools will be added later
export const testCapabilities: ServerCapabilities = { // Export for testing
  resources: {},
  tools: {},
};

/**
 * Initializes and returns the MCP Server instance.
 */
export function initializeMcpServer(): Server {
  const mcpServer = new Server(testServerInfo, { // Use exported constants
    capabilities: testCapabilities,
  });

  console.log("MCP Server initialized with basic capabilities.");
  // TODO: Register resource handlers (Step 4)
  // TODO: Register tool handlers (Step 5)

  return mcpServer;
} 