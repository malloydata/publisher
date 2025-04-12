// Use explicit relative path due to potential resolution issues
import { Server } from "../../../../node_modules/@modelcontextprotocol/sdk/dist/esm/server";
// Import types generated from OpenAPI spec
import type { components } from "../api";
// Import MCP SDK base types and Zod
import { z } from "zod";
import { 
    ServerCapabilities, 
    RequestSchema, 
    ResultSchema, 
    ErrorCode
} from "../../../../node_modules/@modelcontextprotocol/sdk/dist/esm/types";

// --- Reuse Existing Types --- 
type Project = components["schemas"]["Project"]; // e.g., { name?: string }
// We'll define Package, Model etc. when needed for getResource

// --- Schemas Definitions (Shared between server and test) --- 

// Resource Descriptor 
export const ResourceDescriptorSchema = z.object({
    uri: z.string(), 
    name: z.string().optional(),
    description: z.string().optional(),
});
type ResourceDescriptor = z.infer<typeof ResourceDescriptorSchema>;

// mcp/listResources 
const ListResourcesParamsSchema = z.object({}).optional();
export const ListResourcesRequestSchema = RequestSchema.extend({
  method: z.literal("mcp/listResources"),
  params: ListResourcesParamsSchema,
});
export const ListResourcesResultSchema = ResultSchema.extend({
  resources: z.array(ResourceDescriptorSchema),
});

// mcp/getResource Schemas
const GetResourceParamsSchema = z.object({
    uri: z.string(),
});
export const GetResourceRequestSchema = RequestSchema.extend({
    method: z.literal("mcp/getResource"),
    params: GetResourceParamsSchema,
});
export const GetResourceResultSchema = ResultSchema.extend({
    resource: ResourceDescriptorSchema, 
});

// --- End Schemas --- 

// Use existing project name constant (assuming it's accessible or redefined here)
// In the actual server.ts, it's a top-level const.
// For isolation, let's define it here, but ideally import/share it.
const PROJECT_NAME = "home";

// Basic server info
export const testServerInfo = {
  name: "malloy-publisher-mcp-server",
  version: "0.0.1",
  displayName: "Malloy Publisher MCP Server",
  description: "Provides access to Malloy models and query execution via MCP.",
};

// Server Capabilities
export const testCapabilities: ServerCapabilities = {
  resources: {
      listResources: true,
      getResource: true, 
  },
  tools: {},
};

/**
 * Initializes and returns the MCP Server instance.
 */
export function initializeMcpServer(): Server {
  const mcpServer = new Server(testServerInfo, {
    capabilities: testCapabilities,
  });

  console.log("MCP Server initialized.");

  // --- Register mcp/listResources Handler --- 

  mcpServer.setRequestHandler(ListResourcesRequestSchema, async (request): Promise<z.infer<typeof ListResourcesResultSchema>> => {
      console.log("Handling mcp/listResources request:", request.params);
      
      // TODO: Use request.params for filtering later
      // For now, always return the top-level project
      
      // Simulate fetching the project data (replace with actual service call if needed)
      const projectData: Project = { name: PROJECT_NAME }; 

      // Map the project data to the MCP resource descriptor format
      const resource: ResourceDescriptor = {
          uri: `malloy://project/${projectData.name}`, // Construct URI
          name: projectData.name,
          // Add a helpful description for AI agents
          description: `The main project '${projectData.name}' containing Malloy packages and models.`, 
      };

      // Validate and return the result using the Zod schema
      return ListResourcesResultSchema.parse({
          resources: [resource]
      });
  });

  console.log("Registered handler for mcp/listResources");

  // --- Register mcp/getResource Handler (Placeholder) --- 
  mcpServer.setRequestHandler(GetResourceRequestSchema, async (request): Promise<z.infer<typeof GetResourceResultSchema>> => {
      console.log("Handling mcp/getResource request:", request.params);
      // TODO: Implement actual logic to parse URI, find resource, and return it or throw error
      throw new Error(`Resource not found (placeholder): ${request.params.uri}`); // Simulate not found for now
  });
  console.log("Registered handler for mcp/getResource (placeholder)");

  // TODO: Register malloy/executeQuery tool handler 

  return mcpServer;
} 