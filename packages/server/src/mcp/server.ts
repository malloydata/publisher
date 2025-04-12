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
// Import PackageService and related types/errors
import { PackageService } from "../service/package.service";
import { Package } from "../service/package";
import { Model } from "../service/model";
import { PackageNotFoundError } from "../errors"; 
import * as path from 'path'; // Import path module
import { MODEL_FILE_SUFFIX, NOTEBOOK_FILE_SUFFIX } from "../utils"; // Import suffixes

// --- Reuse Existing Types --- 
type Project = components["schemas"]["Project"]; // e.g., { name?: string }
type ApiPackage = components["schemas"]["Package"];
type ApiModel = components["schemas"]["Model"];
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

// --- Helper: URI Parsing --- 
interface ParsedResourceUri {
    type: 'project' | 'package' | 'model' | 'unknown';
    projectName?: string;
    packageName?: string;
    modelPath?: string; // Includes full path within package
}

function parseResourceUri(uri: string): ParsedResourceUri {
    const prefix = 'malloy://';
    if (!uri.startsWith(prefix)) return { type: 'unknown' };

    const path = uri.substring(prefix.length);
    const parts = path.split('/');

    if (parts.length >= 2 && parts[0] === 'project') {
        const projectName = parts[1];
        // Only support 'home' project for now
        if (projectName !== PROJECT_NAME) return { type: 'unknown', projectName }; 

        if (parts.length === 2) {
            return { type: 'project', projectName };
        }
        if (parts.length >= 4 && parts[2] === 'package') {
            const packageName = parts[3];
            if (parts.length === 4) {
                return { type: 'package', projectName, packageName };
            }
            if (parts.length >= 6 && parts[4] === 'models') {
                const modelPath = parts.slice(5).join('/');
                if (modelPath) {
                    return { type: 'model', projectName, packageName, modelPath };
                }
            }
        }
    }
    return { type: 'unknown' };
}

/**
 * Initializes and returns the MCP Server instance.
 * @param packageService Instance of PackageService
 */
export function initializeMcpServer(packageService: PackageService): Server {
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

  // --- Register mcp/getResource Handler --- 
  mcpServer.setRequestHandler(GetResourceRequestSchema, async (request): Promise<z.infer<typeof GetResourceResultSchema>> => {
      console.log("Handling mcp/getResource request:", request.params);
      const requestedUri = request.params.uri;
      const parsedUri = parseResourceUri(requestedUri);

      let resource: ResourceDescriptor | null = null;

      try {
          switch (parsedUri.type) {
              case 'project': {
                  // Only 'home' project is currently supported
                  const projectData: Project = { name: parsedUri.projectName }; 
                  resource = {
                      uri: requestedUri,
                      name: projectData.name,
                      description: `The main project '${projectData.name}' containing Malloy packages and models.`,
                  };
                  break;
              }
              case 'package': {
                  if (parsedUri.packageName) {
                      const pkg = await packageService.getPackage(parsedUri.packageName);
                      const packageData = pkg.getPackageMetadata();
                      resource = {
                          uri: requestedUri,
                          name: packageData.name,
                          description: packageData.description || `Malloy package '${packageData.name}'.`,
                      };
                  }
                  break;
              }
              case 'model': {
                  if (parsedUri.packageName && parsedUri.modelPath) {
                      const pkg = await packageService.getPackage(parsedUri.packageName);
                      const modelInstance = pkg.getModel(parsedUri.modelPath);
                      if (modelInstance) {
                          const modelName = path.basename(parsedUri.modelPath);
                          const modelType = parsedUri.modelPath.endsWith(MODEL_FILE_SUFFIX) ? "source" : "notebook";
                          resource = {
                              uri: requestedUri,
                              name: modelName,
                              description: `Malloy ${modelType} '${modelName}' in package '${parsedUri.packageName}'.`,
                              // TODO: Add kind: 'model' or other model-specific details?
                          };
                      }
                  }
                  break;
              }
          }
      } catch (error) {
          if (error instanceof PackageNotFoundError) {
              console.log(`Package not found for URI ${requestedUri}`);
          } else {
              console.error(`Unexpected error fetching resource ${requestedUri}:`, error);
              throw error;
          }
      }

      if (!resource) {
          throw new Error(`Resource not found: ${requestedUri}`);
      }

      // Result schema expects { resource: ResourceDescriptor }
      return GetResourceResultSchema.parse({ resource });
  });
  console.log("Registered handler for mcp/getResource");

  // TODO: Register malloy/executeQuery tool handler 

  return mcpServer;
} 