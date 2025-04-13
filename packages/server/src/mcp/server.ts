import { McpServer, ResourceTemplate } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { components } from "../api";
import { z } from "zod";
import {
    ResultSchema,
    ErrorCode,
} from "@modelcontextprotocol/sdk/types.js";
import { PackageService } from "../service/package.service";
import { PackageNotFoundError, ModelNotFoundError, ModelCompilationError } from "../errors";
import { MalloyError } from "@malloydata/malloy";

type ApiQueryResult = components["schemas"]["QueryResult"];

export const ResourceDescriptorSchema = z.object({
    uri: z.string(),
    name: z.string().optional(),
    description: z.string().optional(),
    // Add type information if desired based on spec (e.g., 'project', 'package', 'model')
    // type: z.enum(['project', 'package', 'model']).optional(), 
});

// --- MCP Method Schemas ---

// mcp/listResources Schemas
export const ListResourcesParamsSchema = z.object({
    uri: z.string().optional(), // URI of the resource to list children of
});

export const ListResourcesResultSchema = z.object({
    resources: z.array(ResourceDescriptorSchema),
});

// mcp/getResource Schemas
export const GetResourceParamsSchema = z.object({
    uri: z.string(), // URI of the resource to get
});

export const GetResourceResultSchema = ResourceDescriptorSchema; // Result is a single descriptor


// malloy/executeQuery Schemas
const ExecuteQueryParamsShape = {
    projectName: z.string().default("home"), // Add default project
    packageName: z.string(),
    modelPath: z.string(),
    query: z.string().optional(),
    sourceName: z.string().optional(),
    queryName: z.string().optional(),
};

const ExecuteQueryParamsSchema = z.object(ExecuteQueryParamsShape)
    .refine(params => params.query || (params.sourceName && params.queryName), {
        message: "Either 'query' or both 'sourceName' and 'queryName' must be provided",
    })
    .refine(params => !(params.query && params.queryName), {
        message: "Cannot provide both 'query' and 'queryName'",
    });

const QueryResultDataSchema = z.object({
    queryResult: z.string(), 
    modelDef: z.string(),    
    dataStyles: z.string(),
});

export const ExecuteQueryResultSchema = ResultSchema.extend({
    queryResult: QueryResultDataSchema.shape.queryResult,
    modelDef: QueryResultDataSchema.shape.modelDef,
    dataStyles: QueryResultDataSchema.shape.dataStyles,
});

// Use existing project name constant (assuming it's accessible or redefined here)
const PROJECT_NAME = "home";

export const testServerInfo = {
  name: "malloy-publisher-mcp-server",
  version: "0.0.1",
  displayName: "Malloy Publisher MCP Server",
  description: "Provides access to Malloy models and query execution via MCP.",
};

// --- Helper: URI Parsing --- 
interface ParsedResourceUri {
    type: 'project' | 'package' | 'model' | 'unknown';
    projectName?: string;
    packageName?: string;
    modelPath?: string;
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

// --- Helper: URI Construction ---
function constructUri(type: 'project' | 'package' | 'model', params: { projectName: string, packageName?: string, modelPath?: string }): string {
    const base = `malloy://project/${params.projectName}`;
    if (type === 'package' && params.packageName) {
        return `${base}/package/${params.packageName}`;
    }
    if (type === 'model' && params.packageName && params.modelPath) {
        return `${base}/package/${params.packageName}/models/${params.modelPath}`;
    }
    if (type === 'project') {
        return base;
    }
    throw new Error('Invalid parameters for URI construction');
}

/**
 * Initializes and returns the MCP Server instance.
 * @param packageService Instance of PackageService
 */
export function initializeMcpServer(packageService: PackageService): McpServer {

  const mcpServer = new McpServer(testServerInfo);

  console.log("MCP Server initialized.");

  // --- Register Resources using server.resource() ---

  // Project Resource (listing children implicitly handled by SDK?)
  mcpServer.resource(
    'malloy-project',
    `malloy://project/${PROJECT_NAME}`,
    async (uri) => { 
        console.log(`Handling resource request for project: ${uri.href}`);
        // Content might be minimal, focusing on discovery via SDK's listResources
        return {
            contents: [{
                uri: uri.href,
                text: `Malloy Project: ${PROJECT_NAME}` // Basic text content
            }]
        };
    }
  );
  console.log(`Registered resource handler for project: malloy://project/${PROJECT_NAME}`);

  // Package Resource Template (handles packages and implicitly lists models?)
  mcpServer.resource(
    'malloy-package',
    new ResourceTemplate(`malloy://project/${PROJECT_NAME}/package/{packageName}`, { list: undefined }),
    async (uri, { packageName }) => {
        console.log(`Handling resource request for package: ${uri.href} (packageName: ${packageName})`);
        if (typeof packageName !== 'string') {
             throw new Error(`[${ErrorCode.InvalidParams}] Invalid packageName: ${packageName}`);
        }
        try {
            const pkg = await packageService.getPackage(packageName);
            const pkgMeta = pkg.getPackageMetadata();
            // Return basic package info as content
            return {
                contents: [{
                    uri: uri.href,
                    text: JSON.stringify(pkgMeta, null, 2) // Package metadata as text
                }]
            };
        } catch (error) {
             console.error(`Error getting package resource '${uri.href}':`, error);
             if (error instanceof PackageNotFoundError) {
                 throw new Error(`[${ErrorCode.InvalidParams}] Package not found: ${packageName}`);
             }
             throw new Error(`[${ErrorCode.InternalError}] Error getting package: ${error instanceof Error ? error.message : String(error)}`);
        }
    }
  );
 console.log(`Registered resource handler template for packages: malloy://project/${PROJECT_NAME}/package/{packageName}`);

  // Model Resource Template 
  // Using spread syntax for modelPath to capture multi-part paths
  mcpServer.resource(
    'malloy-model',
    new ResourceTemplate(`malloy://project/${PROJECT_NAME}/package/{packageName}/models/{...modelPath}`, { list: undefined }),
    async (uri, { packageName, modelPath }) => {
        const fullModelPath = Array.isArray(modelPath) ? modelPath.join('/') : modelPath; // Handle potential array
        if (typeof packageName !== 'string') {
             throw new Error(`[${ErrorCode.InvalidParams}] Invalid packageName: ${packageName}`);
        }
        console.log(`Handling resource request for model: ${uri.href} (packageName: ${packageName}, modelPath: ${fullModelPath})`);
        try {
            const pkg = await packageService.getPackage(packageName);
            const model = pkg.getModel(fullModelPath);
            if (!model) {
                throw new ModelNotFoundError(`Model not found: ${fullModelPath}`);
            }
            // Use getModelType() - Assuming this getter exists or will be added to Model class
            const modelInfo = { path: fullModelPath, type: model.getModelType() }; 
            return {
                contents: [{
                    uri: uri.href,
                    text: JSON.stringify(modelInfo, null, 2) // Basic model info
                }]
            };
        } catch (error) {
            console.error(`Error getting model resource '${uri.href}':`, error);
             if (error instanceof PackageNotFoundError || error instanceof ModelNotFoundError) {
                 throw new Error(`[${ErrorCode.InvalidParams}] Resource not found: ${error.message}`);
             }
             throw new Error(`[${ErrorCode.InternalError}] Error getting model: ${error instanceof Error ? error.message : String(error)}`);
        }
    }
  );
 console.log(`Registered resource handler template for models: malloy://project/${PROJECT_NAME}/package/{packageName}/models/{...modelPath}`);

  // --- Register malloy/executeQuery Tool Handler (remains unchanged) ---
  mcpServer.tool(
      'malloy/executeQuery',  
      ExecuteQueryParamsShape, 
      async (params: z.infer<typeof ExecuteQueryParamsSchema>): Promise<{ content: { type: 'text', text: string }[] }> => {
          console.log("Handling malloy/executeQuery request:", params);
          // ... (rest of executeQuery handler implementation) ...
          try {
              // 1. Get Package
              const pkg = await packageService.getPackage(params.packageName);

              // 2. Get Model
              const model = pkg.getModel(params.modelPath);
              if (!model) {
                  throw new ModelNotFoundError(`Model not found at path: ${params.modelPath} in package ${params.packageName}`);
              }

              // 3. Execute Query
              const { queryResults, modelDef, dataStyles } = await model.getQueryResults(
                  params.sourceName,
                  params.queryName,
                  params.query
              );

              // 4. Map to ApiQueryResult structure (stringifying complex objects)
              const apiQueryResult: ApiQueryResult = {
                  queryResult: JSON.stringify(queryResults._queryResult),
                  modelDef: JSON.stringify(modelDef),
                  dataStyles: JSON.stringify(dataStyles),
              };

              // 5. Validate the structure (optional but good practice)
              ExecuteQueryResultSchema.parse(apiQueryResult);

              // 6. Return stringified JSON within a text content block
              return {
                  content: [{ type: 'text', text: JSON.stringify(apiQueryResult) }]
              };

          } catch (error) {
              console.error(`Error during malloy/executeQuery for model ${params.modelPath}:`, error);
              let errorMessage = "An unexpected error occurred while executing the Malloy query.";
              let errorCode = ErrorCode.InternalError;
              let suggestion = "Please check the server logs for more details or contact support.";

              if (error instanceof PackageNotFoundError || error instanceof ModelNotFoundError) {
                  errorCode = ErrorCode.InvalidParams;
                  errorMessage = `Resource not found: Could not find package \'${params.packageName}\' or model \'${params.modelPath}\'.`;
                  suggestion = "Please verify the packageName and modelPath parameters match existing resources."; // Removed mention of listResources
              } else if (error instanceof ModelCompilationError) {
                  errorCode = ErrorCode.InvalidRequest;
                  errorMessage = `Malloy model compilation failed: ${error.message}`;
                  suggestion = "Please check the Malloy syntax in your query or the model definition.";
              } else if (error instanceof MalloyError) {
                  errorCode = ErrorCode.InvalidRequest;
                  errorMessage = `Malloy query error: ${error.message}`;
                  suggestion = "Please check the query syntax, parameters, and ensure the data source is accessible.";
              } else if (error instanceof Error) {
                  // Check if message already has our code prefix
                  if (!error.message.startsWith('[')) {
                    errorMessage = `An internal error occurred: ${error.message}`;
                  } else {
                    // Keep the error message as thrown by manual validation or other internal errors
                    errorMessage = error.message;
                    // Attempt to extract code if possible, otherwise default to InternalError
                    const match = error.message.match(/\[(-?\d+)\]/);
                    if (match && match[1]) {
                        errorCode = parseInt(match[1], 10);
                    }
                  }
              }

              // Throw standard Error with code indication for McpServer tool handler
              const formattedMessage = `[${errorCode}] ${errorMessage} Suggestion: ${suggestion}`;
              throw new Error(formattedMessage);
          }
      }
  );
  console.log("Registered tool handler for malloy/executeQuery");

  return mcpServer;
} 