import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
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
});

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

/**
 * Initializes and returns the MCP Server instance.
 * @param packageService Instance of PackageService
 */
// Return McpServer type
export function initializeMcpServer(packageService: PackageService): McpServer {

  const mcpServer = new McpServer(testServerInfo);

  console.log("MCP Server initialized.");

  // --- Register malloy/executeQuery Tool Handler using .tool() ---
  mcpServer.tool(
      'malloy/executeQuery',  
      ExecuteQueryParamsShape,
      async (params: z.infer<typeof ExecuteQueryParamsSchema>): Promise<{ content: { type: 'text', text: string }[] }> => {
          console.log("Handling malloy/executeQuery request:", params);

          // Manually validate refinements if needed inside the handler
          if (!params.query && !(params.sourceName && params.queryName)) {
               throw new Error(`[${ErrorCode.InvalidParams}] Either 'query' or both 'sourceName' and 'queryName' must be provided`);
          }
          if (params.query && params.queryName) {
               throw new Error(`[${ErrorCode.InvalidParams}] Cannot provide both 'query' and 'queryName'`);
          }

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