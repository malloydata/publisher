import {
    McpServer,
    ResourceTemplate,
    ResourceMetadata
} from "@modelcontextprotocol/sdk/server/mcp.js";
import type { components } from "../api";
import { z } from "zod";
import {
    ErrorCode,
    ReadResourceResult,
    ListResourcesResult,
    CallToolResult
} from "@modelcontextprotocol/sdk/types.js";
import { PackageService } from "../service/package.service";
import { PackageNotFoundError, ModelNotFoundError } from "../errors";
import { Model } from "../service/model";
import { RequestHandlerExtra } from "@modelcontextprotocol/sdk/shared/protocol";
import { UriTemplate, Variables } from "@modelcontextprotocol/sdk/shared/uriTemplate.js";
import { MCP_ERROR_MESSAGES } from './mcp_constants';

// --- Query Execution Schema ---

// Raw shape for tool registration
const ExecuteQueryParamsShape = {
    projectName: z.string().default("home"),
    packageName: z.string(),
    modelPath: z.string(),
    query: z.string().optional(),
    sourceName: z.string().optional(),
    queryName: z.string().optional(),
} as const;

// Full schema with refinements for validation
const ExecuteQueryParamsSchema = z.object(ExecuteQueryParamsShape)
    .refine(params => params.query || (params.sourceName && params.queryName), {
        message: MCP_ERROR_MESSAGES.MISSING_REQUIRED_PARAMS,
    })
    .refine(params => !(params.query && params.queryName), {
        message: MCP_ERROR_MESSAGES.MUTUALLY_EXCLUSIVE_PARAMS,
    });

// --- Error Handling ---

/**
 * Represents an error in the MCP server with additional metadata.
 */
class McpError extends Error {
    constructor(
        message: string,
        public readonly code: ErrorCode = ErrorCode.InternalError,
        public readonly suggestion?: string,
        public readonly cause?: unknown
    ) {
        super(message);
        this.name = 'McpError';
    }

    /**
     * Creates an error with context prepended to the message.
     */
    static withContext(error: unknown, context: string): McpError {
        const mcpError = McpError.from(error);
        return new McpError(
            `Error ${context}: ${mcpError.message}`,
            mcpError.code || ErrorCode.InternalError,  // Use original code or fallback to InternalError
            mcpError.suggestion,
            error
        );
    }

    /**
     * Converts an unknown error into an McpError.
     * Protocol errors will be thrown and reject the promise.
     * Application errors will be returned in the response with isError: true.
     */
    static from(error: unknown): McpError {
        if (error instanceof McpError) {
            return error;
        }
        if (error instanceof z.ZodError) {
            // Protocol error - invalid parameters - throw
            throw new McpError(
                error.errors.map(e => `${e.path.join('.')} (${e.message})`).join(', '),
                ErrorCode.InvalidParams,
                "Check the request parameters against the required schema.",
                error
            );
        }
        // All other errors are protocol errors
        throw new McpError(
            error instanceof Error ? error.message : String(error),
            ErrorCode.InternalError
        );
    }
}

// --- Constants & Helpers ---

const PROJECT_NAME = "home";
const PROJECT_URI = `malloy://project/${PROJECT_NAME}`;

// Define URI templates for each resource type
const PROJECT_TEMPLATE = new UriTemplate(`malloy://project/${PROJECT_NAME}`);
const PACKAGE_TEMPLATE = new UriTemplate(`malloy://project/{projectName}/package/{packageName}`);
const MODEL_TEMPLATE = new UriTemplate(`malloy://project/{projectName}/package/{packageName}/models/{+modelPath}`);

export const testServerInfo = {
  name: "malloy-publisher-mcp-server",
  version: "0.0.1",
  displayName: "Malloy Publisher MCP Server",
  description: "Provides access to Malloy models and query execution via MCP.",
};

// --- Resource Metadata Definitions ---

/**
 * Metadata for the project resource.
 * The project is the top-level container that holds packages.
 * Currently only the 'home' project is supported.
 */
const projectMetadata: ResourceMetadata = {
    name: PROJECT_NAME,
    description: "Represents the root Malloy project ('home'). Contains packages.",
    contents: [{ 
        type: 'text', 
        uri: PROJECT_URI, 
        text: 
`Malloy Project Resource ('${PROJECT_URI}')
Type: Project
Description: The top-level container for Malloy packages.
Children: Use 'mcp/listResources' with this URI to list available packages.`
    }]
};

/**
 * Metadata for package resources.
 * A package is a container for Malloy models within a project.
 * Contents are generated dynamically based on the models in the package.
 */
const packageMetadata: ResourceMetadata = {
    description: "Represents a Malloy package containing models.",
    // Contents generated dynamically in the callback
};

/**
 * Metadata for model resources.
 * A model is a Malloy file (.malloy) that defines sources, queries, and data transformations.
 * Contents are generated dynamically based on the model definition.
 */
const modelMetadata: ResourceMetadata = {
    description: "Represents a Malloy model file (.malloy).",
    // Contents generated dynamically in the callback
};

// --- Tool Descriptions ---

/**
 * Comprehensive description of the malloy/executeQuery tool.
 * This description is designed to help AI agents understand the tool's capabilities and requirements.
 * 
 * The tool executes Malloy queries against models in the following ways:
 * 1. Ad-hoc queries: Direct Malloy query strings executed against a model
 * 2. Named queries: Pre-defined queries within a model's source
 * 
 * Resource hierarchy:
 * - Project (home)
 *   - Package (container for models)
 *     - Model (.malloy file)
 *       - Source (named data source)
 *         - Query (named or ad-hoc)
 */
const executeQueryToolDescription = `
Executes a Malloy query against a specified model within a package.

Parameters:
- projectName (string, optional, default: 'home'): The project name (currently only 'home' is supported).
- packageName (string, required): The name of the package containing the model.
- modelPath (string, required): The path to the .malloy model file within the package (e.g., 'flights.malloy', 'nested/model.malloy').
- query (string, optional): An ad-hoc Malloy query string to execute against the model. Either 'query' or ('sourceName' and 'queryName') must be provided.
- sourceName (string, optional): The name of the source defined within the model to run a named query against.
- queryName (string, optional): The name of the query defined within the specified sourceName.

Returns:
- (object): A JSON object containing the query results.
  - queryResult (string): JSON string representation of the Malloy query result data.
  - modelDef (string): JSON string representation of the Malloy model definition used.
  - dataStyles (string): JSON string representation of Malloy data styles.

Example Call (Ad-hoc Query):
{
  "jsonrpc": "2.0",
  "method": "malloy/executeQuery",
  "params": {
    "packageName": "flights",
    "modelPath": "flights.malloy",
    "query": "query: table('flights')->group_by(carrier).aggregate(count())"
  },
  "id": "req-1"
}

Example Call (Named Query):
{
  "jsonrpc": "2.0",
  "method": "malloy/executeQuery",
  "params": {
    "packageName": "flights",
    "modelPath": "flights.malloy",
    "sourceName": "flights",
    "queryName": "carrier_count"
  },
  "id": "req-2"
}

Notes:
- The query parameter takes precedence over sourceName/queryName if both are provided
- Model paths are relative to the package root
- Query results include both data and metadata for rich client experiences
- Error handling provides detailed feedback for invalid queries or missing resources
`;

/**
 * Initializes and returns the MCP Server instance.
 * @param packageService Instance of PackageService
 */
export function initializeMcpServer(packageService: PackageService): McpServer {
    const mcpServer = new McpServer(testServerInfo);

    // Register Project Resource
    mcpServer.resource(
        'project',
        new ResourceTemplate('malloy://project/{projectName}', { list: undefined }),
        async (uri, { projectName }) => ({
            contents: [{
                type: 'text',
                uri: uri.href,
                text: `Project: ${projectName}`
            }]
        })
    );

    // Register Package Resource
    mcpServer.resource(
        'package',
        new ResourceTemplate('malloy://project/{projectName}/package/{packageName}', { list: undefined }),
        async (uri, { projectName, packageName }) => {
            try {
                const pkg = await packageService.getPackage(packageName as string);
                return {
                    contents: [{
                        type: 'text',
                        uri: uri.href,
                        text: `Package: ${packageName}`
                    }]
                };
            } catch (error) {
                if (error instanceof PackageNotFoundError) {
                    return {
                        isError: true,
                        contents: [{
                            type: 'text',
                            uri: uri.href,
                            text: error.message
                        }]
                    };
                }
                throw McpError.from(error);
            }
        }
    );

    // Register Model Resource
    mcpServer.resource(
        'model',
        new ResourceTemplate('malloy://project/{projectName}/package/{packageName}/models/{modelPath}', { list: undefined }),
        async (uri, { projectName, packageName, modelPath }) => {
            try {
                const pkg = await packageService.getPackage(packageName as string);
                const model = pkg.getModel(modelPath as string);
                if (!model) {
                    return {
                        isError: true,
                        contents: [{
                            type: 'text',
                            uri: uri.href,
                            text: `Model not found: ${modelPath}`
                        }]
                    };
                }
                return {
                    contents: [{
                        type: 'text',
                        uri: uri.href,
                        text: `Model: ${modelPath}`
                    }]
                };
            } catch (error) {
                if (error instanceof PackageNotFoundError) {
                    return {
                        isError: true,
                        contents: [{
                            type: 'text',
                            uri: uri.href,
                            text: error.message
                        }]
                    };
                }
                throw McpError.from(error);
            }
        }
    );

    // --- Register Tools using server.tool --- 

    // Register malloy/executeQuery Tool
    mcpServer.tool(
        'malloy/executeQuery',
        ExecuteQueryParamsShape,
        async (params: z.infer<typeof ExecuteQueryParamsSchema>, extra: RequestHandlerExtra): Promise<CallToolResult> => {
            console.log(`[tool] Handling malloy/executeQuery:`, params);
            // 1. Protocol Validation (Throws on failure -> Promise Rejection)
            const validatedParams = await ExecuteQueryParamsSchema.parseAsync(params).catch(error => {
                // Zod errors are protocol errors (Invalid Params)
                throw new McpError(
                    MCP_ERROR_MESSAGES.INVALID_ARGUMENTS('malloy/executeQuery', error.errors.map((e: z.ZodError['errors'][0]) => `${e.path.join('.')} (${e.message})`)),
                    ErrorCode.InvalidParams,
                    "Check the request parameters against the required schema.",
                    error
                );
            });

            try { 
                const pkg = await packageService.getPackage(validatedParams.packageName);
                if (!pkg) {
                    return {
                        isError: true,
                        content: [{
                            type: 'text',
                            text: MCP_ERROR_MESSAGES.PACKAGE_NOT_FOUND(validatedParams.packageName)
                        }]
                    };
                }

                const model = pkg.getModel(validatedParams.modelPath);
                if (!model) {
                    return {
                        isError: true,
                        content: [{
                            type: 'text',
                            text: MCP_ERROR_MESSAGES.MODEL_NOT_FOUND(validatedParams.packageName, validatedParams.modelPath)
                        }]
                    };
                }

                // Use getQueryResults which handles both ad-hoc and named queries
                const { queryResults } = await model.getQueryResults(
                    validatedParams.sourceName,
                    validatedParams.queryName,
                    validatedParams.query
                );

                // Success Case
                return {
                    content: [{
                        type: 'text',
                        text: JSON.stringify(queryResults)
                    }]
                };
            } catch (error) {
                // Handle Application Errors - Return in response
                console.error(`[tool] Application error executing query:`, error);

                let errorMessage: string;
                if (error instanceof PackageNotFoundError) {
                    errorMessage = MCP_ERROR_MESSAGES.PACKAGE_NOT_FOUND(validatedParams.packageName);
                } else if (error instanceof ModelNotFoundError) {
                    errorMessage = MCP_ERROR_MESSAGES.MODEL_NOT_FOUND(validatedParams.packageName, validatedParams.modelPath);
                } else if (error instanceof Error) {
                    const packagePath = `${validatedParams.packageName}/${validatedParams.modelPath}`;
                    errorMessage = `${MCP_ERROR_MESSAGES.ERROR_EXECUTING_QUERY(packagePath)} ${error.message}`;
                } else {
                    errorMessage = "An unknown error occurred during query execution."; 
                }

                return {
                    isError: true,
                    content: [{
                        type: 'text',
                        text: errorMessage
                    }]
                };
            }
        }
    );

    console.log("MCP Resources and Tools registered.");
    return mcpServer;
} 