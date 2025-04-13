import {
    McpServer,
    ResourceTemplate,
    ResourceMetadata,
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
import { PackageNotFoundError, ModelNotFoundError, ModelCompilationError } from "../errors";
import { MalloyError } from "@malloydata/malloy";
import { RequestHandlerExtra } from "@modelcontextprotocol/sdk/shared/protocol";
import { UriTemplate } from "@modelcontextprotocol/sdk/shared/uriTemplate.js";

type ApiQueryResult = components["schemas"]["QueryResult"];

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
        message: "Either 'query' or both 'sourceName' and 'queryName' must be provided",
    })
    .refine(params => !(params.query && params.queryName), {
        message: "Cannot provide both 'query' and 'queryName'",
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
        mcpError.message = `Error ${context}: ${mcpError.message}`;
        return mcpError;
    }

    /**
     * Converts any error to an McpError with appropriate code and suggestion.
     */
    static from(error: unknown): McpError {
        if (error instanceof McpError) {
            return error;
        }

        const message = error instanceof Error ? error.message : String(error);

        if (error instanceof PackageNotFoundError) {
            return new McpError(
                `Resource not found: Package not found. ${message}`,
                ErrorCode.InvalidParams,
                "Verify the packageName exists. You can list packages using mcp/listResources with the project URI (malloy://project/home).",
                error
            );
        }

        if (error instanceof ModelNotFoundError) {
            return new McpError(
                `Resource not found: Model not found. ${message}`,
                ErrorCode.InvalidParams,
                "Verify the modelPath exists within the specified package. You can list models using mcp/listResources with the package URI (e.g., malloy://project/home/package/{packageName}).",
                error
            );
        }

        if (error instanceof ModelCompilationError) {
            return new McpError(
                `Malloy model compilation failed: ${message}`,
                ErrorCode.InvalidRequest,
                "Check the Malloy syntax in the model definition.",
                error
            );
        }

        if (error instanceof MalloyError) {
            return new McpError(
                `Malloy query error: ${message}`,
                ErrorCode.InvalidRequest,
                "Check the query syntax, parameters, and ensure the data source is accessible.",
                error
            );
        }

        if (error instanceof z.ZodError) {
            return new McpError(
                `Invalid parameters provided: ${error.errors.map(e => `${e.path.join('.')} (${e.message})`).join(', ')}`,
                ErrorCode.InvalidParams,
                "Check the request parameters against the required schema.",
                error
            );
        }

        if (error instanceof Error) {
            const match = error.message.match(/\[(-?\d+)\]\s*(.*)/);
            if (match && match[1] && match[2]) {
                const parsedCode = parseInt(match[1], 10);
                if (Object.values(ErrorCode).includes(parsedCode)) {
                    return new McpError(
                        match[2],
                        parsedCode,
                        parsedCode === ErrorCode.InvalidParams ? "Check the parameters provided in your request." : undefined,
                        error
                    );
                }
            }
            return new McpError(`Internal error: ${error.message}`, ErrorCode.InternalError, undefined, error);
        }

        return new McpError(
            "An unexpected error occurred.",
            ErrorCode.InternalError,
            "Check the server logs or contact support.",
            error
        );
    }
}

/**
 * Maps any error to an MCP error with appropriate code and suggestion.
 * @deprecated Use McpError.from() or McpError.withContext() instead
 */
function mapMalloyErrorToMcpError(error: unknown, context?: string): Error {
    return context ? McpError.withContext(error, context) : McpError.from(error);
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
  console.log("MCP Server initialized.");

  // --- Register Resources using server.resource --- 

  // Register Project Resource (Fixed Project Name: 'home')
  mcpServer.resource(
    'project',
    new ResourceTemplate(
      PROJECT_TEMPLATE,
      {
        list: async (extra: RequestHandlerExtra): Promise<ListResourcesResult> => {
          console.log(`[list] Listing packages for project: ${PROJECT_URI}`);
          try {
            const packages = await packageService.listPackages();
            return { 
              resources: packages.filter(pkg => pkg.name).map(pkg => ({
                uri: PACKAGE_TEMPLATE.expand({ projectName: PROJECT_NAME, packageName: pkg.name! }),
                name: pkg.name!,
                description: "Represents a Malloy package containing models.",
              })) 
            };
          } catch (error) {
             console.error(`[list] Error listing packages for project '${PROJECT_URI}':`, error);
             throw mapMalloyErrorToMcpError(error, `listing packages for project '${PROJECT_NAME}'`);
          }
        }
      }
    ),
    projectMetadata,
    // Read callback for the project resource
    async (uri: URL, variables: Record<string, unknown>, extra: RequestHandlerExtra): Promise<ReadResourceResult> => {
      console.log(`[read] Handling project request: ${uri.href}`);
      // Provide static content or potentially dynamic info if needed
      return {
        contents: [
          {
            type: 'text',
            uri: uri.href,
            text:
`Malloy Project Resource ('${uri.href}')
Type: Project
Description: The top-level container for Malloy packages.
Children: Use 'mcp/listResources' with this URI to list available packages.`
          }
        ]
      };
    }
  );

  // Register Package Resource Template
  mcpServer.resource(
    'package',
    new ResourceTemplate(
      PACKAGE_TEMPLATE,
      {
        list: async (extra: RequestHandlerExtra): Promise<ListResourcesResult> => {
          try {
            const packages = await packageService.listPackages();
            return {
              resources: packages.filter(pkg => pkg.name).map(pkg => ({
                uri: PACKAGE_TEMPLATE.expand({ projectName: PROJECT_NAME, packageName: pkg.name! }),
                name: pkg.name!,
                description: "Represents a Malloy package containing models.",
              }))
            };
          } catch (error) {
             console.error(`[list] Error listing packages:`, error);
             throw mapMalloyErrorToMcpError(error, `listing packages`);
          }
        },
        complete: {
          projectName: async (value: string) => [PROJECT_NAME],
          packageName: async (value: string) => {
            try {
              const packages = await packageService.listPackages();
              return packages
                .filter(pkg => pkg.name && pkg.name.toLowerCase().includes(value.toLowerCase()))
                .map(pkg => pkg.name!)
                .slice(0, 10);
            } catch (error) {
              console.error(`[complete] Error completing packageName:`, error);
              return [];
            }
          }
        }
      }
    ),
    packageMetadata,
    // Read callback for package resources
    async (uri: URL, variables: Record<string, unknown>, extra: RequestHandlerExtra): Promise<ReadResourceResult> => {
        const packageName = variables.packageName as string;
        console.log(`[read] Handling package request: ${uri.href} (packageName: ${packageName})`);
        try {
            const pkg = await packageService.getPackage(packageName);
            const models = await pkg.listModels();
            const modelList = models
              .filter(m => m.path)
              .map(m => `- ${m.path} (URI: ${MODEL_TEMPLATE.expand({ projectName: PROJECT_NAME, packageName, modelPath: m.path! })})`).join('\n');
            return {
                 contents: [{
                     type: 'text',
                     uri: uri.href,
                     text:
`Malloy Package Resource
Name: ${packageName}
URI: ${uri.href}
Description: Represents a Malloy package containing models.
Models (${models.length}):
${modelList || '(No models found)'}

Use 'mcp/listResources' with this URI to list models.
Use 'mcp/readResource' with a model URI to view model details.`
                 }]
            };
        } catch (error) {
            console.error(`[read] Error getting package resource '${uri.href}':`, error);
            throw mapMalloyErrorToMcpError(error, `reading package '${packageName}`);
        }
    }
  );

  // Register Model Resource Template
  mcpServer.resource(
    'model',
    new ResourceTemplate(
      MODEL_TEMPLATE,
      {
        list: undefined, // Models don't list children
        complete: {
          projectName: async (value: string) => [PROJECT_NAME],
          packageName: async (value: string) => {
            try {
              const packages = await packageService.listPackages();
              return packages
                .filter(pkg => pkg.name && pkg.name.toLowerCase().includes(value.toLowerCase()))
                .map(pkg => pkg.name!)
                .slice(0, 10);
            } catch (error) {
              console.error(`[complete] Error completing packageName:`, error);
              return [];
            }
          },
          modelPath: async (value: string) => {
            try {
              // Since we can't get the packageName from variables, we'll list all models from all packages
              const packages = await packageService.listPackages();
              const allModels: string[] = [];
              
              for (const pkg of packages) {
                if (!pkg.name) continue;
                try {
                  const pkgInstance = await packageService.getPackage(pkg.name);
                  const models = await pkgInstance.listModels();
                  allModels.push(...models.filter(model => model.path).map(model => model.path!));
                } catch (error) {
                  console.error(`[complete] Error listing models for package ${pkg.name}:`, error);
                }
              }
              
              return allModels
                .filter(path => path.toLowerCase().includes(value.toLowerCase()))
                .slice(0, 10);
            } catch (error) {
              console.error(`[complete] Error completing modelPath:`, error);
              return [];
            }
          }
        }
      }
    ),
    modelMetadata,
    // Add schema validation here in the read callback
    async (uri: URL, variables: Record<string, unknown>, extra: RequestHandlerExtra): Promise<ReadResourceResult> => {
        // Validate variables
        const schema = z.object({
          projectName: z.string().refine(name => name === PROJECT_NAME, {message: `Only project '${PROJECT_NAME}' is supported`}),
          packageName: z.string(),
          modelPath: z.union([z.string(), z.array(z.string())])
        });
        const validated = schema.parse(variables);
        
        const packageName = validated.packageName;
        const modelPath = Array.isArray(validated.modelPath) ? validated.modelPath.join('/') : validated.modelPath;
        console.log(`[read] Handling model request: ${uri.href} (packageName: ${packageName}, modelPath: ${modelPath})`);

        try {
            const pkg = await packageService.getPackage(packageName);
            const model = pkg.getModel(modelPath);
            if (!model) {
                throw new ModelNotFoundError(`Model '${modelPath}' not found in package '${packageName}'.`);
            }
            const modelType = model.getModelType(); 

            return {
                contents: [
                    {
                        type: 'text',
                        uri: uri.href,
                        text:
`Malloy Model Resource
Name: ${modelPath}
URI: ${uri.href}
Package: ${packageName}
Type: ${modelType}
Description: Represents a Malloy model file (.malloy).

Use the 'malloy/executeQuery' tool to run queries against this model.
Example tool call:
{
  "method": "malloy/executeQuery",
  "params": {
    "packageName": "${packageName}",
    "modelPath": "${modelPath}",
    "query": "query: ..."
  }
}`
                    }
                ]
            };
        } catch (error) {
            console.error(`[read] Error getting model resource '${uri.href}':`, error);
            throw mapMalloyErrorToMcpError(error, `reading model '${modelPath}' in package '${packageName}'`);
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
        try {
            const pkg = await packageService.getPackage(params.packageName);
            const model = pkg.getModel(params.modelPath);
            if (!model) {
                throw new ModelNotFoundError(`Model '${params.modelPath}' not found in package '${params.packageName}'.`);
            }

            const { queryResults, modelDef, dataStyles } = await model.getQueryResults(
                params.sourceName,
                params.queryName,
                params.query
            );

            const apiResult: ApiQueryResult = {
                queryResult: JSON.stringify(queryResults.toJSON(), null, 2),
                modelDef: JSON.stringify(modelDef || {}, null, 2),
                dataStyles: JSON.stringify(dataStyles || {}),
            };

            console.log(`[tool] malloy/executeQuery successful.`);
            return {
                content: [{
                    type: 'text',
                    text: JSON.stringify(apiResult)
                }]
            };
        } catch (error) {
            console.error(`[tool] Error executing query:`, error);
            throw mapMalloyErrorToMcpError(error, `executing query on '${params.packageName}/${params.modelPath}'`);
        }
    }
  );

  console.log("MCP Resources and Tools registered.");
  return mcpServer;
} 