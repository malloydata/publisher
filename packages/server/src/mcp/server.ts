import {
    McpServer,
    ResourceTemplate,
    ResourceMetadata,
    ReadResourceTemplateCallback,
    ReadResourceCallback,
    ToolCallback
} from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import {
    ErrorCode,
    CallToolResult
} from "@modelcontextprotocol/sdk/types.js";
import { PackageService } from "../service/package.service";
import { PackageNotFoundError, ModelNotFoundError, ModelCompilationError } from "../errors";
import { MalloyError } from "@malloydata/malloy";

// --- Schemas (Keep Zod schemas for validation) ---

export const ListResourcesParamsSchema = z.object({
    uri: z.string().optional(), // URI of the resource to list children of (optional)
});

// malloy/executeQuery Schemas
const ExecuteQueryParamsShape = {
    projectName: z.string().default("home"),
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

// --- Constants & Helpers ---

const PROJECT_NAME = "home";
const PROJECT_URI = `malloy://project/${PROJECT_NAME}`;

export const testServerInfo = {
  name: "malloy-publisher-mcp-server",
  version: "0.0.1",
  displayName: "Malloy Publisher MCP Server",
  description: "Provides access to Malloy models and query execution via MCP.",
};

// --- Helper: URI Construction ---
// (Keep constructUri)
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

// --- Helper: Error Mapping ---
function mapMalloyErrorToMcpError(error: unknown, context?: string): Error {
    let message = "An unexpected error occurred.";
    let code = ErrorCode.InternalError;
    let suggestion = "Check the server logs or contact support.";
    let originalMessage = error instanceof Error ? error.message : String(error);

    if (context) {
      message = `Error ${context}: ${message}`;
    }

    if (error instanceof PackageNotFoundError) {
        code = ErrorCode.InvalidParams;
        message = `Resource not found: Package not found. ${originalMessage}`; // Include original message detail
        suggestion = "Verify the packageName exists. You can list packages using mcp/listResources with the project URI (malloy://project/home).";
    } else if (error instanceof ModelNotFoundError) {
        code = ErrorCode.InvalidParams;
        message = `Resource not found: Model not found. ${originalMessage}`;
        suggestion = "Verify the modelPath exists within the specified package. You can list models using mcp/listResources with the package URI (e.g., malloy://project/home/package/{packageName}).";
    } else if (error instanceof ModelCompilationError) {
        code = ErrorCode.InvalidRequest;
        message = `Malloy model compilation failed: ${originalMessage}`;
        suggestion = "Check the Malloy syntax in the model definition.";
    } else if (error instanceof MalloyError) {
        code = ErrorCode.InvalidRequest;
        message = `Malloy query error: ${originalMessage}`;
        suggestion = "Check the query syntax, parameters, and ensure the data source is accessible.";
    } else if (error instanceof z.ZodError) {
        code = ErrorCode.InvalidParams;
        message = `Invalid parameters provided: ${error.errors.map(e => `${e.path.join('.')} (${e.message})`).join(', ')}`;
        suggestion = "Check the request parameters against the required schema.";
    } else if (error instanceof Error) {
        // Handle errors potentially thrown by refine validation etc.
         const match = error.message.match(/\[(-?\d+)\]\s*(.*)/);
         if (match && match[1] && match[2]) {
            const parsedCode = parseInt(match[1], 10);
            // Check if it's a known JSON-RPC error code
            if (Object.values(ErrorCode).includes(parsedCode)) {
                code = parsedCode;
                message = match[2]; // Use message after code
                // Provide default suggestion based on common client errors
                if (code === ErrorCode.InvalidParams) {
                    suggestion = "Check the parameters provided in your request.";
                }
            } else {
                 message = `Internal error: ${error.message}`;
            }
         } else {
             message = `Internal error: ${error.message}`;
         }
    }

    const mcpError = new Error(message);
    // Attach the code as a property for the protocol handler
    (mcpError as any).code = code;
    // Attach suggestion for potential client use (not standard MCP, but helpful)
    (mcpError as any).suggestion = suggestion;
    return mcpError;
}

// --- Resource Metadata Definitions ---

const projectMetadata: ResourceMetadata = {
    name: PROJECT_NAME,
    description: "Represents the root Malloy project ('home'). Contains packages.",
    contents: [{ type: 'text', uri: PROJECT_URI, text: 
`Malloy Project Resource ('${PROJECT_URI}')
Type: Project
Description: The top-level container for Malloy packages.
Children: Use 'mcp/listResources' with this URI to list available packages.`
    }]
};

const packageMetadata: ResourceMetadata = {
    description: "Represents a Malloy package containing models.",
    // Contents generated dynamically in the callback
};

const modelMetadata: ResourceMetadata = {
    description: "Represents a Malloy model file (.malloy).",
    // Contents generated dynamically in the callback
};

// --- Tool Description ---

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
`;

/**
 * Initializes and returns the MCP Server instance.
 * @param packageService Instance of PackageService
 */
export function initializeMcpServer(packageService: PackageService): McpServer {

  const mcpServer = new McpServer(testServerInfo);

  console.log("MCP Server initialized.");


  const readProjectCallback: ReadResourceCallback = async (uri) => {
      console.log(`[readResource] Handling project request: ${uri.href}`);
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
  };

  const readPackageCallback: ReadResourceTemplateCallback = async (uri, variables) => {
      const packageName = variables.packageName as string;
      console.log(`[readResource] Handling package request: ${uri.href} (packageName: ${packageName})`);
      if (!packageName) {
          throw mapMalloyErrorToMcpError(new Error("packageName variable missing in URI template."), "reading package");
      }
      try {
          const pkg = await packageService.getPackage(packageName);
          const models = await pkg.listModels();
          const modelList = models.map(m => `- ${m.path} (URI: ${constructUri('model', { projectName: PROJECT_NAME, packageName, modelPath: m.path })})`).join('\n');
          return {
               contents: [{
                   type: 'text',
                   uri: uri.href,
                   text:
`Malloy Package Resource
Name: ${packageName}
URI: ${uri.href}
Description: ${packageMetadata.description}
Models (${models.length}):
${modelList || '(No models found)'}

Use 'mcp/listResources' with this URI to list models.
Use 'mcp/readResource' with a model URI to view model details.`
               }]
          };
      } catch (error) {
          console.error(`[readResource] Error getting package resource '${uri.href}':`, error);
          throw mapMalloyErrorToMcpError(error, `reading package '${packageName}'`);
      }
  };

  const readModelCallback: ReadResourceTemplateCallback = async (uri, variables) => {
      const packageName = variables.packageName as string;
      const modelPath = Array.isArray(variables.modelPath) ? variables.modelPath.join('/') : variables.modelPath as string;
      console.log(`[readResource] Handling model request: ${uri.href} (packageName: ${packageName}, modelPath: ${modelPath})`);

      if (!packageName || !modelPath) {
          throw mapMalloyErrorToMcpError(new Error("packageName or modelPath variable missing in URI template."), "reading model");
      }
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
Description: ${modelMetadata.description}

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
              ],
          };
      } catch (error) {
          console.error(`[readResource] Error getting model resource '${uri.href}':`, error);
          throw mapMalloyErrorToMcpError(error, `reading model '${modelPath}' in package '${packageName}'`);
      }
  };

  // --- Register Resources --- 

  // Register the fixed project resource
  mcpServer.resource(
      'malloy-project',
      PROJECT_URI,
      projectMetadata,
      readProjectCallback
  );
  console.log(`Registered fixed resource: ${PROJECT_URI}`);

  // Register package template
  mcpServer.resource(
      'malloy-package',
      new ResourceTemplate(
          `malloy://project/${PROJECT_NAME}/package/{packageName}`,
          { list: undefined } // listResources handled by SDK based on templates?
      ),
      packageMetadata,
      readPackageCallback
  );
   console.log(`Registered resource template for packages.`);

  // Register model template
  mcpServer.resource(
      'malloy-model',
      new ResourceTemplate(
          `malloy://project/${PROJECT_NAME}/package/{packageName}/models/{...modelPath}`,
          { list: undefined } // listResources handled by SDK based on templates?
      ),
      modelMetadata,
      readModelCallback
  );
 console.log(`Registered resource template for models.`);

  // --- Register Tool --- 
  const executeQueryCallback: ToolCallback<typeof ExecuteQueryParamsShape> = async (params) => {
      console.log("[tool/executeQuery] Handling request:", JSON.stringify(params));
      try {
          ExecuteQueryParamsSchema.parse(params); 
      } catch(error) {
          console.error("[tool/executeQuery] Refinement Validation Error:", error);
          throw mapMalloyErrorToMcpError(error, 'validating parameters');
      }
      try {
          console.log(`[tool/executeQuery] Getting package: ${params.packageName}`);
          const pkg = await packageService.getPackage(params.packageName);
          console.log(`[tool/executeQuery] Getting model: ${params.modelPath}`);
          const model = pkg.getModel(params.modelPath);
          if (!model) {
              throw new ModelNotFoundError(`Model '${params.modelPath}' not found in package '${params.packageName}'.`);
          }
          console.log("[tool/executeQuery] Executing query...");
          // Destructure queryResults (as originally named)
          const { queryResults, modelDef, dataStyles } = await model.getQueryResults(
              params.sourceName || undefined,
              params.queryName || undefined,
              params.query || undefined
          );
          console.log("[tool/executeQuery] Query executed. Preparing result...");
          const resultPayload: CallToolResult = {
              content: [
                  { type: 'text', text: JSON.stringify({ 
                       // Use queryResults
                       queryResult: JSON.stringify(queryResults),
                       modelDef: JSON.stringify(modelDef),
                       dataStyles: JSON.stringify(dataStyles),
                  })} 
              ]
          };
          console.log("[tool/executeQuery] Returning successful result.");
          return resultPayload;
      } catch (error) {
          console.error(`[tool/executeQuery] Error during execution:`, error);
          throw mapMalloyErrorToMcpError(error, `executing query on model '${params.modelPath}'`);
      }
  };

  mcpServer.tool(
      'malloy/executeQuery',
      executeQueryToolDescription,
      ExecuteQueryParamsShape,
      executeQueryCallback
  );
  console.log("Registered tool handler for malloy/executeQuery");

  return mcpServer;
} 