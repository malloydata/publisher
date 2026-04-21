import {
   McpServer,
   ResourceTemplate,
} from "@modelcontextprotocol/sdk/server/mcp.js";
import { URL } from "url";
import type { components } from "../../api"; // Need this for Query definition type
import { ModelCompilationError } from "../../errors";
import { logger } from "../../logger";
import { EnvironmentStore } from "../../service/environment_store";
import {
   getInternalError,
   getMalloyErrorDetails,
   getNotFoundError,
} from "../error_messages";
import {
   getModelForQuery,
   handleResourceGet,
   McpGetResourceError,
} from "../handler_utils";
import { RESOURCE_METADATA } from "../resource_metadata";

// Define the expected parameter types
type QueryParams = {
   environmentName?: unknown;
   packageName?: unknown;
   modelPath?: unknown;
   queryName?: unknown;
};

/**
 * Registers the Malloy Query resource type (nested within a Model).
 */
export function registerQueryResource(
   mcpServer: McpServer,
   environmentStore: EnvironmentStore,
): void {
   mcpServer.resource(
      "query",
      new ResourceTemplate(
         "malloy://environment/{environmentName}/package/{packageName}/models/{modelPath}/queries/{queryName}",
         { list: undefined }, // Listing queries is not supported via this template
      ),
      (uri, params) =>
         handleResourceGet(
            uri,
            params as QueryParams,
            "query",
            async (
               {
                  environmentName,
                  packageName,
                  modelPath,
                  queryName,
               }: QueryParams,
               uri: URL,
            ) => {
               if (
                  typeof environmentName !== "string" ||
                  typeof packageName !== "string" ||
                  typeof modelPath !== "string" ||
                  typeof queryName !== "string"
               ) {
                  throw new Error("Invalid parameters for query resource.");
               }

               try {
                  const modelResult = await getModelForQuery(
                     environmentStore,
                     environmentName,
                     packageName,
                     modelPath,
                  );
                  if ("error" in modelResult) {
                     throw new McpGetResourceError(modelResult.error);
                  }
                  const { model } = modelResult;

                  // Attempt to get the query definition from the model
                  const queries = await model.getQueries(); // Assuming this method exists
                  if (!queries) {
                     throw new Error("Could not retrieve queries from model.");
                  }
                  const query = queries.find(
                     (q: components["schemas"]["Query"]) =>
                        q.name === queryName,
                  );

                  if (!query) {
                     // Specific "Query not found" error
                     const errorDetails = getNotFoundError(
                        `Query '${queryName}' in model '${modelPath}' package '${packageName}' environment '${environmentName}'`,
                     );
                     throw new McpGetResourceError(errorDetails);
                  }
                  return query;
               } catch (error) {
                  if (error instanceof McpGetResourceError) {
                     throw error; // Re-throw already formatted
                  }
                  if (error instanceof ModelCompilationError) {
                     const errorDetails = getMalloyErrorDetails(
                        "GetResource (query - model compilation)",
                        `${environmentName}/${packageName}/${modelPath}`,
                        error,
                     );
                     throw new McpGetResourceError(errorDetails);
                  }
                  logger.error(
                     `[MCP Server Error] Error fetching query '${queryName}' from ${uri.href}:`,
                     { error },
                  );
                  const fallbackErrorDetails = getInternalError(
                     `GetResource (query: ${uri.href})`,
                     error,
                  );
                  throw new McpGetResourceError(fallbackErrorDetails);
               }
            },
            RESOURCE_METADATA.query,
         ),
   );
}
