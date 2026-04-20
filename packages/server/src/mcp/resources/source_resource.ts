import {
   McpServer,
   ResourceTemplate,
} from "@modelcontextprotocol/sdk/server/mcp.js";
import { URL } from "url";
import type { components } from "../../api";
import { ModelCompilationError } from "../../errors";
import { logger } from "../../logger";
import { ProjectStore } from "../../service/project_store";
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

// Define the expected parameter types for this resource
type SourceParams = {
   projectName?: unknown;
   packageName?: unknown;
   modelPath?: unknown;
   sourceName?: unknown;
};

/**
 * Registers the Malloy Source resource type (nested within a Model).
 * Handles getting details for a specific source.
 */
export function registerSourceResource(
   mcpServer: McpServer,
   projectStore: ProjectStore,
): void {
   mcpServer.resource(
      "source",
      new ResourceTemplate(
         "malloy://project/{projectName}/package/{packageName}/models/{modelPath}/sources/{sourceName}",
         { list: undefined }, // Listing sources directly is not supported via this template
      ),
      /** Handles GetResource requests for specific Sources within a Model */
      (uri, params) =>
         handleResourceGet(
            uri,
            params as SourceParams, // Cast params
            "source",
            // Define the getData async callback
            async (
               {
                  projectName,
                  packageName,
                  modelPath,
                  sourceName,
               }: SourceParams, // Use defined type
               // Add type for uri
               uri: URL,
            ) => {
               // Input validation
               if (
                  typeof projectName !== "string" ||
                  typeof packageName !== "string" ||
                  typeof modelPath !== "string" ||
                  typeof sourceName !== "string"
               ) {
                  // Throw simple error, handleResourceGet will wrap it
                  throw new Error("Invalid parameters for source resource.");
               }

               try {
                  // Use the imported getModelForQuery
                  const modelResult = await getModelForQuery(
                     projectStore,
                     projectName,
                     packageName,
                     modelPath,
                  );

                  // Handle error case from getModelForQuery
                  if ("error" in modelResult) {
                     throw new McpGetResourceError(modelResult.error);
                  }
                  const { model } = modelResult;

                  const sources = await model.getSources();
                  // Add check for undefined sources
                  if (!sources) {
                     throw new Error("Could not retrieve sources from model.");
                  }
                  const source = sources.find(
                     (s: components["schemas"]["Source"]) =>
                        s.name === sourceName,
                  );

                  if (!source) {
                     const errorDetails = getNotFoundError(
                        `Source '${sourceName}' in model '${modelPath}' package '${packageName}' environment '${projectName}'`,
                     );
                     throw new McpGetResourceError(errorDetails);
                  }

                  // Strip implicit filters from agent-facing responses
                  if (source.filters) {
                     source.filters = source.filters.filter((f) => !f.implicit);
                  }

                  return source;
               } catch (error) {
                  // Catch errors from getModelForQuery or finding the source
                  if (error instanceof McpGetResourceError) {
                     throw error; // Re-throw already formatted errors
                  }
                  // Check if it's a compilation error (likely from getModel call within getModelForQuery)
                  if (error instanceof ModelCompilationError) {
                     const errorDetails = getMalloyErrorDetails(
                        "GetResource (source - model compilation)",
                        `${projectName}/${packageName}/${modelPath}`,
                        error,
                     );
                     throw new McpGetResourceError(errorDetails);
                  }
                  // Handle other potential errors (e.g., invalid params error from initial check)
                  logger.error(
                     `[MCP Server Error] Error fetching source '${sourceName}' from ${uri.href}:`,
                     { error },
                  );
                  // Fallback: Use getInternalError for unexpected issues
                  // Or getNotFoundError if it's likely a resource access issue
                  const fallbackErrorDetails = getInternalError(
                     `GetResource (source: ${uri.href})`,
                     error,
                  );
                  throw new McpGetResourceError(fallbackErrorDetails);
               }
            },
            RESOURCE_METADATA.source, // Use source-specific metadata
         ),
   );
}
