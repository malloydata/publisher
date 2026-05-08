import {
   McpServer,
   ResourceTemplate,
} from "@modelcontextprotocol/sdk/server/mcp.js";
import { URL } from "url";
import type { components } from "../../api"; // Need this for View definition type
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
type ViewParams = {
   environmentName?: unknown;
   packageName?: unknown;
   modelPath?: unknown;
   sourceName?: unknown;
   viewName?: unknown;
};

/**
 * Registers the Malloy View resource type (nested within a Source).
 */
export function registerViewResource(
   mcpServer: McpServer,
   environmentStore: EnvironmentStore,
): void {
   mcpServer.resource(
      "view",
      new ResourceTemplate(
         "malloy://environment/{environmentName}/package/{packageName}/models/{modelPath}/sources/{sourceName}/views/{viewName}",
         { list: undefined }, // Listing views is not supported via this template
      ),
      (uri, params) =>
         handleResourceGet(
            uri,
            params as ViewParams,
            "view",
            async (
               {
                  environmentName,
                  packageName,
                  modelPath,
                  sourceName,
                  viewName,
               }: ViewParams,
               uri: URL,
            ) => {
               if (
                  typeof environmentName !== "string" ||
                  typeof packageName !== "string" ||
                  typeof modelPath !== "string" ||
                  typeof sourceName !== "string" ||
                  typeof viewName !== "string"
               ) {
                  throw new Error("Invalid parameters for view resource.");
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

                  const sources = await model.getSources();
                  if (!sources) {
                     throw new Error("Could not retrieve sources from model.");
                  }
                  const source = sources.find(
                     (s: components["schemas"]["Source"]) =>
                        s.name === sourceName,
                  );
                  if (!source) {
                     throw new McpGetResourceError(
                        getNotFoundError(
                           `Source '${sourceName}' in model '${modelPath}' package '${packageName}' environment '${environmentName}'`,
                        ),
                     );
                  }

                  // Find the view within the source
                  const view = source.views?.find(
                     (v: components["schemas"]["View"]) => v.name === viewName,
                  );

                  if (!view) {
                     // Specific "View not found" error
                     const errorDetails = getNotFoundError(
                        `View '${viewName}' in source '${sourceName}' model '${modelPath}' package '${packageName}' environment '${environmentName}'`,
                     );
                     throw new McpGetResourceError(errorDetails);
                  }
                  return view;
               } catch (error) {
                  if (error instanceof McpGetResourceError) {
                     throw error; // Re-throw already formatted
                  }
                  if (error instanceof ModelCompilationError) {
                     const errorDetails = getMalloyErrorDetails(
                        "GetResource (view - model compilation)",
                        `${environmentName}/${packageName}/${modelPath}`,
                        error,
                     );
                     throw new McpGetResourceError(errorDetails);
                  }
                  logger.error(
                     `[MCP Server Error] Error fetching view '${viewName}' from ${uri.href}:`,
                     { error },
                  );
                  const fallbackErrorDetails = getInternalError(
                     `GetResource (view: ${uri.href})`,
                     error,
                  );
                  throw new McpGetResourceError(fallbackErrorDetails);
               }
            },
            RESOURCE_METADATA.view,
         ),
   );
}
