import {
   McpServer,
   ResourceTemplate,
} from "@modelcontextprotocol/sdk/server/mcp.js";
import { EnvironmentStore } from "../../service/environment_store";
import {
   ModelNotFoundError,
   PackageNotFoundError,
   ModelCompilationError,
} from "../../errors";
import { handleResourceGet, McpGetResourceError } from "../handler_utils";
import { RESOURCE_METADATA } from "../resource_metadata";
import {
   getNotFoundError,
   getMalloyErrorDetails,
   getInternalError,
} from "../error_messages";
import type { components } from "../../api"; // Import the components type

/**
 * Registers the Malloy Model resource type with the MCP server.
 * Handles getting details for a specific model.
 */
export function registerModelResource(
   mcpServer: McpServer,
   environmentStore: EnvironmentStore,
): void {
   mcpServer.resource(
      "model",
      new ResourceTemplate(
         "malloy://environment/{environmentName}/package/{packageName}/models/{modelPath}",
         { list: undefined }, // No list handler for individual models
      ),
      /** Handles GetResource requests for specific Malloy Models */
      (uri, params) =>
         handleResourceGet(
            uri,
            params,
            "model",
            async ({
               environmentName,
               packageName,
               modelPath,
            }: {
               environmentName?: unknown;
               packageName?: unknown;
               modelPath?: unknown;
            }) => {
               try {
                  // Validate all parameters
                  if (typeof environmentName !== "string") {
                     throw new Error("Invalid environment name parameter.");
                  }
                  if (typeof packageName !== "string") {
                     throw new Error("Invalid package name parameter.");
                  }
                  if (typeof modelPath !== "string") {
                     throw new Error("Invalid model path parameter.");
                  }

                  // *** UPDATED LOGIC using EnvironmentStore ***
                  // getEnvironment can throw EnvironmentNotFoundError (though unlikely if name is 'home')
                  const environment = await environmentStore.getEnvironment(
                     environmentName,
                     false,
                  );
                  // getPackage can throw PackageNotFoundError
                  const pkg = await environment.getPackage(packageName, false);
                  // getModel is SYNCHRONOUS
                  const modelInstance = pkg.getModel(modelPath);
                  // *** END UPDATED LOGIC ***

                  if (
                     !modelInstance ||
                     modelInstance.getModelType() === "notebook"
                  ) {
                     // Explicitly throw the expected error if not found or wrong type
                     throw new ModelNotFoundError(modelPath);
                  }

                  // modelInstance.getModel() is ASYNC and can throw ModelCompilationError
                  const compiledModelDefinition: components["schemas"]["CompiledModel"] =
                     await modelInstance.getModel();

                  // Strip implicit filters from agent-facing responses
                  if (compiledModelDefinition.sources) {
                     for (const source of compiledModelDefinition.sources) {
                        if (source.filters) {
                           source.filters = source.filters.filter(
                              (f) => !f.implicit,
                           );
                        }
                     }
                  }

                  return compiledModelDefinition;
               } catch (error) {
                  let errorDetails;
                  // Provide specific context for error messages
                  // Use validated string parameters here
                  const safeEnvironmentName =
                     typeof environmentName === "string"
                        ? environmentName
                        : "unknown";
                  const safePackageName =
                     typeof packageName === "string" ? packageName : "unknown";
                  const safeModelPath =
                     typeof modelPath === "string" ? modelPath : "unknown";

                  const notFoundContext = `Model '${safeModelPath}' in package '${safePackageName}' for environment '${safeEnvironmentName}'`;
                  const malloyErrorContext = `${safeEnvironmentName}/${safePackageName}/${safeModelPath}`;

                  if (error instanceof PackageNotFoundError) {
                     // Package not found during getPackage call
                     errorDetails = getNotFoundError(
                        `Package '${safePackageName}' in environment '${safeEnvironmentName}'`,
                     );
                  } else if (error instanceof ModelNotFoundError) {
                     // Model not found (either from getModel or type check)
                     errorDetails = getNotFoundError(notFoundContext);
                  } else if (error instanceof ModelCompilationError) {
                     // Compilation error when fetching model definition
                     errorDetails = getMalloyErrorDetails(
                        "GetResource (model)",
                        malloyErrorContext,
                        error,
                     );
                  } else if (error instanceof Error) {
                     // Catch invalid param errors or other generic errors
                     // Provide a clearer message differentiating param errors
                     if (error.message.includes("parameter")) {
                        errorDetails = getNotFoundError(
                           `Invalid identifier parameter provided for URI ${uri.href}: ${error.message}`,
                        );
                     } else {
                        errorDetails = getInternalError(
                           "GetResource (model) - Unexpected Error",
                           error,
                        );
                     }
                  } else {
                     // Fallback for truly unexpected non-Error throws
                     errorDetails = getInternalError(
                        "GetResource (model)",
                        error,
                     );
                  }
                  // Wrap and re-throw for handleResourceGet
                  throw new McpGetResourceError(errorDetails);
               }
            },
            RESOURCE_METADATA.model,
         ),
   );
}
