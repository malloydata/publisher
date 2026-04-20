import {
   McpServer,
   ResourceTemplate,
} from "@modelcontextprotocol/sdk/server/mcp.js";
import { ReadResourceResult } from "@modelcontextprotocol/sdk/types.js";
import { URL } from "url";
import { PackageNotFoundError } from "../../errors";
import { logger } from "../../logger";
import { ProjectStore } from "../../service/project_store";
import {
   getInternalError,
   getNotFoundError,
   type ErrorDetails,
} from "../error_messages";
import {
   buildMalloyUri,
   handleResourceGet,
   McpGetResourceError,
} from "../handler_utils";
import { RESOURCE_METADATA } from "../resource_metadata";

// *** Define handleGetPackageContents function ***
async function handleGetPackageContents(
   uri: URL,
   params: { projectName?: string; packageName?: string },
   projectStore: ProjectStore,
) {
   try {
      const { projectName, packageName } = params;
      if (typeof projectName !== "string" || projectName === "") {
         throw new Error("Invalid environment name parameter.");
      }
      if (typeof packageName !== "string" || packageName === "") {
         throw new Error("Invalid package name parameter.");
      }

      const project = await projectStore.getProject(projectName, false);
      const packageInstance = await project.getPackage(packageName, false);

      // Use listModels() which returns { path: string, type: 'source' | 'notebook' }[]
      const entries = await packageInstance.listModels();

      // Use a type that includes metadata explicitly
      const resourceDefinitions: { uri: string; metadata: unknown }[] = [];

      for (const entry of entries) {
         const entryPath = entry.path; // e.g., "flights.malloy", "overview.malloynb"
         // @ts-expect-error TODO: Fix missing type in API
         const entryType = entry.type; // 'source' or 'notebook'

         if (typeof entryPath !== "string" || entryPath === "") {
            logger.warn(
               `[MCP Server Warning] Skipping entry in package ${packageName} with invalid path:`,
               entry,
            );
            continue;
         }

         // Common components for URI building
         const baseUriComponents = {
            project: projectName,
            package: packageName,
         };

         switch (entryType) {
            case "notebook": {
               const resourceMetadata = RESOURCE_METADATA.notebook;
               if (!resourceMetadata) {
                  logger.warn(
                     `[MCP Server Warning] No metadata found for entry type 'notebook' path ${entryPath} in package ${packageName}`,
                  );
                  continue;
               }
               const notebookUri = buildMalloyUri({
                  ...baseUriComponents,
                  resourceType: "notebooks",
                  resourceName: entryPath,
               });
               resourceDefinitions.push({
                  uri: notebookUri,
                  metadata: resourceMetadata,
               });
               break;
            }
            case "source": {
               // 1. Add the source file itself
               const sourceResourceMetadata = RESOURCE_METADATA.source;
               if (!sourceResourceMetadata) {
                  logger.warn(
                     `[MCP Server Warning] No metadata found for entry type 'source' path ${entryPath} in package ${packageName}`,
                  );
                  // Continue processing views even if source metadata is missing
               } else {
                  const sourceUri = buildMalloyUri({
                     ...baseUriComponents,
                     resourceType: "sources", // Use 'sources' type for the source file resource
                     resourceName: entryPath,
                  });
                  resourceDefinitions.push({
                     uri: sourceUri,
                     metadata: sourceResourceMetadata,
                  });
               }

               // 2. Try to load the model to find views within it
               try {
                  const model = await packageInstance.getModel(entryPath); // Await the promise
                  if (model) {
                     // Ensure model exists before proceeding
                     const modelSources = model.getSources(); // This should be synchronous if model is loaded

                     if (modelSources && Array.isArray(modelSources)) {
                        for (const source of modelSources) {
                           if (source.views && Array.isArray(source.views)) {
                              for (const view of source.views) {
                                 const viewResourceMetadata =
                                    RESOURCE_METADATA.view;
                                 if (!viewResourceMetadata) {
                                    logger.warn(
                                       `[MCP Server Warning] No metadata found for entry type 'view' named '${view.name}' in source '${source.name}'`,
                                    );
                                    continue;
                                 }
                                 // Build the nested view URI using the helper
                                 const viewUri = buildMalloyUri({
                                    ...baseUriComponents,
                                    resourceType: "models", // Base is the model
                                    resourceName: entryPath, // Model file path
                                    subResourceType: "views",
                                    subResourceName: view.name,
                                 });
                                 resourceDefinitions.push({
                                    uri: viewUri,
                                    metadata: viewResourceMetadata,
                                 });
                              }
                           }
                        }
                     } else {
                        logger.warn(
                           `[MCP Server Warning] Could not retrieve sources or sources is not an array for model ${entryPath}`,
                        );
                     }
                  } else {
                     logger.warn(
                        `[MCP Server Warning] Could not load model for path ${entryPath} to extract views.`,
                     );
                  }
               } catch (modelLoadError) {
                  // Log error if model loading fails, but continue processing other files
                  logger.warn(
                     `[MCP Server Warning] Failed to load model ${entryPath} to extract views: ${modelLoadError instanceof Error ? modelLoadError.message : String(modelLoadError)}`,
                  );
               }
               break;
            }
            default:
               logger.warn(
                  `[MCP Server Warning] Unknown entry type '${entryType}' for path ${entryPath} in package ${packageName}`,
               );
               continue; // Skip unknown types
         }
      }

      return resourceDefinitions; // handleResourceGet will stringify this
   } catch (error) {
      let errorDetails;
      if (error instanceof PackageNotFoundError) {
         errorDetails = getNotFoundError(
            `Package for contents listing matching URI '${uri.href}'`,
         );
      } else if (
         error instanceof Error &&
         (error.message.includes("Invalid package name") ||
            error.message.includes("Invalid environment name"))
      ) {
         errorDetails = getNotFoundError(
            `Invalid environment/package identifier in URI '${uri.href}'`,
         );
      } else {
         logger.error(
            `[MCP Server Error] Error getting package contents for ${uri.href}:`,
            { error },
         );
         errorDetails = getInternalError(
            `GetResource (package contents: ${uri.href})`,
            error,
         );
      }
      throw new McpGetResourceError(errorDetails);
   }
}

/**
 * Registers the Malloy Package resource type with the MCP server.
 * Handles listing models/notebooks/sources within packages and getting package details.
 */
export function registerPackageResource(
   mcpServer: McpServer,
   projectStore: ProjectStore,
): void {
   // Define the expected parameter type for the read callback
   type ReadPackageParams = {
      fsPath?: string; // Expect string
      projectName?: string; // Expect string
      packageName?: string; // Expect string
   };

   mcpServer.resource(
      "package",
      new ResourceTemplate(
         "malloy://project/{projectName}/package/{packageName}",
         {
            list: undefined, // TODO: Implement and add listAllPackages here if needed
         },
      ),
      (uri, params) =>
         handleResourceGet(
            uri,
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            params as ReadPackageParams, // Cast params to the updated type
            "package",
            async ({
               projectName,
               packageName,
            }: {
               // Keep unknown here as it reflects the raw input possibility
               // before validation inside the async function
               projectName?: unknown;
               packageName?: unknown;
            }) => {
               try {
                  if (typeof projectName !== "string") {
                     throw new Error("Invalid environment name parameter.");
                  }
                  if (typeof packageName !== "string") {
                     throw new Error("Invalid package name parameter.");
                  }
                  const project = await projectStore.getProject(
                     projectName,
                     false,
                  );
                  const pkg = await project.getPackage(packageName, false);
                  return pkg.getPackageMetadata();
               } catch (error) {
                  let errorDetails;
                  if (error instanceof PackageNotFoundError) {
                     errorDetails = getNotFoundError(
                        `Package resource matching URI '${uri.href}'`,
                     );
                  } else if (
                     error instanceof Error &&
                     (error.message.includes("Invalid package name") ||
                        error.message.includes("Invalid environment name"))
                  ) {
                     errorDetails = getNotFoundError(
                        `Invalid environment/package identifier in URI '${uri.href}'`,
                     );
                  } else {
                     errorDetails = getInternalError(
                        `GetResource (package: ${uri.href})`,
                        error,
                     );
                  }
                  throw new McpGetResourceError(errorDetails);
               }
            },
            RESOURCE_METADATA.package,
         ),
   );

   // *** Register Package Contents Resource ***
   mcpServer.resource(
      "package-contents",
      new ResourceTemplate(
         "malloy://project/{projectName}/package/{packageName}/contents",
         {
            // Contents are listed via GetResource on this specific URI,
            // so no general list callback needed here.
            list: undefined,
         },
      ),
      // Custom handler because the response format is just the array, not {definition, metadata}
      async (
         uri: URL,
         params: ReadPackageParams,
      ): Promise<ReadResourceResult> => {
         try {
            // Reuse param validation/typing
            const validatedParams = params as ReadPackageParams;

            // Get the array of resource definitions
            const resourceDefinitions = await handleGetPackageContents(
               uri,
               validatedParams,
               projectStore,
            );

            // Return the array directly as JSON content
            return {
               contents: [
                  {
                     type: "application/json",
                     uri: uri.href,
                     text: JSON.stringify(resourceDefinitions, null, 2),
                  },
               ],
            };
         } catch (error) {
            logger.error(
               `[MCP Server Error] Error reading package contents ${uri.href}:`,
               { error },
            );

            let errorDetails: ErrorDetails;

            // Check if it's the specific error thrown by handleGetPackageContents
            if (error instanceof McpGetResourceError) {
               // Specifically check if it's a 'not found' error from the inner handler
               if (error.message.includes("not found")) {
                  // Re-use the not found details directly
                  errorDetails = error.details;
               } else {
                  // It was a different McpGetResourceError (e.g., internal from inner handler)
                  errorDetails = error.details;
               }
            } else if (error instanceof PackageNotFoundError) {
               // Handle PackageNotFoundError specifically if it bubbles up here
               errorDetails = getNotFoundError(
                  `Package for contents listing matching URI '${uri.href}'`,
               );
            } else if (
               error instanceof Error &&
               (error.message.includes("Invalid package name") ||
                  error.message.includes("Invalid environment name"))
            ) {
               // Handle invalid identifier errors specifically
               errorDetails = getNotFoundError(
                  `Invalid environment/package identifier in URI '${uri.href}'`,
               );
            } else {
               // Handle other unexpected errors
               errorDetails = getInternalError(
                  `GetResource (package contents: ${uri.href})`,
                  error,
               );
            }

            // Format the error response
            return {
               isError: true,
               contents: [
                  {
                     type: "application/json",
                     uri: uri.href,
                     text: JSON.stringify(
                        {
                           error: errorDetails.message,
                           suggestions: errorDetails.suggestions,
                        },
                        null,
                        2,
                     ),
                  },
               ],
            };
         }
      },
   );
}
