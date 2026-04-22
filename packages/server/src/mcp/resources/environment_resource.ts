import {
   McpServer,
   ResourceTemplate,
} from "@modelcontextprotocol/sdk/server/mcp.js";
import type { ListResourcesResult } from "@modelcontextprotocol/sdk/types.js"; // Needed for list return type
import { logger } from "../../logger";
import { EnvironmentStore } from "../../service/environment_store";
import { getInternalError, getNotFoundError } from "../error_messages"; // Needed for error handling in list AND get
import { handleResourceGet, McpGetResourceError } from "../handler_utils";
import { RESOURCE_METADATA } from "../resource_metadata";

// Define an interface for the package object augmented with environment name
interface PackageWithEnvironment {
   name?: string;
   // Add other relevant package properties if needed
   environmentName: string;
}

/**
 * Registers the Malloy environment resource type with the MCP server (URI scheme malloy://environment/...).
 * Lists packages across environments and resolves environment metadata on read.
 */
export function registerEnvironmentResource(
   mcpServer: McpServer,
   environmentStore: EnvironmentStore,
): void {
   mcpServer.resource(
      "environment",
      new ResourceTemplate("malloy://environment/{environmentName}", {
         /**
          * Handles ListResources requests.
          * If environmentName is specified, lists packages for that environment (only 'home' supported).
          * If environmentName is not specified (general ListResources call), lists packages for the default 'home' environment.
          */
         list: async (/* extra: ListEnvironmentExtra - Deleted */): Promise<ListResourcesResult> => {
            logger.info(
               "[MCP LOG] Entering ListResources (environment) handler (listing ALL packages)...",
            );
            // Ignore parameters from 'extra' as URI path params aren't passed to list handlers.

            try {
               const allEnvironments =
                  await environmentStore.listEnvironments();
               logger.info(
                  `[MCP LOG] Found ${allEnvironments.length} environments defined.`,
               );

               const packagePromises = allEnvironments.map(async (env) => {
                  try {
                     logger.info(
                        `[MCP LOG] Getting environment '${env.name}' to list its packages...`,
                     );
                     const environmentInstance =
                        await environmentStore.getEnvironment(env.name!, false);
                     const packages = await environmentInstance.listPackages();
                     logger.info(
                        `[MCP LOG] Found ${packages.length} packages in environment '${env.name}'.`,
                     );
                     // Return packages along with their environment name for URI construction
                     return packages.map((pkg) => ({
                        ...pkg,
                        environmentName: env.name,
                     }));
                  } catch (environmentError) {
                     logger.error(
                        `[MCP Server Error] Error getting/listing packages for environment ${env.name}:`,
                        { error: environmentError },
                     );
                     return []; // Return empty array for this environment on error
                  }
               });

               const results = await Promise.allSettled(packagePromises);
               const allPackagesWithEnvironmentName = results
                  .filter((result) => result.status === "fulfilled")
                  .flatMap(
                     (result) =>
                        (
                           result as PromiseFulfilledResult<
                              PackageWithEnvironment[]
                           >
                        ).value,
                  );

               logger.info(
                  `[MCP LOG] Total packages found across all environments: ${allPackagesWithEnvironmentName.length}`,
               );

               const packageMetadata = RESOURCE_METADATA.package;
               const mappedResources = allPackagesWithEnvironmentName.map(
                  (pkg) => {
                     const name = pkg.name || "unknown";
                     // Construct URI using the package's specific environmentName
                     const uri = `malloy://environment/${pkg.environmentName}/package/${name}`;
                     return {
                        uri: uri,
                        name: name,
                        type: "package",
                        description: packageMetadata?.description as
                           | string
                           | undefined,
                        metadata: packageMetadata,
                     };
                  },
               );

               logger.info(
                  `[MCP LOG] ListResources (environment): Returning ${mappedResources.length} package resources.`,
               );
               return {
                  resources: mappedResources,
               };
            } catch (error) {
               // Catch errors from environmentStore.listEnvironments() itself
               logger.error(`[MCP Server Error] Error listing environments:`, {
                  error,
               });
               const errorDetails = getInternalError(
                  `ListResources (environment - initial list)`,
                  error,
               );
               logger.error("MCP ListResources (environment) error:", {
                  error: errorDetails.message,
               });
               logger.info(
                  "[MCP LOG] ListResources (environment): Returning empty on error listing environments.",
               );
               return { resources: [] };
            }
         },
      }),
      /** Handles GetResource requests for Malloy environments */
      (uri, params) =>
         handleResourceGet(
            uri,
            params,
            "environment",
            async ({ environmentName }: { environmentName?: unknown }) => {
               logger.info(
                  `[MCP LOG] Entering GetResource (environment) handler for environmentName: ${environmentName}`,
               );
               // Validate environment name parameter
               if (typeof environmentName !== "string") {
                  logger.error(
                     "[MCP LOG] GetResource (environment): Invalid environment name param.",
                  );
                  throw new Error("Invalid environment name parameter.");
               }

               try {
                  logger.info(
                     `[MCP LOG] GetResource: Getting environment '${environmentName}'...`,
                  );
                  // Get the environment instance, but we might not need its metadata directly
                  await environmentStore.getEnvironment(environmentName, false);
                  // Construct the definition object expected by the test
                  const definition = { name: environmentName };
                  logger.info(
                     `[MCP LOG] GetResource (environment): Returning definition for '${environmentName}'.`,
                  );
                  // Return the explicit definition structure
                  return definition;
               } catch (error) {
                  logger.error(
                     `[MCP LOG] GetResource (environment): Error caught for '${environmentName}':`,
                     { error },
                  );
                  // Catch expected errors from this specific resource logic
                  if (error instanceof Error) {
                     // Use getNotFoundError for the specific environment not found case
                     // or a generic message for the invalid param case.
                     const errorDetails = getNotFoundError(
                        error.message.includes("not found")
                           ? `Environment '${environmentName}'` // More specific context
                           : `Invalid environment identifier provided for URI ${uri.href}`, // Generic but informative
                     );
                     // Re-throw structured error for handleResourceGet to catch
                     throw new McpGetResourceError(errorDetails);
                  }
                  // Re-throw unexpected errors to be caught by handleResourceGet's generic handler
                  throw error;
               }
            },
            RESOURCE_METADATA.environment,
         ),
   );
}
