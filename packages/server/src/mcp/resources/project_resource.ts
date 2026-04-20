import {
   McpServer,
   ResourceTemplate,
} from "@modelcontextprotocol/sdk/server/mcp.js";
import type { ListResourcesResult } from "@modelcontextprotocol/sdk/types.js"; // Needed for list return type
import { logger } from "../../logger";
import { ProjectStore } from "../../service/project_store";
import { getInternalError, getNotFoundError } from "../error_messages"; // Needed for error handling in list AND get
import { handleResourceGet, McpGetResourceError } from "../handler_utils";
import { RESOURCE_METADATA } from "../resource_metadata";

// Define an interface for the package object augmented with project name
interface PackageWithProject {
   name?: string;
   // Add other relevant package properties if needed
   projectName: string;
}

/**
 * Registers the Malloy environment resource type with the MCP server (URI scheme malloy://project/...).
 * Lists packages across environments and resolves environment metadata on read.
 */
export function registerProjectResource(
   mcpServer: McpServer,
   projectStore: ProjectStore,
): void {
   mcpServer.resource(
      "project",
      new ResourceTemplate("malloy://project/{projectName}", {
         /**
          * Handles ListResources requests.
          * If projectName is specified, lists packages for that environment (only 'home' supported).
          * If projectName is not specified (general ListResources call), lists packages for the default 'home' environment.
          */
         list: async (/* extra: ListProjectExtra - Deleted */): Promise<ListResourcesResult> => {
            logger.info(
               "[MCP LOG] Entering ListResources (environment) handler (listing ALL packages)...",
            );
            // Ignore parameters from 'extra' as URI path params aren't passed to list handlers.

            try {
               const allProjects = await projectStore.listProjects();
               logger.info(
                  `[MCP LOG] Found ${allProjects.length} environments defined.`,
               );

               const packagePromises = allProjects.map(async (proj) => {
                  try {
                     logger.info(
                        `[MCP LOG] Getting environment '${proj.name}' to list its packages...`,
                     );
                     const projectInstance = await projectStore.getProject(
                        proj.name!,
                        false,
                     ); // Use proj.name
                     const packages = await projectInstance.listPackages();
                     logger.info(
                        `[MCP LOG] Found ${packages.length} packages in environment '${proj.name}'.`,
                     );
                     // Return packages along with their project name for URI construction
                     return packages.map((pkg) => ({
                        ...pkg,
                        projectName: proj.name,
                     }));
                  } catch (projectError) {
                     logger.error(
                        `[MCP Server Error] Error getting/listing packages for environment ${proj.name}:`,
                        { error: projectError },
                     );
                     return []; // Return empty array for this environment on error
                  }
               });

               const results = await Promise.allSettled(packagePromises);
               const allPackagesWithProjectName = results
                  .filter((result) => result.status === "fulfilled")
                  // Use the specific interface instead of any[]
                  .flatMap(
                     (result) =>
                        (result as PromiseFulfilledResult<PackageWithProject[]>)
                           .value,
                  );

               logger.info(
                  `[MCP LOG] Total packages found across all environments: ${allPackagesWithProjectName.length}`,
               );

               const packageMetadata = RESOURCE_METADATA.package;
               const mappedResources = allPackagesWithProjectName.map((pkg) => {
                  const name = pkg.name || "unknown";
                  // Construct URI using the package's specific projectName
                  const uri = `malloy://project/${pkg.projectName}/package/${name}`;
                  return {
                     uri: uri,
                     name: name,
                     type: "package",
                     description: packageMetadata?.description as
                        | string
                        | undefined,
                     metadata: packageMetadata,
                  };
               });

               logger.info(
                  `[MCP LOG] ListResources (environment): Returning ${mappedResources.length} package resources.`,
               );
               return {
                  resources: mappedResources,
               };
            } catch (error) {
               // Catch errors from projectStore.listProjects() itself
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
            "project",
            async ({ projectName }: { projectName?: unknown }) => {
               logger.info(
                  `[MCP LOG] Entering GetResource (environment) handler for projectName: ${projectName}`,
               );
               // Validate environment name parameter
               if (typeof projectName !== "string") {
                  logger.error(
                     "[MCP LOG] GetResource (environment): Invalid environment name param.",
                  );
                  throw new Error("Invalid environment name parameter.");
               }

               try {
                  logger.info(
                     `[MCP LOG] GetResource: Getting environment '${projectName}'...`,
                  );
                  // Get the project instance, but we might not need its metadata directly
                  await projectStore.getProject(projectName, false);
                  // Construct the definition object expected by the test
                  const definition = { name: projectName };
                  logger.info(
                     `[MCP LOG] GetResource (environment): Returning definition for '${projectName}'.`,
                  );
                  // Return the explicit definition structure
                  return definition;
               } catch (error) {
                  logger.error(
                     `[MCP LOG] GetResource (environment): Error caught for '${projectName}':`,
                     { error },
                  );
                  // Catch expected errors from this specific resource logic
                  if (error instanceof Error) {
                     // Use getNotFoundError for the specific environment not found case
                     // or a generic message for the invalid param case.
                     const errorDetails = getNotFoundError(
                        error.message.includes("not found")
                           ? `Environment '${projectName}'` // More specific context
                           : `Invalid environment identifier provided for URI ${uri.href}`, // Generic but informative
                     );
                     // Re-throw structured error for handleResourceGet to catch
                     throw new McpGetResourceError(errorDetails);
                  }
                  // Re-throw unexpected errors to be caught by handleResourceGet's generic handler
                  throw error;
               }
            },
            RESOURCE_METADATA.project,
         ),
   );
}
