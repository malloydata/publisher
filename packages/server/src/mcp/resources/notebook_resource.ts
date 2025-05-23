import {
   McpServer,
   ResourceTemplate,
} from "@modelcontextprotocol/sdk/server/mcp.js";
import { ProjectStore } from "../../service/project_store";
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
import type { components } from "../../api"; // Need this for CompiledModel type
import { URL } from "url";

// Define the expected parameter types
type NotebookParams = {
   projectName?: unknown;
   packageName?: unknown;
   notebookName?: unknown;
};

/**
 * Registers the Malloy Notebook resource type.
 * Handles getting details for a specific notebook.
 */
export function registerNotebookResource(
   mcpServer: McpServer,
   projectStore: ProjectStore,
): void {
   mcpServer.resource(
      "notebook",
      new ResourceTemplate(
         "malloy://project/{projectName}/package/{packageName}/notebooks/{notebookName}",
         { list: undefined }, // Listing notebooks is not supported via this template
      ),
      (uri, params) =>
         handleResourceGet(
            uri,
            params as NotebookParams,
            "notebook",
            async (
               { projectName, packageName, notebookName }: NotebookParams,
               uri: URL,
            ) => {
               if (
                  typeof projectName !== "string" ||
                  typeof packageName !== "string" ||
                  typeof notebookName !== "string"
               ) {
                  throw new Error("Invalid parameters for notebook resource.");
               }

               let modelInstance;
               try {
                  const project = await projectStore.getProject(
                     projectName,
                     false,
                  );
                  const pkg = await project.getPackage(packageName, false);
                  // Get the model instance using the notebookName as the path
                  modelInstance = pkg.getModel(notebookName);

                  // Check if it exists and is actually a notebook
                  if (
                     !modelInstance ||
                     modelInstance.getModelType() !== "notebook"
                  ) {
                     const isNotebookError =
                        modelInstance?.getModelType() !== "notebook";
                     const errorDetails = getNotFoundError(
                        `Notebook '${notebookName}' in package '${packageName}' project '${projectName}'${
                           isNotebookError ? " (not a .malloynb file)" : ""
                        }`,
                     );
                     throw new McpGetResourceError(errorDetails);
                  }

                  // Now try to compile/get the actual notebook content
                  const notebookContent: components["schemas"]["CompiledModel"] =
                     await modelInstance.getModel(); // This can throw ModelCompilationError
                  return notebookContent;
               } catch (error) {
                  if (error instanceof McpGetResourceError) {
                     throw error; // Re-throw if already formatted by checks above
                  }

                  // Handle specific errors like PackageNotFoundError or ModelCompilationError
                  if (error instanceof PackageNotFoundError) {
                     throw new McpGetResourceError(
                        getNotFoundError(
                           `Package '${packageName}' in project '${projectName}'`,
                        ),
                     );
                  }
                  // Handle ModelCompilationError from modelInstance.getModel()
                  if (error instanceof ModelCompilationError) {
                     const malloyErrorContext = `${projectName}/${packageName}/${notebookName}`;
                     throw new McpGetResourceError(
                        getMalloyErrorDetails(
                           "GetResource (notebook compilation)",
                           malloyErrorContext,
                           error,
                        ),
                     );
                  }
                  // Handle ModelNotFoundError specifically from pkg.getModel()
                  if (error instanceof ModelNotFoundError) {
                     throw new McpGetResourceError(
                        getNotFoundError(
                           `Notebook '${notebookName}' not found in package '${packageName}' project '${projectName}'`,
                        ),
                     );
                  }

                  // Handle other unexpected errors
                  console.error(
                     `[MCP Server Error] Error fetching notebook '${notebookName}' from ${uri.href}:`,
                     error,
                  );
                  const fallbackErrorDetails = getInternalError(
                     `GetResource (notebook: ${uri.href})`,
                     error,
                  );
                  throw new McpGetResourceError(fallbackErrorDetails);
               }
            },
            RESOURCE_METADATA.notebook,
         ),
   );
}
