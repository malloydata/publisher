import { URL } from "url";
import type { ResourceMetadata } from "@modelcontextprotocol/sdk/server/mcp";
import type { ReadResourceResult } from "@modelcontextprotocol/sdk/types.js";
import { ProjectStore } from "../service/project_store";
import {
   PackageNotFoundError,
   ModelNotFoundError,
   ModelCompilationError,
   ProjectNotFoundError,
} from "../errors";
import {
   getNotFoundError,
   getInternalError,
   getMalloyErrorDetails,
   type ErrorDetails,
} from "./error_messages";
import type { Model } from "../service/model";

// Custom error to wrap specific GetResource application errors
export class McpGetResourceError extends Error {
   details: ErrorDetails;

   constructor(details: ErrorDetails) {
      super(details.message); // Pass message to the base Error constructor
      this.name = "McpGetResourceError"; // Custom error name
      this.details = details; // Store the structured details

      // Maintain stack trace (important for V8)
      if (Error.captureStackTrace) {
         Error.captureStackTrace(this, McpGetResourceError);
      }
   }
}

// Helper type for the data fetching logic within a resource handler
type GetDataLogic<TParams, TDefinition> = (
   params: TParams,
   uri: URL,
) => Promise<TDefinition>;

/**
 * Handles the common logic for GetResource handlers, fetching data and formatting the response or error.
 */
export async function handleResourceGet<
   TParams extends Record<string, unknown>,
   TDefinition,
>(
   uri: URL,
   params: TParams,
   resourceType: string, // e.g., 'project', 'package' for logging/errors
   getData: GetDataLogic<TParams, TDefinition>,
   resourceMetadata: ResourceMetadata | undefined,
): Promise<ReadResourceResult> {
   try {
      const definition = await getData(params, uri);

      // Combine definition and metadata
      const responsePayload = JSON.stringify(
         { definition: definition, metadata: resourceMetadata },
         null,
         2,
      );

      return {
         contents: [
            {
               type: "application/json",
               uri: uri.href,
               text: responsePayload,
            },
         ],
      };
   } catch (error) {
      console.error(
         `[MCP Server Error] Error reading ${resourceType} ${uri.href}:`,
         error,
      );

      let errorDetails: ErrorDetails;

      // Determine the correct error details based on the error type
      if (error instanceof McpGetResourceError) {
         // The getData function already caught, formatted, and wrapped the error
         errorDetails = error.details;
      } else {
         // Catch-all for truly unexpected errors not handled by the specific getData logic
         console.error(
            "[MCP Server Error] Unexpected error type caught in handleResourceGet:",
            error,
         );
         errorDetails = getInternalError(
            `GetResource (${resourceType})`,
            error,
         );
      }

      // Format the error response consistently
      return {
         isError: true,
         contents: [
            {
               type: "application/json", // Keep JSON for structured error
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
}

/**
 * Fetches and validates the Package and Model instances needed for query execution.
 * Handles errors related to package/model access and initial compilation.
 * @returns An object containing the Model instance or a pre-formatted ErrorDetails object.
 */
export async function getModelForQuery(
   projectStore: ProjectStore,
   projectName: string,
   packageName: string,
   modelPath: string,
): Promise<{ model: Model } | { error: ErrorDetails }> {
   try {
      const project = await projectStore.getProject(projectName, false);
      const pkg = await project.getPackage(packageName, false);
      const model = pkg.getModel(modelPath);
      if (!model || model.getModelType() === "notebook") {
         // Ensure it's actually a model
         const details = getNotFoundError(
            `model '${modelPath}' in package '${packageName}' for project '${projectName}'`,
         );
         return { error: details };
      }
      // Attempt to get the model definition early to catch initial compilation errors
      await model.getModel(); // This might throw ModelCompilationError
      return { model };
   } catch (error) {
      // Handle errors during package/model access or initial compilation
      let errorDetails: ErrorDetails;
      if (error instanceof ProjectNotFoundError) {
         errorDetails = getNotFoundError(`project '${projectName}'`);
      } else if (error instanceof PackageNotFoundError) {
         errorDetails = getNotFoundError(
            `package '${packageName}' in project '${projectName}'`,
         );
      } else if (error instanceof ModelNotFoundError) {
         errorDetails = getNotFoundError(
            `model '${modelPath}' in package '${packageName}' for project '${projectName}'`,
         );
      } else if (error instanceof ModelCompilationError) {
         errorDetails = getMalloyErrorDetails(
            "executeQuery (load model)",
            `${projectName}/${packageName}/${modelPath}`,
            error,
         );
      } else {
         // Unexpected error during setup
         errorDetails = getInternalError("executeQuery (Setup)", error);
      }
      console.error(
         `[MCP Server Error] Error accessing package/model for query: ${projectName}/${packageName}/${modelPath}`,
         error,
      );
      return { error: errorDetails };
   }
}

/**
 * Constructs a valid malloy:// URI string from its components.
 * Handles encoding of path segments.
 *
 * @param components An object containing the URI parts (project, package, model, etc.)
 * @param fragment Optional fragment identifier (e.g., #queryResult)
 * @returns A valid malloy:// URI string.
 */
export function buildMalloyUri(
   components: {
      project?: string;
      package?: string;
      model?: string;
      resourceType?:
         | "models"
         | "packages"
         | "notebooks"
         | "sources"
         | "queries"
         | "views"; // Type of resource list or specific resource
      resourceName?: string; // Specific name for model, query, etc.
      subResourceType?: "views"; // For views within sources
      subResourceName?: string; // Name of the view
   },
   fragment?: string,
): string {
   let path = "/project/";

   if (components.project) {
      path += encodeURIComponent(components.project);
   } else {
      // Default to 'home' if not provided, consistent with current behavior
      path += "home";
   }

   if (components.package) {
      path += "/package/" + encodeURIComponent(components.package);
   }

   if (components.resourceType) {
      path += "/" + components.resourceType;
      if (components.resourceName) {
         path += "/" + encodeURIComponent(components.resourceName);

         if (components.subResourceType && components.subResourceName) {
            path +=
               "/" +
               components.subResourceType +
               "/" +
               encodeURIComponent(components.subResourceName);
         }
      }
   }

   // The URL constructor seems to normalize malloy://path to malloy:///path
   // which breaks the tests expecting malloy://project/...
   // We manually construct the string instead.
   let uriString = "malloy:/" + path; // Start with one slash after scheme

   if (fragment) {
      uriString += "#" + fragment;
   }

   return uriString;
}
