import { EnvironmentStore } from "../service/environment_store";
import {
   AccessDeniedError,
   PackageNotFoundError,
   ModelNotFoundError,
   ModelCompilationError,
   EnvironmentNotFoundError,
   ServiceUnavailableError,
} from "../errors";
import {
   getNotFoundError,
   getInternalError,
   getMalloyErrorDetails,
   type ErrorDetails,
} from "./error_messages";
import type { Model } from "../service/model";
import { logger } from "../logger";

/**
 * Turns a thrown error into agent-facing ErrorDetails, homed by error class.
 *
 * Without this, a tool that funnels every failure through getMalloyErrorDetails
 * dresses each one up as a Malloy language problem: a missing package tells the
 * agent to check its Malloy syntax, and a back-pressure rejection sends it
 * hunting for a typo instead of retrying. Only errors that really are about the
 * Malloy fall through to the Malloy advice.
 *
 * @param operation The operation that failed (e.g. 'compile', 'reloadPackage').
 * @param identifier The resource it failed on (e.g. 'env/package').
 */
export function classifyToolError(
   operation: string,
   identifier: string,
   error: unknown,
): ErrorDetails {
   if (
      error instanceof EnvironmentNotFoundError ||
      error instanceof PackageNotFoundError ||
      error instanceof ModelNotFoundError
   ) {
      return getNotFoundError(identifier);
   }
   if (error instanceof ServiceUnavailableError) {
      // Back-pressure: surface the server's own message so the caller knows to
      // retry rather than to go edit its Malloy.
      return {
         message: error.message,
         suggestions: [
            "Retry once the publisher is below its configured memory or concurrency limit.",
            "If this persists, raise the limit or scale up the pod.",
         ],
      } satisfies ErrorDetails;
   }
   return getMalloyErrorDetails(operation, identifier, error);
}

/**
 * Fetches and validates the Package and Model instances needed for query execution.
 * Handles errors related to package/model access and initial compilation.
 * @returns An object containing the Model instance or a pre-formatted ErrorDetails object.
 */
export async function getModelForQuery(
   environmentStore: EnvironmentStore,
   environmentName: string,
   packageName: string,
   modelPath: string,
): Promise<{ model: Model } | { error: ErrorDetails }> {
   try {
      const environment = await environmentStore.getEnvironment(
         environmentName,
         false,
      );
      // Shed load before any disk / DB work; mirrors the HTTP query
      // controllers so MCP traffic obeys the same back-pressure rules.
      environment.assertCanAdmitQuery();
      const pkg = await environment.getPackage(packageName, false);
      const model = pkg.getModel(modelPath);
      if (!model || model.getModelType() === "notebook") {
         // Ensure it's actually a model
         const details = getNotFoundError(
            `model '${modelPath}' in package '${packageName}' for environment '${environmentName}'`,
         );
         return { error: details };
      }
      // Attempt to get the model definition early to catch initial compilation errors
      await model.getModel(); // This might throw ModelCompilationError
      return { model };
   } catch (error) {
      // Handle errors during package/model access or initial compilation
      let errorDetails: ErrorDetails;
      if (error instanceof EnvironmentNotFoundError) {
         errorDetails = getNotFoundError(`environment '${environmentName}'`);
      } else if (error instanceof PackageNotFoundError) {
         errorDetails = getNotFoundError(
            `package '${packageName}' in environment '${environmentName}'`,
         );
      } else if (error instanceof ModelNotFoundError) {
         errorDetails = getNotFoundError(
            `model '${modelPath}' in package '${packageName}' for environment '${environmentName}'`,
         );
      } else if (error instanceof ModelCompilationError) {
         errorDetails = getMalloyErrorDetails(
            "executeQuery (load model)",
            `${environmentName}/${packageName}/${modelPath}`,
            error,
         );
      } else if (error instanceof AccessDeniedError) {
         // An #(authorize) denial during model setup. Funnel through
         // getMalloyErrorDetails (which recognizes the access-denied message and
         // gives supply-the-givens guidance) so a 403 never surfaces as an
         // opaque internal error. Defensive: the gate fires during query
         // execution today, not setup, but this keeps every error class homed.
         errorDetails = getMalloyErrorDetails(
            "executeQuery (load model)",
            `${environmentName}/${packageName}/${modelPath}`,
            error,
         );
      } else if (error instanceof ServiceUnavailableError) {
         // Back-pressure: don't dress this up as a 404/500. Surface the
         // server's own message so the MCP caller knows to retry.
         errorDetails = {
            message: error.message,
            suggestions: [
               "Retry after the publisher's memory usage drops below the configured low-water mark.",
               "If this happens repeatedly, raise PUBLISHER_MAX_MEMORY_BYTES or scale up the pod.",
            ],
         } satisfies ErrorDetails;
      } else {
         // Unexpected error during setup
         errorDetails = getInternalError("executeQuery (Setup)", error);
      }
      logger.error(
         `[MCP Server Error] Error accessing package/model for query: ${environmentName}/${packageName}/${modelPath}`,
         { error },
      );
      return { error: errorDetails };
   }
}

/**
 * Constructs a malloy:// URI string identifying the model a tool response came
 * from. Attached as `uri` metadata on tool results (get-context, execute-query,
 * docs-search); it is not a fetchable MCP resource.
 *
 * @param components The URI parts (environment, package, and optionally the model).
 * @param fragment Optional fragment identifier (e.g., "result", "get-context").
 * @returns A malloy:// URI string.
 */
export function buildMalloyUri(
   components: {
      environment?: string;
      package?: string;
      resourceType?: "models";
      resourceName?: string;
   },
   fragment?: string,
): string {
   let path = "/environment/";

   if (components.environment) {
      path += encodeURIComponent(components.environment);
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
      }
   }

   // The URL constructor normalizes malloy://path to malloy:///path, so we
   // build the string manually to keep the malloy://environment/... shape.
   let uriString = "malloy:/" + path; // Start with one slash after scheme

   if (fragment) {
      uriString += "#" + fragment;
   }

   return uriString;
}
