// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { MalloyError } from "@malloydata/malloy";
import { EnvironmentStore } from "../service/environment_store";
import {
   AccessDeniedError,
   BadRequestError,
   ConnectionAuthError,
   ConnectionFailedError,
   UnconfiguredConnectionError,
   ConnectionNotFoundError,
   ConnectionPoolExhaustedError,
   InvalidArgumentError,
   PackageNotFoundError,
   ModelNotFoundError,
   ModelCompilationError,
   EnvironmentNotFoundError,
   NotQueryableError,
   OffSurfaceError,
   PackageManifestError,
   PayloadTooLargeError,
   QueryTimeoutError,
   ResponseUnserializableError,
   ServiceUnavailableError,
   logInternalFailure,
} from "../errors";
import {
   getEnvironmentNotFoundError,
   getNotFoundError,
   getInternalError,
   getMalloyErrorDetails,
   type ErrorDetails,
} from "./error_messages";
import type { Model } from "../service/model";
import type { Environment } from "../service/environment";
import type { Package } from "../service/package";
import { logger } from "../logger";

/**
 * Remediation for a ServiceUnavailableError. It covers both the memory governor
 * and the concurrency slots, so the wording names both.
 */
export const BACK_PRESSURE_SUGGESTIONS: readonly string[] = [
   "Retry once the publisher is below its configured memory or concurrency limit.",
   "If this persists, raise the limit or scale up the pod.",
];

/**
 * Turns a thrown error into agent-facing ErrorDetails, homed by error class, so
 * a missing package or a back-pressure rejection is not dressed up as a Malloy
 * syntax problem. Only classes about the caller's Malloy or request get the
 * Malloy advice; anything else is reported as internal.
 *
 * Routing is by class, not message: getMalloyErrorDetails's message patterns
 * do not all match what the shipped engine emits, so "did it refine?" would
 * send genuine compile errors to the internal branch.
 *
 * @param operation The operation that failed (e.g. 'compile', 'reloadPackage').
 * @param identifier The resource it failed on (e.g. 'env/package').
 */
export function classifyToolError(
   operation: string,
   identifier: string,
   error: unknown,
): ErrorDetails {
   if (error instanceof OffSurfaceError) {
      // Before the not-found branch, which would drop this message and tell the
      // agent to check its spelling.
      return {
         message: error.message,
         suggestions: [
            "This is curation, not a typo: the name is real and the package does not publish it. Retrying with a different spelling will not help.",
            "To query what IS published, call get_context for this package and use the model_path it returns, verbatim.",
         ],
      } satisfies ErrorDetails;
   }
   if (error instanceof PackageManifestError) {
      // An unusable publisher.json: neither "retry" (internal) nor "check your
      // .malloy files" (Malloy) applies.
      return {
         message: error.message,
         suggestions: [
            "This is not transient. The package's publisher.json is invalid, so retrying fails the same way until the file is fixed.",
            "Fix the field the message names in publisher.json, then call reload_package.",
         ],
      } satisfies ErrorDetails;
   }
   if (error instanceof EnvironmentNotFoundError && error.lookup) {
      return getEnvironmentNotFoundError(error.lookup);
   }
   if (
      error instanceof EnvironmentNotFoundError ||
      error instanceof PackageNotFoundError ||
      error instanceof ModelNotFoundError ||
      // A query-boundary denial is a deliberate 404; getNotFoundError also
      // drops the source name this class is careful not to confirm.
      error instanceof NotQueryableError ||
      // Otherwise reported as internal, and an agent retries it forever.
      error instanceof ConnectionNotFoundError
   ) {
      return getNotFoundError(identifier);
   }
   if (error instanceof ConnectionFailedError) {
      // The driver's text can name an internal host, so it is logged and left
      // out of the response, as on the HTTP 502.
      logInternalFailure(
         `Database unreachable during ${operation}`,
         error,
         "warn",
      );
      return {
         message: `The database connection for ${identifier} is down: the database could not be reached, so the query never ran.`,
         suggestions: [
            "The query and the model are fine. Do not rewrite them.",
            "Retry once. If it fails again, report that the database connection is down rather than changing the query.",
         ],
      } satisfies ErrorDetails;
   }
   if (error instanceof UnconfiguredConnectionError) {
      return {
         message: `${error.message}. The model for ${identifier} uses a connection this environment does not have, so the query never ran.`,
         suggestions: [
            "The query is fine. Do not rewrite it, and do not retry: it fails the same way until the connection exists.",
            "Report that the environment is missing this connection; it was likely deleted or renamed.",
         ],
      } satisfies ErrorDetails;
   }
   if (error instanceof ConnectionAuthError) {
      // The driver's text can name the user or account, so it is only logged.
      logInternalFailure(
         `Connection credentials rejected during ${operation}`,
         error,
         "warn",
      );
      return {
         message: `The database rejected the connection's credentials for ${identifier}. The query never ran.`,
         suggestions: [
            "The query and the model are fine. Do not rewrite them, and do not retry: it fails the same way until the credentials are fixed.",
            "Report that the connection's user, password, key or token needs updating.",
         ],
      } satisfies ErrorDetails;
   }
   if (error instanceof ServiceUnavailableError) {
      return {
         message: error.message,
         suggestions: [...BACK_PRESSURE_SUGGESTIONS],
      } satisfies ErrorDetails;
   }
   if (error instanceof ConnectionPoolExhaustedError) {
      return {
         message: error.message,
         suggestions: [
            "Retry shortly; this clears as other queries on the connection finish.",
            "If it persists, run fewer queries at once on this connection, or ask the operator to raise PUBLISHER_POSTGRES_POOL_MAX.",
         ],
      } satisfies ErrorDetails;
   }
   if (error instanceof ResponseUnserializableError) {
      // Before the shared branch below, whose "raise the cap" advice cannot
      // help a response that will not serialize at any cap.
      return {
         message: error.message,
         suggestions: [
            "This is not transient. The same query will fail the same way, so change the query rather than retrying it.",
            "Raising the byte cap will not help, because the response cannot be serialized at any cap. Shrink it instead: project fewer columns, add a LIMIT, or filter out the wide values.",
         ],
      } satisfies ErrorDetails;
   }
   if (
      error instanceof QueryTimeoutError ||
      error instanceof PayloadTooLargeError
   ) {
      // The messages already say to refine the query; the suggestions add that
      // this is not transient, which is why these are 504/413 and not the
      // retryable 503.
      return {
         message: error.message,
         suggestions: [
            "This is not transient. The same query will fail the same way, so change the query rather than retrying it.",
            "If it is already minimal, raise the configured cap named in the message.",
         ],
      } satisfies ErrorDetails;
   }
   // Must precede the Malloy branch: InvalidArgumentError extends
   // BadRequestError, which that branch matches.
   if (error instanceof InvalidArgumentError) {
      // A malformed argument that is not about Malloy, so it must not get the
      // Malloy advice. Not widened to BadRequestError: that class also wraps
      // genuine query-time compile failures, which do want the Malloy advice.
      return {
         message: error.message,
         suggestions: [
            "This is not transient. The same arguments will fail the same way, so change them rather than retrying.",
            "The message above names what was wrong. If it names an expected format, use that format exactly.",
         ],
      } satisfies ErrorDetails;
   }
   if (
      error instanceof MalloyError ||
      error instanceof ModelCompilationError ||
      // getMalloyErrorDetails recognizes the #(authorize) denial message.
      error instanceof AccessDeniedError ||
      error instanceof BadRequestError
   ) {
      return getMalloyErrorDetails(operation, identifier, error);
   }
   return getInternalError(operation, error);
}

/**
 * Resolves the model to query, or the ErrorDetails explaining why it cannot be.
 * The Environment is returned too because it owns the connection configs the
 * per-query metadata layers are read from.
 */
export async function getModelForQuery(
   environmentStore: EnvironmentStore,
   environmentName: string,
   packageName: string,
   modelPath: string,
): Promise<
   | { model: Model; environment: Environment; pkg: Package }
   | { error: ErrorDetails }
> {
   try {
      const environment = await environmentStore.getEnvironment(
         environmentName,
         false,
      );
      // Shed load before any disk / DB work, as the HTTP query controllers do.
      environment.assertCanAdmitQuery();
      const pkg = await environment.getPackage(packageName, false);
      const model = pkg.getModel(modelPath);
      if (!model || model.getType() === "notebook") {
         const details = getNotFoundError(
            `model '${modelPath}' in package '${packageName}' for environment '${environmentName}'`,
         );
         return { error: details };
      }
      // Surface a compilation error here rather than mid-query.
      await model.getModel();
      return { model, environment, pkg };
   } catch (error) {
      let errorDetails: ErrorDetails;
      if (error instanceof EnvironmentNotFoundError) {
         errorDetails = classifyToolError(
            "executeQuery (load model)",
            `environment '${environmentName}'`,
            error,
         );
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
         // Defensive: the #(authorize) gate fires at query time, not setup, but
         // a 403 must never surface as an opaque internal error.
         errorDetails = getMalloyErrorDetails(
            "executeQuery (load model)",
            `${environmentName}/${packageName}/${modelPath}`,
            error,
         );
      } else if (error instanceof ServiceUnavailableError) {
         errorDetails = {
            message: error.message,
            suggestions: [...BACK_PRESSURE_SUGGESTIONS],
         } satisfies ErrorDetails;
      } else {
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
 * Builds the malloy:// URI attached as `uri` metadata on tool results. It
 * identifies where a result came from; it is not a fetchable MCP resource.
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
   let uriString = "malloy:/" + path;

   if (fragment) {
      uriString += "#" + fragment;
   }

   return uriString;
}
