// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { EnvironmentStore } from "../service/environment_store";
import {
   EnvironmentNotFoundError,
   errorMapping,
   logInternalFailure,
   ModelNotFoundError,
   type ErrorMapping,
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
import {
   resolveCommandModel,
   resolveQueryTarget,
   type QueryResolutionOptions,
} from "../usecases/query";
import { logger } from "../logger";

/** Remediation for a ServiceUnavailableError; defined with the error table. */
export { BACK_PRESSURE_SUGGESTIONS } from "../errors";

/**
 * Turns a thrown error into agent-facing ErrorDetails, from the error table's
 * `mcp` column for the error's kind (errors.ts ERROR_TABLE), so a missing
 * package or a back-pressure rejection is not dressed up as a Malloy syntax
 * problem. Only kinds about the caller's Malloy or request get the Malloy
 * advice; anything without a row of its own is reported as internal.
 *
 * Routing is by kind, not message: getMalloyErrorDetails's message patterns
 * do not all match what the tools emit, so a pattern check would send
 * genuine compile errors to the internal branch.
 *
 * @param operation The operation that failed (e.g. 'compile', 'reloadPackage').
 * @param identifier The resource it failed on (e.g. 'env/package').
 */
export function classifyToolError(
   operation: string,
   identifier: string,
   error: unknown,
): ErrorDetails {
   const { mcp } = errorMapping(error) as ErrorMapping<unknown>;
   switch (mcp.advice) {
      case "fixed":
         if (mcp.log) {
            // The error's text is withheld from the agent, so it is logged.
            logInternalFailure(
               `${mcp.log} during ${operation}`,
               error as Error,
               "warn",
            );
         }
         return {
            message: mcp.message
               ? mcp.message(error, { operation, identifier })
               : (error as Error).message,
            suggestions: [...mcp.suggestions],
         };
      case "environment_not_found":
         if (error instanceof EnvironmentNotFoundError && error.lookup) {
            return getEnvironmentNotFoundError(error.lookup);
         }
         return getNotFoundError(identifier);
      case "not_found":
         return getNotFoundError(identifier);
      case "malloy":
         return getMalloyErrorDetails(operation, identifier, error);
      case "internal":
         return getInternalError(operation, error);
   }
}

/** The three names a query's model is resolved from. */
export interface QueryModelIds {
   environmentName: string;
   packageName: string;
   modelPath: string;
}

/**
 * How MCP resolves a query's model, beside {@link resolveCommandModel}: a
 * notebook or a missing model is refused while resolving, and the model is
 * compiled eagerly. execute_query and value search both run with it.
 */
export const MCP_MODEL_RESOLUTION = {
   resolveModel: resolveCommandModel,
   refuseNotebook: true,
   eagerCompile: true,
   modelNotFound: (ids: QueryModelIds) =>
      new ModelNotFoundError(modelNotFoundDetails(ids).message),
} satisfies QueryResolutionOptions;

/** MCP's refusal of a model the package does not serve as queryable (a notebook, or no such file). */
export function modelNotFoundDetails({
   environmentName,
   packageName,
   modelPath,
}: QueryModelIds): ErrorDetails {
   return getNotFoundError(
      `model '${modelPath}' in package '${packageName}' for environment '${environmentName}'`,
   );
}

/**
 * The account an MCP caller reads of a failure to set up a query (resolving
 * or compiling its model), logged as it is reported: a failure the caller can
 * act on (the error table's `mcpSetup` column names what it names) is
 * classified and logged as a warning; anything else is an internal setup
 * fault, logged as an error.
 */
export function mapQuerySetupError(
   error: unknown,
   fields: QueryModelIds,
): ErrorDetails {
   const { environmentName, packageName, modelPath } = fields;
   const identifier = setupIdentifier(
      error,
      environmentName,
      packageName,
      modelPath,
   );
   if (identifier === undefined) {
      logger.error(
         "[MCP Server Error] Error accessing package/model for query",
         { ...fields, error },
      );
      return getInternalError("executeQuery (Setup)", error);
   }
   logger.warn("[MCP Tool executeQuery] model setup refused", {
      ...fields,
      error: error instanceof Error ? error.message : String(error),
   });
   return classifyToolError("executeQuery (load model)", identifier, error);
}

/**
 * Resolves the model to query, or the ErrorDetails explaining why it cannot be.
 * The Environment is returned too because it owns the connection configs the
 * per-query metadata layers are read from. Value search's resolver.
 */
export async function getModelForQuery(
   environmentStore: EnvironmentStore,
   environmentName: string,
   packageName: string,
   modelPath: string,
): Promise<
   | { model: Model; environment: Environment; pkg: Package }
   // `cause` is what the access record classifies; `error` is what the caller reads.
   | { error: ErrorDetails; cause: unknown }
> {
   const fields = { environmentName, packageName, modelPath };
   let refusal: ErrorDetails | undefined;
   try {
      // The MCP tools serve the package's `latest`.
      const { environment, pkg, model } = await resolveQueryTarget(
         environmentStore,
         { ...fields, identity: {} },
         {
            ...MCP_MODEL_RESOLUTION,
            modelNotFound: () => {
               refusal = modelNotFoundDetails(fields);
               return new ModelNotFoundError(refusal.message);
            },
         },
      );
      // refuseNotebook refused a missing model above.
      return { model: model!, environment, pkg };
   } catch (error) {
      return {
         error: refusal ?? mapQuerySetupError(error, fields),
         cause: error,
      };
   }
}

/**
 * What a setup failure that is the caller's to act on names, from the error
 * table's `mcpSetup` column. A kind without one (a connection fault, a
 * malformed argument) is undefined: reported as an internal setup fault, not
 * classified.
 */
function setupIdentifier(
   error: unknown,
   environmentName: string,
   packageName: string,
   modelPath: string,
): string | undefined {
   switch (errorMapping(error).mcpSetup) {
      case "environment":
         return `environment '${environmentName}'`;
      case "package":
         return `package '${packageName}' in environment '${environmentName}'`;
      case "model":
         return `model '${modelPath}' in package '${packageName}' for environment '${environmentName}'`;
      case "resource":
         return `${environmentName}/${packageName}/${modelPath}`;
      case undefined:
         return undefined;
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
