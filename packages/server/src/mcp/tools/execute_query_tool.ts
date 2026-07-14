import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { ErrorCode, McpError } from "@modelcontextprotocol/sdk/types.js";
import { z } from "zod";
import type { GivenValue } from "@malloydata/malloy";
import { getQueryTimeoutMs } from "../../config";
import { logger } from "../../logger";
import {
   tryAcquireQuerySlot,
   type QuerySlotHandle,
} from "../../query_concurrency";
import { runWithQueryTimeout } from "../../query_timeout";
import { EnvironmentStore } from "../../service/environment_store";
import { getMalloyErrorDetails, type ErrorDetails } from "../error_messages";
import { buildMalloyUri, getModelForQuery } from "../handler_utils";
import { MCP_ERROR_MESSAGES } from "../mcp_constants";

/**
 * Malloy's two ways of saying a name is not in the model's namespace: a bare
 * reference ("Reference to undefined object 'x'") and a name used where the
 * compiler expected a definition ("'x' is not defined").
 */
function isUndefinedNameError(message: string): boolean {
   return (
      message.includes("is not defined") ||
      message.includes("Reference to undefined object")
   );
}

// Zod shape defining required/optional params for executeQuery
const executeQueryShape = {
   // environmentName is required; other fields mirror SDK expectations
   environmentName: z
      .string()
      .describe(
         "Environment name. Call malloy_getContext with no arguments to list the available environments.",
      ),
   packageName: z
      .string()
      .describe(
         "Package containing the model. Call malloy_getContext with just environmentName to list its packages.",
      ),
   modelPath: z.string().describe("Path to the .malloy model file"),
   query: z.string().optional().describe("Ad-hoc Malloy query code"),
   sourceName: z.string().optional().describe("Source name for a view"),
   queryName: z.string().optional().describe("Named query or view"),
   filterParams: z
      .record(z.union([z.string(), z.array(z.string())]))
      .optional()
      .describe(
         "Filter parameter values keyed by filter name. Used with sources that declare #(filter) annotations.",
      ),
   givens: z
      .record(z.unknown())
      .optional()
      .describe(
         "Per-query given values that override model defaults. Keys are given names declared in the model's given: block.",
      ),
};

// Type inference is handled automatically by the MCP server based on the executeQueryShape

/**
 * Registers the malloy_executeQuery tool with the MCP server.
 */
export function registerExecuteQueryTool(
   mcpServer: McpServer,
   environmentStore: EnvironmentStore,
): void {
   mcpServer.tool(
      "malloy_executeQuery",
      "Executes a Malloy query (either ad-hoc or a named query/view defined in a model) against the specified model and returns the results as JSON.",
      executeQueryShape,
      /** Handles requests for the malloy_executeQuery tool */
      async (params) => {
         // Destructure environmentName as well
         const {
            environmentName,
            packageName,
            modelPath,
            query,
            sourceName,
            queryName,
            filterParams,
            givens,
         } = params;

         logger.info("[MCP Tool executeQuery] Received params:", { params });

         const hasAdhocQuery = !!query;
         const hasNamedQuery = !!queryName;

         if (!hasAdhocQuery && !hasNamedQuery) {
            throw new McpError(
               ErrorCode.InvalidParams,
               MCP_ERROR_MESSAGES.MISSING_REQUIRED_PARAMS,
            );
         }
         if (hasAdhocQuery && hasNamedQuery) {
            throw new McpError(
               ErrorCode.InvalidParams,
               MCP_ERROR_MESSAGES.MUTUALLY_EXCLUSIVE_PARAMS,
            );
         }
         // Zod/SDK handles missing required fields (packageName, modelPath) based on the shape

         // --- Get Package and Model ---
         logger.info(
            `[MCP Tool executeQuery] Calling getModelForQuery for ${environmentName}/${packageName}/${modelPath}`,
         );
         const modelResult = await getModelForQuery(
            environmentStore,
            environmentName,
            packageName,
            modelPath,
         );

         // Handle errors during package/model access (e.g., not found, initial compilation)
         if ("error" in modelResult) {
            // Format error details as structured JSON
            const errorJson = JSON.stringify(
               {
                  error: modelResult.error.message,
                  suggestions: modelResult.error.suggestions,
               },
               null,
               2,
            );
            return {
               isError: true,
               // Return as application/json nested inside a 'resource' type
               content: [
                  {
                     type: "resource", // Use 'resource' type
                     resource: {
                        type: "application/json", // Actual content type
                        uri: "error://executeQuery/modelAccess", // Placeholder URI
                        text: errorJson,
                     },
                  },
               ],
            };
         }

         // --- Execute Query ---
         const { model } = modelResult;
         logger.info(
            `[MCP Tool executeQuery] Model found. Proceeding to execute query.`,
         );
         // Per-pod concurrency slot. MCP shares the same slot pool
         // as the HTTP query routes so a hot agent loop can't
         // bypass PUBLISHER_MAX_CONCURRENT_QUERIES. `mcp:executeQuery`
         // is a fixed label so the dashboard can separate MCP load
         // from HTTP route load. Acquisition can throw
         // ServiceUnavailableError; the existing catch below surfaces
         // it as the standard MCP error-content payload.
         let querySlot: QuerySlotHandle | null = null;
         try {
            querySlot = tryAcquireQuerySlot("mcp:executeQuery");
            // If ad-hoc query is provided, use it directly in the 3rd arg
            if (query) {
               const { result } = await runWithQueryTimeout(
                  (abortSignal) =>
                     model.getQueryResults(
                        undefined,
                        undefined,
                        query,
                        filterParams,
                        undefined,
                        givens as Record<string, GivenValue> | undefined,
                        abortSignal,
                     ),
                  getQueryTimeoutMs(),
               );
               const { validateRenderTags } = await import(
                  "@malloydata/render-validator"
               );
               const renderLogs = validateRenderTags(result);

               const baseUriComponents = {
                  environment: environmentName,
                  package: packageName,
                  resourceType: "models" as const,
                  resourceName: modelPath,
               };
               const resultUri = buildMalloyUri(baseUriComponents, "result");
               const resultString = JSON.stringify(result, null, 2);

               const content = [
                  {
                     type: "resource" as const,
                     resource: {
                        type: "application/json",
                        uri: resultUri,
                        text: resultString,
                     },
                  },
               ];

               if (renderLogs.length > 0) {
                  return {
                     isError: false,
                     content: [
                        ...content,
                        {
                           type: "text" as const,
                           text: `Render tag warnings:\n${JSON.stringify(renderLogs, null, 2)}`,
                        },
                     ],
                  };
               }

               return { isError: false, content };
            } else if (queryName) {
               const { result } = await runWithQueryTimeout(
                  (abortSignal) =>
                     model.getQueryResults(
                        sourceName,
                        queryName,
                        undefined,
                        filterParams,
                        undefined,
                        givens as Record<string, GivenValue> | undefined,
                        abortSignal,
                     ),
                  getQueryTimeoutMs(),
               );
               const { validateRenderTags } = await import(
                  "@malloydata/render-validator"
               );
               const renderLogs = validateRenderTags(result);

               const baseUriComponents = {
                  environment: environmentName,
                  package: packageName,
                  resourceType: "models" as const,
                  resourceName: modelPath,
               };
               const resultUri = buildMalloyUri(baseUriComponents, "result");
               const resultString = JSON.stringify(result, null, 2);

               const content = [
                  {
                     type: "resource" as const,
                     resource: {
                        type: "application/json",
                        uri: resultUri,
                        text: resultString,
                     },
                  },
               ];

               if (renderLogs.length > 0) {
                  return {
                     isError: false,
                     content: [
                        ...content,
                        {
                           type: "text" as const,
                           text: `Render tag warnings:\n${JSON.stringify(renderLogs, null, 2)}`,
                        },
                     ],
                  };
               }

               return { isError: false, content };
            }

            // If execution reaches this point, something has gone wrong with
            // the earlier parameter validation logic. Throw an explicit error
            // so the return type is never 'undefined' from the compiler's
            // perspective.
            throw new McpError(
               ErrorCode.InternalError,
               "Unreachable executeQuery code path – parameters were not validated correctly.",
            );
         } catch (queryError) {
            // Handle query execution errors (syntax errors, invalid queries, etc.)
            logger.error(
               `[MCP Server Error] Error executing query in ${environmentName}/${packageName}/${modelPath}:`,
               { error: queryError },
            );
            const errorDetails: ErrorDetails = getMalloyErrorDetails(
               "executeQuery",
               `${environmentName}/${packageName}/${modelPath}`, // Include environment
               queryError,
            );

            // A name the model does not define reads as a typo, and the
            // suggestions say so. But the same error is what an author gets
            // after saving a new source or view: the served model is the one
            // compiled at boot, so the name exists on disk and not in memory.
            // Point at the reload rather than let them hunt for a typo that
            // isn't there.
            const suggestions = [...errorDetails.suggestions];
            if (isUndefinedNameError(errorDetails.message)) {
               suggestions.push(
                  "If you added or renamed this source or view on disk after the server loaded the package, the running model is still the one compiled at boot. Call malloy_reloadPackage for this package, then retry.",
               );
            }

            // Format error details as structured JSON
            const errorJson = JSON.stringify(
               {
                  error: errorDetails.message,
                  suggestions,
               },
               null,
               2,
            );
            return {
               isError: true,
               // Return as application/json nested inside a 'resource' type
               content: [
                  {
                     type: "resource", // Use 'resource' type
                     resource: {
                        type: "application/json", // Actual content type
                        uri: "error://executeQuery/queryExecution", // Placeholder URI
                        text: errorJson,
                     },
                  },
               ],
            };
         } finally {
            // Release on every exit path — success, error, or
            // unreachable code-path throw. `release()` is idempotent
            // so a double-fault during cleanup can't double-decrement.
            querySlot?.release();
         }
      },
   );
}
