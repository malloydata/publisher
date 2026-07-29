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
import { type ErrorDetails } from "../error_messages";
import {
   buildMalloyUri,
   classifyToolError,
   getModelForQuery,
} from "../handler_utils";
import { jsonResource, jsonToolError } from "../tool_response";
import { buildQueryEnvelope } from "../query_envelope";
import { bigIntReplacer } from "../../json_utils";
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
   verbose: z
      .boolean()
      .optional()
      .describe(
         "Return the raw Malloy result instead of rows plus _meta. Two things live only here: the generated SQL, useful for confirming a query compiled the way you meant, and per-cell type subtypes (integer vs number). The schema, render annotations, and timezone are already in _meta, so do NOT set this to read those. Reading values is what the default rows are for.",
      ),
};

const EXECUTE_QUERY_DESCRIPTION = `Run a Malloy query against a model and return the rows. Takes either ad-hoc Malloy in query, or a named view/query via queryName (with sourceName for a view).

## Contract rules
- Check _limit_hit before reporting any total, count, or "top N". True means the row cap cut the result off and more rows exist, so the numbers in front of you are a partial set, not the answer.
- Never sum or count the returned rows to state a total when _limit_hit or _rows_truncated is set. Aggregate in the query instead.
- Use source, view, and field names exactly as malloy_getContext returned them.

## Response
A JSON object, the same shape Credible's execute_query returns, so a data app behaves the same authored locally and served in production:
- rows: flat objects keyed by column name, the shape an in-package data app receives.
- _meta: the Malloy metadata flat rows drop (schema with field types and render tags, annotations, connection_name, query_timezone).
- _query_row_limit: the cap pushed into the SQL, from the query's own limit: or the server default.
- _limit_hit: the row count equals that cap.
- _rows_truncated / _total_rows / _returned_rows: present only when the payload cap dropped rows.
- warning, renderLogErrors: present only when they apply.

A query with no limit: of its own gets the server default, so a result landing exactly on _query_row_limit is almost never the whole table.`;

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
      EXECUTE_QUERY_DESCRIPTION,
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
            verbose,
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
            return jsonToolError(
               "error://executeQuery/modelAccess",
               modelResult.error,
            );
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
            // The two call modes differ only in which arguments carry the
            // query; everything after the run is identical, so they share one
            // path rather than two copies that can drift.
            const { result, compactResult, rowLimit } =
               await runWithQueryTimeout(
                  (abortSignal) =>
                     query
                        ? model.getQueryResults(
                             undefined,
                             undefined,
                             query,
                             filterParams,
                             undefined,
                             givens as Record<string, GivenValue> | undefined,
                             abortSignal,
                          )
                        : model.getQueryResults(
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

            // Render-tag validation reads the FULL Malloy result: the tags live
            // in its schema annotations, which the flat rows do not carry. It
            // runs regardless of which shape is returned.
            const { validateRenderTags } = await import(
               "@malloydata/render-validator"
            );
            const renderLogs = validateRenderTags(result);

            const resultUri = buildMalloyUri(
               {
                  environment: environmentName,
                  package: packageName,
                  resourceType: "models" as const,
                  resourceName: modelPath,
               },
               "result",
            );

            if (verbose) {
               return jsonResource(resultUri, result, {
                  space: 2,
                  text:
                     renderLogs.length > 0
                        ? `Render tag warnings:\n${JSON.stringify(renderLogs, null, 2)}`
                        : undefined,
               });
            }

            const envelope = buildQueryEnvelope(
               compactResult,
               rowLimit,
               result,
               renderLogs.map((log) => log.message),
            );
            return jsonResource(resultUri, envelope, {
               space: 2,
               // BigInt reaches here: compactResult is raw driver output and
               // DuckDB returns count() as one.
               replacer: bigIntReplacer,
               // A truncated or capped result is the case an agent most needs
               // to notice, so it is stated in text rather than left for a
               // client that parses the payload.
               text: envelope.warning,
            });
         } catch (queryError) {
            // Handle query execution errors (syntax errors, invalid queries, etc.)
            logger.error(
               `[MCP Server Error] Error executing query in ${environmentName}/${packageName}/${modelPath}:`,
               { error: queryError },
            );
            // Home the error by class first. tryAcquireQuerySlot runs inside
            // this try, so at the concurrency cap a ServiceUnavailableError
            // lands here; funnelling that through the Malloy helper told the
            // agent to check its syntax when the answer was to retry.
            const errorDetails: ErrorDetails = classifyToolError(
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

            return jsonToolError("error://executeQuery/queryExecution", {
               message: errorDetails.message,
               suggestions,
            });
         } finally {
            // Release on every exit path — success, error, or
            // unreachable code-path throw. `release()` is idempotent
            // so a double-fault during cleanup can't double-decrement.
            querySlot?.release();
         }
      },
   );
}
