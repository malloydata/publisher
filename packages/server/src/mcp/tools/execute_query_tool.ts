// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
import { filterPublisherOwnedRenderLogs } from "../../service/dashboard";
import { EnvironmentStore } from "../../service/environment_store";
import { RESTRICTED_CONSTRUCTS, type ErrorDetails } from "../error_messages";
import {
   buildMalloyUri,
   classifyToolError,
   getModelForQuery,
} from "../handler_utils";
import { jsonResource, jsonToolError } from "../tool_response";
import { buildQueryEnvelope } from "../query_envelope";
import { executeQueryToolMeta } from "../ui_resources";
import { mintCorrelationId } from "../../service/query_metadata";
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
   query: z
      .string()
      .optional()
      .describe(
         `Ad-hoc Malloy query code. Runs in restricted mode: it may not use ${RESTRICTED_CONSTRUCTS} — put those in a model file and reload instead.`,
      ),
   sourceName: z
      .string()
      .optional()
      .describe(
         "Source name for a view. A NAME, not Malloy code: one name exactly as malloy_getContext returned it, sent bare (the server quotes it, so a hyphen or a reserved word is fine — do not add backticks yourself). Anything richer, such as a parameterized source or an inline extension, goes in query.",
      ),
   queryName: z
      .string()
      .optional()
      .describe(
         "Named query or view. A NAME, not Malloy code, on the same terms as sourceName: one view name as malloy_getContext returned it. A dotted path (carriers.by_name), a refinement (by_carrier + { limit: 10 }), or anything containing a newline goes in query instead.",
      ),
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
   // Read by the MCP Apps widget, not by the server: the handler below ignores
   // it. Declared here because a parameter a client cannot see is a parameter no
   // agent will send, which would leave the widget's expand behaviour
   // unreachable. Matches the `expanded` flag Credible's execute_query takes.
   expanded: z
      .boolean()
      .optional()
      .describe(
         "Rendering hint for chat clients that support MCP Apps: when true, the rendered result opens in the conversation instead of sitting behind a 'Show Query Result' toggle. Set it for a result that answers the question being asked. Do NOT set it for a query containing nest:, or for a wide table that will not fit on one screen. Ignored by clients without MCP Apps support, and it does not change the returned rows.",
      ),
};

const EXECUTE_QUERY_DESCRIPTION = `Run a Malloy query against a model and return the rows. Takes either ad-hoc Malloy in query, or a named view/query via queryName (with sourceName for a view).

## Contract rules
- Check _limit_hit before reporting any total, count, or "top N". True means the server's default cap cut the result off and more rows exist, so what came back is a partial set, not the answer.
- Never sum or count the returned rows to state a total when _limit_hit or _rows_truncated is set. Aggregate in the query instead.
- _returned_rows: 0 with _rows_truncated set means one row was too large to send, NOT that nothing matched. Do not report it as an empty result.
- Use source, view, and field names exactly as malloy_getContext returned them. sourceName/queryName take one NAME each, never Malloy code — they are quoted for you, so send even a hyphenated name bare, and put anything richer (a dotted path, a refinement, a second statement) in query.
- query is RESTRICTED: no raw SQL/import/##! (see its param doc).

## Response
A JSON object, the same shape Credible's execute_query and an in-package data app receive:
- rows: flat objects keyed by column name.
- _meta: the Malloy metadata flat rows drop (schema with field types and render tags, annotations, connection_name, query_timezone).
- _query_row_limit: the cap pushed into the SQL, from the query's own limit: or the server default.
- _limit_source: "query" when the cap came from the query's own limit:/top:, "server_default" otherwise.
- _limit_hit: the row count equals that cap AND the cap was the server default, so a query carrying its own limit:/top: never sets it and exactly that many rows is a complete answer.
- _rows_truncated / _total_rows / _returned_rows: present only when the payload cap dropped rows.
- _query_id: this query's id in the warehouse's own query history. Present only where enabled.
- warning, renderLogErrors: present only when they apply.

Values above 2^53 are returned as JSON strings so their digits survive.`;

// Type inference is handled automatically by the MCP server based on the executeQueryShape

/**
 * Registers the malloy_executeQuery tool with the MCP server.
 */
export function registerExecuteQueryTool(
   mcpServer: McpServer,
   environmentStore: EnvironmentStore,
): void {
   // registerTool, not the older tool(name, description, shape, cb) overload:
   // that overload has no slot for `_meta`, which is how a tool declares the
   // MCP Apps widget that renders its result. The description and the input
   // shape are the same values the overload received, deliberately unchanged:
   // the description's ordering is load-bearing (a client that truncates it must
   // not lose the rules at the top), and the shape is the tool's public
   // contract.
   mcpServer.registerTool(
      "malloy_executeQuery",
      {
         description: EXECUTE_QUERY_DESCRIPTION,
         inputSchema: executeQueryShape,
         // Absent unless the widget bundle was built, so the tool never points
         // at a resource this server cannot serve.
         _meta: executeQueryToolMeta(),
      },
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
            return jsonToolError(
               "error://executeQuery/modelAccess",
               modelResult.error,
            );
         }

         // --- Execute Query ---
         const { model, environment, pkg } = modelResult;
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
            // Per-query metadata, built the same way the HTTP query controller
            // builds it: MCP is a query boundary like any other, and a
            // connection's enforced properties describe the deployment rather
            // than the protocol a query arrived over.
            const queryMetadataInput = {
               environment: environmentName,
               // Minted here because the envelope below returns it.
               correlationId: mintCorrelationId(),
               // The package owns its manifest, so the least-specific
               // author-declared layer is read here; the model knows only its
               // own file and its package's NAME.
               packageDeclaration: pkg.getDeclaredQueryMetadata(),
               // The environment owns the connection configs, so the default
               // and enforced layers are read here rather than from the model.
               connectionMetadata: (connectionName: string) => {
                  try {
                     const connection =
                        environment.getApiConnection(connectionName);
                     return {
                        default: connection.queryMetadata,
                        enforced: connection.queryMetadataEnforced,
                     };
                  } catch (error) {
                     logger.debug(
                        "[MCP Tool executeQuery] No query-metadata layers for connection",
                        { connectionName, error },
                     );
                     return null;
                  }
               },
            };
            // The two call modes differ only in which arguments carry the
            // query; everything after the run is identical, so they share one
            // path rather than two copies that can drift.
            const {
               result,
               compactResult,
               rowLimit,
               rowLimitSource,
               queryCorrelationId,
            } = await runWithQueryTimeout(
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
                          queryMetadataInput,
                          // The envelope below is built from `compactResult`, so
                          // that is the shape to cap and to guard. Left at the
                          // default this measured the full wrapped result and
                          // threw the string away, which meant a query could be
                          // refused on bytes the agent would never receive: the
                          // envelope is truncated to MAX_RESULT_CHARS anyway, so
                          // a wrapped result measuring over the cap was a 413 for
                          // a payload that would have arrived at 90k characters.
                          "compact",
                       )
                     : model.getQueryResults(
                          sourceName,
                          queryName,
                          undefined,
                          filterParams,
                          undefined,
                          givens as Record<string, GivenValue> | undefined,
                          abortSignal,
                          queryMetadataInput,
                          "compact",
                       ),
               getQueryTimeoutMs(),
            );

            // Render-tag validation reads the FULL Malloy result: the tags live
            // in its schema annotations, which the flat rows do not carry. It
            // runs regardless of which shape is returned.
            const { validateRenderTags } = await import(
               "@malloydata/render-validator"
            );
            const renderLogs = filterPublisherOwnedRenderLogs(
               validateRenderTags(result),
            );

            const resultUri = buildMalloyUri(
               {
                  environment: environmentName,
                  package: packageName,
                  resourceType: "models" as const,
                  resourceName: modelPath,
               },
               "result",
            );

            const envelope = buildQueryEnvelope(
               compactResult,
               rowLimit,
               result,
               renderLogs.map((log) => log.message),
               undefined,
               rowLimitSource,
               queryCorrelationId,
            );

            // A capped or truncated result, and a broken render tag, are the
            // things an agent most needs to notice, so they are stated in text
            // rather than left for a client that parses the payload.
            const notes = [
               envelope.warning,
               envelope.renderLogErrors &&
                  `Render tag problems: ${envelope.renderLogErrors.join("; ")}`,
            ].filter(Boolean);

            return jsonResource(resultUri, envelope, {
               space: 2,
               // BigInt reaches here: compactResult is raw driver output and
               // DuckDB returns count() as one.
               replacer: bigIntReplacer,
               text: notes.length > 0 ? notes.join("\n\n") : undefined,
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
