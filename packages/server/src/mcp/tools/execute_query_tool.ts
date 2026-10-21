// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { ErrorCode, McpError } from "@modelcontextprotocol/sdk/types.js";
import { z } from "zod/v3";
import type { GivenValue } from "@malloydata/malloy";
import { validateRenderTags } from "@malloydata/render-validator";
import { getQueryTimeoutMs } from "../../config";
import { DeclaredElsewhereError } from "../../errors";
import { logger } from "../../logger";
import {
   tryAcquireQuerySlot,
   type QuerySlotHandle,
} from "../../query_concurrency";
import { runWithQueryTimeout } from "../../query_timeout";
import { filterPublisherOwnedRenderLogs } from "../../service/report_manifest";
import { EnvironmentStore } from "../../service/environment_store";
import { RESTRICTED_CONSTRUCTS, type ErrorDetails } from "../error_messages";
import {
   buildMalloyUri,
   classifyToolError,
   getModelForQuery,
} from "../handler_utils";
import { jsonResource, jsonToolError } from "../tool_response";
import { buildQueryEnvelope } from "../query_envelope";
import { buildQueryMetadataInput } from "../../service/query_metadata";
import { bigIntReplacer } from "../../json_utils";
import { MCP_ERROR_MESSAGES } from "../mcp_constants";
import { recordDeprecatedFilterUsage } from "../../filter_deprecation";

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

const executeQueryShape = {
   environmentName: z
      .string()
      .describe("Environment name, as list_packages returns it."),
   packageName: z
      .string()
      .describe("Package containing the model, as list_packages returns it."),
   modelPath: z.string().describe("Path to the .malloy model file"),
   query: z
      .string()
      .optional()
      .describe(
         `Ad-hoc Malloy query code with exactly one run: statement; source: and query: definitions may come before it. Text with more than one run: is refused, so send each as its own call. Runs in restricted mode: it may not use ${RESTRICTED_CONSTRUCTS} — put those in a model file and reload instead.`,
      ),
   sourceName: z
      .string()
      .optional()
      .describe(
         "Source name for a view. A NAME, not Malloy code: one name exactly as get_context returned it, sent bare (the server quotes it, so a hyphen or a reserved word is fine — do not add backticks yourself). Anything richer, such as a parameterized source or an inline extension, goes in query.",
      ),
   queryName: z
      .string()
      .optional()
      .describe(
         "Named query or view. A NAME, not Malloy code, on the same terms as sourceName: one view name as get_context returned it. A dotted path (carriers.by_name), a refinement (by_carrier + { limit: 10 }), or anything containing a newline goes in query instead.",
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
   includeHiddenFilesAndSources: z
      .boolean()
      .optional()
      .describe(
         "Set true to also run the files and sources this package's index.malloy hides, so an author can test them without publishing them. It never bypasses #(authorize) or #(access_filter). Default false.",
      ),
};

// What a server offers unless its config is for authoring
// (isMcpIncludeHiddenFilesAndSources).
const { includeHiddenFilesAndSources: _authoringOnly, ...curatedShape } =
   executeQueryShape;

const EXECUTE_QUERY_DESCRIPTION = `Run a Malloy query against a model and return the rows. Takes either ad-hoc Malloy in query, or a named view/query via queryName (with sourceName for a view).

## Contract rules
- Check _limit_hit before reporting any total, count, or "top N". True means the server's default cap cut the result off and more rows exist, so what came back is a partial set, not the answer.
- Never sum or count the returned rows to state a total when _limit_hit or _rows_truncated is set. Aggregate in the query instead.
- _returned_rows: 0 with _rows_truncated set means one row was too large to send, NOT that nothing matched. Do not report it as an empty result.
- Use source, view, and field names exactly as get_context returned them. sourceName/queryName take one NAME each, never Malloy code — they are quoted for you, so send even a hyphenated name bare, and put anything richer (a dotted path, a refinement, a definition) in query.
- One run: per call. query may define sources and queries first, but more than one run: statement is refused rather than run; send each question as its own call.
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

export function registerExecuteQueryTool(
   mcpServer: McpServer,
   environmentStore: EnvironmentStore,
): void {
   const offerHidden =
      environmentStore.mcpIncludeHiddenFilesAndSources === true;
   mcpServer.tool(
      "execute_query",
      EXECUTE_QUERY_DESCRIPTION,
      // Typed as the full shape so the handler can name the parameter; the
      // offerHidden check below is what holds when it is not offered.
      (offerHidden
         ? executeQueryShape
         : curatedShape) as typeof executeQueryShape,
      async (params) => {
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
         // A caller can send the key even when the schema leaves it out, so
         // the setting decides, not the argument alone.
         const includeHiddenFilesAndSources =
            offerHidden && params.includeHiddenFilesAndSources === true;

         logger.debug("[MCP Tool executeQuery] Received params", { params });

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
         const modelResult = await getModelForQuery(
            environmentStore,
            environmentName,
            packageName,
            modelPath,
         );

         if ("error" in modelResult) {
            return jsonToolError(
               "error://executeQuery/modelAccess",
               modelResult.error,
            );
         }

         const { model, environment, pkg } = modelResult;
         // Shares the HTTP routes' slot pool so an agent loop cannot bypass
         // PUBLISHER_MAX_CONCURRENT_QUERIES; the label separates MCP load on
         // the dashboard. A refusal throws into the catch below.
         let querySlot: QuerySlotHandle | null = null;
         try {
            querySlot = tryAcquireQuerySlot("mcp:executeQuery");
            // Built as the HTTP controller builds it: a connection's enforced
            // properties describe the deployment, not the transport.
            const queryMetadataInput = buildQueryMetadataInput({
               environmentName,
               environment,
               pkg,
               noLayersLogMessage:
                  "[MCP Tool executeQuery] No query-metadata layers for connection",
            });
            // The two call modes differ only in which arguments carry the
            // query; everything after the run is identical, so they share one
            // path rather than two copies that can drift. Ad-hoc text runs on
            // its own: a sourceName sent beside it is not passed on.
            const target = query
               ? { sourceName: undefined, queryName: undefined, query }
               : { sourceName, queryName, query: undefined };
            const {
               result,
               compactResult,
               rowLimit,
               rowLimitSource,
               queryCorrelationId,
            } = await runWithQueryTimeout(
               (abortSignal) =>
                  model.getQueryResults(
                     target.sourceName,
                     target.queryName,
                     target.query,
                     filterParams,
                     undefined,
                     givens as Record<string, GivenValue> | undefined,
                     abortSignal,
                     queryMetadataInput,
                     // The envelope is built from `compactResult`, so cap
                     // that shape: measuring the wrapped result would 413 on
                     // bytes the agent never receives.
                     "compact",
                     // MCP sends no #(authorize) bypass, ever.
                     false,
                     includeHiddenFilesAndSources,
                  ),
               getQueryTimeoutMs(),
            );
            recordDeprecatedFilterUsage({ filterParams });

            // Render-tag validation reads the FULL Malloy result: the tags live
            // in its schema annotations, which the flat rows do not carry. It
            // runs regardless of which shape is returned.
            const renderLogs = filterPublisherOwnedRenderLogs(
               validateRenderTags(result),
               pkg.getModel(modelPath)?.carriesReportTag() ?? false,
               result,
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
            logger.error(
               `[MCP Server Error] Error executing query in ${environmentName}/${packageName}/${modelPath}:`,
               { error: queryError },
            );
            // By class: a slot refusal also lands here, and must say "retry",
            // not "check your syntax".
            const errorDetails: ErrorDetails = classifyToolError(
               "executeQuery",
               `${environmentName}/${packageName}/${modelPath}`,
               queryError,
            );

            // An undefined name is also what an author gets after saving a new
            // source or view without reloading, so point at the reload too.
            const suggestions = [...errorDetails.suggestions];
            if (
               !(queryError instanceof DeclaredElsewhereError) &&
               isUndefinedNameError(errorDetails.message)
            ) {
               suggestions.push(
                  "If you added or renamed this source or view on disk after the server loaded the package, the running model is still the one compiled at boot. Call reload_package for this package, then retry.",
               );
            }

            return jsonToolError("error://executeQuery/queryExecution", {
               message: errorDetails.message,
               suggestions,
            });
         } finally {
            // `release()` is idempotent, so a double-fault cannot
            // double-decrement.
            querySlot?.release();
         }
      },
   );
}
