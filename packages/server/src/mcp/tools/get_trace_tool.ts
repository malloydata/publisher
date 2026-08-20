import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { getMcpTraceMode } from "../../config";
import { logger } from "../../logger";
import { EnvironmentStore } from "../../service/environment_store";
import { McpTraceStore } from "../../service/mcp_trace_store";
import { type ErrorDetails } from "../error_messages";
import { buildMalloyUri, classifyToolError } from "../handler_utils";
import { jsonResource, jsonToolError } from "../tool_response";

const GET_TRACE_DESCRIPTION = `Look up a persisted malloy_getContext trace from this local Publisher. Only registered when PUBLISHER_MCP_TRACE=retrieval. Pass traceId for an exact lookup, or omit it to list recent traces. Never returns execute_query rows, givens, or credentials.`;

const getTraceShape = {
   traceId: z
      .string()
      .optional()
      .describe("Exact trace id from a get_context response."),
   limit: z
      .number()
      .int()
      .min(1)
      .max(100)
      .optional()
      .describe("When listing recent traces, how many to return. Default 20."),
};

export function registerGetTraceTool(
   mcpServer: McpServer,
   environmentStore: EnvironmentStore,
): void {
   if (getMcpTraceMode() !== "retrieval") return;

   mcpServer.tool(
      "malloy_getTrace",
      GET_TRACE_DESCRIPTION,
      getTraceShape,
      async (params) => {
         const uri = buildMalloyUri({}, "getTrace");
         try {
            const store = new McpTraceStore(
               environmentStore.storageManager.getDuckDbConnection(),
            );
            if (params.traceId) {
               const trace = await store.get(params.traceId);
               return jsonResource(uri, { traces: trace ? [trace] : [] });
            }
            const traces = await store.listRecent(params.limit ?? 20);
            return jsonResource(uri, { traces });
         } catch (error) {
            logger.warn("[MCP Tool getTrace] lookup failed", {
               error: error instanceof Error ? error.message : String(error),
            });
            const errorDetails: ErrorDetails = classifyToolError(
               "getTrace",
               "server",
               error,
            );
            return jsonToolError(uri, errorDetails);
         }
      },
   );
}
