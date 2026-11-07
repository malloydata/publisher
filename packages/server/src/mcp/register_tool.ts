// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { Tracer } from "@opentelemetry/api";
import type { z } from "zod/v3";
import { logger } from "../logger";
import {
   lazyCounter,
   lazyHistogram,
   REQUEST_DURATION_BUCKETS_MS,
   withSpan,
} from "../telemetry";
import { classifyToolError } from "./handler_utils";
import type { RequestExtra } from "./request_extra";
import { jsonToolError, type ToolResult } from "./tool_response";

type ToolOutcome = "ok" | "error" | "exception";

const toolCalls = lazyCounter(
   "publisher_mcp_tool_calls_total",
   "MCP tool calls. Labels: tool, outcome ('ok' answered, 'error' answered with isError, 'exception' threw).",
);
const toolDuration = lazyHistogram(
   "publisher_mcp_tool_duration_ms",
   "Wall-clock duration of an MCP tool call. Labels: tool, outcome.",
   { buckets: REQUEST_DURATION_BUCKETS_MS },
);

type Params<Shape extends z.ZodRawShape> = z.infer<z.ZodObject<Shape>>;

type ToolHandler<Shape extends z.ZodRawShape> = (
   params: Params<Shape>,
   extra: RequestExtra,
) => Promise<ToolResult>;

/**
 * Wraps a tool handler so each call runs in one `publisher.mcp.tool` span and
 * records one counter increment, one duration and one exit log line.
 */
export function instrumentTool<Shape extends z.ZodRawShape>(
   tool: string,
   handler: ToolHandler<Shape>,
   tracer?: Tracer,
): ToolHandler<Shape> {
   return async (params, extra) => {
      const start = performance.now();
      let outcome: ToolOutcome = "exception";
      try {
         return await withSpan(
            "publisher.mcp.tool",
            { "mcp.tool.name": tool },
            async (span) => {
               const result = await handler(params, extra);
               outcome = result.isError === true ? "error" : "ok";
               span.setAttribute("mcp.tool.outcome", outcome);
               return result;
            },
            tracer,
         );
      } finally {
         const durationMs = performance.now() - start;
         toolCalls().add(1, { tool, outcome });
         toolDuration().record(durationMs, { tool, outcome });
         logger.info("MCP tool call", {
            tool,
            outcome,
            durationMs: Math.round(durationMs),
         });
      }
   };
}

/** Registers an instrumented tool. */
export function registerTool<Shape extends z.ZodRawShape>(
   server: McpServer,
   name: string,
   description: string,
   inputSchema: Shape,
   handler: ToolHandler<Shape>,
): void {
   server.registerTool(
      name,
      { description, inputSchema },
      instrumentTool(name, handler) as never,
   );
}

/** A tool whose every failure becomes a classified isError answer. */
interface GuardedTool<Shape extends z.ZodRawShape> {
   name: string;
   description: string;
   inputSchema: Shape;
   /** classifyToolError's operation, and the `[MCP Tool <op>]` log prefix. */
   op: string;
   /** What the warn line says went wrong, e.g. "lookup failed". */
   failed: string;
   /** The malloy:// URI every answer carries. */
   uri(params: Params<Shape>): string;
   /** The resource an error answer names. */
   identifier(params: Params<Shape>): string;
   /** Request fields the warn line carries beside the error. */
   logFields?(params: Params<Shape>): Record<string, unknown>;
   /** Merged into the error payload, so it keeps the success shape's keys. */
   errorPayload?: Record<string, unknown>;
   run(
      params: Params<Shape>,
      uri: string,
      extra: RequestExtra,
   ): Promise<ToolResult>;
}

/**
 * Registers an instrumented tool that answers a thrown error with a clean
 * isError payload rather than a transport fault.
 */
export function registerGuardedTool<Shape extends z.ZodRawShape>(
   server: McpServer,
   tool: GuardedTool<Shape>,
): void {
   registerTool(
      server,
      tool.name,
      tool.description,
      tool.inputSchema,
      async (params, extra) => {
         const uri = tool.uri(params);
         try {
            return await tool.run(params, uri, extra);
         } catch (error) {
            logger.warn(`[MCP Tool ${tool.op}] ${tool.failed}`, {
               ...tool.logFields?.(params),
               error: error instanceof Error ? error.message : String(error),
            });
            return jsonToolError(
               uri,
               classifyToolError(tool.op, tool.identifier(params), error),
               tool.errorPayload,
            );
         }
      },
   );
}
