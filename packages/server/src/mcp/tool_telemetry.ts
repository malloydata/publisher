// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { logger } from "../logger";
import {
   lazyCounter,
   lazyHistogram,
   REQUEST_DURATION_BUCKETS_MS,
} from "../telemetry";

export type ToolOutcome = "ok" | "error" | "exception";

const toolCalls = lazyCounter(
   "publisher_mcp_tool_calls_total",
   "MCP tool calls. Labels: tool, outcome ('ok' answered, 'error' answered with isError, 'exception' threw).",
);
const toolDuration = lazyHistogram(
   "publisher_mcp_tool_duration_ms",
   "Wall-clock duration of an MCP tool call. Labels: tool, outcome.",
   { buckets: REQUEST_DURATION_BUCKETS_MS },
);

type Handler = (...args: unknown[]) => unknown;

function outcomeOf(result: unknown): ToolOutcome {
   return typeof result === "object" &&
      result !== null &&
      (result as { isError?: unknown }).isError === true
      ? "error"
      : "ok";
}

/**
 * Wrap every tool registered on `server` from here on, so each call records
 * one counter increment, one duration, and one exit log line, whatever the
 * tool. Both registration forms are wrapped: `tool()` and `registerTool()`
 * (which the MCP Apps tools use, for their `_meta`). Neither calls the other,
 * so a call is counted once. The handler is always the registration's last
 * argument.
 */
export function instrumentTools(server: McpServer): void {
   for (const method of ["tool", "registerTool"] as const) {
      const original = server[method] as
         | ((...args: unknown[]) => unknown)
         | undefined;
      if (typeof original !== "function") continue;
      const register = original.bind(server);
      (server as unknown as Record<string, unknown>)[method] = (
         ...args: unknown[]
      ) => {
         const tool = String(args[0]);
         const handler = args[args.length - 1] as Handler;
         args[args.length - 1] = async (...callArgs: unknown[]) => {
            const start = performance.now();
            let outcome: ToolOutcome = "exception";
            try {
               const result = await handler(...callArgs);
               outcome = outcomeOf(result);
               return result;
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
         return register(...args);
      };
   }
}
