// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { resetInstrumentsForTesting } from "../telemetry";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../test_helpers/metrics_harness";
import { instrumentTools } from "./tool_telemetry";

/** A server whose `tool` keeps the (wrapped) handler it was given. */
function fakeServer() {
   const handlers = new Map<string, (...args: unknown[]) => unknown>();
   const server = {
      tool: (...args: unknown[]) => {
         handlers.set(
            args[0] as string,
            args[args.length - 1] as (...a: unknown[]) => unknown,
         );
      },
   };
   instrumentTools(server as unknown as McpServer);
   return { server, handlers };
}

describe("MCP tool telemetry", () => {
   let harness: MetricsHarness;
   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetInstrumentsForTesting();
   });
   afterEach(async () => {
      await harness.shutdown();
      resetInstrumentsForTesting();
   });

   it.each([
      ["ok", async () => ({ content: [] })],
      ["error", async () => ({ content: [], isError: true })],
   ] as const)(
      "records an %s call with its tool name",
      async (outcome, handler) => {
         const { server, handlers } = fakeServer();
         server.tool("get_status", "describe", {}, handler);
         await handlers.get("get_status")!();
         expect(
            await harness.collectCounter("publisher_mcp_tool_calls_total", {
               tool: "get_status",
               outcome,
            }),
         ).toBe(1);
         const duration = await harness.collectHistogram(
            "publisher_mcp_tool_duration_ms",
            { tool: "get_status", outcome },
         );
         expect(duration.count).toBe(1);
      },
   );

   it("records a throwing handler as an exception and rethrows", async () => {
      const { server, handlers } = fakeServer();
      server.tool("execute_query", async () => {
         throw new Error("boom");
      });
      await expect(handlers.get("execute_query")!()).rejects.toThrow("boom");
      expect(
         await harness.collectCounter("publisher_mcp_tool_calls_total", {
            tool: "execute_query",
            outcome: "exception",
         }),
      ).toBe(1);
   });

   it("passes the handler's arguments and result through unchanged", async () => {
      const { server, handlers } = fakeServer();
      const result = { content: [{ type: "text", text: "x" }] };
      let seen: unknown[] = [];
      server.tool("compile_model", "d", {}, async (...args: unknown[]) => {
         seen = args;
         return result;
      });
      expect(await handlers.get("compile_model")!({ a: 1 }, "extra")).toBe(
         result,
      );
      expect(seen).toEqual([{ a: 1 }, "extra"]);
   });
});
