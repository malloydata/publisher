// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { logger } from "../logger";
import { resetInstrumentsForTesting } from "../telemetry";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../test_helpers/metrics_harness";
import type { EnvironmentStore } from "../service/environment_store";
import { initializeMcpServer } from "./server";
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

/** A real server over the in-memory transport, its store answering `getStatus`. */
async function connectedClient(getStatus: () => Promise<unknown>) {
   const server = initializeMcpServer({
      getStatus,
   } as unknown as EnvironmentStore);
   const [clientTransport, serverTransport] =
      InMemoryTransport.createLinkedPair();
   await server.connect(serverTransport);
   const client = new Client({ name: "telemetry-test", version: "0.0.0" });
   await client.connect(clientTransport);
   return client;
}

const exitLines = (spy: { mock: { calls: unknown[][] } }) =>
   spy.mock.calls.filter((args) => args[0] === "MCP tool call");

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
      const info = spyOn(logger, "info");
      try {
         await expect(handlers.get("execute_query")!()).rejects.toThrow("boom");
         const [line] = exitLines(info);
         expect(line?.[1]).toMatchObject({
            tool: "execute_query",
            outcome: "exception",
         });
      } finally {
         info.mockRestore();
      }
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

   describe("over the MCP protocol", () => {
      it.each([
         [
            "ok",
            async () => ({ environments: [], operationalState: "serving" }),
         ],
         [
            "error",
            async () => {
               throw new Error("store down");
            },
         ],
      ] as const)(
         "a get_status call that answers %s records one call, one duration and one exit line",
         async (outcome, getStatus) => {
            const client = await connectedClient(getStatus);
            const info = spyOn(logger, "info");
            try {
               await client.callTool({ name: "get_status", arguments: {} });
               const lines = exitLines(info);
               expect(lines).toHaveLength(1);
               const fields = lines[0]![1] as Record<string, unknown>;
               expect(Object.keys(fields).sort()).toEqual([
                  "durationMs",
                  "outcome",
                  "tool",
               ]);
               expect(fields).toMatchObject({ tool: "get_status", outcome });
               expect(Number.isInteger(fields.durationMs)).toBe(true);
            } finally {
               info.mockRestore();
               await client.close();
            }
            expect(
               await harness.collectAttributes(
                  "publisher_mcp_tool_calls_total",
               ),
            ).toEqual([{ tool: "get_status", outcome }]);
            expect(
               await harness.collectAttributes(
                  "publisher_mcp_tool_duration_ms",
               ),
            ).toEqual([{ tool: "get_status", outcome }]);
         },
      );
   });
});
