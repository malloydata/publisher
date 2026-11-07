// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   afterAll,
   afterEach,
   beforeEach,
   describe,
   expect,
   it,
   spyOn,
} from "bun:test";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { SpanStatusCode } from "@opentelemetry/api";
import {
   BasicTracerProvider,
   InMemorySpanExporter,
   SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-node";
import { logger } from "../logger";
import { PackageNotFoundError } from "../errors";
import { resetInstrumentsForTesting } from "../telemetry";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../test_helpers/metrics_harness";
import type { EnvironmentStore } from "../service/environment_store";
import { getNotFoundError } from "./error_messages";
import { initializeMcpServer } from "./server";
import { instrumentTool, registerGuardedTool } from "./register_tool";
import type { ToolResult } from "./tool_response";

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

const answer = (isError: boolean): ToolResult => ({ isError, content: [] });

describe("MCP tool telemetry", () => {
   let harness: MetricsHarness;
   const exporter = new InMemorySpanExporter();
   const provider = new BasicTracerProvider({
      spanProcessors: [new SimpleSpanProcessor(exporter)],
   });
   // The provider's own tracer: a global one may already be registered.
   const tracer = provider.getTracer("test");

   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetInstrumentsForTesting();
      exporter.reset();
   });
   afterEach(async () => {
      await harness.shutdown();
      resetInstrumentsForTesting();
   });
   afterAll(async () => {
      await provider.shutdown();
   });

   it.each([
      ["ok", false],
      ["error", true],
   ] as const)(
      "records an %s call with its tool name, in one span",
      async (outcome, isError) => {
         await instrumentTool(
            "get_status",
            async () => answer(isError),
            tracer,
         )({}, {});
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
         const [span] = exporter.getFinishedSpans();
         expect(span?.name).toBe("publisher.mcp.tool");
         expect(span?.attributes).toEqual({
            "mcp.tool.name": "get_status",
            "mcp.tool.outcome": outcome,
         });
      },
   );

   it("records a throwing handler as an exception and rethrows", async () => {
      const info = spyOn(logger, "info");
      try {
         await expect(
            instrumentTool(
               "execute_query",
               async () => {
                  throw new Error("boom");
               },
               tracer,
            )({}, {}),
         ).rejects.toThrow("boom");
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
      const [span] = exporter.getFinishedSpans();
      expect(span?.status.code).toBe(SpanStatusCode.ERROR);
   });

   it("passes the handler's arguments and result through unchanged", async () => {
      const result = answer(false);
      let seen: unknown[] = [];
      const extra = { signal: new AbortController().signal };
      const handler = instrumentTool<{ a: never }>(
         "compile_model",
         async (...args) => {
            seen = args;
            return result;
         },
         tracer,
      );
      expect(await handler({ a: 1 } as never, extra)).toBe(result);
      expect(seen).toEqual([{ a: 1 }, extra]);
   });

   it("answers a guarded tool's throw as a classified error and logs it at warn", async () => {
      let handler:
         | ((params: unknown, extra: unknown) => Promise<ToolResult>)
         | undefined;
      registerGuardedTool(
         {
            registerTool: (_n: string, _config: unknown, h: typeof handler) => {
               handler = h;
            },
         } as unknown as McpServer,
         {
            name: "get_thing",
            description: "d",
            inputSchema: {},
            op: "getThing",
            failed: "lookup failed",
            uri: () => "malloy://environment/home#getThing",
            identifier: () => "env/pkg",
            logFields: () => ({ packageName: "pkg" }),
            errorPayload: { things: [] },
            run: async () => {
               throw new PackageNotFoundError("gone");
            },
         },
      );
      const warn = spyOn(logger, "warn");
      let result: ToolResult;
      try {
         result = await handler!({}, {});
         const calls = warn.mock.calls as unknown[][];
         expect(
            calls.filter((c) => c[0] === "[MCP Tool getThing] lookup failed"),
         ).toEqual([
            [
               "[MCP Tool getThing] lookup failed",
               { packageName: "pkg", error: "gone" },
            ],
         ]);
      } finally {
         warn.mockRestore();
      }
      const details = getNotFoundError("env/pkg");
      expect(result.isError).toBe(true);
      expect(result.content[0]).toMatchObject({
         resource: { uri: "malloy://environment/home#getThing" },
      });
      expect(
         JSON.parse(
            (result.content[0] as { resource: { text: string } }).resource.text,
         ),
      ).toEqual({
         error: details.message,
         suggestions: details.suggestions,
         things: [],
      });
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
