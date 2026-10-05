// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { logger } from "../../logger";
import { _resetEmbeddingIndexStateForTests } from "./embedding_index";
import { LlmMeter } from "./get_context_llm";
import {
   runCardStages,
   runQueryStages,
   runRankStages,
   type CardStage,
   type PipelineContext,
   type QueryStage,
   type RankStage,
   type RankedState,
} from "./get_context_pipeline";
import { registerGetContextTool } from "./get_context_tool";
import { _setEmbeddingProviderForTests } from "../../service/embedding_provider";
import type { EnvironmentStore } from "../../service/environment_store";
import type { ChatModel } from "../../providers/types";

function ctxWithTrace(): PipelineContext {
   return { trace: [], meter: new LlmMeter() } as unknown as PipelineContext;
}

const rankedState = (n: number): RankedState => ({
   retrieval: "semantic",
   belowCutoffCount: 0,
   rows: Array.from({ length: n }, (_, i) => ({
      kind: "dimension",
      name: `d${i}`,
      source: "s",
      environmentName: "e",
      packageName: "p",
      modelPath: "m.malloy",
      doc: "",
   })),
});

describe("stage runner trace", () => {
   it("records a ran stage with its row counts and the LLM calls it made", async () => {
      const ctx = ctxWithTrace();
      const chat: ChatModel = ctx.meter!.wrap({
         provider: "openai-compatible",
         model: "m",
         complete: async () => ({ text: "", usage: {} }),
         // A model reports each HTTP request it sends; this double sends one
         // per call.
         completeJson: async (req) => {
            req.onRequest?.();
            return {
               value: req.validate([]),
               usage: { inputTokens: 7, outputTokens: 3 },
            };
         },
      });
      const stage: RankStage = {
         name: "halve",
         enabled: () => true,
         run: async (state) => {
            await chat.completeJson({ prompt: "x", validate: (v) => v });
            await chat.completeJson({ prompt: "y", validate: (v) => v });
            return { ...state, rows: state.rows.slice(0, 2) };
         },
      };
      await runRankStages([stage], rankedState(5), ctx);
      expect(ctx.trace).toHaveLength(1);
      expect(ctx.trace![0]).toMatchObject({
         name: "halve",
         status: "ran",
         in: 5,
         out: 2,
         llmCalls: 2,
         tokens: { input: 14, output: 6 },
      });
      expect(ctx.trace![0].ms).toBeGreaterThanOrEqual(0);
   });

   it("records a disabled stage as skipped with no calls", async () => {
      const ctx = ctxWithTrace();
      const stage: CardStage = {
         name: "off",
         enabled: () => false,
         run: async () => {
            throw new Error("must not run");
         },
      };
      await runCardStages(
         [stage],
         { retrieval: "semantic", belowCutoffCount: 0, cards: [] },
         ctx,
      );
      expect(ctx.trace).toEqual([
         {
            name: "off",
            status: "skipped",
            ms: 0,
            in: 0,
            out: 0,
            llmCalls: 0,
            tokens: { input: 0, output: 0 },
         },
      ]);
   });

   it("records a failing stage as failed and rethrows", async () => {
      const ctx = ctxWithTrace();
      const stage: QueryStage = {
         name: "boom",
         enabled: () => true,
         run: async () => {
            throw new Error("down");
         },
      };
      await expect(
         runQueryStages([stage], { searches: [{}, {}] } as never, ctx),
      ).rejects.toThrow("down");
      expect(ctx.trace![0]).toMatchObject({
         name: "boom",
         status: "failed",
         in: 2,
         out: 2,
      });
   });

   it("passes the state to enabled so a stage can read which retriever ranked", async () => {
      const ctx = ctxWithTrace();
      const stage: RankStage = {
         name: "semantic-only",
         enabled: (_ctx, state) => state.retrieval === "semantic",
         run: async (state) => state,
      };
      await runRankStages(
         [stage],
         { ...rankedState(1), retrieval: "lexical" },
         ctx,
      );
      expect(ctx.trace![0].status).toBe("skipped");
   });
});

// ---------------------------------------------------------------------------
// The header
// ---------------------------------------------------------------------------

type Handler = (
   params: Record<string, unknown>,
   extra?: unknown,
) => Promise<{ content: Array<{ resource?: { text: string } }> }>;

function handlerFor(): Handler {
   let handler: Handler | undefined;
   const pkg = {
      listModels: async () => [{ path: "m.malloy" }],
      getModel: () => ({
         getSourceInfos: () => [
            {
               name: "orders",
               annotations: ["#(doc) Orders."],
               schema: {
                  fields: [
                     {
                        kind: "dimension",
                        name: "state",
                        annotations: ["#(doc) State."],
                     },
                  ],
               },
            },
         ],
         getQueries: () => [],
      }),
   };
   const store = {
      getEnvironment: async () => ({
         getPackage: async () => pkg,
         getStaleCompileErrors: () => new Map(),
      }),
   } as unknown as EnvironmentStore;
   registerGetContextTool(
      {
         tool: (name: string, _d: string, _s: unknown, h: Handler) => {
            if (name === "get_context") handler = h;
         },
      } as never,
      store,
   );
   return handler as Handler;
}

const params = {
   search_targets: [{ target_type: "dimension", search_text: "state" }],
   scopes: [{ environment: "trace", package: "pkg" }],
};

async function run(extra?: unknown) {
   const result = await handlerFor()(params, extra);
   return JSON.parse(result.content[0].resource!.text);
}

describe("X-Publisher-Retrieval-Trace header", () => {
   beforeEach(() => {
      _setEmbeddingProviderForTests(null);
      _resetEmbeddingIndexStateForTests();
   });

   it("adds retrieval_trace when the header is `summary`", async () => {
      const payload = await run({
         requestInfo: {
            headers: { "x-publisher-retrieval-trace": "summary" },
         },
      });
      expect(payload.retrieval_trace).toEqual({ stages: [] });
   });

   it("leaves the payload byte-identical without the header", async () => {
      const plain = await run();
      const other = await run({ requestInfo: { headers: {} } });
      expect("retrieval_trace" in plain).toBe(false);
      expect(JSON.stringify(other)).toBe(JSON.stringify(plain));
   });

   it("changes nothing else when the header is present", async () => {
      const plain = await run();
      const traced = await run({
         requestInfo: {
            headers: { "x-publisher-retrieval-trace": "summary" },
         },
      });
      const { retrieval_trace: _t, ...rest } = traced;
      expect(JSON.stringify(rest)).toBe(JSON.stringify(plain));
   });

   it("ignores any other header value", async () => {
      const payload = await run({
         requestInfo: { headers: { "x-publisher-retrieval-trace": "full" } },
      });
      expect("retrieval_trace" in payload).toBe(false);
   });
});

describe("PUBLISHER_MCP_TRACE=retrieval", () => {
   // The eval loop's serve.py sets this variable for a local run. It writes the
   // trace to the server log and never adds it to a response, so the agent that
   // answers the questions sees the same results as without it.
   const saved = process.env.PUBLISHER_MCP_TRACE;
   beforeEach(() => {
      _setEmbeddingProviderForTests(null);
      _resetEmbeddingIndexStateForTests();
   });
   afterEach(() => {
      if (saved === undefined) delete process.env.PUBLISHER_MCP_TRACE;
      else process.env.PUBLISHER_MCP_TRACE = saved;
   });

   const traceLines = (info: ReturnType<typeof spyOn>) =>
      info.mock.calls.filter(
         (c: unknown[]) => c[0] === "[MCP Tool getContext] Retrieval trace",
      );

   it("logs the stage trace for each ranked call", async () => {
      process.env.PUBLISHER_MCP_TRACE = "retrieval";
      const info = spyOn(logger, "info");
      try {
         await run();
         const lines = traceLines(info);
         expect(lines).toHaveLength(1);
         expect(lines[0][1]).toEqual({
            environmentName: "trace",
            packageName: "pkg",
            retrieval_trace: { stages: [] },
         });
      } finally {
         info.mockRestore();
      }
   });

   it("does not change the response", async () => {
      const plain = await run();
      process.env.PUBLISHER_MCP_TRACE = "retrieval";
      const traced = await run();
      expect("retrieval_trace" in traced).toBe(false);
      expect(JSON.stringify(traced)).toBe(JSON.stringify(plain));
   });

   it("is off when the variable is unset or has another value", async () => {
      const info = spyOn(logger, "info");
      try {
         delete process.env.PUBLISHER_MCP_TRACE;
         await run();
         process.env.PUBLISHER_MCP_TRACE = "all";
         await run();
         expect(traceLines(info)).toHaveLength(0);
      } finally {
         info.mockRestore();
      }
   });

   it("with the header too: the log line and the response block", async () => {
      process.env.PUBLISHER_MCP_TRACE = "retrieval";
      const info = spyOn(logger, "info");
      try {
         const payload = await run({
            requestInfo: {
               headers: { "x-publisher-retrieval-trace": "summary" },
            },
         });
         expect(payload.retrieval_trace).toEqual({ stages: [] });
         expect(traceLines(info)).toHaveLength(1);
      } finally {
         info.mockRestore();
      }
   });
});
