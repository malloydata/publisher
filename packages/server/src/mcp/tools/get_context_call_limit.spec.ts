// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * retrieval.llm.maxCallsPerRequest: the most chat calls one get_context
 * request may make, counted across refine and rerank. The check runs before
 * the call that would pass it.
 */

import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import { _clearChatModelForTests } from "../../providers/active";
import { shopPackage } from "../../test_helpers/get_context_llm_fixture";
import {
   keywordReply,
   scriptedChat,
   semanticHarness,
   untilSemantic,
   useChat,
   type SemanticHarness,
} from "../../test_helpers/get_context_llm_harness";
import { LlmCallLimitError, LlmMeter } from "./get_context_llm";

describe("LlmMeter", () => {
   it("sends the calls up to the limit and refuses the next one before it is sent", async () => {
      const chat = scriptedChat(() => "[]");
      const metered = new LlmMeter(2).wrap(chat.model);
      const ask = () =>
         metered.completeJson({ prompt: "x", validate: (v) => v });
      await ask();
      await ask();
      await expect(ask()).rejects.toBeInstanceOf(LlmCallLimitError);
      expect(chat.prompts).toHaveLength(2);
   });

   it("refuses calls started together once the limit is taken", async () => {
      const chat = scriptedChat(() => "[]", { delayMs: 10 });
      const metered = new LlmMeter(3).wrap(chat.model);
      const results = await Promise.allSettled(
         Array.from({ length: 6 }, () =>
            metered.completeJson({ prompt: "x", validate: (v) => v }),
         ),
      );
      expect(results.filter((r) => r.status === "fulfilled")).toHaveLength(3);
      expect(chat.prompts).toHaveLength(3);
   });

   it("counts complete() too, and has no ceiling with a null limit", async () => {
      const chat = scriptedChat(() => "[]");
      const limited = new LlmMeter(1).wrap(chat.model);
      await limited.complete({ prompt: "x" });
      await expect(limited.complete({ prompt: "x" })).rejects.toBeInstanceOf(
         LlmCallLimitError,
      );
      const free = new LlmMeter(null);
      const unlimited = free.wrap(scriptedChat(() => "[]").model);
      for (let i = 0; i < 50; i++) await unlimited.complete({ prompt: "x" });
      expect(free.snapshot().calls).toBe(50);
   });
});

describe("get_context with maxCallsPerRequest", () => {
   let h: SemanticHarness;
   beforeAll(async () => {
      h = await semanticHarness();
   });
   afterAll(async () => {
      await h.close();
   });
   beforeEach(() => h.reset());
   afterEach(() => _clearChatModelForTests());

   const params = (pkg: string) => ({
      search_targets: [
         { target_type: "dimension", search_text: "state the order ships to" },
      ],
      scopes: [{ environment: "limit", package: pkg }],
   });

   it("stops rerank, which needs the second call, and names the setting", async () => {
      const chat = scriptedChat(keywordReply);
      useChat(chat.model, { maxCallsPerRequest: 1 });
      const handler = h.handlerFor(shopPackage());
      const { isError, payload } = await untilSemantic(handler, params("a"));
      expect(isError).toBe(true);
      expect(payload.retrieval_stage).toBe("rerank");
      expect(payload.error).toContain("failed in the rerank step");
      expect(payload.error).toContain("retrieval.llm.maxCallsPerRequest");
      expect(payload.error).toContain("1 LLM call");
      expect(payload.sources).toEqual([]);
      // Refine used the one call; the second was never sent.
      expect(chat.prompts).toHaveLength(1);
   });

   it("answers when the limit covers every call", async () => {
      const chat = scriptedChat(keywordReply);
      useChat(chat.model, { maxCallsPerRequest: 2 });
      const handler = h.handlerFor(shopPackage());
      const { isError, payload } = await untilSemantic(handler, params("b"));
      expect(isError).toBe(false);
      expect(payload.sources.length).toBeGreaterThan(0);
      expect(chat.prompts).toHaveLength(2);
   });

   it("a limit of 1 is enough when only one stage runs", async () => {
      const chat = scriptedChat(keywordReply);
      useChat(chat.model, { maxCallsPerRequest: 1 });
      const handler = h.handlerFor(
         shopPackage({ rerank: { enabled: false, topSources: 8 } }),
      );
      const { isError } = await untilSemantic(handler, params("c"));
      expect(isError).toBe(false);
      expect(chat.prompts).toHaveLength(1);
   });

   it("is counted per request: the next request starts again from zero", async () => {
      const chat = scriptedChat(keywordReply);
      useChat(chat.model, { maxCallsPerRequest: 2 });
      const handler = h.handlerFor(shopPackage());
      const first = await untilSemantic(handler, params("d"));
      const second = await untilSemantic(handler, params("d"));
      expect(first.isError).toBe(false);
      expect(second.isError).toBe(false);
      expect(chat.prompts).toHaveLength(4);
   });

   it("stops refine part way when its batches alone pass the limit", async () => {
      // 13 sources of 10 fields = 130 candidates, 8 batches of 15.
      const fields = (s: number) =>
         Array.from({ length: 10 }, (_, i) => ({
            kind: "dimension",
            name: `state_${s}_${i}`,
            annotations: ["#(doc) State the order ships to."],
         }));
      const model = {
         getSourceInfos: () =>
            Array.from({ length: 13 }, (_, s) => ({
               name: `src${s}`,
               annotations: [],
               schema: { fields: fields(s) },
            })),
         getQueries: () => [],
      };
      const pkg = {
         listModels: async () => [{ path: "big.malloy" }],
         getModel: () => model,
         getRetrievalSettings: () => ({
            representation: "single",
            keyphrases: "never",
            sourceSummary: { enabled: false },
            prompts: {},
         }),
      };
      const chat = scriptedChat(keywordReply);
      useChat(chat.model, { maxCallsPerRequest: 3, concurrency: 1 });
      const handler = h.handlerFor(pkg);
      const { isError, payload } = await untilSemantic(handler, params("e"));
      expect(isError).toBe(true);
      expect(payload.retrieval_stage).toBe("refine");
      expect(payload.error).toContain("maxCallsPerRequest");
      expect(chat.prompts).toHaveLength(3);
   });
});
