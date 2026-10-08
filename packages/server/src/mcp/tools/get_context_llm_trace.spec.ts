// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The stage trace on a request where both LLM stages run: what it records,
 * that it appears only with the header, and that the header changes no result.
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
import {
   keywordReply,
   scriptedChat,
   semanticHarness,
   untilSemantic,
   useChat,
   type SemanticHarness,
} from "../../test_helpers/get_context_llm_harness";
import { shopPackage } from "../../test_helpers/get_context_llm_fixture";

let h: SemanticHarness;
beforeAll(async () => {
   h = await semanticHarness();
});
afterAll(async () => {
   await h.close();
});
beforeEach(() => h.reset());
afterEach(() => _clearChatModelForTests());

const TRACE = {
   requestInfo: { headers: { "x-publisher-retrieval-trace": "summary" } },
};
const params = (pkg: string) => ({
   search_targets: [
      { target_type: "dimension", search_text: "state the order ships to" },
   ],
   scopes: [{ environment: "trace", package: pkg }],
});

describe("retrieval_trace with the LLM stages", () => {
   it("records each stage's counts, calls and tokens", async () => {
      useChat(scriptedChat(keywordReply).model);
      const handler = h.handlerFor(shopPackage());
      const { payload } = await untilSemantic(handler, params("a"), TRACE);
      const [refine, rerank] = payload.retrieval_trace.stages;
      expect(refine).toMatchObject({
         name: "refine",
         status: "ran",
         llm_calls: 1,
         // The scripted chat reports 11 input and 5 output tokens per call.
         tokens: { input: 11, output: 5 },
      });
      // 5 ranked rows went in (3 in orders, 1 each in shipments and customers) and all rate MEDIUM or better.
      expect(refine.in).toBe(5);
      expect(refine.out).toBe(5);
      expect(rerank).toMatchObject({
         name: "rerank",
         status: "ran",
         in: 3,
         llm_calls: 1,
      });
      // Rerank pruned the two sources it scored below 2.
      expect(rerank.out).toBe(1);
      expect(payload.sources).toHaveLength(1);
      expect(typeof refine.ms).toBe("number");
   });

   it("is absent without the header, and the header changes no result", async () => {
      useChat(scriptedChat(keywordReply).model);
      const plainHandler = h.handlerFor(shopPackage());
      const plain = await untilSemantic(plainHandler, params("b"));
      expect("retrieval_trace" in plain.payload).toBe(false);

      h.reset();
      useChat(scriptedChat(keywordReply).model);
      const tracedHandler = h.handlerFor(shopPackage());
      const traced = await untilSemantic(tracedHandler, params("b"), TRACE);
      const { retrieval_trace: trace, ...rest } = traced.payload;
      expect(trace.stages).toHaveLength(2);
      expect(JSON.stringify(rest)).toBe(JSON.stringify(plain.payload));
   });

   it("lists a skipped stage as skipped", async () => {
      useChat(scriptedChat(keywordReply).model);
      const handler = h.handlerFor(
         shopPackage({
            refine: { enabled: false, minLevel: "MEDIUM" },
         }),
      );
      const { payload } = await untilSemantic(handler, params("c"), TRACE);
      expect(payload.retrieval_trace.stages[0]).toMatchObject({
         name: "refine",
         status: "skipped",
         llm_calls: 0,
      });
      expect(payload.retrieval_trace.stages[1].status).toBe("ran");
   });
});
