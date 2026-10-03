// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The failure policy: an LLM stage that fails after the provider layer's
 * retries makes get_context return an error result that names the stage. It
 * never answers with an unrefined or lexical ranking.
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
   callPayload,
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

const params = (name: string) => ({
   search_targets: [
      { target_type: "dimension", search_text: "state the order ships to" },
   ],
   scopes: [{ environment: "fail", package: name }],
});
const TRACE = {
   requestInfo: { headers: { "x-publisher-retrieval-trace": "summary" } },
};

describe("refine failure", () => {
   it("returns an error that names refine, not a lexical or unrefined answer", async () => {
      useChat(
         scriptedChat(() => {
            throw new Error("upstream exploded");
         }).model,
      );
      const handler = h.handlerFor(shopPackage());
      const { isError, payload } = await untilSemantic(
         handler,
         params("refine-down"),
      );
      expect(isError).toBe(true);
      expect(payload.sources).toEqual([]);
      expect(payload.retrieval).toBe("error");
      expect(payload.retrieval_reason).toBe("llm-stage-failed");
      expect(payload.retrieval_stage).toBe("refine");
      expect(payload.error).toContain("failed in the refine step");
      expect(payload.error).toContain("upstream exploded");
      expect(payload.suggestions.join(" ")).toContain("LLM_API_KEY");
      // No ranking of any kind leaks into the error.
      expect("total_available" in payload).toBe(false);
   });

   it("fails after one repair when the model keeps returning invalid JSON", async () => {
      const chat = scriptedChat(() => "not json at all");
      useChat(chat.model);
      const handler = h.handlerFor(shopPackage());
      const { isError, payload } = await untilSemantic(
         handler,
         params("refine-badjson"),
      );
      expect(isError).toBe(true);
      expect(payload.error).toContain("refine");
      expect(payload.error).toContain("did not return usable JSON");
      // First ask plus one repair, then the stage gave up.
      expect(chat.prompts).toHaveLength(2);
   });

   it("records the failed stage in the trace when the header is sent", async () => {
      useChat(
         scriptedChat(() => {
            throw new Error("boom");
         }).model,
      );
      const handler = h.handlerFor(shopPackage());
      const { payload } = await untilSemantic(
         handler,
         params("refine-trace"),
         TRACE,
      );
      expect(payload.retrieval_trace.stages).toHaveLength(1);
      expect(payload.retrieval_trace.stages[0]).toMatchObject({
         name: "refine",
         status: "failed",
         llm_calls: 1,
      });
   });

   it("a listing runs no LLM stage, so a failing model does not matter", async () => {
      const chat = scriptedChat(() => {
         throw new Error("boom");
      });
      useChat(chat.model);
      const handler = h.handlerFor(shopPackage());
      const { isError, payload } = await callPayload(handler, {
         search_targets: [{ target_type: "source" }],
         scopes: [{ environment: "fail", package: "listing" }],
      });
      expect(isError).toBe(false);
      expect(payload.sources.length).toBeGreaterThan(0);
      expect(chat.prompts).toHaveLength(0);
   });
});

describe("the advice in a stage failure", () => {
   const down = () =>
      scriptedChat(() => {
         throw new Error("upstream exploded");
      }).model;
   const sourceTarget = {
      search_targets: [{ target_type: "source", search_text: "orders" }],
      scopes: [{ environment: "fail", package: "advice" }],
   };

   // Each stage, made the first one to fail, and the setting that turns it off.
   const cases: Array<{
      stage: string;
      setting: string;
      retrieval: Parameters<typeof shopPackage>[0];
      request: ReturnType<typeof params> | typeof sourceTarget;
   }> = [
      {
         stage: "refine",
         setting: "retrieval.refine",
         retrieval: {},
         request: params("advice"),
      },
      {
         stage: "rerank",
         setting: "retrieval.rerank",
         retrieval: {
            refine: { enabled: false, minLevel: "MEDIUM" },
            sourceMatch: { enabled: false },
         },
         request: params("advice"),
      },
      {
         stage: "source_match",
         setting: "retrieval.sourceMatch",
         retrieval: {},
         request: sourceTarget,
      },
   ];

   for (const { stage, setting, retrieval, request } of cases) {
      it(`for ${stage} names ${setting} and every other LLM step setting`, async () => {
         useChat(down());
         const handler = h.handlerFor(shopPackage(retrieval));
         const { isError, payload } = await untilSemantic(handler, request);
         expect(isError).toBe(true);
         expect(payload.retrieval_stage).toBe(stage);
         const advice = payload.suggestions.join(" ");
         // The failing step's own setting, then all three for turning every step off.
         expect(advice).toContain(
            `set the package's ${setting} to enabled: false`,
         );
         for (const key of [
            "retrieval.refine",
            "retrieval.rerank",
            "retrieval.sourceMatch",
         ]) {
            expect(advice).toContain(key);
         }
      });
   }
});
