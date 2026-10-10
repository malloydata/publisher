// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   afterAll,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import { createCatalogTables } from "../../storage/duckdb/schema";
import { compileFcjFixture } from "../../test_helpers/fcj_fixture";
import { fcjValueFetcher } from "../../test_helpers/fcj_index_fixture";
import {
   callPayload,
   scriptedChat,
   semanticHarness,
   untilSemantic,
   type SemanticHarness,
} from "../../test_helpers/get_context_llm_harness";
import {
   _clearChatModelForTests,
   _setChatModelForTests,
} from "../../providers/active";
import { _setFcjValueFetcherForTests } from "./engine_state";

let harness: SemanticHarness;
beforeAll(async () => {
   harness = await semanticHarness();
   await createCatalogTables(harness.db);
   _setFcjValueFetcherForTests(fcjValueFetcher);
});
afterAll(async () => {
   _setFcjValueFetcherForTests(undefined);
   await harness.close();
});
beforeEach(() => harness.reset());

const scopes = [{ environment: "env", package: "fixture" }];
const full = {
   requestInfo: { headers: { "x-publisher-retrieval-trace": "full" } },
};

describe("the fcj switch", () => {
   it("answers a ranked call from the fcj engine once its index is built", async () => {
      const { pkg } = await compileFcjFixture();
      const handler = harness.handlerFor(pkg);
      const first = await callPayload(handler, {
         search_targets: [
            { target_type: "measure", search_text: "number of flights" },
         ],
         scopes,
      });
      expect(first.payload.retrieval).toBe("indexing");
      const { payload } = await untilSemantic(
         handler,
         {
            search_targets: [
               { target_type: "measure", search_text: "number of flights" },
               { target_type: "dimension", search_text: "start date" },
            ],
            scopes,
         },
         full,
      );
      expect(payload.engine).toBe("fcj");
      expect(
         payload.retrieval_trace.stages.map((s: { name: string }) => s.name),
      ).toContain("find");
      const find = payload.retrieval_trace_full.fcj.find;
      expect(find.map((t: { target_index: number }) => t.target_index)).toEqual(
         [0, 1],
      );
      expect(find[0].candidates[0].id).toBe("measure:flights:flight_count");
      const ids = payload.sources.flatMap(
         (s: { entities?: Array<{ entity_id: string }> }) =>
            (s.entities ?? []).map((e) => e.entity_id),
      );
      expect(ids).toContain("measure:flights:flight_count");
   });

   it("leaves a listing on the shipped path", async () => {
      const { pkg } = await compileFcjFixture();
      const handler = harness.handlerFor(pkg);
      const { payload } = await callPayload(handler, {
         search_targets: [{ target_type: "source" }],
         scopes,
      });
      expect(payload.engine).toBeUndefined();
      // The shipped listing: one card per model file a source resolves in,
      // so `orders` (shop.malloy, imported by reports.malloy) is two of nine.
      expect(payload.sources.length).toBe(9);
   });

   it("returns what the judge picked, one call for one cluster", async () => {
      const chat = scriptedChat(() =>
         JSON.stringify({
            picks: [
               { target: 1, candidates: [1], reason: "counts flights" },
               { target: 2, candidates: [2] },
            ],
            caveats: ["start_date is the scheduled date"],
         }),
      );
      _setChatModelForTests(chat.model);
      try {
         const { pkg } = await compileFcjFixture();
         const handler = harness.handlerFor(pkg);
         const { payload } = await untilSemantic(
            handler,
            {
               search_targets: [
                  { target_type: "measure", search_text: "number of flights" },
                  { target_type: "dimension", search_text: "start date" },
               ],
               scopes,
            },
            full,
         );
         const judgeStage = payload.retrieval_trace.stages.find(
            (s: { name: string }) => s.name === "judge",
         );
         expect(judgeStage).toMatchObject({ status: "ran", llm_calls: 1 });
         const ids = payload.sources.flatMap(
            (s: { entities?: Array<{ entity_id: string }> }) =>
               (s.entities ?? []).map((e) => e.entity_id),
         );
         expect(ids).toEqual([
            "measure:flights:flight_count",
            "dimension:flights:start_date",
         ]);
         expect(payload.retrieval_trace_full.fcj.judge[0].caveats).toEqual([
            "start_date is the scheduled date",
         ]);
      } finally {
         _clearChatModelForTests();
      }
   });
});
