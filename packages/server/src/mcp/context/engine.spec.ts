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
import { heldResults } from "./pages";

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

/** Fifteen targets over every island of the fixture. */
const FIFTEEN = [
   ["measure", "number of flights"],
   ["dimension", "start date"],
   ["dimension", "region name"],
   ["dimension", "airport name"],
   ["dimension", "city name"],
   ["measure", "sales amount"],
   ["measure", "net sales"],
   ["measure", "number of races"],
   ["dimension", "order is fulfilled"],
   ["dimension", "reseller order"],
   ["view", "fulfilled sales"],
   ["measure", "number of cities"],
   ["dimension", "arrival airport"],
   ["dimension", "customer email"],
   ["source", "scheduled flights"],
].map(([target_type, search_text]) => ({ target_type, search_text }));

const summary = {
   requestInfo: { headers: { "x-publisher-retrieval-trace": "summary" } },
};
type Payload = {
   sources: Array<{ entities?: Array<{ entity_id: string }> }>;
   next_offset?: number;
   total_available: number;
   returned: number;
   warnings?: string[];
   retrieval_trace: { stages: Array<{ name: string }> };
   error?: string;
};
const stageNames = (p: Payload) => p.retrieval_trace.stages.map((s) => s.name);
const entityIds = (p: Payload) =>
   p.sources.flatMap((s) => (s.entities ?? []).map((e) => e.entity_id));

describe("fcj paging", () => {
   /** A ready handler and the complete result, all on one page. */
   async function ready() {
      heldResults.clear();
      const { pkg } = await compileFcjFixture();
      const handler = harness.handlerFor(pkg);
      const call = async (extra: Record<string, unknown>) =>
         (
            await callPayload(
               handler,
               { search_targets: FIFTEEN, scopes, ...extra },
               summary,
            )
         ).payload as Payload;
      const complete = (
         await untilSemantic(
            handler,
            { search_targets: FIFTEEN, scopes, limit: 150 },
            summary,
         )
      ).payload as Payload;
      expect(complete.next_offset).toBeUndefined();
      // Nothing to come back for, so nothing is held.
      expect(heldResults.size).toBe(0);
      return { call, complete, half: Math.ceil(complete.sources.length / 2) };
   }

   it("pages a fifteen-target result: page 1 and page 2 together are the complete result", async () => {
      const { call, complete, half } = await ready();
      expect(complete.sources.length).toBeGreaterThan(2);
      const one = await call({ limit: half });
      expect(one.returned).toBe(half);
      expect(one.total_available).toBe(complete.sources.length);
      expect(one.next_offset).toBe(half);
      expect(one.warnings!.join(" ")).toContain(
         `offset: ${half} (next_offset)`,
      );
      const two = await call({ limit: half, offset: one.next_offset });
      expect(two.next_offset).toBeUndefined();
      expect([...one.sources, ...two.sources]).toEqual(complete.sources);
      expect([...entityIds(one), ...entityIds(two)]).toEqual(
         entityIds(complete),
      );
   });

   it("serves a later page from the held result without running a stage", async () => {
      const { call, half } = await ready();
      const one = await call({ limit: half });
      expect(stageNames(one)).toEqual([
         "find",
         "connect",
         "judge",
         "respond",
         "page",
      ]);
      const two = await call({ limit: half, offset: one.next_offset });
      expect(stageNames(two)).toEqual(["page"]);
      expect((two.warnings ?? []).join(" ")).not.toContain("ran again");
   });

   it("runs the search again, and says so, when the held result is gone", async () => {
      const { call, complete, half } = await ready();
      const one = await call({ limit: half });
      heldResults.clear();
      const two = await call({ limit: half, offset: one.next_offset });
      expect(stageNames(two)).toContain("judge");
      expect(two.warnings!.join(" ")).toContain(
         "No held result matched this offset, so the search ran again",
      );
      // With no judge the run is deterministic, so the page still lines up.
      expect([...one.sources, ...two.sources]).toEqual(complete.sources);
   });

   it("never serves one caller's held result to a request with other givens", async () => {
      const { call, half } = await ready();
      const one = await call({ limit: half, givens: { region: "west" } });
      const other = await call({
         limit: half,
         offset: one.next_offset,
         givens: { region: "east" },
      });
      expect(stageNames(other)).toContain("find");
      expect(other.warnings!.join(" ")).toContain("ran again");
      const none = await call({ limit: half, offset: one.next_offset });
      expect(stageNames(none)).toContain("find");
      const same = await call({
         limit: half,
         offset: one.next_offset,
         givens: { region: "west" },
      });
      expect(stageNames(same)).toEqual(["page"]);
   });

   it("still refuses an offset on a listing that is not a pure source listing", async () => {
      await ready();
      const { pkg } = await compileFcjFixture();
      const { payload } = await callPayload(harness.handlerFor(pkg), {
         search_targets: [{ target_type: "dimension" }],
         scopes,
         offset: 3,
      });
      expect(payload.error).toContain("Invalid offset");
   });
});
