// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   afterAll,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
   setSystemTime,
} from "bun:test";
import { DEFAULT_EMBEDDING_MIN_SIMILARITY } from "../../config";
import {
   EmbeddingProvider,
   _setEmbeddingProviderForTests,
} from "../../service/embedding_provider";
import { createCatalogTables } from "../../storage/duckdb/schema";
import { compileFcjFixture } from "../../test_helpers/fcj_fixture";
import { fcjValueFetcher } from "../../test_helpers/fcj_index_fixture";
import {
   callPayload,
   embedText,
   scriptedChat,
   semanticHarness,
   untilSemantic,
   useChat,
   type SemanticHarness,
} from "../../test_helpers/get_context_llm_harness";
import {
   _clearChatModelForTests,
   _setChatModelForTests,
} from "../../providers/active";
import { ServiceUnavailableError } from "../../errors";
import { pollFor, pollUntil } from "../../test_helpers/poll";
import { PROVIDER_FAILURE_COOLDOWN_MS } from "../tools/retrieval_shared";
import { _setFcjDeadlineForTests } from "./engine";
import { _setFcjValueFetcherForTests, fcjStateOf } from "./engine_state";
import type { IndexValueFetcher } from "./values_index";
import type { Package } from "../../service/package";
import { heldResults } from "./pages";
import {
   DEFAULT_FCJ_SETTINGS,
   DEFAULT_PACKAGE_RETRIEVAL,
} from "../../service/package_retrieval";

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

describe("the search engine", () => {
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

   it("says lexical, not semantic, when no embedding provider is configured", async () => {
      _setEmbeddingProviderForTests(null);
      const { pkg } = await compileFcjFixture();
      const { payload } = await pollFor(
         () =>
            callPayload(harness.handlerFor(pkg), {
               search_targets: [
                  { target_type: "dimension", search_text: "city" },
               ],
               scopes,
            }),
         (out) => out.payload.retrieval !== "indexing",
         5_000,
      );
      expect(payload.engine).toBe("fcj");
      expect(payload.retrieval).toBe("lexical");
      expect(payload.returned).toBeGreaterThan(0);
   });

   it("answers a listing from the package's entity index, not from the engine", async () => {
      const { pkg } = await compileFcjFixture();
      const handler = harness.handlerFor(pkg);
      const { payload } = await callPayload(handler, {
         search_targets: [{ target_type: "source" }],
         scopes,
      });
      expect(payload.engine).toBeUndefined();
      // A listing: one card per model file a source resolves in,
      // so `orders` (shop.malloy, imported by reports.malloy) is two of nine.
      expect(payload.sources.length).toBe(9);
   });

   it("by default is in list mode, with the union of the meaning and keyword lists and the readings rule on", async () => {
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
      // One group of peers, not clusters: the judge saw every source listed.
      const connect = payload.retrieval_trace_full.fcj.connect;
      expect(connect.clusters).toHaveLength(1);
      expect(connect.clusters[0].peers.length).toBeGreaterThan(1);
      expect(DEFAULT_FCJ_SETTINGS).toMatchObject({
         connect: "list",
         listTopK: 30,
         listUnion: 30,
         dropRoutesAt: 4,
         readingsRule: true,
      });
   });

   it("returns what the judge picked, one call for one cluster (connect: clusters)", async () => {
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
         const { pkg } = await compileFcjFixture(undefined, undefined, {
            ...DEFAULT_PACKAGE_RETRIEVAL,
            fcj: { ...DEFAULT_FCJ_SETTINGS, connect: "clusters" },
         });
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

   it("carries the stale-package note on a ranked answer", async () => {
      const { pkg } = await compileFcjFixture();
      const handler = harness.handlerFor(
         pkg,
         new Map([
            [
               "fixture",
               {
                  message: "line 3: missing ')'",
                  failedAt: "2026-08-13T00:00:00.000Z",
               },
            ],
         ]),
      );
      const { payload } = await untilSemantic(handler, {
         search_targets: [
            { target_type: "measure", search_text: "number of flights" },
         ],
         scopes,
      });
      expect(payload.warnings.join(" ")).toContain("STALE");
      expect(payload.warnings.join(" ")).toContain("reload_package");
   });

   it("returns an error, never a keyword answer, when the embedding provider fails after the index is built", async () => {
      let down = false;
      _setEmbeddingProviderForTests(
         new EmbeddingProvider(
            {
               apiKey: "test",
               model: "stub-model",
               baseUrl: "https://stub.example.com/v1",
               minSimilarity: DEFAULT_EMBEDDING_MIN_SIMILARITY,
            },
            (async (_url: RequestInfo | URL, init?: RequestInit) => {
               if (down) return new Response("down", { status: 500 });
               const body = JSON.parse(String(init?.body)) as {
                  input: string[];
               };
               return new Response(
                  JSON.stringify({
                     data: body.input.map((text, index) => ({
                        index,
                        embedding: embedText(text),
                     })),
                  }),
                  { status: 200 },
               );
            }) as typeof fetch,
         ),
      );
      const { pkg } = await compileFcjFixture();
      const handler = harness.handlerFor(pkg);
      const request = {
         search_targets: [
            { target_type: "measure", search_text: "number of flights" },
         ],
         scopes,
      };
      await untilSemantic(handler, request);
      down = true;
      const { isError, payload } = await callPayload(handler, request);
      expect(isError).toBe(true);
      expect(payload.retrieval).toBe("error");
      expect(payload.retrieval_reason).toBe("provider-error");
      expect(payload.sources).toEqual([]);
   });

   it("stops the judge at maxCallsPerRequest, and sends no call past it", async () => {
      const chat = scriptedChat(() =>
         JSON.stringify({ picks: [], caveats: [] }),
      );
      useChat(chat.model, { maxCallsPerRequest: 1, concurrency: 1 });
      try {
         // Three measures in three unrelated sources: connect: clusters makes
         // three clusters, and the judge makes one call for each.
         const { pkg } = await compileFcjFixture(undefined, undefined, {
            ...DEFAULT_PACKAGE_RETRIEVAL,
            fcj: { ...DEFAULT_FCJ_SETTINGS, connect: "clusters" },
         });
         const handler = harness.handlerFor(pkg);
         const request = {
            search_targets: [
               { target_type: "measure", search_text: "sales amount" },
               { target_type: "measure", search_text: "number of races" },
               { target_type: "measure", search_text: "number of flights" },
            ],
            scopes,
         };
         // The build may have used the chat model; count from here.
         await pollUntilBuilt(handler, request);
         const before = chat.prompts.length;
         const { isError, payload } = await callPayload(handler, request);
         expect(isError).toBe(true);
         expect(payload.retrieval_reason).toBe("llm-stage-failed");
         expect(payload.retrieval_stage).toBe("judge");
         expect(payload.error).toContain("maxCallsPerRequest");
         expect(chat.prompts.length - before).toBeLessThanOrEqual(1);
      } finally {
         _clearChatModelForTests();
      }
   });

   it("fails the request, never skips the judge, when judgeLlm has no key", async () => {
      const chat = scriptedChat(() =>
         JSON.stringify({
            picks: [{ target: 1, candidates: [1] }],
            caveats: [],
         }),
      );
      _setChatModelForTests(chat.model);
      const saved = [process.env.JUDGE_LLM_API_KEY, process.env.LLM_API_KEY];
      delete process.env.JUDGE_LLM_API_KEY;
      delete process.env.LLM_API_KEY;
      try {
         const { pkg } = await compileFcjFixture(undefined, undefined, {
            ...DEFAULT_PACKAGE_RETRIEVAL,
            fcj: {
               ...DEFAULT_FCJ_SETTINGS,
               judgeLlm: { provider: "anthropic", model: "claude-haiku-5-5" },
            },
         });
         const handler = harness.handlerFor(pkg);
         const { payload } = await untilSemantic(handler, {
            search_targets: [
               { target_type: "measure", search_text: "number of flights" },
            ],
            scopes,
         });
         expect(payload.retrieval_reason).toBe("llm-stage-failed");
         expect(payload.retrieval_stage).toBe("judge");
         // The index build may have used the chat model; the judge never did.
         expect(chat.prompts.some((p) => p.includes("Target 1 ("))).toBe(false);
      } finally {
         if (saved[0] !== undefined) process.env.JUDGE_LLM_API_KEY = saved[0];
         if (saved[1] !== undefined) process.env.LLM_API_KEY = saved[1];
         _clearChatModelForTests();
      }
   });
});

describe("fcj request deadline", () => {
   it("answers every target as unresolved, naming a narrower scope, when the search runs out of time", async () => {
      const reply = JSON.stringify({
         picks: [{ target: 1, candidates: [1] }],
         caveats: [],
      });
      // The judge answers, but only after the deadline has passed.
      const chat = scriptedChat((prompt) =>
         prompt.includes("<targets>")
            ? new Promise((resolve) => setTimeout(() => resolve(reply), 1_000))
            : reply,
      );
      _setChatModelForTests(chat.model);
      try {
         const { pkg } = await compileFcjFixture();
         const handler = harness.handlerFor(pkg);
         const request = {
            search_targets: [
               { target_type: "measure", search_text: "number of flights" },
               { target_type: "dimensional_value", search_text: "Denver" },
            ],
            scopes,
         };
         await pollUntilBuilt(handler, request);
         _setFcjDeadlineForTests(200);
         const { payload } = await callPayload(handler, request);
         expect(payload.sources).toEqual([]);
         expect(payload.unresolved_targets).toEqual([
            {
               target_index: 0,
               target_type: "measure",
               search_text: "number of flights",
            },
            {
               target_index: 1,
               target_type: "dimensional_value",
               search_text: "Denver",
            },
         ]);
         expect(payload.warnings.at(-1)).toBe(
            "The search ran out of time after 0.2 seconds, so no target was answered. Search again with a narrower scope: name the source in scopes[].source, and for a value its field in scopes[].entity_name, or send fewer targets.",
         );
      } finally {
         _setFcjDeadlineForTests(undefined);
         // Let the abandoned judge call finish before the next test.
         await new Promise((resolve) => setTimeout(resolve, 1_100));
         _clearChatModelForTests();
      }
   });
});

/** Ask until the index is built, whatever the first answers say. */
async function pollUntilBuilt(
   handler: ReturnType<SemanticHarness["handlerFor"]>,
   request: Record<string, unknown>,
) {
   for (let i = 0; i < 100; i++) {
      const { payload } = await callPayload(handler, request);
      if (payload.retrieval !== "indexing") return;
      await new Promise((resolve) => setTimeout(resolve, 20));
   }
   throw new Error("the index was not built in time");
}

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

describe("splitBundles", () => {
   const listSplit = {
      ...DEFAULT_PACKAGE_RETRIEVAL,
      fcj: {
         ...DEFAULT_FCJ_SETTINGS,
         connect: "list" as const,
         splitBundles: true,
      },
   };
   const bundle = {
      search_targets: [
         {
            target_type: "dimension",
            search_text: "region name, airport name, customer email",
         },
      ],
      scopes,
   };
   /** The first option number listed under target `n` of a judge prompt. */
   const firstOptionOf = (prompt: string, n: number) => {
      const at = prompt.indexOf(`Target ${n} (`);
      const next = prompt.indexOf("Target ", at + 1);
      const m = /\[(\d+)\]/.exec(
         prompt.slice(at, next > at ? next : undefined),
      );
      return m ? Number(m[1]) : undefined;
   };

   it("shows the judge each item under the list target and folds the picks back into it", async () => {
      const chat = scriptedChat((prompt) =>
         JSON.stringify({
            picks: [
               { target: 1, candidates: [], reason: "the list" },
               { target: 2, candidates: [firstOptionOf(prompt, 2)] },
               { target: 3, candidates: [firstOptionOf(prompt, 3)] },
               { target: 4, candidates: [] },
            ],
            caveats: ["No customer email field exists."],
         }),
      );
      _setChatModelForTests(chat.model);
      try {
         const { pkg } = await compileFcjFixture(
            undefined,
            undefined,
            listSplit,
         );
         const { payload } = await untilSemantic(
            harness.handlerFor(pkg),
            bundle,
            full,
         );
         const prompt = chat.prompts.at(-1)!;
         expect(prompt).toContain(
            'Target 2 (dimension; one item of the list in target 1): "region name"',
         );
         expect(prompt).toContain(
            'Target 4 (dimension; one item of the list in target 1): "customer email"',
         );
         const split = payload.retrieval_trace_full.fcj.split;
         expect(split[0].items.map((i: { text: string }) => i.text)).toEqual([
            "region name",
            "airport name",
            "customer email",
         ]);
         // One pick, under the caller's target, holding both items' fields.
         const picks = payload.retrieval_trace_full.fcj.judge[0].picks;
         expect(
            picks.map((p: { target_index: number }) => p.target_index),
         ).toEqual([0]);
         expect(picks[0].ids).toHaveLength(2);
         expect(picks[0].reason).toContain(
            'Not found by this search: "customer email"',
         );
         expect(payload.unresolved_targets).toEqual([]);
         // An absence caveat names no field for "also likely" to add.
         const named = payload.retrieval_trace_full.fcj.also_likely.filter(
            (a: { rule: string }) => a.rule === "named",
         );
         expect(named).toEqual([]);
      } finally {
         _clearChatModelForTests();
      }
   });

   it("leaves a phrase with no list alone, and is off by default", async () => {
      const { pkg } = await compileFcjFixture(undefined, undefined, listSplit);
      const { payload } = await untilSemantic(
         harness.handlerFor(pkg),
         {
            search_targets: [
               { target_type: "dimension", search_text: "region name" },
            ],
            scopes,
         },
         full,
      );
      expect(payload.retrieval_trace_full.fcj.split).toBeUndefined();
      const { pkg: off } = await compileFcjFixture(undefined, undefined, {
         ...listSplit,
         fcj: { ...listSplit.fcj, splitBundles: false },
      });
      const { payload: p2 } = await untilSemantic(
         harness.handlerFor(off),
         bundle,
         full,
      );
      expect(p2.retrieval_trace_full.fcj.split).toBeUndefined();
      expect(p2.retrieval_trace_full.fcj.find).toHaveLength(1);
   });
});

describe("a value read the server refused", () => {
   const CITY = "dimension:cities@travel.malloy:city";
   /** The fixture's Package is typed unknown; the state is keyed by it. */
   const stateOf = (pkg: unknown) => fcjStateOf(pkg as Package);
   const ask = {
      search_targets: [
         { target_type: "measure", search_text: "number of flights" },
      ],
      scopes,
   };
   /** A fetcher whose reads of `cities.city` fail `failures` times, at the query cap. */
   const refusing = (failures: number) => {
      const reads = { city: 0 };
      const fetcher: IndexValueFetcher = async (req) => {
         if (req.source === "cities") {
            reads.city += 1;
            if (reads.city <= failures) {
               throw new ServiceUnavailableError(
                  "Publisher pod is at its maximum concurrent query cap (1).",
               );
            }
         }
         return fcjValueFetcher(req);
      };
      return { reads, fetcher };
   };
   const cityValues = (pkg: unknown) => {
      const state = stateOf(pkg);
      if (state?.status !== "ready") return undefined;
      return state.index.values.values
         .filter((v) => v.dimension === CITY)
         .map((v) => v.value);
   };
   /** Jump past the cool-down, ask once, and wait for the rebuild to land. */
   const askAfterCooldown = async (
      handler: ReturnType<SemanticHarness["handlerFor"]>,
      pkg: unknown,
   ) => {
      setSystemTime(new Date(Date.now() + PROVIDER_FAILURE_COOLDOWN_MS + 1));
      let served;
      try {
         served = await callPayload(handler, ask);
      } finally {
         setSystemTime();
      }
      const landed = await pollUntil(() => {
         const state = stateOf(pkg);
         return state?.status === "ready" && !state.rebuilding;
      }, 5_000);
      expect(landed).toBe(true);
      return served;
   };

   afterAll(() => {
      setSystemTime();
      _setFcjValueFetcherForTests(fcjValueFetcher);
   });

   it("serves without the values, then reads them again after the cool-down when the first read failed", async () => {
      const { reads, fetcher } = refusing(1);
      _setFcjValueFetcherForTests(fetcher);
      const { pkg } = await compileFcjFixture();
      const handler = harness.handlerFor(pkg);
      await untilSemantic(handler, ask);

      const built = stateOf(pkg);
      expect(built?.status).toBe("ready");
      if (built?.status !== "ready") return;
      expect(built.index.report.values).toMatchObject({
         failed: [
            {
               dimension: CITY,
               reason:
                  "the read failed: Publisher pod is at its maximum concurrent query cap (1).",
            },
         ],
         skipped: [],
      });
      expect(cityValues(pkg)).toEqual([]);
      expect(built.rebuildAt).toBeGreaterThan(Date.now());

      // Inside the cool-down nothing is read again.
      await callPayload(handler, ask);
      expect(reads.city).toBe(1);

      // After it, the old index answers while the rebuild reads the values.
      const served = await askAfterCooldown(handler, pkg);
      expect(served.payload.retrieval).toBe("semantic");
      expect(reads.city).toBe(2);
      expect(cityValues(pkg)).toEqual(["Denver", "Boulder"]);
      const rebuilt = stateOf(pkg);
      expect(rebuilt?.status === "ready" && rebuilt.rebuildAt).toBeUndefined();
      expect(
         rebuilt?.status === "ready" && rebuilt.index.report.values,
      ).toMatchObject({
         failed: [],
      });
   });

   it("keeps serving and reads again at most once per cool-down when every read fails", async () => {
      const { reads, fetcher } = refusing(Number.POSITIVE_INFINITY);
      _setFcjValueFetcherForTests(fetcher);
      const { pkg } = await compileFcjFixture();
      const handler = harness.handlerFor(pkg);
      await untilSemantic(handler, ask);
      expect(reads.city).toBe(1);

      for (const round of [2, 3]) {
         const served = await askAfterCooldown(handler, pkg);
         expect(served.payload.retrieval).toBe("semantic");
         expect(reads.city).toBe(round);
         const state = stateOf(pkg);
         expect(state?.status).toBe("ready");
         if (state?.status !== "ready") return;
         expect(state.rebuildAt).toBeGreaterThan(Date.now());
         expect(cityValues(pkg)).toEqual([]);
         // A second question inside the new cool-down reads nothing.
         await callPayload(handler, ask);
         expect(reads.city).toBe(round);
      }
   });

   it("does not read an empty dimension again: no values is an answer, not a failure", async () => {
      let reads = 0;
      _setFcjValueFetcherForTests(async (req) => {
         if (req.source === "cities") {
            reads += 1;
            return { values: [], truncated: false };
         }
         return fcjValueFetcher(req);
      });
      const { pkg } = await compileFcjFixture();
      const handler = harness.handlerFor(pkg);
      await untilSemantic(handler, ask);
      const state = stateOf(pkg);
      expect(state?.status === "ready" && state.rebuildAt).toBeUndefined();
      setSystemTime(new Date(Date.now() + PROVIDER_FAILURE_COOLDOWN_MS + 1));
      try {
         await callPayload(handler, ask);
      } finally {
         setSystemTime();
      }
      expect(stateOf(pkg)?.status).toBe("ready");
      expect(reads).toBe(1);
   });
});
