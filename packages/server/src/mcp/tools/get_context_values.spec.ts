// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Value search: a `dimensional_value` target returns the dimension and the
 * strings it stores. Three layers.
 *
 * 1. The matcher, pure: the scoring table is pinned number by number.
 * 2. The stage over a stand-in warehouse (an injected fetcher): payload shape,
 *    scopes, caps, timeouts, the cache, the egress rule and the warnings, each
 *    asserted at its exact text.
 * 3. The stage over a REAL Malloy model on DuckDB, with the real fetcher: this
 *    is where gates are checked, because the gate is enforced by the query path
 *    and a stand-in would only test itself.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { type Connection } from "@malloydata/malloy";
import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { AccessDeniedError, QueryTimeoutError } from "../../errors";
import {
   parseRetrievalConfig,
   setRetrievalConfig,
} from "../../retrieval_config";
import {
   _clearEmbeddingProviderForTests,
   _setEmbeddingProviderForTests,
} from "../../service/embedding_provider";
import type { EnvironmentStore } from "../../service/environment_store";
import { Model } from "../../service/model";
import type { EmbeddingModel } from "../../providers/types";
import { _resetEmbeddingIndexStateForTests } from "./embedding_index";
import {
   _resetValueCacheForTests,
   _setValueFetcherForTests,
   _valueCacheCounters,
   type ValueFetchRequest,
} from "./get_context_values";
import {
   editDistance,
   filterStem,
   matchValues,
   normalizeText,
   scoreValue,
} from "./get_context_value_match";
import { registerGetContextTool } from "./get_context_tool";

// ---------------------------------------------------------------------------
// 1. The matcher
// ---------------------------------------------------------------------------

describe("value matching", () => {
   const STORED = "Auto Intenders (In-Market)";

   it("scores each kind of match at its published number", () => {
      const score = (q: string) => scoreValue(q, STORED);
      expect(score("Auto Intenders (In-Market)")).toBe(1);
      // Same after case, punctuation and spacing are normalized.
      expect(score("auto intenders in market")).toBe(0.98);
      expect(score("AUTO  INTENDERS")).toBe(0.9167);
      // Prefix of a word ("intender" of "intenders").
      expect(score("auto intender")).toBe(0.8629);
      // One edit, and a swap of two letters counted as one edit.
      expect(score("auto intendrs")).toBe(0.7948);
      expect(score("auto intedners")).toBe(0.8021);
      // Part of the value.
      expect(score("in-market")).toBe(0.875);
      expect(score("market")).toBe(0.85);
      // No word in common.
      expect(score("home shoppers")).toBe(0);
   });

   it("does not match a value on one word of a two-word phrase", () => {
      // Found live: "region 3" returned every "Region N" at 0.5 each.
      expect(scoreValue("region 3", "Region 0")).toBe(0);
      expect(scoreValue("auto buyers", "Auto Intenders")).toBe(0);
      expect(scoreValue("region 3", "Region 3")).toBe(0.98);
   });

   it("ranks a closer value above a longer one and breaks ties stably", () => {
      const hits = matchValues("auto intenders", [
         "Home Shoppers",
         "Auto Intenders (In-Market) 2024",
         "Auto Intenders",
         "Auto Intenders (In-Market)",
         "Travel Seekers",
      ]);
      expect(hits).toEqual([
         { value: "Auto Intenders", score: 0.98 },
         { value: "Auto Intenders (In-Market)", score: 0.9167 },
         { value: "Auto Intenders (In-Market) 2024", score: 0.8966 },
      ]);
   });

   it("keeps at most the asked number of values", () => {
      const values = Array.from({ length: 30 }, (_, i) => `Auto ${i}`);
      expect(matchValues("auto", values)).toHaveLength(10);
      expect(matchValues("auto", values, 3)).toHaveLength(3);
   });

   it("normalizes case, accents, punctuation and spacing", () => {
      expect(normalizeText("  Café   Au-Lait! ")).toBe("cafe au lait");
   });

   it("counts a swap of neighbouring letters as one edit", () => {
      expect(editDistance("intedners", "intenders", 2)).toBe(1);
      expect(editDistance("abc", "xyz", 1)).toBe(2);
   });

   it("builds the text-filter stem from the longest word, four letters", () => {
      expect(filterStem("the Auto Intenders")).toBe("inte");
      expect(filterStem("ab")).toBeUndefined();
   });
});

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

describe("retrieval.dimensionValues settings", () => {
   it("accepts the documented keys, and a zero cache lifetime", () => {
      expect(
         parseRetrievalConfig({
            dimensionValues: {
               enabled: false,
               maxDimensions: 5,
               maxValuesPerDimension: 100,
               timeoutMs: 1000,
               budgetMs: 2000,
               cacheTtlMs: 0,
            },
         })?.dimensionValues,
      ).toEqual({
         enabled: false,
         maxDimensions: 5,
         maxValuesPerDimension: 100,
         timeoutMs: 1000,
         budgetMs: 2000,
         cacheTtlMs: 0,
      });
   });

   it("names the valid keys on a typo", () => {
      expect(() =>
         parseRetrievalConfig({ dimensionValues: { maxDimension: 5 } }),
      ).toThrow(
         "Invalid retrieval.dimensionValues: unknown key 'maxDimension'. Valid keys: enabled, maxDimensions, maxValuesPerDimension, timeoutMs, budgetMs, cacheTtlMs.",
      );
   });

   it("refuses a bad value with the fix", () => {
      expect(() =>
         parseRetrievalConfig({ dimensionValues: { maxDimensions: 0 } }),
      ).toThrow(
         "Invalid retrieval.dimensionValues.maxDimensions: expected a positive integer, got 0. Fix: set it to e.g. 12",
      );
      expect(() =>
         parseRetrievalConfig({ dimensionValues: { cacheTtlMs: -1 } }),
      ).toThrow(
         "Invalid retrieval.dimensionValues.cacheTtlMs: expected a non-negative integer, got -1.",
      );
   });
});

// ---------------------------------------------------------------------------
// Handler plumbing
// ---------------------------------------------------------------------------

type Handler = (params: Record<string, unknown>) => Promise<{
   isError?: boolean;
   content: Array<{ resource?: { text: string } }>;
}>;

function captureHandler(store: unknown): Handler {
   let handler: Handler | undefined;
   const fakeServer = {
      tool: (name: string, _d: string, _s: unknown, h: Handler) => {
         if (name === "get_context") handler = h;
      },
   };
   registerGetContextTool(fakeServer as never, store as EnvironmentStore);
   if (!handler) throw new Error("get_context was not registered");
   return handler;
}

async function call(handler: Handler, params: Record<string, unknown>) {
   const result = await handler(params);
   return JSON.parse(result.content[0].resource!.text);
}

const SCOPE = [{ environment: "env", package: "pkg" }];
const valueTarget = (text: string) => ({
   target_type: "dimensional_value",
   search_text: text,
});

beforeEach(() => {
   _setEmbeddingProviderForTests(null);
   _resetEmbeddingIndexStateForTests();
   _resetValueCacheForTests();
});

afterEach(() => {
   _setValueFetcherForTests(undefined);
   setRetrievalConfig(undefined);
});

afterAll(() => {
   _clearEmbeddingProviderForTests();
});

// ---------------------------------------------------------------------------
// 2. The stage over a stand-in warehouse
// ---------------------------------------------------------------------------

const str = (kind: string, name: string) => ({
   kind,
   name,
   type: { kind: "string_type" },
   annotations: [],
});

/** A package whose entities need no compiler: names, kinds and types only. */
function stubPackage() {
   const model = {
      getSourceInfos: () => [
         {
            name: "segments",
            annotations: ["#(doc) Audience segments."],
            schema: {
               fields: [
                  str("dimension", "segment_name"),
                  str("dimension", "region"),
                  {
                     kind: "dimension",
                     name: "size",
                     type: { kind: "number_type" },
                     annotations: [],
                  },
                  {
                     kind: "measure",
                     name: "members",
                     type: { kind: "number_type" },
                     annotations: [],
                  },
               ],
            },
         },
         {
            name: "locked",
            annotations: ["#(doc) A gated source."],
            schema: { fields: [str("dimension", "locked_name")] },
         },
      ],
      getQueries: () => [],
   };
   return {
      listModels: async () => [{ path: "m.malloy" }],
      getModel: (p: string) => (p === "m.malloy" ? model : undefined),
   };
}

const storeFor = (pkg: unknown) => ({
   getEnvironment: async () => ({
      getPackage: async () => pkg,
      getStaleCompileErrors: () => new Map(),
   }),
});

const STORED: Record<string, string[]> = {
   "segments.segment_name": [
      "Auto Intenders (In-Market)",
      "Home Shoppers",
      "Travel Seekers",
      "Auto Enthusiasts",
   ],
   "segments.region": ["North", "South", "East", "West"],
   "locked.locked_name": ["Secret Auto Intenders"],
};

function warehouse(calls: ValueFetchRequest[] = []) {
   return async (req: ValueFetchRequest) => {
      calls.push(req);
      const key = `${req.source}.${req.dimension}`;
      if (req.source === "locked") throw new AccessDeniedError("denied");
      const all = STORED[key] ?? [];
      const matching = req.stem
         ? all.filter((v) => v.toLowerCase().includes(req.stem as string))
         : all;
      return {
         values: matching.slice(0, req.limit),
         truncated: matching.length > req.limit,
      };
   };
}

describe("get_context value search over a stand-in warehouse", () => {
   it("returns the stored string under its dimension, in the sources -> entities -> values shape", async () => {
      _setValueFetcherForTests(warehouse());
      const handler = captureHandler(storeFor(stubPackage()));
      const payload = await call(handler, {
         search_targets: [valueTarget("Auto Intenders")],
         scopes: [{ ...SCOPE[0], source: "segments" }],
      });
      expect(payload.sources).toHaveLength(1);
      const card = payload.sources[0];
      expect(card.source_info.resource_id).toEqual({
         environment: "env",
         package: "pkg",
         model_path: "m.malloy",
         source: "segments",
      });
      expect(card.entities).toEqual([
         {
            name: "segment_name",
            entity_type: "dimension",
            entity_id: "dimension:segments:segment_name",
            relevance: 0.9167,
            data_type: "string",
            matched_targets: [
               { search_text: "Auto Intenders", relevance: 0.9167 },
            ],
            values: [
               {
                  value: "Auto Intenders (In-Market)",
                  relevance: 0.9167,
                  search_text: "Auto Intenders",
               },
            ],
         },
      ]);
      expect(payload.warnings).toBeUndefined();
   });

   it("finds the value through case, spacing, a typo and a partial phrase", async () => {
      _setValueFetcherForTests(warehouse());
      const handler = captureHandler(storeFor(stubPackage()));
      for (const typed of [
         "AUTO   INTENDERS",
         "auto intender",
         "auto intendrs",
         "in-market",
      ]) {
         const payload = await call(handler, {
            search_targets: [valueTarget(typed)],
            scopes: [{ ...SCOPE[0], source: "segments" }],
         });
         const values = payload.sources[0].entities[0].values;
         expect(values[0].value).toBe("Auto Intenders (In-Market)");
      }
   });

   it("searches only what the scope names", async () => {
      const calls: ValueFetchRequest[] = [];
      _setValueFetcherForTests(warehouse(calls));
      const handler = captureHandler(storeFor(stubPackage()));
      await call(handler, {
         search_targets: [valueTarget("North")],
         scopes: [{ ...SCOPE[0], source: "segments" }],
      });
      // String dimensions of that source only: not the number, not the measure,
      // not the other source.
      expect(calls.map((c) => `${c.source}.${c.dimension}`).sort()).toEqual([
         "segments.region",
         "segments.segment_name",
      ]);
      expect(calls.every((c) => c.modelPath === "m.malloy")).toBe(true);
      expect(calls[0].limit).toBe(2000);
   });

   it("attaches values to a dimension the entity search also returned", async () => {
      _setValueFetcherForTests(warehouse());
      const handler = captureHandler(storeFor(stubPackage()));
      const payload = await call(handler, {
         search_targets: [
            { target_type: "dimension", search_text: "segment name" },
            valueTarget("Home Shoppers"),
         ],
         scopes: [{ ...SCOPE[0], source: "segments" }],
      });
      const names = payload.sources[0].entities.map(
         (e: { name: string }) => e.name,
      );
      expect(names.filter((n: string) => n === "segment_name")).toHaveLength(1);
      const row = payload.sources[0].entities.find(
         (e: { name: string }) => e.name === "segment_name",
      );
      expect(row.values).toEqual([
         { value: "Home Shoppers", relevance: 1, search_text: "Home Shoppers" },
      ]);
   });

   it("says what was and was not searched when nothing matches", async () => {
      _setValueFetcherForTests(warehouse());
      const handler = captureHandler(storeFor(stubPackage()));
      const payload = await call(handler, {
         search_targets: [valueTarget("zzzzzz")],
         scopes: [{ ...SCOPE[0], source: "segments" }],
      });
      expect(payload.sources).toEqual([]);
      expect(payload.warnings).toEqual([
         'No stored value matched "zzzzzz" in 2 string dimensions of 1 source. Check the spelling, or read a dimension\'s distinct values with execute_query.',
      ]);
   });

   it("caps the dimensions searched and says how many it skipped", async () => {
      const calls: ValueFetchRequest[] = [];
      _setValueFetcherForTests(warehouse(calls));
      setRetrievalConfig({ dimensionValues: { maxDimensions: 1 } });
      const handler = captureHandler(storeFor(stubPackage()));
      const payload = await call(handler, {
         search_targets: [valueTarget("North")],
         scopes: [{ ...SCOPE[0], source: "segments" }],
      });
      expect(calls).toHaveLength(1);
      expect(payload.warnings).toContain(
         "Value search covered 1 of 2 string dimensions in scope. Scope the call to the source that holds the value to search the rest, or raise retrieval.dimensionValues.maxDimensions.",
      );
   });

   it("answers a timed-out dimension with a warning, not a failure", async () => {
      _setValueFetcherForTests(async (req) => {
         if (req.dimension === "region") throw new QueryTimeoutError("slow");
         return warehouse()(req);
      });
      const handler = captureHandler(storeFor(stubPackage()));
      const payload = await call(handler, {
         search_targets: [valueTarget("Home Shoppers")],
         scopes: [{ ...SCOPE[0], source: "segments" }],
      });
      expect(payload.isError).toBeUndefined();
      expect(payload.sources[0].entities[0].values[0].value).toBe(
         "Home Shoppers",
      );
      expect(payload.warnings).toEqual([
         "Value search skipped segments.region: the query timed out. Scope the call to the source that holds the value, or see retrieval.dimensionValues in the server configuration.",
      ]);
   });

   it("searches a dimension past the read cap with a text filter, and says so", async () => {
      const calls: ValueFetchRequest[] = [];
      _setValueFetcherForTests(warehouse(calls));
      // 4 stored names, read cap 2: the plain read is truncated.
      setRetrievalConfig({ dimensionValues: { maxValuesPerDimension: 2 } });
      const handler = captureHandler(storeFor(stubPackage()));
      const payload = await call(handler, {
         search_targets: [valueTarget("Auto Enthusiasts")],
         scopes: [
            { ...SCOPE[0], source: "segments", entity_name: "segment_name" },
         ],
      });
      expect(calls.map((c) => c.stem)).toEqual([undefined, "enth"]);
      expect(payload.sources[0].entities[0].values[0]).toEqual({
         value: "Auto Enthusiasts",
         relevance: 1,
         search_text: "Auto Enthusiasts",
      });
      expect(payload.warnings).toContain(
         "segments.segment_name has more than 2 distinct values, so it was searched with a text filter on the main word instead of in full. A value spelled very differently may be missed.",
      );
   });

   it("reuses a dimension's values for the same givens, and reads again for other givens", async () => {
      const calls: ValueFetchRequest[] = [];
      _setValueFetcherForTests(warehouse(calls));
      const handler = captureHandler(storeFor(stubPackage()));
      const ask = (givens?: Record<string, unknown>) =>
         call(handler, {
            search_targets: [valueTarget("North")],
            scopes: [
               { ...SCOPE[0], source: "segments", entity_name: "region" },
            ],
            ...(givens ? { givens } : {}),
         });
      await ask();
      expect(calls).toHaveLength(1);
      expect(_valueCacheCounters()).toEqual({ hits: 0, misses: 1 });
      await ask();
      expect(calls).toHaveLength(1);
      expect(_valueCacheCounters()).toEqual({ hits: 1, misses: 1 });
      // Another caller's givens never read another caller's cached list.
      await ask({ TENANTS: ["acme"] });
      expect(calls).toHaveLength(2);
      expect(calls[1].givens).toEqual({ TENANTS: ["acme"] });
      // Key order inside the givens does not matter.
      await ask({ A: 1, B: 2 });
      await ask({ B: 2, A: 1 });
      expect(calls).toHaveLength(3);
   });

   it("does not cache at all when the lifetime is 0", async () => {
      const calls: ValueFetchRequest[] = [];
      _setValueFetcherForTests(warehouse(calls));
      setRetrievalConfig({ dimensionValues: { cacheTtlMs: 0 } });
      const handler = captureHandler(storeFor(stubPackage()));
      const params = {
         search_targets: [valueTarget("North")],
         scopes: [{ ...SCOPE[0], source: "segments", entity_name: "region" }],
      };
      await call(handler, params);
      await call(handler, params);
      expect(calls).toHaveLength(2);
   });

   it("withholds a gated source: its dimensions come back with no values, and a warning", async () => {
      _setValueFetcherForTests(warehouse());
      const handler = captureHandler(storeFor(stubPackage()));
      const payload = await call(handler, {
         search_targets: [valueTarget("Auto Intenders")],
         scopes: [SCOPE[0]],
      });
      const bySource = Object.fromEntries(
         payload.sources.map(
            (s: {
               source_info: { resource_id: { source: string } };
               entities: unknown[];
            }) => [s.source_info.resource_id.source, s.entities],
         ),
      );
      expect(JSON.stringify(bySource.locked)).not.toContain("Secret");
      expect(bySource.locked).toEqual([
         {
            name: "locked_name",
            entity_type: "dimension",
            entity_id: "dimension:locked:locked_name",
            data_type: "string",
         },
      ]);
      expect(bySource.segments[0].values[0].value).toBe(
         "Auto Intenders (In-Market)",
      );
      expect(payload.warnings).toEqual([
         "Dimension values were not searched in locked because an access gate applies to it and the givens sent with this call did not pass it. Send the givens the gate reads (the same ones execute_query takes), or read the dimension's distinct values with execute_query.",
      ]);
   });

   it("does not send a value to the embedding provider under the default egress preset", async () => {
      const sent: string[] = [];
      _setEmbeddingProviderForTests(fakeEmbedder(sent));
      _setValueFetcherForTests(warehouse());
      const handler = captureHandler(storeFor(stubPackage()));
      const payload = await call(handler, {
         search_targets: [valueTarget("people about to buy a car")],
         scopes: [
            { ...SCOPE[0], source: "segments", entity_name: "segment_name" },
         ],
      });
      expect(sent).toEqual([]);
      // No shared word: spelling-based matching finds nothing.
      expect(payload.sources).toEqual([]);
   });

   it("matches by meaning under egress full, and sends the values only then", async () => {
      const sent: string[] = [];
      _setEmbeddingProviderForTests(fakeEmbedder(sent));
      setRetrievalConfig({ egress: { preset: "full" } });
      _setValueFetcherForTests(warehouse());
      const handler = captureHandler(storeFor(stubPackage()));
      const payload = await call(handler, {
         search_targets: [valueTarget("people about to buy a car")],
         scopes: [
            { ...SCOPE[0], source: "segments", entity_name: "segment_name" },
         ],
      });
      expect(sent).toContain("Auto Intenders (In-Market)");
      expect(payload.sources[0].entities[0].values[0].value).toBe(
         "Auto Intenders (In-Market)",
      );
   });
});

/**
 * An embedder that maps "car"-ish text near "Auto Intenders" and nothing else,
 * and records every text it is asked to embed.
 */
function fakeEmbedder(sent: string[]): EmbeddingModel {
   const vec = (text: string) => {
      const t = text.toLowerCase();
      return t.includes("car") || t.includes("auto intenders")
         ? [1, 0, 0]
         : [0, 1, 0];
   };
   return {
      provider: "openai",
      model: "fake",
      dimensions: 3,
      maxBatch: 100,
      minSimilarity: 0.2,
      queryPrefix: "",
      documentPrefix: "",
      embed: async (texts: string[]) => {
         sent.push(...texts);
         return texts.map(vec);
      },
      embedBatch: async (texts: string[]) => {
         sent.push(...texts);
         return texts.map(vec);
      },
   } as unknown as EmbeddingModel;
}

// ---------------------------------------------------------------------------
// 3. The stage over a real model, with gates
// ---------------------------------------------------------------------------

const REAL_MODEL = `##! experimental.givens

given:
  role :: string
  TENANTS :: string[]

source: seg_data is duckdb.sql("""
  select * from (values
    ('acme',   'Auto Intenders (In-Market)', 'US'),
    ('acme',   'Home Shoppers',              'US'),
    ('globex', 'Auto Intenders (Lapsed)',    'EU'),
    ('globex', 'Travel Seekers',             'EU')
  ) t(tenant, segment_name, market)
""")

source: open_segments is seg_data extend {
  dimension: segment is segment_name
}

#(authorize) 'analyst' = $role
source: locked_segments is seg_data extend {
  dimension: segment is segment_name
}

#(access_filter) tenant in $TENANTS
source: tenant_segments is seg_data extend {
  dimension: segment is segment_name
}
`;

describe("get_context value search over a real model (gates are the query path's)", () => {
   let dir: string;
   let duckdb: DuckDBConnection;
   let handler: Handler;

   beforeAll(async () => {
      dir = fs.mkdtempSync(path.join(os.tmpdir(), "values-real-"));
      fs.writeFileSync(path.join(dir, "m.malloy"), REAL_MODEL);
      duckdb = new DuckDBConnection("duckdb", ":memory:", dir);
      const model = await Model.create(
         "pkg",
         dir,
         "m.malloy",
         new Map<string, Connection>([["duckdb", duckdb]]),
      );
      expect(
         (model as unknown as { compilationError?: Error }).compilationError,
      ).toBeUndefined();
      const pkg = {
         listModels: async () => [{ path: "m.malloy" }],
         getModel: (p: string) => (p === "m.malloy" ? model : undefined),
      };
      handler = captureHandler({
         getEnvironment: async () => ({
            assertCanAdmitQuery: () => undefined,
            getPackage: async () => pkg,
            getStaleCompileErrors: () => new Map(),
         }),
      });
   });

   afterAll(async () => {
      await duckdb.close();
      fs.rmSync(dir, { recursive: true, force: true });
   });

   const ask = (
      source: string,
      text: string,
      extra: Record<string, unknown> = {},
   ) =>
      call(handler, {
         search_targets: [valueTarget(text)],
         scopes: [{ ...SCOPE[0], source, entity_name: "segment" }],
         ...extra,
      });
   const valuesOf = (payload: {
      sources: Array<{
         entities?: Array<{ values?: Array<{ value: string }> }>;
      }>;
   }) =>
      payload.sources.flatMap((s) =>
         (s.entities ?? []).flatMap((e) =>
            (e.values ?? []).map((v) => v.value),
         ),
      );

   it("reads an ungated source's values, without any givens", async () => {
      const payload = await ask("open_segments", "auto intender");
      expect(valuesOf(payload)).toEqual([
         "Auto Intenders (Lapsed)",
         "Auto Intenders (In-Market)",
      ]);
   });

   it("withholds an #(authorize) source from a caller who sends no givens, then serves the one who sends the right ones", async () => {
      const denied = await ask("locked_segments", "auto intenders");
      expect(valuesOf(denied)).toEqual([]);
      expect(JSON.stringify(denied)).not.toContain("Auto Intenders (");
      expect(denied.warnings[0]).toStartWith(
         "Dimension values were not searched in locked_segments because an access gate applies",
      );

      const wrong = await ask("locked_segments", "auto intenders", {
         givens: { role: "intern" },
      });
      expect(valuesOf(wrong)).toEqual([]);

      const admitted = await ask("locked_segments", "auto intenders", {
         givens: { role: "analyst" },
      });
      expect(valuesOf(admitted)).toEqual([
         "Auto Intenders (Lapsed)",
         "Auto Intenders (In-Market)",
      ]);
   });

   it("returns values only from the rows an #(access_filter) admits, and none when the gate cannot apply", async () => {
      const none = await ask("tenant_segments", "auto intenders");
      expect(valuesOf(none)).toEqual([]);
      expect(none.warnings[0]).toStartWith(
         "Dimension values were not searched in tenant_segments",
      );

      const acme = await ask("tenant_segments", "auto intenders", {
         givens: { TENANTS: ["acme"] },
      });
      expect(valuesOf(acme)).toEqual(["Auto Intenders (In-Market)"]);

      // The same phrase for the other tenant must not be served from acme's
      // cached list, and must not show acme's value.
      const globex = await ask("tenant_segments", "auto intenders", {
         givens: { TENANTS: ["globex"] },
      });
      expect(valuesOf(globex)).toEqual(["Auto Intenders (Lapsed)"]);

      // A tenant that owns no rows sees no values, with no error.
      const nobody = await ask("tenant_segments", "auto intenders", {
         givens: { TENANTS: ["nobody"] },
      });
      expect(valuesOf(nobody)).toEqual([]);
      expect(nobody.isError).toBeUndefined();
   });

   it("serves a repeat call from the cache, and a different caller's call from the warehouse", async () => {
      _resetValueCacheForTests();
      // Givens no earlier test used, so the first read is a miss.
      const as = (role: string, text: string) =>
         ask("open_segments", text, { givens: { role } });
      await as("c1", "home shoppers");
      expect(_valueCacheCounters()).toEqual({ hits: 0, misses: 1 });
      await as("c1", "travel");
      expect(_valueCacheCounters()).toEqual({ hits: 1, misses: 1 });
      await as("c2", "travel");
      expect(_valueCacheCounters()).toEqual({ hits: 1, misses: 2 });
   });
});
