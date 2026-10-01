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
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { DEFAULT_EMBEDDING_MIN_SIMILARITY } from "../config";
import { EmbeddingProvider } from "../service/embedding_provider";
import { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";
import { createDimensionValueTables } from "../storage/duckdb/schema";
import {
   _resetValueIndexStateForTests,
   discoverValueDimensions,
   fetchDimensionValues,
   getValueIndexStatus,
   kickValueIndex,
   _settleValueIndexForTests,
   quoteIdentifier,
   searchDimensionValues,
   syncDimensionValues,
   valuesQuery,
   type RunQuery,
   type ValueCandidate,
   type ValueDimension,
} from "./dim_values";
import {
   resolveRetrievalConfig,
   type RetrievalConfig,
} from "./retrieval_config";

const ENV = "env";
const PKG = "pkg";

const dim = (
   source: string,
   name: string,
   extra: Partial<ValueCandidate> = {},
): ValueCandidate => ({
   kind: "dimension",
   name,
   source,
   modelPath: "m.malloy",
   dataType: "string",
   ...extra,
});

const cfgOf = (over: Record<string, unknown> = {}): RetrievalConfig =>
   resolveRetrievalConfig({
      dimensionalValues: { mode: "annotated" },
      ...over,
   });

describe("discoverValueDimensions", () => {
   const never = () => false;
   const names = (list: ReturnType<typeof discoverValueDimensions>) =>
      list.map((d) => `${d.source}.${d.dimension}`).sort();

   it("finds nothing when the feature is off", () => {
      const c = resolveRetrievalConfig({});
      expect(
         discoverValueDimensions(
            [dim("s", "a", { indexValues: {} })],
            c.dimensionalValues,
            never,
         ),
      ).toEqual([]);
   });

   it("takes only tagged dimensions in annotated mode", () => {
      const c = cfgOf();
      const found = discoverValueDimensions(
         [dim("s", "tagged", { indexValues: {} }), dim("s", "plain")],
         c.dimensionalValues,
         never,
      );
      expect(names(found)).toEqual(["s.tagged"]);
   });

   it("takes every string dimension in auto mode, and only strings", () => {
      const c = cfgOf({ dimensionalValues: { mode: "auto" } });
      const found = discoverValueDimensions(
         [
            dim("s", "a"),
            dim("s", "n", { dataType: "number" }),
            dim("s", "d", { dataType: "date" }),
         ],
         c.dimensionalValues,
         never,
      );
      expect(names(found)).toEqual(["s.a"]);
   });

   it("narrows auto with include, and lets include add to annotated", () => {
      const auto = cfgOf({
         dimensionalValues: { mode: "auto", include: ["s.a"] },
      });
      expect(
         names(
            discoverValueDimensions(
               [dim("s", "a"), dim("s", "b")],
               auto.dimensionalValues,
               never,
            ),
         ),
      ).toEqual(["s.a"]);
      const annotated = cfgOf({
         dimensionalValues: { mode: "annotated", include: ["s.b"] },
      });
      expect(
         names(
            discoverValueDimensions(
               [
                  dim("s", "a", { indexValues: {} }),
                  dim("s", "b"),
                  dim("s", "c"),
               ],
               annotated.dimensionalValues,
               never,
            ),
         ),
      ).toEqual(["s.a", "s.b"]);
   });

   it("removes what exclude names, in either mode", () => {
      const c = cfgOf({
         dimensionalValues: { mode: "auto", exclude: ["*.email", "audit.*"] },
      });
      expect(
         names(
            discoverValueDimensions(
               [dim("s", "name"), dim("s", "email"), dim("audit", "user")],
               c.dimensionalValues,
               never,
            ),
         ),
      ).toEqual(["s.name"]);
   });

   it("never takes a gated source, whatever else says to", () => {
      const c = cfgOf({
         dimensionalValues: { mode: "auto", include: ["secure.*"] },
      });
      const found = discoverValueDimensions(
         [
            dim("secure", "tenant_name", { indexValues: {} }),
            dim("open", "name"),
         ],
         c.dimensionalValues,
         (_path, source) => source === "secure",
      );
      expect(names(found)).toEqual([]);
      const auto = cfgOf({ dimensionalValues: { mode: "auto" } });
      expect(
         names(
            discoverValueDimensions(
               [dim("secure", "tenant_name"), dim("open", "name")],
               auto.dimensionalValues,
               (_p, source) => source === "secure",
            ),
         ),
      ).toEqual(["open.name"]);
   });

   it("skips a joined field and an alias: they belong to another source or column", () => {
      const c = cfgOf({ dimensionalValues: { mode: "auto" } });
      const found = discoverValueDimensions(
         [
            dim("s", "customer.tier", { joinPath: "customer" }),
            dim("s", "tier_alias", { aliasOf: "tier" }),
            dim("s", "tier"),
         ],
         c.dimensionalValues,
         never,
      );
      expect(names(found)).toEqual(["s.tier"]);
   });

   it("returns one entry per source and dimension, whatever number of files resolve it", () => {
      const c = cfgOf({ dimensionalValues: { mode: "auto" } });
      const found = discoverValueDimensions(
         [
            dim("s", "tier", { modelPath: "a.malloy" }),
            dim("s", "tier", { modelPath: "b.malloy" }),
         ],
         c.dimensionalValues,
         never,
      );
      expect(found).toHaveLength(1);
      expect(found[0].modelPath).toBe("a.malloy");
   });

   it("caps at the smaller of the author's n and the config", () => {
      const c = cfgOf({
         dimensionalValues: { mode: "annotated", maxValuesPerDimension: 100 },
      });
      const found = discoverValueDimensions(
         [
            dim("s", "small", { indexValues: { n: 20 } }),
            dim("s", "large", { indexValues: { n: 5000 } }),
            dim("s", "plain", { indexValues: {} }),
         ],
         c.dimensionalValues,
         never,
      );
      const caps = Object.fromEntries(found.map((d) => [d.dimension, d.cap]));
      expect(caps).toEqual({ small: 20, large: 100, plain: 100 });
   });
});

describe("the fetch query", () => {
   it("quotes every name so an odd one cannot break out", () => {
      expect(quoteIdentifier("plain")).toBe("`plain`");
      expect(quoteIdentifier("with space")).toBe("`with space`");
      expect(quoteIdentifier("tick`s")).toBe("`tick\\`s`");
      expect(quoteIdentifier("back\\slash")).toBe("`back\\\\slash`");
   });

   it("asks for the most common values, one over the cap", () => {
      const q = valuesQuery("order_items", "status", 101);
      expect(q).toBe(
         "run: `order_items` -> { group_by: `status`; aggregate: `value_weight__` is count(); order_by: `value_weight__` desc; limit: 101 }",
      );
   });
});

const dimension: ValueDimension = {
   source: "customers",
   dimension: "tier",
   modelPath: "m.malloy",
   cap: 100,
};

const rows = (...pairs: Array<[unknown, number]>) =>
   pairs.map(([v, w]) => ({ tier: v, value_weight__: w }));

describe("fetchDimensionValues", () => {
   const runReturning =
      (result: ReturnType<typeof rows>): RunQuery =>
      async () =>
         result;

   it("keeps values in weight order, with their counts", async () => {
      const out = await fetchDimensionValues(
         runReturning(rows(["Premium", 50], ["Basic", 30])),
         dimension,
         10,
         128,
         1000,
      );
      expect(out.values).toEqual([
         { value: "Premium", weight: 50 },
         { value: "Basic", weight: 30 },
      ]);
      expect(out.truncated).toBe(false);
   });

   it("reports truncation when the query saw one more than the cap", async () => {
      const out = await fetchDimensionValues(
         runReturning(rows(["a", 3], ["b", 2], ["c", 1])),
         dimension,
         2,
         128,
         1000,
      );
      expect(out.values.map((v) => v.value)).toEqual(["a", "b"]);
      expect(out.truncated).toBe(true);
      expect(out.distinctSeen).toBe(3);
   });

   it("drops null, blank and oversize values and stringifies numbers", async () => {
      const out = await fetchDimensionValues(
         runReturning(
            rows(
               [null, 9],
               ["  ", 8],
               ["x".repeat(200), 7],
               [42, 6],
               [true, 5],
               ["  padded ", 4],
            ),
         ),
         dimension,
         10,
         50,
         1000,
      );
      expect(out.values.map((v) => v.value)).toEqual(["42", "true", "padded"]);
   });

   it("hands the query the model path and a signal", async () => {
      let seen:
         | { modelPath: string; query: string; signal: AbortSignal }
         | undefined;
      await fetchDimensionValues(
         async (modelPath, query, signal) => {
            seen = { modelPath, query, signal };
            return [];
         },
         dimension,
         5,
         128,
         1000,
      );
      expect(seen?.modelPath).toBe("m.malloy");
      expect(seen?.query).toContain("limit: 6");
      expect(seen?.signal.aborted).toBe(false);
   });
});

function embedder(
   vectorFor: (text: string) => number[],
   asked: string[] = [],
   role: string[] = [],
): EmbeddingProvider {
   const fetchStub = (async (_u: RequestInfo | URL, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body)) as { input: string[] };
      asked.push(...body.input);
      role.push(body.input.length === 1 ? "single" : "batch");
      return new Response(
         JSON.stringify({
            data: body.input.map((t, index) => ({
               index,
               embedding: vectorFor(t),
            })),
         }),
         { status: 200 },
      );
   }) as typeof fetch;
   return new EmbeddingProvider(
      {
         apiKey: "t",
         model: "stub",
         baseUrl: "https://stub.example.com/v1",
         minSimilarity: DEFAULT_EMBEDDING_MIN_SIMILARITY,
      },
      fetchStub,
   );
}

describe("value index storage and search", () => {
   let tempDir: string;
   let db: DuckDBConnection;

   beforeAll(async () => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "dim-values-"));
      db = new DuckDBConnection(path.join(tempDir, "test.db"));
      await db.initialize();
      await createDimensionValueTables(db);
   });
   afterAll(async () => {
      await db.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
   });
   beforeEach(async () => {
      _resetValueIndexStateForTests();
      await db.run("DELETE FROM dimension_values");
      await db.run("DELETE FROM dimension_value_state");
   });

   const sync = (
      run: RunQuery,
      provider: EmbeddingProvider | null,
      over: Record<string, unknown> = {},
      dims: ValueDimension[] = [dimension],
      itemBudget = 10_000,
   ) =>
      syncDimensionValues({
         db,
         provider,
         environmentName: ENV,
         packageName: PKG,
         dims,
         run,
         config: cfgOf(over),
         itemBudget,
      });

   const stored = () =>
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      db.all<any>(
         "SELECT source_name, dimension_name, value, CAST(weight AS DOUBLE) AS weight, embedding IS NOT NULL AS has_vec, embedded_text FROM dimension_values ORDER BY source_name, dimension_name, value",
      );

   const twoValues: RunQuery = async () => rows(["Premium", 50], ["Basic", 30]);

   describe("syncing", () => {
      it("stores values with their counts and what was embedded", async () => {
         const status = await sync(
            twoValues,
            embedder(() => [1, 0]),
         );
         expect(status).toMatchObject({
            status: "ready",
            dimensions: 1,
            values: 2,
            truncated: 0,
            failed: 0,
         });
         const all = await stored();
         expect(
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            all.map((r: any) => `${r.value}:${r.weight}:${r.has_vec}`),
         ).toEqual(["Basic:30:true", "Premium:50:true"]);
         expect(all[0].embedded_text).toBe("Basic");
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const state = await db.all<any>(
            "SELECT truncated, status, kept FROM dimension_value_state",
         );
         expect(state[0]).toMatchObject({ truncated: false, status: "ok" });
         expect(Number(state[0].kept)).toBe(2);
      });

      it("keeps the text when there is no embedding provider, so the lexical arm still works", async () => {
         await sync(twoValues, null);
         const all = await stored();
         expect(all).toHaveLength(2);
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         expect(all.every((r: any) => r.has_vec === false)).toBe(true);
         const hits = await searchDimensionValues({
            db,
            provider: null,
            environmentName: ENV,
            packageName: PKG,
            queries: [{ targetIndex: 0, text: "premium" }],
            config: cfgOf(),
            minSimilarity: 0.2,
         });
         expect(hits.map((h) => h.value)).toEqual(["Premium"]);
      });

      it("does not re-embed a value that has not changed, and drops one that is gone", async () => {
         const asked: string[] = [];
         await sync(
            twoValues,
            embedder(() => [1, 0], asked),
         );
         expect(asked).toEqual(["Premium", "Basic"]);
         asked.length = 0;
         // Age the fetch so the next run is due to read the warehouse again.
         await db.run(
            "UPDATE dimension_value_state SET fetched_at = TIMESTAMP '2000-01-01 00:00:00'",
         );
         await sync(
            async () => rows(["Premium", 60], ["Trial", 5]),
            embedder(() => [1, 0], asked),
            { dimensionalValues: { mode: "annotated", refreshMinutes: 1 } },
         );
         // Premium's text is unchanged, so only Trial is embedded, and Basic goes.
         expect(asked).toEqual(["Trial"]);
         const all = await stored();
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         expect(all.map((r: any) => `${r.value}:${r.weight}`)).toEqual([
            "Premium:60",
            "Trial:5",
         ]);
      });

      it("keeps the top values by count and marks the dimension truncated", async () => {
         const status = await sync(
            async () => rows(["a", 5], ["b", 4], ["c", 3], ["d", 2]),
            null,
            {
               dimensionalValues: {
                  mode: "annotated",
                  maxValuesPerDimension: 2,
               },
            },
            [{ ...dimension, cap: 2 }],
         );
         expect(status).toMatchObject({ values: 2, truncated: 1 });
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         expect((await stored()).map((r: any) => r.value)).toEqual(["a", "b"]);
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const state = await db.all<any>(
            "SELECT truncated FROM dimension_value_state",
         );
         expect(state[0].truncated).toBe(true);
      });

      it("skips an over-cap dimension entirely when onOverflow is skip", async () => {
         const status = await sync(
            async () => rows(["a", 3], ["b", 2], ["c", 1]),
            null,
            { dimensionalValues: { mode: "annotated", onOverflow: "skip" } },
            [{ ...dimension, cap: 2 }],
         );
         expect(status.values).toBe(0);
         expect(await stored()).toEqual([]);
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const state = await db.all<any>(
            "SELECT status, truncated FROM dimension_value_state",
         );
         expect(state[0]).toMatchObject({ status: "skipped", truncated: true });
      });

      it("stops at the package cap, in the order the dimensions were given", async () => {
         const dims: ValueDimension[] = [
            { ...dimension, dimension: "a", cap: 100 },
            { ...dimension, dimension: "b", cap: 100 },
         ];
         const run: RunQuery = async (_m, q) => {
            const d = q.includes("`a`") ? "a" : "b";
            return [
               { [d]: `${d}1`, value_weight__: 3 },
               { [d]: `${d}2`, value_weight__: 2 },
               { [d]: `${d}3`, value_weight__: 1 },
            ];
         };
         const status = await sync(
            run,
            null,
            {
               dimensionalValues: { mode: "annotated", maxValuesPerPackage: 4 },
            },
            dims,
         );
         expect(status.values).toBe(4);
         const all = await stored();
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         expect(all.filter((r: any) => r.dimension_name === "a")).toHaveLength(
            3,
         );
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         expect(all.filter((r: any) => r.dimension_name === "b")).toHaveLength(
            1,
         );
      });

      it("shares the item budget with the entity facets", async () => {
         const status = await sync(twoValues, null, {}, [dimension], 1);
         expect(status.values).toBe(1);
      });

      it("records a failed dimension and carries on with the rest", async () => {
         const dims: ValueDimension[] = [
            { ...dimension, dimension: "bad" },
            { ...dimension, dimension: "tier" },
         ];
         const run: RunQuery = async (_m, q) => {
            if (q.includes("`bad`")) throw new Error("column not found");
            return rows(["Premium", 5]);
         };
         const status = await sync(run, null, {}, dims);
         expect(status).toMatchObject({
            status: "partial",
            values: 1,
            failed: 1,
         });
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const state = await db.all<any>(
            "SELECT dimension_name, status, last_error FROM dimension_value_state ORDER BY dimension_name",
         );
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         expect(state.map((s: any) => s.status)).toEqual(["failed", "ok"]);
         expect(state[0].last_error).toContain("column not found");
      });

      it("does not retry a failed dimension straight away", async () => {
         let calls = 0;
         const run: RunQuery = async () => {
            calls++;
            throw new Error("down");
         };
         await sync(run, null);
         await sync(run, null);
         expect(calls).toBe(1);
      });

      it("keeps a value as text when embedding fails, and embeds it on a later run", async () => {
         const down = new EmbeddingProvider(
            {
               apiKey: "t",
               model: "stub",
               baseUrl: "https://stub.example.com/v1",
               minSimilarity: DEFAULT_EMBEDDING_MIN_SIMILARITY,
            },
            (async () =>
               new Response("down", {
                  status: 500,
               })) as unknown as typeof fetch,
         );
         const first = await sync(twoValues, down);
         expect(first.status).toBe("ready");
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         expect((await stored()).every((r: any) => r.has_vec === false)).toBe(
            true,
         );
         await sync(
            twoValues,
            embedder(() => [1, 0]),
         );
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         expect((await stored()).every((r: any) => r.has_vec === true)).toBe(
            true,
         );
      });

      it("forgets a dimension that is no longer selected", async () => {
         await sync(twoValues, null);
         expect(await stored()).toHaveLength(2);
         await sync(twoValues, null, {}, []);
         expect(await stored()).toEqual([]);
         expect(await db.all("SELECT * FROM dimension_value_state")).toEqual(
            [],
         );
      });

      it("renders the embedded text through the template", async () => {
         const asked: string[] = [];
         await sync(
            twoValues,
            embedder(() => [1, 0], asked),
            {
               dimensionalValues: {
                  mode: "annotated",
                  template: "{dimension}: {value}",
               },
            },
         );
         expect(asked).toEqual(["tier: Premium", "tier: Basic"]);
      });
   });

   describe("searching", () => {
      const seed = async () => {
         await sync(
            async () =>
               rows(
                  ["Premium", 50],
                  ["Premier League", 10],
                  ["Basic", 30],
                  ["Enterprise", 5],
               ),
            embedder((t) => (/premium|premier/i.test(t) ? [1, 0] : [0, 1])),
         );
      };
      const search = (
         text: string,
         provider: EmbeddingProvider | null,
         over: Record<string, unknown> = {},
      ) =>
         searchDimensionValues({
            db,
            provider,
            environmentName: ENV,
            packageName: PKG,
            queries: [{ targetIndex: 2, text }],
            config: cfgOf(over),
            minSimilarity: 0.5,
         });

      it("finds an exact value first, ignoring case", async () => {
         await seed();
         const hits = await search("premium", null);
         expect(hits[0]).toMatchObject({
            value: "Premium",
            score: 1,
            source: "customers",
            dimension: "tier",
            targetIndex: 2,
         });
      });

      it("finds values that start with, or contain, the phrase", async () => {
         await seed();
         const hits = await search("prem", null);
         expect(hits.map((h) => `${h.value}:${h.score}`)).toEqual([
            "Premium:0.95",
            "Premier League:0.95",
         ]);
         const contains = await search("league", null);
         expect(contains.map((h) => `${h.value}:${h.score}`)).toEqual([
            "Premier League:0.9",
         ]);
      });

      it("finds a near spelling", async () => {
         await seed();
         const hits = await search("premuim", null);
         expect(hits.map((h) => h.value)).toContain("Premium");
         expect(hits[0].score).toBeGreaterThanOrEqual(0.85);
         // Below every text match, however close the spelling.
         expect(hits[0].score).toBeLessThan(0.9);
      });

      it("ranks exact, prefix, contains and near spelling in that order", async () => {
         await sync(
            async () =>
               rows(
                  ["car", 1],
                  ["cart", 1],
                  ["scar", 1],
                  ["caar", 1],
                  ["dog", 1],
               ),
            null,
         );
         const hits = await search("car", null);
         // "dog" is not near enough to appear at all.
         expect(hits.map((h) => `${h.value}:${h.score}`)).toEqual([
            "car:1",
            "cart:0.95",
            "scar:0.9",
            expect.stringMatching(/^caar:0\.8/),
         ]);
      });

      it("finds nothing for an unrelated phrase", async () => {
         await seed();
         expect(await search("zzzzzz", null)).toEqual([]);
      });

      it("adds the semantic arm when vectors exist, and keeps the better score per value", async () => {
         await seed();
         const provider = embedder((t) =>
            /premium|premier|top tier/i.test(t) ? [1, 0] : [0, 1],
         );
         const hits = await search("top tier", provider);
         // No text overlap, so only the vectors can find these.
         expect(hits.map((h) => h.value).sort()).toEqual([
            "Premier League",
            "Premium",
         ]);
         expect(hits.every((h) => h.score === 1)).toBe(true);
      });

      it("ranks a heavier value first among equal scores", async () => {
         await seed();
         const provider = embedder((t) =>
            /premium|premier|top tier/i.test(t) ? [1, 0] : [0, 1],
         );
         const hits = await search("top tier", provider);
         expect(hits[0].value).toBe("Premium");
      });

      it("uses only the lexical arm when embedding is switched off for values", async () => {
         await seed();
         const provider = embedder(() => [1, 0]);
         const hits = await search("top tier", provider, {
            dimensionalValues: { mode: "annotated", embed: false },
         });
         expect(hits).toEqual([]);
      });

      it("uses only the semantic arm when the lexical one is off", async () => {
         await seed();
         const provider = embedder((t) =>
            /premium|premier|premuim/i.test(t) ? [1, 0] : [0, 1],
         );
         const hits = await search("premuim", provider, {
            dimensionalValues: { mode: "annotated", lexical: false },
         });
         expect(hits.map((h) => h.value).sort()).toEqual([
            "Premier League",
            "Premium",
         ]);
      });

      it("honours the per-target hit cap", async () => {
         await seed();
         const hits = await search("e", null, {
            dimensionalValues: { mode: "annotated", maxHitsPerTarget: 1 },
         });
         expect(hits.length).toBeLessThanOrEqual(1);
      });

      it("can be limited to one source", async () => {
         await seed();
         const hits = await searchDimensionValues({
            db,
            provider: null,
            environmentName: ENV,
            packageName: PKG,
            queries: [{ targetIndex: 0, text: "premium" }],
            config: cfgOf(),
            sourceName: "orders",
            minSimilarity: 0.2,
         });
         expect(hits).toEqual([]);
      });

      it("keeps one package's values out of another's answers", async () => {
         await seed();
         const hits = await searchDimensionValues({
            db,
            provider: null,
            environmentName: ENV,
            packageName: "other",
            queries: [{ targetIndex: 0, text: "premium" }],
            config: cfgOf(),
            minSimilarity: 0.2,
         });
         expect(hits).toEqual([]);
      });
   });

   describe("kicking", () => {
      it("runs once per package instance and reports its status", async () => {
         let calls = 0;
         const args = {
            db,
            provider: null,
            environmentName: ENV,
            packageName: PKG,
            dims: [dimension],
            run: (async () => {
               calls++;
               return rows(["Premium", 5]);
            }) as RunQuery,
            config: cfgOf(),
            itemBudget: 1000,
         };
         const instance = {};
         kickValueIndex(instance, args);
         kickValueIndex(instance, args);
         expect(getValueIndexStatus(ENV, PKG)?.status).toBe("building");
         await _settleValueIndexForTests(ENV, PKG);
         expect(calls).toBe(1);
         expect(getValueIndexStatus(ENV, PKG)).toMatchObject({
            status: "ready",
            values: 1,
         });
         kickValueIndex(instance, args);
         await _settleValueIndexForTests(ENV, PKG);
         expect(calls).toBe(1); // finished, and not yet due to refresh
      });

      it("does nothing when no dimension is selected", async () => {
         kickValueIndex(
            {},
            {
               db,
               provider: null,
               environmentName: ENV,
               packageName: PKG,
               dims: [],
               run: (async () => []) as RunQuery,
               config: cfgOf(),
               itemBudget: 1000,
            },
         );
         expect(getValueIndexStatus(ENV, PKG)).toBeUndefined();
      });
   });
});
