// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { beforeAll, describe, expect, it } from "bun:test";
import {
   compileFcjFixture,
   type FcjFixture,
} from "../../test_helpers/fcj_fixture";
import embeddings from "../../test_helpers/value_embeddings_fixture.json";
import { normalizeText } from "../tools/get_context_value_match";
import type { DocParts } from "./doc_parts";
import { valueSpans } from "./value_tokens";
import { toUnit } from "./vectors";
import {
   buildValueIndex,
   MEANING_SHORT_FLOOR,
   ValueIndex,
   type IndexValueFetcher,
   type IndexValueRequest,
} from "./values_index";

const CITY = "dimension:cities@travel.malloy:city";
const ARRIVES = "dimension:flights@travel.malloy:arrives_at";

/** Stands in for the warehouse: the two #(index) dimensions of the fixture. */
const DATA: Record<string, Array<{ value: string; count: number }>> = {
   "cities.city": [
      { value: "Denver", count: 3 },
      { value: "Boulder", count: 1 },
   ],
   "flights.arrives_at": [
      { value: "Denver International (DIA)", count: 10 },
      { value: "Chicago O'Hare (ORD)", count: 5 },
      { value: "N/A", count: 1 },
   ],
};

let fixture: FcjFixture;
const asked: IndexValueRequest[] = [];
const fetcher: IndexValueFetcher = async (req) => {
   asked.push(req);
   const all = DATA[`${req.source}.${req.dimension}`] ?? [];
   return {
      values: all.slice(0, req.limit),
      truncated: all.length > req.limit,
   };
};

beforeAll(async () => {
   fixture = await compileFcjFixture();
});

const noParts = new Map<string, DocParts>();
const read = (f = fetcher) => ({
   environmentStore: {} as never,
   environmentName: "env",
   packageName: "fixture",
   fetcher: f,
});

describe("value index build", () => {
   it("reads #(index) dimensions of ungated sources, most frequent first, through the query path", async () => {
      asked.length = 0;
      const { index, report } = await buildValueIndex({
         catalog: fixture.catalog,
         parts: noParts,
         cap: 50_000,
         read: read(),
      });
      expect(
         asked.map((r) => [r.modelPath, r.source, r.dimension, r.limit]),
      ).toEqual([
         ["travel.malloy", "cities", "city", 50_000],
         ["travel.malloy", "flights", "arrives_at", 50_000],
      ]);
      expect(index.size).toBe(5);
      expect(report).toMatchObject({
         dimensionsRead: 2,
         valuesFromData: 5,
         valuesFromDocs: 0,
         cut: [],
         skipped: [],
      });
   });

   it("keeps the most frequent values up to the cap and says which dimension was cut", async () => {
      const { report, index } = await buildValueIndex({
         catalog: fixture.catalog,
         parts: noParts,
         cap: 2,
         read: read(),
      });
      expect(report.cut).toEqual([{ dimension: ARRIVES, kept: 2 }]);
      expect(index.values.map((v) => v.value)).toEqual([
         "Denver",
         "Boulder",
         "Denver International (DIA)",
         "Chicago O'Hare (ORD)",
      ]);
   });

   it("never reads a gated source, and seeds a dimension from its doc's value list", async () => {
      asked.length = 0;
      const cities = fixture.catalog.sources.get("cities@travel.malloy")!;
      cities.gated = true;
      try {
         const parts = new Map<string, DocParts>([
            [
               "dimension:cities@travel.malloy:state",
               {
                  what: "State code.",
                  values: ["CO", "IL"],
                  rules: [],
                  examples: [],
               },
            ],
         ]);
         const { report, index } = await buildValueIndex({
            catalog: fixture.catalog,
            parts,
            cap: 100,
            read: read(),
         });
         expect(asked.map((r) => r.source)).toEqual(["flights"]);
         expect(report.skipped).toEqual([
            {
               dimension: CITY,
               reason:
                  "its source has an access gate; values are read only through a query with the caller's givens",
            },
         ]);
         expect(report.valuesFromDocs).toBe(2);
         expect(
            index.search("CO").map((m) => [m.dimension, m.value, m.how]),
         ).toEqual([["dimension:cities@travel.malloy:state", "CO", "exact"]]);
      } finally {
         cities.gated = false;
      }
   });

   it("reports a failed read and goes on", async () => {
      const failing: IndexValueFetcher = async (req) => {
         if (req.source === "cities") throw new Error("table not found");
         return fetcher(req);
      };
      const { report } = await buildValueIndex({
         catalog: fixture.catalog,
         parts: noParts,
         cap: 100,
         read: read(failing),
      });
      expect(report.dimensionsRead).toBe(1);
      expect(report.skipped).toEqual([
         { dimension: CITY, reason: "the read failed: table not found" },
      ]);
   });
});

describe("value matching", () => {
   let index: ValueIndex;
   beforeAll(async () => {
      ({ index } = await buildValueIndex({
         catalog: fixture.catalog,
         parts: noParts,
         cap: 100,
         read: read(),
      }));
   });

   it("matches exactly in one dimension and by word in another: Denver against DIA", () => {
      expect(
         index
            .search("Denver")
            .map((m) => [m.dimension, m.value, m.score, m.how]),
      ).toEqual([
         [CITY, "Denver", 1, "exact"],
         [ARRIVES, "Denver International (DIA)", 0.85, "words"],
      ]);
   });

   it("tolerates case, punctuation and a typo", () => {
      expect(index.search("denver.")[0]).toMatchObject({
         value: "Denver",
         score: 0.98,
      });
      expect(index.search("Chicgo")[0]).toMatchObject({
         value: "Chicago O'Hare (ORD)",
         how: "words",
      });
   });

   it("weighs a value found in many dimensions down", () => {
      const spread = new ValueIndex([
         {
            value: "N/A",
            normalized: "n a",
            count: 1,
            dimension: "d1",
            origin: "data",
         },
         {
            value: "N/A",
            normalized: "n a",
            count: 1,
            dimension: "d2",
            origin: "data",
         },
         {
            value: "Gold",
            normalized: "gold",
            count: 1,
            dimension: "d3",
            origin: "data",
         },
      ]);
      expect(spread.nominate(["N/A", "Gold"])).toEqual([
         {
            dimension: "d3",
            score: 1,
            matches: [
               { value: "Gold", dimension: "d3", score: 1, how: "exact" },
            ],
         },
         {
            dimension: "d1",
            score: 0.5,
            matches: [
               { value: "N/A", dimension: "d1", score: 1, how: "exact" },
            ],
         },
         {
            dimension: "d2",
            score: 0.5,
            matches: [
               { value: "N/A", dimension: "d2", score: 1, how: "exact" },
            ],
         },
      ]);
   });
});

describe("value matching by meaning", () => {
   // Real text-embedding-3-small vectors at 512 dimensions, recorded once
   // (test_helpers/value_embeddings_fixture.json), so the thresholds are
   // tested against the embedder they were set for.
   const recorded = (
      embeddings as unknown as { vectors: Record<string, number[]> }
   ).vectors;
   const vec = (text: string) => toUnit(recorded[text]);
   const rows: Array<[string, string]> = [
      ["state", "California"],
      ["state", "Colorado"],
      ["state", "Connecticut"],
      ["state", "New York"],
      ["state", "New Jersey"],
      ["city", "New York"],
      ["city", "Newark"],
      ["city", "Denver"],
      ["category", "Athletic Shoes"],
      ["category", "Sandals"],
      ["category", "Boots"],
      ["category", "Accessories"],
      ["code", "CO2"],
      ["code", "COD"],
      ["code", "Co-op"],
      ["org", "Company"],
   ];
   let index: ValueIndex;
   beforeAll(() => {
      index = new ValueIndex(
         rows.map(([dimension, value]) => ({
            value,
            normalized: normalizeText(value),
            count: 1,
            dimension,
            origin: "data",
         })),
      );
      index.setVectors(new Map(index.distinctValues().map((v) => [v, vec(v)])));
   });
   const meaning = (phrase: string) =>
      index
         .searchByMeaning(phrase, vec(phrase))
         .map((m) => [m.dimension, m.value, m.score, m.how]);

   it("embeds each distinct value once", () => {
      expect(index.embedded).toBe(15);
   });

   it("matches an exact value at 1, and nothing more than MEANING_MARGIN below it", () => {
      // Colorado is 0.64 from Denver: a close meaning, but below 1 - 0.15.
      expect(meaning("Denver")).toEqual([["city", "Denver", 1, "meaning"]]);
   });

   it("matches a typo by meaning as well as by spelling", () => {
      expect(meaning("Calfornia")).toEqual([
         ["state", "California", 0.6347, "meaning"],
      ]);
   });

   it("matches a value with other words: NYC to New York, sneakers to Athletic Shoes", () => {
      // Neither shares a word or a three-letter run with the value.
      expect(index.search("NYC")).toEqual([]);
      expect(index.search("sneakers")).toEqual([]);
      expect(meaning("NYC")).toEqual([
         ["city", "New York", 0.8014, "meaning"],
         ["state", "New York", 0.8014, "meaning"],
      ]);
      expect(meaning("sneakers")).toEqual([
         ["category", "Athletic Shoes", 0.5839, "meaning"],
         ["category", "Sandals", 0.501, "meaning"],
      ]);
   });

   it("holds a short code to the higher floor: CO matches none of CO2, COD, Co-op or Colorado", () => {
      // Their cosines (0.61, 0.57, 0.54, 0.45) clear MEANING_FLOOR but not
      // MEANING_SHORT_FLOOR.
      expect(MEANING_SHORT_FLOOR).toBe(0.7);
      expect(meaning("CO")).toEqual([]);
   });

   it("nominates dimensions with the value's spread dividing its cosine, and respects allow", () => {
      expect(
         index.nominateByMeaning([{ phrase: "NYC", vec: vec("NYC") }]),
      ).toEqual([
         {
            dimension: "city",
            score: 0.4007,
            matches: [
               {
                  value: "New York",
                  dimension: "city",
                  score: 0.8014,
                  how: "meaning",
               },
            ],
         },
         {
            dimension: "state",
            score: 0.4007,
            matches: [
               {
                  value: "New York",
                  dimension: "state",
                  score: 0.8014,
                  how: "meaning",
               },
            ],
         },
      ]);
      expect(
         index
            .nominateByMeaning(
               [{ phrase: "NYC", vec: vec("NYC") }],
               (d) => d === "state",
            )
            .map((n) => n.dimension),
      ).toEqual(["state"]);
   });

   it("matches nothing without vectors or with a vector of another length", () => {
      const bare = new ValueIndex([
         {
            value: "New York",
            normalized: "new york",
            count: 1,
            dimension: "state",
            origin: "data",
         },
      ]);
      expect(bare.searchByMeaning("NYC", vec("NYC"))).toEqual([]);
      expect(index.searchByMeaning("NYC", new Float32Array(8))).toEqual([]);
   });
});

describe("value-looking tokens", () => {
   const vocab = new Set(["flight", "order", "ship", "status", "sale"]);
   const inVocab = (t: string) => vocab.has(t);

   it("takes a capitalized run that is not the first word", () => {
      expect(valueSpans("flights to Denver", inVocab)).toEqual(["Denver"]);
      expect(valueSpans("Orders shipped to New York", inVocab)).toEqual([
         "New York",
      ]);
   });

   it("takes quoted strings, codes and words the catalog never uses", () => {
      expect(valueSpans('orders with status "ixnayed"', inVocab)).toEqual([
         "ixnayed",
      ]);
      expect(valueSpans("sales of sku 4X12", inVocab)).toEqual(["4X12"]);
      expect(valueSpans("frobnicated orders", inVocab)).toEqual([
         "frobnicated",
      ]);
      expect(valueSpans("flights by status", inVocab)).toEqual([]);
   });
});
