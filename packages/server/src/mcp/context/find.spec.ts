// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   buildFcjIndexFixture,
   type FcjIndexFixture,
} from "../../test_helpers/fcj_index_fixture";
import { embedText } from "../../test_helpers/get_context_llm_harness";
import {
   find,
   fuseByRank,
   KINDS_BY_FIND_TARGET,
   mentionsPeriod,
   valuePhrasesOf,
   type FindTarget,
} from "./find";
import { routesOf } from "./routes";
import { toUnit } from "./vectors";

let f: FcjIndexFixture;
beforeAll(async () => {
   f = await buildFcjIndexFixture();
});
afterAll(async () => f.close());

const target = (
   index: number,
   type: FindTarget["type"],
   text: string,
): FindTarget => ({ index, type, text, kinds: KINDS_BY_FIND_TARGET[type] });

const run = (targets: FindTarget[], allow = () => true) =>
   find({
      index: f.index,
      targets,
      queries: new Map(
         targets.map((t) => [t.index, toUnit(embedText(t.text))]),
      ),
      perTarget: 30,
      keyword: true,
      values: true,
      allow,
   });

const top = (r: ReturnType<typeof run>[number], n: number) =>
   r.candidates.slice(0, n).map((c) => c.key);

describe("reciprocal rank fusion", () => {
   it("adds 1 / (60 + rank) per list and breaks ties by key", () => {
      const fused = fuseByRank([["a", "b"], ["b", "c"], []]);
      expect(
         fused.map((x) => [x.key, Number(x.score.toFixed(6)), x.ranks]),
      ).toEqual([
         ["b", Number((1 / 62 + 1 / 61).toFixed(6)), [2, 1, 0]],
         ["a", Number((1 / 61).toFixed(6)), [1, 0, 0]],
         ["c", Number((1 / 62).toFixed(6)), [0, 2, 0]],
      ]);
   });
});

describe("find", () => {
   it("finds all four start dates, and nothing ranks them apart but text", () => {
      const [r] = run([target(1, "dimension", "start date")]);
      expect(top(r, 4)).toEqual([
         "dimension:flights@travel.malloy:start_date",
         "dimension:races@reports.malloy:start_date",
         "dimension:orders@shop.malloy:start_date",
         "dimension:orders_public@shop.malloy:start_date",
      ]);
      expect(r.candidates.length).toBe(30);
      expect(r.candidates[0].ranks).toEqual({ close: 1, keyword: 1 });
   });

   it("lets a value nominate a dimension: Denver puts both city and arrives_at on the list", () => {
      const [r] = run([target(2, "dimension", "flights to Denver")]);
      expect(r.valuePhrases).toEqual(["Denver"]);
      expect(r.lists).toEqual({ close: 30, keyword: 6, value: 2 });
      const arrives = r.candidates[0];
      expect(arrives.key).toBe("dimension:flights@travel.malloy:arrives_at");
      expect(arrives.ranks).toEqual({ close: 5, keyword: 1, value: 2 });
      expect(arrives.values?.map((v) => v.value)).toEqual([
         "Denver International (DIA)",
      ]);
      const city = r.candidates.find(
         (c) => c.key === "dimension:cities@travel.malloy:city",
      )!;
      expect(r.candidates.indexOf(city)).toBe(5);
      expect(city.ranks).toEqual({ close: 25, value: 1 });
      expect(city.valueScore).toBe(1);
   });

   it("answers a dimensional_value target from values and keywords only", () => {
      const [r] = run([target(3, "dimensional_value", "Denver")]);
      expect(r.lists).toEqual({ close: 0, keyword: 0, value: 2 });
      expect(top(r, 2)).toEqual([
         "dimension:cities@travel.malloy:city",
         "dimension:flights@travel.malloy:arrives_at",
      ]);
   });

   it("keeps a source imported into two files as one candidate", () => {
      const [r] = run([target(4, "measure", "total sales")]);
      const ids = r.candidates.map((c) => f.index.catalog.nodes.get(c.key)!.id);
      expect(ids.filter((id) => id === "measure:orders:sales")).toHaveLength(1);
      expect(new Set(ids).size).toBe(ids.length);
   });

   it("cuts at N and stays inside the scope", () => {
      const [r] = find({
         index: f.index,
         targets: [target(1, "dimension", "start date")],
         queries: new Map([[1, toUnit(embedText("start date"))]]),
         perTarget: 2,
         keyword: true,
         values: true,
         allow: (n) => n.source === "races",
      });
      expect(top(r, 5)).toEqual([
         "dimension:races@reports.malloy:start_date",
         "dimension:races@reports.malloy:city",
      ]);
   });

   it("ranks by keyword alone with no embedding provider", () => {
      const [r] = find({
         index: f.index,
         targets: [target(0, "measure", "number of flights")],
         queries: new Map(),
         perTarget: 30,
         keyword: true,
         values: true,
         allow: () => true,
      });
      expect(r.lists.close).toBe(0);
      expect(top(r, 1)).toEqual(["measure:flights@travel.malloy:flight_count"]);
   });
});

describe("helpers", () => {
   it("prefers the caller's example values over the token rule", () => {
      expect(
         valuePhrasesOf(
            {
               ...target(2, "dimension", "flights to Denver"),
               exampleValues: ["DIA"],
            },
            () => true,
         ),
      ).toEqual(["DIA"]);
      expect(
         valuePhrasesOf(target(0, "measure", "flights to Denver"), () => false),
      ).toEqual([]);
   });

   it("names every route to a field, two joins deep", () => {
      const node = f.index.catalog.nodes.get(
         "dimension:airports@travel.malloy:airport_name",
      )!;
      expect(routesOf(f.index.catalog, node)).toEqual([
         "dimension:flights:arrival.airport_name",
         "dimension:flights:origin.airport_name",
         "dimension:legs:flight.arrival.airport_name",
         "dimension:legs:flight.origin.airport_name",
      ]);
   });

   it("spots a period in a phrase", () => {
      expect(mentionsPeriod("flights per month")).toBe(true);
      expect(mentionsPeriod("sales in 2024")).toBe(true);
      expect(mentionsPeriod("flights to Denver")).toBe(false);
   });
});
