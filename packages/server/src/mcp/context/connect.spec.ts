// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { DEFAULT_FCJ_SETTINGS } from "../../service/package_retrieval";
import {
   buildFcjIndexFixture,
   type FcjIndexFixture,
} from "../../test_helpers/fcj_index_fixture";
import { embedText } from "../../test_helpers/get_context_llm_harness";
import type { CatalogNode } from "./catalog";
import { connect } from "./connect";
import { find, KINDS_BY_FIND_TARGET, type FindTarget } from "./find";
import { toUnit } from "./vectors";

let f: FcjIndexFixture;
beforeAll(async () => {
   f = await buildFcjIndexFixture();
});
afterAll(async () => f.close());

const t = (
   index: number,
   type: FindTarget["type"],
   text: string,
): FindTarget => ({ index, type, text, kinds: KINDS_BY_FIND_TARGET[type] });

function run(
   targets: FindTarget[],
   allow: (n: CatalogNode) => boolean = () => true,
   settings = DEFAULT_FCJ_SETTINGS,
) {
   const queries = new Map(
      targets.map((x) => [x.index, toUnit(embedText(x.text))]),
   );
   const found = find({
      index: f.index,
      targets,
      queries,
      perTarget: settings.perTarget,
      keyword: true,
      values: true,
      allow,
   });
   return connect({ index: f.index, found, queries, settings, allow });
}

const CONTENDERS_OFF: Array<[string, string[]]> = [
   ["cities@travel.malloy", []],
];
const CONTENDERS_ON: Array<[string, string[]]> = [
   [
      "cities@travel.malloy",
      ["races@reports.malloy", "flights@travel.malloy", "orders@shop.malloy"],
   ],
];
const homes = (out: ReturnType<typeof run>) =>
   out.clusters.map((c) => [c.home, c.covers, c.twins]);
const keys = (out: ReturnType<typeof run>, cluster: number, target: number) =>
   (out.clusters[cluster].candidates.get(target) ?? []).map((c) => c.key);

describe("connect", () => {
   it("picks the start_date that sits beside the measure asked for", () => {
      const out = run([
         t(0, "measure", "number of flights"),
         t(1, "dimension", "start date"),
      ]);
      expect(homes(out)).toEqual([["flights@travel.malloy", [0, 1], []]]);
      expect(keys(out, 0, 1)[0]).toBe(
         "dimension:flights@travel.malloy:start_date",
      );
      expect(out.unresolved).toEqual([]);
      // legs reaches the same fields one join away, so it scores 0.9 of flights.
      const [first, second] = out.considered;
      expect(first.source).toBe("flights@travel.malloy");
      expect(second.source).toBe("legs@travel.malloy");
      expect(Number((second.score / first.score).toFixed(4))).toBe(0.9);
   });

   it("prefers DIA beside the flights measure to an exact Denver in a table no flights measure reaches", () => {
      const out = run([
         t(0, "measure", "how many flights"),
         t(1, "dimension", "flights to Denver"),
      ]);
      expect(homes(out)).toEqual([["flights@travel.malloy", [0, 1], []]]);
      expect(keys(out, 0, 1)[0]).toBe(
         "dimension:flights@travel.malloy:arrives_at",
      );
      const cities = out.considered.find(
         (s) => s.source === "cities@travel.malloy",
      )!;
      expect(cities.covers).toEqual([1]);
      expect(cities.best.get(1)?.key).toBe(
         "dimension:cities@travel.malloy:city",
      );
      expect(cities.score).toBeLessThan(out.clusters[0].score);
   });

   it("answers three measures in three islands with three clusters", () => {
      const out = run([
         t(0, "measure", "total sales"),
         t(1, "measure", "number of races"),
         t(2, "measure", "number of flights"),
      ]);
      expect(homes(out)).toEqual([
         ["flights@travel.malloy", [2], []],
         ["orders_public@shop.malloy", [0], ["orders@shop.malloy"]],
         ["races@reports.malloy", [1], []],
      ]);
      expect(keys(out, 2, 1)).toEqual([
         "measure:races@reports.malloy:race_count",
      ]);
   });

   it("keeps a twin in the home's cluster instead of dropping it for being second", () => {
      const out = run([t(0, "measure", "net sales")]);
      expect(homes(out)).toEqual([
         ["orders_public@shop.malloy", [0], ["orders@shop.malloy"]],
      ]);
      expect(keys(out, 0, 0).slice(0, 2)).toEqual([
         "measure:orders_public@shop.malloy:net_sales",
         "measure:orders@shop.malloy:net_sales",
      ]);
   });

   it("adds a dependent measure's inputs and the home's dates, each with its reason", () => {
      const net = run(
         [t(0, "measure", "net sales")],
         (n) => n.source === "orders",
      );
      expect(net.clusters[0].extras).toEqual([
         { key: "source:orders@shop.malloy:orders", reason: "the home source" },
         {
            key: "dimension:orders@shop.malloy:is_fulfilled",
            reason: "net_sales is computed from it",
         },
         {
            key: "dimension:orders@shop.malloy:amount",
            reason: "sales is computed from it",
         },
      ]);
      const monthly = run([t(0, "measure", "flights per month")]);
      expect(monthly.clusters[0].extras).toEqual([
         {
            key: "source:flights@travel.malloy:flights",
            reason: "the home source",
         },
         {
            key: "dimension:flights@travel.malloy:start_date",
            reason: "a date of the home source, and a target names a period",
         },
      ]);
   });

   it("never offers a field an extension hides", () => {
      const out = run(
         [t(0, "dimension", "customer email")],
         (n) => n.source === "orders_public",
      );
      const all = [...out.clusters[0].candidates.values()]
         .flat()
         .map((c) => c.key);
      expect(all.some((k) => k.endsWith(":customer_email"))).toBe(false);
      expect(out.clusters[0].home).toBe("orders_public@shop.malloy");
   });

   it("reaches a field two joins away and discounts it per hop", () => {
      const out = run([
         t(0, "dimension", "region name of the arrival airport"),
         t(1, "measure", "number of flights"),
      ]);
      expect(out.clusters[0].home).toBe("flights@travel.malloy");
      const region = out.clusters[0].candidates
         .get(0)!
         .find((c) => c.key === "dimension:regions@travel.malloy:region_name");
      expect(region?.hops).toBe(2);
      expect(region!.score).toBeCloseTo(region!.found.score * 0.81, 10);
   });

   it("stops at maxClusters and reports what is left unresolved", () => {
      const out = run(
         [
            t(0, "measure", "total sales"),
            t(1, "measure", "number of races"),
            t(2, "measure", "number of flights"),
         ],
         () => true,
         { ...DEFAULT_FCJ_SETTINGS, maxClusters: 1 },
      );
      expect(out.clusters.map((c) => c.home)).toEqual([
         "flights@travel.malloy",
      ]);
      expect(out.unresolved).toEqual([0, 1]);
   });

   it("with the provisional contenderShare, takes near-tied sources with other fields into the home's cluster", () => {
      const targets = [t(0, "measure", "count of rows")];
      const off = run(targets);
      expect(off.clusters.map((c) => [c.home, c.contenders])).toEqual(
         CONTENDERS_OFF,
      );
      const on = run(targets, () => true, {
         ...DEFAULT_FCJ_SETTINGS,
         contenderShare: 0.9,
      });
      expect(on.clusters.map((c) => [c.home, c.contenders])).toEqual(
         CONTENDERS_ON,
      );
   });

   it("with the provisional collapseCopies, an extension's copy is not a second home", async () => {
      const folded = await buildFcjIndexFixture({
         ...DEFAULT_FCJ_SETTINGS,
         collapseCopies: true,
      });
      try {
         const targets = [t(0, "measure", "net sales")];
         const queries = new Map([[0, toUnit(embedText("net sales"))]]);
         const found = find({
            index: folded.index,
            targets,
            queries,
            perTarget: 30,
            keyword: true,
            values: true,
            allow: () => true,
         });
         expect(
            found[0].candidates.some((c) => c.key.includes("orders_public@")),
         ).toBe(false);
         const out = connect({
            index: folded.index,
            found,
            queries,
            settings: DEFAULT_FCJ_SETTINGS,
            allow: () => true,
         });
         expect(out.clusters.map((c) => [c.home, c.twins])).toEqual([
            ["orders@shop.malloy", []],
         ]);
      } finally {
         await folded.close();
      }
   });

   it("with the provisional collapseCopies, an extension wins when its own field counts and reaches the inherited one", async () => {
      const folded = await buildFcjIndexFixture({
         ...DEFAULT_FCJ_SETTINGS,
         collapseCopies: true,
      });
      try {
         const targets = [
            t(0, "measure", "net sales"),
            t(1, "dimension", "sold through a reseller"),
         ];
         const queries = new Map(
            targets.map((x) => [x.index, toUnit(embedText(x.text))]),
         );
         const found = find({
            index: folded.index,
            targets,
            queries,
            perTarget: 30,
            keyword: true,
            values: true,
            allow: () => true,
         });
         const out = connect({
            index: folded.index,
            found,
            queries,
            settings: DEFAULT_FCJ_SETTINGS,
            allow: () => true,
         });
         expect(out.clusters.map((c) => [c.home, c.covers])).toEqual([
            ["orders_public@shop.malloy", [0, 1]],
         ]);
         // The inherited measure is shown under the extension's own name.
         expect(out.clusters[0].candidates.get(0)![0].key).toBe(
            "measure:orders_public@shop.malloy:net_sales",
         );
         expect(out.clusters[0].candidates.get(1)![0].key).toBe(
            "dimension:orders_public@shop.malloy:is_reseller",
         );
      } finally {
         await folded.close();
      }
   });
});
