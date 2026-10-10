// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   DEFAULT_FCJ_SETTINGS,
   DEFAULT_PACKAGE_RETRIEVAL,
} from "../../service/package_retrieval";
import {
   FCJ_PAYMENTS_FILES,
   FCJ_PEERS_FILES,
} from "../../test_helpers/fcj_fixture";
import {
   buildFcjIndexFixture,
   type FcjIndexFixture,
} from "../../test_helpers/fcj_index_fixture";
import { embedText } from "../../test_helpers/get_context_llm_harness";
import type { ResolvedRequest } from "../tools/get_context_tool";
import type { CatalogNode } from "./catalog";
import { connect } from "./connect";
import { gatedValueWarning, unjudged, withPinnedDimensions } from "./engine";
import { find, KINDS_BY_FIND_TARGET, type FindTarget } from "./find";
import type { JudgeVerdict } from "./judge";
import { populationNote, queryGroups, respond } from "./respond";
import { ValueIndex } from "./values_index";
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

const request: ResolvedRequest = {
   pureSourceListing: false,
   environmentName: "env",
   packageName: "fixture",
   kinds: new Set(),
   searches: [],
   valueSearches: [],
   listingOnly: false,
   includeCode: false,
   unsupported: [],
   limit: 20,
   offset: 0,
};
void DEFAULT_PACKAGE_RETRIEVAL;

async function answer(
   targets: FindTarget[],
   pickFor?: (clusterHome: string) => JudgeVerdict["picks"],
   allow: (n: CatalogNode) => boolean = () => true,
   extra: Partial<Parameters<typeof respond>[0]> = {},
   fx: FcjIndexFixture = f,
   settings = DEFAULT_FCJ_SETTINGS,
) {
   const queries = new Map(
      targets.map((x) => [x.index, toUnit(embedText(x.text))]),
   );
   const req = (extra.request as ResolvedRequest | undefined) ?? request;
   const found = withPinnedDimensions(
      fx.index,
      find({
         index: fx.index,
         targets,
         queries,
         perTarget: 30,
         keyword: true,
         values: true,
         allow,
      }),
      req,
      allow,
   );
   const connected = connect({
      index: fx.index,
      found,
      queries,
      settings,
      allow,
   });
   const verdicts: JudgeVerdict[] = pickFor
      ? connected.clusters.map((c) => ({
           cluster: c.home,
           picks: pickFor(c.home),
           caveats: [],
        }))
      : unjudged(connected, targets);
   return respond({
      index: fx.index,
      request,
      pkgIndex: fx.pkgIndex,
      connected,
      verdicts,
      targets,
      environmentStore: {} as never,
      ...extra,
   });
}

type Card = {
   source_info: {
      resource_id: { source: string; model_path: string };
      docs?: string;
   };
   entities?: Array<{
      entity_id: string;
      join_path?: string;
      description?: string;
      values?: unknown[];
      matched_targets?: Array<{ search_text: string; match_reason?: string }>;
   }>;
};
const ids = (sources: unknown[]) =>
   (sources as Card[]).flatMap((s) =>
      (s.entities ?? []).map((e) => e.entity_id),
   );

describe("respond", () => {
   it("presents a joined pick on the home's card under every route, each with the population note", async () => {
      const out = await answer(
         [
            t(0, "dimension", "region name of the arrival airport"),
            t(1, "measure", "number of flights"),
         ],
         () => [
            {
               target: 0,
               keys: ["dimension:regions@travel.malloy:region_name"],
            },
            {
               target: 1,
               keys: ["measure:flights@travel.malloy:flight_count"],
               reason: "counts flights",
            },
         ],
      );
      const card = (out.sources as Card[])[0];
      expect(card.source_info.resource_id).toEqual({
         environment: "env",
         package: "fixture",
         model_path: "travel.malloy",
         source: "flights",
      } as never);
      expect(ids(out.sources)).toEqual([
         "dimension:flights:arrival.region.region_name",
         "dimension:flights:origin.region.region_name",
         "measure:flights:flight_count",
      ]);
      expect(card.entities![0].join_path).toBe("arrival.region");
      expect(card.entities![0].description).toBe(
         populationNote("flights", ["arrival", "region"]),
      );
      expect(card.entities![2].matched_targets).toEqual([
         {
            search_text: "number of flights",
            relevance: 1,
            match_reason: "counts flights",
         } as never,
      ]);
      expect(out.clusters).toEqual([
         {
            home: { source: "flights", model_path: "travel.malloy" },
            reach: [
               { source: "airports", joins: ["arrival", "origin"] },
               {
                  source: "regions",
                  joins: ["arrival.region", "origin.region"],
               },
            ],
            covers: [0, 1],
            picks: {
               0: [
                  "dimension:flights:arrival.region.region_name",
                  "dimension:flights:origin.region.region_name",
               ],
               1: ["measure:flights:flight_count"],
            },
         },
      ]);
      expect(out.coverage).toBeUndefined();
      expect(out.unresolved_targets).toEqual([]);
   });

   it("nests matched values under their dimension", async () => {
      const out = await answer([
         t(0, "measure", "how many flights"),
         t(1, "dimension", "flights to Denver"),
      ]);
      const arrives = (out.sources as Card[])[0].entities!.find(
         (e) => e.entity_id === "dimension:flights:arrives_at",
      )!;
      expect(arrives.values).toEqual([
         {
            value: "Denver International (DIA)",
            relevance: 0.85,
            search_text: "flights to Denver",
         },
      ]);
   });

   it("gives a coverage table for several clusters and names a target no cluster answered", async () => {
      const out = await answer(
         [
            t(0, "measure", "total sales"),
            t(1, "measure", "number of races"),
            t(2, "measure", "number of flights"),
         ],
         (home) =>
            home === "races@reports.malloy"
               ? []
               : home === "flights@travel.malloy"
                 ? [
                      {
                         target: 2,
                         keys: ["measure:flights@travel.malloy:flight_count"],
                      },
                   ]
                 : [
                      {
                         target: 0,
                         keys: [
                            "measure:orders@shop.malloy:sales",
                            "measure:orders_public@shop.malloy:sales",
                         ],
                      },
                   ],
      );
      expect(out.coverage).toEqual([
         {
            target_index: 0,
            search_text: "total sales",
            clusters: [false, true, false],
         },
         {
            target_index: 1,
            search_text: "number of races",
            clusters: [false, false, true],
         },
         {
            target_index: 2,
            search_text: "number of flights",
            clusters: [true, false, false],
         },
      ]);
      expect(out.unresolved_targets).toEqual([
         {
            target_index: 1,
            target_type: "measure",
            search_text: "number of races",
         },
      ]);
      // The twin's pick is on the twin's own card.
      expect(ids(out.sources)).toEqual([
         "measure:flights:flight_count",
         "measure:orders:sales",
         "measure:orders_public:sales",
      ]);
   });

   it("asks the query path, with the caller's givens, for a value on a gated source", async () => {
      const cities = f.index.catalog.sources.get("cities@travel.malloy")!;
      const values = f.index.values;
      cities.gated = true;
      // A gated source's values are never in the shared index.
      f.index.values = new ValueIndex(
         values.values.filter((v) => !v.dimension.includes("cities@")),
      );
      const asked: unknown[] = [];
      try {
         const out = await answer(
            [t(0, "dimensional_value", "denver")],
            () => [
               { target: 0, keys: ["dimension:cities@travel.malloy:city"] },
            ],
            (n) =>
               n.source === "cities" &&
               (n.kind === "source" || n.name === "city"),
            {
               request: {
                  ...request,
                  sourceName: "cities",
                  entityName: "city",
                  givens: { TENANT: "a" },
               },
               valueFetcher: async (req) => {
                  asked.push([req.source, req.dimension, req.givens]);
                  return { values: ["Denver", "Boulder"], truncated: false };
               },
            },
         );
         expect(asked).toEqual([["cities", "city", { TENANT: "a" }]]);
         const city = (out.sources as Card[])[0].entities![0];
         expect(city.values).toEqual([
            { value: "Denver", relevance: 0.98, search_text: "denver" },
         ]);
      } finally {
         cities.gated = false;
         f.index.values = values;
      }
   });

   it('with connect: "list", puts each pick on its own source\'s card, and two sources no join links are two groups', async () => {
      const out = await answer(
         [
            t(0, "measure", "number of flights"),
            t(1, "measure", "number of races"),
         ],
         () => [
            {
               target: 0,
               keys: ["measure:flights@travel.malloy:flight_count"],
            },
            { target: 1, keys: ["measure:races@reports.malloy:race_count"] },
         ],
         () => true,
         {},
         f,
         { ...DEFAULT_FCJ_SETTINGS, connect: "list" },
      );
      expect(
         (out.sources as Card[]).map((c) => c.source_info.resource_id.source),
      ).toEqual(["flights", "races"]);
      expect(ids(out.sources)).toEqual([
         "measure:flights:flight_count",
         "measure:races:race_count",
      ]);
      expect(out.clusters).toEqual([
         {
            query_from: { source: "flights", model_path: "travel.malloy" },
            sources: [{ source: "flights", joins: [] }],
            covers: [0],
            picks: { 0: ["measure:flights:flight_count"] },
         },
         {
            query_from: { source: "races", model_path: "reports.malloy" },
            sources: [{ source: "races", joins: [] }],
            covers: [1],
            picks: { 1: ["measure:races:race_count"] },
         },
      ]);
      expect(out.coverage).toEqual([
         {
            target_index: 0,
            search_text: "number of flights",
            clusters: [true, false],
         },
         {
            target_index: 1,
            search_text: "number of races",
            clusters: [false, true],
         },
      ]);
      expect(out.unresolved_targets).toEqual([]);
   });

   it("says which gated sources a value search with no pinned dimension skipped", () => {
      const cities = f.index.catalog.sources.get("cities@travel.malloy")!;
      cities.gated = true;
      try {
         const targets = [t(0, "dimensional_value", "Denver")];
         expect(gatedValueWarning(f.index, request, targets, () => true)).toBe(
            "The values of 1 source(s) with an access gate were not searched (cities). To search one, scope the target to its dimension (scopes[].source and entity_name) and send the givens its gate needs.",
         );
         expect(
            gatedValueWarning(
               f.index,
               { ...request, sourceName: "cities", entityName: "city" },
               targets,
               () => true,
            ),
         ).toBeUndefined();
         expect(
            gatedValueWarning(
               f.index,
               request,
               [t(0, "dimension", "city")],
               () => true,
            ),
         ).toBeUndefined();
      } finally {
         cities.gated = false;
      }
   });
});

describe("respond, a picked source the home reaches by a join", () => {
   let p: FcjIndexFixture;
   beforeAll(async () => {
      p = await buildFcjIndexFixture(DEFAULT_FCJ_SETTINGS, FCJ_PAYMENTS_FILES);
   });
   afterAll(async () => p.close());

   it("returns it as its own card and leaves the home card's doc alone", async () => {
      const out = await answer(
         [
            t(0, "source", "merchant account profile"),
            t(1, "dimension", "account type"),
            t(2, "measure", "total volume in euros"),
         ],
         () => [
            {
               target: 0,
               keys: ["source:merchants@payments.malloy:merchants"],
            },
            {
               target: 1,
               keys: ["dimension:merchants@payments.malloy:account_kind"],
            },
            {
               target: 2,
               keys: ["measure:payments@payments.malloy:total_volume_eur"],
            },
         ],
         (n) => n.source === "payments" || n.source === "merchants",
         {},
         p,
      );
      expect(
         (out.sources as Card[]).map((c) => [
            c.source_info.resource_id.source,
            c.source_info.docs,
         ]),
      ).toEqual([
         ["payments", "One row per payment transaction."],
         [
            "merchants",
            "One row per merchant, 30 in all, carrying the account characteristics.",
         ],
      ]);
      // A field of the joined source still rides on the home's card.
      expect(ids(out.sources)).toEqual([
         "dimension:payments:merchant_profile.account_kind",
         "measure:payments:total_volume_eur",
      ]);
      expect(
         (out.clusters as Array<{ home: unknown; picks: unknown }>).map((c) => [
            c.home,
            c.picks,
         ]),
      ).toEqual([
         [
            { source: "payments", model_path: "payments.malloy" },
            {
               0: ["source:merchants:merchants"],
               1: ["dimension:payments:merchant_profile.account_kind"],
               2: ["measure:payments:total_volume_eur"],
            },
         ],
      ]);
   });
});

describe('respond, connect: "list"', () => {
   let p: FcjIndexFixture;
   beforeAll(async () => {
      p = await buildFcjIndexFixture(DEFAULT_FCJ_SETTINGS, FCJ_PEERS_FILES);
   });
   afterAll(async () => p.close());

   it("puts a joined source's pick on its own card, and groups the picks one query can combine after the judge", async () => {
      const targets = [
         t(0, "measure", "total revenue"),
         t(1, "dimension", "venue city"),
      ];
      const out = await answer(
         targets,
         undefined,
         () => true,
         {
            verdicts: [
               {
                  // The list's id: the owner of the measure's best candidate.
                  cluster: "campaign_summary@bars.malloy",
                  picks: [
                     {
                        target: 0,
                        keys: [
                           "measure:sales_mix@bars.malloy:total_revenue",
                           "measure:campaign_summary@bars.malloy:total_revenue_lift",
                        ],
                     },
                     {
                        target: 1,
                        keys: ["dimension:venues@bars.malloy:venue_city"],
                     },
                  ],
                  caveats: [
                     "total_revenue_lift covers each campaign's window only.",
                  ],
               },
            ],
         },
         p,
         { ...DEFAULT_FCJ_SETTINGS, connect: "list", listTopK: 3 },
      );
      // No home card: three sources, each holding only what was picked on it.
      expect(
         (out.sources as Card[]).map((c) => [
            c.source_info.resource_id.source,
            (c.entities ?? []).map((e) => e.entity_id),
         ]),
      ).toEqual([
         ["sales_mix", ["measure:sales_mix:total_revenue"]],
         ["campaign_summary", ["measure:campaign_summary:total_revenue_lift"]],
         ["venues", ["dimension:venues:venue_city"]],
      ]);
      expect(out.clusters).toEqual([
         {
            query_from: { source: "sales_mix", model_path: "bars.malloy" },
            sources: [
               { source: "sales_mix", joins: [] },
               { source: "venues", joins: ["venue"] },
            ],
            covers: [0, 1],
            picks: {
               0: ["measure:sales_mix:total_revenue"],
               1: ["dimension:venues:venue_city"],
            },
            caveats: ["total_revenue_lift covers each campaign's window only."],
         },
         {
            query_from: {
               source: "campaign_summary",
               model_path: "bars.malloy",
            },
            sources: [{ source: "campaign_summary", joins: [] }],
            covers: [0],
            picks: { 0: ["measure:campaign_summary:total_revenue_lift"] },
         },
      ]);
      expect(out.coverage).toEqual([
         {
            target_index: 0,
            search_text: "total revenue",
            clusters: [true, true],
         },
         {
            target_index: 1,
            search_text: "venue city",
            clusters: [true, false],
         },
      ]);
   });

   it("with no pick, returns no group and keeps the judge's caveats as warnings", async () => {
      const targets = [t(0, "measure", "total revenue")];
      const out = await answer(
         targets,
         undefined,
         () => true,
         {
            verdicts: [
               {
                  cluster: "campaign_summary@bars.malloy",
                  picks: [{ target: 0, keys: [] }],
                  caveats: ["No listed source has revenue for August."],
               },
            ],
         },
         p,
         { ...DEFAULT_FCJ_SETTINGS, connect: "list" },
      );
      expect([out.sources, out.clusters, out.coverage, out.warnings]).toEqual([
         [],
         [],
         undefined,
         ["No listed source has revenue for August."],
      ]);
      expect(out.unresolved_targets).toEqual([
         {
            target_index: 0,
            target_type: "measure",
            search_text: "total revenue",
         },
      ]);
   });
});

describe("queryGroups", () => {
   const reach = (source: string, ...paths: string[][]) => ({
      source,
      hops: paths[0].length,
      paths,
   });

   it("names an unpicked source when it is the one query that combines two picked ones", () => {
      const peers = [
         { source: "orders", reaches: [] },
         { source: "products", reaches: [] },
         {
            source: "order_items",
            reaches: [
               reach("orders", ["order"]),
               reach("products", ["product"]),
            ],
         },
      ];
      expect(queryGroups(peers, ["orders", "products"])).toEqual([
         {
            root: "order_items",
            members: [
               { source: "orders", paths: [["order"]] },
               { source: "products", paths: [["product"]] },
            ],
         },
      ]);
      // An unpicked source that combines only one picked source is not a group.
      expect(queryGroups(peers, ["orders"])).toEqual([
         { root: "orders", members: [{ source: "orders", paths: [[]] }] },
      ]);
   });

   it("prefers a picked root over an unpicked one that combines the same sources", () => {
      const peers = [
         { source: "hub", reaches: [reach("a", ["a"]), reach("b", ["b"])] },
         { source: "a", reaches: [reach("b", ["to_b"])] },
         { source: "b", reaches: [] },
      ];
      expect(queryGroups(peers, ["a", "b"])).toEqual([
         {
            root: "a",
            members: [
               { source: "a", paths: [[]] },
               { source: "b", paths: [["to_b"]] },
            ],
         },
      ]);
   });
});
