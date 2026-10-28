// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   buildFcjIndexFixture,
   type FcjIndexFixture,
   CLUSTER_SETTINGS,
} from "../../test_helpers/fcj_index_fixture";
import { embedText } from "../../test_helpers/get_context_llm_harness";
import type { ResolvedRequest } from "../tools/get_context_types";
import { alsoLikelyFields, mentions, nameOfId } from "./also_likely";
import { connect } from "./connect";
import { find, KINDS_BY_FIND_TARGET, type FindTarget } from "./find";
import type { JudgeVerdict } from "./judge";
import { respond } from "./respond";
import { toUnit } from "./vectors";

const SETTINGS = {
   ...CLUSTER_SETTINGS,
   connect: "list" as const,
   listTopK: 15,
   collapseCopies: true,
};

let f: FcjIndexFixture;
beforeAll(async () => {
   f = await buildFcjIndexFixture(SETTINGS);
});
afterAll(async () => f.close());

const t = (
   index: number,
   type: FindTarget["type"],
   text: string,
): FindTarget => ({ index, type, text, kinds: KINDS_BY_FIND_TARGET[type] });

const TARGETS = [
   t(0, "measure", "number of flights"),
   t(1, "dimension", "start date"),
];

function stages(listTopK = SETTINGS.listTopK) {
   const queries = new Map(
      TARGETS.map((x) => [x.index, toUnit(embedText(x.text))]),
   );
   const found = find({
      index: f.index,
      targets: TARGETS,
      queries,
      perTarget: 30,
      keyword: true,
      values: true,
      allow: () => true,
   });
   const connected = connect({
      index: f.index,
      found,
      queries,
      settings: { ...SETTINGS, listTopK },
      allow: () => true,
   });
   return { found, connected };
}

const verdict = (
   home: string,
   picks: JudgeVerdict["picks"],
   caveats: string[] = [],
): JudgeVerdict[] => [{ cluster: home, picks, caveats }];

const FLIGHT_PICKS: JudgeVerdict["picks"] = [
   {
      target: 0,
      keys: ["measure:flights@travel.malloy:flight_count"],
      reason: "counts flights",
   },
   { target: 1, keys: ["dimension:flights@travel.malloy:start_date"] },
];

const NAMED_ARRIVES_AT = {
   key: "dimension:flights@travel.malloy:arrives_at",
   id: "dimension:flights:arrives_at",
   rule: "named" as const,
   reason: "The judge's note names arrives_at, and no pick carries that name.",
};
const TWIN_RACES = {
   key: "dimension:races@reports.malloy:start_date",
   id: "dimension:races:start_date",
   rule: "twin" as const,
   of: "dimension:flights:start_date",
   reason:
      "Same name and kind as the pick flights.start_date, on another source.",
};
const TWIN_ORDERS = {
   key: "dimension:orders@shop.malloy:start_date",
   id: "dimension:orders:start_date",
   rule: "twin" as const,
   of: "dimension:flights:start_date",
   reason:
      "Same name and kind as the pick flights.start_date, on another source.",
};

describe("also likely: names", () => {
   it("reads a field's name past its kind, source and join path", () => {
      expect(nameOfId("dimension:orders:customer.region.name")).toBe("name");
      expect(nameOfId("measure:orders:sales")).toBe("sales");
      expect(nameOfId("source:orders:orders")).toBe("orders");
   });

   it("finds a name as written or with spaces, never inside a longer word", () => {
      expect(mentions("filter on arrives_at.", "arrives_at")).toBe(true);
      expect(mentions("filter on arrives at for denver", "arrives_at")).toBe(
         true,
      );
      expect(
         mentions("filter on the arrives_at_utc column", "arrives_at"),
      ).toBe(false);
      expect(mentions("use fac_building_key", "FAC_BUILDING_KEY")).toBe(true);
      expect(mentions("a (b) c", "(b)")).toBe(true);
   });
});

describe("also likely: rule R", () => {
   it("adds the judge-named field first, then up to two same-named fields per pick, best Stage 1 rank first", () => {
      const { found, connected } = stages();
      const out = alsoLikelyFields({
         index: f.index,
         found,
         connected,
         twins: 2,
         verdicts: verdict(connected.clusters[0].home, FLIGHT_PICKS, [
            "Filter on arrives at for Denver.",
         ]),
      });
      expect(out).toEqual([NAMED_ARRIVES_AT, TWIN_RACES, TWIN_ORDERS]);
   });

   it("caps the twins per pick at the setting", () => {
      const { found, connected } = stages();
      const out = alsoLikelyFields({
         index: f.index,
         found,
         connected,
         twins: 1,
         verdicts: verdict(connected.clusters[0].home, FLIGHT_PICKS),
      });
      expect(out).toEqual([TWIN_RACES]);
      expect(
         alsoLikelyFields({
            index: f.index,
            found,
            connected,
            twins: 0,
            verdicts: verdict(connected.clusters[0].home, FLIGHT_PICKS),
         }),
      ).toEqual([]);
   });

   it("names only a field the judge was shown: a name ranked past the list is not added", () => {
      // arrives_at is 12th in Stage 1 for "start date": in a list of 15, not in a list of 4.
      const { found, connected } = stages(4);
      const out = alsoLikelyFields({
         index: f.index,
         found,
         connected,
         twins: 2,
         verdicts: verdict(connected.clusters[0].home, FLIGHT_PICKS, [
            "Filter on arrives at for Denver.",
         ]),
      });
      expect(out).toEqual([TWIN_RACES, TWIN_ORDERS]);
   });

   it("never adds a pick under another id, nor a name a pick already carries", () => {
      const { found, connected } = stages();
      const out = alsoLikelyFields({
         index: f.index,
         found,
         connected,
         twins: 2,
         verdicts: verdict(
            connected.clusters[0].home,
            [
               ...FLIGHT_PICKS,
               {
                  target: 1,
                  keys: ["dimension:races@reports.malloy:start_date"],
               },
            ],
            ["start_date of flights is the scheduled day."],
         ),
      });
      // races.start_date is picked; the twin of both picks is orders.start_date, once.
      expect(out).toEqual([TWIN_ORDERS]);
   });

   it("adds nothing without a pick", () => {
      const { found, connected } = stages();
      expect(
         alsoLikelyFields({
            index: f.index,
            found,
            connected,
            twins: 2,
            verdicts: verdict(connected.clusters[0].home, [], ["arrives at"]),
         }),
      ).toEqual([]);
   });
});

describe("also likely: in the response", () => {
   it("puts each one on its own source's card after the picks, marked with its reason", async () => {
      const { found, connected } = stages();
      const verdicts = verdict(connected.clusters[0].home, FLIGHT_PICKS, [
         "Filter on arrives at for Denver.",
      ]);
      const alsoLikely = alsoLikelyFields({
         index: f.index,
         found,
         connected,
         twins: 2,
         verdicts,
      });
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
      const out = await respond({
         index: f.index,
         request,
         pkgIndex: f.pkgIndex,
         connected,
         verdicts,
         targets: TARGETS,
         environmentStore: {} as never,
         alsoLikely,
      });
      const cards = out.sources as Array<{
         source_info: { resource_id: { source: string } };
         entities?: Array<{
            entity_id: string;
            also_likely?: true;
            also_likely_reason?: string;
            matched_targets?: unknown[];
         }>;
      }>;
      expect(
         cards.map((c) => [
            c.source_info.resource_id.source,
            (c.entities ?? []).map((e) =>
               e.also_likely
                  ? `${e.entity_id} (also likely: ${e.also_likely_reason})`
                  : e.entity_id,
            ),
         ]),
      ).toEqual([
         [
            "flights",
            [
               "measure:flights:flight_count",
               "dimension:flights:start_date",
               `dimension:flights:arrives_at (also likely: ${NAMED_ARRIVES_AT.reason})`,
            ],
         ],
         [
            "races",
            [`dimension:races:start_date (also likely: ${TWIN_RACES.reason})`],
         ],
         [
            "orders",
            [
               `dimension:orders:start_date (also likely: ${TWIN_ORDERS.reason})`,
            ],
         ],
      ]);
      // An added field answers no target, so it claims none.
      const races = cards[1].entities![0];
      expect(races.matched_targets).toBeUndefined();
      // The groups are the picks' only.
      expect(
         (out.clusters as Array<{ query_from: { source: string } }>).map(
            (c) => c.query_from.source,
         ),
      ).toEqual(["flights"]);
   });
});
