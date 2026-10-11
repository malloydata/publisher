// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { DEFAULT_FCJ_SETTINGS } from "../../service/package_retrieval";
import {
   buildFcjIndexFixture,
   type FcjIndexFixture,
} from "../../test_helpers/fcj_index_fixture";
import { embedText } from "../../test_helpers/get_context_llm_harness";
import type { ResolvedRequest, SourceCard } from "../tools/get_context_tool";
import { connect } from "./connect";
import { unjudged } from "./engine";
import { find, KINDS_BY_FIND_TARGET, type FindTarget } from "./find";
import {
   HeldResults,
   pageAt,
   resultKey,
   shrinkCard,
   type HeldResult,
} from "./pages";
import { respond } from "./respond";
import { toUnit } from "./vectors";

let f: FcjIndexFixture;
beforeAll(async () => {
   f = await buildFcjIndexFixture();
});
afterAll(async () => f.close());

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

/** Fifteen targets over every island of the fixture. */
const FIFTEEN: Array<[FindTarget["type"], string]> = [
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
];

/** The complete answer for the fifteen targets, with no judge (deterministic). */
async function completeAnswer() {
   const targets = FIFTEEN.map(([type, text], index) => ({
      index,
      type,
      text,
      kinds: KINDS_BY_FIND_TARGET[type],
   }));
   const queries = new Map(
      targets.map((x) => [x.index, toUnit(embedText(x.text))]),
   );
   const allow = () => true;
   const found = find({
      index: f.index,
      targets,
      queries,
      perTarget: 30,
      keyword: true,
      values: true,
      allow,
   });
   const connected = connect({
      index: f.index,
      found,
      queries,
      settings: DEFAULT_FCJ_SETTINGS,
      allow,
   });
   const verdicts = unjudged(connected, targets);
   const answer = await respond({
      index: f.index,
      request,
      pkgIndex: f.pkgIndex,
      connected,
      verdicts,
      targets,
      environmentStore: {} as never,
   });
   return { answer, verdicts };
}

const ids = (cards: readonly SourceCard[]) =>
   cards.flatMap((c) => (c.entities ?? []).map((e) => e.entity_id));
const size = (cards: readonly SourceCard[]) => JSON.stringify(cards).length;

describe("pageAt over a complete fifteen-target answer", () => {
   it("splits a result longer than one page into pages that together are the whole result", async () => {
      const { answer, verdicts } = await completeAnswer();
      const cards = answer.sources;
      expect(cards.length).toBeGreaterThan(2);
      // Every unjudged pick is in the complete answer: respond cuts nothing.
      const picks = verdicts.reduce(
         (n, v) => n + v.picks.reduce((m, p) => m + p.keys.length, 0),
         0,
      );
      expect(picks).toBeGreaterThan(15);
      // A budget that holds the first half of the cards, and the rest, but
      // not all of them: exactly two pages.
      const half = Math.ceil(cards.length / 2);
      const room = Math.max(
         size(cards.slice(0, half)),
         size(cards.slice(half)),
      );
      expect(room).toBeLessThan(size(cards));
      const maxChars = room + 10;
      const options = { maxChars, reserveChars: 10, limit: 150 };
      const one = pageAt(cards, 0, options);
      expect(one.nextOffset).toBe(one.cards.length);
      expect(size(one.cards)).toBeLessThanOrEqual(maxChars - 10);
      const two = pageAt(cards, one.nextOffset!, options);
      expect(two.nextOffset).toBeUndefined();
      expect([...one.cards, ...two.cards]).toEqual(cards);
      expect(ids([...one.cards, ...two.cards])).toEqual(ids(cards));
      expect(one.trimmed + two.trimmed).toBe(0);
   });

   it("ends a page at `limit` cards, and a page past the end is empty with no next_offset", async () => {
      const { answer } = await completeAnswer();
      const cards = answer.sources;
      const options = { maxChars: 1_000_000, reserveChars: 0, limit: 2 };
      const seen: SourceCard[] = [];
      let offset: number | undefined = 0;
      let pages = 0;
      while (offset !== undefined) {
         const page = pageAt(cards, offset, options);
         expect(page.cards.length).toBeLessThanOrEqual(2);
         seen.push(...page.cards);
         offset = page.nextOffset;
         pages += 1;
      }
      expect(pages).toBe(Math.ceil(cards.length / 2));
      expect(seen).toEqual(cards);
      const past = pageAt(cards, cards.length + 3, options);
      expect(past.cards).toEqual([]);
      expect(past.nextOffset).toBeUndefined();
   });
});

/** A card with `n` entities, each with a description of `docLength` characters. */
function bigCard(name: string, n: number, docLength: number): SourceCard {
   return {
      source_info: {
         resource_id: {
            environment: "env",
            package: "fixture",
            model_path: "m.malloy",
            source: name,
         },
         docs: "d".repeat(docLength),
         joins: [],
      },
      entities: Array.from({ length: n }, (_, i) => ({
         name: `field_${i}`,
         entity_type: "dimension",
         entity_id: `dimension:${name}:field_${i}`,
         description: `${"word ".repeat(docLength / 5)}`.trim(),
      })),
   } as SourceCard;
}

describe("pageAt, a card bigger than a page", () => {
   it("puts it on a page alone and shortens long descriptions, never an entity", () => {
      const small = bigCard("small", 2, 50);
      const huge = bigCard("huge", 40, 2_000);
      const after = bigCard("after", 2, 50);
      const cards = [small, huge, after];
      const options = { maxChars: 20_000, reserveChars: 1_000, limit: 20 };
      const one = pageAt(cards, 0, options);
      // The small card does not share a page with one that cannot fit.
      expect(one.cards).toEqual([small]);
      const two = pageAt(cards, one.nextOffset!, options);
      expect(two.cards.length).toBe(1);
      expect(two.trimmed).toBeGreaterThan(0);
      expect(two.oversized).toBe(false);
      expect(JSON.stringify(two.cards[0]).length).toBeLessThanOrEqual(
         19_000 - 2,
      );
      expect(ids(two.cards)).toEqual(ids([huge]));
      expect(two.cards[0].entities![0].description!.endsWith("…")).toBe(true);
      // The held card itself is not changed by the trim.
      expect(huge.entities![0].description!.endsWith("…")).toBe(false);
      expect(two.nextOffset).toBe(2);
      expect(pageAt(cards, 2, options).cards).toEqual([after]);
   });

   it("says the card is still over the budget when the descriptions are all at their floor", () => {
      const huge = bigCard("huge", 400, 210);
      const page = pageAt([huge], 0, {
         maxChars: 20_000,
         reserveChars: 1_000,
         limit: 20,
      });
      expect(page.cards.length).toBe(1);
      expect(page.oversized).toBe(true);
      expect(ids(page.cards)).toEqual(ids([huge]));
   });

   it("leaves a card that fits as it is", () => {
      const card = bigCard("fits", 3, 100);
      expect(shrinkCard(card, 1_000_000)).toEqual({ card, trimmed: 0 });
   });
});

describe("resultKey", () => {
   const base: ResolvedRequest = {
      ...request,
      searches: [
         {
            targetIndex: 0,
            targetType: "measure",
            text: "revenue",
            kinds: ["measure"],
         },
      ],
   };

   it("is the same for two pages of one request, whatever the key order of the givens", () => {
      const a = resultKey({
         ...base,
         givens: { region: "west", role: { level: 2, name: "analyst" } },
         offset: 0,
         limit: 5,
      });
      const b = resultKey({
         ...base,
         givens: { role: { name: "analyst", level: 2 }, region: "west" },
         offset: 5,
         limit: 20,
      });
      expect(a).toBe(b);
   });

   it("differs by givens, filter values, scope and search text", () => {
      const keys = [
         resultKey(base),
         resultKey({ ...base, givens: { region: "west" } }),
         resultKey({ ...base, givens: { region: "east" } }),
         resultKey({ ...base, filterParams: { year: "2024" } }),
         resultKey({ ...base, sourceName: "orders" }),
         resultKey({ ...base, modelPath: "shop.malloy" }),
         resultKey({ ...base, packageName: "other" }),
         resultKey({
            ...base,
            searches: [{ ...base.searches[0], text: "net revenue" }],
         }),
         resultKey({ ...base, includeCode: true }),
      ];
      expect(new Set(keys).size).toBe(keys.length);
   });
});

describe("HeldResults", () => {
   const result = (n: number): HeldResult => ({
      cards: [bigCard(`s${n}`, 1, 10)],
      clusters: [],
      unresolved_targets: [],
      warnings: [],
   });

   it("holds a result for its ttl, for the index it was made on only", () => {
      let now = 0;
      const held = new HeldResults(1_000, 10, 1_000_000, () => now);
      const index = {};
      held.set("k", index, result(1));
      expect(held.get("k", index)).toEqual(result(1));
      expect(held.get("k", {})).toBeUndefined();
      held.set("k", index, result(1));
      now = 1_001;
      expect(held.get("k", index)).toBeUndefined();
      expect(held.size).toBe(0);
   });

   it("drops the oldest result past the count or the size cap, and never holds one over the cap", () => {
      const index = {};
      const byCount = new HeldResults(60_000, 2, 1_000_000);
      byCount.set("a", index, result(1));
      byCount.set("b", index, result(2));
      byCount.set("c", index, result(3));
      expect(byCount.get("a", index)).toBeUndefined();
      expect(byCount.get("c", index)).toEqual(result(3));
      const one = JSON.stringify(result(1).cards).length;
      const bySize = new HeldResults(60_000, 10, one * 2);
      bySize.set("a", index, result(1));
      bySize.set("b", index, result(2));
      bySize.set("c", index, result(3));
      expect(bySize.size).toBe(2);
      expect(bySize.get("a", index)).toBeUndefined();
      const tiny = new HeldResults(60_000, 10, one - 1);
      tiny.set("a", index, result(1));
      expect(tiny.size).toBe(0);
   });
});
