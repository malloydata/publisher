// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   buildFcjIndexFixture,
   type FcjIndexFixture,
} from "../../test_helpers/fcj_index_fixture";
import { dateRangeFields, namesDateOrTimeRange } from "./date_range";
import type { JudgeVerdict } from "./judge";
import { routeKey } from "./routes";

let f: FcjIndexFixture;
beforeAll(async () => {
   f = await buildFcjIndexFixture();
});
afterAll(async () => f.close());

const RACES = "measure:races@reports.malloy:race_count";
const RACE_DATE = "dimension:races@reports.malloy:start_date";
const FLIGHTS = "measure:flights@travel.malloy:flight_count";
const FLIGHT_DATE = "dimension:flights@travel.malloy:start_date";

const verdict = (...keys: string[]): JudgeVerdict[] => [
   { cluster: "c", picks: [{ target: 0, keys }], caveats: [] },
];

describe("namesDateOrTimeRange", () => {
   it("reads a year, a month name, a quarter and the range words", () => {
      for (const text of [
         "What was our revenue in 2025?",
         "total revenue in August",
         "orders in May 2026",
         "sales by quarter",
         "Q3 bookings",
         "revenue last month",
         "signups since launch",
         "revenue this year",
         "orders in the past week",
         "daily active users",
         "weekly orders",
         "YTD revenue",
      ]) {
         expect([text, namesDateOrTimeRange(text)]).toEqual([text, true]);
      }
   });

   it("does not read a request with no date or time range", () => {
      for (const text of [
         "Which is our biggest product category?",
         "number of races",
         "venues that may close",
         "average order value by region",
      ]) {
         expect([text, namesDateOrTimeRange(text)]).toEqual([text, false]);
      }
   });
});

describe("dateRangeFields", () => {
   it("adds a date of the source a returned measure sits on, with a traceable reason", () => {
      const out = dateRangeFields({
         index: f.index,
         texts: ["number of races in 2024"],
         verdicts: verdict(RACES),
         alsoLikely: [],
      });
      expect(out.map((a) => [a.key, a.rule, a.of])).toEqual([
         [RACE_DATE, "date_range", "races.race_count"],
      ]);
      expect(out[0].reason).toBe(
         "Added by the date or time range rule: the request names a date or time range, and the measure races.race_count came back with no date or time field of races.",
      );
   });

   it("adds nothing when the request names no date or time range", () => {
      expect(
         dateRangeFields({
            index: f.index,
            texts: ["number of races"],
            verdicts: verdict(RACES),
            alsoLikely: [],
         }),
      ).toEqual([]);
   });

   it("adds nothing for a source whose date already came back, and adds for each other measure's source", () => {
      const out = dateRangeFields({
         index: f.index,
         texts: ["races and flights in 2024"],
         verdicts: verdict(RACES, RACE_DATE, FLIGHTS),
         alsoLikely: [],
      });
      expect(out.map((a) => a.key)).toEqual([FLIGHT_DATE]);
   });

   it("reads a picked route's measure as sitting on the joining source", () => {
      // A route to races' measure from flights (not a real join; the rule only reads the key).
      const route = routeKey({
         from: "flights@travel.malloy",
         path: ["race"],
         node: RACES,
         fanout: "one",
      });
      const out = dateRangeFields({
         index: f.index,
         texts: ["races last year"],
         verdicts: verdict(route),
         alsoLikely: [],
      });
      expect(out.map((a) => [a.key, a.of])).toEqual([
         [FLIGHT_DATE, "flights.race.race_count"],
      ]);
   });
});
