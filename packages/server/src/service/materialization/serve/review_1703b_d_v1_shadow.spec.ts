// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Second review of PR #1703 (F2): the shadow's strict comparison must not flag
// differences two correct plans produce, and must still flag real ones.

import { describe, expect, it } from "bun:test";
import { shadowRowDifference, shadowOutcome } from "./v1_shadow";

describe("F2 review: no false differences", () => {
   it("a float sum landing on an integer one way and just off it the other", () => {
      // sum of ten 0.1s: 0.9999999999999999 in one order, 1 in another.
      let s = 0;
      for (let i = 0; i < 10; i++) s += 0.1;
      expect(s).not.toBe(1);
      expect(shadowRowDifference([{ v: 1 }], [{ v: s }])).toBeUndefined();
      expect(
         shadowRowDifference([{ v: 3 }], [{ v: 3.0000000000000004 }]),
      ).toBeUndefined();
   });

   it("float noise that reorders rows tied on a float column", () => {
      const a = 0.1 + 0.2; // 0.30000000000000004
      const b = 0.3;
      const today = [
         { avg: a, name: "z" },
         { avg: b, name: "a" },
      ];
      const cutover = [
         { avg: b, name: "z" },
         { avg: a, name: "a" },
      ];
      expect(shadowRowDifference(today, cutover)).toBeUndefined();
   });

   it("row order, -0 and 0, bigint and number of one value", () => {
      expect(
         shadowRowDifference(
            [
               { k: "a", n: 1 },
               { k: "b", n: 2 },
            ],
            [
               { k: "b", n: 2 },
               { k: "a", n: 1 },
            ],
         ),
      ).toBeUndefined();
      expect(shadowRowDifference([{ v: -0 }], [{ v: 0 }])).toBeUndefined();
      expect(
         shadowRowDifference([{ v: 12345678901n }], [{ v: 12345678901 }]),
      ).toBeUndefined();
      expect(shadowRowDifference([{ v: null }], [{ v: null }])).toBeUndefined();
   });
});

describe("F2 review: real differences still flagged", () => {
   it("integers, strings with leading zeros, null against a value, bigints", () => {
      expect(shadowRowDifference([{ v: 1 }], [{ v: 2 }])).toBeDefined();
      expect(shadowRowDifference([{ v: "007" }], [{ v: "7" }])).toBeDefined();
      expect(shadowRowDifference([{ v: "7" }], [{ v: 7 }])).toBeDefined();
      expect(shadowRowDifference([{ v: null }], [{ v: 0 }])).toBeDefined();
      expect(shadowRowDifference([{ v: true }], [{ v: 1 }])).toBeDefined();
      expect(
         shadowRowDifference(
            [{ v: 9007199254740993n }],
            [{ v: 9007199254740992 }],
         ),
      ).toBeDefined();
      expect(shadowRowDifference([{ v: 1.5 }], [{ v: 1.6 }])).toBeDefined();
      expect(
         shadowRowDifference([{ d: "2024-01-01" }], [{ d: "2024-01-02" }]),
      ).toBeDefined();
      expect(
         shadowRowDifference(
            [{ d: "2024-01-01" }],
            [{ d: "2024-01-01T00:00:00.000Z" }],
         ),
      ).toBeDefined();
      expect(
         shadowOutcome(
            { rows: [{ v: 1 }], fields: [] },
            {
               rows: [{ v: 1 }, { v: 1 }],
               fields: [],
               servedBy: "materialized",
            },
         ).outcome,
      ).toBe("rows_differ");
   });
});
