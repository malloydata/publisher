// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The conformance runner's strict comparison of a stored answer with live: it
// must not flag differences two correct plans produce (float noise, row order, -0, a bigint and a number of one
// value, the reference id Malloy mints per compile on a nested field), and it
// must still flag real ones (digit strings, integers beyond float tolerance,
// bigints above 2^53, null against a value).

import { describe, expect, it } from "bun:test";
import {
   strictFieldDifference,
   strictRowDifference,
} from "./answer_difference";

describe("strictRowDifference: no false differences", () => {
   it("a float sum landing on an integer one way and just off it the other", () => {
      // sum of ten 0.1s: 0.9999999999999999 in one order, 1 in another.
      let s = 0;
      for (let i = 0; i < 10; i++) s += 0.1;
      expect(s).not.toBe(1);
      expect(strictRowDifference([{ v: 1 }], [{ v: s }])).toBeUndefined();
      expect(
         strictRowDifference([{ v: 3 }], [{ v: 3.0000000000000004 }]),
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
      expect(strictRowDifference(today, cutover)).toBeUndefined();
   });

   it("row order, -0 and 0, bigint and number of one value", () => {
      expect(
         strictRowDifference(
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
      expect(strictRowDifference([{ v: -0 }], [{ v: 0 }])).toBeUndefined();
      expect(
         strictRowDifference([{ v: 12345678901n }], [{ v: 12345678901 }]),
      ).toBeUndefined();
      expect(strictRowDifference([{ v: null }], [{ v: null }])).toBeUndefined();
   });
});

describe("strictRowDifference: real differences still flagged", () => {
   it("integers, strings with leading zeros, null against a value, bigints", () => {
      expect(strictRowDifference([{ v: 1 }], [{ v: 2 }])).toBeDefined();
      expect(strictRowDifference([{ v: "007" }], [{ v: "7" }])).toBeDefined();
      expect(strictRowDifference([{ v: "7" }], [{ v: 7 }])).toBeDefined();
      expect(strictRowDifference([{ v: null }], [{ v: 0 }])).toBeDefined();
      expect(strictRowDifference([{ v: true }], [{ v: 1 }])).toBeDefined();
      expect(
         strictRowDifference(
            [{ v: 9007199254740993n }],
            [{ v: 9007199254740992 }],
         ),
      ).toBeDefined();
      expect(strictRowDifference([{ v: 1.5 }], [{ v: 1.6 }])).toBeDefined();
      expect(
         strictRowDifference([{ d: "2024-01-01" }], [{ d: "2024-01-02" }]),
      ).toBeDefined();
      expect(
         strictRowDifference(
            [{ d: "2024-01-01" }],
            [{ d: "2024-01-01T00:00:00.000Z" }],
         ),
      ).toBeDefined();
      expect(strictRowDifference([{ v: 1 }], [{ v: 1 }, { v: 1 }])).toBe(
         "row counts differ: 1 against 2",
      );
   });
});

describe("strictRowDifference: false equals", () => {
   it("tells two different strings of digits apart", () => {
      // A zip or account code; a cast that round-trips through a number
      // drops the leading zeros.
      expect(strictRowDifference([{ z: "007" }], [{ z: "7" }])).toBeDefined();
   });
   it("compares integers exactly, not within a float tolerance", () => {
      expect(
         strictRowDifference(
            [{ n: 1_000_000_000_000 }],
            [{ n: 1_000_000_000_500 }],
         ),
      ).toBeDefined();
   });
   it("tells two bigints above 2^53 apart", () => {
      expect(
         strictRowDifference(
            [{ n: 9007199254740993n }],
            [{ n: 9007199254740992n }],
         ),
      ).toBeDefined();
   });
});

const nest = (refId: string) => [
   {
      name: "x",
      type: {
         kind: "array_type",
         element_type: {
            kind: "record_type",
            fields: [
               {
                  name: "d",
                  annotations: [
                     {
                        value: `#(malloy) reference_id = "${refId}" drill_expression { kind = field_reference name = d path = [x] code = d }\n`,
                     },
                  ],
                  type: { kind: "date_type" },
               },
            ],
         },
      },
      annotations: [],
   },
];

describe("strictFieldDifference over a nest", () => {
   it("two compiles of one nest query are equal", () => {
      expect(
         strictFieldDifference(
            nest("6b35ece0-770f-4987-afdd-155867a7bb84"),
            nest("9fda3b84-333a-47ef-998a-63e825a0b22a"),
         ),
      ).toBeUndefined();
   });
});

const field = (
   name: string,
   type: unknown,
   annotations: string[] = [],
): unknown => ({
   name,
   type,
   annotations: annotations.map((value) => ({ value })),
});

const LIVE = {
   rows: [
      { region: "east", total: 3 },
      { region: "west", total: 1.5 },
   ],
   fields: [
      field("region", { kind: "string_type" }),
      field("total", { kind: "number_type", subtype: "integer" }, [
         "# currency",
      ]),
   ],
};

describe("a stored answer against live", () => {
   it("is equal for the same rows in another order, a number's subtype and drill annotations aside", () => {
      expect(
         strictRowDifference(LIVE.rows, [
            { region: "west", total: 1.5 + 1e-12 },
            { region: "east", total: 3 },
         ]),
      ).toBeUndefined();
      expect(
         strictFieldDifference(LIVE.fields, [
            field("region", { kind: "string_type" }, [
               '#(malloy) drill_expression = region reference_id = "x"',
            ]),
            field("total", { kind: "number_type" }, ["# currency"]),
         ]),
      ).toBeUndefined();
   });

   it("names where rows differ, never a value", () => {
      expect(
         strictRowDifference(LIVE.rows, [
            { region: "east", total: 4 },
            { region: "west", total: 1.5 },
         ]),
      ).toBe("row 0 column total");
      expect(strictRowDifference(LIVE.rows, LIVE.rows.slice(1))).toBe(
         "row counts differ: 2 against 1",
      );
   });

   it("names a metadata difference: a dropped annotation or timeframe", () => {
      expect(
         strictFieldDifference(LIVE.fields, [
            LIVE.fields[0],
            field("total", { kind: "number_type" }),
         ]),
      ).toBe("total.annotations");
      expect(
         strictFieldDifference(
            [field("m", { kind: "date_type", timeframe: "month" })],
            [field("m", { kind: "date_type" })],
         ),
      ).toBe("m.type");
   });
});
