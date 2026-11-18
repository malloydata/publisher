// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   float4AsLive,
   float4ColumnsByHandle,
   float4OutputShape,
   type Float4Shape,
   liveWritesFloat4AsPostgres,
   roundFloat4AsLive,
} from "./live_float4";

describe("float4AsLive", () => {
   // Each pair: a float4 widened to a double (as a DuckDB read gives it), and
   // the number Postgres 15's `to_json` wrote for the same `real`.
   const measured: [number, number][] = [
      [0.10000000149011612, 0.1],
      [0.30000001192092896, 0.3],
      [3.141590118408203, 3.14159],
      [-2.700000047683716, -2.7],
      [1.0000000116860974e-7, 1e-7],
      [3.3999999521443642e38, 3.4e38],
      [16777216, 16777216],
      [1.5, 1.5],
      // A decimal exactly halfway to the neighbour is not taken, though it
      // would read back as this float4 (ties-to-even).
      [-38261648, -38261648],
      [-1829191936, -1829191900],
      // Of two equally close decimals, the even last digit.
      [3388560.25, 3388560.2],
      // The narrow interval below a power of two.
      [2 ** -126, 1.1754944e-38],
      [2 ** 127, 1.7014118e38],
      // A subnormal.
      [1.401298464324817e-45, 1e-45],
   ];
   for (const [widened, live] of measured) {
      it(`writes ${widened} as Postgres writes it, ${live}`, () => {
         expect(float4AsLive(widened)).toBe(live);
         // A value already as live writes it is unchanged.
         expect(float4AsLive(live)).toBe(live);
      });
   }

   it("rounds a double to its float4 first, as a sum of float4s is", () => {
      // 1.5 + 16777216 = 16777217.5; Postgres's float4 sum is 16777218.
      expect(float4AsLive(16777217.5)).toBe(16777218);
      expect(float4AsLive(0.4415901707244885)).toBe(0.44159016);
   });

   it("leaves a value it cannot write as a float4 as it is", () => {
      expect(float4AsLive(Number.NaN)).toBeNaN();
      expect(float4AsLive(Infinity)).toBe(Infinity);
      expect(float4AsLive(1e39)).toBe(1e39);
      expect(Object.is(float4AsLive(-0), -0)).toBe(true);
      expect(float4AsLive(0)).toBe(0);
   });
});

describe("liveWritesFloat4AsPostgres", () => {
   it("is Postgres alone", () => {
      expect(liveWritesFloat4AsPostgres("postgres")).toBe(true);
      expect(liveWritesFloat4AsPostgres("duckdb")).toBe(false);
      expect(liveWritesFloat4AsPostgres("standardsql")).toBe(false);
      expect(liveWritesFloat4AsPostgres(undefined)).toBe(false);
   });
});

describe("float4ColumnsByHandle", () => {
   it("reads each storage binding's FLOAT columns, by its handle", () => {
      const out = float4ColumnsByHandle([
         {
            virtualHandle: "h1",
            schema: [
               { name: "R", type: "FLOAT" },
               { name: "d", type: "DOUBLE" },
               { name: "r4", type: "real" },
            ],
         },
         { virtualHandle: "h2", schema: [{ name: "d", type: "DOUBLE" }] },
         { schema: [{ name: "x", type: "FLOAT" }] },
      ]);
      expect([...out.entries()].map(([k, v]) => [k, [...v]])).toEqual([
         ["h1", ["r", "r4"]],
      ]);
   });
});

const field = (...path: string[]) => ({ node: "field", path });
const agg = (fn: string, e: unknown) => ({
   node: "aggregate",
   function: fn,
   e,
});
const def = (name: string, e: unknown) => ({ name, type: "number", e });
const ref = (...path: string[]) => ({ type: "fieldref", path });

/** A virtual struct over handle `h` with columns r (FLOAT) and d, n. */
const root = {
   type: "virtual",
   name: "h",
   fields: [
      { name: "g", type: "string" },
      { name: "r", type: "number" },
      { name: "d", type: "number" },
      { name: "n", type: "number" },
      def("r_twice", {
         node: "+",
         kids: { left: field("r"), right: field("r") },
      }),
      def("r_sum", agg("sum", field("r"))),
      {
         type: "turtle",
         name: "by_g",
         pipeline: [
            {
               type: "reduce",
               queryFields: [ref("g"), def("hi", agg("max", field("r")))],
            },
         ],
      },
      {
         name: "j",
         type: "virtual",
         join: "one",
         fields: [{ name: "jr", type: "number" }],
      },
   ],
};

const FLOAT4 = new Map([
   ["h", new Set(["r"])],
   ["hj", new Set(["jr"])],
]);

function shapeOf(pipeline: unknown[], struct: unknown = root): Float4Shape {
   return float4OutputShape({ query: { pipeline }, root: struct }, FLOAT4);
}

function keys(shape: Float4Shape): string[] {
   return [...shape].sort();
}

describe("float4OutputShape", () => {
   it("marks the columns Postgres types real, and nothing else", () => {
      const shape = shapeOf([
         {
            type: "reduce",
            queryFields: [
               ref("g"),
               ref("r"),
               ref("d"),
               def("tot", agg("sum", field("r"))),
               def("lo", agg("min", field("r"))),
               def("hi", agg("max", field("r"))),
               // Postgres's avg of real is a float8.
               def("mean", agg("avg", field("r"))),
               def("n", agg("count", { node: "" })),
               // real * integer is a float8 in Postgres.
               def("r3", {
                  node: "*",
                  kids: {
                     left: field("r"),
                     right: { node: "numberLiteral", literal: "3" },
                  },
               }),
               def("neg", { node: "unary-", e: field("r") }),
               def("par", { node: "()", e: agg("sum", field("r")) }),
               def("filtered", {
                  node: "filteredExpr",
                  kids: { e: agg("sum", field("r")), filterList: [] },
               }),
               def("or0", {
                  node: "coalesce",
                  kids: {
                     left: field("r"),
                     right: { node: "numberLiteral", literal: "0" },
                  },
               }),
               def("ord", {
                  node: "coalesce",
                  kids: { left: field("r"), right: field("d") },
               }),
               ref("r_twice"),
               ref("r_sum"),
            ],
         },
      ]);
      expect(keys(shape)).toEqual(
         [
            "r",
            "tot",
            "lo",
            "hi",
            "neg",
            "par",
            "filtered",
            "or0",
            "r_twice",
            "r_sum",
         ].sort(),
      );
   });

   it("follows a renamed output and a later stage, and never marks a nest", () => {
      const shape = shapeOf([
         {
            type: "reduce",
            queryFields: [
               ref("g"),
               { ...ref("r"), as: "renamed" },
               ref("by_g"),
               {
                  type: "turtle",
                  name: "inline",
                  pipeline: [
                     { type: "project", queryFields: [ref("r"), ref("d")] },
                  ],
               },
               def("tot", agg("sum", field("r"))),
            ],
            outputStruct: {
               type: "query_result",
               fields: [
                  { name: "g", type: "string" },
                  { name: "renamed", type: "number" },
                  { name: "tot", type: "number" },
               ],
            },
         },
      ]);
      // Live's nests are DOUBLE PRECISION: never marked.
      expect(keys(shape)).toEqual(["renamed", "tot"]);
      const staged = shapeOf([
         {
            type: "reduce",
            queryFields: [ref("g"), def("tot", agg("sum", field("r")))],
            outputStruct: {
               type: "query_result",
               fields: [
                  { name: "g", type: "string" },
                  { name: "tot", type: "number" },
               ],
            },
         },
         {
            type: "reduce",
            queryFields: [ref("tot"), def("top", agg("max", field("tot")))],
         },
      ]);
      expect(keys(staged)).toEqual(["top", "tot"]);
   });

   it("reads a joined virtual struct's own columns", () => {
      const joined = {
         ...root,
         fields: [
            ...root.fields.filter((f) => f.name !== "j"),
            {
               name: "j",
               type: "virtual",
               join: "one",
               fields: [{ name: "jr", type: "number" }],
            },
         ],
      };
      // `j` is handle "j", which holds no float4: nothing is marked.
      expect(
         keys(
            shapeOf(
               [{ type: "project", queryFields: [ref("j", "jr")] }],
               joined,
            ),
         ),
      ).toEqual([]);
      const named = {
         ...joined,
         fields: joined.fields.map((f) =>
            f.name === "j" ? { ...f, name: "hj", as: "j" } : f,
         ),
      };
      expect(
         keys(
            shapeOf(
               [{ type: "project", queryFields: [ref("j", "jr")] }],
               named,
            ),
         ),
      ).toEqual(["jr"]);
   });

   it("marks nothing over a table that is not a storage= table, or with no float4 column", () => {
      const table = { ...root, type: "table" };
      expect(
         keys(shapeOf([{ type: "project", queryFields: [ref("r")] }], table)),
      ).toEqual([]);
      expect(
         float4OutputShape(
            {
               query: {
                  pipeline: [{ type: "project", queryFields: [ref("r")] }],
               },
               root,
            },
            new Map(),
         ).size,
      ).toBe(0);
   });

   it("follows a pipe head", () => {
      const shape = float4OutputShape(
         { query: { pipeHead: { name: "by_g" }, pipeline: [] }, root },
         FLOAT4,
      );
      expect(keys(shape)).toEqual(["hi"]);
   });

   it("marks nothing for a segment it does not read", () => {
      expect(
         keys(shapeOf([{ type: "index", queryFields: [ref("r")] }])),
      ).toEqual([]);
   });
});

describe("roundFloat4AsLive", () => {
   it("writes each marked top-level value as live does, in place", () => {
      const rows = [
         {
            r: 0.10000000149011612,
            d: 0.10000000149011612,
            tot: null,
            nest: [{ r: 3.141590118408203 }],
         },
      ];
      roundFloat4AsLive(
         { _queryResult: { result: rows } },
         new Set(["r", "tot", "nest"]),
      );
      expect(rows).toEqual([
         {
            r: 0.1,
            d: 0.10000000149011612,
            tot: null,
            nest: [{ r: 3.141590118408203 }],
         },
      ]);
   });
});
