// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   liveRoundsBigIntegersAsPostgres,
   type ResultRows,
   roundBigIntegersAsLive,
} from "./live_big_integers";

/** 2^53 + 1, which live on Postgres answers as 2^53. */
const ABOVE = 9007199254740993n;
const ROUNDED = 9007199254740992n;

const bigint = (name: string) => ({
   name,
   type: "number",
   numberType: "bigint",
});

function result(rows: Record<string, unknown>[]): ResultRows {
   return {
      _queryResult: {
         result: rows as never,
         structs: [
            {
               fields: [
                  bigint("big"),
                  { name: "n", type: "number", numberType: "integer" },
                  { name: "f", type: "number" },
                  { name: "s", type: "string" },
                  {
                     name: "nest",
                     type: "array",
                     fields: [
                        bigint("inner"),
                        { name: "label", type: "string" },
                     ],
                  },
                  {
                     name: "rec",
                     type: "record",
                     fields: [{ ...bigint("raw"), as: "renamed" }],
                  },
                  {
                     name: "list",
                     type: "array",
                     elementTypeDef: { type: "number", numberType: "bigint" },
                  },
               ],
            },
         ],
      },
   };
}

describe("roundBigIntegersAsLive", () => {
   it("rounds each bigint value, at any depth, to the nearest double's integer, as live's JSON stage does", () => {
      const r = result([
         {
            big: ABOVE,
            n: 3,
            f: 1.5,
            s: `${ABOVE}`,
            nest: [{ inner: -ABOVE, label: `${ABOVE}` }],
            rec: { renamed: `${ABOVE}` },
            list: [ABOVE, 7n, null],
         },
         { big: null, n: null, f: null, s: null, nest: [], rec: null },
      ]);
      roundBigIntegersAsLive(r);
      expect(r._queryResult.result).toEqual([
         {
            big: ROUNDED,
            n: 3,
            f: 1.5,
            // Not a number field: left as it is.
            s: `${ABOVE}`,
            nest: [{ inner: -ROUNDED, label: `${ABOVE}` }],
            rec: { renamed: `${ROUNDED}` },
            list: [ROUNDED, 7n, null],
         },
         { big: null, n: null, f: null, s: null, nest: [], rec: null },
      ] as never);
   });

   it("leaves a value already rounded, or within 2^53, as it is", () => {
      const r = result([{ big: ROUNDED }, { big: 42n }, { big: -(2n ** 53n) }]);
      roundBigIntegersAsLive(r);
      expect(r._queryResult.result.map((row) => row.big)).toEqual([
         ROUNDED,
         42n,
         -(2n ** 53n),
      ]);
   });
});

describe("roundBigIntegersAsLive past 1e21", () => {
   it("gives a value whose nearest double prints exponentially as that number, as Malloy's normaliser keeps it", () => {
      const huge = 1800000000000000000123n;
      const r = result([
         { big: huge, list: [huge] },
         { big: -huge },
         { big: `${huge}` },
         // Below 1e21 the double prints in full: still a BigInt.
         { big: 99999999999999999999n },
      ]);
      roundBigIntegersAsLive(r);
      const rows = r._queryResult.result;
      expect(rows[0].big).toBe(1.8e21);
      expect(rows[0].list).toEqual([1.8e21]);
      expect(rows[1].big).toBe(-1.8e21);
      expect(rows[2].big).toBe(1.8e21);
      expect(rows[3].big).toBe(100000000000000000000n);
      expect(JSON.stringify(rows[0].big)).toBe("1.8e+21");
   });

   it("leaves a result with no bigint field untouched, without reading its rows", () => {
      let read = false;
      const row = new Proxy(
         { n: 1 },
         {
            get(target, key) {
               read = true;
               return Reflect.get(target, key);
            },
            has(target, key) {
               read = true;
               return Reflect.has(target, key);
            },
         },
      );
      roundBigIntegersAsLive({
         _queryResult: {
            result: [row] as never,
            structs: [
               {
                  fields: [
                     { name: "n", type: "number", numberType: "integer" },
                     {
                        name: "nest",
                        type: "array",
                        fields: [{ name: "s", type: "string" }],
                     },
                  ],
               },
            ],
         },
      });
      expect(read).toBe(false);
   });
});

describe("liveRoundsBigIntegersAsPostgres", () => {
   it("is Postgres alone: every other dialect whose live answer rounds is unmeasured", () => {
      expect(liveRoundsBigIntegersAsPostgres("postgres")).toBe(true);
      for (const dialect of [
         "duckdb",
         "trino",
         "presto",
         "databricks",
         "sqlserver",
         undefined,
      ]) {
         expect(liveRoundsBigIntegersAsPostgres(dialect)).toBe(false);
      }
   });
});
