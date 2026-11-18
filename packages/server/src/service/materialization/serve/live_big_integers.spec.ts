// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   type ResultRows,
   roundBigIntegersAsLive,
   roundStoredAnswerAsLive,
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

describe("roundStoredAnswerAsLive", () => {
   const live = (dialect: string) =>
      ({ getPreparedQuery: async () => ({ dialect }) }) as never;

   it("rounds a stored answer to a query whose live dialect rounds (Postgres)", async () => {
      const r = result([{ big: ABOVE }]);
      await roundStoredAnswerAsLive(r, live("postgres"));
      expect(r._queryResult.result[0].big).toBe(ROUNDED);
   });

   it("leaves it exact where live keeps every digit (DuckDB)", async () => {
      const r = result([{ big: ABOVE }]);
      await roundStoredAnswerAsLive(r, live("duckdb"));
      expect(r._queryResult.result[0].big).toBe(ABOVE);
   });

   it("leaves it as it ran when the live compile cannot say its dialect", async () => {
      const r = result([{ big: ABOVE }]);
      await roundStoredAnswerAsLive(r, {
         getPreparedQuery: async () => {
            throw new Error("no compile");
         },
      } as never);
      expect(r._queryResult.result[0].big).toBe(ABOVE);
   });
});
