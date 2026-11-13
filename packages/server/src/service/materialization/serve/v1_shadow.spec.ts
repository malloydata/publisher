// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { shadowOutcome } from "./v1_shadow";

const field = (
   name: string,
   type: unknown,
   annotations: string[] = [],
): unknown => ({
   name,
   type,
   annotations: annotations.map((value) => ({ value })),
});

const TODAY = {
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

describe("shadowOutcome", () => {
   it("is equal for the same rows in another order, a number's subtype and drill annotations aside", () => {
      expect(
         shadowOutcome(TODAY, {
            servedBy: "materialized",
            rows: [
               { region: "west", total: 1.5 + 1e-12 },
               { region: "east", total: 3 },
            ],
            fields: [
               field("region", { kind: "string_type" }, [
                  '#(malloy) drill_expression = region reference_id = "x"',
               ]),
               field("total", { kind: "number_type" }, ["# currency"]),
            ],
         }),
      ).toEqual({ outcome: "equal" });
   });

   it("is live when the cutover answered live, whatever the rows", () => {
      expect(shadowOutcome(TODAY, { ...TODAY, servedBy: "live" }).outcome).toBe(
         "live",
      );
      expect(
         shadowOutcome(TODAY, { ...TODAY, servedBy: undefined }).outcome,
      ).toBe("live");
   });

   it("names where rows differ, never a value", () => {
      const out = shadowOutcome(TODAY, {
         servedBy: "materialized",
         rows: [
            { region: "east", total: 4 },
            { region: "west", total: 1.5 },
         ],
         fields: TODAY.fields,
      });
      expect(out).toEqual({
         outcome: "rows_differ",
         detail: "row 0 column total",
      });
      expect(
         shadowOutcome(TODAY, {
            servedBy: "materialized",
            rows: TODAY.rows.slice(1),
            fields: TODAY.fields,
         }).detail,
      ).toBe("row counts differ: 2 against 1");
   });

   it("names a metadata difference: a dropped annotation or timeframe", () => {
      expect(
         shadowOutcome(TODAY, {
            servedBy: "rollup",
            rows: TODAY.rows,
            fields: [TODAY.fields[0], field("total", { kind: "number_type" })],
         }),
      ).toEqual({ outcome: "metadata_differ", detail: "total.annotations" });
      expect(
         shadowOutcome(
            {
               rows: [],
               fields: [field("m", { kind: "date_type", timeframe: "month" })],
            },
            {
               servedBy: "materialized",
               rows: [],
               fields: [field("m", { kind: "date_type" })],
            },
         ),
      ).toEqual({ outcome: "metadata_differ", detail: "m.type" });
   });
});
