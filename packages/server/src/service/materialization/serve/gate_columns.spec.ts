// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { Binding } from "../domain/binding";
import { judgeGateColumns } from "./gate_columns";

const table = (over: Partial<Binding> = {}): Binding =>
   ({
      sourceName: "orders",
      sourceIDs: ["orders@m"],
      kind: "persist",
      tier: "storage",
      destinationName: "lake",
      virtualHandle: "addr",
      tablePath: "orders_t",
      schema: [
         { name: "acct", type: "DOUBLE" },
         { name: "region", type: "VARCHAR" },
         { name: "day", type: "DATE" },
         { name: "amount", type: "DOUBLE" },
      ],
      holdsOnlyUngatedRows: true,
      ...over,
   }) as Binding;

const LIVE: Record<string, { type: string; numberType?: string }> = {
   acct: { type: "number", numberType: "integer" },
   region: { type: "string" },
   day: { type: "date" },
   amount: { type: "number", numberType: "float" },
};

const judge = (
   gateColumns: ReadonlySet<string> | "all" | undefined,
   tables: Binding[] = [table()],
   zoned = false,
   leftOut: readonly string[] = [],
) =>
   judgeGateColumns({
      tables,
      gateColumns,
      zoned,
      leftOut: () => leftOut,
      liveField: (_t, column) =>
         LIVE[column] ? { name: column, ...LIVE[column] } : undefined,
   }).map((t) => [
      t.tier,
      t.holdsOnlyUngatedRows === true,
      t.ownGateKept === true,
   ]);

describe("judgeGateColumns (question 14, F1 and F2)", () => {
   it("keeps a G3 table whose gate columns are stored exactly", () => {
      expect(judge(new Set(["region"]))).toEqual([["storage", true, false]]);
      // A float live holds approximately compares alike stored as a float.
      expect(judge(new Set(["amount"]))).toEqual([["storage", true, false]]);
   });

   it("clears a table whose gate compares an exact number stored as a float (F2)", () => {
      expect(judge(new Set(["acct"]))).toEqual([["storage", false, false]]);
      expect(
         judge(new Set(["acct"]), [
            table({ holdsOnlyUngatedRows: undefined, ownGateKept: true }),
         ]),
      ).toEqual([["storage", false, false]]);
   });

   it("clears a table whose gate column the served copy left out", () => {
      expect(judge(new Set(["region"]), [table()], false, ["region"])).toEqual([
         ["storage", false, false],
      ]);
   });

   it("clears a table whose gate compares a date in a caller's zone (F1)", () => {
      expect(judge(new Set(["day"]), [table()], true)).toEqual([
         ["storage", false, false],
      ]);
      expect(judge(new Set(["day"]), [table()], false)).toEqual([
         ["storage", true, false],
      ]);
   });

   it("holds a gate it cannot read column by column to every column", () => {
      // `acct` (an exact number stored as a float) is not one the gate
      // names, but the gate reads something other than plain columns.
      expect(judge("all")).toEqual([["storage", false, false]]);
      expect(
         judge("all", [
            table({ schema: [{ name: "region", type: "VARCHAR" }] }),
         ]),
      ).toEqual([["storage", true, false]]);
   });

   it("clears a column whose live field is unknown (fail closed)", () => {
      expect(
         judge("all", [
            table({ schema: [{ name: "mystery", type: "VARCHAR" }] }),
         ]),
      ).toEqual([["storage", false, false]]);
   });

   it("leaves a database table, a table not admitted, and an ungated entry point alone", () => {
      expect(
         judge(new Set(["acct"]), [
            table({ tier: "colocated", holdsOnlyUngatedRows: undefined }),
            table({ holdsOnlyUngatedRows: undefined }),
         ]),
      ).toEqual([
         ["colocated", false, false],
         ["storage", false, false],
      ]);
      expect(judge(undefined)).toEqual([["storage", true, false]]);
   });
});
