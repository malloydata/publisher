// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   decomposeMeasure,
   emitMeasureExpr,
   isMeasureRefusal,
   parseMeasureExpression,
} from "./rollup_measure_grammar";

/** A scope over a fixed set of measures. */
function scope(measures: Record<string, string>) {
   return {
      measureCode: (n: string) => measures[n],
      isJoin: (n: string) => n === "carriers",
   };
}

/** Decompose `name`, and render its partials and merge as text. */
function render(measures: Record<string, string>, name = "m") {
   const d = decomposeMeasure(name, scope(measures));
   if (isMeasureRefusal(d)) return d;
   const columns = d.leaves.map((_, i) => `p${i}`);
   return {
      partials: d.leaves.map(
         (l) => `${l.aggregate}: ${emitMeasureExpr(l.expr)}`,
      ),
      merged: emitMeasureExpr(d.merged(columns)),
   };
}

describe("the #(preaggregate) measure grammar", () => {
   it("stores one partial per aggregate and merges count with sum", () => {
      expect(render({ m: "count()" })).toEqual({
         partials: ["count: count()"],
         merged: "p0.sum()",
      });
      expect(render({ m: "amount.sum()" })).toEqual({
         partials: ["sum: amount.sum()"],
         merged: "p0.sum()",
      });
      expect(render({ m: "max(amount)" })).toEqual({
         partials: ["max: max(amount)"],
         merged: "p0.max()",
      });
      expect(render({ m: "carriers.count()" })).toMatchObject({
         merged: "p0.sum()",
      });
   });

   it("keeps a filter on its aggregate, and pushes a reference's filter into it", () => {
      expect(
         render({ m: "amount.sum() { where: region = 'x', paid }" }),
      ).toEqual({
         partials: ["sum: amount.sum() { where: region = 'x', paid }"],
         merged: "p0.sum()",
      });
      expect(
         render({
            m: "ratio { where: paid }",
            ratio: "total / n",
            total: "amount.sum()",
            n: "count()",
         }),
      ).toEqual({
         partials: [
            "sum: amount.sum() { where: paid }",
            "count: count() { where: paid }",
         ],
         merged: "((p0.sum()) / (p1.sum()))",
      });
   });

   it("merges an expression of aggregates as the expression of merged partials", () => {
      expect(render({ m: "coalesce(amount.max(), 0) * 2" })).toEqual({
         partials: ["max: amount.max()"],
         merged: "coalesce(p0.max(), 0) * 2",
      });
      expect(render({ m: "amount.sum() / count()" })).toEqual({
         partials: ["sum: amount.sum()", "count: count()"],
         merged: "p0.sum() / p1.sum()",
      });
   });

   it("refuses an aggregate whose partials do not merge", () => {
      for (const code of [
         "amount.avg()",
         "avg(amount)",
         "count(customer_id)",
         "customer_id.count()",
         "all(amount.sum())",
         "stddev(amount)",
         "sum(amount.sum())",
      ]) {
         expect(render({ m: code })).toMatchObject({ kind: "not_mergeable" });
      }
   });

   it("reports text outside the grammar as unreadable, not as unmergeable", () => {
      for (const code of [
         "pick 1 when amount.sum() > 1 else 0",
         "amount.sum()::string",
         "amount.sum() { where: d > @2024-01-01 }",
         "amount.sum() { where: s = 'a' | 'b' }",
      ]) {
         expect(render({ m: code })).toMatchObject({ kind: "unreadable" });
      }
   });

   it("refuses a measure that refers to itself rather than looping", () => {
      expect(render({ m: "a + 1", a: "m * 2" })).toMatchObject({
         kind: "unreadable",
      });
   });

   it("re-emits names through malloy_text, quoting a keyword", () => {
      const parsed = parseMeasureExpression(
         "`count`.sum() { where: `date` = 'x' }",
      );
      expect(parsed && emitMeasureExpr(parsed)).toBe(
         "`count`.sum() { where: `date` = 'x' }",
      );
      expect(
         parseMeasureExpression("x.sum() { where: s = 'o\\'brien' }") &&
            emitMeasureExpr(
               parseMeasureExpression(
                  "x.sum() { where: s = 'o\\'brien' }",
               ) as never,
            ),
      ).toBe("x.sum() { where: s = 'o\\'brien' }");
   });
});
