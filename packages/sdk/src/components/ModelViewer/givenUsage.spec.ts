// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { givensForSource, givensInheritedBy, givensReadBy } from "./givenUsage";

const g = (name: string) => ({ name, type: "number", modelFile: "m.malloy" });
const GIVENS = [g("MIN_AMOUNT"), g("CATEGORY"), g("UNUSED"), g("MIN")];

describe("givensReadBy", () => {
   it("finds each declared given a definition reads by $NAME, whole names only", () => {
      expect(
         givensReadBy("is_large is amount >= $MIN_AMOUNT", GIVENS).map(
            (x) => x.name,
         ),
      ).toEqual(["MIN_AMOUNT"]);
      expect(
         givensReadBy(
            "where: category ~ $CATEGORY and amount > $MIN",
            GIVENS,
         ).map((x) => x.name),
      ).toEqual(["CATEGORY", "MIN"]);
      expect(givensReadBy("order_count is count()", GIVENS)).toEqual([]);
      expect(givensReadBy(undefined, GIVENS)).toEqual([]);
   });

   it("ignores a $NAME in a comment", () => {
      expect(givensReadBy("count() // not $CATEGORY", GIVENS)).toEqual([]);
   });
});

const TEXT = `##! experimental.givens
given:
  MIN_AMOUNT :: number is 100
  CATEGORY :: filter<string> is f''
  UNUSED :: number is 1

source: orders is pg.table('public.orders') extend {
  dimension: is_large is amount >= $MIN_AMOUNT
}

#(authorize) 'finance' in $GROUPS
source: targets is pg.sql("SELECT 1") extend {
  measure: n is count()
}

source: narrowed is orders extend {
  where: category ~ $CATEGORY
}

source: vs_targets is targets extend {
  join_one: o is narrowed with category
}

query: q is orders -> { where: amount > $MIN; aggregate: n is count() }
`;
const names = (list: ReadonlyArray<{ name: string }>) =>
   list.map((x) => x.name).sort();

describe("givensForSource", () => {
   it("keeps the givens the source reads, through what it extends, plus those no source reads", () => {
      // UNUSED and MIN are read by no source: model-wide, shown everywhere.
      expect(names(givensForSource(TEXT, "orders", GIVENS))).toEqual([
         "MIN",
         "MIN_AMOUNT",
         "UNUSED",
      ]);
      expect(names(givensForSource(TEXT, "narrowed", GIVENS))).toEqual([
         "CATEGORY",
         "MIN",
         "MIN_AMOUNT",
         "UNUSED",
      ]);
      expect(names(givensForSource(TEXT, "targets", GIVENS))).toEqual([
         "MIN",
         "UNUSED",
      ]);
   });

   it("follows a join to the source it joins", () => {
      expect(names(givensForSource(TEXT, "vs_targets", GIVENS))).toEqual([
         "CATEGORY",
         "MIN",
         "MIN_AMOUNT",
         "UNUSED",
      ]);
   });

   it("keeps every given when the text is absent or the source isn't found", () => {
      expect(givensForSource(undefined, "orders", GIVENS)).toEqual(GIVENS);
      expect(givensForSource(TEXT, "missing", GIVENS)).toEqual(GIVENS);
   });

   it("reaches a given an unexported source reads, through what extends or joins it", () => {
      const text = `source: base_orders is pg.table('public.orders') extend {
  where: amount >= $MIN_AMOUNT
}

source: orders is base_orders extend {
  measure: n is count()
}

source: by_target is pg.sql("SELECT 1") extend {
  join_one: b is base_orders with id
}
`;
      expect(names(givensForSource(text, "orders", GIVENS))).toContain(
         "MIN_AMOUNT",
      );
      expect(names(givensForSource(text, "by_target", GIVENS))).toContain(
         "MIN_AMOUNT",
      );
      // base_orders isn't exported, so the section says where MIN_AMOUNT comes from.
      const exported = new Set(["orders", "by_target"]);
      expect(givensInheritedBy(text, "orders", GIVENS, exported)).toEqual(
         new Map([["MIN_AMOUNT", ["base_orders"]]]),
      );
      // An exported upstream is a page the reader can open: nothing to name.
      expect(
         givensInheritedBy(
            text,
            "orders",
            GIVENS,
            new Set([...exported, "base_orders"]),
         ),
      ).toEqual(new Map());
   });
});
