// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { baseSourceName } from "./sourceLineage";

const MODEL = `
source: raw_orders is materialization_pg.table('public.orders')

#@ persist
source: order_rollup is raw_orders -> {
  measure: total_orders is count()
}

source: category_ranking is order_rollup -> {
  measure: revenue is sum(amount)
}

source: orders_preagg is materialization_pg.table('public.orders') extend {
  measure: n is count()
}

source: orders_ext is order_rollup extend {
  dimension: big is total_orders > 100
}
`;

const KNOWN = new Set([
   "raw_orders",
   "order_rollup",
   "category_ranking",
   "orders_preagg",
   "orders_ext",
]);

describe("baseSourceName", () => {
   it("returns the upstream source of a query-derived source", () => {
      expect(baseSourceName(MODEL, "order_rollup", KNOWN)).toBe("raw_orders");
      expect(baseSourceName(MODEL, "category_ranking", KNOWN)).toBe(
         "order_rollup",
      );
   });

   it("returns the base of a source-extended source", () => {
      expect(baseSourceName(MODEL, "orders_ext", KNOWN)).toBe("order_rollup");
   });

   it("returns undefined for a table-backed source", () => {
      expect(baseSourceName(MODEL, "raw_orders", KNOWN)).toBeUndefined();
   });

   it("returns undefined for a table-backed source that extends the table", () => {
      expect(baseSourceName(MODEL, "orders_preagg", KNOWN)).toBeUndefined();
   });

   it("returns undefined when the parsed base is not a known source", () => {
      expect(
         baseSourceName(MODEL, "order_rollup", new Set(["order_rollup"])),
      ).toBeUndefined();
   });

   it("returns undefined for absent text or name", () => {
      expect(baseSourceName(undefined, "order_rollup", KNOWN)).toBeUndefined();
      expect(baseSourceName(MODEL, undefined, KNOWN)).toBeUndefined();
   });

   it("handles backtick-quoted identifiers", () => {
      const text = "source: `my source` is `raw_orders` -> { }";
      expect(baseSourceName(text, "my source", new Set(["raw_orders"]))).toBe(
         "raw_orders",
      );
   });
});
