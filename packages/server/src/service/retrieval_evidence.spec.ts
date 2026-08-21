import { describe, expect, it } from "bun:test";
import {
   compactRankedSummary,
   compactRankedSummaryForTargets,
} from "./retrieval_evidence";

describe("compactRankedSummary", () => {
   it("numbers legacy results by position", () => {
      const summary = compactRankedSummary([
         { kind: "source", name: "orders" },
         { kind: "measure", source: "orders", name: "total_sales" },
      ]);
      expect(summary).toEqual({
         entityIds: ["source:orders", "measure:orders:total_sales"],
         ranks: [1, 2],
         resultCount: 2,
      });
   });

   it("prefers a typed entity's own rank over its position", () => {
      const summary = compactRankedSummary([
         { entityId: "measure:orders:total_sales", rank: 3 },
         { entityId: "dimension:orders:status", rank: 7 },
      ]);
      expect(summary.ranks).toEqual([3, 7]);
   });

   it("skips rows with no identity", () => {
      const summary = compactRankedSummary([{}, null, { name: "orders" }]);
      expect(summary.entityIds).toEqual(["orders"]);
      expect(summary.resultCount).toBe(1);
   });
});

describe("compactRankedSummaryForTargets", () => {
   it("keeps each target's ranks independent instead of flattening", () => {
      const summary = compactRankedSummaryForTargets([
         {
            target_type: "measure",
            search_text: "total sales",
            results: [
               { entityId: "measure:orders:total_sales", rank: 1 },
               { entityId: "measure:orders:net_sales", rank: 2 },
            ],
         },
         {
            target_type: "dimension",
            search_text: "status",
            results: [{ entityId: "dimension:orders:status", rank: 1 }],
         },
      ]);
      // The second target's first entity is rank 1 within its own target,
      // not rank 3 of a flattened list.
      expect(summary.ranks).toEqual([1, 2, 1]);
      expect(summary.resultCount).toBe(3);
      expect(summary.targets).toEqual([
         {
            targetType: "measure",
            searchText: "total sales",
            entityIds: [
               "measure:orders:total_sales",
               "measure:orders:net_sales",
            ],
            ranks: [1, 2],
         },
         {
            targetType: "dimension",
            searchText: "status",
            entityIds: ["dimension:orders:status"],
            ranks: [1],
         },
      ]);
   });

   it("summarizes an empty target list", () => {
      const summary = compactRankedSummaryForTargets([]);
      expect(summary).toEqual({
         entityIds: [],
         ranks: [],
         resultCount: 0,
         targets: [],
      });
   });
});
