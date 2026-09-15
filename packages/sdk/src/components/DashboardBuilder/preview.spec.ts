// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { DashboardDocument, DashboardTile } from "./document";
import { previewGivens, previewTileQuery, unsavedControls } from "./preview";

const tile = (
   name: string,
   from: string,
   filters?: DashboardTile["filters"],
): DashboardTile => ({
   name,
   source: "overview",
   declaration: { kind: "reference", from },
   ...(filters ? { filters } : {}),
});

const document: DashboardDocument = {
   title: "Overview",
   imports: [],
   sources: [{ name: "overview", base: "order_items" }],
   localGivens: [
      {
         name: "CATEGORY",
         type: "filter<string>",
         default: "f''",
         label: "Category",
         control: "select",
         suggest: { source: "products", dimension: "category" },
      },
      { name: "SINCE", type: "date", default: "@2023-01-01", label: "Since" },
      { name: "UNBOUND", type: "filter<string>", default: "f''" },
   ],
   tiles: [
      tile("kpis", "key_figures", [
         { field: "category", given: "CATEGORY" },
         { field: "created_at", given: "SINCE", op: ">=" },
      ]),
      tile("trend", "sales_by_month", [
         { field: "category", given: "CATEGORY" },
      ]),
      tile("plain", "by_state"),
   ],
};

describe("previewGivens", () => {
   it("shows the givens some tile binds, this file's first, in declaration order", () => {
      const specs = previewGivens(document, [
         { name: "REGION", type: "filter<string>" },
         { name: "CATEGORY", type: "filter<string>", label: "From the model" },
      ]);
      // UNBOUND is declared and bound by nothing, so it is not a control; the
      // model's REGION is bound by nothing either. The model's CATEGORY shares
      // a name with the local one, and the local declaration is the control.
      expect(specs.map((s) => s.name)).toEqual(["CATEGORY", "SINCE"]);
      expect(specs[0].label).toBe("Category");
      expect(specs[0].control).toBe("select");
      expect(specs[0].suggest).toEqual({
         source: "products",
         dimension: "category",
      });
   });

   it("uses the model's own spec for a model given a tile binds", () => {
      const withModel: DashboardDocument = {
         ...document,
         localGivens: [],
         tiles: [tile("t", "v", [{ field: "brand", given: "BRAND" }])],
      };
      const spec = {
         name: "BRAND",
         type: "filter<string>",
         suggest: {
            query: "brand_suggest",
            dimension: "brand",
            givenNames: ["REGION"],
         },
      };
      expect(previewGivens(withModel, [spec])).toEqual([spec]);
   });
});

describe("previewTileQuery", () => {
   const runnable = new Set(["CATEGORY", "SINCE"]);

   it("runs the view on the source the extension extends, with the document's bindings", () => {
      const q = previewTileQuery(document, document.tiles[0], runnable);
      expect(q.expression).toBe(
         "order_items -> key_figures + { where: category ~ $CATEGORY, where: created_at >= $SINCE }",
      );
      expect(q.givenNames).toEqual(["CATEGORY", "SINCE"]);
   });

   it("sends a tile only the givens it binds", () => {
      // `trend` binds CATEGORY alone: SINCE moving must not re-run it, and
      // unbinding a tile is what takes a control's effect off it.
      const q = previewTileQuery(document, document.tiles[1], runnable);
      expect(q.givenNames).toEqual(["CATEGORY"]);
      const bare = previewTileQuery(document, document.tiles[2], runnable);
      expect(bare.expression).toBe("order_items -> by_state");
      expect(bare.givenNames).toEqual([]);
   });

   it("leaves out a binding to a given the server does not know yet", () => {
      const q = previewTileQuery(
         document,
         document.tiles[0],
         new Set(["CATEGORY"]),
      );
      expect(q.expression).toBe(
         "order_items -> key_figures + { where: category ~ $CATEGORY }",
      );
      expect(q.givenNames).toEqual(["CATEGORY"]);
   });

   it("runs an inherited tile as the model has it, sending the whole row", () => {
      const inherited: DashboardTile = {
         name: "by_brand",
         source: "orders",
         declaration: { kind: "inherited" },
      };
      expect(previewTileQuery(document, inherited, runnable)).toEqual({
         expression: "orders -> by_brand",
         givenNames: undefined,
      });
   });
});

describe("unsavedControls", () => {
   it("names the bound controls the server cannot run yet", () => {
      expect(unsavedControls(document, new Set(["CATEGORY"]))).toEqual([
         "SINCE",
      ]);
      expect(unsavedControls(document, new Set(["CATEGORY", "SINCE"]))).toEqual(
         [],
      );
   });
});
