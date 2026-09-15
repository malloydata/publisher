// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { GivenValue } from "../../hooks/givenValue";
import type { DashboardDocument, DashboardTile } from "./document";
import { malloyLiteral, previewGivens, previewTileQuery } from "./preview";

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

   it("writes the value of a given the server does not know yet as a literal", () => {
      // SINCE is declared here and not compiled on the server: it cannot be
      // sent by name, so its value goes into the query, and a new filter works
      // before the file is saved. Only CATEGORY travels with the request.
      const q = previewTileQuery(
         document,
         document.tiles[0],
         new Set(["CATEGORY"]),
         new Map<string, GivenValue>([
            ["CATEGORY", "Shoes"],
            ["SINCE", new Date("2024-01-31T00:00:00Z")],
         ]),
      );
      expect(q.expression).toBe(
         "order_items -> key_figures + { where: category ~ $CATEGORY, where: created_at >= @2024-01-31 }",
      );
      expect(q.givenNames).toEqual(["CATEGORY"]);
   });

   it("leaves out an unsent given with no value yet, and one nobody declares", () => {
      const q = previewTileQuery(document, document.tiles[0], new Set());
      expect(q.expression).toBe("order_items -> key_figures");
      expect(q.givenNames).toEqual([]);
      const stray: DashboardTile = tile("t", "v", [
         { field: "x", given: "NOBODY" },
      ]);
      expect(
         previewTileQuery(document, stray, new Set(), new Map([["NOBODY", 1]]))
            .expression,
      ).toBe("order_items -> v");
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

describe("malloyLiteral", () => {
   it("spells a value the way a given of the type would hold it", () => {
      expect(malloyLiteral("filter<string>", "Nike")).toBe("f'Nike'");
      expect(malloyLiteral("filter<number>", ">= 10")).toBe("f'>= 10'");
      expect(malloyLiteral("string", "O'Neil")).toBe("'O\\'Neil'");
      expect(malloyLiteral("number", "42")).toBe("42");
      expect(malloyLiteral("number", "forty")).toBeUndefined();
      expect(malloyLiteral("boolean", true)).toBe("true");
      expect(malloyLiteral("date", "2024-01-31")).toBe("@2024-01-31");
      expect(malloyLiteral("timestamp", new Date("2024-01-31T09:30:00Z"))).toBe(
         "@2024-01-31 09:30:00",
      );
   });

   it("writes nothing for no value, or a type it cannot spell", () => {
      expect(malloyLiteral("filter<string>", "")).toBeUndefined();
      expect(malloyLiteral("filter<string>", null)).toBeUndefined();
      expect(malloyLiteral("geometry", "x")).toBeUndefined();
   });
});
