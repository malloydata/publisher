// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { GivenValue } from "../../hooks/givenValue";
import type { DashboardDocument, DashboardTile } from "./document";
import { previewGivens, previewTileQuery } from "./preview";

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
         "overview -> key_figures + { where: category ~ $CATEGORY, where: created_at >= $SINCE }",
      );
      expect(q.givenNames).toEqual(["CATEGORY", "SINCE"]);
   });

   it("sends a tile only the givens it binds", () => {
      // `trend` binds CATEGORY alone: SINCE moving must not re-run it, and
      // unbinding a tile is what takes a control's effect off it.
      const q = previewTileQuery(document, document.tiles[1], runnable);
      expect(q.givenNames).toEqual(["CATEGORY"]);
      const bare = previewTileQuery(document, document.tiles[2], runnable);
      expect(bare.expression).toBe("overview -> by_state");
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
         "overview -> key_figures + { where: category ~ $CATEGORY, where: created_at >= @2024-01-31 }",
      );
      expect(q.givenNames).toEqual(["CATEGORY"]);
   });

   it("leaves out an unsent given with no value yet, and one nobody declares", () => {
      const q = previewTileQuery(document, document.tiles[0], new Set());
      expect(q.expression).toBe("overview -> key_figures");
      expect(q.givenNames).toEqual([]);
      const stray: DashboardTile = tile("t", "v", [
         { field: "x", given: "NOBODY" },
      ]);
      expect(
         previewTileQuery(document, stray, new Set(), new Map([["NOBODY", 1]]))
            .expression,
      ).toBe("overview -> v");
   });

   it("runs an inline tile the same as a reference, on its own name", () => {
      // An inline body still names a view of the extension — `view: x is {
      // … }` is as much a named view as `view: x is base_view` — so a
      // binding takes the same `source -> x + { where: … }` path a reference
      // tile does, rather than being sent unrefined.
      const inline: DashboardTile = {
         name: "kpis",
         source: "overview",
         declaration: { kind: "inline" },
         filters: [{ field: "category", given: "CATEGORY" }],
      };
      const q = previewTileQuery(document, inline, runnable);
      expect(q.expression).toBe(
         "overview -> kpis + { where: category ~ $CATEGORY }",
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
