// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { fallbackTileTitle, tileTitle } from "../Dashboard/tileTitle";
import { tileDisplayTitle } from "./tileDisplayTitle";
import type { PackageCatalog } from "./catalog";
import type { DashboardDocument, QueryTile } from "./document";
import { catalogViewOfTile } from "./tileCatalogView";
import { labelOf } from "./viewAnnotations";

/**
 * A tile is named in the builder as the reader titles it: its own label, else
 * the view's `# label`, else a name derived from the view. These are the
 * pieces the builder's title is made of when the served manifest has none.
 */
describe("a tile's title, when nothing serves one", () => {
   it("stays fast on text built to make a backtracking pattern slow", () => {
      const started = performance.now();
      tileTitle(`a${"\t".repeat(50_000)}`);
      tileTitle(`a${"+{{".repeat(20_000)}`);
      expect(performance.now() - started).toBeLessThan(100);
   });

   it("is the view's name, not a refinement's text", () => {
      expect(tileTitle("flights -> by_carrier + { where: carrier ~ $C }")).toBe(
         "By carrier",
      );
      expect(tileTitle("flights -> by_carrier")).toBe("By carrier");
      // An inline stage has no name to derive one from.
      expect(tileTitle("flights -> { group_by: carrier }")).toBe(
         "flights -> { group_by: carrier }",
      );
   });

   it("reads a view's # label in every spelling, the last one winning", () => {
      expect(labelOf(['# label="Plain"\n'])).toBe("Plain");
      expect(labelOf(["# label='Plain'\n"])).toBe("Plain");
      expect(labelOf(["# label=Plain\n"])).toBe("Plain");
      expect(labelOf(['# bar_chart label="Say \\"hi\\""\n'])).toBe('Say "hi"');
      expect(labelOf(['# label="One"\n', '# label="Two"\n'])).toBe("Two");
   });

   it("ignores a label that is not the view's own", () => {
      expect(labelOf(['#(doc) label="no"\n'])).toBeUndefined();
      expect(labelOf(['# subtitle="see label=x"\n'])).toBeUndefined();
      expect(labelOf(['# big_value { label="Inner" }\n'])).toBeUndefined();
      expect(labelOf(['# relabel="x"\n'])).toBeUndefined();
   });
});

describe("the catalog view behind a tile", () => {
   const catalog: PackageCatalog = {
      sources: [
         {
            name: "flights",
            modelPath: "flights.malloy",
            views: [
               { name: "by_carrier", label: "Flights by airline" },
               { name: "kpis", label: "Key figures" },
            ],
            givens: [],
            fields: [],
         },
      ],
   };
   const document = { sources: [], tiles: [] } as unknown as DashboardDocument;
   const tile = (fields: Partial<QueryTile>) =>
      ({ name: "by_carrier", source: "flights", ...fields }) as QueryTile;

   it("is the served view a refined entry names", () => {
      // `"flights -> by_carrier + { where: … }"` reads as the view it refines.
      const refined = tile({ declaration: { kind: "inherited" } });
      expect(catalogViewOfTile(refined, document, catalog)?.label).toBe(
         "Flights by airline",
      );
   });

   it("is the view a named query refines, not the query's own name", () => {
      const named = tile({
         name: "kpis_tile",
         declaration: { kind: "named", from: "kpis" },
      });
      expect(catalogViewOfTile(named, document, catalog)?.label).toBe(
         "Key figures",
      );
   });
});

describe("a query tile's title with no label", () => {
   it("is the view's when the builder named the query for it", () => {
      expect(fallbackTileTitle("top_brands_tile", "orders -> top_brands")).toBe(
         "Top brands",
      );
      expect(
         fallbackTileTitle("top_brands_tile_2", "orders -> top_brands"),
      ).toBe("Top brands");
   });

   it("is the query's own name otherwise", () => {
      expect(fallbackTileTitle("kpis_tile", "orders -> key_figures")).toBe(
         "Kpis tile",
      );
      expect(
         fallbackTileTitle("revenue_by_brand", "orders -> top_brands"),
      ).toBe("Revenue by brand");
      expect(
         fallbackTileTitle("top_brands_tile_x", "orders -> top_brands"),
      ).toBe("Top brands tile x");
      expect(fallbackTileTitle("top_brands_tile")).toBe("Top brands tile");
   });

   it("is the same in the builder, and a label always wins", () => {
      const named = (name: string, from: string, label?: string) =>
         ({
            name,
            source: "orders",
            declaration: { kind: "named", from },
            ...(label ? { label } : {}),
         }) as QueryTile;
      expect(tileDisplayTitle(named("top_brands_tile", "top_brands"))).toBe(
         "Top brands",
      );
      expect(tileDisplayTitle(named("kpis_tile", "key_figures"))).toBe(
         "Kpis tile",
      );
      expect(
         tileDisplayTitle(named("top_brands_tile", "top_brands", "Brands")),
      ).toBe("Brands");
   });
});
