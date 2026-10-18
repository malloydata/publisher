// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { tileTitle } from "../Dashboard/DashboardTile";
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
