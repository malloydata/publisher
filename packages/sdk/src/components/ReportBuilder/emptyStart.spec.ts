// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   buildSuggestQuery,
   SUGGEST_OPTION_LIMIT,
} from "../../hooks/useSuggestOptions";
import { newLocalGiven } from "./controls";
import { readDashboardDocument, readFailed } from "./readDocument";
import { openDocument, refused, splice, spliced } from "./testing/fixtures";
import { spliceFailed } from "./spliceDocument";
import { queryTile } from "./testing/fixtures";
import { tileKey } from "./document";

const EMPTY = `##! experimental.givens
## artifact { title="T" tiles=[] } dashboard { columns=12 }
import { one } from "../m.malloy"
`;

const ONE_TILE = `##! experimental.givens
## artifact { title="T" tiles=["a -> x"] } dashboard { columns=12 }
import { one } from "../m.malloy"

source: a is one extend {
  view: x is vx
}`;

describe("a dashboard with an explicit, empty tile list", () => {
   it("opens with no tiles", async () => {
      const doc = await openDocument(EMPTY);
      expect(doc.tiles).toEqual([]);
      expect(doc.title).toBe("T");
   });

   it("still refuses a file whose artifact tag has no tiles key", async () => {
      const result = await readDashboardDocument(
         '## artifact { title="T" } dashboard { columns=12 }\nimport "../m.malloy"\n',
      );
      expect(readFailed(result)).toBe(true);
   });

   it("still refuses a list holding entries the builder cannot read", async () => {
      const result = await readDashboardDocument(
         '## artifact { title="T" tiles=[text intro] }\nimport "../m.malloy"\n',
      );
      expect(readFailed(result)).toBe(true);
   });

   it("takes its first tile, creating the extension", async () => {
      const out = await spliced(EMPTY, (d) => {
         d.sources.push({ name: "one_tiles", base: "one" });
         d.tiles.push({
            name: "vx_tile",
            source: "one_tiles",
            declaration: { kind: "reference", from: "vx" },
            colspan: 6,
         });
      });
      expect(out).toContain('tiles=["one_tiles -> vx_tile"]');
      const back = await openDocument(out);
      expect(back.tiles.map(tileKey)).toEqual(["one_tiles.vx_tile"]);
   });

   it("refuses to remove the last tile, because an empty file is not served", async () => {
      const reason = await refused(ONE_TILE, (d) => {
         d.tiles = [];
      });
      expect(reason).toContain("not served");
   });

   it("still removes a tile when another remains", async () => {
      const two = ONE_TILE.replace(
         '["a -> x"]',
         '["a -> x", "a -> y"]',
      ).replace("view: x is vx", "view: x is vx\n  view: y is vy");
      const result = await splice(two, (d) => {
         d.tiles.splice(1, 1);
      });
      expect(spliceFailed(result)).toBe(false);
   });
});

describe("a filter on a joined dimension", () => {
   it("keeps the whole path on the declared control", () => {
      const given = newLocalGiven({
         name: "CATEGORY",
         label: "Category",
         kind: "select",
         field: "products.category",
         source: "order_items",
      });
      expect(given.suggest).toEqual({
         source: "order_items",
         dimension: "products.category",
      });
   });

   it("is written quoted, reads back whole, and forms the options query", async () => {
      const source = `##! experimental.givens
## artifact { title="T" tiles=["a -> x"] }
import { one, products } from "../m.malloy"

source: a is one extend {
  view: x is vx
}`;
      const out = await spliced(source, (d) => {
         d.localGivens = [
            newLocalGiven({
               name: "CATEGORY",
               label: "Category",
               kind: "select",
               field: "products.category",
               source: "a",
            }),
         ];
         queryTile(d, 0).filters = [
            { field: "products.category", given: "CATEGORY" },
         ];
      });
      expect(out).toContain(
         '# label="Category" control=select suggest { source=a dimension="products.category" }',
      );
      const back = await openDocument(out);
      const suggest = back.localGivens?.[0].suggest;
      expect(suggest).toEqual({ source: "a", dimension: "products.category" });
      expect(buildSuggestQuery(suggest!.source!, suggest!.dimension)).toBe(
         `run: a -> {\n  group_by: products.category\n  order_by: category asc\n  limit: ${SUGGEST_OPTION_LIMIT}\n}`,
      );
   });
});
