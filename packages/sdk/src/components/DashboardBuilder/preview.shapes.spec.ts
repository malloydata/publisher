// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { DashboardTile } from "./document";
import { previewTileQuery } from "./preview";
import { openDocument, spliced } from "./testing/fixtures";

const NONE =
   "# -line_chart -bar_chart -big_value -scatter_chart -shape_map -segment_map -viz";

const SHAPES = `## artifact { title="T" tiles=["a -> inl", "a -> comp", "a -> pipe", "a -> chain", "a -> paren", "a -> ref"] }
import "../m.malloy"

source: a is one extend {
  view: inl is { group_by: x }
  view: comp is { group_by: x } + { limit: 3 }
  view: pipe is v -> { select: x }
  view: chain is v + { limit: 3 } + { limit: 4 }
  view: paren is (v + { limit: 3 }) + { limit: 4 }
  view: ref is v
}`;

describe("the Default chart preview, for the shapes a real dashboard reads", () => {
   it("clears the saved chart only where the body has no named base view", async () => {
      const document = await openDocument(SHAPES);
      const annotations = Object.fromEntries(
         document.tiles.map((tile) => [
            tile.name,
            previewTileQuery(
               document,
               { ...tile, chart: "default" } as DashboardTile,
               new Set(),
            ).annotation,
         ]),
      );
      expect(annotations).toEqual({
         inl: NONE,
         comp: NONE,
         pipe: NONE,
         chain: undefined,
         paren: undefined,
         ref: undefined,
      });
   });
});

describe("a filter field is read and written as it stands", () => {
   const FILTERS = `##! experimental.givens
## artifact { title="T" tiles=["a -> x", "a -> y", "a -> z"] }
import "../m.malloy"

source: a is one extend {
  view: x is v + { where: created_at.year ~ $Y }
  view: y is v + { where: \`category\` ~ $C }
  view: z is v + { where: lower(\`date\`) ~ $A }
}`;

   it("keeps dotted paths and expressions verbatim, and a redundant quote untouched", async () => {
      const document = await openDocument(FILTERS);
      expect(document.tiles.map((t) => t.filters?.[0]?.field)).toEqual([
         "created_at.year",
         "category",
         "lower(`date`)",
      ]);
      const out = await spliced(FILTERS, (d) => {
         for (const tile of d.tiles) tile.colspan = 4;
      });
      expect(out).toContain("where: created_at.year ~ $Y");
      expect(out).toContain("where: `category` ~ $C");
      expect(out).toContain("where: lower(`date`) ~ $A");
   });
});
