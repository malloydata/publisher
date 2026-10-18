// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { QueryTile } from "./document";
import { previewTileQuery } from "./preview";
import { openDocument, queryTile, spliced } from "./testing/fixtures";

const NONE =
   "# -line_chart -bar_chart -scatter_chart -shape_map -segment_map -combo_chart";

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
               { ...tile, chart: "default" } as QueryTile,
               new Set(),
            ).annotation,
         ]),
      );
      // No tile here has a colour, so each one that runs its own saved view
      // also takes off any colour that view was saved with.
      const RESET = "# -theme.palette.series";
      expect(annotations).toEqual({
         inl: `${NONE}\n${RESET}`,
         comp: `${NONE}\n${RESET}`,
         pipe: `${NONE}\n${RESET}`,
         chain: RESET,
         paren: RESET,
         ref: undefined,
      });
   });
});

describe("the Theme colour preview, for the colour lines a real dashboard reads", () => {
   const COLORS = `## artifact { title="T" tiles=["a -> picked", "a -> listed", "a -> block", "a -> spelled"] }
import "../m.malloy"

source: a is one extend {
  # theme.palette.series=["#7f9862"]
  view: picked is { group_by: x }
  # theme.palette.series=["#111111", "#222222"]
  view: listed is { group_by: x }
  # theme { palette.series=["#111111"] }
  view: block is { group_by: x }
  # theme.palette.series = [ '#111111' ]
  view: spelled is { group_by: x }
}`;

   it("takes a builder colour off on Theme, and keeps a palette the author wrote", async () => {
      const document = await openDocument(COLORS);
      const annotations = Object.fromEntries(
         document.tiles.map((tile) => {
            // Theme: what the Color item's Theme leaves on the tile.
            const { color: _picked, ...theme } = tile as QueryTile;
            return [
               tile.name,
               previewTileQuery(document, theme as QueryTile, new Set())
                  .annotation,
            ];
         }),
      );
      expect(annotations).toEqual({
         picked: "# -theme.palette.series",
         listed: undefined,
         block: undefined,
         spelled: undefined,
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
      expect(
         document.tiles.map(
            (t) =>
               queryTile(document, document.tiles.indexOf(t)).filters?.[0]
                  ?.field,
         ),
      ).toEqual(["created_at.year", "category", "lower(`date`)"]);
      const out = await spliced(FILTERS, (d) => {
         for (const tile of d.tiles) tile.colspan = 4;
      });
      expect(out).toContain("where: created_at.year ~ $Y");
      expect(out).toContain("where: `category` ~ $C");
      expect(out).toContain("where: lower(`date`) ~ $A");
   });
});
