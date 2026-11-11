// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { previewTileQuery } from "./preview";
import { seriesColorOf } from "./chartLine";
import { openDocument, queryTile, spliced } from "./testing/fixtures";

const FILE = (tags: string) => `##! experimental.givens
## artifact { title="T" tiles=["a -> x", "a -> y"] } dashboard { columns=12 }
import { one } from "../m.malloy"

source: a is one extend {
${tags}  # bar_chart
  view: x is vx

  view: y is vy
}`;

const colorOf = async (text: string, at: number) =>
   queryTile(await openDocument(text), at).color;

describe("a tile's chart colour", () => {
   it("writes a picked colour above the declaration, and reads it back", async () => {
      const out = await spliced(FILE(""), (d) => {
         queryTile(d, 0).color = "#1f6aa8";
      });
      expect(out).toContain('  # theme.palette.series=["#1f6aa8"]\n');
      expect(await colorOf(out, 0)).toBe("#1f6aa8");
      // The chart line it sits beside is left alone.
      expect(out).toContain("  # bar_chart\n");
      expect(await colorOf(out, 1)).toBeUndefined();
   });

   it("replaces the colour in place, and takes the line away for the theme's own", async () => {
      const coloured = FILE('  # theme.palette.series=["#1f6aa8"]\n');
      const changed = await spliced(coloured, (d) => {
         queryTile(d, 0).color = "#c2410c";
      });
      expect(changed).toContain('# theme.palette.series=["#c2410c"]');
      expect(changed).not.toContain("#1f6aa8");
      const removed = await spliced(coloured, (d) => {
         delete queryTile(d, 0).color;
      });
      expect(removed).not.toContain("theme.palette.series");
      expect(removed).toContain("  # bar_chart\n  view: x is vx");
   });

   it("reads the colour the last line draws when two colour lines are written", async () => {
      const builderLast = FILE(
         '  # theme.palette.series=["#ff0080", "#ff6b00"]\n' +
            '  # theme.palette.series=["#1f6aa8"]\n',
      );
      const drawn = queryTile(await openDocument(builderLast), 0);
      expect(drawn.color).toBe("#1f6aa8");
      expect(drawn.ownPalette).toBe(true);
      const paletteLast = FILE(
         '  # theme.palette.series=["#1f6aa8"]\n' +
            '  # theme.palette.series=["#ff0080", "#ff6b00"]\n',
      );
      const palette = queryTile(await openDocument(paletteLast), 0);
      expect(palette.color).toBeUndefined();
      expect(palette.ownPalette).toBe(true);
   });

   it("leaves an author's whole palette alone through an edit", async () => {
      const palette = '  # theme.palette.series=["#ff0080", "#ff6b00"]\n';
      expect(await colorOf(FILE(palette), 0)).toBeUndefined();
      const out = await spliced(FILE(palette), (d) => {
         queryTile(d, 0).label = "Revenue";
      });
      expect(out).toContain(palette);
   });

   it("previews a new colour before it is saved", async () => {
      const document = await openDocument(FILE(""));
      const tile = queryTile(document, 0);
      tile.color = "#1f6aa8";
      const query = previewTileQuery(document, tile, new Set());
      expect(query.annotation?.split("\n")).toContain(
         '# theme.palette.series=["#1f6aa8"]',
      );
   });
});

describe("seriesColorOf", () => {
   it("reads only the one-colour line the builder writes", () => {
      expect(seriesColorOf('# theme.palette.series=["#1F6AA8"]')).toBe(
         "#1f6aa8",
      );
      expect(
         seriesColorOf('# theme.palette.series=["#1f6aa8","#000000"]'),
      ).toBe(undefined);
      expect(seriesColorOf('# theme.palette.series=["red"]')).toBeUndefined();
      expect(seriesColorOf("# bar_chart")).toBeUndefined();
   });
});
