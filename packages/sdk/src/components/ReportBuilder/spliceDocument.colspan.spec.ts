// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { withSpan, writtenSpan } from "../Report/layoutRules";
import { addTileToDocument } from "./addTileToDocument";
import { openDocument, queryTile, spliced } from "./testing/fixtures";

/** A grid of `columns` (undeclared: the default two), its first tile carrying `tags`. */
const FILE = (
   columns: number | undefined,
   tags: string,
) => `##! experimental.givens
## artifact { title="T" tiles=["a -> x", "a -> y"] }${
   columns === undefined ? "" : ` dashboard { columns=${columns} }`
}
import { one } from "../m.malloy"

source: a is one extend {
${tags}  view: x is vx

  view: y is vy
}`;

const colspanLines = (text: string) =>
   text.split("\n").filter((line) => /#\s*colspan/.test(line));

describe("a tile's width, as the builder writes it", () => {
   it("takes the colspan line away when a width is set back to one column of two", async () => {
      const out = await spliced(FILE(undefined, "  # colspan=2\n"), (d) => {
         d.tiles[0] = withSpan(queryTile(d, 0), 1, 2);
      });
      expect(colspanLines(out)).toEqual([]);
      expect(queryTile(await openDocument(out), 0).colspan).toBeUndefined();
   });

   it("keeps an explicit one column where a tile with no width draws wider", async () => {
      const out = await spliced(FILE(4, "  # colspan=2\n"), (d) => {
         d.tiles[0] = withSpan(queryTile(d, 0), 1, 4);
      });
      expect(colspanLines(out)).toEqual(["  # colspan=1"]);
   });

   it("adds a tile to a two-column grid with no colspan line, and to a wider grid with half of it", async () => {
      for (const [columns, want] of [
         [undefined, []],
         [2, []],
         [4, ["  # colspan=2"]],
      ] as const) {
         const grid = columns ?? 2;
         const colspan = writtenSpan(Math.ceil(grid / 2), grid);
         const out = await spliced(FILE(columns, ""), (d) =>
            addTileToDocument(
               d,
               {
                  base: "one",
                  modelPath: "m.malloy",
                  view: "vz",
                  ...(colspan !== undefined ? { colspan } : {}),
               },
               {
                  modelPath: "dashboards/x.malloy",
                  textHeld: false,
                  widths: true,
               },
            ),
         );
         expect(colspanLines(out)).toEqual([...want]);
      }
   });
});
