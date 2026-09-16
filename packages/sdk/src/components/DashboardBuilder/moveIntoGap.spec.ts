// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { moveIntoGap } from "./layout";
import type { DashboardTile } from "./document";

const tile = (
   name: string,
   colspan: number,
   starts = false,
): DashboardTile => ({
   name,
   source: "a",
   declaration: { kind: "reference", from: `${name}_view` },
   colspan,
   ...(starts ? { break: true } : {}),
});

const rows = (tiles: DashboardTile[]) =>
   tiles.map((each) => each.break ?? false);
const names = (tiles: DashboardTile[]) => tiles.map((each) => each.name);

describe("moveIntoGap", () => {
   // The storefront overview with the trend narrowed to half width: a gap to
   // its right, because `kpis` breaks onto a fresh row, then the map beside
   // the kpis. The move that was impossible: the map, up into that gap.
   const overview = [
      tile("trend", 6),
      tile("kpis", 6, true),
      tile("map", 6),
      tile("category", 6),
   ];

   it("flows the tile into the gap, and the next row still starts where it did", () => {
      // After `trend` (index 0): slot 1 once `map` is taken out.
      const settled = moveIntoGap(overview, 2, 1);

      expect(names(settled)).toEqual(["trend", "map", "kpis", "category"]);
      // `map` arrives with no break, so it sits beside `trend`; `kpis` keeps
      // its break and still heads the second row. The positional rule would
      // have put the break on `map` by position and left the gap in place.
      expect(rows(settled)).toEqual([false, false, true, false]);
   });

   it("closes up the row the tile left, handing its row start on", () => {
      const grid = [
         tile("one", 6),
         tile("two", 6, true),
         tile("three", 6, true),
         tile("four", 6),
      ];
      // `three` started its row; moving it up beside `one` must not leave
      // `four` flowing into the row above, so `four` starts the row now.
      const settled = moveIntoGap(grid, 2, 1);

      expect(names(settled)).toEqual(["one", "three", "two", "four"]);
      expect(rows(settled)).toEqual([false, false, true, true]);
   });

   it("does not hand on a row start the follower already has", () => {
      const grid = [
         tile("one", 6),
         tile("two", 6, true),
         tile("three", 6, true),
      ];
      const settled = moveIntoGap(grid, 1, 1);
      // `two` was pulled up into the gap above itself. `three` already broke;
      // it is not given a second one, and the object is left alone.
      expect(rows(settled)).toEqual([false, false, true]);
      expect(settled[2]).toBe(grid[2]);
   });

   it("pulling a tile into the gap right above it moves it up, and its row start on", () => {
      // The tile has not moved in the array — but it has moved on the page:
      // `kpis` loses its break and flows up beside `trend`, and `map`, which
      // followed it, heads the row it left. The same hand-off as any other gap
      // drop; the array order happening to be unchanged does not exempt it.
      const settled = moveIntoGap(overview, 1, 1);
      expect(names(settled)).toEqual(names(overview));
      expect(rows(settled)).toEqual([false, false, true, false]);
   });

   it("leaves the tiles it did not touch as the objects they were", () => {
      const settled = moveIntoGap(overview, 2, 1);
      expect(settled[0]).toBe(overview[0]);
      expect(settled[2]).toBe(overview[1]);
      expect(settled[3]).toBe(overview[3]);
   });
});
