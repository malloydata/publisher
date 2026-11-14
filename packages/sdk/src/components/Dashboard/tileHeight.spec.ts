// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   GRID_GAP_PX,
   MAX_TILE_ROWS,
   MIN_TILE_ROWS,
   TILE_ROW_PX,
   placedArea,
   readingOrder,
   rowsForHeight,
   tileHeightPx,
} from "./DashboardGrid";

describe("tile heights in grid rows", () => {
   // A tile N rows tall spans the N-1 gaps between them too, so a tile placed
   // on explicit grid rows later lands on exactly the height it has now.
   it("counts the gaps between the rows a tile spans", () => {
      expect(tileHeightPx(1)).toBe(TILE_ROW_PX);
      expect(tileHeightPx(4)).toBe(4 * TILE_ROW_PX + 3 * GRID_GAP_PX);
   });

   it("reads a drawn height back as the rows it is closest to", () => {
      for (let rows = MIN_TILE_ROWS; rows <= MAX_TILE_ROWS; rows++) {
         expect(rowsForHeight(tileHeightPx(rows))).toBe(rows);
         expect(rowsForHeight(tileHeightPx(rows) + 20)).toBe(rows);
      }
   });

   it("holds a dragged height to the rows the builder offers", () => {
      expect(rowsForHeight(0)).toBe(MIN_TILE_ROWS);
      expect(rowsForHeight(100_000)).toBe(MAX_TILE_ROWS);
   });
});

describe("placed tiles", () => {
   it("sits a placed tile on its row and column, spanning its rows", () => {
      expect(placedArea({ row: 3, col: 7, colspan: 6 }, 12, 4)).toEqual({
         gridRow: "3 / span 4",
         gridColumn: "7 / span 6",
      });
   });

   it("pulls a tile hanging off the right edge back onto the grid", () => {
      expect(
         placedArea({ row: 1, col: 10, colspan: 6 }, 12, 5).gridColumn,
      ).toBe("7 / span 6");
   });

   it("leaves a tile with no place for the grid to fill in", () => {
      expect(placedArea({ colspan: 4 }, 12, 5)).toEqual({
         gridRow: "span 5",
         gridColumn: "span 4",
      });
   });

   it("orders tiles for one column top to bottom, then left to right", () => {
      const order = [
         { row: 2, col: 7 },
         { row: 2, col: 1 },
         { row: 1, col: 9 },
         {},
      ].map(readingOrder);
      expect([...order].sort((a, b) => a - b)).toEqual([
         order[2],
         order[1],
         order[0],
         order[3],
      ]);
   });
});
