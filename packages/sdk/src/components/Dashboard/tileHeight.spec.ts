// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   GRID_GAP_PX,
   MAX_TILE_ROWS,
   MIN_TILE_ROWS,
   TILE_ROW_PX,
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
