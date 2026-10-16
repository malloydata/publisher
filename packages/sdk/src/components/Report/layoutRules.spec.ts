// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   DEFAULT_COLUMNS,
   GRID_GAP_PX,
   MIN_TILE_PX,
   defaultTileSpan,
} from "../Dashboard/DashboardGrid";
import {
   LINEAR_TABLE_MAX_HEIGHT,
   TILE_MAX_HEIGHT,
} from "../RenderedResult/resultSizing";
import { layoutRules, writtenSpan } from "./layoutRules";

describe("layoutRules", () => {
   it("lays a linear report in one column of full-width tiles that grow to fit, a table capped, added between others", () => {
      expect(layoutRules("linear", 12)).toEqual({
         layout: "linear",
         columns: 1,
         widths: false,
         insertBetween: true,
         tileMaxHeight: undefined,
         tableMaxHeight: LINEAR_TABLE_MAX_HEIGHT,
      });
      expect(LINEAR_TABLE_MAX_HEIGHT).toBe(700);
   });

   it("lays a grid across its declared columns, with widths, added at the end", () => {
      expect(layoutRules("grid", 12)).toEqual({
         layout: "grid",
         columns: 12,
         widths: true,
         insertBetween: false,
         tileMaxHeight: TILE_MAX_HEIGHT,
         tableMaxHeight: undefined,
      });
   });

   it("reads no layout as a grid, at the default width", () => {
      expect(layoutRules(undefined).columns).toBe(DEFAULT_COLUMNS);
      expect(layoutRules(undefined).layout).toBe("grid");
   });

   it("gives a one-column grid no widths to set", () => {
      expect(layoutRules("grid", 1).widths).toBe(false);
   });
});

describe("writtenSpan", () => {
   // The reader's own rule for a tile with no colspan is the oracle: absent is
   // drawn at `defaultTileSpan`, so a written 1 repeats the default only where
   // that is 1 at every width the grid is laid out in columns.
   it("writes no width for one column of a one- or two-column grid", () => {
      expect(writtenSpan(1, 2)).toBeUndefined();
      expect(writtenSpan(1, 1)).toBeUndefined();
      for (let px = 2 * MIN_TILE_PX + GRID_GAP_PX; px <= 2400; px += 50)
         expect(defaultTileSpan(2, px, MIN_TILE_PX)).toBe(1);
   });

   it("keeps an explicit 1 where a tile with no width can draw wider", () => {
      // Three columns in a 600px box: an unset tile is widened past one column.
      expect(defaultTileSpan(3, 600, MIN_TILE_PX)).toBeGreaterThan(1);
      expect(writtenSpan(1, 3)).toBe(1);
      expect(writtenSpan(1, 12)).toBe(1);
      // And in two columns while the builder draws an unset tile wider.
      expect(writtenSpan(1, 2, 2)).toBe(1);
   });

   it("writes every other width as it is", () => {
      expect(writtenSpan(2, 2)).toBe(2);
      expect(writtenSpan(6, 12)).toBe(6);
   });
});
