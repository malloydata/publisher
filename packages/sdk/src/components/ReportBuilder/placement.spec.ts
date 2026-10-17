// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { DashboardTile, QueryTile } from "./document";
import {
   cellAt,
   inReadingOrder,
   placeWhereDrawn,
   settle,
   trackWidth,
} from "./placement";

const tile = (name: string, layout: Partial<QueryTile> = {}): QueryTile => ({
   name,
   source: "a",
   declaration: { kind: "inline" },
   ...layout,
});
const where = (tiles: DashboardTile[]) =>
   tiles.map(({ name, row, col, colspan, rowspan }) => ({
      name,
      row,
      col,
      colspan,
      rowspan,
   }));

describe("settle", () => {
   it("pushes a tile the moved one now covers straight down", () => {
      const tiles = [
         tile("a", { row: 1, col: 1, colspan: 6, rowspan: 4 }),
         tile("b", { row: 1, col: 4, colspan: 6, rowspan: 3 }),
      ];
      expect(where(settle(tiles, 12, 1))).toEqual([
         { name: "a", row: 4, col: 1, colspan: 6, rowspan: 4 },
         { name: "b", row: 1, col: 4, colspan: 6, rowspan: 3 },
      ]);
   });

   it("pushes in a chain, top to bottom", () => {
      const tiles = [
         tile("a", { row: 1, col: 1, colspan: 12, rowspan: 2 }),
         tile("b", { row: 3, col: 1, colspan: 12, rowspan: 2 }),
         tile("big", { row: 1, col: 1, colspan: 12, rowspan: 3 }),
      ];
      expect(where(settle(tiles, 12, 2)).map((t) => [t.name, t.row])).toEqual([
         ["a", 4],
         ["b", 6],
         ["big", 1],
      ]);
   });

   it("leaves empty cells empty: nothing is pulled up", () => {
      const tiles = [
         tile("a", { row: 1, col: 1, colspan: 6, rowspan: 2 }),
         tile("b", { row: 9, col: 1, colspan: 6, rowspan: 2 }),
      ];
      expect(settle(tiles, 12, 0)[1].row).toBe(9);
   });

   it("pulls a tile hanging off the right edge back onto the grid", () => {
      expect(
         settle([tile("a", { row: 1, col: 10, colspan: 6 })], 12, 0)[0].col,
      ).toBe(7);
   });

   it("counts a tile with no rowspan at its kind's default rows", () => {
      const tiles = [
         tile("a", { row: 1, col: 1, colspan: 6 }),
         tile("b", { row: 5, col: 1, colspan: 6, rowspan: 2 }),
      ];
      // a is 5 rows by default, so b at row 5 is covered and goes to row 6.
      expect(settle(tiles, 12, 0)[1].row).toBe(6);
   });

   it("leaves a tile that is not placed for the grid to place", () => {
      const tiles = [tile("a", { row: 1, col: 1 }), tile("free")];
      expect(settle(tiles, 12, 0)[1]).toEqual(tile("free"));
   });
});

describe("cellAt", () => {
   const track = trackWidth(1200 - 0, 12);
   it("snaps a corner to the nearest row and column", () => {
      expect(cellAt(0, 0, 6, 12, track)).toEqual({ row: 1, col: 1 });
      expect(cellAt(3 * (track + 16) + 10, 2 * 96 - 20, 6, 12, track)).toEqual({
         row: 3,
         col: 4,
      });
   });

   it("keeps a tile on the grid at its edges", () => {
      expect(cellAt(-500, -500, 6, 12, track)).toEqual({ row: 1, col: 1 });
      expect(cellAt(5000, 0, 6, 12, track)).toEqual({ row: 1, col: 7 });
   });
});

describe("placeWhereDrawn", () => {
   it("reads each tile's row, column, width and height off where it is drawn", () => {
      const track = trackWidth(1200, 12);
      const step = track + 16;
      const tiles = [
         tile("kpis", { colspan: 12, break: true }),
         tile("trend"),
         tile("map", { colspan: 6 }),
      ];
      const boxes = [
         { top: 0, left: 0, width: 1200, height: 272 },
         { top: 288, left: 0, width: 6 * step - 16, height: 492 },
         { top: 288, left: 6 * step, width: 6 * step - 16, height: 492 },
      ];
      expect(where(placeWhereDrawn(tiles, boxes, 12, track))).toEqual([
         { name: "kpis", row: 1, col: 1, colspan: 12, rowspan: 3 },
         { name: "trend", row: 4, col: 1, colspan: 6, rowspan: 5 },
         { name: "map", row: 4, col: 7, colspan: 6, rowspan: 5 },
      ]);
      // A row break means nothing once tiles are placed.
      expect(placeWhereDrawn(tiles, boxes, 12, track)[0].break).toBeUndefined();
   });
});

describe("inReadingOrder", () => {
   it("orders placed tiles top to bottom, then left to right, unplaced last", () => {
      const tiles = [
         tile("free"),
         tile("c", { row: 3, col: 1 }),
         tile("b", { row: 1, col: 7 }),
         tile("a", { row: 1, col: 1 }),
      ];
      expect(inReadingOrder(tiles).map((t) => t.name)).toEqual([
         "a",
         "b",
         "c",
         "free",
      ]);
   });
});
