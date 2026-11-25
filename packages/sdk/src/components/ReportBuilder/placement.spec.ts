// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { DashboardTile, QueryTile } from "./document";
import {
   cellAt,
   dropAt,
   flowTiles,
   hasGaps,
   holdChartRows,
   minRowsOf,
   pack,
   placeAtEnd,
   inReadingOrder,
   placeWhereDrawn,
   settle,
   stepPlaced,
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

   it("rounds a text tile's bottom edge up, so its prose keeps every line", () => {
      const track = trackWidth(1200, 12);
      // A heading and two lines: 124px, under a row and a half, which rounded to one row
      // of 80px and left the last line under the tiles placed beneath.
      const intro: DashboardTile = {
         kind: "text",
         name: "intro",
         markdown: "# Hi\n\ntwo lines",
         colspan: 12,
      };
      const tiles: DashboardTile[] = [intro, tile("trend", { colspan: 12 })];
      const boxes = [
         { top: 0, left: 0, width: 1200, height: 124 },
         { top: 140, left: 0, width: 1200, height: 272 },
      ];
      const placed = where(placeWhereDrawn(tiles, boxes, 12, track));
      expect(placed[0]).toEqual({
         name: "intro",
         row: 1,
         col: 1,
         colspan: 12,
         rowspan: 2,
      });
      // The chart below starts where it was drawn; a text tile that grew a row is settled under it.
      expect(placed[1].row).toBeGreaterThanOrEqual(3);
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

describe("dropAt", () => {
   const tiles = () => [
      tile("map", { row: 1, col: 7, colspan: 6, rowspan: 6 }),
      tile("bar", { row: 1, col: 1, colspan: 6, rowspan: 3 }),
   ];

   it("lands under a tile whose lower part it was dropped on", () => {
      // Dropped at row 4, col 7: on the map's lower half, which starts at row 1.
      expect(where(dropAt(tiles(), 1, { row: 4, col: 7 }, 12))).toEqual([
         { name: "map", row: 1, col: 7, colspan: 6, rowspan: 6 },
         { name: "bar", row: 7, col: 7, colspan: 6, rowspan: 3 },
      ]);
   });

   it("pushes a tile down when dropped at or above its top", () => {
      expect(
         where(dropAt(tiles(), 1, { row: 1, col: 7 }, 12)).map((t) => [
            t.name,
            t.row,
         ]),
      ).toEqual([
         ["map", 4],
         ["bar", 1],
      ]);
   });
});

describe("placeAtEnd", () => {
   it("puts a new tile under everything on a placed report, at the left", () => {
      const tiles = [
         tile("a", { row: 1, col: 1, colspan: 6, rowspan: 3 }),
         tile("b", { row: 2, col: 7, colspan: 6, rowspan: 6 }),
         tile("new", { colspan: 6 }),
      ];
      expect(placeAtEnd(tiles, 2, 12)[2]).toMatchObject({ row: 8, col: 1 });
   });

   it("leaves a report that flows alone", () => {
      const tiles = [tile("a", { colspan: 6 }), tile("new")];
      expect(placeAtEnd(tiles, 1, 12)[1]).toEqual(tile("new"));
   });
});

describe("dropAt, far below", () => {
   it("lands no lower than right under the lowest other tile", () => {
      const tiles = [
         tile("a", { row: 1, col: 1, colspan: 6, rowspan: 3 }),
         tile("b", { row: 1, col: 7, colspan: 6, rowspan: 3 }),
      ];
      expect(dropAt(tiles, 1, { row: 40, col: 7 }, 12)[1].row).toBe(4);
   });
});

describe("pack", () => {
   it("slides every tile up into the gaps above it, never sideways", () => {
      const tiles = [
         tile("a", { row: 1, col: 1, colspan: 6, rowspan: 3 }),
         tile("b", { row: 7, col: 1, colspan: 6, rowspan: 2 }),
         tile("c", { row: 5, col: 7, colspan: 6, rowspan: 4 }),
      ];
      expect(hasGaps(tiles, 12)).toBe(true);
      expect(where(pack(tiles, 12))).toEqual([
         { name: "a", row: 1, col: 1, colspan: 6, rowspan: 3 },
         { name: "b", row: 4, col: 1, colspan: 6, rowspan: 2 },
         { name: "c", row: 1, col: 7, colspan: 6, rowspan: 4 },
      ]);
      expect(hasGaps(pack(tiles, 12), 12)).toBe(false);
   });

   it("stops a tile under the lowest tile it would pass through", () => {
      const tiles = [
         tile("wide", { row: 1, col: 1, colspan: 12, rowspan: 2 }),
         tile("low", { row: 9, col: 4, colspan: 4, rowspan: 2 }),
      ];
      expect(pack(tiles, 12)[1].row).toBe(3);
   });
});

describe("flowTiles", () => {
   it("takes every place off and keeps sizes, in reading order", () => {
      const tiles = [
         tile("b", { row: 4, col: 1, colspan: 6, rowspan: 3 }),
         tile("a", { row: 1, col: 7, colspan: 6, rowspan: 2 }),
      ];
      expect(where(flowTiles(tiles))).toEqual([
         { name: "a", row: undefined, col: undefined, colspan: 6, rowspan: 2 },
         { name: "b", row: undefined, col: undefined, colspan: 6, rowspan: 3 },
      ]);
   });
});

describe("placeWhereDrawn keeps rows where they are drawn", () => {
   it("takes each edge to its nearest row line, so stacked tiles still touch and nothing is pushed", () => {
      const track = trackWidth(1200, 12);
      const half = 6 * (track + 16) - 16;
      // Four rows of two auto tiles, each 300px tall with a 16px gap: drawn
      // tops at 0, 316, 632, 948.
      const tiles = Array.from({ length: 8 }, (_, i) =>
         tile(`t${i}`, { colspan: 6 }),
      );
      const boxes = tiles.map((_, i) => ({
         top: Math.floor(i / 2) * 316,
         left: i % 2 === 0 ? 0 : half + 16,
         width: half,
         height: 300,
      }));
      const placed = placeWhereDrawn(tiles, boxes, 12, track);
      // Every row's top stays within half a row (48px) of where it was drawn.
      for (const [i, each] of placed.entries())
         expect(
            Math.abs((each.row! - 1) * 96 - boxes[i].top),
         ).toBeLessThanOrEqual(48);
      // And each row starts right where the one above ends: no row was pushed.
      for (let r = 2; r < 8; r += 2)
         expect(placed[r].row).toBe(
            placed[r - 2].row! + placed[r - 2].rowspan!,
         );
   });
});

describe("minRowsOf", () => {
   const drawn = (html: string) => {
      const frame = document.createElement("div");
      frame.innerHTML = html;
      return frame;
   };
   it("gives a chart three rows, value cards and a table two, and prose one", () => {
      expect(minRowsOf(tile("chart"), drawn('<svg class="plot"></svg>'))).toBe(
         3,
      );
      expect(
         minRowsOf(tile("kpis"), drawn('<div class="malloy-big-value"></div>')),
      ).toBe(2);
      expect(
         minRowsOf(tile("rows"), drawn('<div class="malloy-table"></div>')),
      ).toBe(2);
      expect(
         minRowsOf({ kind: "text", name: "note", markdown: "" }, null),
      ).toBe(1);
   });

   it("takes a query tile for a chart until it has drawn", () => {
      expect(minRowsOf(tile("t"))).toBe(3);
   });

   it("gives a dashboard with a chart beside its cards a chart's rows", () => {
      expect(
         minRowsOf(
            tile("mixed"),
            drawn(
               '<div class="malloy-dashboard"><div class="malloy-big-value"></div><div class="malloy-chart"></div></div>',
            ),
         ),
      ).toBe(3);
   });

   it("lets a chart inside a card or a table's cell size with its host", () => {
      expect(
         minRowsOf(
            tile("spark"),
            drawn(
               '<div class="malloy-big-value"><div class="malloy-chart"></div></div>',
            ),
         ),
      ).toBe(2);
      expect(
         minRowsOf(
            tile("cells"),
            drawn(
               '<div class="malloy-table"><div class="malloy-chart"></div></div>',
            ),
         ),
      ).toBe(2);
   });
});

describe("holdChartRows", () => {
   it("raises a tile switched to a plotted chart to a chart's rows", () => {
      expect(
         holdChartRows(tile("t", { rowspan: 2, chart: "line_chart" })).rowspan,
      ).toBe(3);
   });
   it("leaves value cards, a table, and a tile on auto height alone", () => {
      expect(
         holdChartRows(tile("t", { rowspan: 2, chart: "big_value" })).rowspan,
      ).toBe(2);
      expect(
         holdChartRows(tile("t", { rowspan: 2, chart: "none" })).rowspan,
      ).toBe(2);
      expect(
         holdChartRows(tile("t", { chart: "bar_chart" })).rowspan,
      ).toBeUndefined();
   });
});

describe("stepPlaced", () => {
   const at = (tiles: DashboardTile[], name: string) => {
      const t = tiles.find((each) => each.name === name)!;
      return [t.row, t.col];
   };
   const stacked = () => [
      tile("a", { row: 1, col: 1, colspan: 6, rowspan: 4 }),
      tile("b", { row: 5, col: 1, colspan: 6, rowspan: 3 }),
   ];

   it("swaps a tile down past the one under it", () => {
      const next = stepPlaced(stacked(), 0, 1, 0, 12);
      expect(at(next, "b")).toEqual([1, 1]);
      expect(at(next, "a")).toEqual([4, 1]);
   });

   it("swaps a tile up past the one over it, the mirror of down", () => {
      const next = stepPlaced(stacked(), 1, -1, 0, 12);
      expect(at(next, "b")).toEqual([1, 1]);
      expect(at(next, "a")).toEqual([4, 1]);
   });

   it("swaps tiles side by side", () => {
      const tiles = [
         tile("a", { row: 1, col: 1, colspan: 4, rowspan: 3 }),
         tile("b", { row: 1, col: 5, colspan: 8, rowspan: 3 }),
      ];
      const right = stepPlaced(tiles, 0, 0, 1, 12);
      expect(at(right, "b")).toEqual([1, 1]);
      expect(at(right, "a")).toEqual([1, 9]);
      const left = stepPlaced(tiles, 1, 0, -1, 12);
      expect(at(left, "b")).toEqual([1, 1]);
      expect(at(left, "a")).toEqual([1, 9]);
   });

   it("steps one cell into empty space", () => {
      const next = stepPlaced(stacked(), 0, 0, 1, 12);
      expect(at(next, "a")).toEqual([1, 2]);
      expect(at(next, "b")).toEqual([5, 1]);
   });

   it("pushes down what it lands on when two tiles are in the way", () => {
      const tiles = [
         tile("a", { row: 1, col: 1, colspan: 12, rowspan: 3 }),
         tile("b", { row: 4, col: 1, colspan: 6, rowspan: 3 }),
         tile("c", { row: 4, col: 7, colspan: 6, rowspan: 3 }),
      ];
      const next = stepPlaced(tiles, 0, 1, 0, 12);
      expect(at(next, "a")).toEqual([2, 1]);
      expect(at(next, "b")).toEqual([5, 1]);
      expect(at(next, "c")).toEqual([5, 7]);
   });

   it("leaves the tiles as they were at the grid's edge", () => {
      const tiles = stacked();
      const next = stepPlaced(tiles, 0, -1, 0, 12);
      expect(next.every((each, i) => each === tiles[i])).toBe(true);
   });
});
