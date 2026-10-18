// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { act, renderHook } from "@testing-library/react";
import { describe, expect, it, mock } from "bun:test";
import type { PointerEvent } from "react";
import { GRID_GAP_PX, TILE_ROW_PX } from "../Dashboard/DashboardGrid";
import type { DashboardTile } from "./document";
import {
   edgeCursor,
   useEdgeResize,
   withShape,
   type EdgeResult,
   type Edges,
} from "./useEdgeResize";

const COLUMNS = 12;
const TRACK = 100;
const COL = TRACK + GRID_GAP_PX;
const ROW = TILE_ROW_PX + GRID_GAP_PX;
const GRID_WIDTH = COLUMNS * TRACK + (COLUMNS - 1) * GRID_GAP_PX;

const tile = (layout: Partial<DashboardTile> = {}): DashboardTile =>
   ({
      name: "a",
      source: "s",
      declaration: { kind: "reference", from: "x" },
      ...layout,
   }) as DashboardTile;

/** A pointer event on an edge whose tile's box is `box`, in grid coordinates (the grid sits at 0,0). */
const pointer = (
   clientX: number,
   clientY: number,
   box = {
      left: 0,
      top: 0,
      width: 6 * COL - GRID_GAP_PX,
      height: 3 * ROW - GRID_GAP_PX,
   },
   type = "pointermove",
) => {
   const edge = document.createElement("div");
   const frame = document.createElement("div");
   frame.appendChild(edge);
   frame.getBoundingClientRect = () => box as DOMRect;
   let captured = false;
   Object.assign(edge, {
      setPointerCapture: () => {
         captured = true;
      },
      hasPointerCapture: () => captured,
      releasePointerCapture: () => {
         captured = false;
      },
   });
   return {
      type,
      button: 0,
      clientX,
      clientY,
      pointerId: 1,
      currentTarget: edge,
      stopPropagation: mock(() => {}),
      preventDefault: mock(() => {}),
   } as unknown as PointerEvent<HTMLElement>;
};

const mount = (tiles: DashboardTile[]) => {
   const commit = mock((_result: EdgeResult) => {});
   const grid = document.createElement("div");
   grid.getBoundingClientRect = () =>
      ({ left: 0, top: 0, width: GRID_WIDTH, height: 2000 }) as DOMRect;
   // Each tile drawn in the grid, as placing a report that flows measures
   // them: six columns wide and three rows tall at the top left.
   for (const each of tiles) {
      const frame = document.createElement("div");
      frame.dataset.tileKey = `${(each as { source: string }).source}.${each.name}`;
      frame.getBoundingClientRect = () =>
         ({
            left: 0,
            top: 0,
            width: 6 * COL - GRID_GAP_PX,
            height: 3 * ROW - GRID_GAP_PX,
         }) as DOMRect;
      grid.appendChild(frame);
   }
   const gridBox = { current: grid as HTMLDivElement | null };
   const view = renderHook(() =>
      useEdgeResize({
         tiles,
         columns: COLUMNS,
         gridBox,
         onStart: () => {},
         commit,
      }),
   );
   const drag = (
      edges: Edges,
      x: number,
      y: number,
      box?: Parameters<typeof pointer>[2],
   ) => {
      act(() => view.result.current.start(pointer(0, 0, box), 0, edges));
      act(() => view.result.current.move(pointer(x, y, box)));
      const shape = view.result.current.drag?.shape;
      act(() => view.result.current.end(pointer(x, y, box, "pointerup")));
      return shape;
   };
   return { view, commit, drag };
};

describe("useEdgeResize on a placed report", () => {
   const placed = () => [tile({ row: 3, col: 4, colspan: 4, rowspan: 4 })];
   // The tile's box: columns 4-7, rows 3-6.
   const box = {
      left: 3 * COL,
      top: 2 * ROW,
      width: 4 * COL - GRID_GAP_PX,
      height: 4 * ROW - GRID_GAP_PX,
   };

   it("moves the left edge and keeps the right one", () => {
      const { commit, drag } = mount(placed());
      // The pointer at the start of column 2.
      expect(drag({ w: true }, 1 * COL + 4, 0, box)).toMatchObject({
         col: 2,
         colspan: 6,
      });
      expect(commit.mock.calls[0]?.[0]).toMatchObject({
         edges: { w: true },
         placed: true,
         shape: { col: 2, colspan: 6 },
      });
   });

   it("moves the top edge and keeps the bottom one", () => {
      const { drag } = mount(placed());
      // The pointer at the top of row 1.
      expect(drag({ n: true }, 0, 4, box)).toMatchObject({
         row: 1,
         rowspan: 6,
      });
   });

   it("holds the top edge to the fewest rows a tile is offered", () => {
      const { drag } = mount(placed());
      // Pulled far below its own bottom: three rows are the floor, ending at row 6.
      expect(drag({ n: true }, 0, 40 * ROW, box)).toMatchObject({
         row: 4,
         rowspan: 3,
      });
   });

   it("moves two edges at once from a corner", () => {
      const { drag } = mount(placed());
      // Bottom-right corner to the end of column 9, row 8.
      expect(
         drag(
            { s: true, e: true },
            9 * COL - GRID_GAP_PX,
            8 * ROW - GRID_GAP_PX,
            box,
         ),
      ).toMatchObject({
         row: 3,
         col: 4,
         colspan: 6,
         rowspan: 6,
      });
   });

   it("measures the bottom edge from the tile as drawn, wherever its row is", () => {
      // A placed tile drawn somewhere its row does not say (stacked in one
      // column on a narrow screen): 4 rows tall at y = 1000.
      const stacked = { ...box, top: 1000 };
      const { drag } = mount(placed());
      expect(
         drag({ s: true }, 0, 1000 + 6 * ROW - GRID_GAP_PX, stacked),
      ).toMatchObject({ row: 3, rowspan: 6 });
   });

   it("stops the right edge at the grid's edge, never moving the left", () => {
      const { drag } = mount(placed());
      expect(drag({ e: true }, 50 * COL, 0, box)).toMatchObject({
         col: 4,
         colspan: 9,
      });
   });
});

describe("useEdgeResize on a tile a placed report leaves to the grid", () => {
   it("places that tile where it is drawn when its left edge is pulled", () => {
      const tiles = [
         tile({ colspan: 6 }),
         tile({
            name: "b",
            row: 9,
            col: 1,
            colspan: 6,
            rowspan: 3,
         } as Partial<DashboardTile>),
      ];
      const { commit, drag } = mount(tiles);
      drag({ w: true }, 1 * COL + 4, 0);
      const result = commit.mock.calls[0]?.[0];
      expect(result?.placed).toBe(true);
      expect(result?.placedAll?.[0]).toMatchObject({ row: 1, col: 1 });
      expect(result?.shape).toMatchObject({ col: 2, colspan: 5 });
   });
});

describe("useEdgeResize on a report that flows", () => {
   it("sets a width from the right edge, and only a width", () => {
      const { commit, drag } = mount([tile({ colspan: 6 })]);
      drag({ e: true }, 4 * COL - GRID_GAP_PX, 0);
      const result = commit.mock.calls[0]?.[0];
      expect(result).toMatchObject({ placed: false, shape: { colspan: 4 } });
      expect(result?.placedAll).toBeUndefined();
      expect(
         withShape(tile({ colspan: 6 }), result!.shape, result!.edges, false),
      ).toEqual(tile({ colspan: 4 }));
   });

   it("sets a height from the bottom edge, and leaves the width it is drawn at alone", () => {
      const { commit, drag } = mount([tile({ colspan: 6 })]);
      drag({ s: true }, 0, 5 * ROW - GRID_GAP_PX);
      const result = commit.mock.calls[0]?.[0];
      expect(result?.shape.rowspan).toBe(5);
      // Written on a tile with no colspan, a height drag adds none.
      expect(withShape(tile(), result!.shape, result!.edges, false)).toEqual(
         tile({ rowspan: 5 }),
      );
   });

   it("places the report when a left edge is pulled, so the edge has a column to move", () => {
      const { commit, drag } = mount([tile({ colspan: 6 })]);
      drag({ w: true }, 1 * COL + 4, 0);
      const result = commit.mock.calls[0]?.[0];
      expect(result?.placed).toBe(true);
      // Placed where it was drawn (columns 1-6), then its left edge moved to 2.
      expect(result?.placedAll?.[0]).toMatchObject({
         row: 1,
         col: 1,
         colspan: 6,
      });
      expect(result?.shape).toMatchObject({ col: 2, colspan: 5 });
   });

   it("writes nothing, and places nothing, for a press that changes nothing", () => {
      const { commit, view } = mount([tile({ colspan: 6 })]);
      act(() => view.result.current.start(pointer(0, 0), 0, { w: true }));
      act(() => view.result.current.end(pointer(0, 0, undefined, "pointerup")));
      expect(commit).not.toHaveBeenCalled();
   });

   it("puts the tile back when the gesture is cancelled", () => {
      const { commit, view } = mount([tile({ colspan: 6 })]);
      act(() => view.result.current.start(pointer(0, 0), 0, { e: true }));
      act(() => view.result.current.move(pointer(2 * COL, 0)));
      act(() =>
         view.result.current.end(
            pointer(2 * COL, 0, undefined, "pointercancel"),
         ),
      );
      expect(commit).not.toHaveBeenCalled();
      expect(view.result.current.drag).toBeUndefined();
   });

   it("puts the tile back, and writes nothing, when Escape is pressed mid-drag", () => {
      const { commit, view } = mount([tile({ colspan: 6 })]);
      act(() => view.result.current.start(pointer(0, 0), 0, { e: true }));
      act(() => view.result.current.move(pointer(2 * COL, 0)));
      expect(view.result.current.drag).toBeDefined();
      const escape = new KeyboardEvent("keydown", {
         key: "Escape",
         cancelable: true,
      });
      act(() => {
         window.dispatchEvent(escape);
      });
      expect(escape.defaultPrevented).toBe(true);
      expect(view.result.current.drag).toBeUndefined();
      // The pointer let go after Escape: still nothing written.
      act(() =>
         view.result.current.end(pointer(2 * COL, 0, undefined, "pointerup")),
      );
      expect(commit).not.toHaveBeenCalled();
   });
});

describe("edgeCursor", () => {
   it("is a two-headed arrow on an edge and a diagonal one on a corner", () => {
      expect(edgeCursor({ e: true })).toBe("ew-resize");
      expect(edgeCursor({ w: true })).toBe("ew-resize");
      expect(edgeCursor({ n: true })).toBe("ns-resize");
      expect(edgeCursor({ s: true })).toBe("ns-resize");
      expect(edgeCursor({ n: true, w: true })).toBe("nwse-resize");
      expect(edgeCursor({ s: true, e: true })).toBe("nwse-resize");
      expect(edgeCursor({ n: true, e: true })).toBe("nesw-resize");
      expect(edgeCursor({ s: true, w: true })).toBe("nesw-resize");
   });
});
