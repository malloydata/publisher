// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useEffect, useRef, useState, type RefObject } from "react";
import {
   GRID_GAP_PX,
   isPlaced,
   MAX_TILE_ROWS,
   rowsForHeight,
   TILE_ROW_PX,
   tileHeightPx,
} from "../Dashboard/DashboardGrid";
import { tileKey, type DashboardTile } from "./document";
import { minRowsOf, placeWhereDrawn, rowsOf, trackWidth } from "./placement";
import { drawnBoxes } from "./useTileReorder";
import { widthLabel } from "./widthLabel";

/** The edges a resize moves: one for an edge, two for a corner. */
export interface Edges {
   n?: boolean;
   s?: boolean;
   e?: boolean;
   w?: boolean;
}

/** A tile's place and size, as a resize changes them. `rowspan` absent is auto height. */
export interface TileShape {
   row?: number;
   col?: number;
   colspan: number;
   rowspan: number | undefined;
}

/** A resize in flight. */
export interface EdgeDrag {
   index: number;
   edges: Edges;
   /** The tiles it started from: placed where they were drawn, when a left or top edge needs places. */
   base: DashboardTile[];
   /** Whether this gesture placed them, which the write then has to keep. */
   placedHere: boolean;
   /** Whether the report is placed for this gesture: the edges then move on the grid's own rows. */
   placed: boolean;
   from: TileShape;
   /** The shape the drag began at, as it is drawn: a release here writes nothing. */
   begun: TileShape;
   /** What the tile would be if the pointer let go now. */
   shape: TileShape;
   /** The tile's top against the grid's, where the row guides of a flowing report are drawn from. */
   guideTop: number;
   /** The fewest rows offered, read off what the tile draws when the drag began. */
   min: number;
   /** The tile's top and left on the page when it began, for a flowing report's edges. */
   top: number;
   left: number;
}

const ROW_STEP = TILE_ROW_PX + GRID_GAP_PX;

/** The cursor an edge, or a corner, takes. */
export const edgeCursor = (edges: Edges): string =>
   (edges.n && edges.w) || (edges.s && edges.e)
      ? "nwse-resize"
      : (edges.n && edges.e) || (edges.s && edges.w)
        ? "nesw-resize"
        : edges.n || edges.s
          ? "ns-resize"
          : "ew-resize";

/** While an edge is dragged the page selects no text and keeps the edge's cursor. */
const dragChrome = (cursor: string | undefined) => {
   const body = window.document.body;
   body.style.userSelect = cursor ? "none" : "";
   body.style.cursor = cursor ?? "";
};

const shapeOf = (tile: DashboardTile): TileShape => ({
   ...(tile.row !== undefined ? { row: tile.row } : {}),
   ...(tile.col !== undefined ? { col: tile.col } : {}),
   colspan: tile.colspan ?? 1,
   rowspan: tile.rowspan,
});

const sameShape = (a: TileShape, b: TileShape) =>
   a.row === b.row &&
   a.col === b.col &&
   a.colspan === b.colspan &&
   a.rowspan === b.rowspan;

/**
 * `tile` with what a resize of `edges` changed on it, and nothing else: a
 * right or left edge its width (and, placed, its column), a bottom or top
 * edge its height (and, placed, its row). So a height drag never writes the
 * width a tile happens to be drawn at.
 */
export function withShape(
   tile: DashboardTile,
   shape: TileShape,
   edges: Edges,
   placed: boolean,
): DashboardTile {
   const next = { ...tile };
   if (edges.e || edges.w) {
      next.colspan = shape.colspan;
      if (placed && shape.col !== undefined) next.col = shape.col;
   }
   if (edges.n || edges.s) {
      if (shape.rowspan === undefined) delete next.rowspan;
      else next.rowspan = shape.rowspan;
      if (placed && shape.row !== undefined) next.row = shape.row;
   }
   return next;
}

/** What a resize asks the caller to write. */
export interface EdgeResult {
   index: number;
   edges: Edges;
   shape: TileShape;
   placed: boolean;
   /** Every tile placed where it was drawn, when this gesture is what placed the report. */
   placedAll?: DashboardTile[];
}

/**
 * Dragging any edge or corner of a tile. The right edge sets its width and
 * the bottom edge its height, as they always have; the left and top edges
 * move that edge and keep the opposite one where it is, which needs the
 * tile's row and column, so on a report that flows they place every tile
 * where it is drawn first, as a move does. A corner moves its two edges at
 * once. Previewed from `drag.shape` and written ONCE, on release, so one
 * gesture is one undo step.
 */
export function useEdgeResize({
   tiles,
   columns,
   gridBox,
   onStart,
   onPlaceAll,
   commit,
}: {
   /** The tiles as drawn: one with no `colspan` carries the span it is drawn at. */
   tiles: readonly DashboardTile[];
   columns: number;
   /** Wraps the grid, so its box is the grid's. */
   gridBox: RefObject<HTMLDivElement | null>;
   /** The tile a drag begins on, which the caller selects. */
   onStart: (index: number) => void;
   /** A drag is about to place every tile of a report that had no places yet. */
   onPlaceAll?: () => void;
   /** What to write when the drag ends. */
   commit: (result: EdgeResult) => void;
}) {
   const [drag, setDragState] = useState<EdgeDrag | undefined>(undefined);
   // The gesture as the handlers last left it: a release can arrive before
   // the render for the last move does.
   const live = useRef<EdgeDrag | undefined>(undefined);
   // A flowing tile that began on auto height: its height then, its row's.
   const startedAuto = useRef<number | undefined>(undefined);
   const setDrag = (next: EdgeDrag | undefined) => {
      live.current = next;
      setDragState(next);
   };
   useEffect(() => () => dragChrome(undefined), []);
   // Escape cancels a resize in flight wherever focus is: the tile goes back
   // to where it began and the release writes nothing.
   const active = drag !== undefined;
   useEffect(() => {
      if (!active) return;
      const onKey = (event: KeyboardEvent) => {
         if (event.key !== "Escape" || !live.current) return;
         event.preventDefault();
         event.stopPropagation();
         live.current = undefined;
         setDragState(undefined);
         startedAuto.current = undefined;
         dragChrome(undefined);
      };
      window.addEventListener("keydown", onKey, true);
      return () => window.removeEventListener("keydown", onKey, true);
   }, [active]);

   /**
    * How tall a flowing tile's row is on auto height: the tallest of the auto
    * tiles beside it, measured now (#1453: a row of auto tiles is rarely a whole number of rows, so auto is a snap
    * point of its own, and what makes a tile reachable level with them).
    */
   const autoHeight = (state: EdgeDrag): number | undefined => {
      const auto = new Set(
         state.base.filter((tile) => tile.rowspan === undefined).map(tileKey),
      );
      const self = state.base[state.index];
      let tallest = startedAuto.current;
      for (const frame of gridBox.current?.querySelectorAll<HTMLElement>(
         "[data-tile-key]",
      ) ?? []) {
         const key = frame.dataset.tileKey;
         if (!key || !auto.has(key) || (self && key === tileKey(self)))
            continue;
         const box = frame.getBoundingClientRect();
         if (Math.abs(box.top - state.top) > 1) continue;
         tallest = Math.max(tallest ?? 0, box.height);
      }
      return tallest;
   };

   const start = (
      event: React.PointerEvent<HTMLElement>,
      index: number,
      edges: Edges,
   ) => {
      const grid = gridBox.current;
      const gridBoxNow = grid?.getBoundingClientRect();
      const tileBox =
         event.currentTarget.parentElement?.getBoundingClientRect();
      if (!grid || !gridBoxNow || !tileBox || !tiles[index]) return;
      event.stopPropagation();
      event.preventDefault();
      event.currentTarget.setPointerCapture(event.pointerId);
      dragChrome(edgeCursor(edges));
      onStart(index);
      // A left or top edge moves a place, which a flowing report has not got
      // yet, nor has a tile a placed report leaves to the grid: each tile
      // without one is placed where it is drawn, so nothing jumps.
      const needsPlace =
         (edges.n || edges.w) &&
         (!isPlaced(tiles) ||
            tiles[index].row === undefined ||
            tiles[index].col === undefined);
      if (needsPlace && !isPlaced(tiles)) onPlaceAll?.();
      const base = needsPlace
         ? (() => {
              const boxes = drawnBoxes(grid);
              return placeWhereDrawn(
                 tiles,
                 tiles.map((tile) => boxes.get(tileKey(tile))),
                 columns,
                 trackWidth(gridBoxNow.width, columns),
              );
           })()
         : [...tiles];
      const placed = isPlaced(base);
      const tile = base[index];
      const from = shapeOf(tile);
      // A flowing tile on auto height starts from the rows it is drawn at,
      // so the edge does not jump when it is first pulled.
      if (!placed && from.rowspan === undefined) {
         startedAuto.current = tileBox.height;
      } else startedAuto.current = undefined;
      const begun = placed ? { ...from, rowspan: rowsOf(tile) } : from;
      setDrag({
         index,
         edges,
         base,
         placedHere: Boolean(needsPlace),
         placed,
         from,
         begun,
         shape: begun,
         guideTop: tileBox.top - gridBoxNow.top,
         min: minRowsOf(tile, event.currentTarget.parentElement),
         top: tileBox.top,
         left: tileBox.left,
      });
   };

   const move = (event: React.PointerEvent<HTMLElement>) => {
      const state = live.current;
      const grid = gridBox.current?.getBoundingClientRect();
      if (!state || !grid) return;
      const tile = state.base[state.index];
      if (!tile) return;
      const track = trackWidth(grid.width, columns);
      const colStep = track + GRID_GAP_PX;
      // The pointer against the grid as it is now, so a page that scrolled
      // during the drag still lands the edge under the pointer.
      const x = event.clientX - grid.left;
      const y = event.clientY - grid.top;
      // The tile's own box as it is drawn now. A bottom or right edge is
      // measured from it, not from the grid's rows: on a narrow screen a
      // placed report stacks in one column, where a tile's row says nothing
      // about where it is, and a page scrolled mid-drag moves it too.
      const own = event.currentTarget.parentElement?.getBoundingClientRect();
      const ownTop = own?.top ?? state.top;
      const ownLeft = own?.left ?? state.left;
      const { min } = state;
      const now = state.shape;
      let { row, col, colspan, rowspan } = now;

      if (state.placed && col !== undefined && row !== undefined) {
         const from = state.from;
         const firstCol = from.col ?? col;
         const lastCol = firstCol + from.colspan - 1;
         const firstRow = from.row ?? row;
         const fromRows = from.rowspan ?? rowsOf(tile);
         const lastRow = firstRow + fromRows - 1;
         if (state.edges.e) {
            const right = Math.round((x + GRID_GAP_PX) / colStep);
            colspan =
               Math.min(Math.max(right, firstCol), columns) - firstCol + 1;
            col = firstCol;
         }
         if (state.edges.w) {
            const left = Math.round(x / colStep) + 1;
            col = Math.min(Math.max(left, 1), lastCol);
            colspan = lastCol - col + 1;
         }
         if (state.edges.s) {
            // From the tile's top, so it is right in the stacked layout too.
            // A top edge moved in the same gesture (a corner) has moved that
            // top already, so it is measured from the row it started on.
            const top = state.edges.n
               ? grid.top + (firstRow - 1) * ROW_STEP
               : ownTop;
            rowspan = rowsForHeight(event.clientY - top, min);
            row = firstRow;
         }
         if (state.edges.n) {
            const top = Math.round(y / ROW_STEP) + 1;
            // A tile already shorter than its floor (a 2-row chart written
            // by hand, or one still loading) holds the rows it has rather
            // than growing upward, which could put its top above row 1.
            const floor = Math.min(min, fromRows);
            row = Math.max(
               1,
               Math.min(
                  Math.max(top, lastRow - MAX_TILE_ROWS + 1),
                  lastRow - floor + 1,
               ),
            );
            rowspan = lastRow - row + 1;
         }
      } else {
         // A report that flows: the right edge is a width and the bottom
         // edge a height, each against the tile's own top left.
         if (state.edges.e) {
            const span = Math.round(
               (event.clientX - ownLeft + GRID_GAP_PX) / colStep,
            );
            colspan = Math.min(Math.max(span, 1), columns);
         }
         if (state.edges.s) {
            const px = event.clientY - ownTop;
            const rows = rowsForHeight(px, min);
            const auto = autoHeight(state);
            // Ties go to rows: a neighbour stretched to the preview is no
            // reason to leave it.
            rowspan =
               auto !== undefined &&
               Math.abs(px - auto) < Math.abs(px - tileHeightPx(rows))
                  ? undefined
                  : rows;
         }
      }
      const next: TileShape = {
         ...(row !== undefined ? { row } : {}),
         ...(col !== undefined ? { col } : {}),
         colspan,
         rowspan,
      };
      if (!sameShape(next, now)) setDrag({ ...state, shape: next });
   };

   const end = (event: React.PointerEvent<HTMLElement>) => {
      if (event.currentTarget.hasPointerCapture(event.pointerId))
         event.currentTarget.releasePointerCapture(event.pointerId);
      dragChrome(undefined);
      const state = live.current;
      startedAuto.current = undefined;
      if (!state) return;
      setDrag(undefined);
      // A cancelled gesture puts the tile back, and a press that changed
      // nothing writes nothing: a click on an edge must not place a report
      // that flows, or pin a tile on auto height to the rows it is drawn at.
      if (event.type === "pointercancel" || sameShape(state.shape, state.begun))
         return;
      commit({
         index: state.index,
         edges: state.edges,
         shape: state.shape,
         placed: state.placed,
         ...(state.placedHere ? { placedAll: state.base } : {}),
      });
   };

   return { drag, start, move, end };
}

/** What a resize would make a tile, in words, for the label beside the pointer. */
export function shapeLabel(drag: EdgeDrag, columns: number): string {
   const parts: string[] = [];
   if (drag.edges.e || drag.edges.w)
      parts.push(widthLabel(drag.shape.colspan, columns));
   if (drag.edges.n || drag.edges.s)
      parts.push(
         drag.shape.rowspan === undefined
            ? "Auto height"
            : `${drag.shape.rowspan} rows tall`,
      );
   return parts.join(" · ");
}
