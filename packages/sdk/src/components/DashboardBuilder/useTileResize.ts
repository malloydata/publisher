// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useRef, useState } from "react";
import { GRID_GAP_PX } from "../Dashboard/DashboardGrid";
import type { DashboardTile } from "./document";

/** A resize in flight: the tile, the width it WOULD be, and the geometry. */
export interface ResizeState {
   index: number;
   span: number;
   left: number;
   track: number;
}

/**
 * While a RESIZE is live, the page has to stop behaving like a document:
 * dragging an edge across a dashboard otherwise selects the text it crosses and
 * leaves the cursor as whatever it was over. (A MOVE gets the same from the
 * library's own plugins.)
 */
const dragChrome = (on: boolean) => {
   const body = window.document.body;
   body.style.userSelect = on ? "none" : "";
   body.style.cursor = on ? "grabbing" : "";
};

/**
 * Dragging a tile's right edge to set its `colspan`. The width is previewed
 * by the caller from `resize.span` and written ONCE, on release: writing on
 * every pointer move would put a dozen documents in the history for one
 * gesture, and undo would walk back through the drag a column at a time.
 */
export function useTileResize({
   tiles,
   columns,
   onStart,
   commit,
}: {
   tiles: readonly DashboardTile[];
   columns: number;
   /** The tile a resize begins on, which the caller selects. */
   onStart: (index: number) => void;
   /** The width to write when the drag ends. */
   commit: (index: number, span: number) => void;
}) {
   const [resize, setResize] = useState<ResizeState | undefined>(undefined);
   // Wraps the grid, so its content box IS the grid's: what a dragged edge has
   // to be measured against to work out a column count.
   const gridBox = useRef<HTMLDivElement>(null);

   /**
    * Width is the ONLY thing a resize can change.
    *
    * The format lays tiles out as a flow — `# colspan` for width, `# break` to
    * start a row — with no row index, no column position and no height. So a
    * right edge maps onto `colspan` and persists; a bottom edge has nothing to
    * be written as, and a left edge would mean placing the tile, which the grid
    * cannot express either. Offering those handles would be offering a drag the
    * writer then refuses, which is worse than not offering it.
    */
   const startResize = (
      event: React.PointerEvent<HTMLDivElement>,
      index: number,
   ) => {
      const grid = gridBox.current?.getBoundingClientRect();
      const tile = event.currentTarget.parentElement?.getBoundingClientRect();
      if (!grid || !tile) return;
      // Stops the tile's own click selecting a second time on release.
      event.stopPropagation();
      event.preventDefault();
      event.currentTarget.setPointerCapture(event.pointerId);
      dragChrome(true);
      onStart(index);
      setResize({
         index,
         span: tiles[index]?.colspan ?? 1,
         left: tile.left,
         // One column track: the row's width less every gutter in it.
         track: (grid.width - GRID_GAP_PX * (columns - 1)) / columns,
      });
   };

   const onResize = (event: React.PointerEvent<HTMLDivElement>) => {
      if (!resize) return;
      const width = event.clientX - resize.left;
      // A tile of N tracks is N tracks plus the N-1 gutters between them, so
      // adding one gutter back makes the division land on whole columns.
      const span = Math.round(
         (width + GRID_GAP_PX) / (resize.track + GRID_GAP_PX),
      );
      const clamped = Math.min(Math.max(span, 1), columns);
      if (clamped !== resize.span) setResize({ ...resize, span: clamped });
   };

   const endResize = (event: React.PointerEvent<HTMLDivElement>) => {
      if (event.currentTarget.hasPointerCapture(event.pointerId))
         event.currentTarget.releasePointerCapture(event.pointerId);
      dragChrome(false);
      if (!resize) return;
      const { index, span } = resize;
      setResize(undefined);
      // ONE history entry for the whole drag. Writing on every pointer move
      // would put a dozen documents in the stack for one gesture, and undo
      // would walk back through the drag a column at a time.
      commit(index, span);
   };

   return { resize, gridBox, startResize, onResize, endResize };
}
