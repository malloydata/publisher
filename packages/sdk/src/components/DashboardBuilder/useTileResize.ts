// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useEffect, useRef, useState, type RefObject } from "react";
import { GRID_GAP_PX } from "../Dashboard/DashboardGrid";
import type { DashboardTile } from "./document";

/** A resize in flight: the tile, the width it WOULD be, and the geometry. */
export interface ResizeState {
   index: number;
   span: number;
   /** The width the drag started from, so a release that changed nothing writes nothing. */
   from: number;
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
   gridBox,
   onStart,
   commit,
}: {
   tiles: readonly DashboardTile[];
   columns: number;
   /** Wraps the grid, so its content box IS the grid's: what a dragged edge is measured against to work out a column count. */
   gridBox: RefObject<HTMLDivElement | null>;
   /** The tile a resize begins on, which the caller selects. */
   onStart: (index: number) => void;
   /** The width to write when the drag ends. */
   commit: (index: number, span: number) => void;
}) {
   const [resize, setResizeState] = useState<ResizeState | undefined>(
      undefined,
   );
   // The gesture as the handlers last left it. A release can arrive before the
   // render for the last move does, and reading state there would write the
   // width one column behind where the pointer let go.
   const live = useRef<ResizeState | undefined>(undefined);
   const setResize = (next: ResizeState | undefined) => {
      live.current = next;
      setResizeState(next);
   };
   // A builder unmounted mid-drag (a route change, say) must not leave the
   // whole page unselectable with a grabbing cursor.
   useEffect(() => () => dragChrome(false), []);

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
      const from = tiles[index]?.colspan ?? 1;
      setResize({
         index,
         span: from,
         from,
         left: tile.left,
         // One column track: the row's width less every gutter in it.
         track: (grid.width - GRID_GAP_PX * (columns - 1)) / columns,
      });
   };

   const onResize = (event: React.PointerEvent<HTMLDivElement>) => {
      const resize = live.current;
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
      const resize = live.current;
      if (!resize) return;
      const { index, span, from } = resize;
      setResize(undefined);
      // A cancelled gesture (the browser took the pointer) puts the width back;
      // a press that moved nothing writes nothing, or a tile with no
      // `# colspan` would gain an explicit `colspan=1` from a click.
      if (event.type === "pointercancel" || span === from) return;
      // ONE history entry for the whole drag. Writing on every pointer move
      // would put a dozen documents in the stack for one gesture, and undo
      // would walk back through the drag a column at a time.
      commit(index, span);
   };

   return { resize, startResize, onResize, endResize };
}
