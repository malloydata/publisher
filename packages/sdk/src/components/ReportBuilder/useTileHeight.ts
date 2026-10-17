// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useEffect, useRef, useState, type RefObject } from "react";
import { rowsForHeight } from "../Dashboard/DashboardGrid";
import type { DashboardTile } from "./document";

/** A height drag in flight: the tile, the rows it WOULD be, and where its top sits. */
export interface HeightState {
   index: number;
   rows: number;
   /** The rows the drag started from, set or drawn: a release that changed nothing writes nothing. */
   from: number;
   top: number;
   /** The tile's box against the grid's, where the row guides are drawn from. */
   guide: { top: number; left: number; width: number };
}

/** While the edge is dragged the page stops selecting text and keeps one cursor, as a width drag does. */
const dragChrome = (on: boolean) => {
   const body = window.document.body;
   body.style.userSelect = on ? "none" : "";
   body.style.cursor = on ? "row-resize" : "";
};

/**
 * Dragging a tile's bottom edge to set its `rowspan`, in whole grid rows. The
 * rows are previewed by the caller from `height.rows` and written ONCE, on
 * release, so one gesture is one undo step.
 *
 * A tile that fits its content starts the drag from the rows it is drawn at,
 * so the edge does not jump when it is first pulled.
 */
export function useTileHeight({
   tiles,
   gridBox,
   onStart,
   commit,
}: {
   tiles: readonly DashboardTile[];
   /** Wraps the grid: the row guides are placed against it. */
   gridBox: RefObject<HTMLDivElement | null>;
   /** The tile a drag begins on, which the caller selects. */
   onStart: (index: number) => void;
   /** The rows to write when the drag ends. */
   commit: (index: number, rows: number) => void;
}) {
   const [height, setHeightState] = useState<HeightState | undefined>(
      undefined,
   );
   // The gesture as the handlers last left it: a release can arrive before
   // the render for the last move does.
   const live = useRef<HeightState | undefined>(undefined);
   const setHeight = (next: HeightState | undefined) => {
      live.current = next;
      setHeightState(next);
   };
   useEffect(() => () => dragChrome(false), []);

   const startHeight = (
      event: React.PointerEvent<HTMLDivElement>,
      index: number,
   ) => {
      const tile = event.currentTarget.parentElement?.getBoundingClientRect();
      const grid = gridBox.current?.getBoundingClientRect();
      if (!tile || !grid) return;
      event.stopPropagation();
      event.preventDefault();
      event.currentTarget.setPointerCapture(event.pointerId);
      dragChrome(true);
      onStart(index);
      const from = tiles[index]?.rowspan ?? rowsForHeight(tile.height);
      setHeight({
         index,
         rows: from,
         from,
         top: tile.top,
         guide: {
            top: tile.top - grid.top,
            left: tile.left - grid.left,
            width: tile.width,
         },
      });
   };

   const onHeight = (event: React.PointerEvent<HTMLDivElement>) => {
      const drag = live.current;
      if (!drag) return;
      const rows = rowsForHeight(event.clientY - drag.top);
      if (rows !== drag.rows) setHeight({ ...drag, rows });
   };

   const endHeight = (event: React.PointerEvent<HTMLDivElement>) => {
      if (event.currentTarget.hasPointerCapture(event.pointerId))
         event.currentTarget.releasePointerCapture(event.pointerId);
      dragChrome(false);
      const drag = live.current;
      if (!drag) return;
      const { index, rows, from } = drag;
      setHeight(undefined);
      // A cancelled gesture puts the height back, and a press that moved
      // nothing writes nothing: a click must not pin a tile that fits its
      // content to the rows it happens to be drawn at.
      if (event.type === "pointercancel" || rows === from) return;
      commit(index, rows);
   };

   return { height, startHeight, onHeight, endHeight };
}
