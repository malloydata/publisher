// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useEffect, useRef, useState, type RefObject } from "react";
import { rowsForHeight, tileHeightPx } from "../Dashboard/DashboardGrid";
import { tileKey, type DashboardTile } from "./document";
import { minRowsOf } from "./placement";

/** A height drag in flight: the tile, what it WOULD be, and where its top sits. */
export interface HeightState {
   index: number;
   /** The rows it would be, or undefined for auto: no `rowspan`, as tall as its row. */
   rows: number | undefined;
   /** Its `rowspan` when the drag began: a release that changed nothing writes nothing. */
   from: number | undefined;
   top: number;
   /** The tile's top against the grid's, where the row guides are drawn from. */
   guideTop: number;
}

/** While the edge is dragged the page stops selecting text and keeps one cursor, as a width drag does. */
const dragChrome = (on: boolean) => {
   const body = window.document.body;
   body.style.userSelect = on ? "none" : "";
   body.style.cursor = on ? "row-resize" : "";
};

/**
 * Dragging a tile's bottom edge to set its `rowspan`, in whole grid rows, or
 * back to auto. Previewed by the caller from `height.rows` and written ONCE,
 * on release, so one gesture is one undo step.
 *
 * Auto is a snap point of its own. A tile with no `rowspan` is as tall as its
 * row, and a row of such tiles is as tall as its tallest content, which is
 * almost never a whole number of rows: 492px sits between 5 rows (464) and 6
 * (560). With rows alone a tile could never be dragged level with an auto
 * neighbour; snapping to the row's own height, and writing that as no
 * `rowspan`, is what makes level reachable.
 */
export function useTileHeight({
   tiles,
   gridBox,
   onStart,
   commit,
}: {
   tiles: readonly DashboardTile[];
   /** Wraps the grid: row-mates are found in it and the row guides are placed against it. */
   gridBox: RefObject<HTMLDivElement | null>;
   /** The tile a drag begins on, which the caller selects. */
   onStart: (index: number) => void;
   /** What to write when the drag ends: rows, or undefined to take the `rowspan` off. */
   commit: (index: number, rows: number | undefined) => void;
}) {
   const [height, setHeightState] = useState<HeightState | undefined>(
      undefined,
   );
   // The gesture as the handlers last left it: a release can arrive before
   // the render for the last move does.
   const live = useRef<HeightState | undefined>(undefined);
   // A tile that began on auto: its height then, which was its row's.
   const startedAuto = useRef<number | undefined>(undefined);
   const setHeight = (next: HeightState | undefined) => {
      live.current = next;
      setHeightState(next);
   };
   useEffect(() => () => dragChrome(false), []);

   /**
    * How tall the row is when this tile is on auto: the tallest of the auto
    * tiles beside it, measured now. A neighbour stretched by this tile's own
    * preview reads as that preview's height, which ties with it and loses;
    * once the preview is shorter than the row, the neighbours read their own
    * height and auto wins. Undefined when nothing says how tall auto would be.
    */
   const autoHeight = (drag: HeightState): number | undefined => {
      const auto = new Set(
         tiles.filter((tile) => tile.rowspan === undefined).map(tileKey),
      );
      const self = tiles[drag.index];
      let tallest = startedAuto.current;
      for (const frame of gridBox.current?.querySelectorAll<HTMLElement>(
         "[data-tile-key]",
      ) ?? []) {
         const key = frame.dataset.tileKey;
         if (!key || !auto.has(key) || (self && key === tileKey(self)))
            continue;
         const box = frame.getBoundingClientRect();
         if (Math.abs(box.top - drag.top) > 1) continue;
         tallest = Math.max(tallest ?? 0, box.height);
      }
      return tallest;
   };

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
      const from = tiles[index]?.rowspan;
      startedAuto.current = from === undefined ? tile.height : undefined;
      setHeight({
         index,
         rows: from,
         from,
         top: tile.top,
         guideTop: tile.top - grid.top,
      });
   };

   const onHeight = (event: React.PointerEvent<HTMLDivElement>) => {
      const drag = live.current;
      if (!drag) return;
      const px = event.clientY - drag.top;
      const self = tiles[drag.index];
      const rows = rowsForHeight(px, self ? minRowsOf(self) : undefined);
      const auto = autoHeight(drag);
      // Ties go to rows: a neighbour stretched to the preview is no reason to leave it.
      const next =
         auto !== undefined &&
         Math.abs(px - auto) < Math.abs(px - tileHeightPx(rows))
            ? undefined
            : rows;
      if (next !== drag.rows) setHeight({ ...drag, rows: next });
   };

   const endHeight = (event: React.PointerEvent<HTMLDivElement>) => {
      if (event.currentTarget.hasPointerCapture(event.pointerId))
         event.currentTarget.releasePointerCapture(event.pointerId);
      dragChrome(false);
      const drag = live.current;
      if (!drag) return;
      const { index, rows, from } = drag;
      setHeight(undefined);
      startedAuto.current = undefined;
      // A cancelled gesture puts the height back, and a press that moved
      // nothing writes nothing.
      if (event.type === "pointercancel" || rows === from) return;
      commit(index, rows);
   };

   return { height, startHeight, onHeight, endHeight };
}
