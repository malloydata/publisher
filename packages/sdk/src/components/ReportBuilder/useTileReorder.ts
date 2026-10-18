// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { move } from "@dnd-kit/helpers";
import type {
   DragEndEvent,
   DragMoveEvent,
   DragOverEvent,
} from "@dnd-kit/react";
import { useRef, useState, type RefObject } from "react";
import { tileKey, type DashboardTile } from "./document";
import { keepRowStructure, moveIntoGap } from "./layout";
import {
   cellAt,
   dropAt,
   inReadingOrder,
   placeWhereDrawn,
   trackWidth,
   type Box,
} from "./placement";
import { GAP_TYPE, type GapData } from "./sortable";

/** The boxes of the tiles as drawn, against the grid's top left corner, by tile key. */
function drawnBoxes(grid: HTMLElement): Map<string, Box> {
   const origin = grid.getBoundingClientRect();
   const boxes = new Map<string, Box>();
   for (const frame of grid.querySelectorAll<HTMLElement>("[data-tile-key]")) {
      const key = frame.dataset.tileKey;
      if (!key) continue;
      const box = frame.getBoundingClientRect();
      boxes.set(key, {
         top: box.top - origin.top,
         left: box.left - origin.left,
         width: box.width,
         height: box.height,
      });
   }
   return boxes;
}

/**
 * Dragging a tile to reorder it. The gesture is `@dnd-kit/react`'s (see
 * `sortable.tsx`); what is decided here is what a drop means for the file,
 * through `keepRowStructure` for a drop onto a tile and `moveIntoGap` for a
 * drop into the empty end of a row. The preview is rebuilt on each report and
 * written ONCE, on release: one history entry for the whole drag.
 */
export function useTileReorder({
   tiles,
   commit,
   onLanded,
   placement,
}: {
   tiles: DashboardTile[];
   /**
    * Set on a grid report, where a move is free placement: the tile lands in
    * the cell under it and anything it covers is pushed down. Absent, a move
    * reorders the tiles, as a linear report's does.
    */
   placement?: {
      columns: number;
      /** Wraps the grid, so its box is the grid's. */
      gridBox: RefObject<HTMLDivElement | null>;
   };
   /** The order to write when the drag ends somewhere new. */
   commit: (next: DashboardTile[]) => void;
   /** Where the dragged tile came to rest, which the caller selects. */
   onLanded: (index: number) => void;
}) {
   // A move in flight: the tiles as they will stand if the drag ends now.
   // CUMULATIVE — each report moves the tile from where the last report left
   // it, which is the sortable convention and what the library's optimistic
   // sorting assumes; rebuilt from the document on every report, the target
   // flips back and forth as the row reflows under the pointer. State for the
   // render, and a ref for `onDragEnd`, which can fire before React has
   // committed it.
   const [preview, setPreview] = useState<DashboardTile[] | undefined>(
      undefined,
   );
   const previewRef = useRef<DashboardTile[] | undefined>(undefined);
   // Whether a drag is live at all: what turns the row-end gaps into drop
   // targets and draws the grid guides.
   const [dragging, setDragging] = useState(false);

   /**
    * Reordering is offered on EVERY tile — an inherited one included.
    *
    * Order is the `tiles=[…]` array on this file's own `## artifact` tag,
    * which this file always owns, unlike a width, which is a `# colspan` tag
    * on a view that may live in the model. So a tile the menu refuses to
    * resize can still be moved.
    *
    * The gesture itself is `@dnd-kit/react`'s — see `sortable.tsx`. What is
    * decided HERE is what a drop means for the file. The library reports which
    * target the tile is over; these handlers turn that into an order and a set
    * of row starts, through `keepRowStructure` for a drop onto a tile and
    * `moveIntoGap` for a drop into the empty end of a row. The preview is
    * rebuilt from the document on each report and written once, on release:
    * ONE history entry for the whole drag, however far it wandered.
    */
   // A placement in flight: the tiles placed where they stood when it began,
   // and the moved tile's box then.
   const placing = useRef<
      | {
           base: DashboardTile[];
           index: number;
           from: Box;
           grab: { x: number; y: number };
        }
      | undefined
   >(undefined);

   const onDragStart = (event?: {
      operation: {
         source: { id: unknown } | null;
         position?: { current: { x: number; y: number } };
      };
   }) => {
      setDragging(true);
      previewRef.current = tiles;
      const grid = placement?.gridBox.current;
      const id = event?.operation.source?.id;
      if (!placement || !grid) return;
      const index = tiles.findIndex((tile) => tileKey(tile) === id);
      const boxes = drawnBoxes(grid);
      const from = boxes.get(String(id));
      if (index < 0 || !from) return;
      // The first move on a report that flows places every tile where it is
      // drawn, so nothing jumps; a placed report keeps what it says.
      const base = placeWhereDrawn(
         tiles,
         tiles.map((tile) => boxes.get(tileKey(tile))),
         placement.columns,
         trackWidth(grid.getBoundingClientRect().width, placement.columns),
      );
      // Where in the tile the pointer took hold, so the tile's corner can be
      // found from the pointer wherever the page has scrolled to since.
      const origin = grid.getBoundingClientRect();
      const pointer = event?.operation.position?.current;
      const grab = pointer
         ? {
              x: pointer.x - (origin.left + from.left),
              y: pointer.y - (origin.top + from.top),
           }
         : { x: 0, y: 0 };
      placing.current = { base, index, from, grab };
      previewRef.current = base;
      setPreview(base);
   };

   const onDragMove = (event: DragMoveEvent) => {
      const live = placing.current;
      const grid = placement?.gridBox.current;
      if (!live || !placement || !grid) return;
      // The tile's corner against the grid as it is now: from the pointer and
      // the grid's live box, so a page that auto-scrolled during the drag
      // lands the tile under the pointer, not where the pointer was on screen.
      const origin = grid.getBoundingClientRect();
      // A keyboard step reports where it is going (`to`, or `by` from here)
      // before the operation's position catches up; read the position alone
      // and the preview trailed the tile by one key press.
      const now = event.operation.position.current;
      const pointer =
         event.to ??
         (event.by ? { x: now.x + event.by.x, y: now.y + event.by.y } : now);
      const moved = live.base[live.index];
      const cell = cellAt(
         pointer.x - live.grab.x - origin.left,
         pointer.y - live.grab.y - origin.top,
         moved.colspan ?? 1,
         placement.columns,
         trackWidth(grid.getBoundingClientRect().width, placement.columns),
      );
      const current = previewRef.current?.[live.index];
      if (current?.row === cell.row && current.col === cell.col) return;
      const next = dropAt(live.base, live.index, cell, placement.columns);
      previewRef.current = next;
      setPreview(next);
   };

   const onDragOver = (event: DragOverEvent) => {
      // Placing, where the tile is over says nothing: the cell under it does.
      // Prevented, so the library does not reorder the tiles under it either.
      if (placing.current) {
         event.preventDefault();
         return;
      }
      const { source, target } = event.operation;
      if (!source || !target) return;
      const current = previewRef.current ?? tiles;
      let next: DashboardTile[];
      if (target.type === GAP_TYPE) {
         const { after } = target.data as GapData;
         const from = current.findIndex((tile) => tileKey(tile) === source.id);
         // The gap right after the dragged tile is the one it is already
         // previewed in; there is nothing to change.
         if (from < 0 || after === source.id) return;
         const rest = current.filter((_, index) => index !== from);
         const to = rest.findIndex((tile) => tileKey(tile) === after) + 1;
         next = moveIntoGap(current, from, to);
      } else {
         // Onto a tile: the library's own `move` — the same arithmetic every
         // sortable list built on it uses, over the order as it stands. Then
         // the row starts re-applied by position, so the rows keep their shape
         // and the tiles flow through them.
         const keys = current.map(tileKey);
         const reordered = move(keys, event);
         if (reordered.every((key, index) => key === keys[index])) return;
         const byKey = new Map(current.map((tile) => [tileKey(tile), tile]));
         next = keepRowStructure(
            reordered.map((key) => byKey.get(key) as DashboardTile),
            tiles.map((tile) => tile.break ?? false),
         );
      }
      previewRef.current = next;
      setPreview(next);
   };

   const onDragEnd = (event: DragEndEvent) => {
      setDragging(false);
      const live = placing.current;
      const wasPlacing = live !== undefined;
      placing.current = undefined;
      const previewed = previewRef.current;
      // A placing drag that let go in the cell it started from moved nothing:
      // it writes nothing, and a report that flowed keeps flowing rather than
      // being placed (and its heights rounded to rows) by a stray wiggle.
      const stayed =
         live !== undefined &&
         previewed !== undefined &&
         previewed[live.index]?.row === live.base[live.index]?.row &&
         previewed[live.index]?.col === live.base[live.index]?.col;
      // Placed tiles are written in reading order, so the file reads the way
      // the page does and a narrow screen stacks them the same way.
      const next =
         wasPlacing && previewed ? inReadingOrder(previewed) : previewed;
      previewRef.current = undefined;
      setPreview(undefined);
      if (event.canceled || !next || stayed) return;
      // ONE history entry for the whole drag; a drag that ends where it began
      // changes nothing and makes none.
      commit(next);
      const landing = next.findIndex(
         (tile) => tileKey(tile) === event.operation.source?.id,
      );
      if (landing >= 0) onLanded(landing);
   };

   return { dragging, preview, onDragStart, onDragMove, onDragOver, onDragEnd };
}
