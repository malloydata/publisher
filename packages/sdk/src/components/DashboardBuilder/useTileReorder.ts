// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { move } from "@dnd-kit/helpers";
import type { DragEndEvent, DragOverEvent } from "@dnd-kit/react";
import { useRef, useState } from "react";
import type { DashboardTile } from "./document";
import { keepRowStructure, moveIntoGap, tileKey } from "./layout";
import { GAP_TYPE, type GapData } from "./sortable";

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
}: {
   tiles: DashboardTile[];
   /** The order to write when the drag ends somewhere new. */
   commit: (next: DashboardTile[]) => void;
   /** Where the dragged tile came to rest, which the caller selects. */
   onLanded: (index: number) => void;
}) {
   // A move in flight: the tiles as they will stand if the drag ends now.
   // CUMULATIVE — each report moves the tile from where the last report left
   // it, which is the sortable convention and what the library's optimistic
   // sorting assumes. The first version rebuilt the order from the document on
   // every report, and as the row reflowed under the pointer the tile beneath
   // it changed, so the target flipped back and forth. State for the render,
   // and a ref for `onDragEnd`, which can fire before React has committed it.
   const [preview, setPreview] = useState<DashboardTile[] | undefined>(
      undefined,
   );
   const previewRef = useRef<DashboardTile[] | undefined>(undefined);
   // Whether a drag is live at all: what turns the row-end gaps into drop
   // targets and draws the grid guides.
   const [dragging, setDragging] = useState(false);

   /**
    * Reordering, unlike resizing, is offered on EVERY tile — an inherited one
    * included.
    *
    * The two edits touch different parts of the file. A width is a `# colspan`
    * tag on the view, so a tile whose view lives in the model cannot be
    * resized here; but order is the `tiles=[…]` array on this file's own
    * `## artifact` tag, which this file always owns. So a tile the properties
    * panel refuses can still be moved.
    *
    * The gesture itself is `@dnd-kit/react`'s — see `sortable.tsx`. What is
    * decided HERE is what a drop means for the file. The library reports which
    * target the tile is over; these handlers turn that into an order and a set
    * of row starts, through `keepRowStructure` for a drop onto a tile and
    * `moveIntoGap` for a drop into the empty end of a row. The preview is
    * rebuilt from the document on each report and written once, on release:
    * ONE history entry for the whole drag, however far it wandered.
    */
   const onDragStart = () => {
      setDragging(true);
      previewRef.current = tiles;
   };

   const onDragOver = (event: DragOverEvent) => {
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
      const next = previewRef.current;
      previewRef.current = undefined;
      setPreview(undefined);
      if (event.canceled || !next) return;
      // ONE history entry for the whole drag; a drag that ends where it began
      // changes nothing and makes none.
      commit(next);
      const landing = next.findIndex(
         (tile) => tileKey(tile) === event.operation.source?.id,
      );
      if (landing >= 0) onLanded(landing);
   };

   return { dragging, preview, onDragStart, onDragOver, onDragEnd };
}
