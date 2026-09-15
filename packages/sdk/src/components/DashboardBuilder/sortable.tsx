// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { pointerIntersection } from "@dnd-kit/collision";
import { Feedback, KeyboardSensor, PointerSensor } from "@dnd-kit/dom";
import { useDroppable } from "@dnd-kit/react";
import { useSortable, type UseSortableInput } from "@dnd-kit/react/sortable";
import type { ReactNode } from "react";

/**
 * The builder's drag and drop, on `@dnd-kit/react`.
 *
 * The library owns the GESTURE: sensors for pointer, touch and keyboard, the
 * activation threshold, collision detection, the copy that follows the pointer,
 * the slide of the other tiles into place, auto-scroll at the viewport's edge,
 * and the screen-reader announcements.
 *
 * The builder owns the LAYOUT and what a drop MEANS. Tiles sit on the reader's
 * own `DashboardGrid`, and where a tile lands in the file — its place in
 * `tiles=[…]`, and which row it starts — is settled by `keepRowStructure` and
 * `moveIntoGap` in the builder. This module only reports "this tile is over
 * that target".
 */

/** The draggable type of a tile, and what every drop target accepts. */
const TILE_TYPE = "tile";
/** The type of a gap: the empty end of a row, offered as a drop target. */
export const GAP_TYPE = "gap";

/**
 * The sensors: the library's own, with one addition and one exception.
 *
 * The ADDITION is `activatorElements`: the whole tile starts a pointer drag,
 * not only the grip. A 22px corner that appears on hover is not a target a
 * reader finds; the whole card is. The grip stays the `handle`, which is what
 * gives it keyboard focus and the screen-reader instructions — and, by the
 * library's own rule, a press on the handle drags at once while a press
 * anywhere else on the card waits for a short hold or a few pixels of travel,
 * so a click inside a tile stays a click. Those thresholds, and the check that
 * a press on a button or a link is never a drag, are the defaults: they are
 * what every surface built on this library feels like, and the hand-tuned
 * versions this replaced were most of why the gesture felt unfamiliar.
 *
 * The EXCEPTION is the resize handle. Its own pointer handling stops the press
 * in React, but React's listener sits at the root and runs after the sensor's
 * own listener on the tile has already seen it; so the sensor is told directly
 * that a press there is not the start of a move.
 */
const RESIZE_HANDLE = '[role="separator"]';
export const builderSensors = [
   PointerSensor.configure({
      activatorElements: (source) => [source.element],
      preventActivation: (event, source) =>
         (PointerSensor.defaults.preventActivation?.(event, source) ?? false) ||
         (event.target instanceof Element &&
            event.target.closest(RESIZE_HANDLE) !== null),
   }),
   KeyboardSensor,
];

/**
 * What a tile looks like while it is dragged: a COPY follows the pointer, and
 * the tile itself stays in the flow at its previewed place, dimmed. The
 * library's default lifts the element itself out and leaves a placeholder,
 * which is the other way round from the point of the preview — that the row
 * you see reflow under the pointer is the row that lands.
 */
const tilePlugins: NonNullable<UseSortableInput["plugins"]> = (defaults) => [
   ...defaults,
   Feedback.configure({ feedback: "clone" }),
];

interface TileSortableHandles {
   /** The tile's element: what is dragged, and what a drop lands on. */
   ref: (element: Element | null) => void;
   /** The grip: keyboard focus and screen-reader instructions live here. */
   handleRef: (element: Element | null) => void;
   /** This tile is the one being dragged. */
   isDragSource: boolean;
}

/**
 * A tile as a sortable item. A component rather than a hook call in the
 * builder's render, because the grid renders tiles through a callback and a
 * hook cannot be called from one.
 *
 * `index` is the tile's place in the PREVIEWED order, which is what lets the
 * library animate a tile to its new slot when that order changes under it.
 */
export function TileSortable({
   id,
   index,
   children,
}: {
   id: string;
   index: number;
   children: (handles: TileSortableHandles) => ReactNode;
}) {
   const { ref, handleRef, isDragSource } = useSortable({
      id,
      index,
      type: TILE_TYPE,
      accept: TILE_TYPE,
      plugins: tilePlugins,
   });
   return <>{children({ ref, handleRef, isDragSource })}</>;
}

/** What a gap drop target carries: the key of the tile the gap follows. */
export interface GapData {
   after: string;
}

/**
 * The empty end of a row, as a drop target.
 *
 * A flow grid has no element where a row runs out; the space is simply not
 * painted. So while a drag is live the builder lays one of these into each
 * such space, spanning exactly the columns left over, and the library can
 * report the pointer over it like any other target. `pointerIntersection`
 * rather than the default shape overlap: the dragged copy is tile-sized and
 * would overlap a neighbouring tile more than a narrow gap; the pointer is
 * where the reader means.
 */
export function GapDroppable({
   id,
   after,
   children,
}: {
   id: string;
   after: string;
   children: (handles: {
      ref: (element: Element | null) => void;
      isDropTarget: boolean;
   }) => ReactNode;
}) {
   const { ref, isDropTarget } = useDroppable<GapData>({
      id,
      type: GAP_TYPE,
      accept: TILE_TYPE,
      collisionDetector: pointerIntersection,
      data: { after },
   });
   return <>{children({ ref, isDropTarget })}</>;
}
