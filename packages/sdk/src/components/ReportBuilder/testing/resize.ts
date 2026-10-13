// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { fireEvent, screen } from "@testing-library/react";
import { GRID_GAP_PX } from "../../Dashboard/DashboardGrid";

/**
 * Drag a tile's right edge until it spans `columns` of a 12-column grid whose
 * tracks are 100px wide and which starts, like every tile, at x = 0. The test
 * DOM lays nothing out, so the measurements the resize reads are stubbed for
 * the length of the gesture.
 */
export function dragEdge(label: string, columns: number) {
   // N tracks and the N - 1 gutters between them.
   const clientX = columns * 100 + (columns - 1) * GRID_GAP_PX;
   const handle = screen.getByRole("separator", { name: `Width of ${label}` });
   const width = 1200 + GRID_GAP_PX * 11;
   const measure = HTMLElement.prototype.getBoundingClientRect;
   HTMLElement.prototype.getBoundingClientRect = () =>
      ({
         left: 0,
         top: 0,
         x: 0,
         y: 0,
         width,
         height: 100,
         right: width,
         bottom: 100,
         toJSON: () => ({}),
      }) as DOMRect;
   let captured = false;
   Object.assign(handle, {
      setPointerCapture: () => {
         captured = true;
      },
      hasPointerCapture: () => captured,
      releasePointerCapture: () => {
         captured = false;
      },
   });
   try {
      fireEvent.pointerDown(handle, { pointerId: 1, clientX: 0, button: 0 });
      fireEvent.pointerMove(handle, { pointerId: 1, clientX });
      fireEvent.pointerUp(handle, { pointerId: 1, clientX });
   } finally {
      HTMLElement.prototype.getBoundingClientRect = measure;
   }
}
