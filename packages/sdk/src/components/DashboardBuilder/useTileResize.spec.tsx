// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { act, renderHook } from "@testing-library/react";
import { describe, expect, it, mock } from "bun:test";
import type { PointerEvent } from "react";
import { GRID_GAP_PX } from "../Dashboard/DashboardGrid";
import type { DashboardTile } from "./document";
import { useTileResize } from "./useTileResize";

const COLUMNS = 12;
const GRID_WIDTH = 1200 + GRID_GAP_PX * (COLUMNS - 1);
const TRACK = 100;

const tiles: DashboardTile[] = [
   {
      name: "a",
      source: "s",
      declaration: { kind: "reference", from: "x" },
      colspan: 6,
   },
   { name: "b", source: "s", declaration: { kind: "inherited" } },
];

/** A pointer event on a handle whose tile starts at `left`, with the grid measured. */
const pointer = (clientX: number, left = 0) => {
   const handle = document.createElement("div");
   const tile = document.createElement("div");
   tile.appendChild(handle);
   tile.getBoundingClientRect = () =>
      ({ left, width: 600, top: 0, height: 100 }) as DOMRect;
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
   return {
      clientX,
      pointerId: 1,
      currentTarget: handle,
      stopPropagation: mock(() => {}),
      preventDefault: mock(() => {}),
   } as unknown as PointerEvent<HTMLDivElement>;
};

const mount = () => {
   const commit = mock((_index: number, _span: number) => {});
   const onStart = mock((_index: number) => {});
   const view = renderHook(() =>
      useTileResize({ tiles, columns: COLUMNS, onStart, commit }),
   );
   const grid = document.createElement("div");
   grid.getBoundingClientRect = () =>
      ({ left: 0, width: GRID_WIDTH, top: 0, height: 400 }) as DOMRect;
   (view.result.current.gridBox as { current: HTMLDivElement | null }).current =
      grid;
   return { view, commit, onStart };
};

describe("useTileResize", () => {
   it("previews the width in whole columns as the edge moves, and writes it once on release", () => {
      const { view, commit, onStart } = mount();
      act(() => view.result.current.startResize(pointer(600), 0));
      expect(onStart).toHaveBeenCalledWith(0);
      expect(view.result.current.resize).toMatchObject({
         index: 0,
         span: 6,
         track: TRACK,
      });

      // Eight tracks and seven gutters: the edge sits at exactly eight columns.
      act(() =>
         view.result.current.onResize(pointer(8 * TRACK + 7 * GRID_GAP_PX)),
      );
      expect(view.result.current.resize?.span).toBe(8);
      // Part way into the ninth column still rounds to eight; nothing is
      // written yet.
      act(() =>
         view.result.current.onResize(
            pointer(8 * TRACK + 7 * GRID_GAP_PX + TRACK / 3),
         ),
      );
      expect(view.result.current.resize?.span).toBe(8);
      expect(commit).not.toHaveBeenCalled();

      act(() => view.result.current.endResize(pointer(0)));
      expect(commit).toHaveBeenCalledTimes(1);
      expect(commit).toHaveBeenCalledWith(0, 8);
      expect(view.result.current.resize).toBeUndefined();
   });

   it("clamps the preview to the grid, one column to full width", () => {
      const { view } = mount();
      act(() => view.result.current.startResize(pointer(600), 0));
      act(() => view.result.current.onResize(pointer(-500)));
      expect(view.result.current.resize?.span).toBe(1);
      act(() => view.result.current.onResize(pointer(GRID_WIDTH * 3)));
      expect(view.result.current.resize?.span).toBe(COLUMNS);
   });

   it("does nothing when the grid has not been measured", () => {
      const commit = mock(() => {});
      const view = renderHook(() =>
         useTileResize({
            tiles,
            columns: COLUMNS,
            onStart: () => {},
            commit,
         }),
      );
      act(() => view.result.current.startResize(pointer(600), 0));
      expect(view.result.current.resize).toBeUndefined();
      act(() => view.result.current.endResize(pointer(0)));
      expect(commit).not.toHaveBeenCalled();
   });
});
