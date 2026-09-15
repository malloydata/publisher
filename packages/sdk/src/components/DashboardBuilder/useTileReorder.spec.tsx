// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { act, renderHook } from "@testing-library/react";
import { describe, expect, it, mock } from "bun:test";
import type { DragEndEvent, DragOverEvent } from "@dnd-kit/react";
import { tileKey, type DashboardTile } from "./document";
import { GAP_TYPE } from "./sortable";
import { useTileReorder } from "./useTileReorder";

const tile = (
   name: string,
   extra: Partial<DashboardTile> = {},
): DashboardTile => ({
   name,
   source: "s",
   declaration: { kind: "reference", from: name },
   colspan: 6,
   ...extra,
});
// Two rows of two: c starts the second row.
const tiles = [tile("a"), tile("b"), tile("c", { break: true }), tile("d")];

/** A report that the tile `id` is over the gap after `after`. */
const overGap = (id: string, after: string) =>
   ({
      operation: {
         source: { id },
         target: { type: GAP_TYPE, data: { after } },
      },
   }) as unknown as DragOverEvent;

const ended = (id: string, canceled = false) =>
   ({ canceled, operation: { source: { id } } }) as unknown as DragEndEvent;

const mount = () => {
   const commit = mock((_next: DashboardTile[]) => {});
   const onLanded = mock((_index: number) => {});
   const view = renderHook(() => useTileReorder({ tiles, commit, onLanded }));
   return { view, commit, onLanded };
};

describe("useTileReorder", () => {
   it("previews a drop into a row's gap, and writes the order once on release", () => {
      const { view, commit, onLanded } = mount();
      act(() => view.result.current.onDragStart());
      expect(view.result.current.dragging).toBe(true);

      // d, dragged up into the gap after b: it joins the first row, and c
      // keeps starting the second.
      act(() =>
         view.result.current.onDragOver(
            overGap(tileKey(tiles[3]), tileKey(tiles[1])),
         ),
      );
      expect(view.result.current.preview?.map((t) => t.name)).toEqual([
         "a",
         "b",
         "d",
         "c",
      ]);
      expect(view.result.current.preview?.map((t) => t.break ?? false)).toEqual(
         [false, false, false, true],
      );
      expect(commit).not.toHaveBeenCalled();

      act(() => view.result.current.onDragEnd(ended(tileKey(tiles[3]))));
      expect(commit).toHaveBeenCalledTimes(1);
      expect(commit.mock.calls[0][0].map((t) => t.name)).toEqual([
         "a",
         "b",
         "d",
         "c",
      ]);
      expect(onLanded).toHaveBeenCalledWith(2);
      expect(view.result.current.dragging).toBe(false);
      expect(view.result.current.preview).toBeUndefined();
   });

   it("ignores the gap right after the tile in hand, and writes nothing on cancel", () => {
      const { view, commit } = mount();
      act(() => view.result.current.onDragStart());
      act(() =>
         view.result.current.onDragOver(
            overGap(tileKey(tiles[1]), tileKey(tiles[1])),
         ),
      );
      expect(view.result.current.preview).toBeUndefined();
      act(() =>
         view.result.current.onDragOver(
            overGap(tileKey(tiles[3]), tileKey(tiles[1])),
         ),
      );
      act(() => view.result.current.onDragEnd(ended(tileKey(tiles[3]), true)));
      expect(commit).not.toHaveBeenCalled();
   });
});
