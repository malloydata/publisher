// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { act, renderHook } from "@testing-library/react";
import { describe, expect, it, mock } from "bun:test";
import type { DragEndEvent, DragOverEvent } from "@dnd-kit/react";
import { canMove, type NotebookDocument } from "./spliceNotebook";
import { useCellReorder } from "./useCellReorder";

const doc: NotebookDocument = {
   cells: [
      { id: "0", kind: "markdown", markdown: "Intro." },
      { id: "1", kind: "definition" },
      { id: "2", kind: "markdown", markdown: "Middle." },
      { id: "3", kind: "query" },
   ],
};
const ids = doc.cells.map((cell) => cell.id);

/** A report that `id` is over the cell `over`, in the shape `move` reads. */
const over = (id: string, target: string) => {
   const preventDefault = mock(() => {});
   const event = {
      operation: {
         source: { id, sortable: {} },
         target: { id: target, sortable: {} },
      },
      preventDefault,
   } as unknown as DragOverEvent;
   return { event, preventDefault };
};

const ended = (id: string, canceled = false) =>
   ({ canceled, operation: { source: { id } } }) as unknown as DragEndEvent;

const mount = () => {
   const commit = mock((_from: number, _to: number) => {});
   const view = renderHook(() =>
      useCellReorder({
         ids,
         canMove: (from, to) => canMove(doc, from, to),
         commit,
      }),
   );
   return { view, commit };
};

describe("useCellReorder", () => {
   it("previews a legal drop and writes it once, on release", () => {
      const { view, commit } = mount();
      act(() => view.result.current.onDragStart("3"));
      expect(view.result.current.dragging).toBe(3);
      const legal = over("3", "2");
      act(() => view.result.current.onDragOver(legal.event));
      expect(view.result.current.preview).toEqual(["0", "1", "3", "2"]);
      expect(legal.preventDefault).not.toHaveBeenCalled();
      expect(commit).not.toHaveBeenCalled();
      act(() => view.result.current.onDragEnd(ended("3")));
      expect(commit).toHaveBeenCalledTimes(1);
      expect(commit.mock.calls[0]).toEqual([3, 2]);
      expect(view.result.current.preview).toBeUndefined();
      expect(view.result.current.dragging).toBeUndefined();
   });

   it("never previews or writes a query above the definition it reads", () => {
      const { view, commit } = mount();
      act(() => view.result.current.onDragStart("3"));
      const refused = over("3", "0");
      act(() => view.result.current.onDragOver(refused.event));
      expect(view.result.current.preview).toBeUndefined();
      // Stops dnd-kit's optimistic sort from moving the DOM on its own.
      expect(refused.preventDefault).toHaveBeenCalledTimes(1);
      act(() => view.result.current.onDragEnd(ended("3")));
      expect(commit).not.toHaveBeenCalled();
   });

   it("writes nothing when the drag is cancelled", () => {
      const { view, commit } = mount();
      act(() => view.result.current.onDragStart("0"));
      act(() => view.result.current.onDragOver(over("0", "3").event));
      act(() => view.result.current.onDragEnd(ended("0", true)));
      expect(commit).not.toHaveBeenCalled();
   });

   it("drops a legal preview without writing when the drag is cancelled", () => {
      const { view, commit } = mount();
      act(() => view.result.current.onDragStart("3"));
      act(() => view.result.current.onDragOver(over("3", "2").event));
      expect(view.result.current.preview).toEqual(["0", "1", "3", "2"]);
      act(() => view.result.current.onDragEnd(ended("3", true)));
      expect(commit).not.toHaveBeenCalled();
      expect(view.result.current.preview).toBeUndefined();
   });
});
