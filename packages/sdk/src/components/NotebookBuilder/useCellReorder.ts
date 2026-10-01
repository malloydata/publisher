// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { move } from "@dnd-kit/helpers";
import type { DragEndEvent, DragOverEvent } from "@dnd-kit/react";
import { useRef, useState } from "react";

/** Drag-to-reorder, committed once on release so a drag is one history entry. */
export function useCellReorder({
   ids,
   canMove,
   commit,
}: {
   ids: string[];
   canMove: (from: number, to: number) => boolean;
   commit: (from: number, to: number) => void;
}) {
   const [preview, setPreview] = useState<string[] | undefined>(undefined);
   // A ref too, because `onDragEnd` can fire before React has committed the last preview.
   const previewRef = useRef<string[] | undefined>(undefined);
   const [dragging, setDragging] = useState<number | undefined>(undefined);

   const onDragStart = (id: string) => {
      setDragging(ids.indexOf(id));
      previewRef.current = ids;
   };

   const onDragOver = (event: DragOverEvent) => {
      const { source } = event.operation;
      if (!source) return;
      const current = previewRef.current ?? ids;
      const next = move(current, event);
      const to = next.indexOf(String(source.id));
      if (to < 0 || next.every((id, index) => id === current[index])) return;
      if (!canMove(ids.indexOf(String(source.id)), to)) {
         // Otherwise dnd-kit's optimistic sorting reorders the DOM itself, out of step with the document.
         event.preventDefault();
         return;
      }
      previewRef.current = next;
      setPreview(next);
   };

   const onDragEnd = (event: DragEndEvent) => {
      const next = previewRef.current;
      previewRef.current = undefined;
      setPreview(undefined);
      setDragging(undefined);
      const id = event.operation.source?.id;
      if (event.canceled || !next || id === undefined) return;
      const from = ids.indexOf(String(id));
      const to = next.indexOf(String(id));
      if (from < 0 || to < 0 || from === to || !canMove(from, to)) return;
      commit(from, to);
   };

   return { preview, dragging, onDragStart, onDragOver, onDragEnd };
}
