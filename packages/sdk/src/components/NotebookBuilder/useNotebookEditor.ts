// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useCallback, useRef, useState } from "react";
import type { SpliceResult } from "../DashboardBuilder/spliceResult";
import {
   type DocumentEditor,
   type SaveOutcome,
   useDocumentEditor,
} from "../DashboardBuilder/useDocumentEditor";
import {
   notebookSourceRefused,
   readNotebookSource,
} from "./readNotebookSource";
import {
   canMove,
   leadingComments,
   spliceNotebookDocument,
   type DefinitionsAbove,
   type NotebookDocument,
} from "./spliceNotebook";

export type { SaveOutcome };

export interface NotebookEditor extends DocumentEditor<NotebookDocument> {
   /** Whether `cells[from]` may land at `to`, judged against the file as opened and as last saved. */
   canMove: (from: number, to: number) => boolean;
   /** The comment lines that saving now would remove along with the cells they open, from the file as last saved. */
   removedComments: () => Promise<string[]>;
}

/** A cell was added or removed, which is when the file's diff is shown before a save. */
const cellsChanged = (saved: NotebookDocument, next: NotebookDocument) => {
   const before = new Set(saved.cells.map((cell) => cell.id));
   const after = new Set(next.cells.map((cell) => cell.id));
   return (
      [...before].some((id) => !after.has(id)) ||
      [...after].some((id) => !before.has(id))
   );
};

/** Doc id → the cell's id in the last saved file's read. */
type Placement = ReadonlyMap<string, string>;

/** `doc` in the ids of the last saved file, so the writer and `canMove` read it against what is on disk. */
export function rebased(
   doc: NotebookDocument,
   placed: Placement | undefined,
): NotebookDocument {
   if (placed === undefined) return doc;
   return {
      cells: doc.cells.map((cell) => {
         const id = placed.get(cell.id);
         if (id !== undefined) {
            const { added: _added, ...rest } = cell;
            return { ...rest, id };
         }
         // Absent from the saved file (a removal undone), so its old read index would name some other cell there.
         if (!cell.added && cell.kind === "markdown")
            return { ...cell, id: `restored-${cell.id}`, added: true };
         return cell;
      }),
   };
}

/** Each query's definitions above it in `doc`, by its id there. */
const definitionsAboveOf = (doc: NotebookDocument): DefinitionsAbove => {
   const above = new Map<string, number>();
   let definitions = 0;
   for (const cell of doc.cells) {
      if (cell.kind === "definition") definitions++;
      else if (cell.kind === "query") above.set(cell.id, definitions);
   }
   return above;
};

/** `opened` re-keyed by the ids `rebased` gave `doc`'s cells, which it keeps in order. */
const rekeyed = (
   opened: DefinitionsAbove,
   doc: NotebookDocument,
   onDisk: NotebookDocument,
): DefinitionsAbove =>
   new Map(
      doc.cells.flatMap((cell, i): [string, number][] => {
         const count = opened.get(cell.id);
         return count === undefined ? [] : [[onDisk.cells[i].id, count]];
      }),
   );

/** Where each cell of `doc` sits in the file a splice of it wrote. */
const placementOf = (doc: NotebookDocument): Placement =>
   new Map(doc.cells.map((cell, index) => [cell.id, String(index)]));

/** Recent splice outputs, since a preview and a save may splice concurrently; a few are plenty. */
const PENDING_LIMIT = 4;

/** The placement for the file `source`, which a splice must have produced; throws, before anything is written, if none did. */
export function takePlacement(
   pending: Map<string, Placement>,
   source: string,
): Placement {
   const placement = pending.get(source);
   if (placement === undefined)
      throw new Error(
         "the editor cannot tell which cells this file holds, so it was not written. Your changes are still here.",
      );
   pending.clear();
   return placement;
}

export function useNotebookEditor(options: {
   /** The file as read from storage. */
   source: string;
   /** The document that file produced. */
   document: NotebookDocument;
   /** Persist the patched file. Rejecting leaves the editor dirty. */
   onSave?: (source: string) => Promise<void> | void;
}): NotebookEditor {
   const { onSave } = options;
   // Cell ids are read indices of the file as opened; after a save they are mapped onto the saved file's read.
   // `undefined` until a save, since an empty placement (every cell removed) is still a save.
   const placed = useRef<Placement | undefined>(undefined);
   const pending = useRef(new Map<string, Placement>());
   // A save never lowers a query's count, so the file as opened is the highest any query may rise.
   const [opened] = useState(() => definitionsAboveOf(options.document));

   const splice = useCallback(
      async (source: string, doc: NotebookDocument): Promise<SpliceResult> => {
         const onDisk = rebased(doc, placed.current);
         const result = await spliceNotebookDocument(
            source,
            onDisk,
            rekeyed(opened, doc, onDisk),
         );
         if (result.ok) {
            pending.current.delete(result.source);
            pending.current.set(result.source, placementOf(doc));
            while (pending.current.size > PENDING_LIMIT)
               pending.current.delete(pending.current.keys().next().value!);
         }
         return result;
      },
      [opened],
   );

   const save = useCallback(
      async (source: string) => {
         const placement = takePlacement(pending.current, source);
         await onSave?.(source);
         placed.current = placement;
      },
      [onSave],
   );

   const editor = useDocumentEditor<NotebookDocument>({
      source: options.source,
      document: options.document,
      ...(onSave ? { onSave: save } : {}),
      splice,
      structural: cellsChanged,
   });

   const { document, source } = editor;
   const canMoveHere = useCallback(
      (from: number, to: number) => {
         const onDisk = rebased(document, placed.current);
         return canMove(onDisk, from, to, rekeyed(opened, document, onDisk));
      },
      [document, opened],
   );

   const removedComments = useCallback(async () => {
      const read = await readNotebookSource(source);
      if (notebookSourceRefused(read)) return [];
      const kept = new Set(
         rebased(document, placed.current)
            .cells.filter((cell) => !cell.added)
            .map((cell) => cell.id),
      );
      const { text, cells } = read.source;
      return cells
         .filter((cell) => cell.kind === "markdown" && !kept.has(cell.id))
         .flatMap((cell) =>
            (leadingComments(text.slice(cell.span.start, cell.span.end)) ?? "")
               .split(/\r?\n/)
               .filter((line) => line.trim() !== ""),
         );
   }, [document, source]);

   return { ...editor, canMove: canMoveHere, removedComments };
}
