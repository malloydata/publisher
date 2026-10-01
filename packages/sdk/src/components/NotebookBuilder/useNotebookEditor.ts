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
   undoUnsafeAfter,
   type DefinitionsAbove,
   type NotebookDocument,
} from "./spliceNotebook";

export type { SaveOutcome };

export interface NotebookEditor extends DocumentEditor<NotebookDocument> {
   /** Whether `cells[from]` may land at `to`, judged against the file as opened and as last saved. */
   canMove: (from: number, to: number) => boolean;
   /** The comment lines that saving now would remove along with the cells they open, from the file as last saved. */
   removedComments: () => Promise<string[]>;
   /** Whether the cell is in the file as last saved, which is when an added query's run and caption can no longer change. */
   isInFile: (id: string) => boolean;
}

const runKey = (cell: NotebookDocument["cells"][number]) =>
   JSON.stringify(cell.run ?? null);

/** The runs of the added query cells in `doc`, by doc id. */
const runsOf = (doc: NotebookDocument): Map<string, string> =>
   new Map(
      doc.cells
         .filter((cell) => cell.added && cell.kind === "query" && cell.run)
         .map((cell) => [cell.id, runKey(cell)]),
   );

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
function rebased(
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
         // A read query cannot be re-authored, so it stays unplaceable and the writer refuses it.
         if (!cell.added && cell.kind === "query")
            return { ...cell, id: `restored-${cell.id}` };
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

/** The span's whole-line comments (line or block), which the writer deletes with their cell. */
function wholeLineComments(span: string): string[] {
   const out: string[] = [];
   let inBlock = false;
   for (const line of span.split(/\r?\n/)) {
      const trimmed = line.trim();
      if (inBlock || /^(\/\/|--|\/\*)/.test(trimmed)) {
         out.push(line);
         if (inBlock) inBlock = !trimmed.includes("*/");
         else if (trimmed.startsWith("/*"))
            inBlock = !trimmed.slice(2).includes("*/");
      }
   }
   return out;
}

export function useNotebookEditor(options: {
   /** The file as read from storage. */
   source: string;
   /** The document that file produced. */
   document: NotebookDocument;
   /** Persist the patched file. Rejecting leaves the editor dirty. */
   onSave?: (source: string) => Promise<void> | void;
   /** The sources the notebook's compiled model offers, which an added query cell must pick from. */
   reachableSources?: readonly string[];
}): NotebookEditor {
   const { onSave, reachableSources } = options;
   // Cell ids are read indices of the file as opened; after a save they are mapped onto the saved file's read.
   // `undefined` until a save, since an empty placement (every cell removed) is still a save.
   const placed = useRef<Placement | undefined>(undefined);
   const pending = useRef(new Map<string, Placement>());
   // What each saved added query was written from: once it is in the file its run and caption are no longer the writer's to change.
   const written = useRef(new Map<string, string>());
   const pendingRuns = useRef(new Map<string, Map<string, string>>());
   // A save never lowers a query's count, so the file as opened is the highest any query may rise.
   const [opened] = useState(() => definitionsAboveOf(options.document));

   const splice = useCallback(
      async (source: string, doc: NotebookDocument): Promise<SpliceResult> => {
         const changed = doc.cells.find(
            (cell) =>
               cell.kind === "query" &&
               placed.current?.has(cell.id) &&
               written.current.has(cell.id) &&
               written.current.get(cell.id) !== runKey(cell),
         );
         if (changed)
            return {
               ok: false,
               reason:
                  "A query that is already saved cannot have its source, view or caption changed here. Remove it and add a new one. Your changes are still here.",
            };
         const onDisk = rebased(doc, placed.current);
         const result = await spliceNotebookDocument(
            source,
            onDisk,
            rekeyed(opened, doc, onDisk),
            reachableSources,
         );
         if (result.ok) {
            pending.current.delete(result.source);
            pending.current.set(result.source, placementOf(doc));
            pendingRuns.current.delete(result.source);
            pendingRuns.current.set(result.source, runsOf(doc));
            while (pending.current.size > PENDING_LIMIT)
               pending.current.delete(pending.current.keys().next().value!);
            while (pendingRuns.current.size > PENDING_LIMIT)
               pendingRuns.current.delete(
                  pendingRuns.current.keys().next().value!,
               );
         }
         return result;
      },
      [opened, reachableSources],
   );

   const save = useCallback(
      async (source: string) => {
         const placement = takePlacement(pending.current, source);
         const runs = pendingRuns.current.get(source) ?? new Map();
         pendingRuns.current.clear();
         await onSave?.(source);
         placed.current = placement;
         written.current = runs;
      },
      [onSave],
   );

   const editor = useDocumentEditor<NotebookDocument>({
      source: options.source,
      document: options.document,
      ...(onSave ? { onSave: save } : {}),
      splice,
      structural: cellsChanged,
      clearsHistory: undoUnsafeAfter,
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
         .filter((cell) => cell.kind !== "definition" && !kept.has(cell.id))
         .flatMap((cell) => {
            const span = text.slice(cell.span.start, cell.span.end);
            // A query's span can also carry comments between its tags and its run.
            const lines =
               cell.kind === "query"
                  ? wholeLineComments(span)
                  : (leadingComments(span) ?? "").split(/\r?\n/);
            return [...new Set(lines.filter((line) => line.trim() !== ""))];
         });
   }, [document, source]);

   const isInFile = useCallback(
      (id: string) => {
         const cell = document.cells.find((c) => c.id === id);
         return (
            cell !== undefined &&
            (!cell.added || placed.current?.has(id) === true)
         );
      },
      [document],
   );

   return { ...editor, canMove: canMoveHere, removedComments, isInFile };
}
