// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useCallback, useMemo, useState } from "react";
import { type DashboardDocument } from "./document";
import {
   spliceDashboardDocument,
   spliceFailed,
   tileFileKey,
} from "./spliceDocument";

/**
 * The editor's state: the document being edited, its history, and saving.
 *
 * Deliberately holds the DOCUMENT and nothing else. Given VALUES — what the
 * preview is running with — are editor state rather than document state and
 * live outside this hook, which is what keeps them out of the undo stack. Undo
 * should take back a layout change; it should not rewind the filter someone was
 * previewing under.
 */

export interface DashboardEditor {
   document: DashboardDocument;
   /** The file as it currently stands, which is what a save patches. */
   source: string;
   canUndo: boolean;
   canRedo: boolean;
   /** Whether the document has changes the file does not have yet. */
   dirty: boolean;
   /**
    * Why the last save did not happen. The document is untouched when this is
    * set: a refused write keeps the reader's work and reports a defect in the
    * writer, rather than discarding an edit because a tool could not read back
    * what it produced.
    */
   error?: string;
   /** Apply a change. Receives a copy; mutate it freely. */
   update: (change: (draft: DashboardDocument) => void) => void;
   undo: () => void;
   redo: () => void;
   /** Splice the change into the file, or say why it was refused. */
   save: () => Promise<SaveOutcome>;
   /**
    * The file a save would write, without writing it: what a diff preview
    * shows. The failure arm is the writer's refusal, worded for the author.
    */
   preview: () => Promise<
      { ok: true; source: string } | { ok: false; reason: string }
   >;
   /**
    * Whether the unsaved change adds or removes a tile. Those moves touch
    * declarations and the comments beside them, which is when a save is worth
    * showing as a diff first; a property edit never is.
    */
   structural: boolean;
}

export type SaveOutcome = { ok: true } | { ok: false; reason: string };

interface History {
   /** Every document state, oldest first. */
   stack: DashboardDocument[];
   /** Which one is current. Undo and redo move this rather than mutating. */
   index: number;
}

/**
 * How many documents of history to keep.
 *
 * A whole document per entry is affordable because the document is small and
 * immutable — it is a list of tiles and their presentation, not the file. Keeping
 * whole states is also what makes undo trivially correct: there is no inverse
 * operation to get wrong for each kind of edit.
 */
const HISTORY_LIMIT = 100;

export function useDashboardEditor(options: {
   /** The file as read from storage. */
   source: string;
   /** The document that file produced. */
   document: DashboardDocument;
   /** Persist the patched file. Rejecting leaves the editor dirty. */
   onSave?: (source: string) => Promise<void> | void;
}): DashboardEditor {
   const [history, setHistory] = useState<History>({
      stack: [options.document],
      index: 0,
   });
   const [source, setSource] = useState(options.source);
   const [error, setError] = useState<string | undefined>(undefined);

   // What the file currently holds, so `dirty` compares against the saved state
   // rather than against where the session started.
   //
   // State, not a ref, and the difference is visible: `dirty` is derived from
   // it, and a ref write does not re-render, so a successful save left the
   // editor reporting unsaved changes it had just written.
   const [saved, setSaved] = useState(options.document);

   const document = history.stack[history.index];

   const update = useCallback((change: (draft: DashboardDocument) => void) => {
      setError(undefined);
      setHistory((previous) => {
         const draft = structuredClone(previous.stack[previous.index]);
         change(draft);
         // A change that changes nothing does not deserve a history entry;
         // otherwise a click that sets a value to what it already was makes
         // undo appear broken.
         if (
            JSON.stringify(draft) ===
            JSON.stringify(previous.stack[previous.index])
         )
            return previous;
         // Editing after undo discards the redo tail, which is what every
         // editor does and what a reader expects.
         const kept = previous.stack.slice(0, previous.index + 1);
         const stack = [...kept, draft].slice(-HISTORY_LIMIT);
         return { stack, index: stack.length - 1 };
      });
   }, []);

   const undo = useCallback(() => {
      setError(undefined);
      setHistory((previous) =>
         previous.index > 0
            ? { ...previous, index: previous.index - 1 }
            : previous,
      );
   }, []);

   const redo = useCallback(() => {
      setError(undefined);
      setHistory((previous) =>
         previous.index < previous.stack.length - 1
            ? { ...previous, index: previous.index + 1 }
            : previous,
      );
   }, []);

   const save = useCallback(async (): Promise<SaveOutcome> => {
      const result = await spliceDashboardDocument(source, document);
      if (spliceFailed(result)) {
         setError(result.reason);
         return { ok: false, reason: result.reason };
      }
      try {
         await options.onSave?.(result.source);
      } catch (failure) {
         // The file may or may not have been written; what is certain is that
         // the editor must not pretend it was. Staying dirty is the safe read.
         const reason = `Could not save: ${failure}`;
         setError(reason);
         return { ok: false, reason };
      }
      // The patched file is the new baseline, so a second save patches what is
      // now on disk rather than re-deriving from the text this session opened.
      setSource(result.source);
      setSaved(document);
      setError(undefined);
      return { ok: true };
   }, [document, options, source]);

   const dirty = useMemo(
      () => JSON.stringify(document) !== JSON.stringify(saved),
      [document, saved],
   );
   const structural = useMemo(() => {
      // The FILE's identity for a tile, not the grid's: `document.tileKey` is
      // `source.name`, so a tile redeclared from another view keeps its key
      // and a save that rewrites its declaration would not be reported as
      // structural — the one case where the author most wants the diff.
      const before = new Set(saved.tiles.map(tileFileKey));
      const after = new Set(document.tiles.map(tileFileKey));
      return (
         [...before].some((k) => !after.has(k)) ||
         [...after].some((k) => !before.has(k))
      );
   }, [document, saved]);
   const preview = useCallback(async () => {
      const result = await spliceDashboardDocument(source, document);
      return spliceFailed(result)
         ? { ok: false as const, reason: result.reason }
         : { ok: true as const, source: result.source };
   }, [document, source]);

   return {
      document,
      source,
      canUndo: history.index > 0,
      canRedo: history.index < history.stack.length - 1,
      dirty,
      ...(error === undefined ? {} : { error }),
      update,
      undo,
      redo,
      save,
      preview,
      structural,
   };
}
