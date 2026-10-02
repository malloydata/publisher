// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useCallback, useMemo, useState } from "react";
import { type SpliceResult, spliceFailed } from "./spliceResult";

/**
 * The editor's state: the document being edited, its history, and saving.
 *
 * Deliberately holds the DOCUMENT and nothing else. Given VALUES — what the
 * preview is running with — are editor state rather than document state and
 * live outside this hook, which is what keeps them out of the undo stack. Undo
 * should take back an edit; it should not rewind the values someone was
 * previewing under.
 *
 * `T` must survive `structuredClone`, and equal documents must `JSON.stringify` equal.
 */

export interface DocumentEditor<T> {
   document: T;
   /** The document as of the last save (or open). */
   saved: T;
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
   update: (change: (draft: T) => void) => void;
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
    * Whether the unsaved change is structural, as the caller defines it (false
    * when it defines nothing): the kind of change worth showing as a diff
    * before it is saved.
    */
   structural: boolean;
   /** Whether saving the unsaved change empties the undo stack, as the caller defines it (false when it defines nothing). */
   clearsHistory: boolean;
}

export type SaveOutcome = { ok: true } | { ok: false; reason: string };

interface History<T> {
   /** Every document state, oldest first. */
   stack: T[];
   /** Which one is current. Undo and redo move this rather than mutating. */
   index: number;
}

/**
 * How many documents of history to keep.
 *
 * A whole document per entry is affordable because the document is small and
 * immutable — it is the parsed structure, not the file. Keeping
 * whole states is also what makes undo trivially correct: there is no inverse
 * operation to get wrong for each kind of edit.
 */
const HISTORY_LIMIT = 100;

export interface DocumentEditorOptions<T> {
   /** The file as read from storage. */
   source: string;
   /** The document that file produced. */
   document: T;
   /** Persist the patched file. Rejecting leaves the editor dirty. */
   onSave?: (source: string) => Promise<void> | void;
   /** Patch the document into the file, or say why that was refused. */
   splice: (source: string, document: T) => Promise<SpliceResult>;
   /** Whether `document` differs structurally from `saved`; omit for never. */
   structural?: (saved: T, document: T) => boolean;
   /** Whether saving `document` over `saved` makes the history unsafe to step back into; the stack is then emptied on save. */
   clearsHistory?: (saved: T, document: T) => boolean;
}

export function useDocumentEditor<T>(
   options: DocumentEditorOptions<T>,
): DocumentEditor<T> {
   const {
      splice,
      onSave,
      structural: isStructural,
      clearsHistory: isClearing,
   } = options;
   const [history, setHistory] = useState<History<T>>({
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

   const update = useCallback((change: (draft: T) => void) => {
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

   // A splice that rejects is the writer failing, not the reader's mistake; it must surface as a refusal rather than an unhandled rejection.
   const safeSplice = useCallback(
      async (from: string, doc: T): Promise<SpliceResult> => {
         try {
            return await splice(from, doc);
         } catch (failure) {
            return {
               ok: false,
               reason: `Could not build the file: ${failure instanceof Error ? failure.message : String(failure)}`,
            };
         }
      },
      [splice],
   );

   const save = useCallback(async (): Promise<SaveOutcome> => {
      const result = await safeSplice(source, document);
      if (spliceFailed(result)) {
         setError(result.reason);
         return { ok: false, reason: result.reason };
      }
      try {
         await onSave?.(result.source);
      } catch (failure) {
         // The file may or may not have been written; what is certain is that
         // the editor must not pretend it was. Staying dirty is the safe read.
         const reason = `Could not save: ${failure instanceof Error ? failure.message : String(failure)}`;
         setError(reason);
         return { ok: false, reason };
      }
      // The patched file is the new baseline, so a second save patches what is
      // now on disk rather than re-deriving from the text this session opened.
      setSource(result.source);
      setSaved(document);
      // Whatever is current when the write resolves survives, including edits typed during it.
      if (isClearing?.(saved, document))
         setHistory((p) => ({ stack: [p.stack[p.index]], index: 0 }));
      setError(undefined);
      return { ok: true };
   }, [document, safeSplice, onSave, source, saved, isClearing]);

   const dirty = useMemo(
      () => JSON.stringify(document) !== JSON.stringify(saved),
      [document, saved],
   );
   const structural = useMemo(
      () => isStructural?.(saved, document) ?? false,
      [document, saved, isStructural],
   );
   const clearsHistory = useMemo(
      () => isClearing?.(saved, document) ?? false,
      [document, saved, isClearing],
   );
   const preview = useCallback(async () => {
      const result = await safeSplice(source, document);
      return spliceFailed(result)
         ? { ok: false as const, reason: result.reason }
         : { ok: true as const, source: result.source };
   }, [document, safeSplice, source]);

   return {
      document,
      saved,
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
      clearsHistory,
   };
}
