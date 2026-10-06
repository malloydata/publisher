// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   useCallback,
   useEffect,
   useLayoutEffect,
   useMemo,
   useRef,
   useState,
} from "react";
import { now } from "../../utils/clock";
import type { BuilderToolbarProps } from "./BuilderToolbar";
import {
   useBuilderShortcuts,
   type BuilderShortcutHandlers,
} from "./useBuilderShortcuts";
import type { LastSave, SaveHandler, SaveOutcome } from "./useDocumentEditor";

/** What a builder's session reads of its document editor. */
export interface SessionEditor<D, L extends LastSave = LastSave> {
   document: D;
   /** The document as of the last save (or open). */
   saved: D;
   dirty: boolean;
   /** Unsaved edits made by a person, when that differs from `dirty` (an open that is unsaved before any edit); absent means `dirty`. */
   edited?: boolean;
   structural: boolean;
   /** Whether the document opened unsaved (a conversion); the save that clears it reports so. */
   pendingOpen?: boolean;
   canUndo: boolean;
   canRedo: boolean;
   undo: () => void;
   redo: () => void;
   save: () => Promise<SaveOutcome>;
   canUndoSave: boolean;
   lastSave?: L;
   undoSave: () => Promise<SaveOutcome>;
}

/** What a builder says about a save, in its own event's words. */
export interface SessionReport {
   /** How many tiles or cells the document has, as the event counts them. */
   size: number;
   saved: (info: SessionSaveInfo) => void;
   refused: (reason: string) => void;
   /** An Undo save that wrote the file back. */
   undone: (info: SessionSaveInfo) => void;
   undoRefused: (reason: string) => void;
}

export interface SessionSaveInfo {
   size: number;
   structural: boolean;
   /** The save wrote a document that opened unsaved. */
   fromOpen: boolean;
   durationMs: number;
}

export interface BuilderSessionOptions<D, L extends LastSave = LastSave> {
   editor: SessionEditor<D, L>;
   onSave?: SaveHandler<D>;
   onDirtyChange?: (dirty: boolean) => void;
   onChange?: (document: D) => void;
   /** Unsaved state the editor does not hold, such as an open text draft. */
   extraDirty?: boolean;
   report: SessionReport;
   /** What the notice counts and calls them: "tile" or "cell". */
   unit: { name: string; count: (document: D) => number };
   /** Runs before every save entry point, handing it the save to run once the document is ready. */
   prepare?: (run: () => Promise<void> | void) => Promise<void> | void;
   /** The builder's own keys, less undo, redo and save, which the session owns. Memoized by the caller. */
   shortcuts: Pick<BuilderShortcutHandlers, "escape" | "nudge">;
}

/**
 * What the dashboard and notebook builders share around their document editor:
 * saving (with its event and timing), the dirty and change reports, the exit
 * guard, the keyboard, and the props their toolbar takes.
 */
export function useBuilderSession<
   D,
   L extends LastSave & { removedComments?: string[] } = LastSave,
>({
   editor,
   onSave,
   onDirtyChange,
   onChange,
   extraDirty = false,
   report,
   unit,
   prepare,
   shortcuts,
}: BuilderSessionOptions<D, L>) {
   const [saving, setSaving] = useState(false);
   const [undone, setUndone] = useState(false);
   const saveButton = useRef<HTMLButtonElement>(null);
   const reportRef = useRef(report);
   reportRef.current = report;
   const dirty = editor.dirty || extraDirty;
   // Work to lose: a conversion nobody has touched is rebuilt by opening the file again.
   const hasEdits = (editor.edited ?? editor.dirty) || extraDirty;

   useEffect(() => {
      onChange?.(editor.document);
   }, [editor.document, onChange]);
   // Also on mount, so a host that remounted the builder on new text is told the slate is clean rather than carrying the previous mount's answer.
   // A layout effect, so a host's leave guard sees the flag in the same task as the edit.
   useLayoutEffect(() => {
      onDirtyChange?.(hasEdits);
   }, [hasEdits, onDirtyChange]);
   // A host guarding navigation on this must not be left holding a stale "dirty" once the builder is gone.
   const onDirtyChangeRef = useRef(onDirtyChange);
   onDirtyChangeRef.current = onDirtyChange;
   useEffect(() => () => onDirtyChangeRef.current?.(false), []);
   const unitRef = useRef(unit);
   unitRef.current = unit;
   // The line saying the undo happened goes with the next edit or save; leaving it up over new work would say something stale.
   useEffect(() => {
      setUndone(false);
   }, [editor.document]);

   const commitSave = useCallback(() => {
      setSaving(true);
      setUndone(false);
      const started = now();
      const { structural } = editor;
      const fromOpen = editor.pendingOpen ?? false;
      const size = reportRef.current.size;
      return editor
         .save()
         .then((outcome) => {
            if (outcome.ok === true) {
               reportRef.current.saved({
                  size,
                  structural,
                  fromOpen,
                  durationMs: now() - started,
               });
            } else reportRef.current.refused(outcome.reason);
         })
         .finally(() => setSaving(false));
   }, [editor]);

   // A ref as well as `saving`, so a second click before the re-render does not write twice.
   const undoingRef = useRef(false);
   const focusSave = useRef(false);
   // The notice that held focus is gone, so the key that follows is the next save; Save is enabled again once the write settles.
   useEffect(() => {
      if (!focusSave.current || saving) return;
      focusSave.current = false;
      saveButton.current?.focus();
   }, [undone, saving]);
   const undoSave = useCallback((): Promise<void> | void => {
      if (!onSave || saving || undoingRef.current || !editor.canUndoSave)
         return;
      undoingRef.current = true;
      setSaving(true);
      const started = now();
      const structural = editor.lastSave?.structural ?? false;
      const size = reportRef.current.size;
      return editor
         .undoSave()
         .then((outcome) => {
            if (outcome.ok === true) {
               focusSave.current = true;
               setUndone(true);
               reportRef.current.undone({
                  size,
                  structural,
                  fromOpen: false,
                  durationMs: now() - started,
               });
            } else reportRef.current.undoRefused(outcome.reason);
         })
         .finally(() => {
            undoingRef.current = false;
            setSaving(false);
         });
   }, [onSave, saving, editor]);

   const guardedSave = useCallback((): Promise<void> | void => {
      if (!onSave || !editor.dirty || saving) return;
      return commitSave();
   }, [onSave, editor, saving, commitSave]);
   const guardedRef = useRef(guardedSave);
   guardedRef.current = guardedSave;
   const save = useCallback((): Promise<void> | void => {
      const run = () => guardedRef.current();
      return prepare ? prepare(run) : run();
   }, [prepare]);

   useBuilderShortcuts(
      // One handlers object per change of what they read, so the key listener is not torn down and re-bound on every render.
      useMemo(
         () => ({
            ...shortcuts,
            undo: editor.undo,
            redo: editor.redo,
            ...(onSave ? { save } : {}),
         }),
         [shortcuts, editor, onSave, save],
      ),
   );

   const canUndoSave = !!onSave && !saving && editor.canUndoSave;
   const lastSave = editor.lastSave;

   const toolbarProps: Pick<
      BuilderToolbarProps,
      | "canUndo"
      | "canRedo"
      | "onUndo"
      | "onRedo"
      | "dirty"
      | "saving"
      | "onSave"
      | "saveButton"
   > = {
      canUndo: editor.canUndo,
      canRedo: editor.canRedo,
      onUndo: editor.undo,
      onRedo: editor.redo,
      dirty,
      saving,
      saveButton,
      ...(onSave ? { onSave: save } : {}),
   };

   return {
      saving,
      save,
      /** Write the file back as it was before the last save, and put the edits back unsaved. */
      undoSave,
      canUndoSave,
      lastSave,
      toolbarProps,
   };
}
