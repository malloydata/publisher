// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { now } from "../../utils/clock";
import type { BuilderToolbarProps } from "./BuilderToolbar";
import {
   useBuilderShortcuts,
   type BuilderShortcutHandlers,
} from "./useBuilderShortcuts";
import type { LastSave, SaveHandler, SaveOutcome } from "./useDocumentEditor";
import { useExitGuard } from "./useExitGuard";

/** What a builder's session reads of its document editor. */
export interface SessionEditor<D, L extends LastSave = LastSave> {
   document: D;
   dirty: boolean;
   structural: boolean;
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
   durationMs: number;
}

/** A review to show before the write, or none when the save needs no review; `ok: false` is a refusal the write itself will report. */
export type SessionReview<R> =
   | Promise<{ ok: true; review: R } | { ok: false }>
   | undefined;

export interface BuilderSessionOptions<D, R, L extends LastSave = LastSave> {
   editor: SessionEditor<D, L>;
   onSave?: SaveHandler<D>;
   onExit?: () => void;
   onDirtyChange?: (dirty: boolean) => void;
   /** Whether the last save can still be undone, on every change and on mount. */
   onCanUndoSaveChange?: (canUndoSave: boolean) => void;
   onChange?: (document: D) => void;
   /** Unsaved state the editor does not hold, such as an open text draft. */
   extraDirty?: boolean;
   report: SessionReport;
   /** Builds the review a save shows first; returns nothing when this save needs none. */
   review?: () => SessionReview<R>;
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
export function useBuilderSession<D, R = never, L extends LastSave = LastSave>({
   editor,
   onSave,
   onExit,
   onDirtyChange,
   onCanUndoSaveChange,
   onChange,
   extraDirty = false,
   report,
   review,
   prepare,
   shortcuts,
}: BuilderSessionOptions<D, R, L>) {
   const [saving, setSaving] = useState(false);
   const [pendingSave, setPendingSave] = useState<R | undefined>(undefined);
   const reportRef = useRef(report);
   reportRef.current = report;
   const reviewRef = useRef(review);
   reviewRef.current = review;
   const dirty = editor.dirty || extraDirty;

   useEffect(() => {
      onChange?.(editor.document);
   }, [editor.document, onChange]);
   // Also on mount, so a host that remounted the builder on new text is told the slate is clean rather than carrying the previous mount's answer.
   useEffect(() => {
      onDirtyChange?.(dirty);
   }, [dirty, onDirtyChange]);
   // A host guarding navigation on this must not be left holding a stale "dirty" once the builder is gone.
   const onDirtyChangeRef = useRef(onDirtyChange);
   onDirtyChangeRef.current = onDirtyChange;
   useEffect(() => () => onDirtyChangeRef.current?.(false), []);
   // The offer, not `canUndoSave`, which also drops while a write is in flight; a host holding a newer version back must keep holding then.
   const undoOffered = editor.lastSave !== undefined;
   useEffect(() => {
      onCanUndoSaveChange?.(undoOffered);
   }, [undoOffered, onCanUndoSaveChange]);
   const onCanUndoSaveChangeRef = useRef(onCanUndoSaveChange);
   onCanUndoSaveChangeRef.current = onCanUndoSaveChange;
   useEffect(() => () => onCanUndoSaveChangeRef.current?.(false), []);

   const commitSave = useCallback(() => {
      setPendingSave(undefined);
      setSaving(true);
      const started = now();
      const { structural } = editor;
      const size = reportRef.current.size;
      return editor
         .save()
         .then((outcome) => {
            if (outcome.ok === true)
               reportRef.current.saved({
                  size,
                  structural,
                  durationMs: now() - started,
               });
            else reportRef.current.refused(outcome.reason);
         })
         .finally(() => setSaving(false));
   }, [editor]);

   // A ref as well as `saving`, so a second click before the re-render does not write twice.
   const undoingRef = useRef(false);
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
            if (outcome.ok === true)
               reportRef.current.undone({
                  size,
                  structural,
                  durationMs: now() - started,
               });
            else reportRef.current.undoRefused(outcome.reason);
         })
         .finally(() => {
            undoingRef.current = false;
            setSaving(false);
         });
   }, [onSave, saving, editor]);

   const askingRef = useRef(false);
   const reviewedSave = useCallback((): Promise<void> | void => {
      if (!onSave || !editor.dirty || saving) return;
      const pending = reviewRef.current?.();
      if (!pending) return commitSave();
      return pending.then((result) => {
         if (result.ok) {
            // The exit dialog came up while this was built; a review over it would stack two modals.
            if (!askingRef.current) setPendingSave(result.review);
         }
         // A refusal surfaces through the same path a save's would.
         else return commitSave();
      });
   }, [onSave, editor, saving, commitSave]);
   const reviewedRef = useRef(reviewedSave);
   reviewedRef.current = reviewedSave;
   const save = useCallback((): Promise<void> | void => {
      const run = () => reviewedRef.current();
      return prepare ? prepare(run) : run();
   }, [prepare]);

   const exitGuard = useExitGuard({
      dirty,
      saving,
      reviewing: pendingSave !== undefined,
      canSave: !!onSave,
      save,
      onExit: () => onExit?.(),
   });
   askingRef.current = exitGuard.dialog.open;

   useBuilderShortcuts(
      // One handlers object per change of what they read, so the key listener is not torn down and re-bound on every render.
      useMemo(
         () => ({
            ...shortcuts,
            undo: editor.undo,
            redo: editor.redo,
            ...(onSave ? { save } : {}),
            paused: exitGuard.dialog.open || pendingSave !== undefined,
         }),
         [shortcuts, editor, onSave, save, exitGuard.dialog.open, pendingSave],
      ),
   );

   const toolbarProps: Pick<
      BuilderToolbarProps,
      | "canUndo"
      | "canRedo"
      | "onUndo"
      | "onRedo"
      | "dirty"
      | "saving"
      | "onSave"
      | "onExit"
   > = {
      canUndo: editor.canUndo,
      canRedo: editor.canRedo,
      onUndo: editor.undo,
      onRedo: editor.redo,
      dirty,
      saving,
      ...(onSave ? { onSave: save } : {}),
      ...(onExit ? { onExit: exitGuard.requestExit } : {}),
   };

   return {
      saving,
      save,
      exitGuard,
      /** The review awaiting the author's go-ahead. */
      pendingSave,
      /** Write the reviewed save. */
      confirmSave: commitSave,
      /** Put the review down without saving. */
      dismissReview: () => setPendingSave(undefined),
      /** Write the file back as it was before the last save, and put the edits back unsaved. */
      undoSave,
      canUndoSave: !!onSave && !saving && editor.canUndoSave,
      lastSave: editor.lastSave,
      toolbarProps,
   };
}
