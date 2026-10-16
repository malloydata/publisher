// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useEffect, useMemo, useRef } from "react";
import type { RestoredDraft } from "./useReportDraft";

/**
 * The builder's page-level controls, as state and actions rather than as a
 * toolbar, so a host can put each one where its own chrome has room: in its
 * header, in its "⋯" menu, or in the SDK's own {@link BuilderBar}. Everything
 * that acts on a tile, a filter or the text stays on the page itself.
 *
 * The actions are stable across renders: a host may keep the object in state
 * and call them later. A new object is handed over only when something a host
 * would draw has changed.
 */
export interface BuilderControls {
   /** Whether the file holds every edit: for a status line. */
   status: "saved" | "unsaved" | "saving";
   /** Where Save writes, in words: "Saves to the package file". */
   saveTarget: string;
   /** Save the edits. Absent when the builder has nowhere to save. */
   save?: () => void;
   canUndo: boolean;
   undo: () => void;
   canRedo: boolean;
   redo: () => void;
   /** What can be added to the page. A missing entry is not offered. */
   add: {
      /** Open the picker of views for a query tile. */
      tile?: () => void;
      /** Append an empty text tile and select it. */
      text?: () => void;
      /** Open the window that declares a filter. */
      filter?: () => void;
      /** Why a filter cannot be added here, when it cannot; `filter` is then absent. */
      filterUnavailable?: string;
   };
   /** Leave the builder: asks first when edits are unsaved. Absent unless the host passed `onExit`. */
   exit?: () => void;
   /**
    * Write the edits, then leave — once the write has landed; a refused write
    * keeps the builder open with its reason. The one way out a host needs when
    * the builder keeps a draft. Absent unless the host passed `onExit`.
    */
   done?: () => void;
   /** When unsaved edits were last kept in this browser (`draftKey`); absent when there are none to keep. */
   draftKeptAt?: number;
   /** Put the report back to the file's, as one undoable edit, and drop the draft. Absent with nothing to discard. */
   discard?: () => void;
   /**
    * Edits kept in this browser that the builder found on opening. Not
    * `pending`: they were put back, which is news for the save status (the
    * SDK's says when they are from in its menu) until `dismissRestored`, or
    * the next edit. `pending`: the report changed since they were kept, so
    * they are offered, not applied; the page asks the author to choose, and
    * `restore` and `discard` are the two answers.
    */
   restored?: RestoredDraft;
   /** Apply an offered (stale) draft, replacing the current version. Present while `restored.pending`. */
   restore?: () => void;
   /** The author has seen the restored edits: `restored` goes. Present while `restored` is, and not pending. */
   dismissRestored?: () => void;
}

/** What a host passes to take over the builder's page-level controls. */
export type BuilderControlsHandler = (
   controls: BuilderControls | undefined,
) => void;

/**
 * Hand the host the builder's controls whenever what they show changes, and
 * `undefined` when the builder goes away. The actions forward to the latest
 * render's, so their identity never changes and a host keeping the object in
 * state cannot loop on it.
 */
export function useReportControls(
   onControls: BuilderControlsHandler | undefined,
   controls: BuilderControls,
): void {
   const latest = useRef(controls);
   latest.current = controls;
   const handlerRef = useRef(onControls);
   handlerRef.current = onControls;

   // Stable forwarders, made once.
   const actions = useMemo(
      () => ({
         save: () => latest.current.save?.(),
         undo: () => latest.current.undo(),
         redo: () => latest.current.redo(),
         addTile: () => latest.current.add.tile?.(),
         addText: () => latest.current.add.text?.(),
         addFilter: () => latest.current.add.filter?.(),
         exit: () => latest.current.exit?.(),
         done: () => latest.current.done?.(),
         discard: () => latest.current.discard?.(),
         restore: () => latest.current.restore?.(),
         dismissRestored: () => latest.current.dismissRestored?.(),
      }),
      [],
   );

   const {
      status,
      saveTarget,
      canUndo,
      canRedo,
      add: { filterUnavailable },
   } = controls;
   const hasSave = controls.save !== undefined;
   const hasTile = controls.add.tile !== undefined;
   const hasText = controls.add.text !== undefined;
   const hasFilter = controls.add.filter !== undefined;
   const hasExit = controls.exit !== undefined;
   const hasDone = controls.done !== undefined;
   const hasDiscard = controls.discard !== undefined;
   const { draftKeptAt } = controls;
   const restoredAt = controls.restored?.savedAt;
   const restoredStale = controls.restored?.stale ?? false;
   const restoredPending = controls.restored?.pending ?? false;
   const hasRestore = controls.restore !== undefined;
   const hasDismissRestored = controls.dismissRestored !== undefined;

   const shown = useMemo<BuilderControls>(
      () => ({
         status,
         saveTarget,
         ...(hasSave ? { save: actions.save } : {}),
         canUndo,
         undo: actions.undo,
         canRedo,
         redo: actions.redo,
         add: {
            ...(hasTile ? { tile: actions.addTile } : {}),
            ...(hasText ? { text: actions.addText } : {}),
            ...(hasFilter ? { filter: actions.addFilter } : {}),
            ...(filterUnavailable !== undefined ? { filterUnavailable } : {}),
         },
         ...(hasExit ? { exit: actions.exit } : {}),
         ...(hasDone ? { done: actions.done } : {}),
         ...(draftKeptAt !== undefined ? { draftKeptAt } : {}),
         ...(hasDiscard ? { discard: actions.discard } : {}),
         ...(restoredAt !== undefined
            ? {
                 restored: {
                    savedAt: restoredAt,
                    stale: restoredStale,
                    pending: restoredPending,
                 },
              }
            : {}),
         ...(hasRestore ? { restore: actions.restore } : {}),
         ...(hasDismissRestored
            ? { dismissRestored: actions.dismissRestored }
            : {}),
      }),
      [
         actions,
         status,
         saveTarget,
         hasSave,
         canUndo,
         canRedo,
         hasTile,
         hasText,
         hasFilter,
         filterUnavailable,
         hasExit,
         hasDone,
         draftKeptAt,
         hasDiscard,
         restoredAt,
         restoredStale,
         restoredPending,
         hasRestore,
         hasDismissRestored,
      ],
   );

   // Also when a handler arrives after the builder has mounted: it takes the
   // inline toolbar away at once, so it has to be handed the controls at once
   // too, not on the next change.
   const hasHandler = onControls !== undefined;
   useEffect(() => {
      handlerRef.current?.(shown);
   }, [shown, hasHandler]);
   // Gone with the builder, so a host does not draw controls for nothing.
   useEffect(() => () => handlerRef.current?.(undefined), []);
}
