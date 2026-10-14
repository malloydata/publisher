// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useEffect, useMemo, useRef } from "react";
import type { ReportLayout } from "../Package/documentLocation";

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
   /** The layout and its switch. Absent when the layout cannot be changed here. */
   layout?: {
      value: ReportLayout;
      set: (layout: ReportLayout) => void;
   };
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
         setLayout: (layout: ReportLayout) =>
            latest.current.layout?.set(layout),
         addTile: () => latest.current.add.tile?.(),
         addText: () => latest.current.add.text?.(),
         addFilter: () => latest.current.add.filter?.(),
         exit: () => latest.current.exit?.(),
         done: () => latest.current.done?.(),
         discard: () => latest.current.discard?.(),
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
   const layout = controls.layout?.value;
   const hasTile = controls.add.tile !== undefined;
   const hasText = controls.add.text !== undefined;
   const hasFilter = controls.add.filter !== undefined;
   const hasExit = controls.exit !== undefined;
   const hasDone = controls.done !== undefined;
   const hasDiscard = controls.discard !== undefined;
   const { draftKeptAt } = controls;

   const shown = useMemo<BuilderControls>(
      () => ({
         status,
         saveTarget,
         ...(hasSave ? { save: actions.save } : {}),
         canUndo,
         undo: actions.undo,
         canRedo,
         redo: actions.redo,
         ...(layout !== undefined
            ? { layout: { value: layout, set: actions.setLayout } }
            : {}),
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
      }),
      [
         actions,
         status,
         saveTarget,
         hasSave,
         canUndo,
         canRedo,
         layout,
         hasTile,
         hasText,
         hasFilter,
         filterUnavailable,
         hasExit,
         hasDone,
         draftKeptAt,
         hasDiscard,
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
