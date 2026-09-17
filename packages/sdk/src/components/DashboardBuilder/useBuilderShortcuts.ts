// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useEffect, useRef } from "react";

/**
 * The keyboard an editor is expected to have.
 *
 * Undo and redo on the platform's usual keys, save on ⌘S / Ctrl+S (which the
 * browser would otherwise take), Escape to drop the selection, and the arrow
 * keys to nudge the selected tile's width a column at a time — the one layout
 * edit that is otherwise drag-only.
 *
 * Nothing but Escape fires while a text field has focus, so typing into a
 * title never undoes a layout.
 */
export interface BuilderShortcutHandlers {
   undo: () => void;
   redo: () => void;
   save?: () => void;
   escape: () => void;
   /** Nudge the selected tile's width by ±1 column. */
   nudge: (delta: 1 | -1) => void;
}

const isMac =
   typeof navigator !== "undefined" &&
   /Mac|iPhone|iPad/.test(navigator.platform);

/** The modifier's glyph for a tooltip. */
export const MOD = isMac ? "⌘" : "Ctrl+";

const inTextEntry = (target: EventTarget | null) => {
   if (!(target instanceof HTMLElement)) return false;
   if (target.isContentEditable) return true;
   const tag = target.tagName;
   return tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT";
};

export function useBuilderShortcuts(handlers: BuilderShortcutHandlers) {
   // The handlers are read at keypress time, not bound at subscription time:
   // the caller passes a fresh object every render, and a listener re-bound on
   // each render would be churn that buys nothing.
   const latest = useRef(handlers);
   useEffect(() => {
      latest.current = handlers;
   });

   useEffect(() => {
      const onKey = (event: KeyboardEvent) => {
         const current = latest.current;
         const mod = isMac ? event.metaKey : event.ctrlKey;
         const key = event.key.toLowerCase();
         if (event.key === "Escape") {
            current.escape();
            return;
         }
         if (inTextEntry(event.target)) return;
         if (mod && key === "z") {
            event.preventDefault();
            if (event.shiftKey) current.redo();
            else current.undo();
            return;
         }
         if (!isMac && mod && key === "y") {
            event.preventDefault();
            current.redo();
            return;
         }
         if (mod && key === "s") {
            event.preventDefault();
            current.save?.();
            return;
         }
         if (
            !mod &&
            !event.altKey &&
            !event.shiftKey &&
            (event.key === "ArrowLeft" || event.key === "ArrowRight")
         ) {
            event.preventDefault();
            current.nudge(event.key === "ArrowRight" ? 1 : -1);
         }
      };
      window.addEventListener("keydown", onKey);
      return () => window.removeEventListener("keydown", onKey);
   }, []);
}
