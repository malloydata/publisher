// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useEffect, useState } from "react";

/**
 * A copy of `value` to edit while a window is open, committed ONCE when it
 * closes, and only if something changed.
 *
 * Per-keystroke commits would put a document in the undo stack for every
 * letter typed into a title, and undo would then walk back through the word.
 */
export function useDraft<T>(
   value: T | undefined,
   open: boolean,
   onCommit: (next: T) => void,
   onClose: () => void,
) {
   const [draft, setDraft] = useState<T | undefined>(undefined);
   useEffect(() => {
      if (open && value !== undefined) setDraft(structuredClone(value));
   }, [open, value]);

   const close = () => {
      if (
         draft !== undefined &&
         value !== undefined &&
         JSON.stringify(draft) !== JSON.stringify(value)
      )
         onCommit(draft);
      onClose();
   };
   const patch = (change: (draft: T) => void) =>
      setDraft((previous) => {
         if (previous === undefined) return previous;
         const next = structuredClone(previous);
         change(next);
         return next;
      });
   /** Drop the draft without committing: for an action that supersedes it. */
   const discard = () => setDraft(undefined);
   return { draft, patch, close, discard };
}
