// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useCallback, useEffect, useRef, useState } from "react";

/**
 * Leaving a builder without losing work: "Done editing" exits at once when
 * nothing is unsaved, and otherwise asks.
 *
 * "Save and exit" exits only after a save this request started has settled
 * with nothing left dirty, so a refused or failed save, or a review dialog
 * dismissed with "Keep editing", leaves the builder open with its edits.
 */
export interface ExitGuardOptions {
   dirty: boolean;
   saving: boolean;
   /** A review dialog stands between the request and the write. */
   reviewing: boolean;
   canSave: boolean;
   /** Resolves once the save has run or been handed to the review dialog. */
   save: () => Promise<void> | void;
   onExit: () => void;
}

type Phase = "idle" | "asking" | "waiting" | "saving";

export function useExitGuard({
   dirty,
   saving,
   reviewing,
   canSave,
   save,
   onExit,
}: ExitGuardOptions) {
   const [phase, setPhase] = useState<Phase>("idle");
   // `save` has returned, so a quiet builder after it means the save is over, not that it has yet to start.
   const [issued, setIssued] = useState(false);
   const saveRef = useRef(save);
   saveRef.current = save;
   const onExitRef = useRef(onExit);
   onExitRef.current = onExit;
   const attempt = useRef(0);

   const issue = useCallback(async () => {
      const mine = ++attempt.current;
      setIssued(false);
      setPhase("saving");
      try {
         await saveRef.current();
      } catch {
         if (attempt.current === mine) setPhase("idle");
         return;
      }
      if (attempt.current === mine) setIssued(true);
   }, []);

   useEffect(() => {
      if (phase === "waiting") {
         if (saving) return;
         if (dirty) void issue();
         else {
            setPhase("idle");
            onExitRef.current();
         }
      } else if (phase === "saving") {
         if (!issued || saving || reviewing) return;
         setPhase("idle");
         if (!dirty) onExitRef.current();
      }
   }, [phase, issued, saving, reviewing, dirty, issue]);

   const requestExit = useCallback(() => {
      if (phase !== "idle") return;
      if (dirty) setPhase("asking");
      else onExitRef.current();
   }, [phase, dirty]);

   return {
      requestExit,
      dialog: {
         open: phase === "asking",
         canSave,
         onKeepEditing: () => {
            attempt.current++;
            setPhase("idle");
         },
         onDiscard: () => {
            setPhase("idle");
            onExitRef.current();
         },
         onSaveAndExit: () => {
            if (saving) setPhase("waiting");
            else void issue();
         },
      },
   };
}
