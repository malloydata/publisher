// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useCallback, useEffect, useRef, useState } from "react";

export interface ExitGuardOptions {
   dirty: boolean;
   saving: boolean;
   canSave: boolean;
   /** Resolves once the save has run. */
   save: () => Promise<void> | void;
   onExit: () => void;
}

type Phase = "idle" | "asking" | "waiting" | "saving";

/** "Done editing" exits at once when clean and asks otherwise; "Save and exit" exits only once its own save settles clean. */
export function useExitGuard({
   dirty,
   saving,
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
         if (!issued || saving) return;
         setPhase("idle");
         if (!dirty) onExitRef.current();
      }
   }, [phase, issued, saving, dirty, issue]);

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
            if (phase !== "asking") return;
            setPhase("idle");
         },
         onDiscard: () => {
            if (phase !== "asking") return;
            setPhase("idle");
            onExitRef.current();
         },
         onSaveAndExit: () => {
            if (phase !== "asking") return;
            if (saving) setPhase("waiting");
            else void issue();
         },
      },
   };
}
