// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { UnsavedChangesDialog } from "@malloy-publisher/sdk";
import { useCallback, useEffect, useRef, useState } from "react";
import { useBlocker } from "react-router-dom";

/**
 * Guards an editor page against leaving with unsaved edits, by a link, Back or
 * closing the tab. The editor's own Done prompts for itself, so `onExit` calls
 * `leaving()` first and its navigation passes unasked.
 */
export function useLeaveGuard() {
   const [dirty, setDirty] = useState(false);
   const dirtyRef = useRef(false);
   const leavingRef = useRef(false);
   const onDirtyChange = useCallback((next: boolean) => {
      dirtyRef.current = next;
      setDirty(next);
   }, []);
   const leaving = useCallback(() => {
      leavingRef.current = true;
   }, []);

   const blocker = useBlocker(
      useCallback(
         ({
            currentLocation,
            nextLocation,
         }: {
            currentLocation: { pathname: string };
            nextLocation: { pathname: string };
         }) =>
            dirtyRef.current &&
            !leavingRef.current &&
            currentLocation.pathname !== nextLocation.pathname,
         [],
      ),
   );

   useEffect(() => {
      if (!dirty) return;
      const warn = (event: BeforeUnloadEvent) => {
         event.preventDefault();
         // Some browsers only prompt when returnValue is set.
         event.returnValue = "";
      };
      window.addEventListener("beforeunload", warn);
      return () => window.removeEventListener("beforeunload", warn);
   }, [dirty]);

   const dialog = (
      <UnsavedChangesDialog
         open={blocker.state === "blocked"}
         canSave={false}
         onKeepEditing={() => blocker.reset?.()}
         onDiscard={() => blocker.proceed?.()}
      />
   );
   return { onDirtyChange, leaving, dialog };
}
