// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { UnsavedChangesDialog } from "@malloy-publisher/sdk";
import { useCallback, useEffect, useRef, useState } from "react";
import { useBlocker } from "react-router-dom";
import { isEditSearch } from "../common/documentRoutes";

/**
 * Guards an editor page against leaving with unsaved edits, by a link, Back,
 * the header's View or closing the tab. The builder draws no way out of
 * itself, so this is the one guard. `leaving()` lets a navigation the page
 * has already confirmed pass unasked.
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
            currentLocation: { pathname: string; search: string };
            nextLocation: { pathname: string; search: string };
         }) =>
            dirtyRef.current &&
            !leavingRef.current &&
            // Another page, or the same report without `?edit` (View): either
            // leaves the builder. A query change that keeps `?edit` stays.
            (currentLocation.pathname !== nextLocation.pathname ||
               isEditSearch(currentLocation.search) !==
                  isEditSearch(nextLocation.search)),
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
