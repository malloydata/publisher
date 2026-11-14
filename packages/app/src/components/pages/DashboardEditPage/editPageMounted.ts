// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useEffect, useSyncExternalStore } from "react";

/**
 * Whether the edit page is on screen, for the header: the edit page draws the
 * builder's bar (Done included) in the header's slot, so the header's own
 * View is redundant while it is mounted and the only way back while it is not
 * (an `?edit` route still listing, or one whose listing failed).
 */
let mounted = 0;
const listeners = new Set<() => void>();
const notify = () => listeners.forEach((listener) => listener());

/** Mark the edit page as on screen for as long as the caller is mounted. */
export function useMarkEditPageMounted(): void {
   useEffect(() => {
      mounted++;
      notify();
      return () => {
         mounted--;
         notify();
      };
   }, []);
}

/** Whether an edit page is on screen. */
export function useEditPageMounted(): boolean {
   return useSyncExternalStore(
      (listener) => {
         listeners.add(listener);
         return () => listeners.delete(listener);
      },
      () => mounted > 0,
      () => false,
   );
}
