// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useNavigate } from "react-router-dom";
import { MouseEvent } from "react";

/**
 * The modifier state a navigation decision reads, and nothing more.
 *
 * Both React's synthetic mouse event and the DOM's satisfy it, which is the
 * point. A click can reach navigation from a React handler, or from outside
 * React entirely (a `# drill` click arrives from the Malloy renderer as a plain
 * DOM event), and neither should have to be converted to the other first.
 */
export type NavigationClick = Pick<
   MouseEvent,
   "metaKey" | "ctrlKey" | "shiftKey" | "button"
>;

/**
 * Custom hook that returns a function for handling clicks with proper modifier key support.
 * The returned function handles CMD/Ctrl clicks to open in new tabs, regular clicks for navigation.
 *
 * @returns A function that takes a relative URL and either navigates or opens in new tab
 */
export function useRouterClickHandler() {
   const navigate = useNavigate();

   return (to: string, event?: NavigationClick) => {
      // If no event or no modifier keys, use normal navigation
      if (
         !event ||
         (!event.metaKey &&
            !event.ctrlKey &&
            !event.shiftKey &&
            event.button !== 1)
      ) {
         navigate(to);
         return;
      }

      // For modifier keys or middle mouse, let browser handle it by opening URL
      const href = new URL(to, window.location.href).href;

      if (event.metaKey || event.ctrlKey || event.button === 1) {
         // CMD/Ctrl click or middle mouse - open in new tab
         window.open(href, "_blank");
      } else if (event.shiftKey) {
         // Shift click - open in new window
         window.open(href, "_blank");
      }
   };
}
