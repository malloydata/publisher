// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useCallback, useRef, useState } from "react";

/**
 * Whether a `position: sticky` bar is currently pinned, so it can draw an edge
 * only while content scrolls under it. Unpinned, the bar sits in the flow and
 * an edge would be a stray line between it and the first tile.
 *
 * Watches a zero-height sentinel placed directly before the bar: once the
 * sentinel has scrolled out through the top, the bar is pinned. No explicit
 * root, so it works whichever ancestor scrolls — an observer with the implicit
 * root counts clipping by every scrolling ancestor, not just the viewport.
 */
export function useStuck(): {
   sentinelRef: (node: HTMLElement | null) => void;
   stuck: boolean;
} {
   const [stuck, setStuck] = useState(false);
   const observer = useRef<IntersectionObserver | null>(null);
   const sentinelRef = useCallback((node: HTMLElement | null) => {
      observer.current?.disconnect();
      observer.current = null;
      if (!node || typeof IntersectionObserver === "undefined") return;
      observer.current = new IntersectionObserver(([entry]) => {
         if (!entry) return;
         // Out of view BELOW the fold is not pinned: only out through the top.
         setStuck(
            !entry.isIntersecting &&
               entry.boundingClientRect.top <
                  (entry.rootBounds?.height ?? window.innerHeight) / 2,
         );
      });
      observer.current.observe(node);
   }, []);
   return { sentinelRef, stuck };
}
