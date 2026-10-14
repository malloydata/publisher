// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useCallback, useRef, useState } from "react";

/**
 * How far above the scroller's edge a pinned bar sits: one pixel, so the bar
 * is clipped by exactly that pixel while pinned and whole while it is not,
 * which is what {@link useStuck} observes.
 */
export const STUCK_TOP_PX = -1;

/**
 * Whether a `position: sticky` bar (pinned at `top: {@link STUCK_TOP_PX}px`)
 * is currently pinned, so it can draw an edge only while content scrolls under
 * it. Unpinned, the bar sits in the flow and an edge would be a stray line
 * between it and the first tile.
 *
 * Observes the bar itself: pinned, its top pixel is clipped by the scroller,
 * so less than all of it is visible. No sentinel element, which as a sibling
 * in a `gap` layout cost the page a gap of its own above the bar. No explicit
 * root, so it works whichever ancestor scrolls: an observer with the implicit
 * root counts clipping by every scrolling ancestor, not just the viewport.
 */
export function useStuck(): {
   stickyRef: (node: HTMLElement | null) => void;
   stuck: boolean;
} {
   const [stuck, setStuck] = useState(false);
   const observer = useRef<IntersectionObserver | null>(null);
   const stickyRef = useCallback((node: HTMLElement | null) => {
      observer.current?.disconnect();
      observer.current = null;
      if (!node || typeof IntersectionObserver === "undefined") return;
      observer.current = new IntersectionObserver(
         ([entry]) => {
            if (!entry) return;
            // Partly hidden BELOW the fold is not pinned: only clipped at the top.
            const atTop =
               entry.boundingClientRect.top <
               (entry.rootBounds?.height ?? window.innerHeight) / 2;
            setStuck(entry.intersectionRatio < 1 && atTop);
         },
         { threshold: [1] },
      );
      observer.current.observe(node);
   }, []);
   return { stickyRef, stuck };
}
