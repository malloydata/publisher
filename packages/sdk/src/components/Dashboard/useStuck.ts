// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useCallback, useRef, useState } from "react";

/**
 * How far above its pinning line a pinned bar sits: one pixel, so the bar's
 * edge meets whatever is pinned above it (the scroller's edge, or a host's own
 * pinned bar) with no hairline of content showing between them.
 */
export const STUCK_TOP_PX = -1;

/** The nearest ancestor that scrolls, or undefined when the page itself does. */
function scrollerOf(node: HTMLElement): HTMLElement | undefined {
   for (let at = node.parentElement; at; at = at.parentElement) {
      if (/(auto|scroll|overlay)/.test(getComputedStyle(at).overflowY))
         return at;
   }
   return undefined;
}

/**
 * Whether a `position: sticky` bar is pinned now: the content under it has
 * scrolled, and the bar sits at its pinning line (its computed `top`, below the
 * scroller's top edge) rather than lower down in the flow.
 */
export function isPinned(
   node: HTMLElement,
   scroller: HTMLElement | undefined,
): boolean {
   const scrolled = scroller ? scroller.scrollTop : window.scrollY;
   if (scrolled <= 0) return false;
   const top = parseFloat(getComputedStyle(node).top);
   if (Number.isNaN(top)) return false;
   const edge = scroller
      ? scroller.getBoundingClientRect().top + scroller.clientTop
      : 0;
   return node.getBoundingClientRect().top - edge <= top + 0.5;
}

/**
 * Whether a `position: sticky` bar is currently pinned, so it can draw a fill
 * and an edge only while content scrolls under it. Unpinned, the bar sits in
 * the flow and an edge would be a stray line between it and the first tile.
 *
 * Measured on scroll, against the bar's own pinning line. It used to watch for
 * the bar's top pixel being clipped by the scroller, which holds only for a bar
 * pinned at the scroller's edge: a host that pins its own bar above the report
 * (`--publisher-sticky-top`) pins this one lower, where nothing clips it. The
 * bar then never counted as pinned, stayed transparent, and its controls were
 * drawn over the tile titles scrolling under it.
 */
export function useStuck(): {
   stickyRef: (node: HTMLElement | null) => void;
   stuck: boolean;
} {
   const [stuck, setStuck] = useState(false);
   const detach = useRef<(() => void) | null>(null);
   const stickyRef = useCallback((node: HTMLElement | null) => {
      detach.current?.();
      detach.current = null;
      if (!node || typeof window === "undefined") return;
      const scroller = scrollerOf(node);
      const target: HTMLElement | Window = scroller ?? window;
      let frame = 0;
      const measure = () => {
         frame = 0;
         setStuck(isPinned(node, scroller));
      };
      // One measurement a frame, however many scroll events land in it.
      const schedule = () => {
         if (frame === 0) frame = requestAnimationFrame(measure);
      };
      target.addEventListener("scroll", schedule, { passive: true });
      window.addEventListener("resize", schedule);
      measure();
      detach.current = () => {
         target.removeEventListener("scroll", schedule);
         window.removeEventListener("resize", schedule);
         if (frame !== 0) cancelAnimationFrame(frame);
      };
   }, []);
   return { stickyRef, stuck };
}
