// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useCallback, useEffect, useRef, useState } from "react";

/** How far above or below the viewport an element counts as near, in pixels: a screen or so of scroll. */
export const NEAR_VIEWPORT_MARGIN_PX = 600;

const observerUnavailable = () => typeof IntersectionObserver === "undefined";

/**
 * The element that scrolls `node`, or null for the viewport. A host that
 * scrolls its page inside a box (the Console does) clips a tile by that box,
 * so a margin on the viewport would never be reached: the look-ahead has to be
 * measured on the box itself.
 */
function scrollRootOf(node: Element): Element | null {
   for (let at = node.parentElement; at; at = at.parentElement) {
      const { overflowY } = getComputedStyle(at);
      if (
         (overflowY === "auto" || overflowY === "scroll") &&
         at.scrollHeight > at.clientHeight
      )
         return at;
   }
   return null;
}

/**
 * Whether an element has come near the viewport, latched: once true it stays
 * true, so scrolling a tile away again never takes back what it has run.
 *
 * Returns a callback ref for the element and the flag. Where there is no
 * `IntersectionObserver` (server rendering, a test DOM) the flag starts true,
 * so gating on it never leaves something waiting for an observer that will not
 * come.
 *
 * For work that costs: a dashboard tile's query runs against a warehouse that
 * bills for it, and a long dashboard used to run every tile on mount, however
 * far below the fold.
 */
export function useNearViewport<T extends Element = HTMLElement>(
   marginPx: number = NEAR_VIEWPORT_MARGIN_PX,
): [ref: (node: T | null) => void, near: boolean] {
   const [near, setNear] = useState(observerUnavailable);
   const nearRef = useRef(near);
   const observerRef = useRef<IntersectionObserver | null>(null);

   const ref = useCallback(
      (node: T | null) => {
         observerRef.current?.disconnect();
         observerRef.current = null;
         if (!node || nearRef.current || observerUnavailable()) return;
         // Already near when it mounts, as everything above the fold is: open
         // now, in the commit that attached it, rather than a frame later when
         // the observer first reports.
         const rect = node.getBoundingClientRect();
         const root = scrollRootOf(node);
         const bounds = root?.getBoundingClientRect() ?? {
            top: 0,
            bottom: window.innerHeight || document.documentElement.clientHeight,
         };
         if (
            rect.bottom >= bounds.top - marginPx &&
            rect.top <= bounds.bottom + marginPx
         ) {
            nearRef.current = true;
            setNear(true);
            return;
         }
         const observer = new IntersectionObserver(
            (entries) => {
               if (!entries.some((entry) => entry.isIntersecting)) return;
               nearRef.current = true;
               observer.disconnect();
               setNear(true);
            },
            { root, rootMargin: `${marginPx}px 0px` },
         );
         observer.observe(node);
         observerRef.current = observer;
      },
      [marginPx],
   );

   useEffect(() => () => observerRef.current?.disconnect(), []);

   // Printing lays out the whole page at once with nobody scrolling: every
   // tile has to have run by then.
   useEffect(() => {
      if (near || typeof window === "undefined") return;
      const open = () => {
         nearRef.current = true;
         observerRef.current?.disconnect();
         setNear(true);
      };
      window.addEventListener("beforeprint", open);
      return () => window.removeEventListener("beforeprint", open);
   }, [near]);

   return [ref, near];
}
