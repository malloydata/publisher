// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useEffect, useRef, type RefObject } from "react";

/**
 * A query tile's card, in the reader and the builder alike (`TileCard`). Not
 * every card: the description and text tiles are cards too, hold no result,
 * and counting them waited out the whole timeout on every switch.
 */
const TILE_CARD = '[data-tile-kind="query"]';
/** A query tile's result, or what it shows in place of one. */
const DRAWN = ".malloy-render, .malloy-table, [role='alert'], .MuiAlert-root";
/** The longest a swap waits on the incoming page before it is shown anyway. */
const READY_TIMEOUT_MS = 3000;

/**
 * Calls `onReady` once, when the page in `root` is open (`open`) and every
 * tile card in view shows something: its result, or an error. Tiles below the
 * fold do not run until scrolled to, so they are not waited on. For a host
 * swapping one page for another, so the swap happens once the new one is drawn.
 */
export function useDrawn(
   root: RefObject<HTMLElement | null>,
   open: boolean,
   onReady: (() => void) | undefined,
) {
   const onReadyRef = useRef(onReady);
   onReadyRef.current = onReady;
   const fired = useRef(false);
   useEffect(() => {
      if (!onReadyRef.current || fired.current) return;
      const fire = () => {
         if (fired.current) return;
         fired.current = true;
         onReadyRef.current?.();
      };
      const timer = setTimeout(fire, READY_TIMEOUT_MS);
      if (!open) return () => clearTimeout(timer);
      let frame = 0;
      const check = () => {
         const el = root.current;
         if (!el) return;
         const fold = window.innerHeight;
         const waiting = [
            ...el.querySelectorAll<HTMLElement>(TILE_CARD),
         ].filter((tile) => {
            const box = tile.getBoundingClientRect();
            if (box.top >= fold) return false;
            const drawn = tile.querySelector<HTMLElement>(DRAWN);
            return !drawn || drawn.getBoundingClientRect().height < 8;
         });
         // Two frames after the last one arrives, so it has painted.
         if (waiting.length === 0 && frame === 0)
            frame = requestAnimationFrame(() => {
               frame = requestAnimationFrame(fire);
            });
      };
      // A result finishes drawing inside the renderer's own tree, which a
      // mutation observer here never sees: its height simply grows. So the
      // check also runs on a short beat; watching mutations alone waited out
      // the whole timeout on every switch.
      const observer = new MutationObserver(check);
      if (root.current)
         observer.observe(root.current, { childList: true, subtree: true });
      const beat = setInterval(check, 50);
      check();
      return () => {
         clearTimeout(timer);
         clearInterval(beat);
         cancelAnimationFrame(frame);
         observer.disconnect();
      };
   }, [open, root]);
}
