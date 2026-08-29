// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A wide table inside the Malloy render scrolls horizontally, and its scrollbar
 * is laid out *inside* the element's box (border-box is global here).
 * `scrollHeight` excludes that scrollbar, so sizing the container to
 * scrollHeight alone clips the final row behind it. Walk the subtree for the
 * element that overflows horizontally and return the vertical space its
 * scrollbar occupies. Returns 0 for overlay scrollbars (macOS), which consume no
 * layout, so there is nothing to add.
 *
 * Split out of renderer.ts to be testable without loading @malloydata/render.
 */

/**
 * The measurements this needs from a node. Structural rather than `HTMLElement`
 * so the arithmetic can be tested against recorded browser values.
 *
 * Every field is optional because the DOM does not guarantee them: an SVG
 * element has no `offsetHeight` or `clientHeight` at all, which is the whole
 * reason this module exists.
 */
export interface MeasurableNode {
   scrollWidth?: number;
   clientWidth?: number;
   offsetHeight?: number;
   clientHeight?: number;
}

export function scrollbarAllowanceFrom(
   nodes: readonly MeasurableNode[],
): number {
   let allowance = 0;
   for (const node of nodes) {
      const scrollWidth = node.scrollWidth;
      const clientWidth = node.clientWidth;
      if (
         scrollWidth === undefined ||
         clientWidth === undefined ||
         !(scrollWidth > clientWidth + 1)
      ) {
         continue;
      }
      const diff =
         (node.offsetHeight as number) - (node.clientHeight as number);
      // The guard is load-bearing, not defensive tidying. A chart subtree
      // contains SVG elements, which report a scrollWidth and clientWidth but no
      // offsetHeight, so this subtraction was NaN. `Math.max` propagates NaN, so
      // one SVG node poisoned the whole walk: the caller set the container height
      // to the invalid value "NaNpx", the browser dropped the assignment
      // silently, the container kept its initial 400px, and the observer never
      // reached its disconnect so it re-measured forever. A dashboard whose
      // content was 1900px rendered clipped to 400px because of it.
      if (!Number.isFinite(diff)) continue;
      allowance = Math.max(allowance, diff);
   }
   return allowance;
}

/** The DOM-facing form: measure an element and its whole subtree. */
export function horizontalScrollbarAllowance(el: HTMLElement): number {
   return scrollbarAllowanceFrom([
      el,
      ...el.querySelectorAll("*"),
   ] as unknown as MeasurableNode[]);
}
