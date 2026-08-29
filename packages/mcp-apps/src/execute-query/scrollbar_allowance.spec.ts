// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   scrollbarAllowanceFrom,
   type MeasurableNode,
} from "./scrollbar_allowance";

/**
 * The fixtures below are RECORDED, not invented. They are the values Chromium
 * reported for the bundled storefront `business_overview` dashboard rendered in
 * the widget, which is the case that was broken:
 *
 *   svg <text>  scrollWidth 37, clientWidth 0, clientHeight 2, offsetHeight ABSENT
 *   container   scrollWidth == clientWidth (no horizontal overflow)
 *
 * 43 of the dashboard's 787 nodes were overflowing SVG nodes like that one. Each
 * produced NaN, `Math.max` propagated it, and the container height became
 * "NaNpx", which the browser drops. The dashboard rendered at its initial 400px
 * with 1500px of content, the KPI row cut mid-glyph, and the four chart tiles
 * below it never visible at all.
 */

/** An overflowing SVG node, exactly as Chromium measured it. */
const SVG_TEXT: MeasurableNode = {
   scrollWidth: 37,
   clientWidth: 0,
   clientHeight: 2,
   // offsetHeight deliberately absent: SVGElement does not define it.
};

/** An overflowing HTML node whose scrollbar occupies 15px of layout. */
const WIDE_TABLE: MeasurableNode = {
   scrollWidth: 900,
   clientWidth: 400,
   offsetHeight: 215,
   clientHeight: 200,
};

/** A node that does not overflow horizontally. */
const NO_OVERFLOW: MeasurableNode = {
   scrollWidth: 400,
   clientWidth: 400,
   offsetHeight: 400,
   clientHeight: 400,
};

describe("scrollbarAllowanceFrom", () => {
   it("returns a finite allowance when an SVG node is present", () => {
      // The regression. Before the guard this returned NaN, and the caller
      // rendered "NaNpx", which is silently dropped rather than throwing.
      const allowance = scrollbarAllowanceFrom([SVG_TEXT]);
      expect(Number.isFinite(allowance)).toBe(true);
      expect(allowance).toBe(0);
   });

   it("does not let one SVG node erase a real HTML scrollbar allowance", () => {
      // Order matters: NaN propagates through Math.max regardless of position,
      // so both orders have to hold.
      expect(scrollbarAllowanceFrom([WIDE_TABLE, SVG_TEXT])).toBe(15);
      expect(scrollbarAllowanceFrom([SVG_TEXT, WIDE_TABLE])).toBe(15);
   });

   it("survives the real dashboard's ratio of SVG nodes to real ones", () => {
      // 43 overflowing SVG nodes was the measured count for the dashboard.
      const nodes = [...Array.from({ length: 43 }, () => SVG_TEXT), WIDE_TABLE];
      expect(scrollbarAllowanceFrom(nodes)).toBe(15);
   });

   it("returns the largest allowance when several nodes overflow", () => {
      const taller: MeasurableNode = {
         scrollWidth: 900,
         clientWidth: 400,
         offsetHeight: 230,
         clientHeight: 200,
      };
      // Both orders, deliberately. With the larger node LAST, an implementation
      // that simply assigns instead of taking the max still returns 30, so that
      // case alone does not pin the property. The larger-first case does.
      expect(scrollbarAllowanceFrom([WIDE_TABLE, taller])).toBe(30);
      expect(scrollbarAllowanceFrom([taller, WIDE_TABLE])).toBe(30);
   });

   it("returns 0 when nothing overflows horizontally", () => {
      expect(scrollbarAllowanceFrom([NO_OVERFLOW])).toBe(0);
   });

   it("returns 0 for overlay scrollbars, which consume no layout", () => {
      // macOS: the element overflows but offsetHeight == clientHeight.
      expect(
         scrollbarAllowanceFrom([
            {
               scrollWidth: 900,
               clientWidth: 400,
               offsetHeight: 200,
               clientHeight: 200,
            },
         ]),
      ).toBe(0);
   });

   it("ignores a node missing the width fields entirely", () => {
      expect(scrollbarAllowanceFrom([{}])).toBe(0);
   });

   it("returns 0 for an empty node list", () => {
      expect(scrollbarAllowanceFrom([])).toBe(0);
   });

   it("treats a 1px difference as not overflowing", () => {
      // The +1 tolerance: sub-pixel layout makes scrollWidth exceed clientWidth
      // by a fraction on elements that are not really scrollable.
      expect(
         scrollbarAllowanceFrom([
            {
               scrollWidth: 401,
               clientWidth: 400,
               offsetHeight: 215,
               clientHeight: 200,
            },
         ]),
      ).toBe(0);
   });
});
