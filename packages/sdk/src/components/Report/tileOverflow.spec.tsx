// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a tile does with what it holds, against the page around it: a result
 * taller than its cap scrolls instead of being cut off, and nothing inside a
 * tile is drawn over the pinned control row.
 */
import { act, render } from "@testing-library/react";
import { describe, expect, it } from "bun:test";
import type { CSSProperties } from "react";
import { TileCard } from "../Dashboard/TileCard";
import ResultContainer, {
   RESULT_SCROLLER_CLASS,
} from "../RenderedResult/ResultContainer";
import { STICKY_TOP_VAR, StickyControls } from "./ReportPage";

describe("a tile's result taller than its cap", () => {
   it("scrolls inside the tile rather than being clipped", () => {
      const { container } = render(<ResultContainer result="{}" fill />);
      const scroller = container.querySelector(`.${RESULT_SCROLLER_CLASS}`);
      expect(scroller).not.toBeNull();
      const style = getComputedStyle(scroller as Element);
      expect(style.overflowY).toBe("auto");
      // Sideways it still follows the tile's width, so a narrowed tile narrows.
      expect(style.overflowX).toBe("hidden");
   });

   it("scrolls an unfilled panel too", () => {
      const { container } = render(<ResultContainer result="{}" />);
      const panel = container.firstElementChild as Element;
      expect(getComputedStyle(panel).overflowY).toBe("auto");
   });
});

describe("a tile against the pinned control row", () => {
   it("is its own stacking context, so a table's pinned header cannot paint over the row", () => {
      const { container } = render(
         <TileCard>
            <div />
         </TileCard>,
      );
      const card = container.querySelector("[data-tile-kind]") as Element;
      expect(getComputedStyle(card).isolation).toBe("isolate");
   });

   it("pins the row under what the host pins above the report", () => {
      const { container } = render(
         <div style={{ [STICKY_TOP_VAR]: "44px" } as CSSProperties}>
            <StickyControls>
               <span>controls</span>
            </StickyControls>
         </div>,
      );
      const row = container.querySelector(".publisher-filter-row") as Element;
      expect(getComputedStyle(row).top).toBe("calc(44px + -1px)");
   });

   // Pinned under a host's bar, nothing clips the row, which is what pinning
   // used to be detected by. It stayed transparent, and "+ Filter" was drawn
   // over the title of the tile scrolling under it.
   it("fills the row once it pins under what the host pins above the report", async () => {
      const { container } = render(
         <div
            data-testid="scroller"
            style={
               {
                  overflowY: "auto",
                  height: "300px",
                  [STICKY_TOP_VAR]: "48px",
               } as CSSProperties
            }
         >
            <StickyControls>
               <span>controls</span>
            </StickyControls>
            <div style={{ height: "2000px" }} />
         </div>,
      );
      const scroller = container.firstElementChild as HTMLElement;
      const row = container.querySelector(
         ".publisher-filter-row",
      ) as HTMLElement;
      // happy-dom neither lays out nor resolves `calc()`: the browser's
      // numbers for a row pinned at 47px in a scroller at the page's top.
      const real = window.getComputedStyle.bind(window);
      const restore = () => {
         window.getComputedStyle = real;
      };
      window.getComputedStyle = ((el: Element) =>
         el === row
            ? ({ ...real(el), top: "47px" } as CSSStyleDeclaration)
            : real(el)) as typeof window.getComputedStyle;
      try {
         const rect = (top: number) => ({ top, bottom: top + 48 }) as DOMRect;
         scroller.getBoundingClientRect = () => rect(0);
         row.getBoundingClientRect = () => rect(47);
         scroller.scrollTop = 230;
         await act(async () => {
            scroller.dispatchEvent(new Event("scroll"));
            await new Promise((r) => requestAnimationFrame(() => r(null)));
         });
         expect(row.getAttribute("data-stuck")).toBe("true");

         // Back at the top the row sits in the flow, under the header.
         row.getBoundingClientRect = () => rect(122);
         scroller.scrollTop = 0;
         await act(async () => {
            scroller.dispatchEvent(new Event("scroll"));
            await new Promise((r) => requestAnimationFrame(() => r(null)));
         });
         expect(row.getAttribute("data-stuck")).toBeNull();
      } finally {
         restore();
      }
   });
});
