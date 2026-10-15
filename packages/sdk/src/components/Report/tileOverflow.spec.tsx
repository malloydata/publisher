// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a tile does with what it holds, against the page around it: a result
 * taller than its cap scrolls instead of being cut off, and nothing inside a
 * tile is drawn over the pinned control row.
 */
import { render } from "@testing-library/react";
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
});
