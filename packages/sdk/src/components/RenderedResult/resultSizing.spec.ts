// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   contentNode,
   contentNodeDepth,
   INITIAL_RENDER_HEIGHT,
   initialResultHeight,
   measureContentHeight,
   remeasuresAfterReady,
   resolveResultHeight,
   resultSizing,
   UNCAPPED_CONTAINER_HEIGHT,
} from "./resultSizing";

describe("resultSizing", () => {
   // `renderAs()` reports the PLUGIN's name, and the renderer's default
   // registry names the bar and line plugins "bar" and "line" — the tag
   // spellings "bar_chart" and "line_chart" never come back from it. Getting
   // this wrong is invisible: the chart is simply measured, and a `# line_chart`
   // tile quietly shrinks to the inset height it drew itself at.
   it.each(["bar", "line", "chart", "scatter_chart"])(
      "calls %s container-sized",
      (renderAs) => {
         expect(resultSizing(renderAs)).toBe("container");
      },
   );

   it.each([
      "table",
      "dashboard",
      "list",
      "big_value",
      "cell",
      "link",
      "image",
      // Measured: a `# shape_map` reports 365 in a 400px tile and 365 in a
      // 700px cell. Its height is its aspect ratio, not its box, so it shrinks
      // to content like a table rather than filling like a bar chart, which
      // reports its box minus an 8px inset at both of those sizes.
      "shape_map",
      "segment_map",
   ])("calls %s content-sized", (renderAs) => {
      expect(resultSizing(renderAs)).toBe("content");
   });

   // A render plugin returns its own name here, and a renderer newer than this
   // SDK can return one we have never heard of. Content-sized is what every
   // root got before this classification existed, so an unknown name keeps the
   // old behavior rather than silently losing its height.
   it("treats an unknown root as content-sized", () => {
      expect(resultSizing("vega_lite_plugin")).toBe("content");
      expect(resultSizing(undefined)).toBe("content");
   });
});

describe("remeasuresAfterReady", () => {
   // The first measurement races the renderer's layout: a table signals ready
   // before its virtualized grid has laid out, and a `# big_value` row reports
   // the box it was handed rather than its own 136px.
   it.each(["table", "big_value", "dashboard", "list", "shape_map"])(
      "keeps watching %s",
      (renderAs) => {
         expect(remeasuresAfterReady(renderAs)).toBe(true);
      },
   );

   // A host's own plugin could be a fill-the-box viz wearing a name we do not
   // know, and one measurement is what it got before any of this existed.
   it.each(["vega_lite_plugin", undefined])(
      "measures the unrecognised %s once",
      (renderAs) => {
         expect(resultSizing(renderAs)).toBe("content");
         expect(remeasuresAfterReady(renderAs)).toBe(false);
      },
   );

   // The one root re-measuring is not safe for, and the reason it used to be
   // tables only: a chart insets itself inside its box, so feeding its height
   // back would ratchet the container down every pass. It is never measured now.
   it.each(["bar", "line", "chart", "scatter_chart"])(
      "never measures %s at all",
      (renderAs) => {
         expect(remeasuresAfterReady(renderAs)).toBe(false);
      },
   );
});

describe("contentNodeDepth", () => {
   it("reads a dashboard grid one level deeper than every other root", () => {
      expect(contentNodeDepth("dashboard")).toBe(3);
      expect(contentNodeDepth("table")).toBe(2);
      expect(contentNodeDepth(undefined)).toBe(2);
   });
});

/** A chain of nested divs, each with the scrollHeight it is given. */
function stageWithHeights(...heights: number[]): HTMLElement {
   const stage = document.createElement("div");
   let parent: HTMLElement = stage;
   for (const height of heights) {
      const node = document.createElement("div");
      Object.defineProperty(node, "scrollHeight", {
         value: height,
         configurable: true,
      });
      parent.appendChild(node);
      parent = node;
   }
   return stage;
}

describe("contentNode", () => {
   // The node measured and the node watched for changes have to be the same one,
   // or a result that settles later settles at a height nothing reported.
   it("is the node measureContentHeight reads", () => {
      const stage = stageWithHeights(400, 137, 120);
      expect(contentNode(stage, 2)?.scrollHeight).toBe(
         measureContentHeight(stage, 2),
      );
   });

   it("is null before the renderer has built anything", () => {
      expect(contentNode(document.createElement("div"), 2)).toBeNull();
   });

   it("is null while the renderer has not built that deep", () => {
      expect(contentNode(stageWithHeights(400, 400), 3)).toBeNull();
   });
});

describe("measureContentHeight", () => {
   // Depth 1 is the renderer's outer wrapper, which is box-sized: measuring it
   // would floor every result at the height the container already has.
   it("reads the root render node at depth 2", () => {
      expect(measureContentHeight(stageWithHeights(400, 137, 120), 2)).toBe(
         137,
      );
   });

   it("reads a dashboard grid at depth 3", () => {
      expect(measureContentHeight(stageWithHeights(400, 400, 227), 3)).toBe(
         227,
      );
   });

   // The shallow nodes are the renderer's box-sized wrappers, so answering with
   // one reports back the height the panel already has. That reads as a
   // successful measurement and stops the retry, which is how a `# big_value`
   // tile got stuck at its 400px cap with its own content measuring 136.
   it("reports nothing while the DOM is shallower than the content node", () => {
      expect(measureContentHeight(stageWithHeights(400, 400), 3)).toBe(0);
   });

   it("reports nothing when the renderer has not built anything yet", () => {
      expect(measureContentHeight(document.createElement("div"), 2)).toBe(0);
   });
});

describe("initialResultHeight", () => {
   it("paints an uncapped result at the seed height", () => {
      expect(initialResultHeight(undefined)).toBe(INITIAL_RENDER_HEIGHT);
   });

   it("never paints taller than the cap", () => {
      expect(initialResultHeight(400)).toBe(400);
   });
});

describe("resolveResultHeight", () => {
   const at = (
      sizing: "content" | "container" | undefined,
      contentHeight: number | undefined,
      maxHeight: number | undefined,
   ) => resolveResultHeight({ sizing, contentHeight, maxHeight });

   it("paints at the seed before anything is known", () => {
      expect(at(undefined, undefined, 400)).toBe(400);
      expect(at(undefined, undefined, undefined)).toBe(INITIAL_RENDER_HEIGHT);
   });

   // The whole point of the classification: a chart fills its box, so the cap
   // IS its height and it is never measured. This is what the 20000px "no cap"
   // sentinel used to turn into a 1992px bar chart.
   it("gives a container-sized root its cap, and a default with no cap", () => {
      expect(at("container", undefined, 400)).toBe(400);
      expect(at("container", undefined, undefined)).toBe(
         UNCAPPED_CONTAINER_HEIGHT,
      );
   });

   // Even if one arrived: a chart's measurement only ever reports back the
   // height it was handed.
   it("ignores a measurement from a container-sized root", () => {
      expect(at("container", 1992, undefined)).toBe(UNCAPPED_CONTAINER_HEIGHT);
   });

   it("shrinks a content-sized root to its content", () => {
      expect(at("content", 137, 400)).toBe(137);
   });

   it("caps a content-sized root taller than the cap", () => {
      expect(at("content", 4000, 700)).toBe(700);
   });

   // No cap means no cap: the single-query dashboard form lets its one result
   // set the page height and scrolls.
   it("lets an uncapped content-sized root use its full height", () => {
      expect(at("content", 4000, undefined)).toBe(4000);
   });

   it("stays at the seed while a content-sized root measures zero", () => {
      expect(at("content", 0, 400)).toBe(400);
   });
});
