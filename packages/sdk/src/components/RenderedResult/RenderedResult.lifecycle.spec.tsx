// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { act, render, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";
import { resolveTheme } from "../../theme/resolveTheme";

/**
 * The renderer's STAGE LIFECYCLE, which is the part of this component that has
 * actually broken.
 *
 * `RenderedResult` never paints into its container directly. Each render builds
 * a fresh "stage" div, appends it, renders the chart into it offscreen, and
 * swaps it in only once the chart has painted — so a re-render never blanks the
 * tile. Every defect this file guards is a bookkeeping error in that dance, and
 * none of them is visible in a screenshot of a working chart:
 *
 * - The drag bug: a cleanup disposed the viz but left its stage node in the
 *   DOM, so the next render appended a second stage BELOW it, outside the
 *   container's clip — the tile drew empty while its rows sat in the DOM the
 *   whole time. Reproducing THAT one needs a real drag over a container that
 *   outlives the cleanup, so it is pinned in `dashboard-builder.spec.ts`; what
 *   is unit-testable here is that unmount takes both, and that a swap leaves
 *   exactly one stage behind.
 * - Two charts at once: a promote that adds the new stage without removing
 *   what was already there.
 * - A leaked viz: an async render that resolves after a newer one started, or
 *   after unmount, and paints anyway.
 *
 * The real renderer is a heavy async chunk that wants a browser; it is faked
 * here down to the five methods this component calls, with `onReady` under the
 * test's control so a render can be held mid-flight.
 */

/** What the fake renderer reports the root draws as; undefined reports no metadata. */
let rootRenderAs: string | undefined;

/** Every viz the component has created, in order. */
const vizzes: FakeViz[] = [];
/** The options each renderer was built with, in order. */
const rendererOptions: Array<{
   vegaConfigOverride?: (chartType: string) => {
      range?: { category?: string[] };
   };
}> = [];

class FakeViz {
   /** The stage it was told to paint into. */
   stage: HTMLElement | null = null;
   removed = false;
   private ready: (() => void) | null = null;
   /** Fire the renderer's "I have painted" signal. */
   paint() {
      this.ready?.();
   }
   /** What the component handed the renderer. */
   result: unknown;
   setResult(result: unknown) {
      this.result = result;
   }
   render(element: HTMLElement) {
      this.stage = element;
      const painted = document.createElement("div");
      painted.className = "malloy-render";
      element.appendChild(painted);
   }
   remove() {
      this.removed = true;
   }
   onReady(callback: () => void) {
      this.ready = callback;
   }
   getMetadata() {
      const renderAs = rootRenderAs;
      return renderAs === undefined
         ? null
         : {
              getRootField: () => ({ key: "root", renderAs: () => renderAs }),
              getPluginsForField: () => [],
           };
   }
   getDrillMetadata() {
      return null;
   }
}

mock.module("@malloydata/render", () => ({
   MalloyRenderer: class {
      constructor(options: (typeof rendererOptions)[number]) {
         rendererOptions.push(options);
      }
      createViz() {
         const viz = new FakeViz();
         vizzes.push(viz);
         return viz;
      }
   },
}));

const RenderedResult = (await import("./RenderedResult")).default;
const { INITIAL_RENDER_HEIGHT, LINEAR_TABLE_MAX_HEIGHT } = await import(
   "./resultSizing"
);

/** A minimal result payload; the fake renderer never reads it. */
const RESULT = JSON.stringify({ tag: "a" });
const OTHER = JSON.stringify({ tag: "b" });

/** The stage divs currently inside the component's container. */
const stagesIn = (container: HTMLElement): Element[] => {
   const host = container.querySelector("[data-testid], div");
   void host;
   return Array.from(container.querySelectorAll(".malloy-render")).map(
      (n) => n.parentElement as Element,
   );
};

beforeEach(() => {
   vizzes.length = 0;
   rendererOptions.length = 0;
   rootRenderAs = undefined;
});

describe("RenderedResult: the stage lifecycle", () => {
   it("paints one chart into one stage", async () => {
      const view = render(<RenderedResult result={RESULT} />);
      await waitFor(() => expect(vizzes).toHaveLength(1));
      act(() => vizzes[0].paint());
      await waitFor(() =>
         expect(stagesIn(view.container as HTMLElement)).toHaveLength(1),
      );
      expect(vizzes[0].removed).toBe(false);
   });

   it("disposes the viz as well as the node when it unmounts", async () => {
      const view = render(<RenderedResult result={RESULT} />);
      await waitFor(() => expect(vizzes).toHaveLength(1));
      act(() => vizzes[0].paint());
      await waitFor(() =>
         expect(stagesIn(view.container as HTMLElement)).toHaveLength(1),
      );
      const stage = vizzes[0].stage!;
      view.unmount();
      expect(vizzes[0].removed).toBe(true);
      expect(stage.isConnected).toBe(false);
   });

   it("keeps the old chart on screen until the new one has painted", async () => {
      // The no-flicker rule: a re-render must never leave the tile empty.
      const view = render(<RenderedResult result={RESULT} />);
      await waitFor(() => expect(vizzes).toHaveLength(1));
      act(() => vizzes[0].paint());
      await waitFor(() =>
         expect(stagesIn(view.container as HTMLElement)).toHaveLength(1),
      );

      view.rerender(<RenderedResult result={OTHER} />);
      await waitFor(() => expect(vizzes).toHaveLength(2));

      // Both are in the DOM, the outgoing one still painted, and the incoming
      // one hidden and overlaid rather than stacked below it.
      await waitFor(() =>
         expect(stagesIn(view.container as HTMLElement)).toHaveLength(2),
      );
      expect(vizzes[0].removed).toBe(false);
      const incoming = vizzes[1].stage as HTMLElement;
      expect(incoming.style.position).toBe("absolute");
      expect(incoming.style.visibility).toBe("hidden");
   });

   it("sweeps the container on promote, so exactly one stage survives", async () => {
      const view = render(<RenderedResult result={RESULT} />);
      await waitFor(() => expect(vizzes).toHaveLength(1));
      act(() => vizzes[0].paint());
      await waitFor(() =>
         expect(stagesIn(view.container as HTMLElement)).toHaveLength(1),
      );

      view.rerender(<RenderedResult result={OTHER} />);
      await waitFor(() => expect(vizzes).toHaveLength(2));
      act(() => vizzes[1].paint());

      await waitFor(() =>
         expect(stagesIn(view.container as HTMLElement)).toHaveLength(1),
      );
      // The outgoing viz is disposed by the swap, not left to leak.
      expect(vizzes[0].removed).toBe(true);
      expect(vizzes[1].removed).toBe(false);
      // And the survivor is revealed rather than left overlaid.
      const live = vizzes[1].stage as HTMLElement;
      expect(live.style.visibility).toBe("");
      expect(live.style.position).toBe("");
   });

   it("ignores a stale render that paints after a newer one took over", async () => {
      // A slow chart resolving late must not resurrect itself over the chart
      // that replaced it, nor leave two stages behind.
      const view = render(<RenderedResult result={RESULT} />);
      await waitFor(() => expect(vizzes).toHaveLength(1));
      act(() => vizzes[0].paint());
      await waitFor(() =>
         expect(stagesIn(view.container as HTMLElement)).toHaveLength(1),
      );

      view.rerender(<RenderedResult result={OTHER} />);
      await waitFor(() => expect(vizzes).toHaveLength(2));
      act(() => vizzes[1].paint());
      await waitFor(() =>
         expect(stagesIn(view.container as HTMLElement)).toHaveLength(1),
      );

      // The superseded render signals ready late. Nothing should move.
      act(() => vizzes[0].paint());
      expect(stagesIn(view.container as HTMLElement)).toHaveLength(1);
      expect(vizzes[1].removed).toBe(false);
   });

   it("does not paint at all once it has unmounted", async () => {
      const view = render(<RenderedResult result={RESULT} />);
      await waitFor(() => expect(vizzes).toHaveLength(1));
      view.unmount();
      // A render resolving after unmount must not re-attach anything.
      act(() => vizzes[0].paint());
      expect(vizzes[0].removed).toBe(true);
   });
});

describe("RenderedResult: how a chart reads", () => {
   const barResult = (rows: number) =>
      JSON.stringify({
         annotations: [{ value: "# bar_chart\n" }],
         schema: {
            fields: [
               { kind: "dimension", name: "carrier", annotations: [] },
               { kind: "dimension", name: "flight_count", annotations: [] },
            ],
         },
         data: {
            kind: "array_cell",
            array_value: Array.from({ length: rows }),
         },
      });

   it("hands the renderer a chart whose unlabelled fields are titled", async () => {
      rootRenderAs = "bar";
      render(<RenderedResult result={barResult(3)} />);
      await waitFor(() => expect(vizzes).toHaveLength(1));
      await waitFor(() => expect(vizzes[0].result).toBeDefined());
      const fields = (
         vizzes[0].result as {
            schema: {
               fields: Array<{ annotations: Array<{ value: string }> }>;
            };
         }
      ).schema.fields;
      expect(fields[1].annotations[0].value).toBe('# label="Flight count"\n');
   });

   it("draws a many-bar chart wide enough for every bar, scrolling sideways", async () => {
      rootRenderAs = "bar";
      const view = render(<RenderedResult result={barResult(80)} />);
      await waitFor(() => expect(vizzes).toHaveLength(1));
      await waitFor(() => expect(vizzes[0].stage).not.toBeNull());
      const stage = vizzes[0].stage!;
      expect(parseInt(stage.style.minWidth, 10)).toBeGreaterThanOrEqual(640);
      const box = view.container.firstElementChild as HTMLElement;
      expect(box.style.overflowX).toBe("auto");
   });

   it("leaves a table's width to its box", async () => {
      rootRenderAs = "table";
      const view = render(<RenderedResult result={barResult(80)} />);
      await waitFor(() => expect(vizzes).toHaveLength(1));
      await waitFor(() => expect(vizzes[0].stage).not.toBeNull());
      expect(vizzes[0].stage!.style.minWidth).toBe("");
      const box = view.container.firstElementChild as HTMLElement;
      expect(box.style.overflowX).toBe("");
   });
});

describe("RenderedResult: a chart's own colour", () => {
   // The builder's Color item writes one colour above the view. A bar and line
   // chart draws its line from the second series colour, so a one-colour list
   // painted the line the same colour as the bars.
   it("sets the first series and keeps the second distinct", async () => {
      const combo = JSON.stringify({
         annotations: [
            { value: "# combo_chart\n" },
            { value: '# theme.palette.series=["#7f9862"]\n' },
         ],
         schema: { fields: [] },
         data: { kind: "array_cell", array_value: [] },
      });
      render(<RenderedResult result={combo} />);
      await waitFor(() => expect(rendererOptions).toHaveLength(1));
      const category =
         rendererOptions[0].vegaConfigOverride?.("combo_chart")?.range
            ?.category;
      expect(category?.[0]).toBe("#7f9862");
      expect(category?.[1]).toBeDefined();
      expect(category?.[1]).not.toBe("#7f9862");
   });

   // The builder previews a tile set back to Theme by running its saved view,
   // which still carries the colour it was saved with, under a line that takes
   // that colour off. The chart must draw the theme's own series.
   it("draws the theme's series when a later line takes a saved colour off", async () => {
      const combo = JSON.stringify({
         annotations: [
            { value: '# theme.palette.series=["#7f9862"]\n' },
            { value: "# combo_chart\n" },
            { value: "# -theme.palette.series\n" },
         ],
         schema: { fields: [] },
         data: { kind: "array_cell", array_value: [] },
      });
      render(<RenderedResult result={combo} />);
      await waitFor(() => expect(rendererOptions).toHaveLength(1));
      const category =
         rendererOptions[0].vegaConfigOverride?.("combo_chart")?.range
            ?.category;
      expect(category).toEqual(resolveTheme([], "light").series);
   });
});

// Through `ResultContainer`, which lazy-loads this same faked module. The box
// the renderer paints into is what a long table scrolls inside, under its
// pinned header row, so its height is the cap a reader sees.
describe("ResultContainer: a linear report's table cap", () => {
   const paintedHeight = async (renderAs: string) => {
      rootRenderAs = renderAs;
      const ResultContainer = (await import("./ResultContainer")).default;
      const view = render(
         <ResultContainer
            result={RESULT}
            fill
            tableMaxHeight={LINEAR_TABLE_MAX_HEIGHT}
         />,
      );
      // The cap applies once the renderer has said what the root is, which
      // it publishes on the stage.
      const stage = () =>
         view.container.querySelector(
            `[data-malloy-render-as="${renderAs}"]`,
         ) as HTMLElement | null;
      await waitFor(() => expect(stage()).not.toBeNull());
      await act(async () => vizzes.at(-1)!.paint());
      const height = (stage()!.parentElement as HTMLElement).style.height;
      view.unmount();
      return height;
   };

   it("holds a table to the cap", async () => {
      expect(await paintedHeight("table")).toBe(`${LINEAR_TABLE_MAX_HEIGHT}px`);
   });

   // A KPI row that wraps one card per line must still show every card.
   it("lets a KPI row grow past it", async () => {
      expect(await paintedHeight("big_value")).toBe(
         `${INITIAL_RENDER_HEIGHT}px`,
      );
   });
});
