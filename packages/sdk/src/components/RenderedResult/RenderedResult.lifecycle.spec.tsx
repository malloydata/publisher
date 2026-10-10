// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { act, render, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";

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

/** Every viz the component has created, in order. */
const vizzes: FakeViz[] = [];

class FakeViz {
   /** The stage it was told to paint into. */
   stage: HTMLElement | null = null;
   removed = false;
   private ready: (() => void) | null = null;
   /** Fire the renderer's "I have painted" signal. */
   paint() {
      this.ready?.();
   }
   setResult() {}
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
      return null;
   }
   getDrillMetadata() {
      return null;
   }
}

mock.module("@malloydata/render", () => ({
   MalloyRenderer: class {
      createViz() {
         const viz = new FakeViz();
         vizzes.push(viz);
         return viz;
      }
   },
}));

const RenderedResult = (await import("./RenderedResult")).default;

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
