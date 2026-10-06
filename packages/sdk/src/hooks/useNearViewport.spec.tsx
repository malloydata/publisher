// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, expect, it } from "bun:test";
import { act, render, screen } from "@testing-library/react";
import { useNearViewport } from "./useNearViewport";

function Probe() {
   const [ref, near] = useNearViewport<HTMLDivElement>();
   return <div ref={ref}>{near ? "near" : "far"}</div>;
}

let callbacks: IntersectionObserverCallback[] = [];
let options: Array<IntersectionObserverInit | undefined> = [];
let disconnects = 0;
class FakeObserver {
   private callback: IntersectionObserverCallback;
   constructor(
      callback: IntersectionObserverCallback,
      init?: IntersectionObserverInit,
   ) {
      this.callback = callback;
      options.push(init);
   }
   observe() {
      callbacks.push(this.callback);
   }
   disconnect() {
      disconnects += 1;
   }
   unobserve() {}
   takeRecords() {
      return [];
   }
}
const report = (isIntersecting: boolean) =>
   act(() =>
      callbacks.forEach((callback) =>
         callback(
            [{ isIntersecting } as IntersectionObserverEntry],
            {} as IntersectionObserver,
         ),
      ),
   );

// happy-dom's own, put back after each spec: @dnd-kit elsewhere needs one.
const original = globalThis.IntersectionObserver;
const install = (observer: unknown) => {
   (globalThis as { IntersectionObserver?: unknown }).IntersectionObserver =
      observer;
};
// Somewhere far below the fold, for an element the observer has to report.
const placeFarBelow = () =>
   Object.assign(HTMLElement.prototype, {
      getBoundingClientRect: () => ({ top: 10_000, bottom: 10_400 }) as DOMRect,
   });
const originalRect = HTMLElement.prototype.getBoundingClientRect;

afterEach(() => {
   install(original);
   HTMLElement.prototype.getBoundingClientRect = originalRect;
   callbacks = [];
   options = [];
   disconnects = 0;
});

it("is open where there is no IntersectionObserver to ask", () => {
   install(undefined);
   render(<Probe />);
   expect(screen.getByText("near")).toBeDefined();
});

it("is open at once for an element that mounts near the viewport", () => {
   install(FakeObserver);
   render(<Probe />);
   expect(screen.getByText("near")).toBeDefined();
   expect(callbacks).toHaveLength(0);
});

it("opens once the element comes within the margin, and stays open", () => {
   install(FakeObserver);
   placeFarBelow();
   render(<Probe />);
   expect(screen.getByText("far")).toBeDefined();
   expect(options[0]?.rootMargin).toBe("600px 0px");

   report(false);
   expect(screen.getByText("far")).toBeDefined();

   report(true);
   expect(screen.getByText("near")).toBeDefined();
   expect(disconnects).toBeGreaterThan(0);

   // Scrolled away again: what has run stays run.
   report(false);
   expect(screen.getByText("near")).toBeDefined();
});
