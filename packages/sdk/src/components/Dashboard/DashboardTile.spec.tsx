// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The tile's own query key, asserted directly rather than through `Dashboard`.
 *
 * `DashboardTile` is exported from the package root, so a host can mount one
 * itself, and `versionId` has to land in both the request and the key from the
 * prop alone.
 */
import { afterEach, beforeEach, expect, it, mock } from "bun:test";
import {
   act,
   fireEvent,
   render,
   screen,
   waitFor,
} from "@testing-library/react";
import {
   cacheKeys,
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";

const executeQueryModel = mock(
   (
      _environmentName: string,
      _packageName: string,
      _modelPath: string,
      _request: { versionId?: string },
      _includeHidden?: boolean,
      _options?: { signal?: AbortSignal },
   ) => pending(),
);

mockServerProvider({ models: { executeQueryModel } });

const { DashboardTile } = await import("./DashboardTile");

const tileAt = (versionId?: string) => (
   <DashboardTile
      environmentName="env"
      packageName="pkg"
      versionId={versionId}
      modelPath="dashboards/ops.malloy"
      tile="sales_by_month"
      givens={new Map()}
      declaredTypes={new Map()}
      height={400}
   />
);

beforeEach(() => {
   clearCache();
   executeQueryModel.mockClear();
});

it("puts the version in the request body and in the key", async () => {
   render(tileAt("v2"), { wrapper: serverWrapper });

   await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
   expect(executeQueryModel.mock.calls[0][3].versionId).toBe("v2");
   expect(cacheKeys("queryResult")[0]).toContain('"v2"');
});

it("sends and keys nothing extra without one", async () => {
   render(tileAt(), { wrapper: serverWrapper });

   await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
   expect(executeQueryModel.mock.calls[0][3].versionId).toBeUndefined();
   // The whole key: an empty slot rather than a literal "undefined", and
   // nothing else disturbed. The narrow assertion above cannot see either.
   expect(cacheKeys("queryResult")[0]).toBe(
      '["queryResult","env","pkg",null,"dashboards/ops.malloy",null,' +
         '"run: sales_by_month",null,"{}","http://localhost/api/v0"]',
   );
});

it("keeps two versions of one tile apart", async () => {
   const { rerender } = render(tileAt("v1"), { wrapper: serverWrapper });
   await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(1));

   rerender(tileAt("v2"));

   await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(2));
   expect(new Set(cacheKeys("queryResult")).size).toBe(2);
});

it("offers to explore from the tile when the host can take it somewhere", () => {
   const onExplore = mock(() => {});
   render(
      <DashboardTile
         environmentName="env"
         packageName="pkg"
         modelPath="dashboards/ops.malloy"
         tile="overview -> sales_by_month"
         givens={new Map()}
         declaredTypes={new Map()}
         height={400}
         onExplore={onExplore}
      />,
      { wrapper: serverWrapper },
   );
   fireEvent.click(screen.getByLabelText("Explore Sales by month"));
   expect(onExplore).toHaveBeenCalledTimes(1);
});

it("has no explore button when the host offers nowhere to go", () => {
   render(tileAt(), { wrapper: serverWrapper });
   expect(screen.queryByLabelText(/^Explore /)).toBeNull();
});

it("puts the filter warning under the heading and above the result", () => {
   render(
      <DashboardTile
         environmentName="env"
         packageName="pkg"
         modelPath="dashboards/ops.malloy"
         tile="overview -> sales_by_month"
         givens={new Map()}
         declaredTypes={new Map()}
         height={400}
         ignoredFilters={["Region"]}
      />,
      { wrapper: serverWrapper },
   );
   const heading = screen.getByText("Sales by month");
   const tag = screen.getByTestId("tile-filter-tag");
   expect(tag.textContent).toBe("Doesn't respond to Region");
   const body = screen.getByText("Running…");
   const follows = (a: Node, b: Node) =>
      Boolean(a.compareDocumentPosition(b) & Node.DOCUMENT_POSITION_FOLLOWING);
   expect(follows(heading, tag)).toBe(true);
   expect(follows(tag, body)).toBe(true);
});

// The request's own signal, as react-query handed it to `executeQueryModel`.
const signalOf = (call: number) =>
   executeQueryModel.mock.calls[call][5]?.signal as AbortSignal;

it("cancels a tile's request when the tile goes away", async () => {
   const { unmount } = render(tileAt("v1"), { wrapper: serverWrapper });
   await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(1));
   expect(signalOf(0).aborted).toBe(false);

   unmount();

   await waitFor(() => expect(signalOf(0).aborted).toBe(true));
});

it("cancels the superseded request when what the tile asks for changes", async () => {
   const { rerender } = render(tileAt("v1"), { wrapper: serverWrapper });
   await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(1));

   rerender(tileAt("v2"));

   await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(2));
   await waitFor(() => expect(signalOf(0).aborted).toBe(true));
   expect(signalOf(1).aborted).toBe(false);
   // A cancelled run is not a failed one: the tile is still waiting, not erroring.
   expect(screen.getByText("Running…")).toBeDefined();
});

// An observer the spec drives by hand: `enter()` reports every observed element
// as intersecting, the way a scroll into the margin would.
let observers: Array<{
   callback: IntersectionObserverCallback;
   targets: Element[];
}> = [];
class DrivenIntersectionObserver {
   private record: (typeof observers)[number];
   constructor(callback: IntersectionObserverCallback) {
      this.record = { callback, targets: [] };
      observers.push(this.record);
   }
   observe(target: Element) {
      this.record.targets.push(target);
   }
   disconnect() {
      this.record.targets = [];
   }
   unobserve() {}
   takeRecords() {
      return [];
   }
}
const enter = () =>
   observers.forEach(({ callback, targets }) =>
      callback(
         targets.map(
            (target) =>
               ({ isIntersecting: true, target }) as IntersectionObserverEntry,
         ),
         {} as IntersectionObserver,
      ),
   );

const originalObserver = globalThis.IntersectionObserver;
const originalRect = HTMLElement.prototype.getBoundingClientRect;
afterEach(() => {
   globalThis.IntersectionObserver = originalObserver;
   HTMLElement.prototype.getBoundingClientRect = originalRect;
   observers = [];
});

it("holds a tile's query until the tile comes near the viewport", async () => {
   (globalThis as { IntersectionObserver?: unknown }).IntersectionObserver =
      DrivenIntersectionObserver;
   // Mounted far below the fold.
   HTMLElement.prototype.getBoundingClientRect = () =>
      ({ top: 10_000, bottom: 10_400 }) as DOMRect;
   render(tileAt(), { wrapper: serverWrapper });

   // Offscreen: nothing asked of the warehouse, and the card is still drawn.
   await new Promise((resolve) => setTimeout(resolve, 20));
   expect(executeQueryModel).not.toHaveBeenCalled();
   expect(screen.getByText("Sales by month")).toBeDefined();

   act(() => enter());

   await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(1));
});
