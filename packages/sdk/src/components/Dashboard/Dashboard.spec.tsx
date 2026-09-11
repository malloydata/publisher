// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a dashboard's queries are CACHED under, not what they render.
 *
 * A version that reaches the request but not the request's key is worse than
 * no version at all: two versions of one dashboard share a cache entry and one
 * silently serves the other's data, which looks correct on screen. Nothing a
 * manual smoke test can see, so it is pinned here.
 */
import { beforeEach, describe, expect, it, mock } from "bun:test";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import {
   cacheKeys,
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";
import type { DashboardManifest } from "../../client";

const getDashboard = mock(
   (
      _environmentName: string,
      _packageName: string,
      _dashboardName: string,
      _versionId?: string,
   ) => pending<{ data: DashboardManifest }>(),
);
const executeQueryModel = mock(
   (
      _environmentName: string,
      _packageName: string,
      _modelPath: string,
      _request: { versionId?: string; givens?: Record<string, string> },
   ) => pending(),
);

mockServerProvider({
   dashboards: { getDashboard },
   models: { executeQueryModel },
});

// Imported after the stub is registered: a static import would hoist above it.
const { default: Dashboard } = await import("./Dashboard");

const URI = "publisher://environments/env/packages/pkg";

const dashboardAt = (versionId?: string) => (
   <Dashboard
      resourceUri={
         versionId === undefined ? URI : `${URI}?versionId=${versionId}`
      }
      dashboard="ops"
   />
);

/** The latest tile request, told apart from a suggest by its `givens`. */
const tileRequest = () =>
   executeQueryModel.mock.calls
      .filter((call) => call[3] !== undefined && "givens" in call[3])
      .at(-1)?.[3];

beforeEach(() => {
   clearCache();
   // `mockClear` leaves a queued `mockImplementationOnce` in place, so a test
   // that queued one it never consumed would hand it to the next test.
   getDashboard.mockReset();
   getDashboard.mockImplementation(() => pending());
   executeQueryModel.mockReset();
   executeQueryModel.mockImplementation(() => pending());
});

describe("the manifest fetch", () => {
   it("sends the version the URI carried, and keys on it", async () => {
      render(dashboardAt("v2"), { wrapper: serverWrapper });

      await waitFor(() => expect(getDashboard).toHaveBeenCalled());
      expect(getDashboard.mock.calls[0]).toEqual(["env", "pkg", "ops", "v2"]);
      expect(cacheKeys("dashboard")[0]).toContain('"v2"');
   });

   it("sends and keys nothing when the URI named no version", async () => {
      render(dashboardAt(), { wrapper: serverWrapper });

      await waitFor(() => expect(getDashboard).toHaveBeenCalled());
      expect(getDashboard.mock.calls[0]).toEqual([
         "env",
         "pkg",
         "ops",
         undefined,
      ]);
      // The whole key, not just the absence of a version: the slot is empty
      // and nothing else about the key moved. A `toContain` here would pass
      // for a key that had also grown something it should not carry.
      expect(cacheKeys("dashboard")[0]).toBe(
         '["dashboard","env","pkg",null,"ops","http://localhost/api/v0"]',
      );
   });

   it("gives two versions of one dashboard two cache entries", async () => {
      const { rerender } = render(dashboardAt("v1"), {
         wrapper: serverWrapper,
      });
      await waitFor(() => expect(getDashboard).toHaveBeenCalledTimes(1));

      rerender(dashboardAt("v2"));

      await waitFor(() => expect(getDashboard).toHaveBeenCalledTimes(2));
      expect(new Set(cacheKeys("dashboard")).size).toBe(2);
   });

   it("keys two spellings of one unversioned dashboard together", async () => {
      // Built from what `parseResourceUri` returned rather than from the URI
      // as the host spelled it, so an empty `?versionId=` and no query string
      // at all stay one cache entry instead of fetching the same thing twice.
      const { rerender } = render(dashboardAt(), { wrapper: serverWrapper });
      await waitFor(() => expect(getDashboard).toHaveBeenCalledTimes(1));

      rerender(<Dashboard resourceUri={`${URI}?versionId=`} dashboard="ops" />);

      await waitFor(() => expect(cacheKeys("dashboard").length).toBe(1));
      expect(getDashboard).toHaveBeenCalledTimes(1);
   });
});

describe("the version reaches what the manifest drives", () => {
   // End to end through the manifest: the version has to survive the fetch and
   // be handed on, which is the step a key-only test cannot see.
   const manifest: DashboardManifest = {
      name: "ops",
      path: "dashboards/ops.malloy",
      query: "overview",
      givens: [
         { name: "REGION", type: "string" },
         {
            name: "TIER",
            control: "select",
            suggest: { source: "orders", dimension: "tier" },
         },
      ],
   };

   beforeEach(() => {
      getDashboard.mockImplementation(() =>
         Promise.resolve({ data: manifest }),
      );
   });

   it("runs the tile's query against the same version", async () => {
      render(dashboardAt("v2"), { wrapper: serverWrapper });

      await waitFor(() => expect(cacheKeys("dashboardTile").length).toBe(1));
      expect(tileRequest()?.versionId).toBe("v2");
      expect(cacheKeys("dashboardTile")[0]).toContain('"v2"');
   });

   it("runs every composite tile against the same version", async () => {
      // The composite branch wraps each tile in its own grid cell, so the prop
      // is threaded at a different call site than the single-query form above.
      getDashboard.mockImplementation(() =>
         Promise.resolve({
            data: {
               ...manifest,
               query: undefined,
               tiles: [{ query: "by_month" }, { query: "by_region" }],
            },
         }),
      );

      render(dashboardAt("v2"), { wrapper: serverWrapper });

      await waitFor(() => expect(cacheKeys("dashboardTile").length).toBe(2));
      for (const key of cacheKeys("dashboardTile"))
         expect(key).toContain('"v2"');
   });

   it("runs the control's suggest query against the same version", async () => {
      render(dashboardAt("v2"), { wrapper: serverWrapper });

      await waitFor(() => expect(cacheKeys("givenSuggest").length).toBe(1));
      expect(cacheKeys("givenSuggest")[0]).toContain('"v2"');
   });

   it("does not carry one version's applied values into another", async () => {
      // `documentKey` is what tells `useGivensState` the document changed. With
      // the version missing from it, a host swapping versions kept the values
      // the reader applied to the old one and filtered the new one by them —
      // the same confusion the keys above exist to prevent, one layer up.
      //
      // Mounted with no `givens` prop, which is the case the key governs. A
      // host feeding its URL back in through `givens` re-supplies the values
      // itself, and no `documentKey` can override that.
      const { rerender } = render(dashboardAt("v1"), {
         wrapper: serverWrapper,
      });
      await waitFor(() => expect(cacheKeys("dashboardTile").length).toBe(1));

      fireEvent.change(await screen.findByLabelText("REGION"), {
         target: { value: "CA" },
      });
      await waitFor(() =>
         expect(tileRequest()?.givens).toEqual({ REGION: "CA" }),
      );

      rerender(dashboardAt("v2"));

      await waitFor(() => expect(cacheKeys("dashboardTile").length).toBe(3));
      const v2 = cacheKeys("dashboardTile").filter((key) =>
         key.includes('"v2"'),
      );
      expect(v2).toHaveLength(1);
      expect(v2[0]).not.toContain("CA");
   });
});
