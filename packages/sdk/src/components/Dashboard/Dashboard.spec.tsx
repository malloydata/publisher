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
import { QueryClientProvider } from "@tanstack/react-query";
import { render, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import type { DashboardManifest } from "../../client";

const getDashboard = mock(
   (
      _environmentName: string,
      _packageName: string,
      _dashboardName: string,
      _versionId?: string,
   ) => new Promise<{ data: DashboardManifest }>(() => {}),
);
const executeQueryModel = mock(
   (
      _environmentName: string,
      _packageName: string,
      _modelPath: string,
      _request: { versionId?: string },
   ) => new Promise<never>(() => {}),
);

mock.module("../ServerProvider", () => ({
   ServerProvider: () => null,
   useServer: () => ({
      server: "http://localhost/api/v0",
      apiClients: {
         dashboards: { getDashboard },
         models: { executeQueryModel },
      },
   }),
}));

// Imported after the mock is registered: a static import would hoist above it.
const { default: Dashboard } = await import("./Dashboard");
const { globalQueryClient } = await import("../../utils/queryClient");

const URI = "publisher://environments/env/packages/pkg";

// The client `ServerProvider` hands down in production, which is the same one
// `useQueryWithApiError` reaches for directly.
const wrapper = ({ children }: { children: ReactNode }) => (
   <QueryClientProvider client={globalQueryClient}>
      {children}
   </QueryClientProvider>
);

/** Every key in the cache, as a string, so a slot can be asserted on. */
const keys = () =>
   globalQueryClient
      .getQueryCache()
      .getAll()
      .map((query) => JSON.stringify(query.queryKey));

const dashboardKey = () => keys().find((key) => key.includes('"dashboard"'));

beforeEach(() => {
   globalQueryClient.clear();
   getDashboard.mockClear();
   executeQueryModel.mockClear();
});

describe("the manifest fetch", () => {
   it("sends the version the URI carried, and keys on it", async () => {
      render(
         <Dashboard resourceUri={`${URI}?versionId=v2`} dashboard="ops" />,
         {
            wrapper,
         },
      );

      await waitFor(() => expect(getDashboard).toHaveBeenCalled());
      expect(getDashboard.mock.calls[0]).toEqual(["env", "pkg", "ops", "v2"]);
      expect(dashboardKey()).toContain("versionId=v2");
   });

   it("sends nothing when the URI named no version", async () => {
      render(<Dashboard resourceUri={URI} dashboard="ops" />, { wrapper });

      await waitFor(() => expect(getDashboard).toHaveBeenCalled());
      expect(getDashboard.mock.calls[0]).toEqual([
         "env",
         "pkg",
         "ops",
         undefined,
      ]);
   });

   it("gives two versions of one dashboard two cache entries", async () => {
      const { rerender } = render(
         <Dashboard resourceUri={`${URI}?versionId=v1`} dashboard="ops" />,
         { wrapper },
      );
      await waitFor(() => expect(getDashboard).toHaveBeenCalledTimes(1));

      rerender(
         <Dashboard resourceUri={`${URI}?versionId=v2`} dashboard="ops" />,
      );

      await waitFor(() => expect(getDashboard).toHaveBeenCalledTimes(2));
      const dashboardKeys = keys().filter((key) => key.includes('"dashboard"'));
      expect(new Set(dashboardKeys).size).toBe(2);
   });
});

describe("the version reaches the tile it renders", () => {
   // End to end through the manifest: the URI's version has to survive the
   // fetch and be handed down as a prop, which is the step a key-only test
   // would miss.
   const manifest: DashboardManifest = {
      name: "ops",
      path: "dashboards/ops.malloy",
      query: "overview",
   };

   it("runs the tile's query against the same version", async () => {
      getDashboard.mockImplementationOnce(() =>
         Promise.resolve({ data: manifest }),
      );

      render(
         <Dashboard resourceUri={`${URI}?versionId=v2`} dashboard="ops" />,
         {
            wrapper,
         },
      );

      await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
      expect(executeQueryModel.mock.calls[0][3].versionId).toBe("v2");
      const tileKey = keys().find((key) => key.includes('"dashboardTile"'));
      expect(tileKey).toContain('"v2"');
   });
});
