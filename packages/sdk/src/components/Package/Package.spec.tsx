// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The dashboards listing follows the package's other listings, and only the
 * data apps stay unversioned.
 *
 * Both exceptions used to be written down as one, so this pins the pair apart:
 * `/data-apps` serves static files and declares no `versionId` in the spec,
 * while `list-dashboards` declares one exactly as `list-notebooks` does.
 */
import { beforeEach, expect, it, mock } from "bun:test";
import { QueryClientProvider } from "@tanstack/react-query";
import { render, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";

const pending = () => new Promise<never>(() => {});
const listDashboards = mock(
   (_environmentName: string, _packageName: string, _versionId?: string) =>
      pending(),
);
const listDataApps = mock((_environmentName: string, _packageName: string) =>
   pending(),
);

mock.module("../ServerProvider", () => ({
   ServerProvider: () => null,
   useServer: () => ({
      server: "http://localhost/api/v0",
      apiClients: {
         packages: { getPackage: pending },
         notebooks: { listNotebooks: pending },
         models: { listModels: pending },
         databases: { listDatabases: pending },
         dataApps: { listDataApps },
         dashboards: { listDashboards },
      },
   }),
}));

const { default: Package } = await import("./Package");
const { globalQueryClient } = await import("../../utils/queryClient");

const wrapper = ({ children }: { children: ReactNode }) => (
   <QueryClientProvider client={globalQueryClient}>
      {children}
   </QueryClientProvider>
);

const keyFor = (name: string) =>
   globalQueryClient
      .getQueryCache()
      .getAll()
      .map((query) => JSON.stringify(query.queryKey))
      .find((key) => key.startsWith(`["${name}"`));

beforeEach(() => {
   globalQueryClient.clear();
   listDashboards.mockClear();
   listDataApps.mockClear();
});

it("lists a version's dashboards, and keys the listing on it", async () => {
   render(
      <Package resourceUri="publisher://environments/env/packages/pkg?versionId=v2" />,
      { wrapper },
   );

   await waitFor(() => expect(listDashboards).toHaveBeenCalled());
   expect(listDashboards.mock.calls[0]).toEqual(["env", "pkg", "v2"]);
   expect(keyFor("dashboards")).toContain('"v2"');
});

it("still leaves the data apps unversioned", async () => {
   render(
      <Package resourceUri="publisher://environments/env/packages/pkg?versionId=v2" />,
      { wrapper },
   );

   await waitFor(() => expect(listDataApps).toHaveBeenCalled());
   expect(listDataApps.mock.calls[0]).toEqual(["env", "pkg"]);
   expect(keyFor("data-apps")).not.toContain('"v2"');
});
