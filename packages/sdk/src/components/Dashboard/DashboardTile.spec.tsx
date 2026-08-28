// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The tile's own query key, asserted directly rather than through `Dashboard`.
 *
 * `DashboardTile` is exported, so a host can mount one itself, and `versionId`
 * has to land in both the request and the key from the prop alone.
 */
import { beforeEach, expect, it, mock } from "bun:test";
import { QueryClientProvider } from "@tanstack/react-query";
import { render, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";

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
      apiClients: { models: { executeQueryModel } },
   }),
}));

const { DashboardTile } = await import("./DashboardTile");
const { globalQueryClient } = await import("../../utils/queryClient");

const wrapper = ({ children }: { children: ReactNode }) => (
   <QueryClientProvider client={globalQueryClient}>
      {children}
   </QueryClientProvider>
);

const tileKeys = () =>
   globalQueryClient
      .getQueryCache()
      .getAll()
      .map((query) => JSON.stringify(query.queryKey))
      .filter((key) => key.includes('"dashboardTile"'));

const renderTile = (versionId?: string) =>
   render(
      <DashboardTile
         environmentName="env"
         packageName="pkg"
         versionId={versionId}
         modelPath="dashboards/ops.malloy"
         tile="sales_by_month"
         givens={new Map()}
         declaredTypes={new Map()}
         height={400}
      />,
      { wrapper },
   );

beforeEach(() => {
   globalQueryClient.clear();
   executeQueryModel.mockClear();
});

it("puts the version in the request body and in the key", async () => {
   renderTile("v2");

   await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
   expect(executeQueryModel.mock.calls[0][3].versionId).toBe("v2");
   expect(tileKeys()[0]).toContain('"v2"');
});

it("sends and keys nothing extra without one", async () => {
   renderTile();

   await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
   expect(executeQueryModel.mock.calls[0][3].versionId).toBeUndefined();
});

it("keeps two versions of one tile apart", async () => {
   const { rerender } = renderTile("v1");
   await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(1));

   rerender(
      <DashboardTile
         environmentName="env"
         packageName="pkg"
         versionId="v2"
         modelPath="dashboards/ops.malloy"
         tile="sales_by_month"
         givens={new Map()}
         declaredTypes={new Map()}
         height={400}
      />,
   );

   await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(2));
   expect(new Set(tileKeys()).size).toBe(2);
});
