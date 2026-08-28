// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The tile's own query key, asserted directly rather than through `Dashboard`.
 *
 * `DashboardTile` is exported from the package root, so a host can mount one
 * itself, and `versionId` has to land in both the request and the key from the
 * prop alone.
 */
import { beforeEach, expect, it, mock } from "bun:test";
import { render, waitFor } from "@testing-library/react";
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
   expect(cacheKeys("dashboardTile")[0]).toContain('"v2"');
});

it("sends and keys nothing extra without one", async () => {
   render(tileAt(), { wrapper: serverWrapper });

   await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
   expect(executeQueryModel.mock.calls[0][3].versionId).toBeUndefined();
   // The whole key: an empty slot rather than a literal "undefined", and
   // nothing else disturbed. The narrow assertion above cannot see either.
   expect(cacheKeys("dashboardTile")[0]).toBe(
      '["dashboardTile","env","pkg",null,"dashboards/ops.malloy",null,' +
         '"sales_by_month","{}","http://localhost/api/v0"]',
   );
});

it("keeps two versions of one tile apart", async () => {
   const { rerender } = render(tileAt("v1"), { wrapper: serverWrapper });
   await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(1));

   rerender(tileAt("v2"));

   await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(2));
   expect(new Set(cacheKeys("dashboardTile")).size).toBe(2);
});
