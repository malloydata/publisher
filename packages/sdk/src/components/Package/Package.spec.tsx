// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The dashboards listing follows the package's other listings; the data apps
 * do not.
 *
 * Two exceptions that look alike and are not: `/data-apps` serves static files
 * and declares no `versionId` in the spec, while `list-dashboards` declares one
 * exactly as `list-notebooks` does. Pinned as a pair, because the cost of
 * confusing them is a listing that answers for the wrong version.
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

const listDashboards = mock(
   (_environmentName: string, _packageName: string, _versionId?: string) =>
      pending(),
);
const listDataApps = mock((_environmentName: string, _packageName: string) =>
   pending(),
);

mockServerProvider({
   packages: { getPackage: pending },
   notebooks: { listNotebooks: pending },
   models: { listModels: pending },
   databases: { listDatabases: pending },
   dataApps: { listDataApps },
   dashboards: { listDashboards },
});

const { default: Package } = await import("./Package");

const packageAt = (versionId?: string) => (
   <Package
      resourceUri={
         versionId === undefined
            ? "publisher://environments/env/packages/pkg"
            : `publisher://environments/env/packages/pkg?versionId=${versionId}`
      }
   />
);

beforeEach(() => {
   clearCache();
   listDashboards.mockClear();
   listDataApps.mockClear();
});

it("lists a version's dashboards, and keys the listing on it", async () => {
   render(packageAt("v2"), { wrapper: serverWrapper });

   await waitFor(() => expect(listDashboards).toHaveBeenCalled());
   expect(listDashboards.mock.calls[0]).toEqual(["env", "pkg", "v2"]);
   expect(cacheKeys("dashboards")[0]).toContain('"v2"');
});

it("keys an unversioned listing beside the package's other listings", async () => {
   render(packageAt(), { wrapper: serverWrapper });

   await waitFor(() => expect(listDashboards).toHaveBeenCalled());
   expect(listDashboards.mock.calls[0]).toEqual(["env", "pkg", undefined]);
   expect(cacheKeys("dashboards")[0]).toBe(
      '["dashboards","env","pkg",null,"http://localhost/api/v0"]',
   );
   expect(cacheKeys("notebooks")[0]).toBe(
      '["notebooks","env","pkg",null,"http://localhost/api/v0"]',
   );
});

it("still leaves the data apps unversioned", async () => {
   render(packageAt("v2"), { wrapper: serverWrapper });

   await waitFor(() => expect(listDataApps).toHaveBeenCalled());
   expect(listDataApps.mock.calls[0]).toEqual(["env", "pkg"]);
   expect(cacheKeys("data-apps")[0]).not.toContain('"v2"');
});
