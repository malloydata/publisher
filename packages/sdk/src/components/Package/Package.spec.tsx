// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The reports listing and the data apps listing follow the package's other
 * listings: each names the version on screen, and is keyed on it.
 *
 * Pinned as a pair, because the data apps were once left unversioned (their
 * route declared no `versionId`), and the cost of that is a version's page
 * listing, and linking to, `latest`'s apps.
 */
import { beforeEach, expect, it, mock } from "bun:test";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import {
   cacheKeys,
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";

const listReports = mock(
   (_environmentName: string, _packageName: string, _versionId?: string) =>
      pending(),
);
const listDataApps = mock(
   (_environmentName: string, _packageName: string, _versionId?: string) =>
      pending(),
);

mockServerProvider({
   packages: { getPackage: pending },
   models: { listModels: pending },
   databases: { listDatabases: pending },
   dataApps: { listDataApps },
   reports: { listReports },
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
   listReports.mockClear();
   listDataApps.mockClear();
});

it("lists a version's reports, and keys the listing on it", async () => {
   render(packageAt("v2"), { wrapper: serverWrapper });

   await waitFor(() => expect(listReports).toHaveBeenCalled());
   expect(listReports.mock.calls[0]).toEqual(["env", "pkg", "v2"]);
   expect(cacheKeys("reports")[0]).toContain('"v2"');
});

it("keys an unversioned listing beside the package's other listings", async () => {
   render(packageAt(), { wrapper: serverWrapper });

   await waitFor(() => expect(listReports).toHaveBeenCalled());
   expect(listReports.mock.calls[0]).toEqual(["env", "pkg", undefined]);
   expect(cacheKeys("reports")[0]).toBe(
      '["reports","env","pkg",null,"http://localhost/api/v0"]',
   );
});

it("lists a version's data apps, and keys the listing on it", async () => {
   render(packageAt("v2"), { wrapper: serverWrapper });

   await waitFor(() => expect(listDataApps).toHaveBeenCalled());
   expect(listDataApps.mock.calls[0]).toEqual(["env", "pkg", "v2"]);
   expect(cacheKeys("data-apps")[0]).toContain('"v2"');
});

it("keys an unversioned data apps listing as DataAppViewer does", async () => {
   render(packageAt(), { wrapper: serverWrapper });

   await waitFor(() => expect(listDataApps).toHaveBeenCalled());
   expect(listDataApps.mock.calls[0]).toEqual(["env", "pkg", undefined]);
   expect(cacheKeys("data-apps")[0]).toBe(
      '["data-apps","env","pkg",null,"http://localhost/api/v0"]',
   );
});

it("says so when the reports listing fails, rather than showing none", async () => {
   // Falling back to an empty list would report a failure as "no reports",
   // which is a wrong answer wearing a right one's clothes.
   listReports.mockImplementationOnce(() =>
      Promise.reject({ response: { status: 501 } }),
   );

   render(packageAt("v2"), { wrapper: serverWrapper });

   expect(await screen.findByText(/Could not list/)).toBeDefined();
});

it("keeps showing no section at all for an older server with no route", async () => {
   // A 404 is a Publisher that predates the endpoint, which genuinely has no
   // reports to list. No error, no section.
   listReports.mockImplementationOnce(() =>
      Promise.reject({ response: { status: 404 } }),
   );

   render(packageAt(), { wrapper: serverWrapper });

   await waitFor(() => expect(listReports).toHaveBeenCalled());
   expect(screen.queryAllByText(/Could not list/)).toHaveLength(0);
});

it("says so when the reports listing gets no answer at all, and retries on request", async () => {
   // A transport failure (no response) is not "no reports": reading it as an
   // empty listing would be cached as an authoritative answer.
   listReports.mockImplementationOnce(() =>
      Promise.reject(new Error("Network Error")),
   );
   listReports.mockImplementationOnce(
      () => Promise.resolve({ data: [] }) as Promise<never>,
   );

   render(packageAt(), { wrapper: serverWrapper });

   expect(await screen.findByText(/Could not list/)).toBeDefined();
   fireEvent.click(screen.getByRole("button", { name: "Retry" }));
   await waitFor(() => expect(listReports).toHaveBeenCalledTimes(2));
   await waitFor(() =>
      expect(screen.queryAllByText(/Could not list/)).toHaveLength(0),
   );
});
