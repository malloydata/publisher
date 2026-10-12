// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** How the package page opens a report, and which one it pins as the README. */
import { beforeEach, expect, it, mock } from "bun:test";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";

let listed: { path: string; layout?: string; title?: string }[] = [];

const getReport = mock(
   (
      _environmentName: string,
      _packageName: string,
      _path: string,
      _versionId?: string,
   ) => pending(),
);

mockServerProvider({
   packages: { getPackage: pending },
   reports: {
      listReports: () => Promise.resolve({ data: listed }),
      getReport,
   },
   models: { listModels: pending },
   databases: { listDatabases: pending },
   dataApps: { listDataApps: pending },
});

const { default: Package } = await import("./Package");

beforeEach(() => {
   clearCache();
   getReport.mockClear();
});

it("opens a report by its path, and a .malloynb by its file", async () => {
   listed = [
      { path: "sales/tour.malloy", layout: "linear", title: "Tour" },
      { path: "old.malloynb", layout: "linear", title: "Old" },
   ];
   const onClick = mock((_to: string) => {});
   render(
      <Package
         resourceUri="publisher://environments/env/packages/pkg"
         onClickPackageFile={onClick}
      />,
      { wrapper: serverWrapper },
   );

   fireEvent.click(await screen.findByText("Tour"));
   fireEvent.click(await screen.findByText("Old"));

   expect(onClick.mock.calls.map((call) => call[0])).toEqual([
      "/env/pkg/sales/tour",
      "/env/pkg/old.malloynb",
   ]);
});

it("opens a report in dashboards/ by its path too: the directory means nothing", async () => {
   listed = [{ path: "dashboards/story.malloy", layout: "linear" }];
   const onClick = mock((_to: string) => {});
   render(
      <Package
         resourceUri="publisher://environments/env/packages/pkg"
         onClickPackageFile={onClick}
      />,
      { wrapper: serverWrapper },
   );

   fireEvent.click(await screen.findByText("story"));

   expect(onClick.mock.calls.map((call) => call[0])).toEqual([
      "/env/pkg/dashboards/story",
   ]);
});

it("pins a root report named README to the front page, matching case-insensitively", async () => {
   listed = [{ path: "Readme.malloy", layout: "linear" }];
   render(<Package resourceUri="publisher://environments/env/packages/pkg" />, {
      wrapper: serverWrapper,
   });

   await waitFor(() => expect(getReport).toHaveBeenCalled());
   expect(getReport.mock.calls[0].slice(0, 4)).toEqual([
      "env",
      "pkg",
      "Readme.malloy",
      undefined,
   ]);
});

it("does not pin a README below the package root", async () => {
   listed = [{ path: "notebooks/README.malloy", layout: "linear" }];
   render(<Package resourceUri="publisher://environments/env/packages/pkg" />, {
      wrapper: serverWrapper,
   });

   await screen.findByText("README");
   expect(getReport).not.toHaveBeenCalled();
});

it("pins the root README.malloynb over a root README.malloy when both exist", async () => {
   listed = [
      { path: "README.malloy", layout: "linear" },
      { path: "README.malloynb", layout: "linear" },
   ];
   render(<Package resourceUri="publisher://environments/env/packages/pkg" />, {
      wrapper: serverWrapper,
   });

   await waitFor(() => expect(getReport).toHaveBeenCalled());
   expect(getReport.mock.calls[0][2]).toBe("README.malloynb");
});
