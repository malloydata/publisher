// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { beforeEach, expect, it, mock } from "bun:test";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";

let listed: { path: string; title?: string }[] = [];

const getNotebook = mock(
   (
      _environmentName: string,
      _packageName: string,
      _notebookPath: string,
      _versionId?: string,
   ) => pending(),
);

mockServerProvider({
   packages: { getPackage: pending },
   notebooks: {
      listNotebooks: () => Promise.resolve({ data: listed }),
      getNotebook,
      executeNotebookCell: pending,
   },
   models: { listModels: pending },
   databases: { listDatabases: pending },
   dataApps: { listDataApps: pending },
   dashboards: { listDashboards: () => Promise.resolve({ data: [] }) },
});

const { default: Package } = await import("./Package");

beforeEach(() => {
   clearCache();
   getNotebook.mockClear();
});

it("links a served notebook by slug and a .malloynb by path", async () => {
   listed = [
      { path: "notebooks/tour.malloy", title: "Tour" },
      { path: "old.malloynb", title: "Old" },
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
      "/env/pkg/notebooks/tour",
      "/env/pkg/old.malloynb",
   ]);
});

it("pins notebooks/README.malloy to the front page, matching case-insensitively", async () => {
   listed = [{ path: "notebooks/Readme.malloy" }];
   render(<Package resourceUri="publisher://environments/env/packages/pkg" />, {
      wrapper: serverWrapper,
   });

   await waitFor(() => expect(getNotebook).toHaveBeenCalled());
   expect(getNotebook.mock.calls[0]).toEqual([
      "env",
      "pkg",
      "notebooks/Readme.malloy",
      undefined,
   ]);
});
