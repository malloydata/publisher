// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The package page's one Reports list: both layouts, named by one rule, opened by path. */
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";
import type { ReactNode } from "react";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";
import type {
   DocumentLocator,
   DocumentStorage,
} from "../DocumentStorage/DocumentStorage";
import { DocumentStorageProvider } from "../DocumentStorage/DocumentStorageProvider";

let reports: { path: string; layout: string; title?: string }[] = [];
const listReports = mock((_e: string, _p: string, _v?: string) =>
   Promise.resolve({ data: reports }),
);

mockServerProvider(
   {
      packages: { getPackage: pending },
      reports: { listReports, getReport: pending },
      models: { listModels: () => Promise.resolve({ data: [] }) },
      databases: { listDatabases: () => Promise.resolve({ data: [] }) },
      dataApps: { listDataApps: () => Promise.resolve({ data: [] }) },
      materializations: {
         listMaterializations: () => Promise.resolve({ data: [] }),
      },
   },
   { mutable: true },
);

const { default: Package } = await import("./Package");

const onClick = mock((_to: string) => {});

function mount(storage?: DocumentStorage) {
   const wrap = ({ children }: { children: ReactNode }) =>
      serverWrapper({
         children: storage ? (
            <DocumentStorageProvider documentStorage={storage}>
               {children}
            </DocumentStorageProvider>
         ) : (
            children
         ),
      });
   return render(
      <Package
         resourceUri="publisher://environments/env/packages/pkg"
         onClickPackageFile={onClick}
      />,
      { wrapper: wrap },
   );
}

/** The report rows' text, top to bottom. */
const rows = async () => {
   await screen.findByRole("heading", { name: "Reports" });
   const section = screen.getByRole("region", { name: "Reports" });
   return Array.from(section.querySelectorAll('[role="button"]'))
      .map((row) => row.textContent ?? "")
      .filter((text) => text !== "New report");
};

beforeEach(() => {
   clearCache();
   reports = [];
   onClick.mockClear();
   listReports.mockClear();
});
afterEach(cleanup);

it("lists both layouts in one section, each marked with its layout, ordered by what is shown", async () => {
   reports = [
      { path: "overview.malloy", layout: "grid", title: "Zebra" },
      { path: "tour.malloy", layout: "linear", title: "Aardvark" },
      { path: "plain.malloy", layout: "linear", title: "plain.malloy" },
   ];
   mount();

   expect(await rows()).toEqual([
      "Aardvarktour.malloylinear",
      "plainlinear",
      "Zebraoverview.malloygrid",
   ]);
   expect(screen.queryByRole("heading", { name: "Dashboards" })).toBeNull();
   expect(screen.queryByRole("heading", { name: "Notebooks" })).toBeNull();
   expect(screen.queryByRole("heading", { name: "Artifacts" })).toBeNull();
});

it("shows the path when two reports share a basename, and opens each by its own path", async () => {
   reports = [
      { path: "dashboards/story.malloy", layout: "grid" },
      { path: "notebooks/story.malloy", layout: "linear" },
      { path: "solo.malloy", layout: "linear" },
   ];
   mount();

   expect(await rows()).toEqual([
      "sololinear",
      "story" + "dashboards/story.malloy" + "grid",
      "story" + "notebooks/story.malloy" + "linear",
   ]);
   fireEvent.click(screen.getByText("dashboards/story.malloy"));
   fireEvent.click(screen.getByText("notebooks/story.malloy"));
   expect(onClick.mock.calls.map((call) => call[0])).toEqual([
      "/env/pkg/dashboards/story",
      "/env/pkg/notebooks/story",
   ]);
});

it("opens a nested report at its path route, encoded segment by segment", async () => {
   reports = [{ path: "sales/q 3.malloy", layout: "grid" }];
   mount();

   fireEvent.click(await screen.findByText("sales/q 3.malloy"));
   expect(onClick.mock.calls[0][0]).toBe("/env/pkg/sales/q%203");
});

// DEPRECATED FEATURE COVERAGE — delete together with
// `.malloynb` notebooks (see RELEASE_NOTES deprecations).
describe("deprecated: .malloynb notebooks", () => {
   it("keeps a titled .malloynb's path as its secondary text", async () => {
      reports = [
         { path: "orders.malloynb", layout: "linear", title: "Orders" },
      ];
      mount();

      expect(await rows()).toEqual(["Ordersorders.malloynblinear"]);
   });

   it("names an untitled .malloynb by its file, extension included, with no secondary text", async () => {
      reports = [{ path: "orders.malloynb", layout: "linear" }];
      mount();

      expect(await rows()).toEqual(["orders.malloynblinear"]);
   });
});

it("says so when the listing fails", async () => {
   listReports.mockImplementationOnce(() =>
      Promise.reject({ response: { status: 501 } }),
   );
   mount();

   expect(
      await screen.findByText(/Could not list this package's reports/),
   ).toBeDefined();
});

it("treats a 404 on the reports list as none, without an error", async () => {
   listReports.mockImplementationOnce(() =>
      Promise.reject({ response: { status: 404 } }),
   );
   mount();

   expect(await rows()).toEqual([]);
   expect(screen.queryByText(/Could not list/)).toBeNull();
});

it("lists host drafts by path whatever type they are filed as, each opening its editor", async () => {
   const draft = (
      type: "dashboard" | "notebook",
      path: string,
   ): DocumentLocator => ({
      workspace: "browser",
      type,
      path: `env/pkg/${path}`,
   });
   const stored = [
      draft("dashboard", "d-one.malloy"),
      draft("notebook", "team/n-one.malloy"),
      // Another package's draft is not this one's.
      { workspace: "browser", type: "dashboard", path: "env/other/x.malloy" },
   ] as DocumentLocator[];
   const storage: DocumentStorage = {
      listWorkspaces: async () => [
         { name: "browser", writeable: true, description: "This browser" },
      ],
      listDocuments: async (_workspace, type) =>
         stored.filter(
            (locator) => type === undefined || locator.type === type,
         ),
      getDocument: async () => {
         throw new Error("absent");
      },
      saveDocument: async () => {},
      deleteDocument: async () => {},
      moveDocument: async () => {},
   };
   reports = [{ path: "overview.malloy", layout: "grid" }];
   mount(storage);

   fireEvent.click(await screen.findByText("d-one.malloy"));
   fireEvent.click(await screen.findByText("team/n-one.malloy"));
   expect(screen.queryByText("x.malloy")).toBeNull();
   expect(onClick.mock.calls.map((call) => call[0])).toEqual([
      "/env/pkg/d-one?edit",
      "/env/pkg/team/n-one?edit",
   ]);
});
