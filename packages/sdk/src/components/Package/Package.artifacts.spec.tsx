// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The package page's one Artifacts list: dashboards and notebooks, named by one rule. */
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, mock } from "bun:test";
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

let dashboards: { name: string; path: string; title?: string }[] = [];
let notebooks: { path: string; title?: string }[] = [];
const listDashboards = mock((_e: string, _p: string, _v?: string) =>
   Promise.resolve({ data: dashboards }),
);
const listNotebooks = mock((_e: string, _p: string, _v?: string) =>
   Promise.resolve({ data: notebooks }),
);

mockServerProvider(
   {
      packages: { getPackage: pending },
      notebooks: { listNotebooks, getNotebook: pending },
      models: { listModels: () => Promise.resolve({ data: [] }) },
      databases: { listDatabases: () => Promise.resolve({ data: [] }) },
      dataApps: { listDataApps: () => Promise.resolve({ data: [] }) },
      dashboards: { listDashboards },
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

/** The artifact rows' text, top to bottom. */
const rows = async () => {
   await screen.findByRole("heading", { name: "Artifacts" });
   const section = screen.getByRole("region", { name: "Artifacts" });
   return Array.from(section.querySelectorAll('[role="button"]'))
      .map((row) => row.textContent ?? "")
      .filter((text) => text !== "New");
};

beforeEach(() => {
   clearCache();
   dashboards = [];
   notebooks = [];
   onClick.mockClear();
   listDashboards.mockClear();
   listNotebooks.mockClear();
});
afterEach(cleanup);

it("lists a dashboard and a notebook in one section, each with its kind, ordered by what is shown", async () => {
   dashboards = [
      { name: "overview", path: "dashboards/overview.malloy", title: "Zebra" },
   ];
   notebooks = [
      { path: "notebooks/tour.malloy", title: "Aardvark" },
      { path: "notebooks/plain.malloy", title: "notebooks/plain.malloy" },
   ];
   mount();

   expect(await rows()).toEqual([
      "AardvarkNotebook",
      "plainNotebook",
      "ZebraDashboard",
   ]);
   expect(screen.queryByRole("heading", { name: "Dashboards" })).toBeNull();
   expect(screen.queryByRole("heading", { name: "Notebooks" })).toBeNull();
});

it("shows the folder only to tell two files with one slug apart, and opens each by its kind", async () => {
   dashboards = [{ name: "story", path: "dashboards/story.malloy" }];
   notebooks = [
      { path: "notebooks/story.malloy" },
      { path: "notebooks/solo.malloy" },
   ];
   mount();

   expect(await rows()).toEqual([
      "solo" + "Notebook",
      "story" + "dashboards/story.malloy" + "Dashboard",
      "story" + "notebooks/story.malloy" + "Notebook",
   ]);
   fireEvent.click(screen.getByText("dashboards/story.malloy"));
   fireEvent.click(screen.getByText("notebooks/story.malloy"));
   expect(onClick.mock.calls.map((call) => call[0])).toEqual([
      "/env/pkg/dashboards/story",
      "/env/pkg/notebooks/story",
   ]);
});

it("keeps a titled .malloynb's path as its secondary text", async () => {
   notebooks = [{ path: "orders.malloynb", title: "Orders" }];
   mount();

   expect(await rows()).toEqual(["Ordersorders.malloynbNotebook"]);
});

it("says so when either list fails, and still lists the other", async () => {
   dashboards = [{ name: "overview", path: "dashboards/overview.malloy" }];
   listNotebooks.mockImplementationOnce(() =>
      Promise.reject({ response: { status: 501 } }),
   );
   mount();

   expect(
      await screen.findByText(/Could not list some artifacts/),
   ).toBeDefined();
   expect(await rows()).toEqual(["overviewDashboard"]);
});

it("treats a 404 on the notebooks list as none, without an error", async () => {
   dashboards = [{ name: "overview", path: "dashboards/overview.malloy" }];
   listNotebooks.mockImplementationOnce(() =>
      Promise.reject({ response: { status: 404 } }),
   );
   mount();

   expect(await rows()).toEqual(["overviewDashboard"]);
   expect(screen.queryByText(/Could not list/)).toBeNull();
});

it("lists host drafts of both kinds, each opening its own editor", async () => {
   const draft = (
      type: "dashboard" | "notebook",
      slug: string,
   ): DocumentLocator => ({
      workspace: "browser",
      type,
      path: `env/pkg/${type}s/${slug}.malloy`,
   });
   const stored = [draft("dashboard", "d-one"), draft("notebook", "n-one")];
   const storage: DocumentStorage = {
      listWorkspaces: async () => [
         { name: "browser", writeable: true, description: "This browser" },
      ],
      listDocuments: async (_workspace, type) =>
         stored.filter((locator) => locator.type === type),
      getDocument: async () => {
         throw new Error("absent");
      },
      saveDocument: async () => {},
      deleteDocument: async () => {},
      moveDocument: async () => {},
   };
   dashboards = [{ name: "overview", path: "dashboards/overview.malloy" }];
   mount(storage);

   fireEvent.click(await screen.findByText("d-one"));
   fireEvent.click(await screen.findByText("n-one"));
   expect(onClick.mock.calls.map((call) => call[0])).toEqual([
      "/env/pkg/dashboards/d-one/edit",
      "/env/pkg/notebooks/n-one/edit",
   ]);
});
