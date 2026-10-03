// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Creating a dashboard or notebook from the package page: who is offered it,
 * where the file goes, where the reader lands, and what is fetched for it.
 */
import {
   cleanup,
   fireEvent,
   render,
   screen,
   waitFor,
} from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";
import type { ReactNode } from "react";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";
import {
   DocumentNotFoundError,
   type DocumentLocator,
   type DocumentStorage,
   type Workspace,
} from "../DocumentStorage/DocumentStorage";
import { DocumentStorageProvider } from "../DocumentStorage/DocumentStorageProvider";

const listModels = mock((_env: string, _pkg: string, _version?: string) =>
   Promise.resolve({
      data: [{ path: "storefront.malloy" }, { path: "dashboards/old.malloy" }],
   }),
);
const listDashboards = mock((_env: string, _pkg: string, _version?: string) =>
   Promise.resolve({
      data: [{ name: "old", path: "dashboards/old.malloy" }],
   }),
);
const listNotebooks = mock((_env: string, _pkg: string, _version?: string) =>
   Promise.resolve({ data: [] }),
);
const getModel = mock((_env: string, _pkg: string, _path: string) =>
   Promise.resolve({
      data: {
         modelPath: "storefront.malloy",
         modelInfo: JSON.stringify({
            entries: [{ kind: "source", name: "order_items" }],
         }),
         sources: [{ name: "order_items", views: [{ name: "by_category" }] }],
      },
   }),
);
const updateModelSource = mock((_env: string, _pkg: string, path: string) =>
   Promise.resolve({ data: { path, contentHash: "h", created: true } }),
);

// Mutated per test: the stub hands the same object to every render.
const context: Record<string, unknown> = { mutable: true };
mockServerProvider(
   {
      packages: { getPackage: pending },
      notebooks: { listNotebooks },
      models: { listModels, getModel, updateModelSource },
      databases: { listDatabases: () => Promise.resolve({ data: [] }) },
      dataApps: { listDataApps: () => Promise.resolve({ data: [] }) },
      dashboards: { listDashboards },
      materializations: {
         listMaterializations: () => Promise.resolve({ data: [] }),
      },
   },
   context,
);

const { default: Package } = await import("./Package");

const BROWSER: Workspace = {
   name: "browser",
   writeable: true,
   description: "This browser",
};
const RECORD: Workspace = {
   name: "record",
   writeable: true,
   description: "The record",
   authoritative: true,
};

function fakeStorage(workspaces: Workspace[] | Error) {
   const saved: DocumentLocator[] = [];
   const storage: DocumentStorage = {
      listWorkspaces: async () => {
         if (workspaces instanceof Error) throw workspaces;
         return workspaces;
      },
      listDocuments: async () => [],
      getDocument: async () => {
         throw new DocumentNotFoundError("absent");
      },
      saveDocument: async (locator) => {
         saved.push(locator);
      },
      deleteDocument: async () => {},
      moveDocument: async () => {},
   };
   return { storage, saved };
}

const onClickPackageFile = mock((_to: string) => {});

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
         onClickPackageFile={onClickPackageFile}
      />,
      { wrapper: wrap },
   );
}

const settled = () => screen.findByText("old");

beforeEach(() => {
   clearCache();
   context.mutable = true;
   for (const fn of [
      listModels,
      listDashboards,
      listNotebooks,
      getModel,
      updateModelSource,
      onClickPackageFile,
   ])
      fn.mockClear();
});

describe("who is offered New", () => {
   it("a writable package with no host storage", async () => {
      mount();
      await settled();
      expect(screen.getByRole("button", { name: "New" })).toBeDefined();
      // One create entry, on the Artifacts heading row.
      expect(
         screen.queryByRole("button", { name: "Add dashboard" }),
      ).toBeNull();
      expect(screen.queryByRole("button", { name: "Add notebook" })).toBeNull();
   });

   it("no one when the server takes no writes and nothing authoritative keeps documents", async () => {
      context.mutable = false;
      mount();
      await settled();
      expect(screen.queryByRole("button", { name: "New" })).toBeNull();
      expect(
         screen.queryByRole("button", { name: "Add dashboard" }),
      ).toBeNull();
      expect(screen.queryByRole("button", { name: "Add notebook" })).toBeNull();
   });

   it("not a browser workspace beside a server that takes no writes", async () => {
      context.mutable = false;
      mount(fakeStorage([BROWSER]).storage);
      await settled();
      await waitFor(() =>
         expect(screen.queryByRole("button", { name: "New" })).toBeNull(),
      );
      expect(screen.queryByRole("button", { name: "Add notebook" })).toBeNull();
   });

   it("the Console's browser workspace beside a writable server, written to the package", async () => {
      mount(fakeStorage([BROWSER]).storage);
      await settled();
      await waitFor(() =>
         expect(screen.getByRole("button", { name: "New" })).toBeDefined(),
      );
   });

   it("an empty list's row offers New, and no one when New is not offered", async () => {
      listDashboards.mockImplementationOnce(() =>
         Promise.resolve({ data: [] }),
      );
      mount();
      await screen.findByText("No artifacts yet");
      expect(
         screen.getByRole("button", { name: "New artifact" }),
      ).toBeDefined();
      cleanup();
      clearCache();
      context.mutable = false;
      listDashboards.mockImplementationOnce(() =>
         Promise.resolve({ data: [] }),
      );
      mount();
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(screen.queryByRole("button", { name: "New artifact" })).toBeNull();
   });

   it("no New menu below 600px, where the editors step aside", async () => {
      const was = window.matchMedia;
      window.matchMedia = ((query: string) => ({
         matches: query.includes("max-width"),
         media: query,
         addEventListener: () => {},
         removeEventListener: () => {},
         addListener: () => {},
         removeListener: () => {},
         onchange: null,
         dispatchEvent: () => false,
      })) as unknown as typeof window.matchMedia;
      try {
         mount();
         await settled();
         await new Promise((resolve) => setTimeout(resolve, 20));
         expect(screen.queryByRole("button", { name: "New" })).toBeNull();
         cleanup();
         clearCache();
         listDashboards.mockImplementationOnce(() =>
            Promise.resolve({ data: [] }),
         );
         mount();
         await screen.findByText("No artifacts yet");
         expect(
            screen.queryByRole("button", { name: "New artifact" }),
         ).toBeNull();
      } finally {
         window.matchMedia = was;
      }
   });

   it("nothing until the models listing, which names the taken files, has landed", async () => {
      listModels.mockImplementationOnce(() => pending());
      mount();
      await settled();
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(screen.queryByRole("button", { name: "New" })).toBeNull();
   });

   it("nothing while the host's workspaces are still being listed, and nothing when they cannot be", async () => {
      mount(fakeStorage(new Error("backend down")).storage);
      await settled();
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(screen.queryByRole("button", { name: "New" })).toBeNull();
   });
});

const openNew = async (kind: "Dashboard" | "Notebook") => {
   fireEvent.click(await screen.findByRole("button", { name: "New" }));
   fireEvent.click(await screen.findByRole("menuitem", { name: kind }));
   await waitFor(() =>
      expect(screen.getByLabelText(`${kind} title`)).toHaveProperty(
         "value",
         "by category",
      ),
   );
};

describe("creating", () => {
   it("fetches nothing for the dialog until it is opened, then one model each", async () => {
      mount();
      await settled();
      expect(getModel).not.toHaveBeenCalled();
      await openNew("Dashboard");
      expect(getModel).toHaveBeenCalledTimes(1);
      // dashboards/ files are not models to start from.
      expect(getModel.mock.calls[0][2]).toBe("storefront.malloy");
   });

   it("opens the New menu from the empty row, then the dialog on the kind picked", async () => {
      listDashboards.mockImplementationOnce(() =>
         Promise.resolve({ data: [] }),
      );
      mount();
      fireEvent.click(
         await screen.findByRole("button", { name: "New artifact" }),
      );
      fireEvent.click(
         await screen.findByRole("menuitem", { name: "Notebook" }),
      );
      await waitFor(() =>
         expect(screen.getByLabelText("Notebook title")).toBeDefined(),
      );
   });

   it("writes a dashboard into the package, refreshes the listings, and opens its editor", async () => {
      mount();
      await settled();
      await openNew("Dashboard");
      const before = {
         dashboards: listDashboards.mock.calls.length,
         notebooks: listNotebooks.mock.calls.length,
         models: listModels.mock.calls.length,
      };
      fireEvent.click(screen.getByRole("button", { name: "Create dashboard" }));
      await waitFor(() =>
         expect(onClickPackageFile).toHaveBeenCalledWith(
            "/env/pkg/dashboards/by-category/edit",
         ),
      );
      const [, , path, body] = updateModelSource.mock.calls[0] as unknown as [
         string,
         string,
         string,
         { source: string },
      ];
      expect(path).toBe("dashboards/by-category.malloy");
      expect(body.source).toContain(
         'tiles=["order_items_tiles -> by_category_tile"]',
      );
      await waitFor(() => {
         expect(listDashboards).toHaveBeenCalledTimes(before.dashboards + 1);
         expect(listNotebooks).toHaveBeenCalledTimes(before.notebooks + 1);
         expect(listModels).toHaveBeenCalledTimes(before.models + 1);
      });
   });

   it("writes a notebook into the package and opens its editor", async () => {
      mount();
      await settled();
      await openNew("Notebook");
      fireEvent.click(screen.getByRole("button", { name: "Create notebook" }));
      await waitFor(() =>
         expect(onClickPackageFile).toHaveBeenCalledWith(
            "/env/pkg/notebooks/by-category/edit",
         ),
      );
      expect(updateModelSource.mock.calls[0][2]).toBe(
         "notebooks/by-category.malloy",
      );
   });

   it("a dashboard that is already there is not written over", async () => {
      mount();
      await settled();
      await openNew("Dashboard");
      fireEvent.change(screen.getByLabelText("Dashboard title"), {
         target: { value: "Old" },
      });
      fireEvent.click(screen.getByRole("button", { name: "Create dashboard" }));
      await waitFor(() =>
         expect(onClickPackageFile).toHaveBeenCalledWith(
            "/env/pkg/dashboards/old-2/edit",
         ),
      );
      expect(updateModelSource.mock.calls[0][2]).toBe(
         "dashboards/old-2.malloy",
      );
   });

   it("an authoritative host's document goes to its store, never the package", async () => {
      const { storage, saved } = fakeStorage([BROWSER, RECORD]);
      mount(storage);
      await settled();
      await openNew("Notebook");
      fireEvent.click(screen.getByRole("button", { name: "Create notebook" }));
      await waitFor(() =>
         expect(onClickPackageFile).toHaveBeenCalledWith(
            "/env/pkg/notebooks/by-category/edit",
         ),
      );
      expect(updateModelSource).not.toHaveBeenCalled();
      expect(saved).toEqual([
         {
            workspace: "record",
            type: "notebook",
            path: "env/pkg/notebooks/by-category.malloy",
         },
      ]);
   });
});
