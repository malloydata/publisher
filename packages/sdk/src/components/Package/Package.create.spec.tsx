// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Creating a report from the package page: who is offered it, where the file
 * goes (the package root, whatever the layout), where the reader lands, and
 * what is fetched for it.
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
import type { OpenDocumentRequest } from "./Package";

const listModels = mock((_env: string, _pkg: string, _version?: string) =>
   Promise.resolve({
      data: [
         { path: "storefront.malloy" },
         { path: "old.malloy" },
         // A model in a directory named like the old folders is still a model.
         { path: "dashboards/shared.malloy" },
      ],
   }),
);
const listReports = mock((_env: string, _pkg: string, _version?: string) =>
   Promise.resolve({
      data: [{ name: "old", path: "old.malloy", layout: "grid" }],
   }),
);
const getModel = mock((_env: string, _pkg: string, path: string) =>
   Promise.resolve({
      data: {
         modelPath: path,
         modelInfo: JSON.stringify({
            entries: [{ kind: "source", name: "order_items" }],
         }),
         // Only the model the tests start from offers a view.
         sources:
            path === "storefront.malloy"
               ? [{ name: "order_items", views: [{ name: "by_category" }] }]
               : [],
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
      reports: { listReports, getReport: pending },
      models: { listModels, getModel, updateModelSource },
      databases: { listDatabases: () => Promise.resolve({ data: [] }) },
      dataApps: { listDataApps: () => Promise.resolve({ data: [] }) },
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

function mount(
   storage?: DocumentStorage,
   onOpenDocument?: (request: OpenDocumentRequest) => void,
) {
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
         {...(onOpenDocument ? { onOpenDocument } : {})}
      />,
      { wrapper: wrap },
   );
}

const settled = () => screen.findByText("old");
const NEW = { name: "New report" } as const;

beforeEach(() => {
   clearCache();
   context.mutable = true;
   for (const fn of [
      listModels,
      listReports,
      getModel,
      updateModelSource,
      onClickPackageFile,
   ])
      fn.mockClear();
});

describe("who is offered New", () => {
   it("a writable package with no host storage, as one New report action", async () => {
      mount();
      await settled();
      expect(screen.getAllByRole("button", NEW)).toHaveLength(1);
      // One create entry, and no per-kind menu.
      expect(screen.queryByRole("button", { name: "New" })).toBeNull();
      expect(screen.queryByRole("menuitem")).toBeNull();
   });

   it("no one when the server takes no writes and nothing authoritative keeps documents", async () => {
      context.mutable = false;
      mount();
      await settled();
      expect(screen.queryByRole("button", NEW)).toBeNull();
   });

   it("not a browser workspace beside a server that takes no writes", async () => {
      context.mutable = false;
      mount(fakeStorage([BROWSER]).storage);
      await settled();
      await waitFor(() => expect(screen.queryByRole("button", NEW)).toBeNull());
   });

   it("the Console's browser workspace beside a writable server, written to the package", async () => {
      mount(fakeStorage([BROWSER]).storage);
      await settled();
      await waitFor(() =>
         expect(screen.getAllByRole("button", NEW)).toHaveLength(1),
      );
   });

   it("an empty list's row offers New report, and no one when New is not offered", async () => {
      listReports.mockImplementationOnce(() => Promise.resolve({ data: [] }));
      mount();
      await screen.findByText("No reports yet");
      // The heading's action and the empty row's.
      expect(screen.getAllByRole("button", NEW)).toHaveLength(2);
      cleanup();
      clearCache();
      context.mutable = false;
      listReports.mockImplementationOnce(() => Promise.resolve({ data: [] }));
      mount();
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(screen.queryByRole("button", NEW)).toBeNull();
   });

   it("no New report below 600px, where the editors step aside", async () => {
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
         expect(screen.queryByRole("button", NEW)).toBeNull();
         cleanup();
         clearCache();
         listReports.mockImplementationOnce(() =>
            Promise.resolve({ data: [] }),
         );
         mount();
         await screen.findByText("No reports yet");
         expect(screen.queryByRole("button", NEW)).toBeNull();
      } finally {
         window.matchMedia = was;
      }
   });

   it("nothing until the models listing, which names the taken files, has landed", async () => {
      listModels.mockImplementationOnce(() => pending());
      mount();
      await settled();
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(screen.queryByRole("button", NEW)).toBeNull();
   });

   it("nothing while the host's workspaces are still being listed, and nothing when they cannot be", async () => {
      mount(fakeStorage(new Error("backend down")).storage);
      await settled();
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(screen.queryByRole("button", NEW)).toBeNull();
   });
});

const openNew = async (layout: "linear" | "grid" = "linear") => {
   fireEvent.click((await screen.findAllByRole("button", NEW))[0]);
   await waitFor(() =>
      expect(screen.getByLabelText("Report title")).toHaveProperty(
         "value",
         "by category",
      ),
   );
   if (layout === "grid") {
      fireEvent.mouseDown(screen.getByRole("combobox", { name: /Layout/ }));
      fireEvent.click(screen.getByRole("option", { name: /^Grid/ }));
   }
};

describe("creating", () => {
   it("fetches nothing for the dialog until it is opened, then one model each, never a report", async () => {
      mount();
      await settled();
      expect(getModel).not.toHaveBeenCalled();
      await openNew();
      // The listed report is not a model to start from; a model in dashboards/ is.
      expect(getModel.mock.calls.map((call) => call[2]).sort()).toEqual([
         "dashboards/shared.malloy",
         "storefront.malloy",
      ]);
   });

   it("opens the dialog from the empty row, on linear by default", async () => {
      listReports.mockImplementationOnce(() => Promise.resolve({ data: [] }));
      mount();
      fireEvent.click((await screen.findAllByRole("button", NEW))[1]);
      await waitFor(() =>
         expect(screen.getByLabelText("Report title")).toBeDefined(),
      );
      expect(
         screen.getByRole("combobox", { name: /Layout/ }).textContent,
      ).toMatch(/^Linear/);
   });

   it("writes a linear report at the package root, refreshes the listings, and opens its editor", async () => {
      mount();
      await settled();
      await openNew();
      const before = {
         reports: listReports.mock.calls.length,
         models: listModels.mock.calls.length,
      };
      fireEvent.click(screen.getByRole("button", { name: "Create report" }));
      await waitFor(() =>
         expect(onClickPackageFile).toHaveBeenCalledWith(
            "/env/pkg/by-category/edit",
         ),
      );
      const [, , path, body] = updateModelSource.mock.calls[0] as unknown as [
         string,
         string,
         string,
         { source: string },
      ];
      expect(path).toBe("by-category.malloy");
      expect(body.source).toContain("layout=linear");
      expect(body.source).not.toContain("kind=notebook");
      await waitFor(() => {
         expect(listReports).toHaveBeenCalledTimes(before.reports + 1);
         expect(listModels).toHaveBeenCalledTimes(before.models + 1);
      });
   });

   it("writes a grid report at the package root too: the layout never picks a directory", async () => {
      mount();
      await settled();
      await openNew("grid");
      fireEvent.click(screen.getByRole("button", { name: "Create report" }));
      await waitFor(() =>
         expect(onClickPackageFile).toHaveBeenCalledWith(
            "/env/pkg/by-category/edit",
         ),
      );
      const [, , path, body] = updateModelSource.mock.calls[0] as unknown as [
         string,
         string,
         string,
         { source: string },
      ];
      expect(path).toBe("by-category.malloy");
      expect(body.source).toContain("layout=grid");
      expect(body.source).toContain('from "storefront.malloy"');
   });

   it("hands the new report to the host's onOpenDocument by path, which owns the route", async () => {
      const onOpenDocument = mock((_request: OpenDocumentRequest) => {});
      mount(undefined, onOpenDocument);
      await settled();
      await openNew("grid");
      fireEvent.click(screen.getByRole("button", { name: "Create report" }));
      await waitFor(() =>
         expect(onOpenDocument).toHaveBeenCalledWith({
            path: "by-category.malloy",
            layout: "grid",
            kind: "dashboard",
            slug: "by-category",
            mode: "edit",
         }),
      );
      expect(onClickPackageFile).not.toHaveBeenCalled();
   });

   it("writes at a location the author types, with its import relative to it", async () => {
      mount();
      await settled();
      await openNew();
      fireEvent.change(screen.getByLabelText("Location"), {
         target: { value: "sales/q3" },
      });
      fireEvent.click(screen.getByRole("button", { name: "Create report" }));
      await waitFor(() =>
         expect(onClickPackageFile).toHaveBeenCalledWith(
            "/env/pkg/sales/q3/edit",
         ),
      );
      const [, , path, body] = updateModelSource.mock.calls[0] as unknown as [
         string,
         string,
         string,
         { source: string },
      ];
      expect(path).toBe("sales/q3.malloy");
      expect(body.source).toContain('from "../storefront.malloy"');
   });

   it("a report that is already there is not written over", async () => {
      mount();
      await settled();
      await openNew();
      fireEvent.change(screen.getByLabelText("Report title"), {
         target: { value: "Old" },
      });
      fireEvent.click(screen.getByRole("button", { name: "Create report" }));
      await waitFor(() =>
         expect(onClickPackageFile).toHaveBeenCalledWith("/env/pkg/old-2/edit"),
      );
      expect(updateModelSource.mock.calls[0][2]).toBe("old-2.malloy");
   });

   it("an authoritative host's report goes to its store, never the package", async () => {
      const { storage, saved } = fakeStorage([BROWSER, RECORD]);
      mount(storage);
      await settled();
      await openNew();
      fireEvent.click(screen.getByRole("button", { name: "Create report" }));
      await waitFor(() =>
         expect(onClickPackageFile).toHaveBeenCalledWith(
            "/env/pkg/by-category/edit",
         ),
      );
      expect(updateModelSource).not.toHaveBeenCalled();
      expect(saved).toEqual([
         {
            workspace: "record",
            type: "notebook",
            path: "env/pkg/by-category.malloy",
         },
      ]);
   });
});
