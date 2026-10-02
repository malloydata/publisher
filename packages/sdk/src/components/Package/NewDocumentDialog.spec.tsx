// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";
import {
   cacheKeys,
   clearCache,
   mockServerProvider,
   serverWrapper,
   TEST_SERVER,
} from "../../../test/serverProvider";
import type { CreateTarget } from "../DocumentCreate";
import {
   DocumentNotFoundError,
   type DocumentLocator,
   type DocumentStorage,
   type Workspace,
} from "../DocumentStorage/DocumentStorage";

const info = (names: string[]) =>
   JSON.stringify({
      entries: names.map((name) => ({ kind: "source", name })),
   });

const MODELS: Record<string, unknown> = {
   "storefront.malloy": {
      modelPath: "storefront.malloy",
      modelInfo: info(["order_items", "products"]),
      sources: [
         {
            name: "order_items",
            views: [{ name: "by_category" }, { name: "by_brand" }],
         },
         { name: "products", views: [] },
      ],
   },
   "other.malloy": { modelPath: "other.malloy", sources: [] },
   // Imports `order_items` whole-file: it is in `sources` but not exported, so `import { order_items } from "../mid.malloy"` would not compile.
   "mid.malloy": {
      modelPath: "mid.malloy",
      modelInfo: info(["mid_src"]),
      sources: [
         { name: "order_items", views: [{ name: "by_category" }] },
         { name: "mid_src", views: [{ name: "overview" }] },
      ],
   },
};
const getModel = mock(
   (_env: string, _pkg: string, path: string, _versionId?: string) =>
      Promise.resolve({ data: MODELS[path] }),
);
mockServerProvider({ models: { getModel } });

const { NewDocumentDialog } = await import("./NewDocumentDialog");

type Dialog = Parameters<typeof NewDocumentDialog>[0];

const packageTarget = (
   existing: string[] = [],
   write: (path: string, source: string) => Promise<void> = async () => {},
): CreateTarget => ({ route: "package", existing, write });

const mount = (props: Partial<Dialog> = {}) =>
   render(
      <NewDocumentDialog
         open
         kind="dashboard"
         environmentName="env"
         packageName="pkg"
         models={["storefront.malloy", "other.malloy"]}
         target={packageTarget()}
         onClose={() => {}}
         onCreated={() => {}}
         {...props}
      />,
      { wrapper: serverWrapper },
   );

const titleFilled = (value: string, label = "Dashboard title") =>
   waitFor(() =>
      expect(screen.getByLabelText(label)).toHaveProperty("value", value),
   );

beforeEach(() => {
   clearCache();
   getModel.mockClear();
});

describe("NewDocumentDialog", () => {
   it("writes a dashboard with the picked view as its first tile and hands back where it went", async () => {
      const write = mock((_path: string, _source: string) => Promise.resolve());
      const onCreated = mock((_created: unknown) => {});
      const onEvent = mock((_event: unknown) => {});
      mount({ target: packageTarget([], write), onCreated, onEvent });
      await titleFilled("by category");

      // A source with no views is not on the list, so a pair that cannot be written cannot be picked.
      fireEvent.mouseDown(screen.getByRole("combobox", { name: /First tile/ }));
      expect(
         screen.getAllByRole("option").map((option) => option.textContent),
      ).toEqual(["order_items → by_category", "order_items → by_brand"]);
      fireEvent.click(
         screen.getByRole("option", { name: "order_items → by_brand" }),
      );
      fireEvent.change(screen.getByLabelText("Dashboard title"), {
         target: { value: "Sales by Region" },
      });
      expect(
         screen.getByText(/Written as dashboards\/sales-by-region\.malloy/),
      ).toBeDefined();

      fireEvent.click(screen.getByRole("button", { name: "Create dashboard" }));
      await waitFor(() => expect(onCreated).toHaveBeenCalledTimes(1));
      expect(onCreated.mock.calls[0][0]).toEqual({
         kind: "dashboard",
         path: "dashboards/sales-by-region.malloy",
         slug: "sales-by-region",
      });
      const [path, source] = write.mock.calls[0];
      expect(path).toBe("dashboards/sales-by-region.malloy");
      expect(source).toContain('tiles=["order_items_tiles -> by_brand_tile"]');
      expect(source).toContain(
         'import { order_items } from "../storefront.malloy"',
      );
      expect(onEvent.mock.calls).toEqual([
         [{ type: "dashboard.created", where: "package" }],
      ]);
   });

   it("writes a notebook under notebooks/ with its first query", async () => {
      const write = mock((_path: string, _source: string) => Promise.resolve());
      const onCreated = mock((_created: unknown) => {});
      mount({
         kind: "notebook",
         target: packageTarget([], write),
         onCreated,
      });
      await titleFilled("by category", "Notebook title");
      expect(
         screen.getByRole("combobox", { name: /First query/ }),
      ).toBeDefined();

      fireEvent.click(screen.getByRole("button", { name: "Create notebook" }));
      await waitFor(() => expect(onCreated).toHaveBeenCalledTimes(1));
      expect(onCreated.mock.calls[0][0]).toEqual({
         kind: "notebook",
         path: "notebooks/by-category.malloy",
         slug: "by-category",
      });
      const [path, source] = write.mock.calls[0];
      expect(path).toBe("notebooks/by-category.malloy");
      expect(source).toContain("kind=notebook");
      expect(source).toContain("view: cell_1 is by_category");
   });

   it("switches kind in place, keeping the choices already read", async () => {
      mount();
      await titleFilled("by category");
      fireEvent.mouseDown(screen.getByRole("combobox", { name: /Type/ }));
      fireEvent.click(screen.getByRole("option", { name: "Notebook" }));
      expect(
         screen.getByRole("button", { name: "Create notebook" }),
      ).toBeDefined();
      expect(screen.getByLabelText("Notebook title")).toBeDefined();
      expect(getModel).toHaveBeenCalledTimes(2);
   });

   it("takes the next free name when the package already has the title's file", async () => {
      const write = mock((_path: string, _source: string) => Promise.resolve());
      mount({
         target: packageTarget(
            ["dashboards/overview.malloy", "dashboards/overview-2.malloy"],
            write,
         ),
      });
      await titleFilled("by category");
      fireEvent.change(screen.getByLabelText("Dashboard title"), {
         target: { value: "Overview" },
      });
      expect(
         screen.getByText(/Written as dashboards\/overview-3\.malloy/),
      ).toBeDefined();
      fireEvent.click(screen.getByRole("button", { name: "Create dashboard" }));
      await waitFor(() => expect(write).toHaveBeenCalledTimes(1));
      expect(write.mock.calls[0][0]).toBe("dashboards/overview-3.malloy");
   });

   it("does not offer a notebook name a dashboard already took, and the reverse", async () => {
      mount({
         kind: "notebook",
         target: packageTarget(["dashboards/by-category.malloy"]),
      });
      await titleFilled("by category", "Notebook title");
      expect(
         screen.getByText(/Written as notebooks\/by-category\.malloy/),
      ).toBeDefined();
   });

   it("shows what a failed write said and stays open", async () => {
      const onCreated = mock((_created: unknown) => {});
      mount({
         target: packageTarget([], () =>
            Promise.reject(
               Object.assign(new Error("x"), {
                  response: { data: { message: "File already exists" } },
               }),
            ),
         ),
         onCreated,
      });
      await titleFilled("by category");
      fireEvent.click(screen.getByRole("button", { name: "Create dashboard" }));
      expect(await screen.findByText("File already exists")).toBeDefined();
      expect(onCreated).not.toHaveBeenCalled();
   });

   it("offers only the sources a model exports, not the ones it imports", async () => {
      mount({ models: ["mid.malloy"] });
      await titleFilled("overview");
      fireEvent.mouseDown(screen.getByRole("combobox", { name: /First tile/ }));
      expect(
         screen.getAllByRole("option").map((option) => option.textContent),
      ).toEqual(["mid_src → overview"]);
   });

   it("offers nothing from a model whose exports are not stated", async () => {
      MODELS["bare.malloy"] = {
         modelPath: "bare.malloy",
         sources: [{ name: "s", views: [{ name: "v" }] }],
      };
      mount({ models: ["bare.malloy"] });
      expect(
         await screen.findByText(/no model in this package declares one yet/),
      ).toBeDefined();
   });

   it("says what to do when the package has no models", () => {
      mount({ models: [] });
      expect(screen.getByText(/This package has no models yet/)).toBeDefined();
      expect(getModel).not.toHaveBeenCalled();
      expect(
         screen
            .getByRole("button", { name: "Create dashboard" })
            .hasAttribute("disabled"),
      ).toBe(true);
   });

   it("says what to do when no model declares a view", async () => {
      mount({ models: ["other.malloy"] });
      expect(
         await screen.findByText(/no model in this package declares one yet/),
      ).toBeDefined();
      expect(screen.getByText(/Add a view to a source/)).toBeDefined();
   });

   describe("requests", () => {
      it("reads nothing until it is opened, then one model each under its own key", async () => {
         const view = mount({ open: false });
         await new Promise((resolve) => setTimeout(resolve, 20));
         expect(getModel).not.toHaveBeenCalled();

         view.rerender(
            <NewDocumentDialog
               open
               kind="dashboard"
               environmentName="env"
               packageName="pkg"
               models={["storefront.malloy", "other.malloy"]}
               target={packageTarget()}
               onClose={() => {}}
               onCreated={() => {}}
            />,
         );
         await titleFilled("by category");
         expect(getModel).toHaveBeenCalledTimes(2);
         expect(cacheKeys("new-document-model")).toEqual([
            `["new-document-model","env","pkg",null,"storefront.malloy","${TEST_SERVER}"]`,
            `["new-document-model","env","pkg",null,"other.malloy","${TEST_SERVER}"]`,
         ]);

         // Closing and reopening reads the cache, not the server.
         view.rerender(
            <NewDocumentDialog
               open={false}
               kind="dashboard"
               environmentName="env"
               packageName="pkg"
               models={["storefront.malloy", "other.malloy"]}
               target={packageTarget()}
               onClose={() => {}}
               onCreated={() => {}}
            />,
         );
         view.rerender(
            <NewDocumentDialog
               open
               kind="notebook"
               environmentName="env"
               packageName="pkg"
               models={["storefront.malloy", "other.malloy"]}
               target={packageTarget()}
               onClose={() => {}}
               onCreated={() => {}}
            />,
         );
         await titleFilled("by category", "Notebook title");
         expect(getModel).toHaveBeenCalledTimes(2);
      });
   });

   describe("on the storage route", () => {
      const WORKSPACE: Workspace = {
         name: "record",
         writeable: true,
         description: "The record",
         authoritative: true,
      };
      const saved: string[] = [];
      const storage: DocumentStorage = {
         listWorkspaces: async () => [WORKSPACE],
         // A stale listing: it does not know about the file the store has.
         listDocuments: async () => [],
         getDocument: async (locator: DocumentLocator) => {
            if (locator.path === "env/pkg/notebooks/overview.malloy")
               return "someone else's";
            throw new DocumentNotFoundError("absent");
         },
         saveDocument: async (locator: DocumentLocator) => {
            saved.push(locator.path);
         },
         deleteDocument: async () => {},
         moveDocument: async () => {},
      };

      it("saves into the store under a name the store does not have", async () => {
         saved.length = 0;
         const onCreated = mock((_created: unknown) => {});
         mount({
            kind: "notebook",
            target: {
               route: "storage",
               storage,
               workspace: WORKSPACE,
               environmentName: "env",
               packageName: "pkg",
            },
            onCreated,
         });
         await titleFilled("by category", "Notebook title");
         fireEvent.change(screen.getByLabelText("Notebook title"), {
            target: { value: "Overview" },
         });
         fireEvent.click(
            screen.getByRole("button", { name: "Create notebook" }),
         );
         await waitFor(() => expect(onCreated).toHaveBeenCalledTimes(1));
         expect(saved).toEqual(["env/pkg/notebooks/overview-2.malloy"]);
         expect(onCreated.mock.calls[0][0]).toMatchObject({
            slug: "overview-2",
            locator: { workspace: "record", type: "notebook" },
         });
      });
   });
});

describe("NewDocumentDialog: what a model read is cached under", () => {
   it("reads each model at the package's version, under a key that names the version and the server", async () => {
      mount({ versionId: "v3" });
      await titleFilled("by category");
      expect(getModel.mock.calls[0]).toEqual([
         "env",
         "pkg",
         "storefront.malloy",
         "v3",
      ]);
      const [key] = cacheKeys("new-document-model");
      expect(key).toContain('"v3"');
      expect(key).toContain(TEST_SERVER);
   });
});
