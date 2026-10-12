// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";
import {
   cacheKeys,
   clearCache,
   mockServerProvider,
   pending,
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
const serve = (
   _env: string,
   _pkg: string,
   path: string,
   _versionId?: string,
): Promise<{ data: unknown }> => Promise.resolve({ data: MODELS[path] });
const getModel = mock(serve);
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
         layout="grid"
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

const titleFilled = (value: string, label = "Report title") =>
   waitFor(() =>
      expect(screen.getByLabelText(label)).toHaveProperty("value", value),
   );

beforeEach(() => {
   clearCache();
   getModel.mockClear();
   getModel.mockImplementation(serve);
});

const failing = (status?: number) =>
   Promise.reject(
      Object.assign(
         new Error("Request failed"),
         status === undefined ? {} : { response: { status } },
      ),
   );

const createButton = (name: string | RegExp) =>
   screen.getByRole("button", { name });

const isDisabled = (element: HTMLElement) => element.hasAttribute("disabled");

describe("NewDocumentDialog", () => {
   it("writes a dashboard with the picked view as its first tile and hands back where it went", async () => {
      const write = mock((_path: string, _source: string) => Promise.resolve());
      const onCreated = mock((_created: unknown) => {});
      const onEvent = mock((_event: unknown) => {});
      mount({ target: packageTarget([], write), onCreated, onEvent });
      await titleFilled("by category");

      // A source with no views is not on the list, so a pair that cannot be written cannot be picked.
      fireEvent.mouseDown(
         screen.getByRole("combobox", { name: /First query/ }),
      );
      expect(
         screen.getAllByRole("option").map((option) => option.textContent),
      ).toEqual(["order_items → by_category", "order_items → by_brand"]);
      fireEvent.click(
         screen.getByRole("option", { name: "order_items → by_brand" }),
      );
      fireEvent.change(screen.getByLabelText("Report title"), {
         target: { value: "Sales by Region" },
      });
      expect(
         screen.getByText(/Written as sales-by-region\.malloy in the package/),
      ).toBeDefined();

      fireEvent.click(screen.getByRole("button", { name: "Create report" }));
      await waitFor(() => expect(onCreated).toHaveBeenCalledTimes(1));
      expect(onCreated.mock.calls[0][0]).toEqual({
         layout: "grid",
         path: "sales-by-region.malloy",
         slug: "sales-by-region",
      });
      const [path, source] = write.mock.calls[0];
      expect(path).toBe("sales-by-region.malloy");
      expect(source).toContain("layout=grid");
      expect(source).toContain('"order_items_tiles -> by_brand_tile"');
      expect(source).toContain(
         'import { order_items } from "storefront.malloy"',
      );
      expect(onEvent.mock.calls).toEqual([
         [
            expect.objectContaining({
               type: "report.created",
               where: "package",
               layout: "grid",
            }),
         ],
      ]);
   });

   it("writes a linear report at the package root with its first query", async () => {
      const write = mock((_path: string, _source: string) => Promise.resolve());
      const onCreated = mock((_created: unknown) => {});
      mount({
         layout: "linear",
         target: packageTarget([], write),
         onCreated,
      });
      await titleFilled("by category");
      expect(
         screen.getByRole("combobox", { name: /First query/ }),
      ).toBeDefined();

      fireEvent.click(screen.getByRole("button", { name: "Create report" }));
      await waitFor(() => expect(onCreated).toHaveBeenCalledTimes(1));
      expect(onCreated.mock.calls[0][0]).toEqual({
         layout: "linear",
         path: "by-category.malloy",
         slug: "by-category",
      });
      const [path, source] = write.mock.calls[0];
      expect(path).toBe("by-category.malloy");
      expect(source).toContain("layout=linear");
      expect(source).not.toContain("kind=notebook");
      expect(source).toContain("view: by_category_tile is by_category");
   });

   it("switches layout in place, keeping the choices already read, and writes it", async () => {
      const write = mock((_path: string, _source: string) => Promise.resolve());
      mount({ target: packageTarget([], write) });
      await titleFilled("by category");
      fireEvent.mouseDown(screen.getByRole("combobox", { name: /Layout/ }));
      fireEvent.click(screen.getByRole("option", { name: /^Linear/ }));
      expect(getModel).toHaveBeenCalledTimes(2);
      fireEvent.click(screen.getByRole("button", { name: "Create report" }));
      await waitFor(() => expect(write).toHaveBeenCalledTimes(1));
      expect(write.mock.calls[0][0]).toBe("by-category.malloy");
      expect(write.mock.calls[0][1]).toContain("layout=linear");
   });

   it("opens on linear when the host names no layout", async () => {
      mount({ layout: undefined });
      await titleFilled("by category");
      expect(
         screen.getByRole("combobox", { name: /Layout/ }).textContent,
      ).toMatch(/^Linear/);
   });

   it("writes at a typed location, with the import relative to it", async () => {
      const write = mock((_path: string, _source: string) => Promise.resolve());
      const onCreated = mock((_created: unknown) => {});
      mount({ target: packageTarget([], write), onCreated });
      await titleFilled("by category");
      fireEvent.change(screen.getByLabelText("Location"), {
         target: { value: "team/sales/q3" },
      });
      expect(
         screen.getByText(/Written as team\/sales\/q3\.malloy in the package/),
      ).toBeDefined();
      fireEvent.click(screen.getByRole("button", { name: "Create report" }));
      await waitFor(() => expect(onCreated).toHaveBeenCalledTimes(1));
      expect(write.mock.calls[0][0]).toBe("team/sales/q3.malloy");
      expect(write.mock.calls[0][1]).toContain(
         'import { order_items } from "../../storefront.malloy"',
      );
      expect(onCreated.mock.calls[0][0]).toMatchObject({
         path: "team/sales/q3.malloy",
         slug: "q3",
      });
   });

   it("refuses a typed location that is taken, under public/, or the package's index", async () => {
      const write = mock((_path: string, _source: string) => Promise.resolve());
      mount({ target: packageTarget(["taken.malloy"], write) });
      await titleFilled("by category");
      const location = screen.getByLabelText("Location");
      fireEvent.change(location, { target: { value: "taken.malloy" } });
      expect(
         screen.getByText(
            /taken\.malloy already exists; choose another location\./,
         ),
      ).toBeDefined();
      fireEvent.click(screen.getByRole("button", { name: "Create report" }));
      fireEvent.change(location, { target: { value: "public/x" } });
      expect(screen.getByText(/cannot live under public\//)).toBeDefined();
      fireEvent.change(location, { target: { value: "index" } });
      expect(screen.getByText(/index\.malloy is its surface/)).toBeDefined();
      expect(write).not.toHaveBeenCalled();
   });

   it("takes the next free name when the package already has the title's file", async () => {
      const write = mock((_path: string, _source: string) => Promise.resolve());
      mount({
         target: packageTarget(["overview.malloy", "overview-2.malloy"], write),
      });
      await titleFilled("by category");
      fireEvent.change(screen.getByLabelText("Report title"), {
         target: { value: "Overview" },
      });
      expect(
         screen.getByText(/Written as overview-3\.malloy in the package/),
      ).toBeDefined();
      fireEvent.click(screen.getByRole("button", { name: "Create report" }));
      await waitFor(() => expect(write).toHaveBeenCalledTimes(1));
      expect(write.mock.calls[0][0]).toBe("overview-3.malloy");
   });

   it("checks a name against paths only: a same-named file in another directory does not take it", async () => {
      mount({
         layout: "linear",
         target: packageTarget(["dashboards/by-category.malloy"]),
      });
      await titleFilled("by category");
      expect(
         screen.getByText(/Written as by-category\.malloy in the package/),
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
      fireEvent.click(screen.getByRole("button", { name: "Create report" }));
      expect(await screen.findByText("File already exists")).toBeDefined();
      expect(onCreated).not.toHaveBeenCalled();
   });

   it("offers only the sources a model exports, not the ones it imports", async () => {
      mount({ models: ["mid.malloy"] });
      await titleFilled("overview");
      fireEvent.mouseDown(
         screen.getByRole("combobox", { name: /First query/ }),
      );
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
            .getByRole("button", {
               name: "Create report: No model view to start from yet",
            })
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
               layout="grid"
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
               layout="grid"
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
               layout="linear"
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
            if (locator.path === "env/pkg/overview.malloy")
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
            layout: "linear",
            target: {
               route: "storage",
               storage,
               workspace: WORKSPACE,
               environmentName: "env",
               packageName: "pkg",
            },
            onCreated,
         });
         await titleFilled("by category");
         fireEvent.change(screen.getByLabelText("Report title"), {
            target: { value: "Overview" },
         });
         fireEvent.click(screen.getByRole("button", { name: "Create report" }));
         await waitFor(() => expect(onCreated).toHaveBeenCalledTimes(1));
         expect(saved).toEqual(["env/pkg/overview-2.malloy"]);
         expect(onCreated.mock.calls[0][0]).toMatchObject({
            slug: "overview-2",
            locator: { workspace: "record", type: "linear" },
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

describe("NewDocumentDialog: a model it could not read", () => {
   it("names it, keeps the rest, and offers a retry that brings its views back", async () => {
      let healthy = false;
      getModel.mockImplementation((env, pkg, path, versionId) =>
         path === "storefront.malloy" && !healthy
            ? failing(503)
            : serve(env, pkg, path, versionId),
      );
      mount();
      expect(
         await screen.findByText(
            "Couldn't read storefront.malloy, so its views aren't listed.",
         ),
      ).toBeDefined();
      expect(
         screen.queryByText(/no model in this package declares/),
      ).toBeNull();
      expect(
         isDisabled(
            createButton(
               "Create report: Couldn't read the model views to start from",
            ),
         ),
      ).toBe(true);

      healthy = true;
      fireEvent.click(screen.getByRole("button", { name: "Retry" }));
      await titleFilled("by category");
      expect(screen.queryByText(/Couldn't read storefront\.malloy/)).toBeNull();
      expect(isDisabled(createButton("Create report"))).toBe(false);
   });

   it("names every model it could not read", async () => {
      getModel.mockImplementation(() => failing(500));
      mount();
      expect(
         await screen.findByText(
            "Couldn't read storefront.malloy, other.malloy, so their views aren't listed.",
         ),
      ).toBeDefined();
   });

   it("lets Create through when another model still offers a view", async () => {
      getModel.mockImplementation((env, pkg, path, versionId) =>
         path === "other.malloy"
            ? failing(500)
            : serve(env, pkg, path, versionId),
      );
      mount();
      await titleFilled("by category");
      expect(
         await screen.findByText(
            "Couldn't read other.malloy, so its views aren't listed.",
         ),
      ).toBeDefined();
      expect(isDisabled(createButton("Create report"))).toBe(false);
   });

   for (const [status, retry] of [
      [401, false],
      [403, false],
      [404, false],
      [500, true],
      [503, true],
      [undefined, true],
   ] as const)
      it(`${retry ? "offers" : "offers no"} Retry after ${status ?? "no response"}`, async () => {
         getModel.mockImplementation(() => failing(status));
         mount({ models: ["storefront.malloy"] });
         await screen.findByText(/Couldn't read storefront\.malloy/);
         expect(screen.queryByRole("button", { name: "Retry" }) !== null).toBe(
            retry,
         );
      });
});

describe("NewDocumentDialog: why Create is off", () => {
   it("while the views load, in its name and its tooltip", async () => {
      getModel.mockImplementation(() => pending());
      mount();
      const button = createButton("Create report: Loading model views");
      expect(isDisabled(button)).toBe(true);
      fireEvent.mouseOver(button.parentElement!);
      expect((await screen.findByRole("tooltip")).textContent).toBe(
         "Loading model views",
      );
   });

   it("while the host still lists the models, not that the package has none", () => {
      mount({ models: [], modelsLoading: true });
      expect(screen.queryByText(/This package has no models yet/)).toBeNull();
      expect(
         isDisabled(createButton("Create report: Loading model views")),
      ).toBe(true);
   });

   it("when the host could not list the models, with its Retry", () => {
      const onRetryModels = mock(() => {});
      mount({
         models: [],
         modelsError: Object.assign(new Error("x"), {
            response: { status: 502 },
         }),
         onRetryModels,
      });
      expect(screen.queryByText(/This package has no models yet/)).toBeNull();
      expect(
         screen.getByText(/Couldn't read this package's models/),
      ).toBeDefined();
      expect(
         isDisabled(
            createButton(
               "Create report: Couldn't read the model views to start from",
            ),
         ),
      ).toBe(true);
      fireEvent.click(screen.getByRole("button", { name: "Retry" }));
      expect(onRetryModels).toHaveBeenCalledTimes(1);
   });

   it("offers no Retry for a models listing a retry cannot fix", () => {
      mount({
         models: [],
         modelsError: { status: 403 },
         onRetryModels: () => {},
      });
      expect(
         screen.getByText(/Couldn't read this package's models/),
      ).toBeDefined();
      expect(screen.queryByRole("button", { name: "Retry" })).toBeNull();
   });

   it("when every model failed to read", async () => {
      getModel.mockImplementation(() => failing(500));
      mount();
      await screen.findByText(/Couldn't read storefront\.malloy/);
      expect(
         isDisabled(
            createButton(
               "Create report: Couldn't read the model views to start from",
            ),
         ),
      ).toBe(true);
   });

   it("when no model has a view", async () => {
      mount({ models: ["other.malloy"] });
      await screen.findByText(/no model in this package declares one yet/);
      expect(
         isDisabled(
            createButton("Create report: No model view to start from yet"),
         ),
      ).toBe(true);
   });

   it("not for a blank title, whose problem it shows on press", async () => {
      const write = mock((_path: string, _source: string) => Promise.resolve());
      mount({ target: packageTarget([], write) });
      await titleFilled("by category");
      fireEvent.change(screen.getByLabelText("Report title"), {
         target: { value: "  " },
      });
      expect(screen.queryByText("A title cannot be empty.")).toBeNull();
      const button = createButton("Create report");
      expect(isDisabled(button)).toBe(false);
      fireEvent.click(button);
      expect(screen.getByText("A title cannot be empty.")).toBeDefined();
      expect(
         screen.getByLabelText("Report title").getAttribute("aria-invalid"),
      ).toBe("true");
      expect(write).not.toHaveBeenCalled();
   });

   it("not for a title that cannot be written, whose problem shows as it is typed", async () => {
      mount({ layout: "linear" });
      await titleFilled("by category");
      fireEvent.change(screen.getByLabelText("Report title"), {
         target: { value: "a |## b" },
      });
      expect(screen.getByText(/A title cannot contain `\|##`/)).toBeDefined();
      expect(isDisabled(createButton("Create report"))).toBe(false);
   });
});

describe("NewDocumentDialog: the picked view", () => {
   const base = MODELS["storefront.malloy"] as Record<string, unknown>;
   const versions: Record<string, unknown> = {
      reordered: {
         ...base,
         sources: [
            {
               name: "order_items",
               views: [{ name: "by_brand" }, { name: "by_category" }],
            },
         ],
      },
      dropped: {
         ...base,
         sources: [{ name: "order_items", views: [{ name: "by_category" }] }],
      },
   };

   const reread = (view: ReturnType<typeof mount>, versionId: string) =>
      view.rerender(
         <NewDocumentDialog
            open
            layout="grid"
            environmentName="env"
            packageName="pkg"
            versionId={versionId}
            models={["storefront.malloy"]}
            target={packageTarget()}
            onClose={() => {}}
            onCreated={() => {}}
         />,
      );

   const firstTile = () =>
      screen.getByRole("combobox", { name: /First query/ });

   const pickBrand = async () => {
      await titleFilled("by category");
      fireEvent.mouseDown(firstTile());
      fireEvent.click(
         screen.getByRole("option", { name: "order_items → by_brand" }),
      );
      expect(firstTile().textContent).toBe("order_items → by_brand");
   };

   beforeEach(() => {
      getModel.mockImplementation((env, pkg, path, versionId) =>
         versionId !== undefined && versions[versionId] !== undefined
            ? Promise.resolve({ data: versions[versionId] })
            : serve(env, pkg, path, versionId),
      );
   });

   it("stays picked when the views are read again in another order", async () => {
      const view = mount({ models: ["storefront.malloy"], versionId: "first" });
      await pickBrand();
      reread(view, "reordered");
      await waitFor(() => expect(getModel).toHaveBeenCalledTimes(2));
      await waitFor(() =>
         expect(firstTile().textContent).toBe("order_items → by_brand"),
      );
      expect(isDisabled(createButton("Create report"))).toBe(false);
   });

   it("is not swapped for another when it leaves the list", async () => {
      const view = mount({ models: ["storefront.malloy"], versionId: "first" });
      await pickBrand();
      reread(view, "dropped");
      expect(
         await screen.findByText(
            "The view you picked is no longer in this model; pick another.",
         ),
      ).toBeDefined();
      expect(firstTile().textContent).not.toContain("by_category");
      expect(
         isDisabled(createButton("Create report: Pick a view to start from")),
      ).toBe(true);
   });
});

describe("NewDocumentDialog: while it creates", () => {
   it("says so, and neither Cancel, Escape nor the backdrop closes it", async () => {
      let finish: () => void = () => {};
      const write = mock(
         (_path: string, _source: string) =>
            new Promise<void>((resolve) => {
               finish = resolve;
            }),
      );
      const onClose = mock(() => {});
      const onCreated = mock((_created: unknown) => {});
      mount({ target: packageTarget([], write), onClose, onCreated });
      await titleFilled("by category");
      fireEvent.click(createButton("Create report"));

      const creating = await screen.findByRole("button", {
         name: "Creating report…",
      });
      expect(isDisabled(creating)).toBe(true);
      expect(isDisabled(screen.getByRole("button", { name: "Cancel" }))).toBe(
         true,
      );
      fireEvent.keyDown(screen.getByRole("dialog"), { key: "Escape" });
      fireEvent.click(document.querySelector(".MuiBackdrop-root")!);
      expect(onClose).not.toHaveBeenCalled();

      finish();
      await waitFor(() => expect(onCreated).toHaveBeenCalledTimes(1));
   });

   it("names the report while it creates a linear one", async () => {
      mount({ layout: "linear", target: packageTarget([], () => pending()) });
      await titleFilled("by category");
      fireEvent.click(createButton("Create report"));
      expect(
         await screen.findByRole("button", { name: "Creating report…" }),
      ).toBeDefined();
   });
});

describe("NewDocumentDialog: what the host decides", () => {
   it("closes from Cancel and from Escape", async () => {
      const onClose = mock(() => {});
      mount({ onClose });
      await titleFilled("by category");
      fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
      fireEvent.keyDown(screen.getByRole("dialog"), { key: "Escape" });
      expect(onClose).toHaveBeenCalledTimes(2);
   });

   it("offers no layout to switch to when allowLayoutChange is false", async () => {
      mount({ layout: "linear", allowLayoutChange: false });
      await titleFilled("by category");
      expect(screen.queryByRole("combobox", { name: /Layout/ })).toBeNull();
      expect(createButton("Create report")).toBeDefined();
   });

   it("says where the host saves it, in the host's words", async () => {
      mount({
         target: {
            route: "storage",
            storage: {} as DocumentStorage,
            workspace: {
               name: "w",
               writeable: true,
               description: "",
               authoritative: true,
            },
            environmentName: "env",
            packageName: "pkg",
            existing: [],
         },
         savedAs: "Saved to your workspace, and opened in the builder.",
      });
      await titleFilled("by category");
      expect(
         screen.getByText(
            "Saved to your workspace, and opened in the builder.",
         ),
      ).toBeDefined();
   });
});
