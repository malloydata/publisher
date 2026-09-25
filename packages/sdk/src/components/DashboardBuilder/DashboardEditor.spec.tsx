// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";
import { BrowserDocumentStorage } from "../DocumentStorage/BrowserDocumentStorage";
import { DocumentStorageProvider } from "../DocumentStorage/DocumentStorageProvider";
import type { DashboardEvent } from "../Dashboard/telemetry";

/**
 * The Console's path through the builder, with the server mocked at the
 * client and the browser's storage real: open the package file, offer a
 * saved draft, save an edit into storage, export the file.
 */
const PACKAGE_FILE = `## artifact { title="Storefront" tiles=["a -> by_cat"] } dashboard { columns=12 }
import { scoped_orders } from "../data_app.malloy"

source: a is scoped_orders extend {
  # colspan=6
  # label="By category"
  view: by_cat is by_category
}`;

const getModel = mock((_env: string, _pkg: string, path: string) =>
   Promise.resolve({
      data:
         path === "dashboards/overview.malloy"
            ? { modelPath: path, sourceText: PACKAGE_FILE }
            : {
                 modelPath: path,
                 sources: [
                    {
                       name: "scoped_orders",
                       views: [{ name: "by_category" }, { name: "by_brand" }],
                    },
                 ],
                 sourceInfos: [
                    JSON.stringify({
                       name: "scoped_orders",
                       schema: {
                          fields: [
                             {
                                name: "cat",
                                kind: "dimension",
                                type: { kind: "string_type" },
                             },
                          ],
                       },
                    }),
                 ],
              },
   }),
);
const getDashboard = mock(() =>
   Promise.resolve({
      data: { path: "dashboards/overview.malloy", givens: [], tiles: [] },
   }),
);
const listDashboards = mock(() =>
   Promise.resolve({ data: [{ name: "overview" }, { name: "regions" }] }),
);
const executeQueryModel = mock(() => pending());
// What the package publishes: the catalog is built from these, not from
// whatever file the dashboard imports.
const listModels = mock(() =>
   Promise.resolve({ data: [{ path: "data_app.malloy" }] }),
);
mockServerProvider({
   models: { getModel, executeQueryModel, listModels },
   dashboards: { getDashboard, listDashboards },
});

// Imported after the stub is registered: a static import would hoist above it.
const { DashboardEditor } = await import("./DashboardEditor");

const DRAFT = {
   workspace: "Local",
   type: "dashboard" as const,
   path: "env/pkg/dashboards/overview.malloy",
};

const mount = (
   onExit?: () => void,
   onEvent?: (event: DashboardEvent) => void,
) => {
   const storage = new BrowserDocumentStorage();
   render(
      <DocumentStorageProvider documentStorage={storage}>
         <DashboardEditor
            environmentName="env"
            packageName="pkg"
            dashboardName="overview"
            {...(onExit ? { onExit } : {})}
            {...(onEvent ? { onEvent } : {})}
         />
      </DocumentStorageProvider>,
      { wrapper: serverWrapper },
   );
   return storage;
};

const button = (name: string | RegExp) =>
   screen.getByRole("button", { name, hidden: true });

beforeEach(() => {
   clearCache();
   localStorage.clear();
});

describe("DashboardEditor", () => {
   it("opens the package file in the builder, with the package's sources to pick from", async () => {
      mount();
      expect(await screen.findByText("Storefront")).toBeDefined();
      expect(screen.getByLabelText("Tile by_cat")).toBeDefined();
      // The catalog came from the published models, and the file's import of
      // scoped_orders is what keeps it on offer.
      await waitFor(() =>
         expect(listModels.mock.calls.length).toBeGreaterThan(0),
      );
      await waitFor(() =>
         expect(
            getModel.mock.calls.some((call) => call[2] === "data_app.malloy"),
         ).toBe(true),
      );
      await screen.findByRole("button", { name: "Add tile", hidden: true });
   });

   it("saves an edit into the browser and marks it saved", async () => {
      const storage = mount();
      await screen.findByText("Storefront");
      fireEvent.click(screen.getByLabelText("Settings for By category"));
      fireEvent.change(screen.getByLabelText("Tile title"), {
         target: { value: "Categories" },
      });
      fireEvent.keyDown(screen.getByLabelText("Tile title"), { key: "Escape" });
      fireEvent.click(button("Save changes"));
      await waitFor(async () =>
         expect(await storage.getDocument(DRAFT)).toContain(
            '# label="Categories"',
         ),
      );
      expect(await storage.getDocument(DRAFT)).toContain("# colspan=6");
      // The copy just written is not "edits from an earlier visit": no offer
      // to resume it appears over the page being edited.
      await waitFor(() => expect(button("Saved")).toBeDefined());
      expect(screen.queryByText(/saved in this browser/)).toBeNull();
   });

   it("offers a saved draft that differs from the package, and opens it on Resume", async () => {
      const draft = PACKAGE_FILE.replace(
         'title="Storefront"',
         'title="Drafted"',
      );
      await new BrowserDocumentStorage().saveDocument(DRAFT, draft);
      const onEvent = mock((_event: DashboardEvent) => {});
      mount(undefined, onEvent);
      expect(
         await screen.findByText(
            /edits to this dashboard saved in this browser/,
         ),
      ).toBeDefined();
      // Until the reader chooses, the package file is what is open.
      expect(await screen.findByText("Storefront")).toBeDefined();
      fireEvent.click(button("Resume"));
      expect(await screen.findByText("Drafted")).toBeDefined();
      expect(onEvent.mock.calls.at(-1)?.[0]).toMatchObject({
         type: "dashboard.opened",
         from: "draft",
      });
      expect(screen.queryByText(/saved in this browser/)).toBeNull();
   });

   it("hands the exit to the host and reports the open", async () => {
      const onExit = mock(() => {});
      const onEvent = mock((_event: DashboardEvent) => {});
      mount(onExit, onEvent);
      await screen.findByText("Storefront");
      await waitFor(() =>
         expect(onEvent.mock.calls.map((call) => call[0].type)).toContain(
            "dashboard.opened",
         ),
      );
      expect(onEvent.mock.calls[0][0]).toMatchObject({
         type: "dashboard.opened",
         from: "package",
         tiles: 1,
      });
      fireEvent.click(button("Done editing"));
      expect(onExit).toHaveBeenCalledTimes(1);
   });
});
