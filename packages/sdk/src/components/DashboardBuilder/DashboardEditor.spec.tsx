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
mockServerProvider({
   models: { getModel, executeQueryModel },
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
      // The catalog came from the model the file imports, by its resolved path.
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
   });

   it("offers a saved draft that differs from the package, and opens it on Resume", async () => {
      const draft = PACKAGE_FILE.replace(
         'title="Storefront"',
         'title="Drafted"',
      );
      await new BrowserDocumentStorage().saveDocument(DRAFT, draft);
      mount();
      expect(
         await screen.findByText(
            /edits to this dashboard saved in this browser/,
         ),
      ).toBeDefined();
      // Until the reader chooses, the package file is what is open.
      expect(await screen.findByText("Storefront")).toBeDefined();
      fireEvent.click(button("Resume"));
      expect(await screen.findByText("Drafted")).toBeDefined();
   });

   it("exports the file a save would write, hands Done to the host, and reports both opening and exporting", async () => {
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
      const created: Blob[] = [];
      const createObjectURL = mock((blob: Blob) => {
         created.push(blob);
         return "blob:test";
      });
      const revoked = mock(() => {});
      Object.assign(URL, { createObjectURL, revokeObjectURL: revoked });
      const clicked = mock(() => {});
      const click = HTMLAnchorElement.prototype.click;
      HTMLAnchorElement.prototype.click = clicked;
      try {
         fireEvent.click(button("Export"));
         await waitFor(() => expect(created).toHaveLength(1));
         expect(await created[0].text()).toBe(PACKAGE_FILE);
         expect(clicked).toHaveBeenCalledTimes(1);
         expect(revoked).toHaveBeenCalledWith("blob:test");
         expect(onEvent.mock.calls.at(-1)?.[0]).toEqual({
            type: "dashboard.exported",
            bytes: PACKAGE_FILE.length,
         });
      } finally {
         HTMLAnchorElement.prototype.click = click;
      }
      fireEvent.click(button("Done"));
      expect(onExit).toHaveBeenCalledTimes(1);
   });
});
