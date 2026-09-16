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
import { sha256Hex } from "../../utils/sha256";
import { BrowserDocumentStorage } from "../DocumentStorage/BrowserDocumentStorage";
import { DocumentStorageProvider } from "../DocumentStorage/DocumentStorageProvider";

/**
 * The editor against a server that takes writes: Save goes into the package,
 * carrying the hash of the text that was opened, and a browser draft of the
 * same file is superseded by it.
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
            : { modelPath: path, sources: [], sourceInfos: [] },
   }),
);
const putModelSource = mock(
   (_env: string, _pkg: string, path: string, body: { source: string }) =>
      Promise.resolve({
         data: {
            path,
            contentHash: `hash-of-${body.source.length}`,
            created: false,
         },
      }),
);
mockServerProvider(
   {
      models: {
         getModel,
         executeQueryModel: mock(() => pending()),
         putModelSource,
      },
      dashboards: {
         getDashboard: mock(() =>
            Promise.resolve({
               data: {
                  path: "dashboards/overview.malloy",
                  givens: [],
                  tiles: [],
               },
            }),
         ),
         listDashboards: mock(() =>
            Promise.resolve({ data: [{ name: "overview" }] }),
         ),
      },
   },
   { mutable: true },
);

const { DashboardEditor } = await import("./DashboardEditor");

const DRAFT = {
   workspace: "Local",
   type: "dashboard" as const,
   path: "env/pkg/dashboards/overview.malloy",
};

const mount = () => {
   const storage = new BrowserDocumentStorage();
   render(
      <DocumentStorageProvider documentStorage={storage}>
         <DashboardEditor
            environmentName="env"
            packageName="pkg"
            dashboardName="overview"
         />
      </DocumentStorageProvider>,
      { wrapper: serverWrapper },
   );
   return storage;
};
const button = (name: string) =>
   screen.getByRole("button", { name, hidden: true });

beforeEach(() => {
   clearCache();
   localStorage.clear();
   putModelSource.mockClear();
});

describe("DashboardEditor, when the server takes writes", () => {
   it("saves into the package with the hash of the text it opened, and supersedes the browser draft", async () => {
      const storage = mount();
      await new BrowserDocumentStorage().saveDocument(DRAFT, "stale draft");
      await screen.findByText("Storefront");
      expect(
         screen.getByText("Save writes the file into the package."),
      ).toBeDefined();

      fireEvent.click(screen.getByLabelText("Settings for By category"));
      fireEvent.change(screen.getByLabelText("Tile title"), {
         target: { value: "Categories" },
      });
      fireEvent.keyDown(screen.getByLabelText("Tile title"), { key: "Escape" });
      fireEvent.click(button("Save changes"));

      await waitFor(() => expect(putModelSource).toHaveBeenCalledTimes(1));
      const [env, pkg, path, body] = putModelSource.mock.calls[0];
      expect([env, pkg, path]).toEqual([
         "env",
         "pkg",
         "dashboards/overview.malloy",
      ]);
      expect(body.source).toContain('# label="Categories"');
      expect((body as { expectedHash?: string }).expectedHash).toBe(
         await sha256Hex(PACKAGE_FILE),
      );
      await waitFor(() => expect(button("Saved")).toBeDefined());
      await expect(storage.getDocument(DRAFT)).rejects.toBeDefined();
      // No offer to resume a draft over what was just written.
      expect(screen.queryByText(/saved in this browser/)).toBeNull();
   });

   it("keeps the edit and shows the server's reason when the package refuses the write", async () => {
      putModelSource.mockImplementationOnce(() =>
         Promise.reject({
            response: {
               data: {
                  message:
                     "`dashboards/overview.malloy` changed in the package since you opened it.",
               },
            },
         }),
      );
      mount();
      await screen.findByText("Storefront");
      fireEvent.click(screen.getByLabelText("Settings for By category"));
      fireEvent.change(screen.getByLabelText("Tile title"), {
         target: { value: "Categories" },
      });
      fireEvent.keyDown(screen.getByLabelText("Tile title"), { key: "Escape" });
      fireEvent.click(button("Save changes"));
      await waitFor(() =>
         expect(screen.getByRole("alert").textContent).toContain(
            "changed in the package since you opened it",
         ),
      );
      expect(button("Save changes")).toBeDefined();
   });
});
