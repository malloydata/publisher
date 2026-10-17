// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
   TEST_SERVER,
} from "../../../test/serverProvider";
import { globalQueryClient } from "../../utils/queryClient";
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
import { editInline } from "./testing/inline";

const getModel = mock((_env: string, _pkg: string, path: string) =>
   Promise.resolve({
      data:
         path === "dashboards/overview.malloy"
            ? { modelPath: path, sourceText: PACKAGE_FILE }
            : { modelPath: path, sources: [], sourceInfos: [] },
   }),
);
const updateModelSource = mock(
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
         updateModelSource,
      },
      reports: {
         getReport: mock(() =>
            Promise.resolve({
               data: {
                  path: "dashboards/overview.malloy",
                  givens: [],
                  tiles: [],
               },
            }),
         ),
         listReports: mock(() =>
            Promise.resolve({
               data: [
                  {
                     path: "dashboards/overview.malloy",
                     name: "overview",
                     layout: "grid",
                  },
               ],
            }),
         ),
      },
   },
   { mutable: true },
);

const { ReportEditor } = await import("./ReportEditor");

const DRAFT = {
   workspace: "Local",
   type: "grid" as const,
   path: "env/pkg/dashboards/overview.malloy",
};

const mount = () => {
   const storage = new BrowserDocumentStorage();
   render(
      <DocumentStorageProvider documentStorage={storage}>
         <ReportEditor
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
   updateModelSource.mockClear();
});

describe("ReportEditor, when the server takes writes", () => {
   it("saves into the package with the hash of the text it opened, and supersedes the browser draft", async () => {
      const storage = mount();
      await new BrowserDocumentStorage().saveDocument(DRAFT, "stale draft");
      await screen.findByText("Storefront");
      expect(
         document.querySelector('[data-saves-to="package"]'),
      ).not.toBeNull();
      // The ordinary case is not a line on the page; the status says it.
      expect(
         screen.queryByText("Save writes the file into the package."),
      ).toBeNull();

      editInline("By category", "Tile title", "Categories");
      fireEvent.click(button("Save"));

      await waitFor(() => expect(updateModelSource).toHaveBeenCalledTimes(1));
      const [env, pkg, path, body] = updateModelSource.mock.calls[0];
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
      updateModelSource.mockImplementationOnce(() =>
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
      editInline("By category", "Tile title", "Categories");
      fireEvent.click(button("Save"));
      await waitFor(() =>
         expect(screen.getByRole("alert").textContent).toContain(
            "changed in the package since you opened it",
         ),
      );
      expect(button("Save")).toBeDefined();
   });

   it("drops the cached results of the saved file, which are keyed on query text, not content", async () => {
      const key = (modelPath: string) => [
         "queryResult",
         "env",
         "pkg",
         undefined,
         modelPath,
         undefined,
         "run: a -> by_cat",
         undefined,
         "{}",
         TEST_SERVER,
      ];
      globalQueryClient.setQueryData(key("dashboards/overview.malloy"), "old");
      globalQueryClient.setQueryData(key("dashboards/other.malloy"), "other");
      mount();
      await screen.findByText("Storefront");
      editInline("By category", "Tile title", "Categories");
      fireEvent.click(button("Save"));
      await waitFor(() =>
         expect(
            globalQueryClient.getQueryState(key("dashboards/overview.malloy"))
               ?.isInvalidated,
         ).toBe(true),
      );
      expect(
         globalQueryClient.getQueryState(key("dashboards/other.malloy"))
            ?.isInvalidated,
      ).toBe(false);
   });
});

describe("ReportEditor writes the report file and nothing else", () => {
   // The whole report is the one file: its tiles, its queries, its controls
   // and their bindings. A session that retitles a tile, copies it and saves
   // writes that file, and never a model.
   it("writes only the report's own path, holding every setting itself", async () => {
      mount();
      await screen.findByText("Storefront");
      editInline("By category", "Tile title", "Categories");
      fireEvent.click(screen.getByLabelText("Settings for Categories"));
      fireEvent.click(
         await screen.findByRole("menuitem", {
            name: "Duplicate",
            hidden: true,
         }),
      );
      fireEvent.click(button("Save"));
      await waitFor(() => expect(updateModelSource).toHaveBeenCalled());
      const paths = updateModelSource.mock.calls.map((call) => call[2]);
      expect(new Set(paths)).toEqual(new Set(["dashboards/overview.malloy"]));
      const written = updateModelSource.mock.calls.at(-1)?.[3].source ?? "";
      expect(written).toContain(
         '# label="Categories"\n  view: by_cat is by_category',
      );
      expect(written).toContain(
         '\n\n# colspan=6\n# label="Categories"\nquery: by_category_tile is a -> by_category\n',
      );
   });
});
