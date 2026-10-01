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

/**
 * A dashboard created empty, on a server that takes writes: once a save puts
 * its first tile in the package the server serves it, so the manifest (and the
 * controls it carries) is fetched then, once.
 */
const PACKAGE_FILE = `## artifact { title="Storefront" tiles=["a -> by_cat"] } dashboard { columns=12 }
import { scoped_orders } from "../data_app.malloy"

source: a is scoped_orders extend {
  # colspan=6
  # label="By category"
  view: by_cat is by_category
}`;

const served = PACKAGE_FILE.replace('tiles=["a -> by_cat"]', "tiles=[]");

const getModel = mock((_env: string, _pkg: string, path: string) =>
   path === "broken.malloy"
      ? Promise.reject(new Error("reloading"))
      : Promise.resolve({
           data:
              path === "dashboards/overview.malloy"
                 ? { modelPath: path, sourceText: served }
                 : {
                      modelPath: path,
                      sources: [
                         {
                            name: "scoped_orders",
                            views: [
                               { name: "by_category" },
                               { name: "by_brand" },
                            ],
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
      data: {
         path: "dashboards/overview.malloy",
         givens: [],
         tiles: [],
      },
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
const listDashboards = mock(() =>
   Promise.resolve({ data: [{ name: "overview" }, { name: "regions" }] }),
);
const executeQueryModel = mock(() => pending());
// What the package publishes: the catalog is built from these, not from
// whatever file the dashboard imports.
const listModels = mock(() =>
   Promise.resolve({ data: [{ path: "data_app.malloy" }] }),
);
mockServerProvider(
   {
      models: { getModel, executeQueryModel, listModels, updateModelSource },
      dashboards: { getDashboard, listDashboards },
   },
   { mutable: true },
);

// Imported after the stub is registered: a static import would hoist above it.
const { DashboardEditor } = await import("./DashboardEditor");

beforeEach(() => {
   clearCache();
   getDashboard.mockClear();
   updateModelSource.mockClear();
});

describe("DashboardEditor, starting empty", () => {
   it("fetches the manifest once, after the save that gives it its first tile", async () => {
      render(
         <DashboardEditor
            environmentName="env"
            packageName="pkg"
            dashboardName="overview"
         />,
         { wrapper: serverWrapper },
      );
      await screen.findByText(/not served until it has a tile/);
      expect(getDashboard).not.toHaveBeenCalled();

      fireEvent.click(
         (
            await screen.findAllByRole("button", {
               name: "Add tile",
               hidden: true,
            })
         )[0],
      );
      fireEvent.click(await screen.findByLabelText("View by_brand"));
      fireEvent.click(screen.getByRole("button", { name: "Add tile" }));
      fireEvent.click(
         screen.getByRole("button", { name: "Save changes", hidden: true }),
      );
      fireEvent.click(await screen.findByRole("button", { name: "Save this" }));

      await waitFor(() => expect(updateModelSource).toHaveBeenCalledTimes(1));
      await waitFor(() => expect(getDashboard).toHaveBeenCalledTimes(1));
      // Settled, and not asked again by a later render.
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(getDashboard).toHaveBeenCalledTimes(1);
   });
});
