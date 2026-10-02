// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";

/** The builder draws the read view's filter tag under each tile a control reaches. */
const PACKAGE_FILE = `## artifact { title="Storefront" tiles=["a -> by_cat", "a -> by_brand"] } dashboard { columns=12 }
import { scoped_orders } from "../data_app.malloy"

source: a is scoped_orders extend {
  view: by_cat is by_category + { where: cat ~ $CATEGORY }
  view: by_brand is by_brand_view
}`;

const getModel = mock((_env: string, _pkg: string, path: string) =>
   Promise.resolve({
      data:
         path === "dashboards/overview.malloy"
            ? { modelPath: path, sourceText: PACKAGE_FILE }
            : { modelPath: path, sources: [], sourceInfos: [] },
   }),
);
const getDashboard = mock(() =>
   Promise.resolve({
      data: {
         path: "dashboards/overview.malloy",
         givens: [{ name: "CATEGORY", label: "Category", type: "filter<string>" }],
         tiles: [],
      },
   }),
);
mockServerProvider(
   {
      models: {
         getModel,
         executeQueryModel: mock(() => pending()),
         listModels: mock(() => Promise.resolve({ data: [] })),
      },
      dashboards: {
         getDashboard,
         listDashboards: mock(() => Promise.resolve({ data: [] })),
      },
   },
   { mutable: true },
);

const { DashboardEditor } = await import("./DashboardEditor");

beforeEach(() => clearCache());

describe("DashboardEditor filter tags", () => {
   it("tags only the tile a control reaches, by the control's label", async () => {
      render(
         <DashboardEditor
            environmentName="env"
            packageName="pkg"
            dashboardName="overview"
         />,
         { wrapper: serverWrapper },
      );
      const tags = await screen.findAllByTestId("tile-filter-tag");
      expect(tags.map((tag) => tag.textContent)).toEqual(["Category"]);
      expect(tags[0].closest("[data-tile-key]")?.getAttribute("aria-label")).toBe(
         "Tile by_cat",
      );
   });
});
