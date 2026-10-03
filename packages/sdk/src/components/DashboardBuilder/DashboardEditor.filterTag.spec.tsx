// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";
import type { DashboardManifest } from "../../client";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";

/** The builder warns on a tile that ignores some of the page's controls, as the read view does. */
let packageFile = "";
let served: Partial<DashboardManifest> = {};

const CATEGORY = {
   name: "CATEGORY",
   label: "Category",
   type: "filter<string>",
};

const getModel = mock((_env: string, _pkg: string, path: string) =>
   Promise.resolve({
      data:
         path === "dashboards/overview.malloy"
            ? { modelPath: path, sourceText: packageFile }
            : { modelPath: path, sources: [], sourceInfos: [] },
   }),
);
const getDashboard = mock(() =>
   Promise.resolve({
      data: { path: "dashboards/overview.malloy", tiles: [], ...served },
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

async function warnings(): Promise<Record<string, string>> {
   render(
      <DashboardEditor
         environmentName="env"
         packageName="pkg"
         dashboardName="overview"
      />,
      { wrapper: serverWrapper },
   );
   await screen.findAllByLabelText(/^Tile /);
   const tags = await screen
      .findAllByTestId("tile-filter-tag", undefined, { timeout: 500 })
      .catch(() => []);
   return Object.fromEntries(
      tags.map((tag) => [
         tag.closest("[data-tile-key]")?.getAttribute("aria-label") ?? "",
         tag.textContent ?? "",
      ]),
   );
}

describe("DashboardEditor filter warnings", () => {
   it("warns only on the tile the control does not reach", async () => {
      packageFile = `## artifact { title="Storefront" tiles=["a -> by_cat", "a -> by_brand"] } dashboard { columns=12 }
import { scoped_orders } from "../data_app.malloy"

source: a is scoped_orders extend {
  view: by_cat is by_category + { where: cat ~ $CATEGORY }
  view: by_brand is by_brand_view
}`;
      served = { givens: [CATEGORY] };
      expect(await warnings()).toEqual({
         "Tile by_brand": "Doesn't respond to Category",
      });
   });

   it("does not warn on a tile its extension's own where: scopes", async () => {
      packageFile = `## artifact { title="Storefront" tiles=["a -> by_cat", "a -> by_brand"] } dashboard { columns=12 }
import { scoped_orders } from "../data_app.malloy"

source: a is scoped_orders extend {
  where: cat ~ $CATEGORY
  view: by_cat is by_category + { where: cat ~ $CATEGORY }
  view: by_brand is by_brand_view
}`;
      served = { givens: [CATEGORY] };
      expect(await warnings()).toEqual({});
   });

   it("does not warn about a filter just added on a local given the server has not compiled", async () => {
      packageFile = `## artifact { title="Storefront" tiles=["a -> by_cat", "a -> by_brand"] } dashboard { columns=12 }
import { scoped_orders } from "../data_app.malloy"

given: BRAND :: filter<string> is f''

source: a is scoped_orders extend {
  view: by_cat is by_category + { where: cat ~ $CATEGORY }
  view: by_brand is by_brand_view + { where: brand ~ $BRAND }
}`;
      // The served manifest predates BRAND.
      served = { givens: [CATEGORY] };
      expect(await warnings()).toEqual({
         "Tile by_cat": "Doesn't respond to BRAND",
         "Tile by_brand": "Doesn't respond to Category",
      });
   });

   it("reads an inherited tile's applicability from the served manifest", async () => {
      packageFile = `## artifact { title="Storefront" tiles=["a -> by_cat", "scoped_orders -> totals", "scoped_orders -> by_region"] } dashboard { columns=12 }
import { scoped_orders } from "../data_app.malloy"

source: a is scoped_orders extend {
  view: by_cat is by_category + { where: cat ~ $CATEGORY }
}`;
      served = {
         givens: [CATEGORY],
         tiles: [
            { kind: "query", query: "a -> by_cat", givenNames: ["CATEGORY"] },
            { kind: "query", query: "scoped_orders->totals", givenNames: [] },
            // Unresolved: the whole row applies.
            { kind: "query", query: "scoped_orders -> by_region" },
         ],
      };
      expect(await warnings()).toEqual({
         "Tile totals": "Doesn't respond to Category",
      });
   });
});
