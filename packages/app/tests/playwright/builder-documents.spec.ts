// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test, type Page } from "@playwright/test";
import {
   exampleFixture,
   registerPackageEnv,
   serverFixture,
   type PackageEnv,
} from "./helpers/packageEnv";
import { saveChanges } from "./helpers/save";

/**
 * Documents the builders open or create in shapes the happy path skips: a
 * notebook on a curated package, a dashboard with no tile yet, and a filter on
 * a field reached through a join.
 */

const baseOf = (info: { project: { use: { baseURL?: string } } }) =>
   info.project.use.baseURL ?? "http://localhost:4000";

const editing = (page: Page) =>
   expect(page.getByRole("button", { name: "Undo" })).toBeVisible({
      timeout: 60_000,
   });

test.describe("curated package notebook", () => {
   let pe: PackageEnv;
   test.beforeAll(async ({}, testInfo) => {
      pe = await registerPackageEnv(
         baseOf(testInfo),
         "surface",
         serverFixture("notebooks-malloyyo-surface"),
         "notebooks-malloyyo-surface",
      );
   });
   test.afterAll(async () => {
      await pe?.dispose();
   });

   test("a notebook with its own sources opens in the editor instead of waiting on the package's text", async ({
      page,
   }) => {
      await page.goto(`/${pe.env}/${pe.pkg}/notebooks/local/edit`);
      await editing(page);
      await expect(page.getByText("Opening the notebook…")).toHaveCount(0);
      await expect(page.getByText("A source over the surface")).toBeVisible();
   });
});

test.describe("dashboard with no tile", () => {
   let pe: PackageEnv;
   test.beforeAll(async ({}, testInfo) => {
      pe = await registerPackageEnv(
         baseOf(testInfo),
         "empty",
         serverFixture("dashboards-test"),
         "dashboards-test",
         {
            "dashboards/blank.malloy": `##! experimental.givens
## artifact { title="Blank start" tiles=[] }
import { orders } from '../orders.malloy'
`,
         },
      );
   });
   test.afterAll(async () => {
      await pe?.dispose();
   });

   test("opens with the empty state, takes a first tile, and then serves", async ({
      page,
   }) => {
      await page.goto(`/${pe.env}/${pe.pkg}/dashboards/blank/edit`);
      await editing(page);
      await expect(
         page.getByText("This dashboard is not served until it has a tile."),
      ).toBeVisible();

      await page.locator("button", { hasText: /^Add tile$/ }).click();
      const dialog = page.getByRole("dialog", { name: "Add a tile" });
      await dialog.getByRole("button", { name: "View totals" }).click();
      await dialog
         .getByRole("button", { name: "Add tile", exact: true })
         .click();
      await expect(page.getByLabel(/^Tile /)).toHaveCount(1);

      await saveChanges(page);
      expect(await pe.readSource("dashboards/blank.malloy")).not.toContain(
         "tiles=[]",
      );

      await page.goto(`/${pe.env}/${pe.pkg}/dashboards/blank`);
      await expect(page.locator("[data-malloy-render-as]").first()).toBeVisible(
         {
            timeout: 60_000,
         },
      );
   });
});

test.describe("joined filter", () => {
   let pe: PackageEnv;
   test.beforeAll(async ({}, testInfo) => {
      pe = await registerPackageEnv(
         baseOf(testInfo),
         "joined",
         exampleFixture("storefront"),
         "storefront",
      );
   });
   test.afterAll(async () => {
      await pe?.dispose();
   });

   test("a filter on a joined dimension is added, loads its options, saves quoted, and the package still loads", async ({
      page,
   }) => {
      await page.goto(`/${pe.env}/${pe.pkg}/dashboards/overview/edit`);
      await editing(page);
      await page.getByRole("button", { name: "Add filter" }).click();
      const dialog = page.getByRole("dialog");
      const field = dialog.getByRole("combobox", { name: "Field to filter" });
      await field.fill("customers.state");
      await page
         .getByRole("option", { name: /customers\.state/ })
         .first()
         .click();
      await dialog
         .getByRole("button", { name: "Add filter", exact: true })
         .click();

      // The new control's picker asks the server for its values.
      const control = page.getByRole("combobox", { name: /state/i }).first();
      await expect(control).toBeVisible({ timeout: 30_000 });
      await control.click();
      await expect(page.getByRole("option").first()).toBeVisible({
         timeout: 30_000,
      });
      await page.keyboard.press("Escape");

      await saveChanges(page);
      expect(await pe.readSource("dashboards/overview.malloy")).toContain(
         'dimension="customers.state"',
      );

      const status = (await fetch(`${pe.baseURL}/api/v0/status`).then((r) =>
         r.json(),
      )) as { loadErrors?: Array<{ package?: string; error?: string }> };
      expect(
         (status.loadErrors ?? []).filter((e) => e.package === pe.pkg),
      ).toEqual([]);

      // The reader serves the saved file with the control in place.
      await page.goto(`/${pe.env}/${pe.pkg}/dashboards/overview`);
      await expect(
         page.getByRole("combobox", { name: /state/i }).first(),
      ).toBeVisible({
         timeout: 60_000,
      });
   });
});
