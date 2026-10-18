// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import {
   editorOpen,
   pickChart,
   queryTiles,
   readerOpen,
} from "./helpers/builder";
import {
   exampleFixture,
   registerPackageEnv,
   type PackageEnv,
} from "./helpers/packageEnv";
import { saveChanges } from "./helpers/save";

/**
 * "What clicking a value does", set in the builder and then clicked in the
 * report: the tile's entry is what the reader's click menu reads, and the
 * dialog's preview is what that menu shows.
 */

let pe: PackageEnv;

test.describe("builder click actions", () => {
   // eslint-disable-next-line no-empty-pattern
   test.beforeAll(async ({}, testInfo) => {
      pe = await registerPackageEnv(
         testInfo.project.use.baseURL ?? "http://localhost:4000",
         "clicks",
         exampleFixture("storefront"),
         "storefront",
      );
   });
   test.afterAll(async () => {
      await pe?.dispose();
   });

   const openClickActions = async (
      page: import("@playwright/test").Page,
      title: RegExp,
   ) => {
      const tile = queryTiles(page).filter({ hasText: title }).first();
      await tile.scrollIntoViewIfNeeded();
      // The dialog reads the tile's values off its result: wait for it to draw.
      await expect(
         tile.locator(".malloy-render, [data-malloy-render-as]").first(),
      ).toBeVisible({
         timeout: 60_000,
      });
      await tile.getByLabel(/^Settings for /).click();
      await page
         .getByRole("menuitem", { name: /^What clicking a value does/ })
         .click();
      return page.getByRole("dialog");
   };

   test("a tile over a model dimension offers it, and asks which filter a value sets when none is named like it", async ({
      page,
   }) => {
      await page.goto(`/${pe.env}/${pe.pkg}/overview?edit`);
      await editorOpen(page);
      const dialog = await openClickActions(page, /Top products/);
      await expect(dialog).toContainText(
         "When someone clicks a value in Top products",
      );
      // The report has text filters but none for Product: the box is offered,
      // and nothing is guessed until the author picks which filter it sets.
      const self = dialog.getByRole("checkbox", {
         name: "Filter this report by Product",
      });
      await expect(self).toBeEnabled();
      await self.check();
      await expect(dialog).toContainText(
         "Choose the filter a Product value sets.",
      );
      await expect(dialog.getByLabel("Preview")).not.toContainText(
         "Filter this report to",
      );
      await expect(dialog).toContainText(/Show rows for /);
      await dialog.getByRole("button", { name: "Cancel" }).click();
   });

   test("set Filter this report in the builder, and a click in the report offers it, as the preview said", async ({
      page,
   }) => {
      await page.goto(`/${pe.env}/${pe.pkg}/overview?edit`);
      await editorOpen(page);
      // A chart's bars report no click, so show the tile as a table first: that
      // also makes it a query of the report's own, the form #1443 writes.
      await pickChart(
         page,
         queryTiles(page)
            .filter({ hasText: /Revenue by category/ })
            .first(),
         "Table",
      );
      const dialog = await openClickActions(page, /Revenue by category/);
      const self = dialog.getByRole("checkbox", {
         name: "Filter this report by Category",
      });
      await expect(self).toBeEnabled();
      if (!(await self.isChecked())) await self.check();
      const preview = dialog.getByLabel("Preview");
      const first = (await preview.locator("p").first().textContent()) ?? "";
      const value = /Clicking "(.*)" shows:/.exec(first)?.[1] ?? "";
      expect(value).not.toBe("");
      const previewed = await preview.locator("li").allTextContents();
      expect(previewed[0]).toBe(`Filter this report to ${value}`);
      await dialog.getByRole("button", { name: "Apply" }).click();
      await saveChanges(page);

      await page.getByRole("button", { name: "Done" }).click();
      await readerOpen(page);
      const tile = page
         .locator("[data-tile-kind]")
         .filter({ hasText: "Revenue by category" })
         .first();
      await tile.scrollIntoViewIfNeeded();
      await tile.getByText(value, { exact: true }).first().click();
      await expect(page.getByRole("menuitem")).toHaveText(previewed, {
         timeout: 30_000,
      });
   });
});
