// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";
import { openFilter } from "./helpers/filters";

/**
 * The storefront package ships `category-review.malloy` at its root, a linear
 * report of text and query tiles, so this spec reads the bundled example
 * rather than writing a fixture.
 * It opens at its path, without `.malloy`.
 */
test.describe("package-served-notebook", () => {
   test("opens from the package page and renders prose, a caption and a result", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.storefront);

      await page.getByText("Category review", { exact: true }).click();

      await expect(page).toHaveURL(
         new RegExp(`/${DEFAULT_ENV}/${PACKAGES.storefront}/category-review$`),
      );
      await expect(
         page.getByRole("heading", { name: "Category review", level: 1 }),
      ).toBeVisible({ timeout: 30_000 });
      await expect(
         page.getByText("Revenue by month for the selected category"),
      ).toBeVisible();
      await expect(page.locator(".malloy-render").first()).toBeVisible({
         timeout: 30_000,
      });
   });

   test("changing the Category control re-runs the tiles", async ({ page }) => {
      await page.goto(`/${DEFAULT_ENV}/${PACKAGES.storefront}/category-review`);

      // All three queries render; the last is the named-query table, bound to the control.
      const renders = page.locator(".malloy-render");
      await expect(renders).toHaveCount(3, { timeout: 30_000 });
      const table = renders.nth(2);
      await expect(table).toContainText("Jacket");
      await expect(table).not.toContainText("Denim");

      const filter = await openFilter(page, "Category");
      await filter.getByRole("combobox", { name: "Category" }).click();
      await page.getByRole("option", { name: "Jeans" }).click({
         timeout: 30_000,
      });

      await expect(page).toHaveURL(/[?&]CATEGORY=Jeans/);
      // Denim products only exist in the Jeans category.
      await expect(table).toContainText("Denim", { timeout: 30_000 });
      await expect(table).not.toContainText("Jacket");
   });

   test("an old notebooks/ link redirects to the report's path, keeping its query", async ({
      page,
   }) => {
      // The deprecated by-name alias: nothing is at notebooks/category-review.malloy any more.
      await page.goto(
         `/${DEFAULT_ENV}/${PACKAGES.storefront}/notebooks/category-review?CATEGORY=Jeans`,
      );
      await expect(page).toHaveURL(
         new RegExp(
            `/${DEFAULT_ENV}/${PACKAGES.storefront}/category-review\\?CATEGORY=Jeans$`,
         ),
         { timeout: 30_000 },
      );
      await expect(
         page.getByRole("heading", { name: "Category review", level: 1 }),
      ).toBeVisible({ timeout: 30_000 });
   });

   test("an old dashboards/ link opens the grid report of that name, not the linear one", async ({
      page,
   }) => {
      await page.goto(
         `/${DEFAULT_ENV}/${PACKAGES.storefront}/dashboards/overview`,
      );
      await expect(page).toHaveURL(
         new RegExp(`/${DEFAULT_ENV}/${PACKAGES.storefront}/overview$`),
         { timeout: 30_000 },
      );
   });
});
