// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";

/**
 * The storefront package ships `notebooks/category-review.malloy`, a served
 * notebook (a `.malloy` with a `kind=notebook` artifact tag), so this spec
 * reads the bundled example rather than writing a fixture.
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
         new RegExp(
            `/${DEFAULT_ENV}/${PACKAGES.storefront}/notebooks/category-review$`,
         ),
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

   test("changing the Category control re-runs the cells", async ({ page }) => {
      await page.goto(
         `/${DEFAULT_ENV}/${PACKAGES.storefront}/notebooks/category-review`,
      );

      // All three queries render; the last is the named-query table, bound to the control.
      const renders = page.locator(".malloy-render");
      await expect(renders).toHaveCount(3, { timeout: 30_000 });
      const table = renders.nth(2);
      await expect(table).toContainText("Jacket");
      await expect(table).not.toContainText("Denim");

      await page.getByRole("combobox", { name: "Category" }).click();
      await page.getByRole("option", { name: "Jeans" }).click({
         timeout: 30_000,
      });

      await expect(page).toHaveURL(/[?&]CATEGORY=Jeans/);
      // Denim products only exist in the Jeans category.
      await expect(table).toContainText("Denim", { timeout: 30_000 });
      await expect(table).not.toContainText("Jacket");
   });
});
