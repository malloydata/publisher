// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test, Page } from "@playwright/test";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";

// .first() is defensive: the package page yields exactly one exact match for a
// model path today, in the Models section.
function modelRow(page: Page, name: string) {
   return page.getByText(name, { exact: true }).first();
}

test.describe("package-models", () => {
   test("Models section lists .malloy files", async ({ page }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.storefront);

      await expect(modelRow(page, "storefront.malloy")).toBeVisible();
   });

   test("opening a model routes to /:env/:pkg/:model and loads Sources", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.storefront);

      await modelRow(page, "storefront.malloy").click();
      await expect(page).toHaveURL(
         new RegExp(
            `/${DEFAULT_ENV}/${PACKAGES.storefront}/storefront\\.malloy$`,
         ),
      );
      await expect(
         page.getByRole("heading", { name: "Sources", level: 1 }),
      ).toBeVisible();
   });

   test("lists each source with its fields", async ({ page }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.storefront);
      await modelRow(page, "storefront.malloy").click();

      // Each source is a section named by its own heading.
      const customers = page.getByRole("region", {
         name: "customers",
         exact: true,
      });
      await expect(customers).toBeVisible();
      await expect(
         customers.getByText("full_name", { exact: true }),
      ).toBeVisible();
      await expect(
         customers.getByText("customer_count", { exact: true }),
      ).toBeVisible();
      await expect(
         page.getByRole("region", { name: "order_items", exact: true }),
      ).toBeVisible();
   });

   test("running a named query returns rows", async ({ page }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.storefront);
      await modelRow(page, "storefront.malloy").click();

      await expect(
         page.getByRole("heading", { name: "Named Queries", level: 2 }),
      ).toBeVisible();
      // brand_suggest is the model's only named query: distinct brands, in
      // order, so the first two are the fixture data's first two.
      await page.getByRole("button", { name: "Run Query" }).click();
      await expect(page.getByText("Alpine", { exact: true })).toBeVisible();
      await expect(page.getByText("Aurora", { exact: true })).toBeVisible();
   });
});
