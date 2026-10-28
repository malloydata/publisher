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

   test("opening a model routes to /:env/:pkg/:model and opens a source", async ({
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
      await expect(page.getByRole("heading", { level: 1 })).toBeVisible();
   });

   test("lists a source's fields, and switches source from its picker", async ({
      page,
   }) => {
      await page.goto(
         `/${DEFAULT_ENV}/${PACKAGES.storefront}/storefront.malloy?source=customers`,
      );
      await expect(
         page.getByRole("heading", { name: "customers", level: 1 }),
      ).toBeVisible();
      await expect(
         page
            .getByRole("region", { name: "Dimensions" })
            .getByText("full_name", { exact: true }),
      ).toBeVisible();
      await expect(
         page
            .getByRole("region", { name: "Measures" })
            .getByText("customer_count", { exact: true }),
      ).toBeVisible();

      // The source is in the address, so the picker's choice is a link to it.
      await page.getByRole("button", { name: /\d+ sources/ }).click();
      await page
         .getByRole("button", { name: /^Open source order_items/ })
         .click();
      await expect(
         page.getByRole("heading", { name: "order_items", level: 1 }),
      ).toBeVisible();
      await expect(page).toHaveURL(/[?&]source=order_items\b/);
   });

   test("running a named query returns rows", async ({ page }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.storefront);
      await modelRow(page, "storefront.malloy").click();

      // brand_suggest is the model's only named query: distinct brands, in
      // order, so the first two are the fixture data's first two. A query
      // runs when its row is opened.
      const queries = page.getByRole("region", { name: "Queries" });
      await queries.getByRole("button", { name: /brand_suggest/ }).click();
      await expect(queries.getByText("Alpine", { exact: true })).toBeVisible();
      await expect(queries.getByText("Aurora", { exact: true })).toBeVisible();
   });
});
