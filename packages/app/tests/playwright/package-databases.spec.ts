// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";

test.describe("package-databases — embedded", () => {
   test("Data section is visible with at least one parquet row", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.storefront);

      // Redesigned package page lists databases under a "Data"
      // section as flat rows (no <table>). The h6 heading + at least one
      // .parquet row are the load-bearing affordances.
      await expect(
         page.getByRole("heading", { name: "Data", level: 6, exact: true }),
      ).toBeVisible();
      await expect(page.getByText(/\.parquet/).first()).toBeVisible();
   });

   test("clicking a parquet row opens a schema dialog that can close", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.storefront);

      await page
         .getByText(/\.parquet/)
         .first()
         .click();

      const dialog = page.getByRole("dialog");
      await expect(dialog).toBeVisible();
      await dialog.getByRole("button", { name: "close" }).click();
      await expect(dialog).toHaveCount(0);
   });
});
