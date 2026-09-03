// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";

test.describe("package-notebooks", () => {
   test("Notebooks section lists .malloynb files", async ({ page }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.storefront);

      // The section label is the only user-visible string the Pages/Console
      // rename changes, and no other test reads it, so a relabel that misses a
      // surface leaves the suite green. Assert the heading before the absence
      // check: toHaveCount(0) is already satisfied while the page is blank, so
      // on its own it would pin nothing.
      await expect(
         page.getByRole("heading", { name: "Notebooks" }),
      ).toBeVisible();
      await expect(
         page.getByRole("heading", { name: "Governed Reports" }),
      ).toHaveCount(0);

      await expect(
         page.getByText("storefront.malloynb", { exact: true }),
      ).toBeVisible();
   });

   test("opening a notebook routes into the workbook view", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.storefront);

      await page.getByText("storefront.malloynb", { exact: true }).click();

      // Router uses a workbook-scoped path; assert we navigated off the package route.
      await expect(page).not.toHaveURL(
         new RegExp(`/${DEFAULT_ENV}/${PACKAGES.storefront}/?$`),
      );
      await expect(page).toHaveURL(/storefront\.malloynb/);
   });

   test("workbook renders authored content from the notebook", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.storefront);
      await page.getByText("storefront.malloynb", { exact: true }).click();
      // The storefront.malloynb renders an authored H1 ("Storefront — a guided
      // tour"): presence confirms the Workbook mounted and executed the notebook.
      await expect(
         page.getByRole("heading", {
            name: "Storefront — a guided tour",
            level: 1,
         }),
      ).toBeVisible();
   });

   test("clicking a notebook row keeps the package segment in the URL", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.governed);

      // orders.malloynb shares no name overlap with the governed-analytics
      // package, so a dropped package segment lands on /examples/orders.malloynb
      // and 404s. The storefront.malloynb case above cannot catch that (the
      // URL still matches either way).
      await page.getByText("orders.malloynb", { exact: true }).click();

      await expect(page).toHaveURL(
         new RegExp(`/${DEFAULT_ENV}/${PACKAGES.governed}/orders\\.malloynb$`),
      );
      await expect(page.getByText(/does not exist/i)).toHaveCount(0);
   });

   test("a table cell is as tall as its table, not as tall as the cell cap", async ({
      page,
   }) => {
      await page.goto(
         `/${DEFAULT_ENV}/${PACKAGES.storefront}/storefront.malloynb`,
      );

      // Every notebook table, measured against the box the cell gives it. The
      // renderer signals ready before a table's virtualized grid has settled,
      // so the cell used to latch onto the height it read at that moment --
      // scrollHeight 10028 for a four-row table -- clamp it to the 700px cap
      // and paint ~390px of blank space under the rows. Asserted on the ratio
      // rather than on a pixel count so it survives a row-height or font
      // change, and over every table on the page rather than the first,
      // because the first one to settle is not deterministic.
      const tables = page.locator(".malloy-render > .malloy-table.root");
      await expect(tables.first()).toBeVisible();
      await expect
         .poll(
            () =>
               tables.evaluateAll((nodes) =>
                  nodes
                     .map((node) => {
                        const table = node as HTMLElement;
                        const box = table.parentElement as HTMLElement;
                        return {
                           table: table.offsetHeight,
                           box: box.offsetHeight,
                        };
                     })
                     // Only the ones with dead space left under the table, so a
                     // failure names the measurements rather than saying false.
                     .filter(
                        ({ table, box }) => table === 0 || box - table > 4,
                     ),
               ),
            { timeout: 30_000 },
         )
         .toEqual([]);
   });
});
