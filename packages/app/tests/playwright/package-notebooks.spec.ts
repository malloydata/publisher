// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";

/**
 * `.malloynb` is deprecated and the example packages no longer ship one, so
 * this suite writes the notebook it reads about and removes it afterwards. The
 * viewer stays supported, so the coverage stays.
 */
const FIXTURE_NOTEBOOK = "test_package_notebook.malloynb";
const PKG_DIR = path.resolve(
   path.dirname(fileURLToPath(import.meta.url)),
   "../../../server/publisher_data/examples/storefront",
);
const GOVERNED_NOTEBOOK = "test_governed_notebook.malloynb";
const GOVERNED_DIR = path.resolve(
   path.dirname(fileURLToPath(import.meta.url)),
   "../../../server/publisher_data/examples/governed-analytics",
);
const GOVERNED_SOURCE = `>>>markdown
# Governed notebook

>>>malloy
import "orders.malloy"

>>>malloy
run: sales -> by_region
`;
// Three cells, all of views with no render tag, because the cell-height test
// below counts the tables a notebook paints.
const NOTEBOOK_SOURCE = `>>>markdown
# A notebook

>>>malloy
import "storefront.malloy"

>>>malloy
run: order_items -> top_products

>>>malloy
run: order_items -> top_customers

>>>malloy
run: order_items -> category_performance
`;

async function reloadPackage(
   baseURL: string,
   pkg: string = PACKAGES.storefront,
): Promise<void> {
   const url = `${baseURL}/api/v0/environments/${DEFAULT_ENV}/packages/${pkg}?reload=true`;
   const res = await fetch(url);
   if (!res.ok) throw new Error(`Package reload failed: ${res.status}`);
}

test.describe("package-notebooks", () => {
   test.beforeAll(async ({ baseURL }) => {
      await fs.writeFile(path.join(PKG_DIR, FIXTURE_NOTEBOOK), NOTEBOOK_SOURCE);
      await fs.writeFile(
         path.join(GOVERNED_DIR, GOVERNED_NOTEBOOK),
         GOVERNED_SOURCE,
      );
      await reloadPackage(baseURL!);
      await reloadPackage(baseURL!, PACKAGES.governed);
   });

   test.afterAll(async ({ baseURL }) => {
      await fs
         .unlink(path.join(PKG_DIR, FIXTURE_NOTEBOOK))
         .catch(() => undefined);
      await fs
         .unlink(path.join(GOVERNED_DIR, GOVERNED_NOTEBOOK))
         .catch(() => undefined);
      await reloadPackage(baseURL!).catch(() => undefined);
      await reloadPackage(baseURL!, PACKAGES.governed).catch(() => undefined);
   });

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
         page.getByText(FIXTURE_NOTEBOOK, { exact: true }),
      ).toBeVisible();
   });

   test("opening a notebook routes into the notebook view", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.storefront);

      await page.getByText(FIXTURE_NOTEBOOK, { exact: true }).click();

      // The notebook opens on its own route; assert we navigated off the package route.
      await expect(page).not.toHaveURL(
         new RegExp(`/${DEFAULT_ENV}/${PACKAGES.storefront}/?$`),
      );
      await expect(page).toHaveURL(/test_package_notebook\.malloynb/);
   });

   test("the notebook view renders authored content", async ({ page }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.storefront);
      await page.getByText(FIXTURE_NOTEBOOK, { exact: true }).click();
      // The fixture's own H1: its presence confirms the Notebook mounted and
      // ran the cells.
      await expect(
         page.getByRole("heading", { name: "A notebook", level: 1 }),
      ).toBeVisible();
   });

   test("clicking a notebook row keeps the package segment in the URL", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.governed);

      // The fixture's name shares nothing with its package, so a dropped
      // package segment lands on /examples/<file> and 404s. The storefront
      // case above cannot catch that: its URL matches either way.
      await page.getByText(GOVERNED_NOTEBOOK, { exact: true }).click();

      await expect(page).toHaveURL(
         new RegExp(
            `/${DEFAULT_ENV}/${PACKAGES.governed}/${GOVERNED_NOTEBOOK}$`,
         ),
      );
      await expect(page.getByText(/does not exist/i)).toHaveCount(0);
   });

   test("a table cell is as tall as its table, not as tall as the cell cap", async ({
      page,
   }) => {
      await page.goto(
         `/${DEFAULT_ENV}/${PACKAGES.storefront}/test_package_notebook.malloynb`,
      );

      // Every notebook table, measured against the box the cell gives it: a
      // cell must end up as tall as its table, not as tall as the 700px cap.
      // Asserted on the ratio rather than on a pixel count so it survives a
      // row-height or font change, and over every table on the page rather
      // than the first, because the first one to settle is not deterministic.
      const tables = page.locator(".malloy-render > .malloy-table.root");
      // Pin the count before measuring anything. Notebook cells render
      // progressively, and the poll below succeeds the moment nothing it can
      // see has dead space, so without this a run where only one table has
      // painted is a pass - and that is the run least likely to reproduce the
      // measurement race this test guards. Three is every untagged view in
      // test_package_notebook.malloynb: top_products, top_customers, and the two-row
      // top_products cell. Every other view carries a render tag, and the
      // dashboard's nested tables match neither .root nor the direct-child
      // step. Adding an untagged cell to that notebook means updating this.
      await expect(tables).toHaveCount(3);
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
