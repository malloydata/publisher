// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// DEPRECATED FEATURE COVERAGE — delete together with
// `.malloynb` notebooks (see RELEASE_NOTES deprecations). Every test in
// this file is about that deprecated feature.

import { expect, test, type Page } from "@playwright/test";
import { PACKAGES } from "./helpers/fixtures";
import {
   exampleFixture,
   publishPackageEnv,
   type PackageEnv,
} from "./helpers/packageEnv";

/**
 * `.malloynb` is deprecated and the example packages no longer ship one, so
 * this suite adds the notebook it reads about to a throwaway copy of each
 * package. The viewer stays supported, so the coverage stays.
 */
const FIXTURE_NOTEBOOK = "test_package_notebook.malloynb";
const README_NOTEBOOK = "README.malloynb";
const README_SOURCE = `>>>markdown
# Readme notebook

>>>malloy
import "storefront.malloy"

>>>malloy
run: order_items -> top_products
`;
const GOVERNED_NOTEBOOK = "test_governed_notebook.malloynb";
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

/** Every API request the page makes, as `METHOD /path` after the package. */
function apiRequests(page: Page): string[] {
   const sent: string[] = [];
   page.on("request", (request) => {
      const { pathname } = new URL(request.url());
      const at = pathname.indexOf("/packages/");
      if (pathname.startsWith("/api/v0/") && at >= 0) {
         const rest = pathname.slice(at).split("/").slice(3).join("/");
         sent.push(`${request.method()} /${rest}`);
      }
   });
   return sent;
}

let storefront: PackageEnv;
let governed: PackageEnv;

test.describe("package-notebooks", () => {
   // eslint-disable-next-line no-empty-pattern
   test.beforeAll(async ({}, testInfo) => {
      const baseURL = testInfo.project.use.baseURL ?? "http://localhost:4000";
      storefront = await publishPackageEnv(
         baseURL,
         "pkgnb",
         exampleFixture(PACKAGES.storefront),
         PACKAGES.storefront,
         {
            [FIXTURE_NOTEBOOK]: NOTEBOOK_SOURCE,
            [README_NOTEBOOK]: README_SOURCE,
         },
      );
      governed = await publishPackageEnv(
         baseURL,
         "pkgnbgov",
         exampleFixture(PACKAGES.governed),
         PACKAGES.governed,
         { [GOVERNED_NOTEBOOK]: GOVERNED_SOURCE },
      );
   });

   test.afterAll(async () => {
      await storefront?.dispose();
      await governed?.dispose();
   });

   test("Reports section lists .malloynb files", async ({ page }) => {
      await page.goto(`/${storefront.path}`);

      // The section label is the only user-visible string the Pages/Console
      // rename changes, and no other test reads it, so a relabel that misses a
      // surface leaves the suite green. Assert the heading before the absence
      // check: toHaveCount(0) is already satisfied while the page is blank, so
      // on its own it would pin nothing.
      await expect(
         page.getByRole("heading", { name: "Reports", exact: true }),
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
      await page.goto(`/${storefront.path}`);

      await page.getByText(FIXTURE_NOTEBOOK, { exact: true }).click();

      // The notebook opens on its own route; assert we navigated off the package route.
      await expect(page).not.toHaveURL(new RegExp(`/${storefront.path}/?$`));
      await expect(page).toHaveURL(/test_package_notebook\.malloynb/);
   });

   test("the notebook view renders authored content, its cells run through the query endpoint", async ({
      page,
   }) => {
      const sent = apiRequests(page);
      await page.goto(`/${storefront.path}`);
      await page.getByText(FIXTURE_NOTEBOOK, { exact: true }).click();
      // The fixture's own H1, and a result per cell that runs.
      await expect(
         page.getByRole("heading", { name: "A notebook", level: 1 }),
      ).toBeVisible();
      await expect(page.locator(".malloy-render")).toHaveCount(3, {
         timeout: 30_000,
      });
      // Each `run:` cell is a query against the notebook's own path; the
      // import cell sends nothing, and the deprecated notebook routes are
      // never read.
      expect(
         sent.filter((request) =>
            request.endsWith(`POST /models/${FIXTURE_NOTEBOOK}/query`),
         ),
      ).toHaveLength(3);
      expect(sent.filter((request) => request.includes("/notebooks"))).toEqual(
         [],
      );
   });

   test("a root README.malloynb renders on the package page, its cell run", async ({
      page,
   }) => {
      const sent = apiRequests(page);
      await page.goto(`/${storefront.path}`);
      await expect(
         page.getByRole("heading", { name: "Readme notebook", level: 1 }),
      ).toBeVisible({ timeout: 60_000 });
      await expect(page.locator(".malloy-render")).toHaveCount(1, {
         timeout: 30_000,
      });
      await expect(page.getByText(/render it with Notebook/)).toHaveCount(0);
      expect(
         sent.filter((request) =>
            request.endsWith(`POST /models/${README_NOTEBOOK}/query`),
         ),
      ).toHaveLength(1);
   });

   test("clicking a notebook row keeps the package segment in the URL", async ({
      page,
   }) => {
      await page.goto(`/${governed.path}`);

      // The fixture's name shares nothing with its package, so a dropped
      // package segment lands on /<env>/<file> and 404s. The storefront
      // case above cannot catch that: its URL matches either way.
      await page.getByText(GOVERNED_NOTEBOOK, { exact: true }).click();

      await expect(page).toHaveURL(
         new RegExp(`/${governed.path}/${GOVERNED_NOTEBOOK}$`),
      );
      await expect(page.getByText(/does not exist/i)).toHaveCount(0);
   });

   test("a table cell is as tall as its table, not as tall as the cell cap", async ({
      page,
   }) => {
      await page.goto(`/${storefront.path}/test_package_notebook.malloynb`);

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
