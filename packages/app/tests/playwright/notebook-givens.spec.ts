// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import { PACKAGES } from "./helpers/fixtures";
import {
   closeFilter,
   expectFilterValue,
   filterButton,
   openFilter,
} from "./helpers/filters";
import {
   exampleFixture,
   publishPackageEnv,
   type PackageEnv,
} from "./helpers/packageEnv";

/**
 * End-to-end coverage for the Notebook "Filters" panel that surfaces
 * Malloy `given:` runtime parameters. The publisher's storefront
 * fixture set doesn't ship a model with `given:` declarations, so the
 * spec adds its own model and linear report to a throwaway copy of the
 * `storefront` package.
 */

const FIXTURE_MODEL = "test_givens.malloy";
const FIXTURE_NOTEBOOK = "test_givens_notebook.malloy";

const MODEL_SOURCE = `##! experimental.givens

#(description="Product category to spotlight")
given: target_code :: string is 'Jeans'
given: cutoff :: date is @2024-01-01
given: include_x :: boolean is true

source: products_with_given is duckdb.table('data/products.parquet') extend {
  primary_key: product_id
  measure: product_count is count()

  // The tile reads all three givens, so the report shows a control for each;
  // the last two conditions are always true and leave the rows alone.
  view: by_code is {
    where: category = $target_code
    where: $cutoff <= @2100-01-01
    where: $include_x or not $include_x
    select: product_id, name, brand
    order_by: product_id
    limit: 1
  }
}
`;

const NOTEBOOK_SOURCE = `##! experimental.givens
## artifact { layout=linear title="Test Givens Notebook" tiles=[intro { kind=text }, "products_with_given -> by_code"] }
import "test_givens.malloy"

##|(markdown) intro
The givens the model declares, as filters.
|##
`;

let pe: PackageEnv;

test.describe("notebook-givens", () => {
   // eslint-disable-next-line no-empty-pattern
   test.beforeAll(async ({}, testInfo) => {
      pe = await publishPackageEnv(
         testInfo.project.use.baseURL ?? "http://localhost:4000",
         "nbgivens",
         exampleFixture(PACKAGES.storefront),
         PACKAGES.storefront,
         { [FIXTURE_MODEL]: MODEL_SOURCE, [FIXTURE_NOTEBOOK]: NOTEBOOK_SOURCE },
      );
   });

   test.afterAll(async () => {
      await pe?.dispose();
   });

   async function openGivensNotebook(page: import("@playwright/test").Page) {
      await page.goto(`/${pe.path}`);
      await page.getByText(FIXTURE_NOTEBOOK, { exact: true }).click();
      await expect(page).toHaveURL(/\/test_givens_notebook$/);
   }

   test("server surfaces declared givens on the report response", async ({
      baseURL,
   }) => {
      // Sanity-check the wire shape before driving the UI: if this fails,
      // the test fixture is wrong or the introspection regressed, not the
      // SDK widgets.
      const url = `${baseURL}/api/v0/environments/${pe.env}/packages/${pe.pkg}/reports/${FIXTURE_NOTEBOOK}?versionId=${pe.versionId}`;
      const res = await fetch(url);
      expect(res.ok).toBe(true);
      // The report's control row: the givens its tiles read.
      const body = (await res.json()) as {
         givens?: Array<{ name?: string }>;
      };
      const names = (body.givens ?? []).map((g) => g.name);
      expect(names).toContain("target_code");
      expect(names).toContain("cutoff");
      expect(names).toContain("include_x");
   });

   test("the filter row has one button per declared given", async ({
      page,
   }) => {
      await openGivensNotebook(page);

      for (const name of ["target_code", "cutoff", "include_x"])
         await expect(filterButton(page, name)).toBeVisible();
      const filter = await openFilter(page, "target_code");
      await expect(
         filter.getByLabel("target_code", { exact: true }),
      ).toBeVisible();
   });

   test("description annotation surfaces as helper text", async ({ page }) => {
      await openGivensNotebook(page);
      const filter = await openFilter(page, "target_code");

      // Not exact: the helper line now also carries the `Default: …` caption.
      await expect(
         filter.getByText("Product category to spotlight", { exact: false }),
      ).toBeVisible();
   });

   test("model default surfaces on the button and as a helper line", async ({
      page,
   }) => {
      await openGivensNotebook(page);

      // target_code defaults to 'Jeans' (string literal unquoted) and cutoff to
      // @2024-01-01. Unset, each button reads its default, the value the
      // query runs with.
      await expectFilterValue(page, "target_code", "Jeans");
      await expectFilterValue(page, "cutoff", "Jan 1, 2024");
      const filter = await openFilter(page, "target_code");
      await expect(
         filter.getByText("Default: Jeans", { exact: false }),
      ).toBeVisible();
      await closeFilter(page);
      const cutoff = await openFilter(page, "cutoff");
      await expect(
         cutoff.getByText("Default: 2024-01-01", { exact: false }),
      ).toBeVisible();
   });

   test("default given value drives the initial cell result", async ({
      page,
   }) => {
      await openGivensNotebook(page);

      // Default `target_code` is 'Jeans' -> first Jeans product by id.
      await expect(page.getByText("Cobalt Bootcut Jean").first()).toBeVisible();
   });

   test("typing a value updates the cell result and × clears it", async ({
      page,
   }) => {
      await openGivensNotebook(page);
      const filter = await openFilter(page, "target_code");
      const input = filter.getByLabel("target_code", { exact: true });

      await input.fill("Tops");
      await expect(page.getByText("Aurora Boxy Blouse").first()).toBeVisible();

      const clearBtn = filter.getByRole("button", { name: "Clear value" });
      await expect(clearBtn).toBeVisible();
      await clearBtn.click();

      // After clear, the input is empty and the cell reverts to the default.
      await expect(input).toHaveValue("");
      await expect(page.getByText("Cobalt Bootcut Jean").first()).toBeVisible();
   });

   test("Reset button clears all given values", async ({ page }) => {
      await openGivensNotebook(page);
      const filter = await openFilter(page, "target_code");
      await filter.getByLabel("target_code", { exact: true }).fill("Shorts");
      await closeFilter(page);
      await expectFilterValue(page, "target_code", "Shorts");

      const resetBtn = page.getByRole("button", { name: "Reset" });
      await expect(resetBtn).toBeVisible();
      await resetBtn.click();

      await expectFilterValue(page, "target_code", "Jeans");
      await expect(resetBtn).toBeHidden();
      await expect(page.getByText("Cobalt Bootcut Jean").first()).toBeVisible();
   });

   test("empty string is a deliberate override, distinct from revert-to-default", async ({
      page,
   }) => {
      await openGivensNotebook(page);
      const filter = await openFilter(page, "target_code");
      const input = filter.getByLabel("target_code", { exact: true });

      // Default Jeans -> Cobalt Bootcut Jean.
      await expect(page.getByText("Cobalt Bootcut Jean").first()).toBeVisible();

      // Explicit Tops -> Aurora Boxy Blouse.
      await input.fill("Tops");
      await expect(page.getByText("Aurora Boxy Blouse").first()).toBeVisible();

      // Typing the field empty is an explicit "" override, NOT a revert: the
      // cell re-runs with "" (no category matches), so the Tops row disappears and
      // the default (Jeans/Cobalt Bootcut Jean) does not come back.
      await input.fill("");
      await expect(page.getByText("Aurora Boxy Blouse")).toHaveCount(0);
      await expect(page.getByText("Cobalt Bootcut Jean")).toHaveCount(0);

      // The × is present even though the field is empty — an override is active,
      // distinct from unset. Clicking it reverts to the model default.
      const clearBtn = filter.getByRole("button", { name: "Clear value" });
      await expect(clearBtn).toBeVisible();
      await clearBtn.click();
      await expect(page.getByText("Cobalt Bootcut Jean").first()).toBeVisible();
   });

   test("boolean reflects the default when unset, and toggles/reverts as an explicit override", async ({
      page,
   }) => {
      await openGivensNotebook(page);
      // Reset sits in the filter row, which an open popover hides from the
      // accessibility tree, so it is read with the popover closed.
      const reset = page.getByRole("button", { name: "Reset" });
      let filter = await openFilter(page, "include_x");
      let box = filter.getByLabel("include_x", { exact: true });

      // Unset → reflects the model default (`is true`), so the box shows what
      // the query actually runs with. No override yet, so no Reset.
      await expect(box).toBeChecked();
      await closeFilter(page);
      await expect(reset).toBeHidden();

      // Toggle → explicit `false` override (a real value, not "unset").
      filter = await openFilter(page, "include_x");
      box = filter.getByLabel("include_x", { exact: true });
      await box.uncheck();
      await expect(box).not.toBeChecked();
      await closeFilter(page);
      await expect(reset).toBeVisible();

      // Revert (×) → drop the override, back to the default (checked).
      filter = await openFilter(page, "include_x");
      await filter.getByRole("button", { name: "Clear value" }).click();
      await expect(
         filter.getByLabel("include_x", { exact: true }),
      ).toBeChecked();
      await closeFilter(page);
      await expect(reset).toBeHidden();
   });
});
