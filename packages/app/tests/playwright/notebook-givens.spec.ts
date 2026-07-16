import { expect, test } from "@playwright/test";
import * as fs from "fs/promises";
import * as path from "path";
import { fileURLToPath } from "url";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";

/**
 * End-to-end coverage for the Notebook "Parameters" panel that surfaces
 * Malloy `given:` runtime parameters. The publisher's storefront
 * fixture set doesn't ship a model with `given:` declarations, so the
 * spec writes its own .malloy + .malloynb into the `storefront` package directory
 * before the suite runs, triggers a package reload, and cleans up after.
 *
 * Run this against a normal server, not one started with `--watch-env examples`:
 * watch mode symlinks PKG_DIR to the tracked `examples/storefront` sources, so
 * the fixture writes below would land in version control.
 */

const FIXTURE_MODEL = "test_givens.malloy";
const FIXTURE_NOTEBOOK = "test_givens_notebook.malloynb";

const PKG_DIR = path.resolve(
   path.dirname(fileURLToPath(import.meta.url)),
   "../../../server/publisher_data/examples/storefront",
);

const MODEL_SOURCE = `##! experimental.givens

#(description="Product category to spotlight")
given: target_code :: string is 'Jeans'
given: cutoff :: date is @2024-01-01
given: include_x :: boolean is true

source: products_with_given is duckdb.table('data/products.parquet') extend {
  primary_key: product_id
  measure: product_count is count()

  view: by_code is {
    where: category = $target_code
    select: product_id, name, brand
    order_by: product_id
    limit: 1
  }
}
`;

const NOTEBOOK_SOURCE = `>>>markdown
# Test Givens Notebook

>>>malloy
import "test_givens.malloy"

>>>malloy
run: products_with_given -> by_code
`;

async function reloadPackage(baseURL: string): Promise<void> {
   const url = `${baseURL}/api/v0/environments/${DEFAULT_ENV}/packages/${PACKAGES.storefront}?reload=true`;
   const res = await fetch(url);
   if (!res.ok) {
      throw new Error(`Package reload failed: ${res.status} ${res.statusText}`);
   }
}

test.describe("notebook-givens", () => {
   test.beforeAll(async ({ baseURL }) => {
      await fs.writeFile(path.join(PKG_DIR, FIXTURE_MODEL), MODEL_SOURCE);
      await fs.writeFile(path.join(PKG_DIR, FIXTURE_NOTEBOOK), NOTEBOOK_SOURCE);
      await reloadPackage(baseURL!);
   });

   test.afterAll(async ({ baseURL }) => {
      await fs.unlink(path.join(PKG_DIR, FIXTURE_MODEL)).catch(() => undefined);
      await fs
         .unlink(path.join(PKG_DIR, FIXTURE_NOTEBOOK))
         .catch(() => undefined);
      await reloadPackage(baseURL!).catch(() => undefined);
   });

   async function openGivensNotebook(page: import("@playwright/test").Page) {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.storefront);
      await page.getByText(FIXTURE_NOTEBOOK, { exact: true }).click();
      await expect(page).toHaveURL(/test_givens_notebook\.malloynb/);
   }

   test("server surfaces declared givens on the notebook response", async ({
      baseURL,
   }) => {
      // Sanity-check the wire shape before driving the UI: if this fails,
      // the test fixture is wrong or the introspection regressed, not the
      // SDK widgets.
      const url = `${baseURL}/api/v0/environments/${DEFAULT_ENV}/packages/${PACKAGES.storefront}/notebooks/${FIXTURE_NOTEBOOK}`;
      const res = await fetch(url);
      expect(res.ok).toBe(true);
      const body = (await res.json()) as {
         sources?: Array<{ name?: string; givens?: Array<{ name?: string }> }>;
      };
      const givens = body.sources?.[0]?.givens ?? [];
      const names = givens.map((g) => g.name);
      expect(names).toContain("target_code");
      expect(names).toContain("cutoff");
      expect(names).toContain("include_x");
   });

   test("Parameters panel renders one input per declared given", async ({
      page,
   }) => {
      await openGivensNotebook(page);

      await expect(
         page.getByRole("heading", { name: "Parameters", level: 6 }),
      ).toBeVisible();
      await expect(page.getByLabel("target_code")).toBeVisible();
      await expect(page.getByLabel("cutoff")).toBeVisible();
      await expect(page.getByLabel("include_x")).toBeVisible();
   });

   test("description annotation surfaces as helper text", async ({ page }) => {
      await openGivensNotebook(page);

      // Not exact: the helper line now also carries the `Default: …` caption.
      await expect(
         page.getByText("Product category to spotlight", { exact: false }),
      ).toBeVisible();
   });

   test("model default surfaces as a helper line", async ({ page }) => {
      await openGivensNotebook(page);

      // target_code defaults to 'Jeans' (string literal unquoted) and cutoff to
      // @2024-01-01 (date, @ stripped). Both shown as a Default caption while
      // the inputs stay empty — the value still comes from the model default.
      await expect(
         page.getByText("Default: Jeans", { exact: false }),
      ).toBeVisible();
      await expect(
         page.getByText("Default: 2024-01-01", { exact: false }),
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
      const input = page.getByLabel("target_code");

      await input.fill("Tops");
      await expect(page.getByText("Aurora Boxy Blouse").first()).toBeVisible();

      const clearBtn = page.getByRole("button", { name: "clear value" });
      await expect(clearBtn).toBeVisible();
      await clearBtn.click();

      // After clear, the input is empty and the cell reverts to the default.
      await expect(input).toHaveValue("");
      await expect(page.getByText("Cobalt Bootcut Jean").first()).toBeVisible();
   });

   test("Reset button clears all given values", async ({ page }) => {
      await openGivensNotebook(page);
      const input = page.getByLabel("target_code");

      await input.fill("Shorts");
      const resetBtn = page.getByRole("button", { name: "Reset" });
      await expect(resetBtn).toBeVisible();
      await resetBtn.click();

      await expect(input).toHaveValue("");
      await expect(resetBtn).toBeHidden();
      await expect(page.getByText("Cobalt Bootcut Jean").first()).toBeVisible();
   });

   test("empty string is a deliberate override, distinct from revert-to-default", async ({
      page,
   }) => {
      await openGivensNotebook(page);
      const input = page.getByLabel("target_code");

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
      const clearBtn = page.getByRole("button", { name: "clear value" });
      await expect(clearBtn).toBeVisible();
      await clearBtn.click();
      await expect(page.getByText("Cobalt Bootcut Jean").first()).toBeVisible();
   });

   test("boolean reflects the default when unset, and toggles/reverts as an explicit override", async ({
      page,
   }) => {
      await openGivensNotebook(page);
      const box = page.getByLabel("include_x");

      // Unset → reflects the model default (`is true`), so the box shows what
      // the query actually runs with. No override yet, so no Reset.
      await expect(box).toBeChecked();
      await expect(page.getByRole("button", { name: "Reset" })).toBeHidden();

      // Toggle → explicit `false` override (a real value, not "unset").
      await box.uncheck();
      await expect(box).not.toBeChecked();
      await expect(page.getByRole("button", { name: "Reset" })).toBeVisible();

      // Revert (×) → drop the override, back to the default (checked).
      await page.getByRole("button", { name: "clear value" }).click();
      await expect(box).toBeChecked();
      await expect(page.getByRole("button", { name: "Reset" })).toBeHidden();
   });
});
