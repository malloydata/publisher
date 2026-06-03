import { expect, test } from "@playwright/test";
import * as fs from "fs/promises";
import * as path from "path";
import { fileURLToPath } from "url";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";

/**
 * End-to-end coverage for the Notebook "Parameters" panel that surfaces
 * Malloy `given:` runtime parameters. The publisher's malloy-samples
 * fixture set doesn't ship a model with `given:` declarations, so the
 * spec writes its own .malloy + .malloynb into the `faa` package directory
 * before the suite runs, triggers a package reload, and cleans up after.
 */

const FIXTURE_MODEL = "test_givens.malloy";
const FIXTURE_NOTEBOOK = "test_givens_notebook.malloynb";

const FAA_DIR = path.resolve(
   path.dirname(fileURLToPath(import.meta.url)),
   "../../../server/publisher_data/malloy-samples/faa",
);

const MODEL_SOURCE = `##! experimental.givens

#(description="Two-letter IATA carrier code")
given: target_code :: string is 'WN'
given: cutoff :: date is @2024-01-01

source: carriers_with_given is duckdb.table('data/carriers.parquet') extend {
  primary_key: code
  measure: carrier_count is count()

  view: by_code is {
    where: code = $target_code
    select: code, name, nickname
    limit: 1
  }
}
`;

const NOTEBOOK_SOURCE = `>>>markdown
# Test Givens Notebook

>>>malloy
import "test_givens.malloy"

>>>malloy
run: carriers_with_given -> by_code
`;

async function reloadFaaPackage(baseURL: string): Promise<void> {
   const url = `${baseURL}/api/v0/environments/${DEFAULT_ENV}/packages/${PACKAGES.faa}?reload=true`;
   const res = await fetch(url);
   if (!res.ok) {
      throw new Error(`Package reload failed: ${res.status} ${res.statusText}`);
   }
}

test.describe("notebook-givens", () => {
   test.beforeAll(async ({ baseURL }) => {
      await fs.writeFile(path.join(FAA_DIR, FIXTURE_MODEL), MODEL_SOURCE);
      await fs.writeFile(path.join(FAA_DIR, FIXTURE_NOTEBOOK), NOTEBOOK_SOURCE);
      await reloadFaaPackage(baseURL!);
   });

   test.afterAll(async ({ baseURL }) => {
      await fs.unlink(path.join(FAA_DIR, FIXTURE_MODEL)).catch(() => undefined);
      await fs
         .unlink(path.join(FAA_DIR, FIXTURE_NOTEBOOK))
         .catch(() => undefined);
      await reloadFaaPackage(baseURL!).catch(() => undefined);
   });

   async function openGivensNotebook(page: import("@playwright/test").Page) {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.faa);
      await page.getByText(FIXTURE_NOTEBOOK, { exact: true }).click();
      await expect(page).toHaveURL(/test_givens_notebook\.malloynb/);
   }

   test("server surfaces declared givens on the notebook response", async ({
      baseURL,
   }) => {
      // Sanity-check the wire shape before driving the UI: if this fails,
      // the test fixture is wrong or the introspection regressed, not the
      // SDK widgets.
      const url = `${baseURL}/api/v0/environments/${DEFAULT_ENV}/packages/${PACKAGES.faa}/notebooks/${FIXTURE_NOTEBOOK}`;
      const res = await fetch(url);
      expect(res.ok).toBe(true);
      const body = (await res.json()) as {
         sources?: Array<{ name?: string; givens?: Array<{ name?: string }> }>;
      };
      const givens = body.sources?.[0]?.givens ?? [];
      const names = givens.map((g) => g.name);
      expect(names).toContain("target_code");
      expect(names).toContain("cutoff");
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
   });

   test("description annotation surfaces as helper text", async ({ page }) => {
      await openGivensNotebook(page);

      await expect(
         page.getByText("Two-letter IATA carrier code", { exact: true }),
      ).toBeVisible();
   });

   test("default given value drives the initial cell result", async ({
      page,
   }) => {
      await openGivensNotebook(page);

      // Default `target_code` is 'WN' → carrier row should be Southwest Airlines.
      await expect(page.getByText("Southwest Airlines").first()).toBeVisible();
   });

   test("typing a value updates the cell result and × clears it", async ({
      page,
   }) => {
      await openGivensNotebook(page);
      const input = page.getByLabel("target_code");

      await input.fill("AA");
      await expect(page.getByText("American Airlines").first()).toBeVisible();

      const clearBtn = page.getByRole("button", { name: "clear value" });
      await expect(clearBtn).toBeVisible();
      await clearBtn.click();

      // After clear, the input is empty and the cell reverts to the default.
      await expect(input).toHaveValue("");
      await expect(page.getByText("Southwest Airlines").first()).toBeVisible();
   });

   test("Reset button clears all given values", async ({ page }) => {
      await openGivensNotebook(page);
      const input = page.getByLabel("target_code");

      await input.fill("DL");
      const resetBtn = page.getByRole("button", { name: "Reset" });
      await expect(resetBtn).toBeVisible();
      await resetBtn.click();

      await expect(input).toHaveValue("");
      await expect(resetBtn).toBeHidden();
      await expect(page.getByText("Southwest Airlines").first()).toBeVisible();
   });
});
