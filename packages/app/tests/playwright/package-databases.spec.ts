import { expect, test } from "@playwright/test";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";

test.describe("package-databases — embedded", () => {
   test("Package Data section is visible with at least one parquet row", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.imdb);

      // Redesigned package page lists databases under a "Package Data"
      // section as flat rows (no <table>). The h6 heading + at least one
      // .parquet row are the load-bearing affordances.
      await expect(
         page.getByRole("heading", { name: "Package Data", level: 6 }),
      ).toBeVisible();
      await expect(page.getByText(/\.parquet/).first()).toBeVisible();
   });

   test("clicking a parquet row opens a schema dialog that can close", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.imdb);

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
