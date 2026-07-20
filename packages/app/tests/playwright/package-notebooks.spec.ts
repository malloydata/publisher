import { expect, test } from "@playwright/test";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";

test.describe("package-notebooks", () => {
   test("Notebooks section lists .malloynb files", async ({ page }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.storefront);

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
      await openPackage(page, DEFAULT_ENV, PACKAGES.imdb);

      // README.malloynb shares no name overlap with the package, so a dropped
      // package segment lands on /malloy-samples/README.malloynb and 404s. The
      // imdb.malloynb case above cannot catch that (the URL still matches).
      await page
         .getByRole("button", { name: "README.malloynb", exact: true })
         .click();

      await expect(page).toHaveURL(
         new RegExp(`/${DEFAULT_ENV}/${PACKAGES.imdb}/README\\.malloynb$`),
      );
      await expect(page.getByText(/does not exist/i)).toHaveCount(0);
   });
});
