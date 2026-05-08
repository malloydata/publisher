import { expect, test } from "@playwright/test";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";

test.describe("package-notebooks", () => {
   test("Notebooks section lists .malloynb files", async ({ page }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.imdb);

      await expect(
         page.getByRole("treeitem", { name: "imdb.malloynb", exact: true }),
      ).toBeVisible();
      await expect(
         page.getByRole("treeitem", { name: "README.malloynb", exact: true }),
      ).toBeVisible();
   });

   test("opening a notebook routes into the workbook view", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.imdb);

      await page
         .getByRole("treeitem", { name: "imdb.malloynb", exact: true })
         .click();

      // Router uses a workbook-scoped path; assert we navigated off the package route.
      await expect(page).not.toHaveURL(
         new RegExp(`/${DEFAULT_ENV}/${PACKAGES.imdb}/?$`),
      );
      await expect(page).toHaveURL(/imdb\.malloynb/);
   });

   test("workbook renders authored content from the notebook", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.imdb);
      await page
         .getByRole("treeitem", { name: "imdb.malloynb", exact: true })
         .click();
      // The imdb.malloynb renders an authored H1 ("IMDB Analysis in Malloy") —
      // presence confirms the Workbook mounted and executed the notebook.
      await expect(
         page.getByRole("heading", {
            name: "IMDB Analysis in Malloy",
            level: 1,
         }),
      ).toBeVisible();
   });
});
