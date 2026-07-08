import { expect, test } from "@playwright/test";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";

test.describe("notebook-readme-links", () => {
   // Regression test for F07. Links inside a rendered README/notebook are
   // authored relative to the package (e.g. `spielberg.malloynb`). A plain
   // browser anchor resolved them against the current URL, dropped the package
   // segment, and 404'd with a misleading "Package manifest ... does not exist".
   // They must route within the package instead. The existing package-notebooks
   // spec only covers the notebook-list path, which already worked.
   test("clicking a README link opens the notebook inside its package", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.imdb);

      // imdb's README.malloynb renders a table of contents linking to the
      // package's notebooks. Clicking one must stay within the imdb package.
      const link = page.getByRole("link", { name: "Steven Spielberg" });
      await expect(link).toBeVisible({ timeout: 60_000 });
      await link.click();

      await expect(page).toHaveURL(
         new RegExp(`/${DEFAULT_ENV}/${PACKAGES.imdb}/spielberg\\.malloynb`),
      );
      // The spielberg notebook renders its authored H1, proving it loaded
      // rather than the "does not exist" error the dropped-package route gave.
      await expect(
         page.getByRole("heading", { name: "Steven Spielberg", level: 1 }),
      ).toBeVisible();
   });
});
