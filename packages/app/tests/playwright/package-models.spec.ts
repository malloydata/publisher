import { expect, test, Page } from "@playwright/test";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";

// Redesigned package page also renders the README.malloynb inline at the
// bottom, which contains the literal text "imdb.malloy" inside a <strong>
// tag. The flat-row label is rendered first in DOM order (Semantic Models
// section is above the README), so .first() reliably hits the row.
function modelRow(page: Page, name: string) {
   return page.getByText(name, { exact: true }).first();
}

test.describe("package-models", () => {
   test("Semantic Models section lists .malloy files", async ({ page }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.imdb);

      await expect(modelRow(page, "imdb.malloy")).toBeVisible();
   });

   test("opening a model routes to /:env/:pkg/:model and loads Sources", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.imdb);

      await modelRow(page, "imdb.malloy").click();
      await expect(page).toHaveURL(
         new RegExp(`/${DEFAULT_ENV}/${PACKAGES.imdb}/imdb\\.malloy$`),
      );
      await expect(
         page.getByRole("heading", { name: "Sources", level: 1 }),
      ).toBeVisible();
   });

   test("source combobox defaults to a known source value", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.imdb);
      await modelRow(page, "imdb.malloy").click();

      // MUI Autocomplete: input value, not innerText.
      await expect(page.getByRole("combobox").first()).toHaveValue("people");
   });

   test("picking a dimension and running returns rows", async ({ page }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.imdb);
      await modelRow(page, "imdb.malloy").click();

      const sourceSelect = page.getByRole("combobox").first();
      await expect(sourceSelect).toHaveValue("people");
      await page.keyboard.press("Escape");

      const dimensionsToggle = page.getByText("Dimensions5");
      await expect(dimensionsToggle).toBeVisible();
      await dimensionsToggle.scrollIntoViewIfNeeded();
      await dimensionsToggle.click();

      const primaryNameField = page.getByText("primaryName", { exact: true });
      await expect(primaryNameField).toBeVisible();
      await primaryNameField.scrollIntoViewIfNeeded();
      // force: label sits under an icon button; Playwright otherwise reports pointer interception.
      await primaryNameField.click({ force: true });

      const runQuery = page.getByRole("button", { name: "Run", exact: true });
      await expect(runQuery).toBeEnabled();
      await runQuery.click();

      await expect(page.getByRole("tab", { name: "Results" })).toBeVisible();
      const resultsPanel = page.getByRole("tabpanel", { name: "Results" });
      await expect(resultsPanel).toContainText(
         /(?:'|’)?A\.J\.(?:'|’)? Marriot|50 Cent|A Martinez|A Winged Victory for the Sullen|A\. Kitman Ho|A\. Michael Baldwin|A\. Sreekar Prasad|A\.A\. Milne/,
      );
   });
});
