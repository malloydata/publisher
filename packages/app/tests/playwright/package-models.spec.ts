import { expect, test, Page } from "@playwright/test";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";

// Redesigned package page also renders the README.malloynb inline at the
// bottom, which contains the literal text "storefront.malloy" inside a <strong>
// tag. The flat-row label is rendered first in DOM order (Semantic Models
// section is above the README), so .first() reliably hits the row.
function modelRow(page: Page, name: string) {
   return page.getByText(name, { exact: true }).first();
}

test.describe("package-models", () => {
   test("Semantic Models section lists .malloy files", async ({ page }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.storefront);

      await expect(modelRow(page, "storefront.malloy")).toBeVisible();
   });

   test("opening a model routes to /:env/:pkg/:model and loads Sources", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.storefront);

      await modelRow(page, "storefront.malloy").click();
      await expect(page).toHaveURL(
         new RegExp(
            `/${DEFAULT_ENV}/${PACKAGES.storefront}/storefront\\.malloy$`,
         ),
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
      await openPackage(page, DEFAULT_ENV, PACKAGES.storefront);
      await modelRow(page, "storefront.malloy").click();

      // MUI Autocomplete: input value, not innerText.
      await expect(page.getByRole("combobox").first()).toHaveValue("customers");
   });

   test("picking a dimension and running returns rows", async ({ page }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.storefront);
      await modelRow(page, "storefront.malloy").click();

      const sourceSelect = page.getByRole("combobox").first();
      await expect(sourceSelect).toHaveValue("customers");
      await page.keyboard.press("Escape");

      const dimensionsToggle = page.getByText("Dimensions5");
      await expect(dimensionsToggle).toBeVisible();
      await dimensionsToggle.scrollIntoViewIfNeeded();
      await dimensionsToggle.click();

      const nameField = page.getByText("full_name", { exact: true });
      await expect(nameField).toBeVisible();
      await nameField.scrollIntoViewIfNeeded();
      // force: label sits under an icon button; Playwright otherwise reports pointer interception.
      await nameField.click({ force: true });

      const runQuery = page.getByRole("button", { name: "Run", exact: true });
      await expect(runQuery).toBeEnabled();
      await runQuery.click();

      await expect(page.getByRole("tab", { name: "Results" })).toBeVisible();
      const resultsPanel = page.getByRole("tabpanel", { name: "Results" });
      await expect(resultsPanel).toContainText(
         /Aiden Ali|Aiden Andersson|Aiden Cohen|Aiden Diaz/,
      );
   });
});
