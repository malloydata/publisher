import { expect, test } from "@playwright/test";
import { DEFAULT_ENV, tmpName } from "./helpers/fixtures";
import { gotoHome, openEnvironment } from "./helpers/navigation";
import { getPublisherStatus } from "./helpers/publisherStatus";

test.describe("environments — read", () => {
   test("Home loads with the Publisher heading", async ({ page }) => {
      await gotoHome(page);
   });

   test(`lists the ${DEFAULT_ENV} environment card`, async ({ page }) => {
      await gotoHome(page);
      await expect(
         page.getByRole("heading", { name: DEFAULT_ENV, level: 6 }),
      ).toBeVisible();
   });

   test("Open Environment navigates to /:environmentName", async ({ page }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
   });
});

test.describe("environments — mutable CRUD", () => {
   test.beforeAll(async ({}, testInfo) => {
      const baseURL = testInfo.project.use.baseURL ?? "http://localhost:4000";
      const { mutable } = await getPublisherStatus(baseURL);
      test.skip(
         !mutable,
         "publisher is read-only (frozenConfig or mutable=false)",
      );
   });

   test("Create New Environment button is present", async ({ page }) => {
      await gotoHome(page);
      await expect(
         page.getByRole("button", { name: "Create New Environment" }),
      ).toBeVisible();
   });

   test("environment card exposes Environment actions menu", async ({
      page,
   }) => {
      await gotoHome(page);
      const actions = page.getByRole("button", {
         name: `Environment actions for ${DEFAULT_ENV}`,
      });
      await expect(actions).toBeVisible();
      // Sticky `Publisher API` header link sits over the env card's top-right;
      // a regular (or force) click lands on the link and navigates away. Dispatch
      // the React onClick directly instead of going through the pointer pipeline.
      await actions.dispatchEvent("click");
      await expect(
         page.getByRole("menuitem", { name: /edit|delete/i }).first(),
      ).toBeVisible();
      await page.keyboard.press("Escape");
   });

   test("create → verify → edit → verify → delete disposable environment", async ({
      page,
   }) => {
      const name = tmpName("env");
      const description = `created by playwright at ${new Date().toISOString()}`;
      const editedDescription = `${description} [edited]`;

      await gotoHome(page);

      // --- Create ---
      await page
         .getByRole("button", { name: "Create New Environment" })
         .click();
      const createDialog = page.getByRole("dialog", {
         name: "Create New Environment",
      });
      await expect(createDialog).toBeVisible();
      await createDialog.getByLabel("Environment Name").fill(name);
      await createDialog
         .getByLabel("Environment Description")
         .fill(description);
      await createDialog
         .getByRole("button", { name: "Create Environment" })
         .click();
      await expect(createDialog).toBeHidden();

      // Verify the new card (and its description) rendered on Home.
      const newCardHeading = page.getByRole("heading", { name, level: 6 });
      await expect(newCardHeading).toBeVisible();
      await expect(page.getByText(description)).toBeVisible();

      // --- Edit ---
      await page
         .getByRole("button", { name: `Environment actions for ${name}` })
         .dispatchEvent("click");
      await page.getByRole("menuitem", { name: "Edit" }).click();
      const editDialog = page.getByRole("dialog", { name: "Edit Environment" });
      await expect(editDialog).toBeVisible();
      const descField = editDialog.getByLabel("Environment Description");
      await descField.fill(editedDescription);
      await editDialog.getByRole("button", { name: "Save Changes" }).click();
      await expect(editDialog).toBeHidden();
      await expect(page.getByText(editedDescription)).toBeVisible();

      // --- Delete ---
      await page
         .getByRole("button", { name: `Environment actions for ${name}` })
         .dispatchEvent("click");
      await page.getByRole("menuitem", { name: "Delete" }).click();
      const deleteDialog = page.getByRole("dialog", {
         name: "Delete Environment",
      });
      await expect(deleteDialog).toBeVisible();
      await expect(deleteDialog).toContainText(name);
      await deleteDialog.getByRole("button", { name: "Delete" }).click();
      await expect(deleteDialog).toBeHidden();
      await expect(newCardHeading).toHaveCount(0);
   });
});

test.describe("environments — mutability parity with /api/v0/status", () => {
   test("Create/Edit controls render iff the publisher reports mutable", async ({
      page,
   }, testInfo) => {
      const baseURL = testInfo.project.use.baseURL ?? "http://localhost:4000";
      const { mutable } = await getPublisherStatus(baseURL);
      const expected = mutable ? 1 : 0;

      await gotoHome(page);
      await expect(
         page.getByRole("button", { name: "Create New Environment" }),
      ).toHaveCount(expected);
      await expect(
         page.getByRole("button", {
            name: `Environment actions for ${DEFAULT_ENV}`,
         }),
      ).toHaveCount(expected);
   });
});
