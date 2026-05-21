import { expect, test } from "@playwright/test";
import { DEFAULT_ENV } from "./helpers/fixtures";
import { gotoHome, openEnvironment } from "./helpers/navigation";
import { getPublisherStatus } from "./helpers/publisherStatus";

test.describe("environment-connections — read", () => {
   test("Connections section renders a card for each connection", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);

      await expect(
         page.getByRole("heading", { name: "Connections", level: 6 }),
      ).toBeVisible();
      // `bigquery` is the fixture connection present on every malloy-samples environment.
      const bigqueryCard = page.locator("h6", { hasText: "bigquery" });
      await expect(bigqueryCard).toBeVisible();
      await expect(page.getByText("BigQuery", { exact: true })).toBeVisible();
   });
});

test.describe("environment-connections — mutable", () => {
   test.beforeAll(async ({}, testInfo) => {
      const baseURL = testInfo.project.use.baseURL ?? "http://localhost:4000";
      const { mutable } = await getPublisherStatus(baseURL);
      test.skip(!mutable, "publisher is read-only");
   });

   test("Add Connection button is present in the section header", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);

      await expect(
         page.getByRole("button", { name: "Add Connection" }),
      ).toBeVisible();
   });

   test("kebab menu exposes Edit and Delete actions", async ({ page }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);

      await page
         .getByRole("button", { name: /Connection actions for bigquery/ })
         .click();

      await expect(
         page.getByRole("menuitem", { name: "Edit connection bigquery" }),
      ).toBeVisible();
      await expect(
         page.getByRole("menuitem", { name: "Delete connection bigquery" }),
      ).toBeVisible();
   });
});

test.describe("environment-connections — mutable CRUD", () => {
   test.beforeAll(async ({}, testInfo) => {
      const baseURL = testInfo.project.use.baseURL ?? "http://localhost:4000";
      const { mutable } = await getPublisherStatus(baseURL);
      test.skip(!mutable, "publisher is read-only");
   });

   test("Create → Edit → Delete a postgres connection", async ({ page }) => {
      test.setTimeout(60_000);
      const connName = `tmp_test_${Date.now()}`;

      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);

      // --- 1. Create ---
      await page.getByRole("button", { name: "Add Connection" }).click();
      const addDialog = page.getByRole("dialog", {
         name: "Create New Connection",
      });
      await expect(addDialog).toBeVisible();
      await addDialog.getByLabel("Connection Name").fill(connName);
      // Type defaults to postgres. Postgres requires either a Connection String
      // or all 5 detail fields — give it a connection string, which is the
      // shortest valid form.
      await addDialog
         .locator("input[name=connectionString]")
         .fill("postgres://test@localhost:5432/test");
      await addDialog
         .getByRole("button", { name: "Create Connection" })
         .click();
      await expect(addDialog).toBeHidden({ timeout: 15_000 });

      // Verify the card rendered with the right name + type label
      const card = page.locator("h6", { hasText: connName });
      await expect(card).toBeVisible();
      await expect(
         page.getByText("PostgreSQL", { exact: true }).first(),
      ).toBeVisible();

      // --- 2. Edit ---
      await page
         .getByRole("button", { name: `Connection actions for ${connName}` })
         .click();
      await page
         .getByRole("menuitem", { name: `Edit connection ${connName}` })
         .click();
      const editDialog = page.getByRole("dialog", { name: "Edit Connection" });
      await expect(editDialog).toBeVisible();
      // Change the host field to verify the round-trip works
      await editDialog.locator("input[name=host]").fill("edited-host.example");
      await editDialog.getByRole("button", { name: "Edit Connection" }).click();
      await expect(editDialog).toBeHidden({ timeout: 15_000 });
      // Card name + type label are unchanged, so it should still render
      await expect(card).toBeVisible();

      // --- 3. Delete ---
      await page
         .getByRole("button", { name: `Connection actions for ${connName}` })
         .click();
      await page
         .getByRole("menuitem", { name: `Delete connection ${connName}` })
         .click();
      const deleteDialog = page.getByRole("dialog", {
         name: "Delete Connection",
      });
      await expect(deleteDialog).toBeVisible();
      await expect(deleteDialog).toContainText(connName);
      await deleteDialog.getByRole("button", { name: "Delete" }).click();
      await expect(deleteDialog).toBeHidden({ timeout: 15_000 });
      await expect(card).toHaveCount(0);
   });
});

test.describe("environment-connections — mutability parity with /api/v0/status", () => {
   test("Add Connection button renders iff publisher reports mutable", async ({
      page,
   }, testInfo) => {
      const baseURL = testInfo.project.use.baseURL ?? "http://localhost:4000";
      const { mutable } = await getPublisherStatus(baseURL);
      const expected = mutable ? 1 : 0;

      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);

      await expect(
         page.getByRole("button", { name: "Add Connection" }),
      ).toHaveCount(expected);
   });
});
