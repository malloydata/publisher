import { expect, test } from "@playwright/test";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";
import { getPublisherStatus } from "./helpers/publisherStatus";

test.describe("package-databases — embedded", () => {
   test("Embedded Databases section is visible with at least one row", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.imdb);

      await expect(page.getByText("Embedded Databases")).toBeVisible();
      // Section renders `Fetching Databases...` until the API resolves; wait for the first row.
      const rows = page.locator("tr").filter({ hasText: ".parquet" });
      await expect(rows.first()).toBeVisible();
   });

   test("clicking a parquet row opens a schema dialog that can close", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.imdb);

      const firstRow = page
         .locator("tr")
         .filter({ hasText: ".parquet" })
         .first();
      await firstRow.click();

      const dialog = page.getByRole("dialog");
      await expect(dialog).toBeVisible();
      await dialog.getByRole("button", { name: "close" }).click();
      await expect(dialog).toHaveCount(0);
   });
});

test.describe("package-databases — connections read", () => {
   test("Database Connections section lists connections", async ({ page }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.imdb);

      await expect(page.getByText("Database Connections")).toBeVisible();
      // `bigquery` is the fixture connection present on every malloy-samples package.
      await expect(
         page.getByRole("row", { name: /bigquery.*bigquery/ }),
      ).toBeVisible();
   });
});

test.describe("package-databases — connections mutable", () => {
   test.beforeAll(async ({}, testInfo) => {
      const baseURL = testInfo.project.use.baseURL ?? "http://localhost:4000";
      const { mutable } = await getPublisherStatus(baseURL);
      test.skip(!mutable, "publisher is read-only");
   });

   test("Add Connection button is present", async ({ page }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.imdb);

      await expect(
         page.getByRole("button", { name: "Add Connection" }),
      ).toBeVisible();
   });

   test("connection row exposes Edit and Delete actions", async ({ page }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.imdb);

      await expect(
         page.getByRole("button", { name: "Edit connection bigquery" }),
      ).toBeVisible();
      await expect(
         page.getByRole("button", { name: "Delete connection bigquery" }),
      ).toBeVisible();
   });
});

test.describe("package-databases — mutability parity with /api/v0/status", () => {
   test("connection mutation controls render iff publisher reports mutable", async ({
      page,
   }, testInfo) => {
      const baseURL = testInfo.project.use.baseURL ?? "http://localhost:4000";
      const { mutable } = await getPublisherStatus(baseURL);
      const expected = mutable ? 1 : 0;

      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.imdb);

      await expect(
         page.getByRole("button", { name: "Add Connection" }),
      ).toHaveCount(expected);
   });
});
