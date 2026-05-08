import { expect, test } from "@playwright/test";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";
import { getPublisherStatus } from "./helpers/publisherStatus";

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
