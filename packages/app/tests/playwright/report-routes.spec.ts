// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test, type Page } from "@playwright/test";
import {
   registerPackageEnv,
   serverFixture,
   type PackageEnv,
} from "./helpers/packageEnv";

/**
 * A report's route is its package path without `.malloy`, and `?edit` on it
 * opens the builder. Because the edit mode is a query parameter, not a path
 * segment, a report whose own path ends in `edit` opens like any other, and a
 * report under `data-apps/` opens as a report rather than as a data app. The
 * old `<path>/edit` route still opens the builder, but only when no report is
 * at `<path>/edit.malloy`, and an old by-name `dashboards/<name>` link lands on
 * the report's own path.
 */

const report = (title: string, importDepth: string) => `##! experimental.givens
## artifact { layout=grid title="${title}" tiles=["orders -> by_brand"] } dashboard { columns=4 }
import { orders } from '${importDepth}orders.malloy'
import { BRAND, REGION } from '${importDepth}givens.malloy'
`;

const editing = (page: Page) =>
   expect(page.getByRole("button", { name: "Undo" })).toBeVisible({
      timeout: 60_000,
   });

const reading = async (page: Page, title: string) => {
   await expect(page.getByText(title, { exact: true }).first()).toBeVisible({
      timeout: 60_000,
   });
   await expect(page.getByRole("button", { name: "Undo" })).toHaveCount(0);
};

test.describe("report routes", () => {
   let pe: PackageEnv;
   // eslint-disable-next-line no-empty-pattern
   test.beforeAll(async ({}, testInfo) => {
      pe = await registerPackageEnv(
         testInfo.project.use.baseURL ?? "http://localhost:4000",
         "routes",
         serverFixture("dashboards-test"),
         "dashboards-test",
         {
            "sales/edit.malloy": report("Edit named report", "../"),
            "quarterly.malloy": report("Quarterly report", ""),
            "data-apps/board.malloy": report("Board under data-apps", "../"),
            "reports/2026/q3.malloy": report("Third quarter", "../../"),
         },
      );
   });
   test.afterAll(async () => {
      await pe?.dispose();
   });

   test("a report whose path ends in edit opens as itself, not as the editor of its parent", async ({
      page,
   }) => {
      await page.goto(`/${pe.env}/${pe.pkg}/sales/edit`);
      await reading(page, "Edit named report");
      await expect(page).toHaveURL(new RegExp(`/${pe.pkg}/sales/edit$`));
   });

   test("?edit opens the builder, including for a report named edit", async ({
      page,
   }) => {
      await page.goto(`/${pe.env}/${pe.pkg}/quarterly?edit`);
      await editing(page);
      await expect(page.getByText("Quarterly report").first()).toBeVisible();

      await page.goto(`/${pe.env}/${pe.pkg}/sales/edit?edit`);
      await editing(page);
      await expect(page.getByText("Edit named report").first()).toBeVisible();
   });

   test("Edit and View in the header switch ?edit on and off", async ({
      page,
   }) => {
      await page.goto(`/${pe.env}/${pe.pkg}/sales/edit`);
      await reading(page, "Edit named report");
      await page.getByRole("button", { name: "Edit", exact: true }).click();
      await expect(page).toHaveURL(new RegExp(`/${pe.pkg}/sales/edit\\?edit$`));
      await editing(page);
      await page
         .getByRole("toolbar", { name: "Report builder" })
         .getByRole("button", { name: "Done", exact: true })
         .click();
      await expect(page).toHaveURL(new RegExp(`/${pe.pkg}/sales/edit$`));
      await reading(page, "Edit named report");
   });

   test("a legacy /edit route redirects to ?edit when no report has that path", async ({
      page,
   }) => {
      await page.goto(`/${pe.env}/${pe.pkg}/quarterly/edit?BRAND=Acme`);
      await expect(page).toHaveURL(
         new RegExp(`/${pe.pkg}/quarterly\\?edit&BRAND=Acme$`),
      );
      await editing(page);
   });

   test("a report under data-apps/ opens as a report, and edits", async ({
      page,
   }) => {
      await page.goto(`/${pe.env}/${pe.pkg}/data-apps/board`);
      await reading(page, "Board under data-apps");
      await expect(page.locator("iframe")).toHaveCount(0);

      await page.goto(`/${pe.env}/${pe.pkg}/data-apps/board?edit`);
      await editing(page);
   });

   test("an old by-name dashboards/ link lands on the report's nested path, keeping its query and ?edit", async ({
      page,
   }) => {
      await page.goto(`/${pe.env}/${pe.pkg}/dashboards/q3?BRAND=Acme`);
      await expect(page).toHaveURL(
         new RegExp(`/${pe.pkg}/reports/2026/q3\\?BRAND=Acme$`),
      );
      await reading(page, "Third quarter");

      await page.goto(`/${pe.env}/${pe.pkg}/dashboards/q3?edit`);
      await expect(page).toHaveURL(
         new RegExp(`/${pe.pkg}/reports/2026/q3\\?edit$`),
      );
      await editing(page);
   });
});
