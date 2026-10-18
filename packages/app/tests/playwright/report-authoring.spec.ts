// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test, type Page } from "@playwright/test";
import { saveChanges } from "./helpers/save";
import {
   registerPackageEnv,
   serverFixture,
   type PackageEnv,
} from "./helpers/packageEnv";

/**
 * An authoring path only a browser proves end to end: a draft kept in the
 * browser when the server takes no writes, listed on the package page and
 * opened again from there.
 */

const GRID = `##! experimental.givens
## artifact { layout=grid title="Draft me" tiles=["orders -> by_brand"] } dashboard { columns=4 }
import { orders } from 'orders.malloy'
import { BRAND, REGION } from 'givens.malloy'
`;

const editing = (page: Page) =>
   expect(page.getByRole("button", { name: "Undo" })).toBeVisible({
      timeout: 60_000,
   });

/** The server as a read-only deployment reports itself, so the Console keeps documents in the browser. */
const frozenServer = (page: Page) =>
   page.route("**/api/v0/status", async (route) => {
      const response = await route.fetch();
      const body = (await response.json()) as Record<string, unknown>;
      await route.fulfill({
         response,
         json: { ...body, frozenConfig: true, mutable: false },
      });
   });

test.describe("report authoring", () => {
   let pe: PackageEnv;
   // eslint-disable-next-line no-empty-pattern
   test.beforeAll(async ({}, testInfo) => {
      pe = await registerPackageEnv(
         testInfo.project.use.baseURL ?? "http://localhost:4000",
         "authoring",
         serverFixture("dashboards-test"),
         "dashboards-test",
         {
            "reports/draft.malloy": GRID.replace(
               /'(orders|givens)\.malloy'/g,
               "'../$1.malloy'",
            ),
         },
      );
   });
   test.afterAll(async () => {
      await pe?.dispose();
   });

   test("on a read-only server a save is a browser draft, listed under Drafts and resumed from there", async ({
      page,
   }) => {
      await frozenServer(page);
      // The server takes no writes, so Save keeps the edit in the browser.
      await page.goto(`/${pe.env}/${pe.pkg}/reports/draft?edit`);
      await editing(page);
      // The report title is the page's h1, edited where it stands.
      await page
         .getByRole("heading", { level: 1 })
         .first()
         .getByRole("button")
         .click();
      const title = page.getByLabel("Report title");
      await title.fill("Draft idea");
      await title.press("Enter");
      await saveChanges(page);

      // The package was not written: the draft is the browser's alone.
      const inPackage = await fetch(
         `${pe.baseURL}/api/v0/environments/${pe.env}/packages/${pe.pkg}/models/${encodeURIComponent("reports/draft.malloy")}`,
      );
      expect(
         ((await inPackage.json()) as { sourceText?: string }).sourceText,
      ).not.toContain("Draft idea");

      await page.goto(`/${pe.env}/${pe.pkg}`);
      const drafts = page.getByRole("region", { name: "Drafts", exact: true });
      const row = drafts
         .getByRole("button", { name: /reports\/draft\.malloy/ })
         .first();
      await expect(row).toBeVisible({ timeout: 60_000 });
      await expect(drafts).toContainText("Stored in this browser only");

      await row.click();
      await expect(page).toHaveURL(
         new RegExp(`/${pe.pkg}/reports/draft\\?edit$`),
      );
      await editing(page);
      // Reopened, it offers the browser's copy over the package's text.
      await expect(
         page.getByText(
            "You have edits to this report saved in this browser that the package does not have.",
         ),
      ).toBeVisible({ timeout: 30_000 });
      await page.getByRole("button", { name: "Resume", exact: true }).click();
      await expect(
         page.getByRole("button", { name: "Draft idea", exact: true }),
      ).toBeVisible({ timeout: 30_000 });
   });
});
