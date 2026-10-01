// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test, type Locator, type Page } from "@playwright/test";
import {
   exampleFixture,
   registerPackageEnv,
   type PackageEnv,
} from "./helpers/packageEnv";

/**
 * The chart picker, judged by what the renderer actually drew. A table passes
 * a `.malloy-render` check, so every assertion here reads the render-as the
 * result stage publishes, which is the only thing that tells a bar from a line.
 */

let pe: PackageEnv;

test.describe("builder chart type", () => {
   // eslint-disable-next-line no-empty-pattern
   test.beforeAll(async ({}, testInfo) => {
      pe = await registerPackageEnv(
         testInfo.project.use.baseURL ?? "http://localhost:4000",
         "charts",
         exampleFixture("storefront"),
         "storefront",
      );
   });
   test.afterAll(async () => {
      await pe?.dispose();
   });

   const renderAs = (scope: Locator | Page) =>
      scope.locator("[data-malloy-render-as]").first();

   const pickChart = async (page: Page, cellLabel: string, option: string) => {
      await page.getByRole("combobox", { name: `Chart, ${cellLabel}` }).click();
      await page.getByRole("option", { name: option, exact: true }).click();
   };

   test("a notebook cell follows the picker: bar, default, no chart", async ({
      page,
   }) => {
      await page.goto(`/${pe.env}/${pe.pkg}/notebooks/category-review/edit`);
      await expect(page.getByText("Editing", { exact: true })).toBeVisible({
         timeout: 60_000,
      });
      // The monthly cell carries an explicit `# line_chart` on its run.
      const cell = page
         .getByRole("group", { name: /^Cell \d+, query$/ })
         .first();
      await expect(renderAs(cell)).toHaveAttribute(
         "data-malloy-render-as",
         "line",
         { timeout: 60_000 },
      );
      const label = await cell
         .getByRole("combobox", { name: /^Chart, / })
         .getAttribute("aria-label");
      const cellLabel = label!.replace(/^Chart, /, "");

      await pickChart(page, cellLabel, "Bar");
      await expect(renderAs(cell)).toHaveAttribute(
         "data-malloy-render-as",
         "bar",
         { timeout: 60_000 },
      );
      await pickChart(page, cellLabel, "Default");
      await expect(renderAs(cell)).toHaveAttribute(
         "data-malloy-render-as",
         "line",
         { timeout: 60_000 },
      );
      await pickChart(page, cellLabel, "No chart (table)");
      await expect(renderAs(cell)).toHaveAttribute(
         "data-malloy-render-as",
         "table",
         { timeout: 60_000 },
      );
   });

   const createDocument = async (
      page: Page,
      kind: "Dashboard" | "Notebook",
      title: string,
   ) => {
      await page.goto(`/${pe.env}/${pe.pkg}`);
      await page.getByRole("button", { name: "New", exact: true }).click({
         timeout: 60_000,
      });
      await page.getByRole("menuitem", { name: kind }).click();
      const dialog = page.getByRole("dialog");
      await expect(dialog.getByLabel(`${kind} title`)).not.toHaveValue("", {
         timeout: 30_000,
      });
      await dialog.getByRole("combobox", { name: "Model" }).click();
      await page.getByRole("option", { name: "storefront.malloy" }).click();
      await dialog
         .getByRole("combobox", {
            name: kind === "Dashboard" ? "First tile" : "First query",
         })
         .click();
      await page
         .getByRole("option", {
            name: "order_items → sales_by_month",
            exact: true,
         })
         .click();
      await dialog.getByLabel(`${kind} title`).fill(title);
      await dialog.getByRole("button", { name: `Create ${kind}` }).click();
      const kindPath = kind === "Dashboard" ? "dashboards" : "notebooks";
      await expect(page).toHaveURL(
         new RegExp(`/${kindPath}/[a-z0-9-]+/edit$`),
         {
            timeout: 60_000,
         },
      );
      await expect(page.getByText("Editing", { exact: true })).toBeVisible({
         timeout: 60_000,
      });
      const slug = new URL(page.url()).pathname.split("/").slice(-2)[0]!;
      // Deduplication changes the slug on a repeat run, never its prefix.
      expect(slug.startsWith(title.toLowerCase().replace(/ /g, "-"))).toBe(
         true,
      );
      return slug;
   };

   test("a new notebook: a query added with a chart is saved and the reader draws it", async ({
      page,
   }) => {
      const slug = await createDocument(page, "Notebook", "Chart notebook");
      const queries = page.getByRole("group", { name: /^Cell \d+, query$/ });
      await expect(renderAs(queries.first())).toHaveAttribute(
         "data-malloy-render-as",
         "line",
         { timeout: 60_000 },
      );

      // A second query, added below the first, set to a bar.
      await queries
         .first()
         .getByRole("button", { name: "Add query below" })
         .click();
      const dialog = page.getByRole("dialog", { name: "Add a query" });
      await dialog.getByRole("combobox", { name: "Source" }).click();
      await page.getByRole("option", { name: "order_items" }).first().click();
      await dialog.getByRole("button", { name: "View sales_by_month" }).click();
      await dialog
         .getByRole("button", { name: "Add query", exact: true })
         .click();

      await expect(queries).toHaveCount(2);
      await queries
         .nth(1)
         .getByRole("combobox", { name: /^Chart, / })
         .click();
      await page.getByRole("option", { name: "Bar", exact: true }).click();
      await expect(renderAs(queries.nth(1))).toHaveAttribute(
         "data-malloy-render-as",
         "bar",
         { timeout: 60_000 },
      );

      await page.getByRole("button", { name: "Save changes" }).click();
      await page.getByRole("button", { name: "Save this" }).click();
      await expect(page.getByRole("button", { name: "Saved" })).toBeVisible({
         timeout: 30_000,
      });

      // The saved file, not the editor's memory, is what the reader draws.
      await page.goto(`/${pe.env}/${pe.pkg}/notebooks/${slug}`);
      const drawn = page.locator("[data-malloy-render-as]");
      await expect(drawn).toHaveCount(2, { timeout: 60_000 });
      await expect(drawn.nth(0)).toHaveAttribute(
         "data-malloy-render-as",
         "line",
      );
      await expect(drawn.nth(1)).toHaveAttribute(
         "data-malloy-render-as",
         "bar",
      );
   });

   const pickBarOnFirstTile = async (page: Page) => {
      await page.getByLabel(/^Settings for /).click();
      await page.getByRole("combobox", { name: /^Chart, / }).click();
      await page.getByRole("option", { name: "Bar", exact: true }).click();
      await page.keyboard.press("Escape");
   };

   test("a new dashboard: the first tile is set to a bar, saved, and the reader draws it", async ({
      page,
   }) => {
      const slug = await createDocument(page, "Dashboard", "Chart dashboard");
      const tile = page.locator('[aria-label^="Tile "]');
      await expect(renderAs(tile)).toHaveAttribute(
         "data-malloy-render-as",
         "line",
         { timeout: 60_000 },
      );
      await pickBarOnFirstTile(page);
      await page.getByRole("button", { name: "Save changes" }).click();
      await expect(page.getByRole("button", { name: "Saved" })).toBeVisible({
         timeout: 30_000,
      });
      await page.goto(`/${pe.env}/${pe.pkg}/dashboards/${slug}`);
      await expect(
         page.locator("[data-malloy-render-as]").first(),
      ).toHaveAttribute("data-malloy-render-as", "bar", { timeout: 60_000 });

      // No chart, then Default, each saved and read back by the reader.
      for (const [option, drawn] of [
         ["No chart (table)", "table"],
         ["Default", "line"],
      ] as const) {
         await page.goto(`/${pe.env}/${pe.pkg}/dashboards/${slug}/edit`);
         await expect(page.getByText("Editing", { exact: true })).toBeVisible({
            timeout: 60_000,
         });
         await page.getByLabel(/^Settings for /).click();
         await page.getByRole("combobox", { name: /^Chart, / }).click();
         await page.getByRole("option", { name: option, exact: true }).click();
         await page.keyboard.press("Escape");
         await page.getByRole("button", { name: "Save changes" }).click();
         await expect(page.getByRole("button", { name: "Saved" })).toBeVisible({
            timeout: 30_000,
         });
         await page.goto(`/${pe.env}/${pe.pkg}/dashboards/${slug}`);
         await expect(
            page.locator("[data-malloy-render-as]").first(),
         ).toHaveAttribute("data-malloy-render-as", drawn, { timeout: 60_000 });
      }
   });

   // The editor previews a reference tile by running its BASE view
   // (preview.ts), which never carries the tile wrapper's chart line, so the
   // picker changes the file and the reader but not the tile under the author's
   // cursor. Expected to fail until the preview carries the chart line.
   test("a new dashboard: the editor's own tile follows the picker", async ({
      page,
   }) => {
      test.fail(true, "tile preview runs the base view, not the chart line");
      await createDocument(page, "Dashboard", "Chart preview");
      const tile = page.locator('[aria-label^="Tile "]');
      await expect(renderAs(tile)).toHaveAttribute(
         "data-malloy-render-as",
         "line",
         { timeout: 60_000 },
      );
      await pickBarOnFirstTile(page);
      await expect(renderAs(tile)).toHaveAttribute(
         "data-malloy-render-as",
         "bar",
         { timeout: 10_000 },
      );
   });
});
