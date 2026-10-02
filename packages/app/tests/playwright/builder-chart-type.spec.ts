// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test, type Locator, type Page } from "@playwright/test";
import {
   createDocument,
   editorOpen,
   pickChart,
   queryTiles,
} from "./helpers/builder";
import {
   exampleFixture,
   registerPackageEnv,
   type PackageEnv,
} from "./helpers/packageEnv";
import { saveChanges } from "./helpers/save";

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

   test("a converted notebook tile follows the picker: bar, from the view, table", async ({
      page,
   }) => {
      await page.goto(`/${pe.env}/${pe.pkg}/notebooks/category-review/edit`);
      await editorOpen(page);
      // Opened as an unsaved conversion, whose tiles are placeholders until it is saved.
      await saveChanges(page);
      // The monthly query carries an explicit `# line_chart`.
      const tile = queryTiles(page).first();
      await expect(renderAs(tile)).toHaveAttribute(
         "data-malloy-render-as",
         "line",
         { timeout: 60_000 },
      );

      await pickChart(page, tile, "Bar");
      await expect(renderAs(tile)).toHaveAttribute(
         "data-malloy-render-as",
         "bar",
         { timeout: 60_000 },
      );
      await pickChart(page, tile, "From the view");
      await expect(renderAs(tile)).toHaveAttribute(
         "data-malloy-render-as",
         "line",
         { timeout: 60_000 },
      );
      await pickChart(page, tile, "Table");
      await expect(renderAs(tile)).toHaveAttribute(
         "data-malloy-render-as",
         "table",
         { timeout: 60_000 },
      );
   });

   test("a new notebook: a query added with a chart is saved and the reader draws it", async ({
      page,
   }) => {
      const slug = await createDocument(page, pe, "Notebook", "Chart notebook");
      const queries = queryTiles(page);
      await expect(renderAs(queries.first())).toHaveAttribute(
         "data-malloy-render-as",
         "line",
         { timeout: 60_000 },
      );

      // A second query, added from the toolbar, set to a bar.
      await page.getByRole("button", { name: "Add tile", exact: true }).click();
      const dialog = page.getByRole("dialog", { name: "Add a tile" });
      await dialog.getByRole("combobox", { name: "Source" }).click();
      await page.getByRole("option", { name: "order_items" }).first().click();
      await dialog.getByRole("button", { name: "View sales_by_month" }).click();
      await dialog
         .getByRole("button", { name: "Add tile", exact: true })
         .click();

      await expect(queries).toHaveCount(2);
      await pickChart(page, queries.nth(1), "Bar");
      await expect(renderAs(queries.nth(1))).toHaveAttribute(
         "data-malloy-render-as",
         "bar",
         { timeout: 60_000 },
      );

      await saveChanges(page);

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

   test("a new dashboard: the first tile is set to a bar, saved, and the reader draws it", async ({
      page,
   }) => {
      const slug = await createDocument(
         page,
         pe,
         "Dashboard",
         "Chart dashboard",
      );
      const tile = queryTiles(page).first();
      await expect(renderAs(tile)).toHaveAttribute(
         "data-malloy-render-as",
         "line",
         { timeout: 60_000 },
      );
      await pickChart(page, tile, "Bar");
      await saveChanges(page);
      await page.goto(`/${pe.env}/${pe.pkg}/dashboards/${slug}`);
      await expect(
         page.locator("[data-malloy-render-as]").first(),
      ).toHaveAttribute("data-malloy-render-as", "bar", { timeout: 60_000 });

      // Table, then From the view, each saved and read back by the reader.
      for (const [option, drawn] of [
         ["Table", "table"],
         ["From the view", "line"],
      ] as const) {
         await page.goto(`/${pe.env}/${pe.pkg}/dashboards/${slug}/edit`);
         await editorOpen(page);
         await pickChart(page, queryTiles(page).first(), option);
         await saveChanges(page);
         await page.goto(`/${pe.env}/${pe.pkg}/dashboards/${slug}`);
         await expect(
            page.locator("[data-malloy-render-as]").first(),
         ).toHaveAttribute("data-malloy-render-as", drawn, { timeout: 60_000 });
      }
   });

   // The editor previews the base view, so the tile's chart line has to ride
   // on the preview query or the tile would keep drawing the view's own chart.
   test("a new dashboard: the editor's own tile follows the picker", async ({
      page,
   }) => {
      await createDocument(page, pe, "Dashboard", "Chart preview");
      const tile = queryTiles(page).first();
      await expect(renderAs(tile)).toHaveAttribute(
         "data-malloy-render-as",
         "line",
         { timeout: 60_000 },
      );
      await pickChart(page, tile, "Bar");
      await expect(renderAs(tile)).toHaveAttribute(
         "data-malloy-render-as",
         "bar",
         { timeout: 30_000 },
      );
      for (const [option, drawn] of [
         ["Table", "table"],
         ["From the view", "line"],
      ] as const) {
         await pickChart(page, tile, option);
         await expect(renderAs(tile)).toHaveAttribute(
            "data-malloy-render-as",
            drawn,
            { timeout: 30_000 },
         );
      }
   });
});
