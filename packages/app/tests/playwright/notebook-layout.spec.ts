// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import {
   addTextTile,
   createDocument,
   editText,
   queryTiles,
   tileByKey,
} from "./helpers/builder";
import {
   exampleFixture,
   registerPackageEnv,
   type PackageEnv,
} from "./helpers/packageEnv";
import { saveChanges } from "./helpers/save";

/**
 * A notebook is a one-column dashboard: authored in the dashboard builder,
 * saved in place, and read with the same cards a grid draws.
 *
 * It writes into a throwaway copy of the storefront package.
 */

let pe: PackageEnv;

test.describe("layout notebooks", () => {
   // eslint-disable-next-line no-empty-pattern
   test.beforeAll(async ({}, testInfo) => {
      pe = await registerPackageEnv(
         testInfo.project.use.baseURL ?? "http://localhost:4000",
         "nblayout",
         exampleFixture("storefront"),
         "storefront",
      );
   });
   test.afterAll(async () => {
      await pe?.dispose();
   });

   test("a new notebook takes a text tile and a query tile, an inline title, and reads in one column", async ({
      page,
      context,
   }) => {
      const slug = await createDocument(
         page,
         pe,
         "Notebook",
         "Layout notebook",
      );
      const file = `${slug}.malloy`;

      // A text tile, written where it is shown.
      await addTextTile(page);
      await editText(tileByKey(page, "text.text_1"), "A note added in place.");

      // A query tile beside the one the notebook was created with.
      await page.getByRole("button", { name: "Add tile", exact: true }).click();
      const dialog = page.getByRole("dialog", { name: "Add a tile" });
      await dialog.getByRole("button", { name: "View sales_by_month" }).click();
      await dialog
         .getByRole("button", { name: "Add tile", exact: true })
         .click();
      await expect(queryTiles(page)).toHaveCount(2);

      // The title is edited where it is shown.
      // The report title is the page's h1, edited where it stands.
      await page
         .getByRole("heading", { level: 1 })
         .first()
         .getByRole("button")
         .click();
      const title = page.getByLabel("Report title");
      await title.fill("Layout notebook, edited");
      await title.press("Enter");
      await expect(
         page.getByText("Layout notebook, edited", { exact: true }),
      ).toBeVisible();

      // So is the description.
      await page.getByRole("button", { name: "Add a description" }).click();
      await page.getByLabel("Markdown").fill("A one-line description");
      await page.getByLabel("Markdown").press("Escape");
      await expect(page.getByText("A one-line description")).toBeVisible();

      await saveChanges(page);
      const saved = await pe.readSource(file);
      expect(saved).toMatch(/^##" A one-line description\n##\| artifact/m);
      expect(saved).not.toContain('##"\n');
      expect(saved).toContain("A note added in place.");
      expect(saved).toContain('title="Layout notebook, edited"');
      expect(saved).toContain("layout=linear");
      expect(saved).not.toContain("kind=notebook");

      // The reader draws it in one column, query tiles in cards.
      const reader = await context.newPage();
      await reader.goto(`/${pe.env}/${pe.pkg}/${slug}`);
      await expect(reader.getByText("A note added in place.")).toBeVisible({
         timeout: 60_000,
      });
      await expect(reader.locator("[data-malloy-render-as]")).toHaveCount(2, {
         timeout: 60_000,
      });
      await expect(
         reader.getByRole("heading", { name: "Layout notebook, edited" }),
      ).toBeVisible();
      await expect(reader.getByText("A one-line description")).toBeVisible();
      // One column, query tiles in cards as a grid's are, text and description bare.
      await expect(reader.locator('[data-layout="linear"]')).toHaveCount(1);
      await expect(reader.locator('[data-chrome="card"]')).toHaveCount(2);
      const bare = reader.locator('[data-chrome="none"]');
      // The text tile and the description.
      expect(await bare.count()).toBeGreaterThanOrEqual(2);
      const borders = await bare.evaluateAll((els) =>
         els.map((el) => getComputedStyle(el).borderTopStyle),
      );
      expect(borders.every((style) => style === "none")).toBe(true);
      await reader.close();
   });
});
