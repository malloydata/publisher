// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import {
   addTextTile,
   createDocument,
   editText,
   editorOpen,
   queryTiles,
   tileByKey,
} from "./helpers/builder";
import {
   exampleFixture,
   registerPackageEnv,
   type PackageEnv,
} from "./helpers/packageEnv";
import { saveChanges, undoSave } from "./helpers/save";

/**
 * A notebook is a one-column dashboard: authored in the dashboard builder,
 * saved at once with a way back, and read with no card around its tiles. A
 * notebook still in the cell format is converted when it is saved, and Undo
 * save puts the original text back.
 *
 * Both cases write into a throwaway copy of the storefront package.
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

   test("a new notebook takes a text tile and a query tile, an inline title, and reads with no card chrome; Undo save restores the file", async ({
      page,
      context,
   }) => {
      const slug = await createDocument(
         page,
         pe,
         "Notebook",
         "Layout notebook",
      );
      const file = `notebooks/${slug}.malloy`;
      const created = await pe.readSource(file);

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
      await page.getByRole("heading", { level: 5 }).getByRole("button").click();
      const title = page.getByLabel("Notebook title");
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
      expect(saved).toContain("kind=notebook");

      // The reader draws every tile with no card around it.
      const reader = await context.newPage();
      await reader.goto(`/${pe.env}/${pe.pkg}/notebooks/${slug}`);
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
      await expect(reader.locator('[data-chrome="card"]')).toHaveCount(0);
      const bare = reader.locator('[data-chrome="none"]');
      expect(await bare.count()).toBeGreaterThanOrEqual(3);
      const borders = await bare.evaluateAll((els) =>
         els.map((el) => getComputedStyle(el).borderTopStyle),
      );
      expect(borders.every((style) => style === "none")).toBe(true);
      await reader.close();

      // Undo save writes the file back as it was created.
      await undoSave(page);
      expect(await pe.readSource(file)).toBe(created);
   });

   test("a notebook in the cell format opens as a conversion, saves as a layout, and Undo save restores the original text", async ({
      page,
      context,
   }) => {
      const file = "notebooks/category-review.malloy";
      const original = await pe.readSource(file);

      await page.goto(`/${pe.env}/${pe.pkg}/notebooks/category-review/edit`);
      await editorOpen(page);
      await expect(
         page.getByText(
            /This notebook is in the cell format\. Saving rewrites it/,
         ),
      ).toBeVisible();
      // Nothing was edited, and Save is already on: the conversion is the change.
      await expect(
         page.getByRole("button", { name: "Save changes" }),
      ).toBeEnabled();
      await expect(queryTiles(page)).toHaveCount(3, { timeout: 60_000 });

      await saveChanges(page);
      const converted = await pe.readSource(file);
      expect(converted).not.toBe(original);
      expect(converted).toContain("tiles=[");
      expect(converted).not.toMatch(/^run:/m);

      // The converted notebook reads: its three queries still draw.
      const reader = await context.newPage();
      await reader.goto(`/${pe.env}/${pe.pkg}/notebooks/category-review`);
      await expect(reader.locator("[data-malloy-render-as]")).toHaveCount(3, {
         timeout: 60_000,
      });
      await expect(reader.locator('[data-chrome="card"]')).toHaveCount(0);
      await reader.close();

      // Byte for byte, not merely equivalent.
      await undoSave(page);
      expect(await pe.readSource(file)).toBe(original);
   });
});
