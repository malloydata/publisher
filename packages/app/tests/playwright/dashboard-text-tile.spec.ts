// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import {
   addTextTile,
   createDocument,
   editText,
   tileByKey,
} from "./helpers/builder";
import {
   exampleFixture,
   registerPackageEnv,
   type PackageEnv,
} from "./helpers/packageEnv";
import { saveChanges } from "./helpers/save";

/**
 * A markdown text tile in a dashboard: added from the Add tile dialog, written
 * on the tile itself, saved, and rendered as prose by the reader.
 */

let pe: PackageEnv;

test.describe("dashboard text tile", () => {
   // eslint-disable-next-line no-empty-pattern
   test.beforeAll(async ({}, testInfo) => {
      pe = await registerPackageEnv(
         testInfo.project.use.baseURL ?? "http://localhost:4000",
         "dashtext",
         exampleFixture("storefront"),
         "storefront",
      );
   });
   test.afterAll(async () => {
      await pe?.dispose();
   });

   test("a text tile is added, edited in place, saved, and the reader renders its markdown", async ({
      page,
   }) => {
      const slug = await createDocument(
         page,
         pe,
         "Dashboard",
         "Text dashboard",
      );

      await addTextTile(page);
      const tile = tileByKey(page, "text.text_1");
      await expect(tile).toBeVisible();
      await editText(tile, "# Heading here\n\nSome **bold** words.");
      await expect(tile.locator("strong", { hasText: "bold" })).toBeVisible();

      await saveChanges(page);
      const saved = await pe.readSource(`dashboards/${slug}.malloy`);
      expect(saved).toContain("##|(markdown) text_1");
      expect(saved).toContain("Some **bold** words.");

      await page.goto(`/${pe.env}/${pe.pkg}/dashboards/${slug}`);
      await expect(
         page.getByRole("heading", { name: "Heading here" }),
      ).toBeVisible({ timeout: 60_000 });
      await expect(page.locator("strong", { hasText: "bold" })).toBeVisible();
      // A dashboard keeps its cards: the text tile sits in one, beside the query tile's.
      await expect(page.locator('[data-chrome="card"]')).toHaveCount(2);
      await expect(page.locator("[data-malloy-render-as]").first()).toBeVisible(
         { timeout: 60_000 },
      );
   });
});
