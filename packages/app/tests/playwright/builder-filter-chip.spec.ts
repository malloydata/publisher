// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import { editorOpen, queryTiles } from "./helpers/builder";
import {
   exampleFixture,
   registerPackageEnv,
   type PackageEnv,
} from "./helpers/packageEnv";
import { saveChanges } from "./helpers/save";
import { filterButton } from "./helpers/filters";

/**
 * The x on a filter chip removes the filter from the file: its declaration and
 * every tile binding that read it. Writes into a throwaway copy of storefront.
 */

let pe: PackageEnv;

test.describe("filter chip", () => {
   // eslint-disable-next-line no-empty-pattern
   test.beforeAll(async ({}, testInfo) => {
      pe = await registerPackageEnv(
         testInfo.project.use.baseURL ?? "http://localhost:4000",
         "chip",
         exampleFixture("storefront"),
         "storefront",
      );
   });
   test.afterAll(async () => {
      await pe?.dispose();
   });

   test("removing a filter takes its declaration and its bindings out of the file, and the reader loses the control", async ({
      page,
   }) => {
      const file = "overview.malloy";
      const original = await pe.readSource(file);
      expect(original).toContain("given: CATEGORY");
      expect(original).toContain("$CATEGORY");

      await page.goto(`/${pe.env}/${pe.pkg}/overview?edit`);
      await editorOpen(page);
      await expect(queryTiles(page)).toHaveCount(5, { timeout: 60_000 });

      await page.getByLabel("Remove filter CATEGORY").click();
      await expect(filterButton(page, "Category")).toHaveCount(0);
      await expect(filterButton(page, "Brand")).toBeVisible();
      await saveChanges(page);

      const saved = await pe.readSource(file);
      expect(saved).not.toContain("given: CATEGORY");
      expect(saved).not.toContain("$CATEGORY");
      // Its neighbours are untouched.
      expect(saved).toContain("given: BRAND");
      expect(saved).toContain("$BRAND");

      const reader = await page.context().newPage();
      await reader.goto(`/${pe.env}/${pe.pkg}/overview`);
      await expect(filterButton(reader, "Brand")).toBeVisible({
         timeout: 60_000,
      });
      await expect(filterButton(reader, "Category")).toHaveCount(0);
      await reader.close();
   });
});
