// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import { editorOpen, queryTiles } from "./helpers/builder";
import {
   exampleFixture,
   registerPackageEnv,
   type PackageEnv,
} from "./helpers/packageEnv";
import { saveChanges, undoSave } from "./helpers/save";

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
      const file = "dashboards/overview.malloy";
      const original = await pe.readSource(file);
      expect(original).toContain("given: CATEGORY");
      expect(original).toContain("$CATEGORY");

      await page.goto(`/${pe.env}/${pe.pkg}/dashboards/overview/edit`);
      await editorOpen(page);
      await expect(queryTiles(page)).toHaveCount(5, { timeout: 60_000 });

      await page.getByLabel("Remove filter CATEGORY").click();
      await expect(page.getByLabel("Edit filter CATEGORY")).toHaveCount(0);
      await expect(page.getByLabel("Edit filter BRAND")).toBeVisible();
      await saveChanges(page);

      const saved = await pe.readSource(file);
      expect(saved).not.toContain("given: CATEGORY");
      expect(saved).not.toContain("$CATEGORY");
      // Its neighbours are untouched.
      expect(saved).toContain("given: BRAND");
      expect(saved).toContain("$BRAND");

      const reader = await page.context().newPage();
      await reader.goto(`/${pe.env}/${pe.pkg}/dashboards/overview`);
      await expect(reader.getByRole("combobox", { name: "Brand" })).toBeVisible(
         { timeout: 60_000 },
      );
      await expect(
         reader.getByRole("combobox", { name: "Category" }),
      ).toHaveCount(0);
      await reader.close();

      await undoSave(page);
      expect(await pe.readSource(file)).toBe(original);
   });
});
