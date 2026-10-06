// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test, type Page } from "@playwright/test";
import { editText, editorOpen, queryTiles, tileByKey } from "./helpers/builder";
import {
   exampleFixture,
   registerPackageEnv,
   type PackageEnv,
} from "./helpers/packageEnv";
import { saveChanges } from "./helpers/save";

/**
 * A document's kind is a tag in its file, not a folder: the Console finds a
 * notebook filed in dashboards/ (and a dashboard in notebooks/) at either
 * address, and the builder saves it back to the path it is listed at. Each
 * test writes into a throwaway copy of the storefront package.
 */

const layout = (kind: "dashboard" | "notebook", title: string, note: string) =>
   `##! experimental.givens
## artifact { kind=${kind} title="${title}" tiles=["order_items -> sales_by_month", intro { kind=text }] }
import { order_items } from "../storefront.malloy"

##|(markdown) intro
${note}
|##
`;

const MISFILED_NOTEBOOK = "Misfiled notebook note.";
const MISFILED_DASHBOARD = "Misfiled dashboard note.";

let pe: PackageEnv;

test.describe("a kind that disagrees with its folder", () => {
   // eslint-disable-next-line no-empty-pattern
   test.beforeAll(async ({}, testInfo) => {
      pe = await registerPackageEnv(
         testInfo.project.use.baseURL ?? "http://localhost:4000",
         "misfiled",
         exampleFixture("storefront"),
         "storefront",
         {
            // A notebook in dashboards/, a dashboard in notebooks/.
            "dashboards/misfiled.malloy": layout(
               "notebook",
               "Misfiled notebook",
               MISFILED_NOTEBOOK,
            ),
            "notebooks/mixed.malloy": layout(
               "dashboard",
               "Mixed dashboard",
               MISFILED_DASHBOARD,
            ),
         },
      );
   });
   test.afterAll(async () => {
      await pe?.dispose();
   });

   const show = (...parts: string[]) =>
      `/${pe.env}/${pe.pkg}/${parts.join("/")}`;

   const openEditor = async (page: Page, ...parts: string[]) => {
      await page.goto(show(...parts, "edit"));
      await editorOpen(page);
      await expect(queryTiles(page).first()).toBeVisible({ timeout: 60_000 });
   };

   const isStatus = async (modelPath: string) =>
      (
         await fetch(
            `${pe.baseURL}/api/v0/environments/${pe.env}/packages/${pe.pkg}/models/${encodeURIComponent(modelPath)}`,
         )
      ).status;

   test("a notebook in dashboards/ opens at either address and saves to the path it is listed at", async ({
      page,
   }) => {
      for (const folder of ["dashboards", "notebooks"]) {
         await page.goto(show(folder, "misfiled"));
         await expect(page.getByText(MISFILED_NOTEBOOK)).toBeVisible({
            timeout: 60_000,
         });
         await expect(page.locator('[data-chrome="card"]')).toHaveCount(0);
      }

      await openEditor(page, "notebooks", "misfiled");
      await editText(tileByKey(page, "text.intro"), "Edited in place.");
      await saveChanges(page);

      expect(await pe.readSource("dashboards/misfiled.malloy")).toContain(
         "Edited in place.",
      );
      expect(await isStatus("notebooks/misfiled.malloy")).toBe(404);
   });

   test("a dashboard in notebooks/ opens at either address and saves to the path it is listed at", async ({
      page,
   }) => {
      for (const folder of ["dashboards", "notebooks"]) {
         await page.goto(show(folder, "mixed"));
         await expect(page.getByText(MISFILED_DASHBOARD)).toBeVisible({
            timeout: 60_000,
         });
         await expect(page.locator('[data-chrome="card"]').first()).toBeVisible(
            {
               timeout: 60_000,
            },
         );
      }

      await openEditor(page, "dashboards", "mixed");
      await editText(tileByKey(page, "text.intro"), "Edited in place.");
      await saveChanges(page);

      expect(await pe.readSource("notebooks/mixed.malloy")).toContain(
         "Edited in place.",
      );
      expect(await isStatus("dashboards/mixed.malloy")).toBe(404);
   });
});
