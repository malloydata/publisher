// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import {
   exampleFixture,
   registerPackageEnv,
   type PackageEnv,
} from "./helpers/packageEnv";

/**
 * `?edit` on a `.malloynb`: the builder cannot edit the older notebook format,
 * and says so in words a reader acts on, rather than a not-found for a
 * `.malloy` file of the same name.
 */

let pe: PackageEnv;

test.describe("builder on an older notebook", () => {
   // eslint-disable-next-line no-empty-pattern
   test.beforeAll(async ({}, testInfo) => {
      pe = await registerPackageEnv(
         testInfo.project.use.baseURL ?? "http://localhost:4000",
         "legacy-nb",
         exampleFixture("storefront"),
         "storefront",
         {
            "q3.malloynb":
               '>>>markdown\n# Q3\n>>>malloy\nimport "storefront.malloy"\nrun: order_items -> { aggregate: order_count }\n',
         },
      );
   });
   test.afterAll(async () => {
      await pe?.dispose();
   });

   test("says it is an older notebook that can't be edited here, and how to edit it", async ({
      page,
   }) => {
      await page.goto(`/${pe.env}/${pe.pkg}/q3?edit`);
      await expect(
         page.getByText(
            "This is an older notebook. It can't be edited here. Convert it to a report to edit it.",
         ),
      ).toBeVisible({ timeout: 60_000 });
      await expect(page.getByText(/does not exist/)).toHaveCount(0);
   });

   test("says the same on the notebook's own path, and offers to view it", async ({
      page,
   }) => {
      await page.goto(`/${pe.env}/${pe.pkg}/q3.malloynb?edit`);
      await expect(
         page.getByText(
            "This is an older notebook. It can't be edited here. Convert it to a report to edit it.",
         ),
      ).toBeVisible({ timeout: 60_000 });
      await page.getByRole("link", { name: "View the notebook" }).click();
      await expect(page).toHaveURL(/q3\.malloynb$/);
   });
});
