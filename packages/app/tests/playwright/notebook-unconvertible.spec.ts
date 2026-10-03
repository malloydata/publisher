// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import {
   exampleFixture,
   registerPackageEnv,
   type PackageEnv,
} from "./helpers/packageEnv";

/**
 * A cell-format notebook the converter cannot place (here a run that extends
 * its source inline) still reads, but the builder refuses to open it and says
 * why, rather than offering a Save that would rewrite it wrongly.
 */

const NOTEBOOK = `##! experimental.givens
## artifact { kind=notebook title="Inline extend" }
import { order_items } from "../storefront.malloy"

##(markdown) Unconvertible notebook prose.

run: order_items extend { dimension: shouted is "x" } -> by_category
`;

let pe: PackageEnv;

test.describe("an unconvertible notebook", () => {
   // eslint-disable-next-line no-empty-pattern
   test.beforeAll(async ({}, testInfo) => {
      pe = await registerPackageEnv(
         testInfo.project.use.baseURL ?? "http://localhost:4000",
         "unconv",
         exampleFixture("storefront"),
         "storefront",
         { "notebooks/inline-extend.malloy": NOTEBOOK },
      );
   });
   test.afterAll(async () => {
      await pe?.dispose();
   });

   test("opens read-only in the builder with the reason, and still reads", async ({
      page,
   }) => {
      await page.goto(`/${pe.env}/${pe.pkg}/notebooks/inline-extend/edit`);
      const refusal = page.getByRole("alert");
      await expect(refusal).toContainText(
         "This notebook cannot be opened in the builder",
         { timeout: 60_000 },
      );
      await expect(refusal).toContainText("extends its source inline");
      await expect(refusal).toContainText(/line \d+/i);
      await expect(
         page.getByRole("button", { name: "Save changes" }),
      ).toHaveCount(0);

      // The read view is unaffected by the refusal.
      await page.goto(`/${pe.env}/${pe.pkg}/notebooks/inline-extend`);
      await expect(page.getByText("Unconvertible notebook prose.")).toBeVisible(
         { timeout: 60_000 },
      );
      await expect(page.locator("[data-malloy-render-as]")).toHaveCount(1, {
         timeout: 60_000,
      });

      // And the file was never touched.
      expect(await pe.readSource("notebooks/inline-extend.malloy")).toBe(
         NOTEBOOK,
      );
   });
});
