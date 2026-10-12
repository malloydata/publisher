// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";

/**
 * A report route whose listing request gets no answer says so and offers a
 * retry, rather than drawing "nothing is here": the listing is cached for
 * every reader, so an empty one standing in for a failure would be believed.
 */
test("a report route whose listing fails says so, and Retry opens the report", async ({
   page,
}) => {
   let fail = true;
   await page.route(
      /\/api\/v0\/environments\/[^/]+\/packages\/[^/]+\/reports(\?.*)?$/,
      (route) => (fail ? route.abort("failed") : route.continue()),
   );

   await page.goto(`/${DEFAULT_ENV}/${PACKAGES.storefront}/category-review`);
   await expect(
      page.getByText(/Could not list this package's reports/),
   ).toBeVisible({ timeout: 60_000 });

   fail = false;
   await page.getByRole("button", { name: "Retry" }).click();
   await expect(
      page.getByRole("heading", { name: "Category review", level: 1 }),
   ).toBeVisible({ timeout: 30_000 });
   await expect(page.getByText(/Could not list/)).toHaveCount(0);
});
