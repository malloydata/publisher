// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, Page, Request, test } from "@playwright/test";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";

/**
 * The model page's givens, on the storefront's `overview.malloy`, whose
 * `overview` source reads five givens with select and multiselect pickers.
 *
 * Two things the page promises: it sends nothing until the reader opens a row
 * or a picker (each picker's options are a query of their own), and the given
 * values are in the address, so a link opens with them applied and Copy link
 * carries what the page is running with.
 */

const MODEL = `/${DEFAULT_ENV}/${PACKAGES.storefront}/overview.malloy`;

function queryPosts(page: Page): Request[] {
   const sent: Request[] = [];
   page.on("request", (request) => {
      if (request.method() === "POST" && request.url().includes("/query")) {
         sent.push(request);
      }
   });
   return sent;
}

async function openOverview(page: Page, search: string): Promise<void> {
   await page.goto(`${MODEL}?source=overview${search}`);
   await expect(
      page.getByRole("heading", { name: "overview", level: 1 }),
   ).toBeVisible();
}

test.describe("model-page-givens", () => {
   test("sends nothing on render, and reads a picker's options when it is opened", async ({
      page,
   }) => {
      const sent = queryPosts(page);
      await openOverview(page, "");
      const givens = page.getByTestId("model-givens-section");
      await expect(
         givens.getByRole("button", { name: /^Region/ }),
      ).toBeVisible();
      await page.waitForLoadState("networkidle");
      expect(sent).toHaveLength(0);

      await givens.getByRole("button", { name: /^Region/ }).click();
      await expect.poll(() => sent.length).toBe(1);
      expect(sent[0].postData()).toContain("group_by: `region`");
   });

   test("applies a link's given values and keeps them in the address", async ({
      page,
   }) => {
      const sent = queryPosts(page);
      await openOverview(page, "&REGION=West");
      const givens = page.getByTestId("model-givens-section");
      await expect(
         givens.getByRole("button", { name: /^Region: .*West/ }),
      ).toBeVisible();

      // A view runs with the link's value, sent as a given and never as text.
      await page
         .getByRole("region", { name: "Views" })
         .getByRole("button", { name: /best_sellers/ })
         .click();
      await expect.poll(() => sent.length).toBe(1);
      const body = sent[0].postDataJSON() as {
         query: string;
         givens?: Record<string, unknown>;
      };
      expect(body.givens).toMatchObject({ REGION: "West" });
      expect(body.query).not.toContain("West");

      // Reset takes the value out of the address as well as off the page.
      await givens.getByRole("button", { name: "Reset" }).click();
      await expect(page).not.toHaveURL(/REGION=/);
      await expect(page).toHaveURL(/source=overview/);
   });
});
