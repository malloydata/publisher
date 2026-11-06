// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, Page, Request, test } from "@playwright/test";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";
import { requireModelViewer } from "./helpers/publisherStatus";

/**
 * The model viewer's givens, on a server with `PUBLISHER_MODEL_PAGE=viewer`
 * (skipped on any other; see `requireModelViewer`), on the storefront's
 * `overview.malloy`, whose `overview` source reads five givens with select and
 * multiselect pickers.
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
   test.beforeAll(async ({ baseURL }) => {
      await requireModelViewer(baseURL!);
   });

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

   test("names a run's defaults as its controls do, where the source reads them", async ({
      page,
   }) => {
      // `overview` is built on the imported `order_items` and reads all five
      // givens in its own text, so the page can tell, and says nothing more.
      await openOverview(page, "&REGION=West");
      await expect(page.getByTestId("model-givens-unknown")).toHaveCount(0);
      await page
         .getByRole("region", { name: "Views" })
         .getByRole("button", { name: /best_sellers/ })
         .click();
      const note = page.getByTestId("model-viewer-run-defaults");
      await expect(note).toHaveText(
         "Ran with the defaults Category: All, Brand: All, Ordered since: Jan 1, 2023, Minimum line total: Any.",
      );
   });

   test("says when it can't tell whether a source reads the givens it shows", async ({
      page,
   }) => {
      // `order_items` is declared in storefront.malloy, which overview.malloy
      // imports: the page can't see what it reads.
      await page.goto(`${MODEL}?source=order_items&REGION=West`);
      await expect(
         page.getByRole("heading", { name: "order_items", level: 1 }),
      ).toBeVisible();
      await expect(page.getByTestId("model-givens-unknown")).toHaveText(
         "This page can't tell which of these givens this source reads, so a value set here may not apply to it.",
      );
      await page
         .getByRole("region", { name: "Views" })
         .getByRole("button", { name: /top_products/ })
         .click();
      await expect(page.getByTestId("model-viewer-run-defaults")).toHaveText(
         /^This page can't tell which givens this source reads\. Where it reads one, it ran with Region: West and the defaults /,
      );
   });

   test("lists the givens of a model that declares nothing else", async ({
      page,
   }) => {
      await page.goto(`/${DEFAULT_ENV}/${PACKAGES.storefront}/givens.malloy`);
      await expect(
         page.getByText("This model defines no sources or queries."),
      ).toBeVisible();
      const declared = page.getByTestId("model-viewer-declared-givens");
      await expect(declared).toContainText("CATEGORY");
      await expect(declared).toContainText("Category");
   });
});
