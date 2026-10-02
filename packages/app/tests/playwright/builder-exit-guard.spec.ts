// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test, type Page } from "@playwright/test";
import { saveChanges } from "./helpers/save";
import {
   registerPackageEnv,
   serverFixture,
   type PackageEnv,
} from "./helpers/packageEnv";

/**
 * Leaving the notebook editor: the unsaved-changes prompt by Back and by Done
 * editing, the ways out that must not prompt, and the text cell's Cancel and
 * keyboard commit.
 */

const PKG = "notebooks-malloyyo";
const TOUR = "notebooks/browser_tour.malloy";

const TOUR_SOURCE = `##! experimental.givens
## artifact { kind=notebook title="Browser tour" }
import "../models/orders.malloy"

##(markdown) First note.

##(markdown) Second note.

run: orders -> kpis
`;

let pe: PackageEnv;

test.describe("notebook exit guard", () => {
   // eslint-disable-next-line no-empty-pattern
   test.beforeEach(async ({}, testInfo) => {
      pe = await registerPackageEnv(
         testInfo.project.use.baseURL ?? "http://localhost:4000",
         "nbguard",
         serverFixture(PKG),
         PKG,
         { [TOUR]: TOUR_SOURCE },
      );
   });
   test.afterEach(async () => {
      await pe?.dispose();
   });

   const readerUrl = () =>
      new RegExp(`/${pe.env}/${pe.pkg}/notebooks/browser_tour$`);

   // Through the reader's Edit button, so Back has an in-app entry to return to.
   const openEditor = async (page: Page) => {
      await page.goto(`/${pe.env}/${pe.pkg}/notebooks/browser_tour`);
      await expect(page.getByText("First note.")).toBeVisible({
         timeout: 60_000,
      });
      await page.getByRole("button", { name: "Edit", exact: true }).click();
      await expect(page.getByText("Editing", { exact: true })).toBeVisible({
         timeout: 60_000,
      });
   };

   const prompt = (page: Page) =>
      page.getByRole("dialog", { name: "Leave with unsaved changes?" });

   const markdownField = (page: Page) =>
      page.getByLabel("Markdown", { exact: true });

   /** Rewrites the first note and commits it with Done, leaving the notebook dirty. */
   const dirtyFirstNote = async (page: Page, text = "Edited first.") => {
      await page.getByRole("button", { name: "Edit text" }).first().click();
      await markdownField(page).fill(text);
      await page.getByRole("button", { name: "Done", exact: true }).click();
      await expect(page.getByText(text)).toBeVisible();
   };

   const goBack = (page: Page) => page.evaluate(() => history.back());

   test("Back with a text draft still open asks first, and Keep editing keeps the draft", async ({
      page,
   }) => {
      await openEditor(page);
      await page.getByRole("button", { name: "Edit text" }).first().click();
      await markdownField(page).fill("Half-typed thought");

      await goBack(page);
      await expect(prompt(page)).toBeVisible();
      await expect(page).toHaveURL(/\/edit$/);

      await prompt(page).getByRole("button", { name: "Keep editing" }).click();
      await expect(prompt(page)).toHaveCount(0);
      await expect(page).toHaveURL(/\/edit$/);
      await expect(page.getByText("Half-typed thought")).toBeVisible();
   });

   test("Back with a text draft open and Discard changes leaves", async ({
      page,
   }) => {
      await openEditor(page);
      await page.getByRole("button", { name: "Edit text" }).first().click();
      await markdownField(page).fill("Half-typed thought");

      await goBack(page);
      await prompt(page)
         .getByRole("button", { name: "Discard changes" })
         .click();
      await expect(page).toHaveURL(readerUrl());
      await expect(prompt(page)).toHaveCount(0);
      expect(await pe.readSource(TOUR)).not.toContain("Half-typed thought");
   });

   test("Done editing with unsaved edits asks, and Discard changes exits with no second prompt", async ({
      page,
   }) => {
      await openEditor(page);
      await dirtyFirstNote(page);

      await page.getByRole("button", { name: "Done editing" }).click();
      await expect(prompt(page)).toBeVisible();
      await prompt(page)
         .getByRole("button", { name: "Discard changes" })
         .click();

      await expect(page).toHaveURL(readerUrl());
      await expect(prompt(page)).toHaveCount(0);
      await expect(page.getByText("First note.")).toBeVisible({
         timeout: 60_000,
      });
      expect(await pe.readSource(TOUR)).not.toContain("Edited first.");
   });

   test("Tab to a text cell's Done, then Done editing, still asks and Save and exit keeps the draft", async ({
      page,
   }) => {
      await openEditor(page);
      await page.getByRole("button", { name: "Edit text" }).first().click();
      await markdownField(page).fill("Tabbed draft");
      await markdownField(page).press("Tab");
      await expect(
         page.getByRole("button", { name: "Done", exact: true }),
      ).toBeFocused();

      await page.getByRole("button", { name: "Done editing" }).click();
      await expect(prompt(page)).toBeVisible();
      await prompt(page).getByRole("button", { name: "Save and exit" }).click();

      await expect(page).toHaveURL(readerUrl(), { timeout: 30_000 });
      expect(await pe.readSource(TOUR)).toContain("Tabbed draft");
   });

   test("Done editing then Save and exit writes the file and leaves with no prompt after", async ({
      page,
   }) => {
      await openEditor(page);
      await dirtyFirstNote(page);

      await page.getByRole("button", { name: "Done editing" }).click();
      await prompt(page).getByRole("button", { name: "Save and exit" }).click();

      await expect(page).toHaveURL(readerUrl(), { timeout: 30_000 });
      await expect(prompt(page)).toHaveCount(0);
      await expect(page.getByText("Edited first.")).toBeVisible({
         timeout: 60_000,
      });
      expect(await pe.readSource(TOUR)).toContain("Edited first.");
   });

   test("Save then Back leaves without a prompt", async ({ page }) => {
      await openEditor(page);
      await dirtyFirstNote(page);
      await saveChanges(page);

      await goBack(page);
      await expect(page).toHaveURL(readerUrl());
      await expect(prompt(page)).toHaveCount(0);
   });

   test("Back from an untouched editor leaves without a prompt", async ({
      page,
   }) => {
      await openEditor(page);

      await goBack(page);
      await expect(page).toHaveURL(readerUrl());
      await expect(prompt(page)).toHaveCount(0);
   });

   test("a text cell's Cancel drops the draft", async ({ page }) => {
      await openEditor(page);
      await page.getByRole("button", { name: "Edit text" }).first().click();
      await markdownField(page).fill("Dropped draft");
      await page.getByRole("button", { name: "Cancel", exact: true }).click();

      await expect(markdownField(page)).toHaveCount(0);
      await expect(page.getByText("Dropped draft")).toHaveCount(0);
      await expect(page.getByText("First note.")).toBeVisible();

      // Nothing was left unsaved, so leaving does not ask.
      await goBack(page);
      await expect(page).toHaveURL(readerUrl());
      await expect(prompt(page)).toHaveCount(0);
   });

   test("Ctrl+Enter in a text cell commits the draft", async ({ page }) => {
      await openEditor(page);
      await page.getByRole("button", { name: "Edit text" }).first().click();
      await markdownField(page).fill("Committed by keyboard");
      await markdownField(page).press("Control+Enter");

      await expect(markdownField(page)).toHaveCount(0);
      await expect(page.getByText("Committed by keyboard")).toBeVisible();
      await expect(
         page.getByRole("button", { name: "Save changes" }),
      ).toBeEnabled();
   });
});
