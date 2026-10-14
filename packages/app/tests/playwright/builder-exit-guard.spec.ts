// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test, type Page } from "@playwright/test";
import { editorOpen, tileByKey } from "./helpers/builder";
import { saveChanges } from "./helpers/save";
import {
   registerPackageEnv,
   serverFixture,
   type PackageEnv,
} from "./helpers/packageEnv";

/**
 * Leaving the notebook editor. Edits are kept in this browser as they are
 * made, so leaving by Back loses nothing and asks nothing, and opening the
 * editor again puts them back; Done saves them and shows the report. The
 * unsaved-changes prompt is left for text still being typed into an open
 * field, which is not an edit yet. Also a text tile's Escape and keyboard
 * commit.
 */

const PKG = "notebooks-malloyyo";
const TOUR = "notebooks/browser_tour.malloy";

const TOUR_SOURCE = `##! experimental.givens
## artifact { layout=linear title="Browser tour" tiles=[first { kind=text }, second { kind=text }, "orders -> kpis"] }
import "../models/orders.malloy"

##|(markdown) first
First note.
|##

##|(markdown) second
Second note.
|##
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
      await editorOpen(page);
   };

   const prompt = (page: Page) =>
      page.getByRole("dialog", { name: "Leave with unsaved changes?" });

   const markdownField = (page: Page) =>
      page.getByLabel("Markdown", { exact: true });

   /** The header bar's Done, which returns to the read-only page. */
   const view = (page: Page) =>
      page
         .getByRole("toolbar", { name: "Report builder" })
         .getByRole("button", { name: "Done", exact: true });

   /** Opens the first note's field. */
   const openFirstNote = (page: Page) =>
      tileByKey(page, "text.first").locator('[role="button"]').first().click();

   /** Rewrites the first note and commits it with ⌘/Ctrl+Enter, leaving the notebook dirty. */
   const dirtyFirstNote = async (page: Page, text = "Edited first.") => {
      await openFirstNote(page);
      await markdownField(page).fill(text);
      await markdownField(page).press("ControlOrMeta+Enter");
      await expect(page.getByText(text)).toBeVisible();
   };

   const goBack = (page: Page) => page.evaluate(() => history.back());

   test("Back with a text draft still open asks first, and Keep editing keeps the draft", async ({
      page,
   }) => {
      await openEditor(page);
      await openFirstNote(page);
      await markdownField(page).fill("Half-typed thought");

      await goBack(page);
      await expect(prompt(page)).toBeVisible();
      await expect(page).toHaveURL(/\?edit$/);

      await prompt(page).getByRole("button", { name: "Keep editing" }).click();
      await expect(prompt(page)).toHaveCount(0);
      await expect(page).toHaveURL(/\?edit$/);
      await expect(page.getByText("Half-typed thought")).toBeVisible();
   });

   test("Back with a text draft open and Discard changes leaves", async ({
      page,
   }) => {
      await openEditor(page);
      await openFirstNote(page);
      await markdownField(page).fill("Half-typed thought");

      await goBack(page);
      await prompt(page)
         .getByRole("button", { name: "Discard changes" })
         .click();
      await expect(page).toHaveURL(readerUrl());
      await expect(prompt(page)).toHaveCount(0);
      expect(await pe.readSource(TOUR)).not.toContain("Half-typed thought");
   });

   /** The bar's status line: Saved, Draft saved, Saving…. */
   const status = (page: Page) =>
      page.getByRole("toolbar", { name: "Report builder" }).getByRole("status");

   test("an edit then Back leaves without asking, and editing again puts it back", async ({
      page,
   }) => {
      await openEditor(page);
      await dirtyFirstNote(page);
      await expect(status(page)).toHaveText("Draft saved");

      await goBack(page);
      await expect(page).toHaveURL(readerUrl());
      await expect(prompt(page)).toHaveCount(0);
      expect(await pe.readSource(TOUR)).not.toContain("Edited first.");

      await page.getByRole("button", { name: "Edit", exact: true }).click();
      await editorOpen(page);
      await expect(
         page.getByText(/Restored your unsaved edits from/),
      ).toBeVisible();
      await expect(page.getByText("Edited first.")).toBeVisible();
      await expect(status(page)).toHaveText("Draft saved");
   });

   test("Done saves the edits and shows the report, and no draft comes back", async ({
      page,
   }) => {
      await openEditor(page);
      await dirtyFirstNote(page);

      await view(page).click();
      await expect(page).toHaveURL(readerUrl(), { timeout: 30_000 });
      await expect(prompt(page)).toHaveCount(0);
      await expect(page.getByText("Edited first.")).toBeVisible({
         timeout: 60_000,
      });
      expect(await pe.readSource(TOUR)).toContain("Edited first.");

      await page.getByRole("button", { name: "Edit", exact: true }).click();
      await editorOpen(page);
      await expect(status(page)).toHaveText("Saved");
      await expect(page.getByText(/Restored your unsaved edits/)).toHaveCount(
         0,
      );
   });

   test("Done with a text field open keeps its text, saves, and shows the report", async ({
      page,
   }) => {
      await openEditor(page);
      await openFirstNote(page);
      await markdownField(page).fill("Draft then Done");

      await view(page).click();
      await expect(page).toHaveURL(readerUrl(), { timeout: 30_000 });
      await expect(prompt(page)).toHaveCount(0);
      expect(await pe.readSource(TOUR)).toContain("Draft then Done");
   });

   test("Discard edits from the status puts the saved report back", async ({
      page,
   }) => {
      await openEditor(page);
      await dirtyFirstNote(page);

      await status(page).click();
      await page.getByRole("menuitem", { name: /Discard edits/ }).click();
      await expect(page.getByText("First note.")).toBeVisible();
      await expect(page.getByText("Edited first.")).toHaveCount(0);
      await expect(status(page)).toHaveText("Saved");

      await goBack(page);
      await expect(page).toHaveURL(readerUrl());
      await expect(prompt(page)).toHaveCount(0);
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

   test("Escape in a text tile drops the draft", async ({ page }) => {
      await openEditor(page);
      await openFirstNote(page);
      await markdownField(page).fill("Dropped draft");
      await markdownField(page).press("Escape");

      await expect(markdownField(page)).toHaveCount(0);
      await expect(page.getByText("Dropped draft")).toHaveCount(0);
      await expect(page.getByText("First note.")).toBeVisible();

      // Nothing was left unsaved, so leaving does not ask.
      await goBack(page);
      await expect(page).toHaveURL(readerUrl());
      await expect(prompt(page)).toHaveCount(0);
   });

   test("Ctrl+Enter in a text tile commits the draft", async ({ page }) => {
      await openEditor(page);
      await openFirstNote(page);
      await markdownField(page).fill("Committed by keyboard");
      await markdownField(page).press("Control+Enter");

      await expect(markdownField(page)).toHaveCount(0);
      await expect(page.getByText("Committed by keyboard")).toBeVisible();
      await expect(status(page)).toHaveText("Draft saved");
   });
});
