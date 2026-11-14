// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, type Page } from "@playwright/test";

/**
 * Save without leaving. In the Console the bar's Done saves and leaves, so a
 * save that keeps the builder open is the keyboard's: Cmd/Ctrl+S. The status
 * beside the undo buttons reads "Saved" once the write has landed, which is
 * what proves it.
 */
export async function saveChanges(page: Page): Promise<void> {
   // By its attributes rather than its role: while a dialog is open the
   // header is hidden from the accessibility tree, and a role query misses it.
   const bar = page.locator('[role="toolbar"][aria-label="Report builder"]');
   if ((await bar.count()) > 0) {
      await page.keyboard.press("ControlOrMeta+s");
      await expect(bar.getByRole("status")).toHaveText("Saved", {
         timeout: 30_000,
      });
      return;
   }
   // A host that keeps the builder's own toolbar on the page (the embedded
   // harness): Save turns into a disabled "Saved" once the write lands.
   await page.getByRole("button", { name: "Save", exact: true }).click();
   await expect(
      page.getByRole("button", { name: "Saved", exact: true }),
   ).toBeVisible({ timeout: 30_000 });
}

/**
 * There are edits the file does not have yet. The Console keeps them as a
 * draft in this browser, and the bar's status says so; a host that keeps the
 * builder's own toolbar shows an enabled Save instead.
 */
export async function expectUnsaved(page: Page): Promise<void> {
   const bar = page.locator('[role="toolbar"][aria-label="Report builder"]');
   if ((await bar.count()) > 0) {
      await expect(bar.getByRole("status")).toHaveText(
         /Draft saved|Unsaved changes/,
      );
      return;
   }
   await expect(
      page.getByRole("button", { name: "Save", exact: true }),
   ).toBeEnabled();
}
