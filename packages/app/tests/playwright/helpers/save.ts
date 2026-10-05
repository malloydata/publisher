// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, type Page } from "@playwright/test";

/**
 * Save through the editor toolbar. A save writes at once and then shows a
 * notice offering View change and Undo save; that notice is what proves the
 * write landed, and it stays until the next edit, so nothing here dismisses it.
 */
export async function saveChanges(page: Page): Promise<void> {
   await page.getByRole("button", { name: "Save changes" }).click();
   await expect(
      page.getByRole("button", { name: "Saved", exact: true }),
   ).toBeVisible({ timeout: 30_000 });
   await expect(page.getByRole("button", { name: "Undo save" })).toBeVisible({
      timeout: 30_000,
   });
}

/** Takes the last save back from its notice and waits for the notice to say so. */
export async function undoSave(page: Page): Promise<void> {
   await page.getByRole("button", { name: "Undo save" }).click();
   await expect(page.getByText("Save undone.")).toBeVisible({
      timeout: 30_000,
   });
}
