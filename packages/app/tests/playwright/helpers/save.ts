// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, type Page } from "@playwright/test";

/**
 * Save through the editor toolbar. A change that adds or removes a cell or a
 * tile stops at a review dialog first; one that does not goes straight
 * through, so the dialog is confirmed only when it appears.
 */
export async function saveChanges(page: Page): Promise<void> {
   await page.getByRole("button", { name: "Save changes" }).click();
   const confirm = page.getByRole("button", { name: "Save this" });
   const saved = page.getByRole("button", { name: "Saved", exact: true });
   // Whichever shows first decides: waiting out a fixed delay for a dialog that never opens only slows the save.
   await confirm
      .or(saved)
      .first()
      .waitFor({ state: "visible", timeout: 30_000 });
   if (await confirm.isVisible()) await confirm.click();
   await expect(saved).toBeVisible({ timeout: 30_000 });
}
