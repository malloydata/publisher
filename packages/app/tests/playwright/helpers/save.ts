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
   await confirm
      .waitFor({ state: "visible", timeout: 3_000 })
      .then(() => confirm.click())
      .catch(() => undefined);
   await expect(page.getByRole("button", { name: "Saved" })).toBeVisible({
      timeout: 30_000,
   });
}
