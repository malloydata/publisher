// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, type Page } from "@playwright/test";

/**
 * Save through the builder's bar in the header. Save writes in place and the
 * builder stays open; the button shows while there are unsaved edits, and the
 * status beside the undo buttons reads "Saved" once the write has landed,
 * which is what proves it.
 */
export async function saveChanges(page: Page): Promise<void> {
   // By its attributes rather than its role: while a dialog is open the
   // header is hidden from the accessibility tree, and a role query misses it.
   const bar = page.locator('[role="toolbar"][aria-label="Report builder"]');
   if ((await bar.count()) > 0) {
      await bar.getByRole("button", { name: "Save", exact: true }).click();
      // The status says Saved once the write lands, and Save leaves the bar.
      await expect(bar.getByRole("status")).toHaveText("Saved", {
         timeout: 30_000,
      });
      await expect(
         bar.getByRole("button", { name: "Save", exact: true }),
      ).toHaveCount(0);
      return;
   }
   // A host that keeps the builder's own toolbar on the page (the embedded
   // harness): Save turns into a disabled "Saved" once the write lands.
   await page.getByRole("button", { name: "Save", exact: true }).click();
   await expect(
      page.getByRole("button", { name: "Saved", exact: true }),
   ).toBeVisible({ timeout: 30_000 });
}
