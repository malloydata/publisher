// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, type Page } from "@playwright/test";

/**
 * Save through the builder toolbar. Save writes in place and the builder stays
 * open; the button reads "Save" while there are unsaved edits, "Saving…" while
 * the write is in flight, and "Saved" (disabled) once it has landed, which is
 * what proves the write.
 */
export async function saveChanges(page: Page): Promise<void> {
   await page.getByRole("button", { name: "Save", exact: true }).click();
   const saved = page.getByRole("button", { name: "Saved", exact: true });
   await expect(saved).toBeVisible({ timeout: 30_000 });
}
