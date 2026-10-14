// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, type Locator, type Page } from "@playwright/test";

/**
 * A report's filter row is a row of buttons, one per filter, each named
 * `<label>: <value>` (`Brand: All`, `Ordered since: Jan 1, 2023`). The control
 * itself (picker, slider, date, text box) lives in a popover the button opens,
 * a dialog named `<label> filter`.
 */

const escape = (text: string) => text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/** A filter's button in the row. */
export const filterButton = (page: Page, label: string): Locator =>
   page.getByRole("button", { name: new RegExp(`^${escape(label)}: `) });

/** A filter's popover, open or not. */
export const filterDialog = (page: Page, label: string): Locator =>
   page.getByRole("dialog", { name: `${label} filter` });

/** Open a filter's popover (if it is not open already) and return it. */
export async function openFilter(
   page: Page,
   label: string,
   timeout = 30_000,
): Promise<Locator> {
   const dialog = filterDialog(page, label);
   if (!(await dialog.isVisible()))
      await filterButton(page, label).click({ timeout });
   await expect(dialog).toBeVisible();
   return dialog;
}

/**
 * Close whatever filter popover is open. A picker's own list closes on the
 * first Escape, the popover on the next. Each key waits for the popover to
 * leave, so no spare Escape reaches the page behind it (in the builder one
 * drops the selected tile). Escape reaches the popover only while focus is
 * inside it, which a control that removes itself (a clear ×) can end, so a
 * click outside it is the fallback.
 */
export async function closeFilter(page: Page): Promise<void> {
   const open = page.getByRole("dialog", { name: / filter$/ });
   const gone = () =>
      open
         .first()
         .waitFor({ state: "detached", timeout: 1_000 })
         .then(() => true)
         .catch(() => false);
   for (let i = 0; i < 2 && (await open.count()) > 0; i++) {
      await page.keyboard.press("Escape");
      if (await gone()) break;
   }
   if ((await open.count()) > 0) {
      await page.mouse.click(2, 2);
      await gone();
   }
   await expect(open).toHaveCount(0);
}

/** The value a filter's button shows: `Nike`, `All`, `Jan 1, 2023`. */
export async function expectFilterValue(
   page: Page,
   label: string,
   value: string,
   timeout = 30_000,
): Promise<void> {
   await expect(filterButton(page, label)).toHaveAccessibleName(
      `${label}: ${value}`,
      { timeout },
   );
}
