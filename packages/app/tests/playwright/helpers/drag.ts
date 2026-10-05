// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Locator, Page } from "@playwright/test";

/**
 * A real pointer drag from one grip onto another element: the sensors the
 * builders use only react to pointer events, so a synthetic drop would pass
 * without exercising the reorder at all.
 */
export async function dragGrip(
   page: Page,
   grip: Locator,
   onto: Locator,
   where: "top" | "center" | "bottom" = "center",
): Promise<void> {
   await animationsDone(page);
   await grip.scrollIntoViewIfNeeded();
   const from = await grip.boundingBox();
   const target = await onto.boundingBox();
   if (!from || !target) throw new Error("drag endpoints are not on screen");
   const startX = from.x + from.width / 2;
   const startY = from.y + from.height / 2;
   const endX = target.x + Math.min(40, target.width / 2);
   const endY =
      where === "top"
         ? target.y + 2
         : where === "bottom"
           ? target.y + target.height - 2
           : target.y + target.height / 2;
   await page.mouse.move(startX, startY);
   await page.mouse.down();
   for (let step = 1; step <= 12; step++) {
      await page.mouse.move(
         startX + ((endX - startX) * step) / 12,
         startY + ((endY - startY) * step) / 12,
         { steps: 2 },
      );
   }
   await page.mouse.up();
   await animationsDone(page);
}

/** The sortable slides cells into place for a moment after a drop; a drag begun inside that window grabs a moving target. */
async function animationsDone(page: Page): Promise<void> {
   await page.evaluate(async () => {
      await new Promise((resolve) => requestAnimationFrame(resolve));
      await Promise.all(
         document.getAnimations().map((a) => a.finished.catch(() => undefined)),
      );
   });
}
