// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Locator, Page } from "@playwright/test";

/**
 * A real pointer drag of a tile, picked up by its card, onto another element:
 * the sensors the builders use only react to pointer events, so a synthetic
 * drop would pass without exercising the reorder at all. The whole card is the
 * drag's activator (the grip is gone): a press on the card itself, rather
 * than on a button, a link or a chart inside it, starts the move once the
 * pointer travels a few pixels.
 */
export async function dragTile(
   page: Page,
   tile: Locator,
   onto: Locator,
   where: "top" | "center" | "bottom" = "center",
): Promise<void> {
   await animationsDone(page);
   await tile.scrollIntoViewIfNeeded();
   const from = await tile.boundingBox();
   const target = await onto.boundingBox();
   if (!from || !target) throw new Error("drag endpoints are not on screen");
   // The card's own top padding, inside its edge and above its title: the
   // title is a button and the body may be a chart that takes the press, so
   // the padding is the one place that is always the card.
   const startX = from.x + from.width / 2;
   const startY = from.y + 8;
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
