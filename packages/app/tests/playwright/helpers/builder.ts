// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, type Locator, type Page } from "@playwright/test";
import type { PackageEnv } from "./packageEnv";

/** Waits for the builder to be open on a document. */
export async function editorOpen(page: Page): Promise<void> {
   await expect(page.getByRole("button", { name: "Undo" })).toBeVisible({
      timeout: 60_000,
   });
}

/** A tile by the key the builder tags it with: `text.<name>` or `<source>.<view>`. */
export const tileByKey = (page: Page, key: string): Locator =>
   page.locator(`[data-tile-key="${key}"]`);

/** Every tile, in the order the grid draws them. */
export const allTiles = (page: Page): Locator =>
   page.locator("[data-tile-key]");

/** Tile keys in the order the page shows them. */
export const tileOrder = (page: Page): Promise<string[]> =>
   allTiles(page).evaluateAll((els) =>
      els.map((el) => el.getAttribute("data-tile-key") ?? ""),
   );

/** The tiles that run a query, as opposed to text tiles. */
export const queryTiles = (page: Page): Locator =>
   page.locator('[data-tile-key]:not([data-tile-key^="text."])');

/** Opens a text tile's markdown field, replaces its text, and commits it with Done. */
export async function editText(tile: Locator, text: string): Promise<void> {
   await tile.locator('[role="button"]').first().click();
   const page = tile.page();
   await page.getByLabel("Markdown", { exact: true }).fill(text);
   await page.getByRole("button", { name: "Done", exact: true }).click();
}

/** Adds an empty text tile from the toolbar's Add tile dialog. */
export async function addTextTile(page: Page): Promise<void> {
   await page.getByRole("button", { name: "Add tile", exact: true }).click();
   const dialog = page.getByRole("dialog", { name: "Add a tile" });
   await dialog.getByRole("button", { name: "Text", exact: true }).click();
   await dialog.getByRole("button", { name: "Add text", exact: true }).click();
   await expect(dialog).toHaveCount(0);
}

/** Sets a tile's chart from its settings menu, then closes the menu so the choice commits. */
export async function pickChart(
   page: Page,
   tile: Locator,
   option: "Bar" | "From the view" | "Table",
): Promise<void> {
   await tile.getByLabel(/^Settings for /).click();
   await page.getByRole("combobox", { name: /^Viz type, / }).click();
   await page.getByRole("option", { name: option, exact: true }).click();
   await page.keyboard.press("Escape");
}

/** Creates a document through the package page's New menu and returns its slug. */
export async function createDocument(
   page: Page,
   pe: PackageEnv,
   kind: "Dashboard" | "Notebook",
   title: string,
): Promise<string> {
   await page.goto(`/${pe.env}/${pe.pkg}`);
   await page.getByRole("button", { name: "New", exact: true }).click({
      timeout: 60_000,
   });
   await page.getByRole("menuitem", { name: kind }).click();
   const dialog = page.getByRole("dialog");
   await expect(dialog.getByLabel(`${kind} title`)).not.toHaveValue("", {
      timeout: 30_000,
   });
   await dialog.getByRole("combobox", { name: "Model" }).click();
   await page.getByRole("option", { name: "storefront.malloy" }).click();
   await dialog
      .getByRole("combobox", {
         name: kind === "Dashboard" ? "First tile" : "First query",
      })
      .click();
   await page
      .getByRole("option", {
         name: "order_items → sales_by_month",
         exact: true,
      })
      .click();
   await dialog.getByLabel(`${kind} title`).fill(title);
   await dialog.getByRole("button", { name: `Create ${kind}` }).click();
   const folder = kind === "Dashboard" ? "dashboards" : "notebooks";
   await expect(page).toHaveURL(new RegExp(`/${folder}/[a-z0-9-]+/edit$`), {
      timeout: 60_000,
   });
   await editorOpen(page);
   const slug = new URL(page.url()).pathname.split("/").slice(-2)[0]!;
   // Deduplication changes the slug on a repeat run, never its prefix.
   expect(slug.startsWith(title.toLowerCase().replace(/ /g, "-"))).toBe(true);
   return slug;
}
