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

/**
 * The reader is back and the builder gone. Leaving the builder is a handoff:
 * the reader draws out of sight under it first, so for a moment the page holds
 * both, and a text locator would find each line twice.
 */
export async function readerOpen(page: Page): Promise<void> {
   // One condition, read at once: checked one after the other, the first
   // could pass before the switch began and the second once it was under way.
   await page.waitForFunction(
      () =>
         document.querySelector("[data-switching]") === null &&
         document.querySelector('[aria-label="Undo"]') === null &&
         [...document.querySelectorAll("button")].some(
            (button) => button.textContent?.trim() === "Edit",
         ),
      null,
      { timeout: 30_000 },
   );
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

/** Opens a text tile's markdown field, replaces its text, and commits it with ⌘/Ctrl+Enter. */
export async function editText(tile: Locator, text: string): Promise<void> {
   await tile.locator('[role="button"]').first().click();
   const page = tile.page();
   await page.getByLabel("Markdown", { exact: true }).fill(text);
   await page
      .getByLabel("Markdown", { exact: true })
      .press("ControlOrMeta+Enter");
}

/** The builder's bar in the header, where its page-level controls are. */
export const builderBar = (page: Page): Locator =>
   page.getByRole("toolbar", { name: "Report builder" });

/** Picks one item of the bar's Add menu: a tile (opens the picker), text, or a filter. */
export async function addFromBar(
   page: Page,
   what: "Tile" | "Text" | "Filter",
): Promise<void> {
   await builderBar(page)
      .getByRole("button", { name: "Add", exact: true })
      .click();
   await page
      .getByRole("menuitem")
      .filter({ hasText: new RegExp(`^${what}`) })
      .click();
}

/** Opens the Add a tile picker from the bar's Add menu. */
export async function openAddTile(page: Page): Promise<void> {
   await addFromBar(page, "Tile");
   await expect(page.getByRole("dialog", { name: "Add a tile" })).toBeVisible();
}

/** Adds an empty text tile from the bar's Add menu. */
export async function addTextTile(page: Page): Promise<void> {
   await addFromBar(page, "Text");
}

/** Sets a tile's chart from its ⋯ menu's Chart submenu, then closes the menu so the choice commits. */
export async function pickChart(
   page: Page,
   tile: Locator,
   option: "Bar" | "From the view" | "Table",
): Promise<void> {
   await tile.getByLabel(/^Settings for /).click();
   await page.getByRole("menuitem", { name: /^Chart/ }).click();
   await page
      .getByRole("menu", { name: "Chart" })
      .getByRole("menuitemradio")
      .filter({ hasText: new RegExp(`^${option}$`) })
      .click();
   // The submenu closes on the pick; Escape closes the tile menu, which commits.
   await page.keyboard.press("Escape");
}

/**
 * Creates a report through the package page's New report action and returns
 * its route: its package path without `.malloy`, which at the package root
 * (where New report writes by default) is its slug. `kind` picks the layout:
 * `Dashboard` is grid, `Notebook` linear.
 */
export async function createDocument(
   page: Page,
   pe: PackageEnv,
   kind: "Dashboard" | "Notebook",
   title: string,
): Promise<string> {
   await page.goto(`/${pe.env}/${pe.pkg}`);
   await page
      .getByRole("button", { name: "Add report", exact: true })
      .first()
      .click({
         timeout: 60_000,
      });
   const dialog = page.getByRole("dialog");
   await expect(dialog.getByLabel("Report title")).not.toHaveValue("", {
      timeout: 30_000,
   });
   await chooseLayout(dialog, kind === "Dashboard" ? "Grid" : "Linear");
   await pickView(dialog, "sales_by_month", "order_items in storefront.malloy");
   await dialog.getByLabel("Report title").fill(title);
   await dialog.getByRole("button", { name: "Create report" }).click();
   // At the package root: no directory is created for a new report.
   await expect(page).toHaveURL(
      new RegExp(`/${pe.env}/${pe.pkg}/[a-z0-9-]+\\?edit$`),
      { timeout: 60_000 },
   );
   await editorOpen(page);
   const slug = new URL(page.url()).pathname.split("/").slice(-1)[0]!;
   // Deduplication changes the slug on a repeat run, never its prefix.
   expect(slug.startsWith(title.toLowerCase().replace(/ /g, "-"))).toBe(true);
   return slug;
}

/**
 * Picks a view in the Add a tile window by its Malloy name, inside one
 * source's group when `source` is given (several sources can offer a view of
 * the same name).
 */
export async function pickView(
   dialog: Locator,
   view: string,
   source?: string,
): Promise<void> {
   const scope = source
      ? dialog.getByRole("group", { name: source, exact: true })
      : dialog;
   const escaped = view.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
   await scope
      .getByRole("option", { name: new RegExp(`\\(${escaped}\\)$`) })
      .first()
      .click();
}

/** Chooses the layout in the New report window. */
export async function chooseLayout(
   dialog: Locator,
   layout: "Linear" | "Grid",
): Promise<void> {
   await dialog.getByRole("radio", { name: layout, exact: true }).click();
}
