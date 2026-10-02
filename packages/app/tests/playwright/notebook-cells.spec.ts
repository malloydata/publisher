// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test, type Page } from "@playwright/test";
import { createHash } from "crypto";
import {
   addTextTile,
   editText,
   editorOpen,
   tileByKey,
   tileOrder,
} from "./helpers/builder";
import { dragGrip } from "./helpers/drag";
import { saveChanges } from "./helpers/save";
import {
   registerPackageEnv,
   serverFixture,
   type PackageEnv,
} from "./helpers/packageEnv";

/**
 * The notebook editor's tile operations against a real server: reorder by
 * pointer, add and remove a text tile, prose that mentions a gate, a file that
 * changed underneath the editor, and starting givens that live in the artifact
 * tag. The tour is a layout notebook; `autorun_off` stays in the cell format,
 * which the editor converts on open.
 */

const PKG = "notebooks-malloyyo";
const TOUR = "notebooks/browser_tour.malloy";

const TOUR_SOURCE = `##! experimental.givens
## artifact { kind=notebook title="Browser tour" tiles=[first { kind=text }, second { kind=text }, third { kind=text }, "orders -> kpis", fourth { kind=text }, "orders -> by_month"] }
import "../models/orders.malloy"

##|(markdown) first
First note.
|##

// A comment that belongs to the second note.
##|(markdown) second
Second note.
|##

##|(markdown) third
Third note.
|##

##|(markdown) fourth
Fourth note.
|##
`;

const AUTORUN_OFF = `##! experimental.givens
## artifact { kind=notebook title="Autorun off" autorun=false givens { REGION=f'US' } }
import "../models/orders.malloy"

# label="Region" control=select suggest { source=orders dimension=region }
given: REGION :: filter<string> is f''

##(markdown) Pick a region, then apply.

run: orders -> by_month + { where: region ~ $REGION }
`;

let pe: PackageEnv;

test.describe("notebook tiles", () => {
   // Tall enough that a drag's target tile is on screen, since a pointer cannot drop onto what is scrolled away.
   test.use({ viewport: { width: 1280, height: 1600 } });

   // eslint-disable-next-line no-empty-pattern
   test.beforeEach(async ({}, testInfo) => {
      pe = await registerPackageEnv(
         testInfo.project.use.baseURL ?? "http://localhost:4000",
         "nbcells",
         serverFixture(PKG),
         PKG,
         {
            [TOUR]: TOUR_SOURCE,
            "notebooks/autorun_off.malloy": AUTORUN_OFF,
         },
      );
   });
   test.afterEach(async () => {
      await pe?.dispose();
   });

   const open = async (page: Page, slug = "browser_tour") => {
      await page.goto(`/${pe.env}/${pe.pkg}/notebooks/${slug}/edit`);
      await editorOpen(page);
   };

   const textKeys = (keys: string[]) =>
      keys.filter((key) => key.startsWith("text."));

   /** The notes in the order the page shows them, by the tile's name. */
   const noteOrder = async (page: Page) =>
      textKeys(await tileOrder(page)).map((key) => key.slice("text.".length));

   /** Order of the text tiles in the file's `tiles=[…]` list, which is what a reader renders. */
   const fileOrder = (text: string) =>
      [
         ...text.matchAll(
            /\b(first|second|third|fourth|text_\d+)\s*\{\s*kind=text/g,
         ),
      ].map((m) => m[1]!);

   const settled = async (page: Page, charts: number) => {
      await expect(page.locator("[data-malloy-render-as]")).toHaveCount(
         charts,
         { timeout: 60_000 },
      );
   };

   const grip = (page: Page, name: string) =>
      tileByKey(page, `text.${name}`).getByLabel(`Move ${name}`);

   test("a pointer drag reorders the tiles, and the file follows", async ({
      page,
   }) => {
      await open(page);
      await settled(page, 2);
      expect(await noteOrder(page)).toEqual([
         "first",
         "second",
         "third",
         "fourth",
      ]);

      // Third note onto the first.
      await dragGrip(page, grip(page, "third"), tileByKey(page, "text.first"));
      await expect
         .poll(() => noteOrder(page))
         .toEqual(["third", "first", "second", "fourth"]);
      await settled(page, 2);

      // The page still takes a drag after one.
      await dragGrip(page, grip(page, "fourth"), tileByKey(page, "text.first"));
      await expect
         .poll(() => noteOrder(page))
         .toEqual(["third", "fourth", "first", "second"]);
      await settled(page, 2);

      await saveChanges(page);

      const after = await pe.readSource(TOUR);
      expect(fileOrder(after)).toEqual(["third", "fourth", "first", "second"]);
      // The comment kept its block when the tile moved.
      expect(after).toContain(
         "// A comment that belongs to the second note.\n##|(markdown) second",
      );
      expect(after).toContain("Third note.");
   });

   test("removing a text tile takes its block with it; adding one lands in the file", async ({
      page,
   }) => {
      await open(page);
      await settled(page, 2);

      await tileByKey(page, "text.second")
         .getByLabel("Settings for second")
         .click();
      await page.getByRole("button", { name: "Remove tile" }).click();
      await saveChanges(page);
      // The drag-and-drop live region is a status too, so pick this one by its text.
      await expect(
         page.getByRole("status").filter({ hasText: "Removed 1 tile" }),
      ).toBeVisible();
      let file = await pe.readSource(TOUR);
      expect(file).not.toContain("Second note");
      expect(file).not.toContain("second { kind=text }");

      // A new text tile goes at the end, and is written where it is shown.
      await addTextTile(page);
      await editText(tileByKey(page, "text.text_1"), "Added note.");
      await saveChanges(page);
      file = await pe.readSource(TOUR);
      expect(file).toContain("Added note.");
      expect(fileOrder(file)).toEqual(["first", "third", "fourth", "text_1"]);
      await expect
         .poll(() => noteOrder(page))
         .toEqual(["first", "third", "fourth", "text_1"]);
   });

   test("prose that names #(authorize) saves, one line or several, and the notebook still serves", async ({
      page,
   }) => {
      await open(page);
      const first = tileByKey(page, "text.first");

      // Prose is not a gate: the server takes the words in either form.
      await editText(first, "A one-liner naming #(authorize) in prose.");
      await saveChanges(page);
      let file = await pe.readSource(TOUR);
      expect(file).toContain("A one-liner naming #(authorize) in prose.");

      await editText(
         first,
         "Two lines.\nThe second names #(authorize) in prose.",
      );
      await saveChanges(page);
      file = await pe.readSource(TOUR);
      expect(file).toContain("The second names #(authorize) in prose.");
      expect(file).not.toContain("A one-liner");
      // And the notebook still serves, queries and all.
      await page.goto(`/${pe.env}/${pe.pkg}/notebooks/browser_tour`);
      await expect(page.getByText("The second names")).toBeVisible({
         timeout: 60_000,
      });
      await expect(page.locator("[data-malloy-render-as]")).toHaveCount(2, {
         timeout: 60_000,
      });
   });

   test("a save over a file that changed underneath is refused and nothing is overwritten", async ({
      page,
   }) => {
      await open(page);
      const before = await pe.readSource(TOUR);
      const elsewhere = before.replace(
         "Fourth note.",
         "Fourth note, changed elsewhere.",
      );
      const res = await fetch(
         `${pe.baseURL}/api/v0/environments/${pe.env}/packages/${pe.pkg}/models/${encodeURIComponent(TOUR)}`,
         {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
               source: elsewhere,
               expectedHash: createHash("sha256").update(before).digest("hex"),
            }),
         },
      );
      expect(res.ok, await res.text()).toBe(true);

      await editText(tileByKey(page, "text.first"), "Mine.");
      await page.getByRole("button", { name: "Save changes" }).click();
      await expect(page.getByRole("alert").first()).toBeVisible({
         timeout: 30_000,
      });
      const file = await pe.readSource(TOUR);
      expect(file).toContain("Fourth note, changed elsewhere.");
      expect(file).not.toContain("Mine.");
      // The edit is still in the editor, ready to be re-applied.
      await expect(page.getByText("Mine.")).toBeVisible();
   });

   test("the reader shows a saved edit without a reload", async ({ page }) => {
      await page.goto(`/${pe.env}/${pe.pkg}/notebooks/browser_tour`);
      await expect(page.getByText("First note.")).toBeVisible({
         timeout: 60_000,
      });
      await page.getByRole("button", { name: "Edit", exact: true }).click();
      await editorOpen(page);
      await editText(tileByKey(page, "text.first"), "Edited first.");
      await saveChanges(page);
      await page.getByRole("button", { name: "Close", exact: true }).click();
      await expect(page).toHaveURL(/\/notebooks\/browser_tour$/);
      await expect(page.getByText("Edited first.")).toBeVisible({
         timeout: 60_000,
      });
   });

   test("starting givens and autorun=false in the artifact tag reach the reader and the editor", async ({
      page,
   }) => {
      await page.goto(`/${pe.env}/${pe.pkg}/notebooks/autorun_off`);
      await expect(page.getByRole("button", { name: "Apply" })).toBeVisible({
         timeout: 60_000,
      });
      await expect(page.getByRole("button", { name: "Reset" })).toBeVisible();
      await open(page, "autorun_off");
      await expect(
         page.getByRole("button", { name: "Apply", exact: true }),
      ).toBeVisible({
         timeout: 60_000,
      });
      await expect(page.getByRole("button", { name: "Reset" })).toBeVisible();
   });
});
