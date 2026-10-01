// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test, type Page } from "@playwright/test";
import { createHash } from "crypto";
import { dragGrip } from "./helpers/drag";
import { saveChanges } from "./helpers/save";
import {
   registerPackageEnv,
   serverFixture,
   type PackageEnv,
} from "./helpers/packageEnv";

/**
 * The notebook editor's cell operations against a real server: reorder by
 * pointer, add and remove text through the review dialog, prose that mentions
 * a gate, a file that changed underneath the editor, and starting givens that
 * live in the artifact tag.
 */

const PKG = "notebooks-malloyyo";
const TOUR = "notebooks/browser_tour.malloy";

const TOUR_SOURCE = `##! experimental.givens
## artifact { kind=notebook title="Browser tour" }
import "../models/orders.malloy"

##(markdown) First note.

// A comment that belongs to the second note.
##(markdown) Second note.

##(markdown) Third note.

run: orders -> kpis

##(markdown) Fourth note.

run: orders -> by_month
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

test.describe("notebook cells", () => {
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
      await expect(page.getByText("Editing", { exact: true })).toBeVisible({
         timeout: 60_000,
      });
   };

   const cells = (page: Page) =>
      page.getByRole("group", { name: /^Cell \d+, / });

   /** The notes, in the order they sit in the page. */
   const noteOrder = async (page: Page) => {
      const texts = await cells(page).allInnerTexts();
      return texts
         .map((t) => /(First|Second|Third|Fourth|Added) note/.exec(t)?.[1])
         .filter((n): n is string => n !== undefined);
   };

   /** Source order of the notes in the file, which is what a reader renders. */
   const fileOrder = (text: string) =>
      [...text.matchAll(/(First|Second|Third|Fourth|Added) note/g)].map(
         (m) => m[1]!,
      );

   const settled = async (page: Page, charts: number) => {
      await expect(page.locator("[data-malloy-render-as]")).toHaveCount(
         charts,
         {
            timeout: 60_000,
         },
      );
   };

   test("a pointer drag reorders the cells, a dragged definition stays put, and the file follows", async ({
      page,
   }) => {
      await open(page);
      await settled(page, 2);
      expect(await noteOrder(page)).toEqual([
         "First",
         "Second",
         "Third",
         "Fourth",
      ]);

      const first = cells(page).filter({ hasText: "First note" });
      const grip = (note: string) =>
         cells(page)
            .filter({ hasText: note })
            .getByRole("button", { name: /^Move Cell/ });

      // A definition never moves, so dropping the import elsewhere is a no-op.
      const kinds = async () =>
         (await cells(page).allInnerTexts()).map((t) =>
            t.includes("kpis") ? "kpis" : t.includes("import") ? "import" : "-",
         );
      const kindsBefore = await kinds();
      await dragGrip(
         page,
         grip("import"),
         cells(page).filter({ hasText: "Fourth note" }),
         "bottom",
      );
      expect(await kinds()).toEqual(kindsBefore);
      expect(await noteOrder(page)).toEqual([
         "First",
         "Second",
         "Third",
         "Fourth",
      ]);

      // Third note onto the first.
      await dragGrip(page, grip("Third note"), first);
      await expect
         .poll(() => noteOrder(page))
         .toEqual(["Third", "First", "Second", "Fourth"]);
      await settled(page, 2);

      // The page still takes a drag after a refused one.
      await dragGrip(page, grip("Fourth note"), first);
      await expect
         .poll(() => noteOrder(page))
         .toEqual(["Third", "Fourth", "First", "Second"]);
      await settled(page, 2);

      await saveChanges(page);

      const after = await pe.readSource(TOUR);
      expect(fileOrder(after)).toEqual(["Third", "Fourth", "First", "Second"]);
      // The comment kept its cell when it moved with it.
      expect(after).toContain(
         "// A comment that belongs to the second note.\n##(markdown) Second note.",
      );
      expect(after).toContain("run: orders -> kpis");
   });

   test("removing a text cell lists the comment that leaves with it; adding text lands in the file", async ({
      page,
   }) => {
      await open(page);
      await settled(page, 2);

      await cells(page)
         .filter({ hasText: "Second note" })
         .getByRole("button", { name: "Remove text" })
         .click();
      await page.getByRole("button", { name: "Save changes" }).click();
      const removed = page.getByLabel("Comments removed with their cell");
      await expect(removed).toContainText(
         "// A comment that belongs to the second note.",
      );
      await page.getByRole("button", { name: "Save this" }).click();
      await expect(page.getByRole("button", { name: "Saved" })).toBeVisible({
         timeout: 30_000,
      });
      let file = await pe.readSource(TOUR);
      expect(file).not.toContain("Second note");
      expect(file).not.toContain("A comment that belongs");

      // Add text below the first note.
      await cells(page)
         .filter({ hasText: "First note" })
         .getByRole("button", { name: "Add text below" })
         .click();
      await page.getByLabel("Markdown", { exact: true }).fill("Added note.");
      await page.getByRole("button", { name: "Done", exact: true }).click();
      await saveChanges(page);
      file = await pe.readSource(TOUR);
      expect(fileOrder(file)).toEqual(["First", "Added", "Third", "Fourth"]);
      await expect
         .poll(() => noteOrder(page))
         .toEqual(["First", "Added", "Third", "Fourth"]);
   });

   test("prose that names #(authorize) saves, one line or several, and the notebook still serves", async ({
      page,
   }) => {
      await open(page);
      const edit = async (text: string) => {
         await page.getByRole("button", { name: "Edit text" }).first().click();
         await page.getByLabel("Markdown", { exact: true }).fill(text);
         await page.getByRole("button", { name: "Done", exact: true }).click();
      };

      // Prose is not a gate: the server takes the words in either form.
      await edit("A one-liner naming #(authorize) in prose.");
      await saveChanges(page);
      let file = await pe.readSource(TOUR);
      expect(file).toContain("A one-liner naming #(authorize) in prose.");

      await edit("Two lines.\nThe second names #(authorize) in prose.");
      await saveChanges(page);
      file = await pe.readSource(TOUR);
      expect(file).toContain("##|(markdown)");
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

      await cells(page)
         .filter({ hasText: "First note" })
         .getByRole("button", { name: "Edit text" })
         .click();
      await page.getByLabel("Markdown", { exact: true }).fill("Mine.");
      await page.getByRole("button", { name: "Done", exact: true }).click();
      await page.getByRole("button", { name: "Save changes" }).click();
      await expect(page.getByRole("alert")).toBeVisible({ timeout: 30_000 });
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
      await expect(page.getByText("Editing", { exact: true })).toBeVisible({
         timeout: 60_000,
      });
      await cells(page)
         .filter({ hasText: "First note" })
         .getByRole("button", { name: "Edit text" })
         .click();
      await page.getByLabel("Markdown", { exact: true }).fill("Edited first.");
      await page.getByRole("button", { name: "Done", exact: true }).click();
      await saveChanges(page);
      await page.getByRole("button", { name: "Done editing" }).click();
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
      await expect(page.getByRole("button", { name: "Apply" })).toBeVisible({
         timeout: 60_000,
      });
      await expect(page.getByRole("button", { name: "Reset" })).toBeVisible();
   });
});
