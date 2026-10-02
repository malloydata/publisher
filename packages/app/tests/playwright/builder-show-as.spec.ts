// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test, type Page } from "@playwright/test";
import { createHash } from "crypto";
import fs from "fs";
import path from "path";
import { editText, editorOpen, queryTiles, tileByKey } from "./helpers/builder";
import {
   exampleFixture,
   registerPackageEnv,
   type PackageEnv,
} from "./helpers/packageEnv";
import { saveChanges, undoSave } from "./helpers/save";

/**
 * A document's kind is a tag in its file, not a folder: Settings > Show as
 * rewrites that tag in place, and the Console finds the document wherever the
 * kind put it. Each test writes into a throwaway copy of the storefront package.
 */

const OVERVIEW = "dashboards/overview.malloy";

// `# drill { to=overview }` on the category dimension, so a second dashboard
// can link to the one that is flipped.
const storefront = fs
   .readFileSync(
      path.join(exampleFixture("storefront"), "storefront.malloy"),
      "utf8",
   )
   .replace(
      "# drill { to=self given=CATEGORY }",
      "# drill { to=overview given=CATEGORY }",
   );

const LINKER = `##! experimental.givens
## artifact { title="By category" tiles=["order_items -> category_performance"] }
import { order_items } from "../storefront.malloy"
`;

const layout = (kind: "dashboard" | "notebook", title: string, note: string) =>
   `##! experimental.givens
## artifact { kind=${kind} title="${title}" tiles=["order_items -> sales_by_month", intro { kind=text }] }
import { order_items } from "../storefront.malloy"

##|(markdown) intro
${note}
|##
`;

const MISFILED_NOTEBOOK = "Misfiled notebook note.";
const MISFILED_DASHBOARD = "Misfiled dashboard note.";

let pe: PackageEnv;

test.describe("Show as, and a kind that disagrees with its folder", () => {
   // eslint-disable-next-line no-empty-pattern
   test.beforeAll(async ({}, testInfo) => {
      pe = await registerPackageEnv(
         testInfo.project.use.baseURL ?? "http://localhost:4000",
         "showas",
         exampleFixture("storefront"),
         "storefront",
         {
            "storefront.malloy": storefront,
            "dashboards/by-category.malloy": LINKER,
            // A notebook in dashboards/, a dashboard in notebooks/.
            "dashboards/misfiled.malloy": layout(
               "notebook",
               "Misfiled notebook",
               MISFILED_NOTEBOOK,
            ),
            "notebooks/mixed.malloy": layout(
               "dashboard",
               "Mixed dashboard",
               MISFILED_DASHBOARD,
            ),
            "dashboards/clash.malloy": layout("notebook", "Clash", "Clash."),
            "notebooks/clash.malloy": layout("dashboard", "Clash", "Clash."),
            "notebooks/flip.malloy": layout("notebook", "Flip", "Flip."),
         },
      );
   });
   test.afterAll(async () => {
      await pe?.dispose();
   });

   const show = (...parts: string[]) =>
      `/${pe.env}/${pe.pkg}/${parts.join("/")}`;

   /** Settings > Show as, committed by closing the popover. */
   const showAs = async (page: Page, kind: "Dashboard" | "Notebook") => {
      await page.getByRole("button", { name: "Settings", exact: true }).click();
      await page.getByRole("button", { name: kind, exact: true }).click();
      await page.keyboard.press("Escape");
   };

   const openEditor = async (page: Page, ...parts: string[]) => {
      await page.goto(show(...parts, "edit"));
      await editorOpen(page);
      await expect(queryTiles(page).first()).toBeVisible({ timeout: 60_000 });
   };

   const isStatus = async (modelPath: string) =>
      (
         await fetch(
            `${pe.baseURL}/api/v0/environments/${pe.env}/packages/${pe.pkg}/models/${encodeURIComponent(modelPath)}`,
         )
      ).status;

   test("a dashboard shown as a notebook reads in one column at its old address; shown as a dashboard again it keeps its tiles but not its grid width", async ({
      page,
   }) => {
      const original = await pe.readSource(OVERVIEW);
      expect(original).toContain("dashboard { columns=12 }");
      expect(original).toContain("# colspan=6");

      await openEditor(page, "dashboards", "overview");
      await showAs(page, "Notebook");
      await saveChanges(page);

      const asNotebook = await pe.readSource(OVERVIEW);
      expect(asNotebook).toContain("kind=notebook");
      expect(asNotebook).not.toMatch(/^## artifact .*dashboard \{/m);
      // The tile spans stay behind as plain tags; a notebook ignores them.
      expect(asNotebook).toContain("# colspan=6");

      // The old address opens the document as what its tag now makes it.
      const reader = await page.context().newPage();
      await reader.goto(show("dashboards", "overview"));
      await expect(
         reader.locator("[data-malloy-render-as]").first(),
      ).toBeVisible({ timeout: 60_000 });
      await expect(reader.locator('[data-chrome="card"]')).toHaveCount(0);
      const lefts = await reader
         .locator("[data-malloy-render-as]")
         .evaluateAll((els) =>
            els.map((el) => Math.round(el.getBoundingClientRect().left)),
         );
      expect(new Set(lefts).size).toBe(1);

      // A link from another dashboard to the flipped slug still lands on it,
      // with the clicked value seeded.
      await reader.goto(show("dashboards", "by-category"));
      const cell = reader.locator(".column-cell.td.publisher-drill").first();
      await expect(cell).toBeVisible({ timeout: 60_000 });
      const value = (await cell.innerText()).trim();
      await cell.click();
      await expect(reader).toHaveURL(
         new RegExp(
            `/(?:dashboards|notebooks)/overview\\?CATEGORY=${encodeURIComponent(value).replace(/%20/g, "(?:%20|\\+)")}$`,
         ),
         { timeout: 60_000 },
      );
      await expect(
         reader.getByRole("combobox", { name: "Category" }),
      ).toHaveValue(value, { timeout: 60_000 });
      await reader.close();

      // The editor that did the flip is still usable on the flipped file.
      await expect(queryTiles(page)).toHaveCount(5);

      await showAs(page, "Dashboard");
      await saveChanges(page);
      const back = await pe.readSource(OVERVIEW);
      expect(back).not.toContain("kind=notebook");
      expect(back).toContain("# colspan=6");
      for (const view of ["kpis", "revenue_trend", "best_sellers"])
         expect(back).toContain(`overview -> ${view}`);
      // Known loss: the notebook flip deleted `columns`, and flipping back does
      // not restore it, so the grid falls to its default width. If the builder
      // ever remembers the width, this is the line to turn into toContain.
      expect(back).not.toMatch(/^## artifact .*dashboard \{/m);
      expect(back).not.toBe(original);

      const again = await page.context().newPage();
      await again.goto(show("dashboards", "overview"));
      await expect(again.locator('[data-chrome="card"]').first()).toBeVisible({
         timeout: 60_000,
      });
      await again.close();

      // Undo save undoes only the last save: back to the notebook, not the original.
      await undoSave(page);
      expect(await pe.readSource(OVERVIEW)).toBe(asNotebook);
   });

   test("Undo save after a kind flip puts the dashboard back byte for byte, and it reads as a dashboard again", async ({
      page,
   }) => {
      const file = "dashboards/by-category.malloy";
      const original = await pe.readSource(file);

      await openEditor(page, "dashboards", "by-category");
      await showAs(page, "Notebook");
      await saveChanges(page);
      expect(await pe.readSource(file)).toContain("kind=notebook");

      await undoSave(page);
      expect(await pe.readSource(file)).toBe(original);

      const reader = await page.context().newPage();
      await reader.goto(show("dashboards", "by-category"));
      await expect(reader).toHaveURL(show("dashboards", "by-category"));
      await expect(reader.locator('[data-chrome="card"]').first()).toBeVisible({
         timeout: 60_000,
      });
      await reader.close();
   });

   test("a notebook in dashboards/ opens at either address and saves to the path it is listed at", async ({
      page,
   }) => {
      for (const folder of ["dashboards", "notebooks"]) {
         await page.goto(show(folder, "misfiled"));
         await expect(page.getByText(MISFILED_NOTEBOOK)).toBeVisible({
            timeout: 60_000,
         });
         await expect(page.locator('[data-chrome="card"]')).toHaveCount(0);
      }

      await openEditor(page, "notebooks", "misfiled");
      await editText(tileByKey(page, "text.intro"), "Edited in place.");
      await saveChanges(page);

      expect(await pe.readSource("dashboards/misfiled.malloy")).toContain(
         "Edited in place.",
      );
      expect(await isStatus("notebooks/misfiled.malloy")).toBe(404);
   });

   test("a dashboard in notebooks/ opens at either address and saves to the path it is listed at", async ({
      page,
   }) => {
      for (const folder of ["dashboards", "notebooks"]) {
         await page.goto(show(folder, "mixed"));
         await expect(page.getByText(MISFILED_DASHBOARD)).toBeVisible({
            timeout: 60_000,
         });
         await expect(page.locator('[data-chrome="card"]').first()).toBeVisible(
            {
               timeout: 60_000,
            },
         );
      }

      await openEditor(page, "dashboards", "mixed");
      await editText(tileByKey(page, "text.intro"), "Edited in place.");
      await saveChanges(page);

      expect(await pe.readSource("notebooks/mixed.malloy")).toContain(
         "Edited in place.",
      );
      expect(await isStatus("dashboards/mixed.malloy")).toBe(404);
   });

   test("showing a notebook as a dashboard over a slug another dashboard holds is refused, and nothing is written", async ({
      page,
   }) => {
      // The dashboard `clash` lives in notebooks/; the notebook `clash` in dashboards/.
      const before = await pe.readSource("dashboards/clash.malloy");
      await openEditor(page, "notebooks", "clash");
      await showAs(page, "Dashboard");
      await page.getByRole("button", { name: "Save changes" }).click();
      await expect(
         page
            .getByRole("alert")
            .filter({ hasText: "already holds the dashboard name" }),
      ).toBeVisible({ timeout: 30_000 });
      expect(await pe.readSource("dashboards/clash.malloy")).toBe(before);
   });

   // Known bug: Show as > Dashboard drops `kind=notebook`, and the server reads an untagged file under notebooks/ as a notebook, so nothing changes. Remove test.fail() when the builder writes `kind=dashboard` there.
   test("a notebook in notebooks/ shown as a dashboard is served as a dashboard", async ({
      page,
   }) => {
      test.fail();
      await openEditor(page, "notebooks", "flip");
      await showAs(page, "Dashboard");
      await saveChanges(page);
      const res = await fetch(
         `${pe.baseURL}/api/v0/environments/${pe.env}/packages/${pe.pkg}/dashboards`,
      );
      const paths = ((await res.json()) as { path: string }[]).map(
         (d) => d.path,
      );
      expect(paths).toContain("notebooks/flip.malloy");
   });

   test("Undo save over a file that changed since the save is refused, and the other text stays", async ({
      page,
   }) => {
      const file = "dashboards/misfiled.malloy";
      await openEditor(page, "dashboards", "misfiled");
      await editText(tileByKey(page, "text.intro"), "Mine.");
      await saveChanges(page);

      const saved = await pe.readSource(file);
      const elsewhere = saved.replace("Mine.", "Theirs.");
      const res = await fetch(
         `${pe.baseURL}/api/v0/environments/${pe.env}/packages/${pe.pkg}/models/${encodeURIComponent(file)}`,
         {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
               source: elsewhere,
               expectedHash: createHash("sha256").update(saved).digest("hex"),
            }),
         },
      );
      expect(res.ok, await res.text()).toBe(true);

      await page.getByRole("button", { name: "Undo save" }).click();
      await expect(
         page.getByRole("alert").filter({ hasText: "Could not undo the save" }),
      ).toContainText("changed in the package since you opened it", {
         timeout: 30_000,
      });
      await expect(page.getByText("Save undone.")).toHaveCount(0);
      expect(await pe.readSource(file)).toBe(elsewhere);
   });
});
