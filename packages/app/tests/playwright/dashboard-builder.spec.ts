// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test, type Page } from "@playwright/test";
import fs from "fs";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import { tmpName } from "./helpers/fixtures";

/**
 * The dashboard builder, end to end in a browser: open a package dashboard in
 * the Console's editor, change the page's settings, save it back into the
 * package, and reorder the grid without a tile losing its chart.
 *
 * Runs against its own environment built from the server's dashboards
 * fixture, registered and removed the way `package-dashboards.spec` does it.
 * The fixture is copied to a temporary directory first, because saving writes
 * the dashboard file and the fixture in the repository is not the place for
 * that. A server that refuses the registration is read-only, and the builder's
 * browser-draft path is covered by the SDK's own tests instead.
 */

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE = path.resolve(
   __dirname,
   "../../../server/tests/fixtures/dashboards-test",
);
const PKG = "dashboards-test";

let env: string;
let baseURL: string;
let location: string;

test.describe("dashboard-builder", () => {
   // eslint-disable-next-line no-empty-pattern
   test.beforeAll(async ({}, testInfo) => {
      baseURL = testInfo.project.use.baseURL ?? "http://localhost:4000";
      env = tmpName("builder");
      location = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-builder-"));
      fs.cpSync(FIXTURE, location, { recursive: true });
      const res = await fetch(`${baseURL}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: env,
            packages: [{ name: PKG, location }],
            connections: [],
         }),
      });
      test.skip(
         res.status === 405 || res.status === 403,
         "publisher is read-only",
      );
      expect(res.ok, await res.text()).toBe(true);
   });

   test.afterAll(async () => {
      if (env && baseURL)
         await fetch(`${baseURL}/api/v0/environments/${env}`, {
            method: "DELETE",
         }).catch(() => undefined);
      if (location) fs.rmSync(location, { recursive: true, force: true });
   });

   // The title is matched loosely because one test above saves a new one into
   // the package, and every test here opens the same dashboard.
   const openEditor = async (page: Page) => {
      await page.goto(`/${env}/${PKG}/dashboards/tiled/edit`);
      await expect(page.getByText("Editing")).toBeVisible({ timeout: 60_000 });
      await expect(page.getByText(/^Tiled/).first()).toBeVisible();
   };

   test("opens a package dashboard and saves a settings edit into the package", async ({
      page,
   }) => {
      await openEditor(page);
      // The file's tiles, laid out; the reader's header over them.
      await expect(page.getByLabel("Tile order_tile")).toBeVisible();
      await expect(
         page.getByText("Two rows of two, one of them a chart."),
      ).toBeVisible();

      await page.getByRole("button", { name: "Settings", exact: true }).click();
      const title = page.getByLabel("Dashboard title");
      await title.fill("Tiled, edited");
      await title.press("Escape");
      await expect(
         page.getByText("Tiled, edited", { exact: true }),
      ).toBeVisible();
      await expect(page.getByText("Unsaved changes")).toBeVisible();

      await page.getByRole("button", { name: "Save changes" }).click();
      await expect(page.getByRole("button", { name: "Saved" })).toBeVisible();

      // The save went into the package, so the next visit opens the edited
      // file itself: the new title, and nothing offering a browser draft.
      await page.goto(`/${env}/${PKG}/dashboards/tiled/edit`);
      await expect(page.getByText("Editing")).toBeVisible({ timeout: 60_000 });
      await expect(
         page.getByText("Tiled, edited", { exact: true }),
      ).toBeVisible();
      await expect(
         page.getByText(/edits to this dashboard saved in this browser/),
      ).toHaveCount(0);
   });

   test("a tile keeps its chart when the grid is reordered", async ({
      page,
   }) => {
      await openEditor(page);
      // Every tile painted before the drag, so a blank one afterwards is the
      // drag's doing and not a query that never came back.
      const rendered = page.locator('[aria-label^="Tile "] .malloy-render');
      await expect(rendered).toHaveCount(4, { timeout: 60_000 });

      // A real pointer drag, because that is what re-runs the tiles' render
      // effects: the grip of the last tile onto the first.
      const grip = page.getByLabel("Move By region");
      await grip.scrollIntoViewIfNeeded();
      const from = await grip.boundingBox();
      const onto = await page.getByLabel("Tile order_tile").boundingBox();
      expect(from && onto).toBeTruthy();
      if (!from || !onto) return;
      await page.mouse.move(from.x + from.width / 2, from.y + from.height / 2);
      await page.mouse.down();
      for (let step = 1; step <= 10; step++) {
         await page.mouse.move(
            from.x + ((onto.x + 40 - from.x) * step) / 10,
            from.y + ((onto.y + 40 - from.y) * step) / 10,
            { steps: 2 },
         );
      }
      await page.mouse.up();
      await expect(page.getByText("Unsaved changes")).toBeVisible();

      // Each tile's chart still sits INSIDE its tile. The regression this
      // guards was not a missing chart but a displaced one: a stale render
      // stage left in the container pushed the live chart below the tile's
      // clip, so the tile drew empty while its rows were still in the DOM.
      await expect(rendered).toHaveCount(4);
      const outside = await page.evaluate(() =>
         Array.from(document.querySelectorAll('[aria-label^="Tile "]'))
            .map((tile) => {
               const chart = tile.querySelector(".malloy-render");
               if (!chart)
                  return `${tile.getAttribute("aria-label")}: no chart`;
               const a = tile.getBoundingClientRect();
               const b = chart.getBoundingClientRect();
               return b.top >= a.top - 1 && b.bottom <= a.bottom + 1
                  ? undefined
                  : `${tile.getAttribute("aria-label")}: chart ${Math.round(b.top)}-${Math.round(b.bottom)} outside tile ${Math.round(a.top)}-${Math.round(a.bottom)}`;
            })
            .filter(Boolean),
      );
      expect(outside).toEqual([]);
   });

   test("a tile's width is set from its menu and previewed on the grid", async ({
      page,
   }) => {
      await openEditor(page);
      await page.getByLabel("Settings for Orders").click();
      await page.getByRole("button", { name: "Width Full" }).click();
      await page.keyboard.press("Escape");
      const tile = page.getByLabel("Tile order_tile");
      const revenue = page.getByLabel("Tile revenue_tile");
      // Full width: the next tile starts under it rather than beside it.
      const [a, b] = await Promise.all([
         tile.boundingBox(),
         revenue.boundingBox(),
      ]);
      expect(a && b && b.y > a.y + a.height - 1).toBe(true);
      await expect(page.getByText("Unsaved changes")).toBeVisible();
   });
});
