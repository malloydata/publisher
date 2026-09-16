// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test, type Page } from "@playwright/test";
import path from "path";
import { fileURLToPath } from "url";
import { tmpName } from "./helpers/fixtures";

/**
 * The dashboard builder, end to end in a browser: open a package dashboard in
 * the Console's editor, change the page's settings, save into the browser,
 * and find the draft offered again on the next visit.
 *
 * Runs against its own environment built from the server's dashboards
 * fixture, registered and removed the way `package-dashboards.spec` does it.
 */

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE = path.resolve(
   __dirname,
   "../../../server/tests/fixtures/dashboards-test",
);
const PKG = "dashboards-test";

let env: string;
let baseURL: string;

test.describe("dashboard-builder", () => {
   // eslint-disable-next-line no-empty-pattern
   test.beforeAll(async ({}, testInfo) => {
      baseURL = testInfo.project.use.baseURL ?? "http://localhost:4000";
      env = tmpName("builder");
      const res = await fetch(`${baseURL}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: env,
            packages: [{ name: PKG, location: FIXTURE }],
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
      if (!env || !baseURL) return;
      await fetch(`${baseURL}/api/v0/environments/${env}`, {
         method: "DELETE",
      }).catch(() => undefined);
   });

   const openEditor = async (page: Page) => {
      await page.goto(`/${env}/${PKG}/dashboards/tiled/edit`);
      await expect(page.getByText("Editing")).toBeVisible({ timeout: 60_000 });
      await expect(page.getByText("Tiled", { exact: true })).toBeVisible();
   };

   test("opens a package dashboard, saves a settings edit, and offers the draft again", async ({
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

      // The save went into this browser, not the package: a fresh visit opens
      // the package file and offers the draft.
      await page.goto(`/${env}/${PKG}/dashboards/tiled/edit`);
      await expect(
         page.getByText(/edits to this dashboard saved in this browser/),
      ).toBeVisible({ timeout: 60_000 });
      await expect(page.getByText("Tiled", { exact: true })).toBeVisible();
      await page.getByRole("button", { name: "Resume" }).click();
      await expect(
         page.getByText("Tiled, edited", { exact: true }),
      ).toBeVisible();
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
