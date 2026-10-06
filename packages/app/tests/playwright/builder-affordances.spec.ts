// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import {
   registerPackageEnv,
   serverFixture,
   type PackageEnv,
} from "./helpers/packageEnv";

/**
 * Builder affordances a unit render cannot judge: a disabled control that
 * still says why, a dialog that fits a laptop screen, and the create action an
 * empty section offers.
 */

const SOLO_DASHBOARD = `##! experimental.givens
import { orders } from '../orders.malloy'
import { BRAND } from '../givens.malloy'

## artifact { title="Solo" tiles=["tiles -> order_tile"] } dashboard { columns=12 }
source: tiles is orders extend {
   where: brand_name ~ $BRAND

   # colspan=6
   # label="Orders"
   view: order_tile is {
      aggregate: order_count
   }
}
`;

const baseOf = (testInfo: { project: { use: { baseURL?: string } } }) =>
   testInfo.project.use.baseURL ?? "http://localhost:4000";

test.describe("builder affordances", () => {
   let pe: PackageEnv | undefined;
   test.afterEach(async () => {
      await pe?.dispose();
      pe = undefined;
   });

   test("the last tile of a saved dashboard cannot be removed, and the menu says why", async ({
      page,
   }, testInfo) => {
      pe = await registerPackageEnv(
         baseOf(testInfo),
         "lasttile",
         serverFixture("dashboards-test"),
         "dashboards-test",
         { "dashboards/solo.malloy": SOLO_DASHBOARD },
      );
      await page.goto(`/${pe.env}/${pe.pkg}/dashboards/solo/edit`);
      await expect(page.getByRole("button", { name: "Undo" })).toBeVisible({
         timeout: 60_000,
      });
      const tile = page.getByLabel("Tile order_tile");
      await expect(tile).toBeVisible();

      await page.getByRole("button", { name: /^Settings for / }).click();
      const remove = page.getByRole("button", { name: "Delete" });
      await expect(remove).toHaveAttribute("aria-disabled", "true");
      await expect(
         page.getByText("A saved dashboard needs at least one tile."),
      ).toBeVisible();
      await expect(remove).toHaveAccessibleDescription(
         "A saved dashboard needs at least one tile.",
      );

      // Refused, not ignored: the click changes nothing.
      await remove.click({ force: true });
      await expect(tile).toBeVisible();
   });

   test("the Add a tile dialog fits a 1280x720 window", async ({
      page,
   }, testInfo) => {
      pe = await registerPackageEnv(
         baseOf(testInfo),
         "dialogfit",
         serverFixture("notebooks-malloyyo"),
         "notebooks-malloyyo",
      );
      await page.setViewportSize({ width: 1280, height: 720 });
      await page.goto(`/${pe.env}/${pe.pkg}/notebooks/layout/edit`);
      await expect(page.getByRole("button", { name: "Undo" })).toBeVisible({
         timeout: 60_000,
      });
      await page.getByRole("button", { name: "Add tile", exact: true }).click();

      const dialog = page.getByRole("dialog", { name: "Add a tile" });
      await expect(
         dialog.getByRole("combobox", { name: "Source" }),
      ).toBeVisible({ timeout: 30_000 });
      const box = await dialog.boundingBox();
      expect(box).not.toBeNull();
      expect(box!.y).toBeGreaterThanOrEqual(0);
      expect(box!.y + box!.height).toBeLessThanOrEqual(720);
   });

   test("an empty package offers New artifact, which opens the create dialog on the kind picked", async ({
      page,
   }, testInfo) => {
      pe = await registerPackageEnv(
         baseOf(testInfo),
         "emptyart",
         serverFixture("dashboards-test"),
         "dashboards-test",
         {},
         [
            "dashboards",
            "brands.malloynb",
            "orders-since.malloynb",
            "orders-start.malloynb",
         ],
      );
      await page.goto(`/${pe.env}/${pe.pkg}`);
      await expect(page.getByText("No artifacts yet")).toBeVisible({
         timeout: 60_000,
      });
      await page.getByRole("button", { name: "New artifact" }).click();
      await page.getByRole("menuitem", { name: "Notebook" }).click();
      await expect(
         page.getByRole("dialog", { name: "New notebook" }),
      ).toBeVisible();
   });

   test("the Artifacts heading row carries New, whichever kind the package already has", async ({
      page,
   }, testInfo) => {
      pe = await registerPackageEnv(
         baseOf(testInfo),
         "newrow",
         serverFixture("notebooks-malloyyo"),
         "notebooks-malloyyo",
         {},
         ["dashboards"],
      );
      await page.goto(`/${pe.env}/${pe.pkg}`);
      const section = page.getByRole("region", { name: "Artifacts" });
      await expect(section).toBeVisible({ timeout: 60_000 });
      await section.getByRole("button", { name: "New", exact: true }).click();
      await page.getByRole("menuitem", { name: "Dashboard" }).click();
      await expect(
         page.getByRole("dialog", { name: "New dashboard" }),
      ).toBeVisible();
   });
});
