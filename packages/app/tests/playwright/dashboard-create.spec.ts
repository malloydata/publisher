// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import { chooseLayout, pickView, readerOpen } from "./helpers/builder";
import {
   registerPackageEnv,
   serverFixture,
   type PackageEnv,
} from "./helpers/packageEnv";
import { saveChanges } from "./helpers/save";

/**
 * A dashboard's whole life through the Console, against a server that takes
 * writes: created from the package page, saved into the package from the
 * builder, and served to a reader.
 *
 * It runs against a throwaway copy of the fixture package, mounted in place
 * (see registerPackageEnv): the flow writes into the package, the fixture in
 * the repository is not the place for that, and a published version takes no
 * writes at all.
 */

const PKG = "dashboards-test";

let pe: PackageEnv;

test.describe("dashboard-create", () => {
   // eslint-disable-next-line no-empty-pattern
   test.beforeAll(async ({}, testInfo) => {
      const baseURL = testInfo.project.use.baseURL ?? "http://localhost:4000";
      const status = await fetch(`${baseURL}/api/v0/status`).then((r) =>
         r.json(),
      );
      test.skip(status.frozenConfig === true, "publisher is read-only");
      pe = await registerPackageEnv(baseURL, "create", serverFixture(PKG), PKG);
   });

   test.afterAll(async () => {
      await pe?.dispose();
   });

   test("creates a dashboard from the package page, saves an edit into the package, and a reader sees it", async ({
      page,
   }) => {
      await page.goto(`/${pe.env}/${pe.pkg}`);
      await page
         .getByRole("button", { name: "Add report", exact: true })
         .first()
         .click({
            timeout: 60_000,
         });

      const dialog = page.getByRole("dialog");
      // The model and a first tile fill themselves in; only the ones with a
      // view to put on a tile are offered at all.
      await expect(dialog.getByLabel("Report title")).not.toHaveValue("", {
         timeout: 30_000,
      });
      await chooseLayout(dialog, "Grid");
      await pickView(dialog, "by_brand", "orders in orders.malloy");
      await dialog.getByLabel("Report title").fill("Created here");
      // Written at the package root: no directory is created for a report.
      await expect(
         dialog.getByRole("textbox", { name: "Location" }),
      ).toHaveValue("created-here.malloy");
      await dialog.getByRole("button", { name: "Create report" }).click();

      // The builder opens on the file just written into the package.
      await expect(page).toHaveURL(
         new RegExp(`/${pe.env}/${pe.pkg}/created-here\\?edit$`),
      );
      await expect(page.getByRole("button", { name: "Undo" })).toBeVisible({
         timeout: 60_000,
      });
      // The title, shown in the header and nowhere else: a new report carries
      // no intro text tile repeating it as a heading.
      await expect(
         page.getByRole("button", { name: "Created here", exact: true }),
      ).toBeVisible();
      await expect(page.locator('[data-saves-to="package"]')).toHaveCount(1);

      // An edit, saved into the package.
      // The title is edited where it is shown.
      // The report title is the page's h1, edited where it stands.
      await page
         .getByRole("heading", { level: 1 })
         .first()
         .getByRole("button")
         .click();
      const title = page.getByLabel("Report title");
      await title.fill("Created and saved");
      await title.press("Enter");
      await saveChanges(page);

      // The reader's view is served from the package, so it shows the save.
      // The header's View button leaves the builder for it.
      await page
         .getByRole("toolbar", { name: "Report builder" })
         .getByRole("button", { name: "Done", exact: true })
         .click();
      await expect(page).toHaveURL(
         new RegExp(`/${pe.env}/${pe.pkg}/created-here$`),
      );
      // The builder stays up until the reader is drawn: wait for the swap.
      await readerOpen(page);
      await expect(
         page.getByRole("heading", { name: "Created and saved" }),
      ).toBeVisible({ timeout: 60_000 });
      // The one tile the new dashboard was created with, headed by the
      // `# label` New report wrote for it (the view's title, not the
      // extension's `by_brand_tile` humanized) and carrying the run
      // expression as its tooltip, the way every report heads a panel.
      const tile = page.getByText("By brand", { exact: true });
      await expect(tile).toBeVisible({ timeout: 30_000 });
      await expect(tile).toHaveAttribute(
         "title",
         "orders_tiles -> by_brand_tile",
      );

      // And the package page lists it.
      await page.goto(`/${pe.env}/${pe.pkg}`);
      await expect(
         page.getByRole("button", { name: /Created and saved/ }),
      ).toBeVisible({ timeout: 60_000 });
   });
});
