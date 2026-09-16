// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import fs from "fs";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import { tmpName } from "./helpers/fixtures";

/**
 * A dashboard's whole life through the Console, against a server that takes
 * writes: created from the package page, saved into the package from the
 * builder, and served to a reader.
 *
 * The fixture package is copied to a temporary directory first: the flow
 * writes into the package, and the fixture in the repository is not the
 * place for that.
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

test.describe("dashboard-create", () => {
   // eslint-disable-next-line no-empty-pattern
   test.beforeAll(async ({}, testInfo) => {
      baseURL = testInfo.project.use.baseURL ?? "http://localhost:4000";
      const status = await fetch(`${baseURL}/api/v0/status`).then((r) =>
         r.json(),
      );
      test.skip(status.frozenConfig === true, "publisher is read-only");
      env = tmpName("create");
      location = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-create-"));
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

   test("creates a dashboard from the package page, saves an edit into the package, and a reader sees it", async ({
      page,
   }) => {
      await page.goto(`/${env}/${PKG}`);
      await page.getByRole("button", { name: "New dashboard" }).click({
         timeout: 60_000,
      });

      const dialog = page.getByRole("dialog");
      await dialog.getByRole("combobox", { name: "Model" }).click();
      await page.getByRole("option", { name: "orders.malloy" }).click();
      const source = dialog.getByRole("combobox", { name: "Source" });
      await expect(source).not.toHaveAttribute("aria-disabled", "true", {
         timeout: 30_000,
      });
      await source.click();
      await page.getByRole("option", { name: "orders", exact: true }).click();
      await dialog.getByRole("combobox", { name: "First tile" }).click();
      await page.getByRole("option", { name: "by_brand", exact: true }).click();
      await dialog.getByLabel("Dashboard title").fill("Created here");
      await dialog.getByRole("button", { name: "Create" }).click();

      // The builder opens on the file just written into the package.
      await expect(page).toHaveURL(
         new RegExp(`/${env}/${PKG}/dashboards/created-here/edit$`),
      );
      await expect(page.getByText("Editing")).toBeVisible({ timeout: 60_000 });
      await expect(
         page.getByText("Created here", { exact: true }),
      ).toBeVisible();
      await expect(
         page.getByText("Save writes the file into the package."),
      ).toBeVisible();

      // An edit, saved into the package.
      await page.getByRole("button", { name: "Settings", exact: true }).click();
      const title = page.getByLabel("Dashboard title");
      await title.fill("Created and saved");
      await title.press("Escape");
      await page.getByRole("button", { name: "Save changes" }).click();
      await expect(page.getByRole("button", { name: "Saved" })).toBeVisible({
         timeout: 30_000,
      });

      // The reader's view is served from the package, so it shows the save.
      await page.getByRole("button", { name: "Done" }).click();
      await expect(page).toHaveURL(
         new RegExp(`/${env}/${PKG}/dashboards/created-here$`),
      );
      await expect(
         page.getByRole("heading", { name: "Created and saved" }),
      ).toBeVisible({ timeout: 60_000 });
      // The one tile the new dashboard was created with, headed by its
      // humanized view name and carrying the run expression as its tooltip,
      // the way every composite dashboard heads a panel.
      const tile = page.getByText("By brand tile", { exact: true });
      await expect(tile).toBeVisible({ timeout: 30_000 });
      await expect(tile).toHaveAttribute(
         "title",
         "orders_tiles -> by_brand_tile",
      );

      // And the package page lists it.
      await page.goto(`/${env}/${PKG}`);
      await expect(
         page.getByRole("button", { name: /Created and saved/ }),
      ).toBeVisible({ timeout: 60_000 });
   });
});
