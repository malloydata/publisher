// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test, type Page } from "@playwright/test";
import fs from "fs";
import os from "os";
import path from "path";
import { DEFAULT_ENV, PACKAGES, tmpName } from "./helpers/fixtures";

/**
 * The package page's version switch, against a server that publishes
 * versions (`PUBLISHER_PACKAGE_VERSIONING=on`, promotion `on-publish`). The
 * spec publishes its own package to a fresh environment: `sales` 1.0.0 with
 * one model, then 1.1.0, which adds a second, so each version's page is told
 * apart by what it lists. Skipped on a server with versioning off.
 *
 * Routes carry the version as a path segment, `/<env>/<pkg>/<version>/…`,
 * `latest` spelled out, and a bare package route is canonicalized onto
 * `latest`.
 */

const PKG = "sales";
const SHOT_DIR = process.env.VERSIONS_SHOT_DIR;

const ORDERS = `source: orders is duckdb.sql("select 1 as id, 10 as amount") extend {
  measure: revenue is amount.sum()
}
`;
const CUSTOMERS = `source: customers is duckdb.sql("select 1 as id, 'Ada' as name") extend {
  measure: customer_count is count()
}
`;

function writeVersion(version: string, files: Record<string, string>): string {
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), `publisher-${PKG}-`));
   fs.writeFileSync(
      path.join(dir, "publisher.json"),
      JSON.stringify(
         { name: PKG, version, description: `Sales, release ${version}` },
         null,
         2,
      ),
   );
   for (const [name, text] of Object.entries(files))
      fs.writeFileSync(path.join(dir, name), text);
   return dir;
}

async function api(
   baseURL: string,
   method: string,
   route: string,
   body?: unknown,
): Promise<Response> {
   return fetch(`${baseURL}/api/v0${route}`, {
      method,
      headers: { "Content-Type": "application/json" },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
   });
}

async function shot(page: Page, name: string) {
   const file = test.info().outputPath(`${name}.png`);
   // Menus and dialogs fade in; finish their transitions before the shot.
   await page.screenshot({ path: file, animations: "disabled" });
   await test.info().attach(name, { path: file, contentType: "image/png" });
   if (SHOT_DIR) {
      fs.mkdirSync(SHOT_DIR, { recursive: true });
      fs.copyFileSync(file, path.join(SHOT_DIR, `${name}.png`));
   }
}

const versionSwitch = (page: Page) =>
   page.getByRole("button", { name: "Select package version" });
const versionRow = (page: Page, id: string) =>
   page
      .getByRole("menu", { name: `Versions of ${PKG}` })
      .getByTestId("version-row")
      .filter({ hasText: new RegExp(`^${id.replace(/\./g, "\\.")}`) });

async function openVersions(page: Page) {
   await versionSwitch(page).click();
   await expect(
      page.getByRole("menu", { name: `Versions of ${PKG}` }),
   ).toBeVisible();
}

test.describe("package versions — the version switch", () => {
   test.describe.configure({ mode: "serial" });
   let env = "";

   // Playwright needs the fixtures argument to pass testInfo second.
   // eslint-disable-next-line no-empty-pattern
   test.beforeAll(async ({}, testInfo) => {
      const baseURL = testInfo.project.use.baseURL ?? "http://localhost:4000";
      const status = await (await api(baseURL, "GET", "/status")).json();
      // CI runs this spec against a server it started with versioning on, and
      // says so: there a server reporting it off is a failure, not a skip.
      if (process.env.EXPECT_PACKAGE_VERSIONING === "on") {
         expect(status.packageVersioning).toBe("on");
      }
      test.skip(
         status.packageVersioning !== "on",
         "the server does not publish versions (PUBLISHER_PACKAGE_VERSIONING is off)",
      );
      env = tmpName("versions");
      const created = await api(baseURL, "POST", "/environments", {
         name: env,
         packages: [],
         connections: [],
      });
      expect(created.ok, await created.text()).toBe(true);
      for (const [version, files] of [
         ["1.0.0", { "orders.malloy": ORDERS }],
         ["1.1.0", { "orders.malloy": ORDERS, "customers.malloy": CUSTOMERS }],
      ] as const) {
         const published = await api(
            baseURL,
            "POST",
            `/environments/${env}/packages`,
            { name: PKG, location: writeVersion(version, files) },
         );
         expect(published.ok, await published.text()).toBe(true);
      }
   });

   // Playwright needs the fixtures argument to pass testInfo second.
   // eslint-disable-next-line no-empty-pattern
   test.afterAll(async ({}, testInfo) => {
      if (!env) return;
      const baseURL = testInfo.project.use.baseURL ?? "http://localhost:4000";
      await api(baseURL, "DELETE", `/environments/${env}`);
   });

   test("a bare package route canonicalizes onto latest", async ({ page }) => {
      await page.goto(`/${env}/${PKG}`);
      await expect(page).toHaveURL(new RegExp(`/${env}/${PKG}/latest$`));
      await expect(
         page.getByRole("heading", { name: PKG, level: 1 }),
      ).toBeVisible();
      await expect(versionSwitch(page)).toHaveText("v1.1.0");
      // 1.1.0 is the latest: it lists both models.
      await expect(page.getByText("customers.malloy")).toBeVisible();
      await expect(page.getByText("orders.malloy")).toBeVisible();
      // A published version is immutable: its scope and schedule come from
      // its publisher.json, so only a new build is offered.
      await expect(
         page.getByRole("button", { name: "Materialization" }),
      ).toBeVisible();
      await expect(page.getByRole("button", { name: /^Scope/ })).toHaveCount(0);
      await expect(page.getByRole("button", { name: /^Schedule/ })).toHaveCount(
         0,
      );
      await shot(page, "01-latest-package-page");
   });

   test("the switch lists every version, highest first, latest marked", async ({
      page,
   }) => {
      await page.goto(`/${env}/${PKG}/latest`);
      await openVersions(page);
      const rows = page.getByTestId("version-row");
      await expect(rows).toHaveCount(2);
      await expect(rows.nth(0)).toContainText("1.1.0");
      await expect(rows.nth(1)).toContainText("1.0.0");
      await expect(versionRow(page, "1.1.0")).toContainText("Latest");
      await expect(versionRow(page, "1.1.0")).toHaveAttribute(
         "aria-current",
         "true",
      );
      await expect(versionRow(page, "1.0.0")).not.toContainText("Latest");
      await shot(page, "02-version-menu");
   });

   test("picking a version pins the page, its listings and its links", async ({
      page,
   }) => {
      await page.goto(`/${env}/${PKG}/latest`);
      await openVersions(page);
      const pinned = (resource: string) =>
         page.waitForRequest(
            (request) =>
               request.url().includes(`/packages/${PKG}/${resource}`) &&
               request.url().includes("versionId=1.0.0"),
         );
      const listed = Promise.all([
         pinned("models"),
         pinned("materializations"),
      ]);
      await versionRow(page, "1.0.0").click();
      await expect(page).toHaveURL(new RegExp(`/${env}/${PKG}/1\\.0\\.0$`));
      // The page's listings ask for the pinned version, not latest.
      await listed;
      await expect(versionSwitch(page)).toHaveText("v1.0.0");
      // 1.0.0 predates the customers model.
      await expect(page.getByText("orders.malloy")).toBeVisible();
      await expect(page.getByText("customers.malloy")).toHaveCount(0);
      await shot(page, "03-pinned-1.0.0");

      // A model opened from a pinned version stays on that version, and the
      // package crumb leads back to it rather than to latest.
      const modelRead = page.waitForRequest(
         (request) =>
            request.url().includes(`/packages/${PKG}/models/orders.malloy`) &&
            request.url().includes("versionId=1.0.0"),
      );
      await page.getByText("orders.malloy").click();
      await expect(page).toHaveURL(
         new RegExp(`/${env}/${PKG}/1\\.0\\.0/orders\\.malloy$`),
      );
      await modelRead;
      // The model page itself, not the package page it came from: the crumb
      // names the model and the package heading is gone.
      await expect(
         page
            .getByRole("navigation", { name: "breadcrumb" })
            .getByText("orders.malloy"),
      ).toBeVisible();
      await expect(
         page.getByRole("heading", { name: PKG, level: 1 }),
      ).toHaveCount(0);
      await shot(page, "04-model-of-1.0.0");
      await page
         .getByRole("navigation", { name: "breadcrumb" })
         .getByText(PKG, { exact: true })
         .click();
      await expect(page).toHaveURL(new RegExp(`/${env}/${PKG}/1\\.0\\.0/?$`));
      await expect(versionSwitch(page)).toHaveText("v1.0.0");
   });

   test("the latest version's actions say why they are unavailable", async ({
      page,
   }) => {
      await page.goto(`/${env}/${PKG}/latest`);
      await openVersions(page);
      await page
         .getByRole("button", { name: "More actions for version 1.1.0" })
         .click();
      const actions = page.getByRole("menu", {
         name: "Actions for version 1.1.0",
      });
      await expect(
         actions.getByRole("menuitem", { name: /Set as latest/ }),
      ).toHaveAttribute("aria-disabled", "true");
      await expect(actions).toContainText("Already the latest version");
      await expect(
         actions.getByRole("menuitem", { name: /Archive/ }),
      ).toHaveAttribute("aria-disabled", "true");
      await expect(actions).toContainText(
         "The latest version can't be archived",
      );
      await shot(page, "05-latest-actions-disabled");
   });

   test("set as latest moves latest after a confirmation", async ({
      page,
   }, testInfo) => {
      await page.goto(`/${env}/${PKG}/latest`);
      await openVersions(page);
      await page
         .getByRole("button", { name: "More actions for version 1.0.0" })
         .click();
      await page.getByRole("menuitem", { name: /Set as latest/ }).click();
      const dialog = page.getByRole("dialog", { name: "Set v1.0.0 as latest" });
      await expect(dialog).toBeVisible();
      await expect(dialog).toContainText(
         "Requests that name no version will be served from v1.0.0.",
      );
      await shot(page, "06-set-latest-dialog");
      await dialog.getByRole("button", { name: "Set as latest" }).click();
      await expect(dialog).toBeHidden();

      // `latest` now answers with 1.0.0: one model, and the chip moved.
      await expect(versionSwitch(page)).toHaveText("v1.0.0");
      await expect(page.getByText("customers.malloy")).toHaveCount(0);
      await openVersions(page);
      await expect(versionRow(page, "1.0.0")).toContainText("Latest");
      await expect(versionRow(page, "1.1.0")).not.toContainText("Latest");
      await shot(page, "07-latest-moved-to-1.0.0");
      await page.keyboard.press("Escape");

      // Put latest back for the tests after this one.
      const baseURL = testInfo.project.use.baseURL ?? "http://localhost:4000";
      const restored = await api(
         baseURL,
         "PUT",
         `/environments/${env}/packages/${PKG}/latest`,
         { version: "1.1.0" },
      );
      expect(restored.ok, await restored.text()).toBe(true);
   });

   test("archive takes a version out of service; unarchive brings it back", async ({
      page,
   }) => {
      await page.goto(`/${env}/${PKG}/latest`);
      await expect(versionSwitch(page)).toHaveText("v1.1.0");
      await openVersions(page);
      await page
         .getByRole("button", { name: "More actions for version 1.0.0" })
         .click();
      await page.getByRole("menuitem", { name: "Archive" }).click();
      await expect(versionRow(page, "1.0.0")).toContainText("Archived");
      await shot(page, "08-archived-1.0.0");

      // An archived version is listed but cannot be opened.
      await versionRow(page, "1.0.0").click();
      await expect(page).toHaveURL(new RegExp(`/${env}/${PKG}/latest$`));

      await page
         .getByRole("button", { name: "More actions for version 1.0.0" })
         .click();
      await page.getByRole("menuitem", { name: "Unarchive" }).click();
      await expect(versionRow(page, "1.0.0")).not.toContainText("Archived");
      await versionRow(page, "1.0.0").click();
      await expect(page).toHaveURL(new RegExp(`/${env}/${PKG}/1\\.0\\.0$`));
      await expect(versionSwitch(page)).toHaveText("v1.0.0");
   });

   test("a package with no versions keeps its route and shows no switch", async ({
      page,
   }) => {
      await page.goto(`/${DEFAULT_ENV}/${PACKAGES.storefront}`);
      await expect(
         page.getByRole("heading", { name: PACKAGES.storefront, level: 1 }),
      ).toBeVisible();
      await expect(page).toHaveURL(
         new RegExp(`/${DEFAULT_ENV}/${PACKAGES.storefront}$`),
      );
      await expect(versionSwitch(page)).toHaveCount(0);
   });
});
