import { expect, test } from "@playwright/test";
import * as fs from "fs/promises";
import * as path from "path";
import { fileURLToPath } from "url";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";

/**
 * Regression test for F07. Links inside a rendered README/notebook are
 * authored relative to the package (e.g. `linked_tour.malloynb`). A plain
 * browser anchor resolved them against the current URL, dropped the package
 * segment, and 404'd with a misleading "Package manifest ... does not exist".
 * They must route within the package instead.
 *
 * No bundled example ships a README.malloynb, so the spec writes its own
 * README.malloynb + target notebook into the `storefront` package directory
 * before the suite runs, triggers a package reload, and cleans up after
 * (the same fixture pattern as notebook-givens.spec.ts). The target name
 * shares nothing with the package name, so a dropped package segment cannot
 * accidentally still resolve.
 *
 * Run this against a normal server, not one started with `--watch-env
 * examples`: watch mode symlinks PKG_DIR to the tracked `examples/storefront`
 * sources, so the fixture writes below would land in version control.
 */

const FIXTURE_README = "README.malloynb";
const FIXTURE_TARGET = "linked_tour.malloynb";

const PKG_DIR = path.resolve(
   path.dirname(fileURLToPath(import.meta.url)),
   "../../../server/publisher_data/examples/storefront",
);

const README_SOURCE = `>>>markdown
# Storefront README

Table of contents:

- [Linked tour](${FIXTURE_TARGET})
`;

const TARGET_SOURCE = `>>>markdown
# Linked tour

Fixture notebook for the README-link routing regression spec.
`;

async function reloadPackage(baseURL: string): Promise<void> {
   const url = `${baseURL}/api/v0/environments/${DEFAULT_ENV}/packages/${PACKAGES.storefront}?reload=true`;
   const res = await fetch(url);
   if (!res.ok) {
      throw new Error(`Package reload failed: ${res.status} ${res.statusText}`);
   }
}

test.describe("notebook-readme-links", () => {
   test.beforeAll(async ({ baseURL }) => {
      await fs.writeFile(path.join(PKG_DIR, FIXTURE_README), README_SOURCE);
      await fs.writeFile(path.join(PKG_DIR, FIXTURE_TARGET), TARGET_SOURCE);
      await reloadPackage(baseURL!);
   });

   test.afterAll(async ({ baseURL }) => {
      await fs
         .unlink(path.join(PKG_DIR, FIXTURE_README))
         .catch(() => undefined);
      await fs
         .unlink(path.join(PKG_DIR, FIXTURE_TARGET))
         .catch(() => undefined);
      await reloadPackage(baseURL!).catch(() => undefined);
   });

   test("clicking a README link opens the notebook inside its package", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.storefront);

      // The fixture README.malloynb renders a table of contents linking to
      // another notebook. Clicking it must stay within the storefront package.
      const link = page.getByRole("link", { name: "Linked tour" });
      await expect(link).toBeVisible({ timeout: 60_000 });
      await link.click();

      await expect(page).toHaveURL(
         new RegExp(
            `/${DEFAULT_ENV}/${PACKAGES.storefront}/linked_tour\\.malloynb`,
         ),
      );
      // The target notebook renders its authored H1, proving it loaded rather
      // than the "does not exist" error the dropped-package route gave.
      await expect(
         page.getByRole("heading", { name: "Linked tour", level: 1 }),
      ).toBeVisible();
   });
});
