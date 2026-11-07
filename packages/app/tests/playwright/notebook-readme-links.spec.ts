// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import { PACKAGES } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";
import {
   exampleFixture,
   registerPackageEnv,
   type PackageEnv,
} from "./helpers/packageEnv";

/**
 * Regression test for F07. Links inside a rendered README/notebook are
 * authored relative to the package (e.g. `linked_tour.malloynb`). A plain
 * browser anchor resolved them against the current URL, dropped the package
 * segment, and 404'd with a misleading "Package manifest ... does not exist".
 * They must route within the package instead.
 *
 * No bundled example ships a README.malloy, so the spec adds its own
 * README.malloy + target notebook to a throwaway copy of the `storefront`
 * package (the same fixture pattern as notebook-givens.spec.ts). The target
 * name shares nothing with the package name, so a dropped package segment
 * cannot accidentally still resolve.
 */

const FIXTURE_README = "README.malloy";
const FIXTURE_TARGET = "linked_tour.malloy";

// A report's route is its path without `.malloy`, so that is what the link
// names; `linked_tour.malloy` would open the model view instead.
const README_SOURCE = `## artifact { layout=linear tiles=[toc { kind=text }] }

##|(markdown) toc
# Storefront README

Table of contents:

- [Linked tour](linked_tour)
|##
`;

const TARGET_SOURCE = `## artifact { layout=linear tiles=[intro { kind=text }] }

##|(markdown) intro
# Linked tour

Fixture report for the README-link routing regression spec.
|##
`;

let pe: PackageEnv;

test.describe("notebook-readme-links", () => {
   // eslint-disable-next-line no-empty-pattern
   test.beforeAll(async ({}, testInfo) => {
      pe = await registerPackageEnv(
         testInfo.project.use.baseURL ?? "http://localhost:4000",
         "nbreadme",
         exampleFixture(PACKAGES.storefront),
         PACKAGES.storefront,
         { [FIXTURE_README]: README_SOURCE, [FIXTURE_TARGET]: TARGET_SOURCE },
      );
   });

   test.afterAll(async () => {
      await pe?.dispose();
   });

   test("clicking a README link opens the notebook inside its package", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, pe.env);
      await openPackage(page, pe.env, pe.pkg);

      // The fixture README.malloy renders a table of contents linking to
      // another notebook. Clicking it must stay within the storefront package.
      const link = page.getByRole("link", { name: "Linked tour" });
      await expect(link).toBeVisible({ timeout: 60_000 });
      await link.click();

      await expect(page).toHaveURL(
         new RegExp(`/${pe.env}/${pe.pkg}/linked_tour$`),
      );
      // The target notebook renders its authored H1, proving it loaded rather
      // than the "does not exist" error the dropped-package route gave.
      await expect(
         // The report's title and the prose's own heading both say it.
         page.getByRole("heading", { name: "Linked tour", level: 1 }).first(),
      ).toBeVisible();
   });
});
