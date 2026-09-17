// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Page, expect } from "@playwright/test";
import { DEFAULT_ENV } from "./fixtures";

export async function gotoHome(page: Page): Promise<void> {
   await page.goto("/");
   await expect(
      page.getByRole("heading", { name: "Publisher", level: 1 }),
   ).toBeVisible();
}

export async function openEnvironment(
   page: Page,
   name: string = DEFAULT_ENV,
): Promise<void> {
   // An environment is a row on Home, and the row itself is the click target
   // — no separate "Open Environment" button. Its accessible name is the
   // environment's name alone.
   // Scoped to the Environments region, because the sidebar lists every
   // environment by the same name.
   const row = page
      .getByRole("region", { name: "Environments" })
      .getByRole("button", { name, exact: true });
   await expect(row).toBeVisible();
   await row.click();
   await expect(page).toHaveURL(new RegExp(`/${name}/?$`));
}

export async function openPackage(
   page: Page,
   env: string,
   pkg: string,
): Promise<void> {
   // The environment page renders its own name as h4 and each section header
   // as an h6, so "Packages" is its own heading rather than part of a longer
   // string.
   await expect(
      page.getByRole("heading", { name: "Packages", level: 6 }),
   ).toBeVisible();
   await page.getByText(pkg, { exact: true }).first().click();
   await expect(page).toHaveURL(new RegExp(`/${env}/${pkg}/?$`));
}

/**
 * Open the package page and wait for its Materializations section.
 *
 * Named `goto…` rather than `open…`: there is no materializations screen to
 * open, and the previous name outlived the page it referred to.
 */
export async function gotoMaterializations(
   page: Page,
   env: string,
   pkg: string,
): Promise<void> {
   await gotoHome(page);
   await openEnvironment(page, env);
   await openPackage(page, env, pkg);
   // Materializations are a section of the package's own page: the runs are
   // that package's history, so there is no separate screen to open. A
   // git-cloned package can take a while to appear.
   await expect(
      page.getByRole("heading", { name: "Materializations", level: 6 }),
   ).toBeVisible({ timeout: 60_000 });
}
