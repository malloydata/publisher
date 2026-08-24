// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";

/**
 * The only coverage of the data-app click-through. Three things have to agree
 * for it to work and none of them is checked anywhere else: the section label,
 * the route Package.tsx emits, and the prefix ModelPage strips to build the
 * viewer's resource URI. A rename that updates two of the three leaves the
 * listing pointing somewhere the router does not answer, and the rest of the
 * suite stays green because no other spec opens this package's detail page.
 */
test.describe("package-data-apps", () => {
   test("Data Apps section lists the package's HTML apps", async ({ page }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.dataApp);

      await expect(
         page.getByRole("heading", { name: "Data Apps" }),
      ).toBeVisible();
      await expect(page.getByText("index.html", { exact: true })).toBeVisible();
   });

   test("the renamed section labels are the ones rendered", async ({
      page,
   }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.dataApp);

      // Anchor on a positive assertion first. toHaveCount(0) is satisfied the
      // instant the page is blank, so a bare absence check here passes before
      // any section has rendered and pins nothing.
      await expect(
         page.getByRole("heading", { name: "Notebooks" }),
      ).toBeVisible();
      await expect(
         page.getByRole("heading", { name: "Data Apps" }),
      ).toBeVisible();

      await expect(
         page.getByRole("heading", { name: "Governed Reports" }),
      ).toHaveCount(0);
      await expect(page.getByRole("heading", { name: "Pages" })).toHaveCount(0);
   });

   test("clicking a data app routes to data-apps/ and mounts the viewer", async ({
      page,
   }) => {
      // Record client-side navigations before anything loads. The final URL alone
      // stopped being enough when the deprecated `pages/` alias landed: if the
      // listing emitted the OLD route, the alias would rewrite it and this test
      // would still see `data-apps/` at the end, so a regression in the emitted
      // route would pass unnoticed until the alias is deleted and it becomes a
      // broken listing with nothing covering it. The route the app pushes is
      // observable even when the URL it settles on is not. Delete this along with
      // the alias, which is when the final URL becomes sufficient again.
      await page.addInitScript(() => {
         const seen: string[] = [];
         (window as unknown as { __navs: string[] }).__navs = seen;
         const record = (url?: unknown) =>
            seen.push(url === undefined ? location.href : String(url));
         const push = history.pushState.bind(history);
         const replace = history.replaceState.bind(history);
         history.pushState = (a: unknown, b: unknown, url?: unknown) => {
            record(url);
            return push(a, b as string, url as string);
         };
         history.replaceState = (a: unknown, b: unknown, url?: unknown) => {
            record(url);
            return replace(a, b as string, url as string);
         };
      });

      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.dataApp);

      await page.getByText("index.html", { exact: true }).click();

      // Package.tsx emits `data-apps/<file>`; ModelPage strips that prefix and
      // renders <DataAppViewer>, which iframes the standalone URL. Three separate
      // things, so three assertions: the URL it settles on, what the iframe points
      // at, and the route the app actually asked for. Each of the two below exists
      // because this one alone passed a mutation of the thing it was meant to pin.
      await expect(page).toHaveURL(
         new RegExp(
            `/${DEFAULT_ENV}/${PACKAGES.dataApp}/data-apps/index\\.html`,
         ),
      );
      // Assert what the iframe POINTS AT, not just that one exists. Counting it
      // cannot see a wrong target: leaving the `data-apps/` prefix on the path
      // still renders an iframe, one that 404s inside itself, and `toHaveCount(1)`
      // passes throughout. Mutation-checked, and it is the prefix strip this URL
      // is the only witness to.
      await expect(page.locator("iframe")).toHaveAttribute(
         "src",
         new RegExp(
            `/environments/${DEFAULT_ENV}/packages/${PACKAGES.dataApp}/index\\.html`,
         ),
      );

      // The listing must route straight to the new form. Asserted on what the app
      // pushed rather than on where it ended up, so the alias cannot cover for a
      // listing that still emits `pages/`.
      const navigations = await page.evaluate(
         () => (window as unknown as { __navs: string[] }).__navs,
      );
      expect(navigations.some((url) => url.includes("/data-apps/"))).toBe(true);
      expect(navigations.filter((url) => url.includes("/pages/"))).toEqual([]);
   });

   test("an old pages/ bookmark self-corrects to the data-apps URL", async ({
      page,
   }) => {
      // The deprecated alias, and the only end-to-end proof it works. Both
      // halves have to hold and they live in different packages: `pages` is in
      // SPA_OWNED_SEGMENTS (server) so the shell is served at all, and ModelPage
      // (app) rewrites the path. Drop either and this fails, which is the point,
      // because the alias is scheduled for deletion and a test is the only thing
      // that will tell whoever deletes it that they took half of it.
      await page.goto(`/${DEFAULT_ENV}/${PACKAGES.dataApp}/pages/index.html`);

      // The address bar is the deliverable here: a viewer that mounted on the
      // old URL would still leave the stale link in circulation.
      await expect(page).toHaveURL(
         new RegExp(
            `/${DEFAULT_ENV}/${PACKAGES.dataApp}/data-apps/index\\.html`,
         ),
      );
      // Same reason as above: the target, not the count. An alias that reaches the
      // viewer with the wrong path is a broken bookmark that looks like a working
      // one, which is the failure this whole alias exists to avoid.
      await expect(page.locator("iframe")).toHaveAttribute(
         "src",
         new RegExp(
            `/environments/${DEFAULT_ENV}/packages/${PACKAGES.dataApp}/index\\.html`,
         ),
      );
   });

   test("the pages/ alias leaves a model path alone", async ({ page }) => {
      // The exclusion that keeps the alias from eating real routes: a `.malloy`
      // or `.malloynb` can legitimately live in a package's `pages/` directory,
      // and the old data-app URL never named one, so it must NOT be rewritten.
      // Broadening the alias to every path under `pages/` is the obvious
      // simplification of that branch, and this is what refuses it.
      //
      // Assert on RENDERED TEXT rather than on the URL, which is the whole
      // subtlety here. `toHaveURL` given the path just requested is satisfied on
      // its first poll, before React has rendered and had any chance to navigate,
      // so it passes whether the rewrite happens or not: dropping the guard left
      // this test green in 273ms while the page really did end up on `data-apps/`.
      // The message below is written by the app after that decision is made, and
      // it quotes the path, so it cannot pass early and it changes if the path is
      // rewritten. The file does not need to exist; not existing is what produces
      // a message naming it.
      await page.goto(
         `/${DEFAULT_ENV}/${PACKAGES.dataApp}/pages/report.malloy`,
      );

      await expect(
         page.getByText("pages/report.malloy does not exist"),
      ).toBeVisible();
      await expect(page).not.toHaveURL(/data-apps/);
   });
});
