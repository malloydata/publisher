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
      // any section has rendered and pins nothing. "Data Apps" rather than
      // "Notebooks": an empty section no longer renders, and this package has
      // no notebooks.
      await expect(
         page.getByRole("heading", { name: "Data Apps" }),
      ).toBeVisible();
      await expect(
         page.getByRole("heading", { name: "Semantic Models" }),
      ).toBeVisible();

      await expect(
         page.getByRole("heading", { name: "Governed Reports" }),
      ).toHaveCount(0);
      // The section this package has nothing for is absent rather than empty.
      await expect(
         page.getByRole("heading", { name: "Notebooks" }),
      ).toHaveCount(0);
      await expect(page.getByRole("heading", { name: "Pages" })).toHaveCount(0);
   });

   test("clicking a data app routes to data-apps/ and mounts the viewer", async ({
      page,
   }) => {
      // Record client-side navigations before anything loads, and assert on the
      // route the app PUSHES rather than on where the URL settles: the listing
      // must emit `data-apps/` itself. (The old `pages/` alias that would once
      // have masked a wrong route is gone; this keeps the listing honest on its
      // own.)
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

   test("an old pages/ bookmark is no longer rewritten to data-apps", async ({
      page,
   }) => {
      // The `pages/` alias for data apps is retired: neither half of it remains
      // (`pages` is out of SPA_OWNED_SEGMENTS on the server, and ModelPage no
      // longer rewrites the path), so an old bookmark is an ordinary path into
      // the package's `public/` directory and the viewer does not mount for it.
      await page.goto(`/${DEFAULT_ENV}/${PACKAGES.dataApp}/pages/index.html`);
      await expect(page).not.toHaveURL(/data-apps/);
      await expect(page.locator("iframe")).toHaveCount(0);
   });

   test("a model path under pages/ is an ordinary model path", async ({
      page,
   }) => {
      // A `.malloy` or `.malloynb` can legitimately live in a package's `pages/`
      // directory. Asserted on RENDERED TEXT rather than on the URL: `toHaveURL`
      // given the path just requested is satisfied on its first poll, before
      // React has rendered, so it would pass whether or not a rewrite happened.
      // The message is written by the app and quotes the path, so it cannot pass
      // early and changes if the path is rewritten. The file need not exist; not
      // existing is what produces a message naming it.
      await page.goto(
         `/${DEFAULT_ENV}/${PACKAGES.dataApp}/pages/report.malloy`,
      );

      await expect(
         page.getByText("pages/report.malloy does not exist"),
      ).toBeVisible();
      await expect(page).not.toHaveURL(/data-apps/);
   });
});
