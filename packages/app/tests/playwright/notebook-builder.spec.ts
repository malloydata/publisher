// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import { editText, editorOpen, tileByKey } from "./helpers/builder";
import {
   registerPackageEnv,
   serverFixture,
   type PackageEnv,
} from "./helpers/packageEnv";
import { saveChanges } from "./helpers/save";

/**
 * The fixture is copied to a temp directory because saving writes the notebook
 * file. A server that refuses the environment registration is read-only, so the
 * spec skips; the browser-draft save path is covered by the SDK's tests.
 */

const PKG = "notebooks-malloyyo";
const SLUG = "revenue_review";
const FILE = `notebooks/${SLUG}.malloy`;
const EDITED = "Revenue prose, edited in the browser";

const REVENUE_REVIEW = `##! experimental.givens
## artifact { kind=notebook title="Revenue review" tiles=[prose { kind=text }, "revenue_tiles -> by_month_tile"] }
import "../models/orders.malloy"

##|(markdown) prose
# Where revenue came from
Prose in **markdown**, any length.
|##

source: revenue_tiles is orders extend {
   # bar_chart
   # label="Revenue by month"
   view: by_month_tile is by_month
}
`;

let pe: PackageEnv;

test.describe("notebook-builder", () => {
   // eslint-disable-next-line no-empty-pattern
   test.beforeAll(async ({}, testInfo) => {
      pe = await registerPackageEnv(
         testInfo.project.use.baseURL ?? "http://localhost:4000",
         "nbbuilder",
         serverFixture(PKG),
         PKG,
         { [FILE]: REVENUE_REVIEW },
      );
   });
   test.afterAll(async () => {
      await pe?.dispose();
   });

   test("edits a text tile where it is shown and saves it into the package file", async ({
      page,
   }) => {
      const before = await pe.readSource(FILE);

      // The reader's Edit button leads into the editor.
      await page.goto(`/${pe.env}/${pe.pkg}/notebooks/${SLUG}`);
      await page.getByRole("button", { name: "Edit", exact: true }).click();
      await expect(page).toHaveURL(new RegExp(`/notebooks/${SLUG}/edit$`));
      await editorOpen(page);
      await expect(page.getByText("Where revenue came from")).toBeVisible();

      await editText(tileByKey(page, "text.prose"), EDITED);
      await expect(
         page.getByRole("button", { name: "Save", exact: true }),
      ).toBeEnabled();
      await saveChanges(page);

      // On disk: the edited tile, and every byte after it untouched.
      const after = await pe.readSource(FILE);
      expect(after).toContain(EDITED);
      expect(after).not.toContain("Where revenue came from");
      expect(after.slice(after.indexOf("source: revenue_tiles"))).toBe(
         before.slice(before.indexOf("source: revenue_tiles")),
      );

      // A reload re-reads the saved file, and the reader renders it too.
      await page.reload();
      await expect(page.getByText(EDITED)).toBeVisible({ timeout: 60_000 });
      await page.goto(`/${pe.env}/${pe.pkg}/notebooks/${SLUG}`);
      await expect(page.getByText(EDITED)).toBeVisible({ timeout: 60_000 });
      // A saved file whose queries no longer run would leave this empty.
      await expect(page.locator(".malloy-render").first()).toBeVisible({
         timeout: 60_000,
      });
   });
});
