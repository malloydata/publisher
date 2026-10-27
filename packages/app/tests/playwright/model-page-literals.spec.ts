// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, Page, test } from "@playwright/test";
import * as fs from "fs/promises";
import * as path from "path";
import { fileURLToPath } from "url";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";

/**
 * A model whose data holds double quotes, on the model page.
 *
 * The page used to embed the Explorer, which wrote a drilled cell back into
 * Malloy with `ASTQuery.toMalloy()`. `@malloydata/malloy-interfaces` puts a
 * string value into that text unescaped (its `escapeString` is a TODO), so
 * drilling into `27" Monitor` sent `drill: name = "27" Monitor"`, a compile
 * error, and drilling into `x" or "a" = "a` sent a filter that matched every
 * row. Nobody had to type anything: the values are data.
 *
 * The Explorer is gone. These specs hold the page to what replaces it: the
 * same data is listed and queried, and nothing on the page writes a value
 * into Malloy text.
 *
 * Like notebook-givens, the spec writes its model into the `storefront`
 * package directory and reloads the package, so run it against a normal
 * server, not one started with `--watch-env examples`.
 */

const FIXTURE_MODEL = "model_page_literals.malloy";

const PKG_DIR = path.resolve(
   path.dirname(fileURLToPath(import.meta.url)),
   "../../../server/publisher_data/examples/storefront",
);

const QUOTED = `27" Monitor`;
const CRAFTED = `x" or "a" = "a`;
const PLAIN = "Plain";

const MODEL_SOURCE = `source: items is duckdb.sql("""
  select * from (values ('${QUOTED}'), ('${PLAIN}'), ('${CRAFTED}')) as t(name)
""") extend {
  measure: item_count is count()
}

query: all_names is items -> {
  group_by: name
  order_by: name
}
`;

async function reloadPackage(baseURL: string): Promise<void> {
   const url = `${baseURL}/api/v0/environments/${DEFAULT_ENV}/packages/${PACKAGES.storefront}?reload=true`;
   const res = await fetch(url);
   if (!res.ok) {
      throw new Error(`Package reload failed: ${res.status} ${res.statusText}`);
   }
}

async function openFixtureModel(page: Page): Promise<void> {
   await page.goto(`/${DEFAULT_ENV}/${PACKAGES.storefront}/${FIXTURE_MODEL}`);
   await expect(
      page.getByRole("heading", { name: "Sources", level: 1 }),
   ).toBeVisible();
}

test.describe("model-page-literals", () => {
   test.beforeAll(async ({ baseURL }) => {
      await fs.writeFile(path.join(PKG_DIR, FIXTURE_MODEL), MODEL_SOURCE);
      await reloadPackage(baseURL!);
   });

   test.afterAll(async ({ baseURL }) => {
      await fs.unlink(path.join(PKG_DIR, FIXTURE_MODEL)).catch(() => undefined);
      await reloadPackage(baseURL!).catch(() => undefined);
   });

   test("lists the source and its fields, read-only", async ({ page }) => {
      await openFixtureModel(page);

      const source = page.getByRole("region", { name: "items", exact: true });
      await expect(source).toBeVisible();
      await expect(source.getByText("name", { exact: true })).toBeVisible();
      await expect(source.getByText("dimension · string")).toBeVisible();
      await expect(
         source.getByText("item_count", { exact: true }),
      ).toBeVisible();
      await expect(source.getByText("measure · number")).toBeVisible();
      // A field is something to read, not a control.
      await expect(source.getByRole("button")).toHaveCount(0);
   });

   test("offers no query builder to write a value into", async ({ page }) => {
      await openFixtureModel(page);
      // Loaded: the named query renders with or without a builder beside it.
      // The Explorer was a lazy chunk, so an absence check before the network
      // settles passes whether or not the page would have drawn one.
      await expect(
         page.getByRole("button", { name: "Run Query" }),
      ).toBeVisible();
      await page.waitForLoadState("networkidle");

      // The Explorer's own controls: its Run, and the query it edits.
      await expect(
         page.getByRole("button", { name: "Run", exact: true }),
      ).toHaveCount(0);
      await expect(page.getByText("Main query", { exact: true })).toHaveCount(
         0,
      );
   });

   // The values themselves were never the problem: a named query is sent by
   // name, so they come back as written.
   test("runs a named query over those values and shows each as written", async ({
      page,
   }) => {
      await openFixtureModel(page);

      await page.getByRole("button", { name: "Run Query" }).click();
      for (const name of [QUOTED, PLAIN, CRAFTED]) {
         await expect(page.getByText(name, { exact: true })).toBeVisible();
      }
   });
});
