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
 * The Explorer is gone, and the page is the model viewer. These specs hold it
 * to that: the same data is listed and queried, each value comes back as
 * written, and nothing on the page writes a value into Malloy text.
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
  #(index)
  dimension: item_name is name
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
      page.getByRole("heading", { name: "items", level: 1 }),
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

   test("lists the source's fields", async ({ page }) => {
      await openFixtureModel(page);

      const dimensions = page.getByRole("region", { name: "Dimensions" });
      await expect(
         dimensions.getByText("item_name", { exact: true }),
      ).toBeVisible();
      await expect(
         dimensions.getByText("string", { exact: true }).first(),
      ).toBeVisible();
      const measures = page.getByRole("region", { name: "Measures" });
      await expect(
         measures.getByText("item_count", { exact: true }),
      ).toBeVisible();
   });

   test("offers no query builder to write a value into", async ({ page }) => {
      await openFixtureModel(page);
      // Loaded: the named query's row is drawn with or without a builder
      // beside it, and nothing is lazy-loading behind it.
      await expect(
         page
            .getByRole("region", { name: "Queries" })
            .getByRole("button", { name: /all_names/ }),
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

      const queries = page.getByRole("region", { name: "Queries" });
      await queries.getByRole("button", { name: /all_names/ }).click();
      for (const name of [QUOTED, PLAIN, CRAFTED]) {
         await expect(queries.getByText(name, { exact: true })).toBeVisible();
      }
   });

   // Where the Explorer drilled into a value, the viewer lists them: one
   // query that groups by the quoted field name, with no value in its text.
   test("lists a dimension's values as written, with none in the query", async ({
      page,
   }) => {
      await openFixtureModel(page);
      const sent: string[] = [];
      page.on("request", (request) => {
         if (request.method() === "POST" && request.url().includes("/query")) {
            sent.push(request.postData() ?? "");
         }
      });

      const dimensions = page.getByRole("region", { name: "Dimensions" });
      await dimensions.getByRole("button", { name: /item_name/ }).click();
      const values = page.getByTestId("model-viewer-values");
      for (const name of [QUOTED, PLAIN, CRAFTED]) {
         await expect(values.getByText(name, { exact: true })).toBeVisible();
      }
      expect(sent).toHaveLength(1);
      expect(sent[0]).toContain("group_by: `item_name`");
      expect(sent[0]).not.toContain("Monitor");
   });
});
