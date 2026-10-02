// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test, type Page, type Request } from "@playwright/test";
import fs from "fs";
import path from "path";
import {
   exampleFixture,
   registerPackageEnv,
   serverFixture,
   type PackageEnv,
} from "./helpers/packageEnv";

/**
 * The Console mounted the way an embedder mounts it: a host page that calls
 * `createMalloyRouter` with a document store of its own (see `harness/`). It
 * proves, in a real browser against a real server, that an authoritative host
 * keeps its documents in its own store and never in the package, and that a
 * host which is not the record is not offered a Save or a New it cannot honour.
 *
 * Needs the harness dev server (playwright.config.ts starts it) and the
 * Publisher it proxies to.
 */

const HARNESS = process.env.HARNESS_URL ?? "http://localhost:5199";
const API = process.env.PUBLISHER_URL ?? "http://localhost:4000";
const SURFACE = "notebooks-malloyyo-surface";
const LOCAL_TEXT = fs.readFileSync(
   path.join(serverFixture(SURFACE), "notebooks/local.malloy"),
   "utf8",
);

interface Host {
   documents: Map<string, string>;
   saves: Array<{ locator: { path: string }; content: string }>;
}

const hostOf = (page: Page) =>
   page.evaluate(() => {
      const host = (window as unknown as { __host: Host }).__host;
      return {
         documents: Object.fromEntries(host.documents),
         saves: host.saves.map((s) => ({
            path: s.locator.path,
            content: s.content,
         })),
      };
   });

/** Package writes the page makes: a PUT to a model file is the only way a document reaches the package. */
const watchPackageWrites = (page: Page) => {
   const writes: string[] = [];
   page.on("request", (request: Request) => {
      if (request.method() === "PUT" && request.url().includes("/models/"))
         writes.push(request.url());
   });
   return writes;
};

/** The server as a read-only deployment reports itself, so the Console sees no writeable package. */
const frozenServer = (page: Page) =>
   page.route("**/api/v0/status", async (route) => {
      const response = await route.fetch();
      const body = (await response.json()) as Record<string, unknown>;
      await route.fulfill({
         response,
         json: { ...body, frozenConfig: true, mutable: false },
      });
   });

test.describe("embedded host", () => {
   test.use({ baseURL: HARNESS });
   let curated: PackageEnv;
   let storefront: PackageEnv;

   test.beforeAll(async () => {
      // Without the config's webServer nothing starts the harness, so a missing one is a skip, not a failure.
      if (process.env.PLAYWRIGHT_USE_WEBSERVER === "0") {
         const up = await fetch(HARNESS).then(
            (res) => res.ok,
            () => false,
         );
         test.skip(
            !up,
            `the embedded-host harness is not running at ${HARNESS}`,
         );
      }
      curated = await registerPackageEnv(
         API,
         "hostcur",
         serverFixture(SURFACE),
         SURFACE,
      );
      storefront = await registerPackageEnv(
         API,
         "hostsf",
         exampleFixture("storefront"),
         "storefront",
      );
   });
   test.afterAll(async () => {
      await curated?.dispose();
      await storefront?.dispose();
   });

   test("opens from the host's record when the package withholds its text, and Save goes into the store, not the package", async ({
      page,
   }) => {
      const record = `${curated.env}/${curated.pkg}/notebooks/local.malloy`;
      await page.addInitScript(
         (seed) => {
            (window as unknown as { __seed: unknown }).__seed = seed;
         },
         { [record]: LOCAL_TEXT },
      );
      const writes = watchPackageWrites(page);
      await page.goto(`/${curated.env}/${curated.pkg}/notebooks/local/edit`);
      await expect(page.getByText("Editing", { exact: true })).toBeVisible({
         timeout: 60_000,
      });
      await expect(page.getByText("Kept in the host's record")).toBeVisible();
      await expect(page.getByText("read-only here")).toHaveCount(0);

      await page.getByRole("button", { name: "Edit text" }).first().click();
      await page
         .getByLabel("Markdown", { exact: true })
         .fill("Edited in the host.");
      await page.getByRole("button", { name: "Done", exact: true }).click();
      await page.getByRole("button", { name: "Save changes" }).click();
      await expect(
         page.getByRole("button", { name: "Saved", exact: true }),
      ).toBeVisible({
         timeout: 30_000,
      });

      const held = await hostOf(page);
      expect(held.saves).toHaveLength(1);
      expect(held.saves[0]!.path).toBe(record);
      expect(held.saves[0]!.content).toContain("Edited in the host.");
      expect(held.saves[0]!.content).toContain("kind=notebook");
      expect(writes).toEqual([]);
   });

   test("New creates into the host's store through the helper, opens it from there, and never writes the package", async ({
      page,
   }) => {
      const writes = watchPackageWrites(page);
      await page.goto(`/${storefront.env}/${storefront.pkg}`);
      await page.getByRole("button", { name: "New", exact: true }).click({
         timeout: 60_000,
      });
      await page.getByRole("menuitem", { name: "Notebook" }).click();
      const dialog = page.getByRole("dialog");
      await expect(dialog.getByLabel("Notebook title")).not.toHaveValue("", {
         timeout: 30_000,
      });
      await dialog.getByRole("combobox", { name: "Model" }).click();
      await page.getByRole("option", { name: "storefront.malloy" }).click();
      await dialog.getByRole("combobox", { name: "First query" }).click();
      await page
         .getByRole("option", {
            name: "order_items → sales_by_month",
            exact: true,
         })
         .click();
      await dialog.getByLabel("Notebook title").fill("Hosted notebook");
      await dialog.getByRole("button", { name: "Create Notebook" }).click();

      await expect(page).toHaveURL(
         /\/notebooks\/hosted-notebook(-\d+)?\/edit$/,
         {
            timeout: 60_000,
         },
      );
      const slug = new URL(page.url()).pathname.split("/").slice(-2)[0]!;
      await expect(page.getByText("Editing", { exact: true })).toBeVisible({
         timeout: 60_000,
      });

      const held = await hostOf(page);
      const stored = `${storefront.env}/${storefront.pkg}/notebooks/${slug}.malloy`;
      expect(Object.keys(held.documents)).toContain(stored);
      expect(held.documents[stored]).toContain(
         "run: order_items -> sales_by_month",
      );
      expect(writes).toEqual([]);
      // The package has no such file: the record is the only copy.
      const inPackage = await fetch(
         `${API}/api/v0/environments/${storefront.env}/packages/${storefront.pkg}/models/${encodeURIComponent(`notebooks/${slug}.malloy`)}`,
      );
      expect(inPackage.status).toBe(404);
   });

   test("a dashboard created through the helper lands in the host's store, not the package", async ({
      page,
   }) => {
      const writes = watchPackageWrites(page);
      await page.goto(`/${storefront.env}/${storefront.pkg}`);
      await page.getByRole("button", { name: "New", exact: true }).click({
         timeout: 60_000,
      });
      await page.getByRole("menuitem", { name: "Dashboard" }).click();
      const dialog = page.getByRole("dialog");
      await expect(dialog.getByLabel("Dashboard title")).not.toHaveValue("", {
         timeout: 30_000,
      });
      await dialog.getByRole("combobox", { name: "Model" }).click();
      await page.getByRole("option", { name: "storefront.malloy" }).click();
      await dialog.getByRole("combobox", { name: "First tile" }).click();
      await page
         .getByRole("option", {
            name: "order_items → sales_by_month",
            exact: true,
         })
         .click();
      await dialog.getByLabel("Dashboard title").fill("Hosted dashboard");
      await dialog.getByRole("button", { name: "Create Dashboard" }).click();
      await expect(page).toHaveURL(
         /\/dashboards\/hosted-dashboard(-\d+)?\/edit$/,
         {
            timeout: 60_000,
         },
      );
      const slug = new URL(page.url()).pathname.split("/").slice(-2)[0]!;
      const held = await hostOf(page);
      const stored = `${storefront.env}/${storefront.pkg}/dashboards/${slug}.malloy`;
      expect(held.documents[stored]).toContain("tiles=");
      expect(writes).toEqual([]);
      // The dashboard editor opens a file the package serves, so a dashboard
      // that exists only in the host's store waits for its deploy.
      await expect(page.getByText(/does not exist/)).toBeVisible({
         timeout: 60_000,
      });
   });

   test("a dashboard the package serves opens from the host's record, and Save goes into the store", async ({
      page,
   }) => {
      const record = `${storefront.env}/${storefront.pkg}/dashboards/overview.malloy`;
      const text = fs
         .readFileSync(
            path.join(
               exampleFixture("storefront"),
               "dashboards/overview.malloy",
            ),
            "utf8",
         )
         .replace('title="Storefront overview"', 'title="Record overview"');
      await page.addInitScript(
         (seed) => {
            (window as unknown as { __seed: unknown }).__seed = seed;
         },
         { [record]: text },
      );
      const writes = watchPackageWrites(page);
      await page.goto(
         `/${storefront.env}/${storefront.pkg}/dashboards/overview/edit`,
      );
      await expect(page.getByText("Editing", { exact: true })).toBeVisible({
         timeout: 60_000,
      });
      await expect(page.getByText("Record overview")).toBeVisible();
      await page.getByRole("button", { name: "Settings", exact: true }).click();
      const title = page.getByLabel("Dashboard title");
      await title.fill("Record overview, edited");
      await title.press("Escape");
      await page.getByRole("button", { name: "Save changes" }).click();
      await expect(
         page.getByRole("button", { name: "Saved", exact: true }),
      ).toBeVisible({
         timeout: 30_000,
      });
      const held = await hostOf(page);
      expect(held.saves.at(-1)!.path).toBe(record);
      expect(held.saves.at(-1)!.content).toContain("Record overview, edited");
      expect(writes).toEqual([]);
   });

   test("a host that is not the record saves into the package on a writeable server", async ({
      page,
   }) => {
      const writes = watchPackageWrites(page);
      await page.goto(
         `/${storefront.env}/${storefront.pkg}/notebooks/category-review/edit?host=scratch`,
      );
      await expect(page.getByText("Editing", { exact: true })).toBeVisible({
         timeout: 60_000,
      });
      await expect(
         page.getByText("Save writes the file into the package."),
      ).toBeVisible();
      await page.getByRole("button", { name: "Edit text" }).first().click();
      await page
         .getByLabel("Markdown", { exact: true })
         .fill("Scratch host edit.");
      await page.getByRole("button", { name: "Done", exact: true }).click();
      await page.getByRole("button", { name: "Save changes" }).click();
      await expect(
         page.getByRole("button", { name: "Saved", exact: true }),
      ).toBeVisible({
         timeout: 30_000,
      });
      expect(writes).toHaveLength(1);
      expect((await hostOf(page)).saves).toEqual([]);
   });

   test("a host that is not the record, on a server that takes no writes, shows no Save and no New", async ({
      page,
   }) => {
      await frozenServer(page);
      await page.goto(
         `/${storefront.env}/${storefront.pkg}/notebooks/category-review/edit?host=scratch`,
      );
      await expect(page.getByText("Editing", { exact: true })).toBeVisible({
         timeout: 60_000,
      });
      await expect(
         page.getByRole("button", { name: "Save changes" }),
      ).toHaveCount(0);
      await expect(page.getByText("does not take writes")).toBeVisible();

      await page.goto(`/${storefront.env}/${storefront.pkg}?host=scratch`);
      await expect(
         page.getByRole("heading", { name: "Notebooks", level: 6 }),
      ).toBeVisible({ timeout: 60_000 });
      await expect(
         page.getByRole("button", { name: "New", exact: true }),
      ).toHaveCount(0);
      await expect(
         page.getByRole("button", { name: "New notebook" }),
      ).toHaveCount(0);
   });
});
