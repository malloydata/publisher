// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import * as fs from "fs/promises";
import * as path from "path";
import { fileURLToPath } from "url";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";
import { filterButton, openFilter } from "./helpers/filters";

/**
 * End-to-end coverage for `#(authorize)` source gates in a notebook. The
 * storefront example ships no gated model, so the spec writes its own
 * model and a linear report into the `storefront` package, reloads, and cleans up.
 *
 * The gate is a LOCK: `#(authorize)` asks whether the caller may reach the
 * source at all, so every way of not satisfying it is a 403 and the cell
 * renders no result. `role` has no default, so on load the lock is unsatisfied
 * and grants once the user supplies `role = analyst` in the Filters panel.
 * The query spotlights a single product in the Jeans category ("Cobalt Bootcut
 * Jean"), the visible signal that the lock admitted.
 *
 * One outcome, three ways to reach it, and the point of the spec is that the
 * caller cannot tell them apart: the given is unset, the given is set to
 * something the lock does not admit, or the lock could not be attached at all.
 * All three are a 403 naming the source and nothing else. A row filter would
 * be the other shape — `#(access_filter)` answers 200 with the caller's
 * (empty) rows — and this source carries none.
 *
 * Run this against a normal server, not one started with `--watch-env examples`:
 * watch mode symlinks PKG_DIR to the tracked `examples/storefront` sources, so
 * the fixture writes below would land in version control.
 */

const FIXTURE_MODEL = "authz_gate.malloy";
const FIXTURE_NOTEBOOK = "authz_gate_notebook.malloy";

const PKG_DIR = path.resolve(
   path.dirname(fileURLToPath(import.meta.url)),
   "../../../server/publisher_data/examples/storefront",
);

const MODEL_SOURCE = `##! experimental.givens

given: role :: string

#(authorize) 'analyst' = $role
source: gated_products is duckdb.table('data/products.parquet') extend {
  primary_key: product_id
  view: spotlight is {
    where: category = 'Jeans'
    select: product_id, name
    order_by: product_id
    limit: 1
  }
}
`;

// One query tile, run as this query. The report enables `experimental.givens`:
// authorize validation compiles a `$role` probe against the report's model,
// which needs the flag set here even though `role` itself is declared in the
// imported model.
const CELL = "run: gated_products -> spotlight";
// The tile reads `role` (a condition true whenever role is set), so the report shows a
// control for it: a report shows controls only for the givens its tiles read,
// and a gate's own givens are otherwise the host's to supply.
const NOTEBOOK_SOURCE = `##! experimental.givens
## artifact { layout=linear tiles=["gated_tiles -> spotlight_for_role"] }
import "authz_gate.malloy"

source: gated_tiles is gated_products extend {
  view: spotlight_for_role is spotlight + { where: $role = $role }
}
`;

async function reloadPackage(baseURL: string): Promise<void> {
   const url = `${baseURL}/api/v0/environments/${DEFAULT_ENV}/packages/${PACKAGES.storefront}?reload=true`;
   const res = await fetch(url);
   if (!res.ok) {
      throw new Error(`Package reload failed: ${res.status} ${res.statusText}`);
   }
}

test.describe("notebook-authorize", () => {
   test.beforeAll(async ({ baseURL }) => {
      await fs.writeFile(path.join(PKG_DIR, FIXTURE_MODEL), MODEL_SOURCE);
      await fs.writeFile(path.join(PKG_DIR, FIXTURE_NOTEBOOK), NOTEBOOK_SOURCE);
      await reloadPackage(baseURL!);
   });

   test.afterAll(async ({ baseURL }) => {
      await fs.unlink(path.join(PKG_DIR, FIXTURE_MODEL)).catch(() => undefined);
      await fs
         .unlink(path.join(PKG_DIR, FIXTURE_NOTEBOOK))
         .catch(() => undefined);
      await reloadPackage(baseURL!).catch(() => undefined);
   });

   /** A report's cell runs through the query route, its text as the query. */
   const QUERY_ROUTE = new RegExp(
      `/models/${FIXTURE_NOTEBOOK.replace(".", "\\.")}/query`,
   );
   const runCell = (baseURL: string, givens?: Record<string, string>) =>
      fetch(
         `${baseURL}/api/v0/environments/${DEFAULT_ENV}/packages/${PACKAGES.storefront}/models/${FIXTURE_NOTEBOOK}/query`,
         {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ query: CELL, givens }),
         },
      );

   /** Rows out of a cell response, whose `result` is JSON text. */
   async function cellRows(res: Response): Promise<unknown[]> {
      const body = (await res.json()) as { result?: string };
      if (!body.result) throw new Error("cell response carried no result");
      const parsed = JSON.parse(body.result) as {
         data?: { array_value?: unknown[] };
      };
      return parsed.data?.array_value ?? [];
   }

   test("an unsupplied given is a 403 that names only the source", async ({
      baseURL,
   }) => {
      // `role` has no default, so the lock has nothing to evaluate and denies.
      // The message must name the source and stop there — leaking either the
      // given's name or the literal it is compared against would tell an
      // outsider how to satisfy a lock they were just refused.
      const res = await runCell(baseURL!);
      expect(res.status).toBe(403);
      const body = (await res.json()) as { message?: string };
      expect(body.message).toContain("gated_products");
      expect(body.message ?? "").not.toContain("analyst");
      expect(body.message ?? "").not.toContain("role");
   });

   test("a non-matching given is a 403, indistinguishable from an unset one", async ({
      baseURL,
   }) => {
      // The denial shape for a lock, and the case the whole route split exists
      // for: `intern` is refused outright rather than served an empty result
      // it could mistake for "this source holds no Jeans".
      const res = await runCell(baseURL!, { role: "intern" });
      expect(res.status).toBe(403);
      const body = (await res.json()) as { message?: string };
      expect(body.message ?? "").not.toContain("analyst");
   });

   test("role = analyst returns the gated row", async ({ baseURL }) => {
      // Asserts the ROW, not just the 200: a 200 carrying zero rows is what a
      // silently-dropped gate produces, and it would pass a status-only
      // version of this test.
      const res = await runCell(baseURL!, { role: "analyst" });
      expect(res.status).toBe(200);
      const rows = await cellRows(res);
      expect(rows).toHaveLength(1);
      expect(JSON.stringify(rows)).toContain("Cobalt Bootcut Jean");
   });

   async function openNotebook(page: import("@playwright/test").Page) {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.storefront);
      // An untitled report is listed by its basename.
      await page.getByText("authz_gate_notebook", { exact: true }).click();
      await expect(page).toHaveURL(/\/authz_gate_notebook$/);
      await expect(filterButton(page, "role")).toBeVisible();
   }

   /** Type a role into its filter's popover. */
   async function supplyRole(
      page: import("@playwright/test").Page,
      role = "analyst",
   ) {
      const filter = await openFilter(page, "role");
      await filter.getByLabel("role", { exact: true }).fill(role);
   }

   test("UI: result is gated until the given is supplied", async ({ page }) => {
      // `role` declares no default, so it is a required filter: the tile that
      // reads it waits for a value instead of running into the lock, and no
      // cell request is sent while it waits.
      const cellRequests: string[] = [];
      page.on("request", (r) => {
         if (QUERY_ROUTE.test(r.url())) cellRequests.push(r.url());
      });
      await openNotebook(page);
      await expect(page.getByText("Waiting for role.")).toBeVisible();
      await expect(page.getByText("Cobalt Bootcut Jean")).toHaveCount(0);
      expect(cellRequests).toHaveLength(0);

      // Supplying the satisfying given runs the tile and the result appears.
      const cellResponse = page.waitForResponse(
         (r) => QUERY_ROUTE.test(r.url()) && r.request().method() === "POST",
         { timeout: 30_000 },
      );
      await supplyRole(page);
      expect((await cellResponse).status()).toBe(200);
      await expect(page.getByText("Cobalt Bootcut Jean").first()).toBeVisible();
   });

   test("UI: a given supplied mid-execution is applied once the run finishes", async ({
      page,
   }) => {
      // Hold the first (denied) cell run so the report stays executing while
      // we change `role`. This is the window where a given change used to be
      // recorded but never re-run. Only the first cell run is held.
      let held = false;
      await page.route(QUERY_ROUTE, async (route) => {
         if (!held) {
            held = true;
            await new Promise((r) => setTimeout(r, 5000));
         }
         await route.continue();
      });

      // `role` is required, so nothing runs until it is set. Set one the lock
      // refuses, wait until that run is actually in flight (a deterministic
      // "mid-execution" signal, no fixed delay), then change it to one it admits.
      await openNotebook(page);
      const firstCellRequest = page.waitForRequest(
         (r) => QUERY_ROUTE.test(r.url()),
         { timeout: 30_000 },
      );
      await supplyRole(page, "intern");
      await firstCellRequest;
      await supplyRole(page, "analyst");

      // Once the held run finishes, the mid-flight given must be picked up and
      // re-executed — the result appears without any further interaction.
      await expect(page.getByText("Cobalt Bootcut Jean").first()).toBeVisible();
   });
});
