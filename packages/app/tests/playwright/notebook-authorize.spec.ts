// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import * as fs from "fs/promises";
import * as path from "path";
import { fileURLToPath } from "url";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";

/**
 * End-to-end coverage for `#(authorize)` source gates in a notebook. The
 * storefront example ships no gated model, so the spec writes its own
 * .malloy + .malloynb into the `storefront` package, reloads, and cleans up.
 *
 * The gated source requires `$role = 'analyst'`; `role` has no default, so on
 * load the cell fails resolving the given (HTTP 400, no cell result) and
 * grants once the user supplies `role = analyst` in the Parameters panel. The
 * query spotlights a single product in the Jeans category ("Cobalt Bootcut
 * Jean"), the visible signal that the gate passed.
 *
 * Note the three distinct outcomes, which earlier versions of this spec
 * conflated: an unsupplied given is a 400, a NON-matching given is a 200 with
 * zero rows (the gate verdict — a row-level gate filters, it does not refuse),
 * and a 403 means the gate could not be attached at all.
 *
 * Run this against a normal server, not one started with `--watch-env examples`:
 * watch mode symlinks PKG_DIR to the tracked `examples/storefront` sources, so
 * the fixture writes below would land in version control.
 */

const FIXTURE_MODEL = "authz_gate.malloy";
const FIXTURE_NOTEBOOK = "authz_gate_notebook.malloynb";

const PKG_DIR = path.resolve(
   path.dirname(fileURLToPath(import.meta.url)),
   "../../../server/publisher_data/examples/storefront",
);

const MODEL_SOURCE = `##! experimental.givens

given: role :: string

#(authorize) $role = 'analyst'
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

// Single malloy cell (index 0) so the cell-GET index is unambiguous. The cell
// enables `experimental.givens` in its own compile scope: authorize validation
// compiles a `$role` probe against the notebook model, which needs the flag set
// here even though `role` itself is declared in the imported model.
const NOTEBOOK_SOURCE = `>>>malloy
##! experimental.givens
import "authz_gate.malloy"
run: gated_products -> spotlight
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

   const cellUrl = (baseURL: string, givens?: Record<string, string>) => {
      const base = `${baseURL}/api/v0/environments/${DEFAULT_ENV}/packages/${PACKAGES.storefront}/notebooks/${FIXTURE_NOTEBOOK}/cells/0`;
      return givens
         ? `${base}?givens=${encodeURIComponent(JSON.stringify(givens))}`
         : base;
   };

   /** Rows out of a cell response, whose `result` is JSON text. */
   async function cellRows(res: Response): Promise<unknown[]> {
      const body = (await res.json()) as { result?: string };
      if (!body.result) throw new Error("cell response carried no result");
      const parsed = JSON.parse(body.result) as {
         data?: { array_value?: unknown[] };
      };
      return parsed.data?.array_value ?? [];
   }

   test("an unsupplied given is a 400 about the given, not a gate verdict", async ({
      baseURL,
   }) => {
      // `role` has no default, so this fails resolving the given BEFORE any
      // gate decision is reached — a 400, not the 403 this test used to
      // assert. That earlier assertion passed for the wrong reason: the gate
      // could not attach over the `import` at all, and an attach failure
      // denies with exactly the 403 shape a real denial has. It therefore
      // passed whether the feature worked or was completely dead.
      const res = await fetch(cellUrl(baseURL!));
      expect(res.status).toBe(400);
      const body = (await res.json()) as { message?: string };
      expect(body.message).toContain("role");
      // Still never leaks the gate expression, whatever the status.
      expect(body.message ?? "").not.toContain("analyst");
   });

   test("a non-matching given returns 200 with ZERO rows — the gate verdict", async ({
      baseURL,
   }) => {
      // The actual denial shape for a row-level gate: not a status code, but
      // an empty result on a source the caller may query. This is the case a
      // status-only assertion cannot distinguish from success, and the one
      // that fails outright if the gate never attaches.
      const res = await fetch(cellUrl(baseURL!, { role: "intern" }));
      expect(res.status).toBe(200);
      expect(await cellRows(res)).toHaveLength(0);
   });

   test("role = analyst returns the gated row", async ({ baseURL }) => {
      // Asserts the ROW, not just the 200. A 200 carrying zero rows — which
      // is what a silently-dropped gate or an over-broad filter produces —
      // passed the previous status-only version of this test.
      const res = await fetch(cellUrl(baseURL!, { role: "analyst" }));
      expect(res.status).toBe(200);
      const rows = await cellRows(res);
      expect(rows).toHaveLength(1);
      expect(JSON.stringify(rows)).toContain("Cobalt Bootcut Jean");
   });

   async function openNotebook(page: import("@playwright/test").Page) {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PACKAGES.storefront);
      await page.getByText(FIXTURE_NOTEBOOK, { exact: true }).click();
      await expect(page).toHaveURL(/authz_gate_notebook\.malloynb/);
      await expect(page.getByLabel("role")).toBeVisible();
   }

   test("UI: result is gated until the given is supplied", async ({ page }) => {
      // Wait on the actual denied cell response, not a fixed delay: the gated
      // cell runs on load (role unset) and returns 400 — unset given, decided
      // before any gate verdict. Arm the wait before
      // opening so we can't miss it. A fixed timeout would either flake on a
      // slow runner or — worse — let the `role` fill land while the notebook is
      // still executing, where the change is recorded but not re-run.
      const deniedResponse = page.waitForResponse(
         (r) =>
            /\/notebooks\/.*authz_gate_notebook\.malloynb\/cells\/0/.test(
               r.url(),
            ) && r.request().method() === "GET",
         { timeout: 30_000 },
      );
      await openNotebook(page);
      expect((await deniedResponse).status()).toBe(400);

      // Execution finished (no spinner) and, denied, rendered no result.
      await expect(page.getByRole("progressbar")).toHaveCount(0);
      await expect(page.getByText("Cobalt Bootcut Jean")).toHaveCount(0);

      // With the notebook idle, supplying the satisfying given re-executes and
      // the result appears.
      await page.getByLabel("role").fill("analyst");
      await expect(page.getByText("Cobalt Bootcut Jean").first()).toBeVisible();
   });

   test("UI: a given supplied mid-execution is applied once the run finishes", async ({
      page,
   }) => {
      // Hold the first (denied) cell GET so the notebook stays executing while
      // we fill `role`. This is the window where a given change used to be
      // recorded but never re-run. Only the first /cells/0 request is held.
      let held = false;
      await page.route(/\/cells\/0/, async (route) => {
         if (!held) {
            held = true;
            await new Promise((r) => setTimeout(r, 5000));
         }
         await route.continue();
      });

      // Wait until the held request is actually in flight (notebook executing),
      // then fill — a deterministic "mid-execution" signal, no fixed delay.
      const firstCellRequest = page.waitForRequest(
         (r) => /\/cells\/0/.test(r.url()),
         { timeout: 30_000 },
      );
      await openNotebook(page);
      await firstCellRequest;
      await page.getByLabel("role").fill("analyst");

      // Once the held run finishes, the mid-flight given must be picked up and
      // re-executed — the result appears without any further interaction.
      await expect(page.getByText("Cobalt Bootcut Jean").first()).toBeVisible();
   });
});
