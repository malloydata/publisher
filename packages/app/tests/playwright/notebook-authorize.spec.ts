import { expect, test } from "@playwright/test";
import * as fs from "fs/promises";
import * as path from "path";
import { fileURLToPath } from "url";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";
import { gotoHome, openEnvironment, openPackage } from "./helpers/navigation";

/**
 * End-to-end coverage for `#(authorize)` source gates in a notebook. The
 * the storefront example ships no gated model, so the spec writes its own
 * .malloy + .malloynb into the `storefront` package, reloads, and cleans up.
 *
 * The gated source requires `$role = 'analyst'`; `role` has no default, so the
 * gate denies on load (HTTP 403, no cell result) and grants once the user
 * supplies `role = analyst` in the Parameters panel. The query spotlights a
 * single product in the Jeans category ("Cobalt Bootcut Jean"), the visible
 * signal that the gate passed.
 */

const FIXTURE_MODEL = "authz_gate.malloy";
const FIXTURE_NOTEBOOK = "authz_gate_notebook.malloynb";

const PKG_DIR = path.resolve(
   path.dirname(fileURLToPath(import.meta.url)),
   "../../../server/publisher_data/examples/storefront",
);

const MODEL_SOURCE = `##! experimental.givens

given: role :: string

#(authorize) "$role = 'analyst'"
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

async function reloadFaaPackage(baseURL: string): Promise<void> {
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
      await reloadFaaPackage(baseURL!);
   });

   test.afterAll(async ({ baseURL }) => {
      await fs.unlink(path.join(PKG_DIR, FIXTURE_MODEL)).catch(() => undefined);
      await fs
         .unlink(path.join(PKG_DIR, FIXTURE_NOTEBOOK))
         .catch(() => undefined);
      await reloadFaaPackage(baseURL!).catch(() => undefined);
   });

   const cellUrl = (baseURL: string, givens?: Record<string, string>) => {
      const base = `${baseURL}/api/v0/environments/${DEFAULT_ENV}/packages/${PACKAGES.storefront}/notebooks/${FIXTURE_NOTEBOOK}/cells/0`;
      return givens
         ? `${base}?givens=${encodeURIComponent(JSON.stringify(givens))}`
         : base;
   };

   test("notebook cell is denied (403) without the gate-passing given", async ({
      baseURL,
   }) => {
      const res = await fetch(cellUrl(baseURL!));
      expect(res.status).toBe(403);
      const body = (await res.json()) as { message?: string };
      // Names the source; never the gate expression.
      expect(body.message).toContain("gated_products");
      expect(body.message ?? "").not.toContain("analyst");
   });

   test("notebook cell succeeds (200) with role = analyst", async ({
      baseURL,
   }) => {
      const res = await fetch(cellUrl(baseURL!, { role: "analyst" }));
      expect(res.status).toBe(200);
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
      // cell runs on load (role unset) and returns 403. Arm the wait before
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
      expect((await deniedResponse).status()).toBe(403);

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
