import { expect, test } from "@playwright/test";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";
import {
   gotoHome,
   openEnvironment,
   openMaterializations,
   openPackage,
} from "./helpers/navigation";
import { getPublisherStatus } from "./helpers/publisherStatus";

const PKG = PACKAGES.storefront;

// The runs list shows friendly status labels: in-progress phases read as
// "Pending", the terminal success state as "Done", and failures/cancellations
// keep their own labels. Auto-run drives every phase to a terminal state on its
// own. Terminal states are the only ones a record can be deleted from.
const TERMINAL_STATUS = /^(Done|Failed|Cancelled)$/;

test.describe("package-materializations: read", () => {
   test("package page exposes a Materializations entry", async ({ page }) => {
      await gotoHome(page);
      await openEnvironment(page, DEFAULT_ENV);
      await openPackage(page, DEFAULT_ENV, PKG);

      await expect(
         page.getByRole("heading", { name: "Materializations", level: 6 }),
      ).toBeVisible({ timeout: 60_000 });
      await expect(
         page.getByRole("button", { name: "Materializations" }),
      ).toBeVisible();
   });

   test("materializations screen renders the Runs section", async ({
      page,
   }) => {
      await openMaterializations(page, DEFAULT_ENV, PKG);

      await expect(
         page.getByRole("heading", { name: "Runs", level: 6 }),
      ).toBeVisible();
   });
});

test.describe("package-materializations: mutable", () => {
   test.beforeAll(async ({}, testInfo) => {
      const baseURL = testInfo.project.use.baseURL ?? "http://localhost:4000";
      const { mutable } = await getPublisherStatus(baseURL);
      test.skip(!mutable, "publisher is read-only");
   });

   test("New materialization dialog defaults to a full run", async ({
      page,
   }) => {
      await openMaterializations(page, DEFAULT_ENV, PKG);

      await page.getByRole("button", { name: "New materialization" }).click();
      const dialog = page.getByRole("dialog", { name: "New materialization" });
      await expect(dialog).toBeVisible();
      await expect(
         dialog.getByText("Force refresh (rebuild even if unchanged)"),
      ).toBeVisible();
      // The dialog always runs every phase — there is no two-round toggle.
      await expect(dialog.getByLabel(/Pause between phases/)).toHaveCount(0);
      await expect(
         dialog.getByRole("button", { name: "Materialize" }),
      ).toBeVisible();

      await dialog.getByRole("button", { name: "Cancel" }).click();
      await expect(dialog).toBeHidden();
   });

   test("auto-run drives to a terminal state on its own → delete", async ({
      page,
   }) => {
      test.setTimeout(120_000);
      await openMaterializations(page, DEFAULT_ENV, PKG);

      const actions = page.getByRole("button", {
         name: /Materialization actions for/,
      });

      // --- Materialize (auto-run: compile + plan + build + load) ---
      await page.getByRole("button", { name: "New materialization" }).click();
      const dialog = page.getByRole("dialog", { name: "New materialization" });
      await expect(dialog).toBeVisible();
      await dialog.getByRole("button", { name: "Materialize" }).click();
      await expect(dialog).toBeHidden({ timeout: 15_000 });

      // A run row appears in the table while the run progresses.
      const row = page.locator('table tbody tr[role="button"]').first();
      await expect(row).toBeVisible({ timeout: 30_000 });

      // --- Auto-run settles itself ---
      // The publisher drives every phase without pausing, so the run reaches a
      // terminal state (MANIFEST_FILE_READY on success, FAILED on a broken
      // package) with no manual follow-up build call.
      await expect(page.getByText(TERMINAL_STATUS).first()).toBeVisible({
         timeout: 90_000,
      });

      // --- Delete the terminal run ---
      const countBefore = await actions.count();
      await actions.first().click();
      await page
         .getByRole("menuitem", { name: /Delete materialization/ })
         .click();
      const deleteDialog = page.getByRole("dialog", {
         name: "Delete Materialization",
      });
      await expect(deleteDialog).toBeVisible();
      await deleteDialog.getByRole("button", { name: "Delete" }).click();
      await expect(deleteDialog).toBeHidden({ timeout: 15_000 });
      await expect(actions).toHaveCount(Math.max(0, countBefore - 1));
   });

   test("a run row is keyboard-accessible and opens the detail dialog", async ({
      page,
   }) => {
      test.setTimeout(120_000);
      await openMaterializations(page, DEFAULT_ENV, PKG);

      const row = page.locator('table tbody tr[role="button"]').first();
      if ((await row.count()) === 0) {
         await page
            .getByRole("button", { name: "New materialization" })
            .click();
         const create = page.getByRole("dialog", {
            name: "New materialization",
         });
         await create.getByRole("button", { name: "Materialize" }).click();
         await create.waitFor({ state: "hidden" });
         await row.waitFor({ timeout: 30_000 });
      }
      // Wait for a terminal state so the run list stops polling and the detail
      // content is stable before we assert against it.
      await expect(page.getByText(TERMINAL_STATUS).first()).toBeVisible({
         timeout: 90_000,
      });

      // The run row is a keyboard-operable button (role + tabindex), and Enter
      // opens the detail dialog, which always renders a "Build plan" section
      // sourced from Package.buildPlan (compile-time persist plan).
      await expect(row).toHaveAttribute("tabindex", "0");
      await row.focus();
      await page.keyboard.press("Enter");
      const detail = page.getByRole("dialog");
      await expect(detail).toBeVisible();
      await expect(
         detail.getByRole("heading", { name: "Build plan" }),
      ).toBeVisible();
      await detail.getByRole("button", { name: "close" }).click();
      await expect(detail).toHaveCount(0);
   });
});

test.describe("package-materializations: mutability parity with /api/v0/status", () => {
   test("New materialization button renders iff publisher reports mutable", async ({
      page,
   }, testInfo) => {
      const baseURL = testInfo.project.use.baseURL ?? "http://localhost:4000";
      const { mutable } = await getPublisherStatus(baseURL);
      const expected = mutable ? 1 : 0;

      await openMaterializations(page, DEFAULT_ENV, PKG);

      await expect(
         page.getByRole("button", { name: "New materialization" }),
      ).toHaveCount(expected);
   });
});
