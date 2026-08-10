import { expect, test, type Page } from "@playwright/test";
import { DEFAULT_ENV, PACKAGES } from "./helpers/fixtures";

/**
 * The notebook's Parameters panel, now that its values live in the URL.
 *
 * Runs against `governed-analytics`, which is the only shipped package whose
 * model declares `given:`, `REGION :: filter<string>` and
 * `MIN_AMOUNT :: number`. Both render as typed text boxes: `select` and
 * `multiselect` need `Given.control` and `Given.suggest` populated by the
 * server, which no shipped model does yet, so the picker path is covered by
 * unit tests rather than here. Do not read a green run here as evidence that
 * the multiselect works end to end.
 */
const NOTEBOOK = `/${DEFAULT_ENV}/${PACKAGES.governed}/orders.malloynb`;

/** The panel is only rendered once the notebook's sources have loaded. */
async function openNotebook(page: Page, search = "") {
   await page.goto(`${NOTEBOOK}${search}`);
   await expect(page.getByText("Parameters", { exact: true })).toBeVisible({
      timeout: 60_000,
   });
}

test.describe("notebook givens are URL-addressable", () => {
   test("renders a control per declared given, with its description", async ({
      page,
   }) => {
      await openNotebook(page);

      await expect(page.getByLabel("REGION")).toBeVisible();
      await expect(page.getByLabel("MIN_AMOUNT")).toBeVisible();
      // The `#(description=...)` annotation is the helper text, which is the
      // only thing telling a reader what a given means.
      await expect(
         page.getByText("Region to focus on — leave empty for all regions"),
      ).toBeVisible();
   });

   test("setting a control writes it to the URL", async ({ page }) => {
      await openNotebook(page);

      await page.getByLabel("MIN_AMOUNT").fill("500");
      await expect(page).toHaveURL(/[?&]MIN_AMOUNT=500/);
   });

   test("a control change re-runs the cells and they render", async ({
      page,
   }) => {
      // The assertion this suite was missing, and its absence hid a regression
      // that left every cell blank: an abort wired as the run effect's cleanup
      // fired on the host's own URL echo, cancelling the run the change had just
      // started, while the run-key guard declined to start a replacement. There
      // was no error and no spinner, so a suite that only checked the URL and the
      // control passed over a completely empty notebook.
      await openNotebook(page);
      const results = page.locator(".malloy-table.root, .malloy-render");
      await expect(results.first()).toBeVisible({ timeout: 60_000 });
      const before = await results.count();
      expect(before).toBeGreaterThan(0);

      await page.getByLabel("MIN_AMOUNT").fill("500");
      await expect(page).toHaveURL(/[?&]MIN_AMOUNT=500/);

      // Back to the same number of rendered results, not zero.
      await expect(results).toHaveCount(before, { timeout: 60_000 });
   });

   test("a value in the URL arrives applied, so a link reproduces a view", async ({
      page,
   }) => {
      await openNotebook(page, "?MIN_AMOUNT=500");

      await expect(page.getByLabel("MIN_AMOUNT")).toHaveValue("500");
      // And it is still there once the cells have run, rather than being
      // cleared by the panel reporting its own empty state on mount.
      await expect(page).toHaveURL(/[?&]MIN_AMOUNT=500/);
   });

   test("an unrelated query parameter survives a control change", async ({
      page,
   }) => {
      // The page's query string is not the panel's alone. Writing the applied
      // values used to replace it wholesale, which deleted anything else in it
      // on the first control change.
      await openNotebook(page, "?utm_source=slack");

      await page.getByLabel("MIN_AMOUNT").fill("500");

      await expect(page).toHaveURL(/[?&]MIN_AMOUNT=500/);
      await expect(page).toHaveURL(/[?&]utm_source=slack/);
   });

   test("Reset clears the controls and the URL with them", async ({ page }) => {
      // This notebook declares no starting values, so its starting point is
      // "nothing set" and Reset goes there. Reset used to be inert or to
      // double-run, depending on autorun; both are covered in the hook's spec.
      await openNotebook(page, "?MIN_AMOUNT=500");
      await expect(page.getByLabel("MIN_AMOUNT")).toHaveValue("500");

      await page.getByRole("button", { name: "Reset" }).click();

      await expect(page).not.toHaveURL(/MIN_AMOUNT/);
      await expect(page.getByLabel("MIN_AMOUNT")).toHaveValue("");
   });
});
