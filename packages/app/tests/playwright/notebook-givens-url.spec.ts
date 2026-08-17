import { expect, test, type Locator, type Page } from "@playwright/test";
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

/**
 * A count that has stopped moving, rather than the first count seen.
 *
 * The notebook renders its cells over several frames, so sampling as soon as
 * the first result appears pins a partial render and lets a later assertion
 * pass over a notebook that came back mostly blank.
 */
async function settledCount(locator: Locator): Promise<number> {
   // THREE consecutive equal samples, not two, and it throws rather than
   // returning a number it is unsure of.
   //
   // Two samples 250ms apart prove the count PAUSED, not that it settled: the
   // notebook renders its cells over several frames, so any gap longer than the
   // interval yields two equal readings mid-render, and the helper returned a
   // partial count — the exact thing it was written to prevent. The old
   // give-up path made that worse by returning the unsettled count silently
   // after ten seconds, so a slow render pinned a baseline instead of failing.
   const STABLE = 3;
   let last = -1;
   let repeats = 0;
   for (let i = 0; i < 60; i++) {
      const current = await locator.count();
      repeats = current === last ? repeats + 1 : 0;
      if (current > 0 && repeats >= STABLE - 1) return current;
      last = current;
      await locator.page().waitForTimeout(250);
   }
   throw new Error(
      `Result count never settled: last saw ${last} over 15s. A baseline taken here would pin a partial render.`,
   );
}

/** Everything the rendered results say, as one string. */
async function resultsText(page: Page): Promise<string> {
   return (await page.locator(".malloy-render").allInnerTexts()).join("\u0001");
}

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
      // Wait for the count to STOP moving before sampling it. Taking it as soon
      // as the first result appeared pinned a partial render, so `toHaveCount`
      // below could be satisfied by a notebook that came back mostly blank.
      const before = await settledCount(results);
      expect(before).toBeGreaterThan(0);

      // Latch onto a node from the PRE-change render. `{cell.result && …}`
      // unmounts the subtree when a run starts, and a detached node stays
      // detached, so this cannot be missed the way a spinner can: a spinner is
      // a window between two polls, this is a one-way transition.
      const stale = await results.first().elementHandle();

      await page.getByLabel("MIN_AMOUNT").fill("500");
      await expect(page).toHaveURL(/[?&]MIN_AMOUNT=500/);

      // A run actually started. Without this the assertion below resolves on its
      // first poll against the PRE-change DOM: the URL is written immediately
      // while the run waits out GIVEN_SETTLE_MS. Measured on this notebook, the
      // count matched at 23ms and the run did not begin until 425ms, so the
      // assertion was passing 402ms before anything ran and pinned nothing.
      // `getByRole("progressbar")` reads better but is a window between two
      // polls rather than a one-way transition, so a fast run closes it unseen.
      await expect
         .poll(() => stale?.evaluate((n) => n.isConnected), { timeout: 30_000 })
         .toBe(false);

      // Back to the same number of rendered results, not zero.
      await expect(results).toHaveCount(before, { timeout: 60_000 });
   });

   test("a value in the URL arrives applied, so a link reproduces a view", async ({
      page,
   }) => {
      // The unfiltered notebook first, as the baseline this has to differ from.
      await openNotebook(page);
      const results = page.locator(".malloy-table.root, .malloy-render");
      await expect(results.first()).toBeVisible({ timeout: 60_000 });
      await settledCount(results);
      const unfiltered = await resultsText(page);

      await openNotebook(page, "?MIN_AMOUNT=500");
      await expect(page.getByLabel("MIN_AMOUNT")).toHaveValue("500");
      // And it is still there once the cells have run, rather than being
      // cleared by the panel reporting its own empty state on mount.
      await expect(page).toHaveURL(/[?&]MIN_AMOUNT=500/);

      // The assertion this test was named for and did not make. Checking the
      // control's text and the URL says nothing about what the CELLS ran with,
      // so the "runs bare, then runs again" regression the release notes claim
      // to fix would have passed here.
      await expect(results.first()).toBeVisible({ timeout: 60_000 });
      await settledCount(results);
      const filtered = await resultsText(page);
      expect(unfiltered.length).toBeGreaterThan(0);
      // Different numbers on screen. A row count cannot be used: these cells
      // render as charts and expose no rows to the page (`.malloy-table`
      // matches nothing here), which is why the first attempt at this
      // assertion counted zero of everything and passed vacuously.
      expect(filtered).not.toBe(unfiltered);
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

   test("a control change keeps the URL fragment", async ({ page }) => {
      // The page used to write the URL through `setSearchParams`, which
      // navigates to a search-only target; a target carrying no hash resolves
      // the fragment to empty, so changing any control dropped whatever anchor
      // the reader had arrived on. Browser-level, so it is pinned here rather
      // than in a unit test.
      await page.goto(`${NOTEBOOK}#cell-2`);
      await expect(page.getByText("Parameters", { exact: true })).toBeVisible({
         timeout: 60_000,
      });

      await page.getByLabel("MIN_AMOUNT").fill("500");
      await expect(page).toHaveURL(/[?&]MIN_AMOUNT=500/);
      await expect(page).toHaveURL(/#cell-2$/);
   });
});
