import { expect, Page, test } from "@playwright/test";

/**
 * E2E coverage for the `given:` runtime-parameter flow on a notebook.
 *
 * Exercises the `faa-givens-demo` package shipped from
 * credibledata/malloy-samples (registered in publisher.config.json),
 * which declares two givens on `carriers_with_parameters.malloy`:
 *
 *   given: carrier :: string is 'WN'
 *   given: after :: timestamp is @2003-01-01 00:00:00
 *
 * The notebook (`carriers_with_parameters.malloynb`) runs
 * `carrier_overview` and `by_destination` views that filter by
 * `$carrier`. The data on disk is fixed (FAA carriers.parquet +
 * flights.parquet), so the rendered numbers are stable — we can
 * hard-code them and detect regressions if the SDK ever stops
 * forwarding the given to Malloy.
 *
 * Flow exercised below:
 *
 *   1. Click through Home → malloy-samples → faa-givens-demo →
 *      carriers_with_parameters.malloynb (don't direct-navigate;
 *      the breadcrumb chips are part of the contract too).
 *   2. Assert the Parameters panel renders both givens.
 *   3. Default (no override) shows WN's numbers.
 *   4. Type carrier="WN" + Tab → same numbers (explicit override
 *      matches default).
 *   5. Click the input's clear-value button → defaults restored.
 *   6. Type carrier="AA" + Tab → American Airlines numbers appear.
 *   7. Clear again → defaults restored a second time.
 *
 * Requires the publisher running at http://localhost:4000 with the
 * faa-givens-demo package registered and the FAA parquet data
 * available (the package depends on /faa/data/{carriers,flights}.parquet).
 */

const BASE_URL = "http://localhost:4000/";

test.setTimeout(90_000);

// Numbers captured by inspecting the live notebook against the
// faa-givens-demo package on 2026-05-28. The data is static, so
// these are hard-coded assertions. If they drift, either the
// Malloy fixture data changed or the SDK stopped passing the
// given through — both are worth flagging.
const WN_OVERVIEW = {
  flightCount: "46,489",
  totalDistance: "29,552,271",
  name: "Southwest Airlines",
  nickname: "Southwest",
};
const WN_FIRST_DESTINATION = {
  code: "LAS",
  flightCount: "3,392",
  totalDistance: "2,339,062",
};
const AA_OVERVIEW = {
  flightCount: "16,713",
  totalDistance: "17,060,634",
  name: "American Airlines",
  nickname: "American",
};
const AA_FIRST_DESTINATION = {
  code: "DFW",
  flightCount: "4,601",
  totalDistance: "4,331,317",
};

// after=1998-05-15 widens the date window (default is @2003-01-01) so
// the counts grow. Captures both the single-given case (only `after`
// set; carrier defaults to WN) and the multi-given case (both set).
const WN_AFTER_1998_OVERVIEW = {
  flightCount: "88,751",
  totalDistance: "54,619,152",
  name: "Southwest Airlines",
  nickname: "Southwest",
};
const WN_AFTER_1998_FIRST_DESTINATION = {
  // Note: with the wider window, the top destination shifts from
  // LAS (default window) to PHX.
  code: "PHX",
  flightCount: "6,437",
  totalDistance: "4,869,780",
};
const AA_AFTER_1998_OVERVIEW = {
  flightCount: "34,577",
  totalDistance: "37,684,885",
  name: "American Airlines",
  nickname: "American",
};
const AA_AFTER_1998_FIRST_DESTINATION = {
  code: "DFW",
  flightCount: "8,745",
  totalDistance: "8,389,413",
};

async function expectOverview(
  page: Page,
  overview: typeof WN_OVERVIEW,
  firstDestination: typeof WN_FIRST_DESTINATION,
): Promise<void> {
  // The MalloyRender output lives in a custom div tree (no <table>),
  // so we assert against `body.innerText`. The numbers are formatted
  // with thousands separators and are specific enough that false
  // positives across cells are vanishingly unlikely.
  await expect
    .poll(async () => await page.evaluate(() => document.body.innerText), {
      timeout: 30_000,
      intervals: [500, 1000, 2000],
    })
    .toContain(overview.flightCount);

  const body = await page.evaluate(() => document.body.innerText);
  expect(body).toContain(overview.totalDistance);
  expect(body).toContain(overview.name);
  expect(body).toContain(overview.nickname);
  expect(body).toContain(firstDestination.code);
  expect(body).toContain(firstDestination.flightCount);
  expect(body).toContain(firstDestination.totalDistance);
}

async function setCarrier(page: Page, value: string): Promise<void> {
  const carrierInput = page.getByRole("textbox", { name: "carrier" });
  await carrierInput.fill(value);
  // The SDK commits the value on a real focus-out event; programmatic
  // `.blur()` doesn't trigger it. Pressing Tab moves focus off the
  // field the way a user would.
  await carrierInput.press("Tab");
}

async function clearCarrier(page: Page): Promise<void> {
  // GivenInput renders an MUI text adornment with `aria-label="clear
  // value"` only when the field has an override. The carrier input
  // is the only `clear value` adornment in the panel (the after
  // DatePicker uses its own native clear), so we can target it by
  // role+name without disambiguation.
  const clearBtn = page.getByRole("button", { name: "clear value" });
  await clearBtn.click();
}

async function setAfter(page: Page, value: string): Promise<void> {
  const afterInput = page.getByRole("textbox", { name: "after" });
  await afterInput.fill(value);
  // DatePicker commits on focus-out, same as the text input.
  await afterInput.press("Tab");
}

async function clearAfter(page: Page): Promise<void> {
  // MUI's DatePicker uses its own clear icon (we wired showClear on
  // the slotProps in GivenInput). Three-dot delete it by clearing
  // the input directly — fill('') + Tab triggers the same commit
  // path as clicking the X.
  const afterInput = page.getByRole("textbox", { name: "after" });
  await afterInput.fill("");
  await afterInput.press("Tab");
}

test.describe("Notebook givens (faa-givens-demo)", () => {
  test("navigates via UI and exercises carrier overrides end-to-end", async ({
    page,
  }) => {
    // ── 1. Click through Home → environment → package → notebook ──
    await page.goto(BASE_URL, { timeout: 60_000 });
    await page.waitForLoadState("domcontentloaded");

    // Open the malloy-samples environment.
    await page
      .getByRole("heading", { name: "malloy-samples", exact: true })
      .first()
      .click();
    await expect(page).toHaveURL(/malloy-samples/, { timeout: 30_000 });

    // Open the faa-givens-demo package.
    await page.getByText("faa-givens-demo", { exact: true }).first().click();
    await expect(page).toHaveURL(/faa-givens-demo/, { timeout: 30_000 });

    // Open the notebook.
    await page
      .getByText("carriers_with_parameters.malloynb", { exact: true })
      .first()
      .click();
    await expect(page).toHaveURL(/carriers_with_parameters\.malloynb/, {
      timeout: 30_000,
    });

    // ── 2. Parameters panel renders with both declared givens ──
    await expect(
      page.getByRole("heading", { name: /^parameters$/i }).first(),
    ).toBeVisible({ timeout: 30_000 });
    await expect(page.getByRole("textbox", { name: "carrier" })).toBeVisible();
    await expect(page.getByRole("textbox", { name: "after" })).toBeVisible();

    // ── 3. Default (no override): WN's numbers should appear ──
    await expectOverview(page, WN_OVERVIEW, WN_FIRST_DESTINATION);

    // ── 4. Explicit carrier="WN" should match the default exactly ──
    await setCarrier(page, "WN");
    await expectOverview(page, WN_OVERVIEW, WN_FIRST_DESTINATION);

    // ── 5. Clear → defaults restored (still WN's numbers) ──
    await clearCarrier(page);
    await expectOverview(page, WN_OVERVIEW, WN_FIRST_DESTINATION);

    // ── 6. carrier="AA" → American Airlines numbers ──
    await setCarrier(page, "AA");
    await expectOverview(page, AA_OVERVIEW, AA_FIRST_DESTINATION);

    // ── 7. Clear → defaults restored again ──
    await clearCarrier(page);
    await expectOverview(page, WN_OVERVIEW, WN_FIRST_DESTINATION);

    // ── 8. after="05/15/1998" alone → widens window, WN-with-1998 numbers ──
    // Sanity-checks the timestamp-given path (DatePicker code path,
    // different from the string-textbox path the carrier uses).
    await setAfter(page, "05/15/1998");
    await expectOverview(
      page,
      WN_AFTER_1998_OVERVIEW,
      WN_AFTER_1998_FIRST_DESTINATION,
    );

    // ── 9. Multi-given: carrier="AA" + after still set → AA-with-1998 ──
    // Both givens applied together. Confirms the SDK forwards every
    // declared given on each request, not just the most recently
    // edited one.
    await setCarrier(page, "AA");
    await expectOverview(
      page,
      AA_AFTER_1998_OVERVIEW,
      AA_AFTER_1998_FIRST_DESTINATION,
    );

    // ── 10. Clear after → carrier=AA + default after → original AA numbers ──
    await clearAfter(page);
    await expectOverview(page, AA_OVERVIEW, AA_FIRST_DESTINATION);

    // ── 11. Clear carrier → both back to defaults ──
    await clearCarrier(page);
    await expectOverview(page, WN_OVERVIEW, WN_FIRST_DESTINATION);
  });
});

/**
 * API-level deprecation-header contract (PR #775).
 *
 * These hit the publisher directly via Playwright's request fixture
 * — no browser. The unit tests in
 * packages/server/src/filter_deprecation.spec.ts cover the helper
 * in isolation; this proves the server actually wires the helper
 * into the live POST /…/query route with the right inputs.
 *
 * Uses faa-givens-demo because it's the only package on the live
 * test environment that compiles successfully under both surfaces.
 * The model doesn't declare any `#(filter)` annotations — but that
 * doesn't matter for the header contract, which keys off the
 * *request*, not the model.
 */
const QUERY_URL =
  "http://localhost:4000/api/v0/environments/malloy-samples/packages/faa-givens-demo/models/carriers_with_parameters.malloy/query";
const DEPRECATION_LINK_RE =
  /docs\/givens\.md.*rel="deprecation".*type="text\/markdown"/;

test.describe("Deprecation headers on POST /query", () => {
  test("fires Deprecation+Link when filterParams is present", async ({
    request,
  }) => {
    const res = await request.post(QUERY_URL, {
      data: {
        query: "run: spotlight_flights -> carrier_overview",
        filterParams: { unused: "value" },
      },
    });
    expect(res.status()).toBe(200);
    expect(res.headers()["deprecation"]).toBe("true");
    expect(res.headers()["link"]).toMatch(DEPRECATION_LINK_RE);
  });

  test("fires Deprecation+Link when bypassFilters is true", async ({
    request,
  }) => {
    const res = await request.post(QUERY_URL, {
      data: {
        query: "run: spotlight_flights -> carrier_overview",
        bypassFilters: true,
      },
    });
    expect(res.status()).toBe(200);
    expect(res.headers()["deprecation"]).toBe("true");
    expect(res.headers()["link"]).toMatch(DEPRECATION_LINK_RE);
  });

  test("does NOT fire on a givens-only call", async ({ request }) => {
    const res = await request.post(QUERY_URL, {
      data: {
        query: "run: spotlight_flights -> carrier_overview",
        givens: { carrier: "AA" },
      },
    });
    expect(res.status()).toBe(200);
    expect(res.headers()["deprecation"]).toBeUndefined();
    expect(res.headers()["link"]).toBeUndefined();
  });

  test("does NOT fire on a vanilla call with no legacy fields", async ({
    request,
  }) => {
    const res = await request.post(QUERY_URL, {
      data: { query: "run: spotlight_flights -> carrier_overview" },
    });
    expect(res.status()).toBe(200);
    expect(res.headers()["deprecation"]).toBeUndefined();
    expect(res.headers()["link"]).toBeUndefined();
  });
});
