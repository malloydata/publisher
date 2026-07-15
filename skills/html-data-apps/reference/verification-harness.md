# Headless Verification Harness

> Scaffolding to verify a finished data app end-to-end without a live warehouse. Referenced from `SKILL.md` ("Verify before you call it done"). You are building for someone who cannot tell a correct dashboard from a broken one — this harness is how you check, so they don't have to.

The app talks to the world through exactly one seam: `window.Publisher.query(modelPath, malloy)`. Mock that seam and you can load the real page in a real browser with canned data, then assert on what actually rendered.

## 1. Mock `sdk/publisher.js`

Serve a stand-in at the same root-relative path the page loads (`/sdk/publisher.js`). It returns canned rows keyed by `(modelPath, query)`, and — this is the part that bites — it reproduces the **async delay** of the real runtime, so your assertions exercise the loading→loaded transition instead of racing a synchronous stub.

```js
// mock/sdk/publisher.js — served at /sdk/publisher.js during verification
(function () {
  // Key canned data by "modelPath::query" (exact strings from tiles.js).
  const FIXTURES = {
    "carriers.malloy::run: carriers -> kpis": [{ total: 1234, active: 1180 }],
    "carriers.malloy::run: carriers -> by_letter": [
      { letter: "A", n: 12 }, { letter: "B", n: 7 },
    ],
    // ...one entry per (model, query) your tiles.js declares
  };
  const DELAY_MS = 40; // > 0 on purpose: mimic the real async round-trip

  function resolve(modelPath, malloy) {
    const rows = FIXTURES[`${modelPath}::${malloy}`];
    // Unknown key = test bug (query string drifted). Fail loudly, don't return [].
    if (!rows) return Promise.reject(new Error(`No fixture for ${modelPath}::${malloy}`));
    return new Promise((r) => setTimeout(() => r(rows.map((x) => ({ ...x }))), DELAY_MS));
  }
  window.Publisher = {
    query: resolve,
    // Placeholder shape ONLY. The real queryFull returns a Malloy result
    // *envelope* handed to `<malloy-render>` el.result (see
    // skill:html-data-app-runtime), NOT { data: rows }. If any tile renders
    // via <malloy-render>, make this fixture a real envelope or that tile breaks.
    queryFull: (m, q) => resolve(m, q).then((rows) => ({ data: rows })),
    setToken() {},
  };
})();
```

Point the harness at the mock by serving it *over* the real path. Copy `public/` and the mock into a webroot so `/sdk/publisher.js` resolves to the mock:

```sh
webroot=$(mktemp -d)
cp -r public/* "$webroot"/
mkdir -p "$webroot/sdk" && cp mock/sdk/publisher.js "$webroot/sdk/publisher.js"
python3 -m http.server 4173 --directory "$webroot" &
server=$!
trap 'kill "$server" 2>/dev/null; rm -rf "$webroot"' EXIT   # always tear the server down
```

## 2. Drive it with Playwright and assert on the rendered DOM

```js
// verify.mjs — node verify.mjs   (assumes the server above is on :4173)
import { chromium } from "playwright";

const browser = await chromium.launch();
const page = await browser.newPage();
const errors = [];
page.on("pageerror", (e) => errors.push(e.message));
page.on("console", (m) => m.type() === "error" && errors.push(m.text()));

await page.goto("http://localhost:4173/index.html", { waitUntil: "load" });
// publisher.js holds an SSE stream open under --watch, so networkidle NEVER fires.
// Wait on CONTENT — and for ALL tiles to RESOLVE, not just the first to appear.
// Asserting after only the first .value renders races the others and yields a
// false "stuck skeleton" / "empty value" FAIL. "Resolved" = every tile has a
// value and no skeleton remains. This single wait subsumes the DELAY_MS delay
// AND the stuck-skeleton check; if it times out, a tile really is stuck.
// NOTE: assumes a NON-lazy page — every tile loads on open. For a lazy-loaded
// page (reference/lazy-load.md) below-fold tiles legitimately keep their
// skeletons until scrolled in, so this wait would (correctly) time out. Test a
// lazy page with the scroll-and-assert loop in lazy-load.md instead.
await page.waitForFunction(() => {
  const tiles = [...document.querySelectorAll(".tile")];
  // Assert PER TILE, not global counts. `.tile .value` count >= tile count
  // false-FAILs a chart/table tile (it has no .value) and false-PASSes when one
  // multi-.value KPI tile inflates the total enough to mask a silently-empty
  // sibling. "Resolved" = each tile shows its OWN content: a value, a chart, a
  // table row, or its error state.
  return tiles.length > 0 &&
    document.querySelectorAll(".kit-skeleton").length === 0 &&
    tiles.every((t) => t.querySelector(".value, canvas, table tbody tr, .is-error"));
}, null, { timeout: 5000 });

// Per tile: flag any tile that is content-empty (nothing rendered at all) or
// whose KPI values are blank/"NaN". A global .value sweep would let a valueless
// tile vanish; walking tiles keeps every one accountable.
const tileReport = await page.$$eval(".tile", (tiles) =>
  tiles.map((t, i) => {
    const vals = [...t.querySelectorAll(".value")].map((e) => e.textContent.trim());
    return {
      i,
      empty: !t.querySelector(".value, canvas, table tbody tr, .is-error"),
      errored: !!t.querySelector(".is-error"),
      badVals: vals.filter((v) => !v || /^(loading|nan|undefined|null)$/i.test(v)),
    };
  }));

const problems = [];
const emptyTiles = tileReport.filter((t) => t.empty);
const badValueTiles = tileReport.filter((t) => t.badVals.length);
const errorTiles = tileReport.filter((t) => t.errored);
if (emptyTiles.length) problems.push(`empty tiles (no content): ${emptyTiles.map((t) => t.i)}`);
if (badValueTiles.length) problems.push(`blank/NaN values: ${JSON.stringify(badValueTiles)}`);
if (errorTiles.length) problems.push(`${errorTiles.length} tile(s) in error state: ${errorTiles.map((t) => t.i)}`);
if (errors.length) problems.push(`console/page errors: ${errors.join(" | ")}`);

await browser.close();
if (problems.length) { console.error("FAIL:\n- " + problems.join("\n- ")); process.exit(1); }
console.log(`OK — all ${tileReport.length} tiles rendered content`);
```

## Gotchas (each cost a real debugging cycle)

- **Wait out the mock's async delay before asserting.** Asserting immediately after `load` reads the skeleton, not the resolved tile, and reports a false "stuck skeleton." Wait until every tile shows its own resolved content (the per-tile `waitForFunction` above), never on `networkidle` — `publisher.js` keeps the live-reload SSE stream open, so the page never reaches network idle.
- **A missing fixture is a test bug, not empty data.** Reject on an unknown `(model, query)` key so a drifted query string fails loudly, instead of returning `[]` and masquerading as a real empty state.
- **Assert on the rendered DOM, not on `Publisher.query` return values.** The bugs live in the render path (NaN formatting, wrong column read, `|| 0` faking a zero). Reading the query result back proves nothing the model didn't already prove.
- Match the selectors (`.tile .value`, `.is-error`, `.kit-skeleton`, `canvas`) to whatever your app actually emits.
