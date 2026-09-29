<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->
# Headless Verification Harness

> Scaffolding to verify a finished data app end-to-end without a live warehouse. Referenced from `SKILL.md` ("Verify before you call it done"). You are building for someone who cannot tell a correct dashboard from a broken one; this harness is how you check, so they don't have to.

**If you have a live Publisher serving real data, verify against that first.** It is strictly the stronger check: it exercises the real queries, the real column names and the real edge cases (empty results, nulls, a season with no data) that canned fixtures are exactly the thing to miss. Mock only what the live server cannot give you.

The mock below is for when there is no live server, or when you need a specific shape on demand - an empty result, a failing query, a tile whose data does not exist yet. The app talks to the world through exactly one seam: `window.Publisher.query(modelPath, malloy)`. Mock that seam and you can load the real page in a real browser with canned data, then assert on what actually rendered.

## 1. Mock `sdk/publisher.js` (when there is no live server)

Serve a stand-in at the same root-relative path the page loads (`/sdk/publisher.js`). It returns canned rows keyed by `(modelPath, query)`, and (this is the part that bites) it reproduces the **async delay** of the real runtime, so your assertions exercise the loading→loaded transition instead of racing a synchronous stub.

```js
// mock/sdk/publisher.js: served at /sdk/publisher.js during verification
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
    // skill:malloy-html-data-app-runtime), NOT { data: rows }. If any tile renders
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

## 2. Drive it in a REAL browser and assert on the rendered DOM

**It must be a real browser engine. jsdom cannot verify this app, and it fails in the most dangerous possible way.** jsdom does not execute `<script type="module">` at all, and it reports no error for it - so a run comes back with no console errors, no error tiles and no stuck skeletons, which looks like a clean pass while *none of your code ran*. That is exactly the false green this document exists to prevent, and it is worse because `skill:malloy-html-data-app-runtime` requires the ES-module entry point that breaks it. jsdom also lacks `fetch` (the query promise rejects and boot hangs), `ResizeObserver` (chart libraries throw, and it surfaces as your own error state), and a 2D canvas.

Playwright is the example below. If it is not installed and cannot be fetched - which is common in agent sandboxes - use the CDP harness at the end of this file instead. It needs nothing but a browser you already have and a WebSocket.


> **If your page lazy-loads (and `SKILL.md` says it should past about eight tiles), a whole-page "no skeletons anywhere" predicate will time out and report a failure that is not real.** Below-fold tiles are *supposed* to still be skeletons. The wait below is therefore scoped to tiles currently in the viewport, which is correct for both a lazy and a non-lazy page. The trade runs the dangerous way, so be explicit about it: **this predicate cannot fail on a tile it cannot see**, so a below-fold tile that never resolves passes silently. On a lazy page the viewport wait is necessary but **not sufficient** - the scroll-and-assert loop in `reference/lazy-load.md` is what actually covers the deferred tiles, and a lazy page is not verified without it.

```js
// verify.mjs: node verify.mjs   (assumes the server above is on :4173)
import { chromium } from "playwright";

const browser = await chromium.launch();
const page = await browser.newPage();
const errors = [];
page.on("pageerror", (e) => errors.push(e.message));
page.on("console", (m) => m.type() === "error" && errors.push(m.text()));

await page.goto("http://localhost:4173/index.html", { waitUntil: "load" });
// publisher.js holds an SSE stream open (even with watch off), so networkidle NEVER fires.
// Wait on CONTENT, and for ALL tiles to RESOLVE, not just the first to appear.
// Asserting after only the first .value renders races the others and yields a
// false "stuck skeleton" / "empty value" FAIL. "Resolved" = every tile has a
// value and no skeleton remains. This single wait subsumes the DELAY_MS delay
// AND the stuck-skeleton check; if it times out, a tile really is stuck.
// SCOPED TO THE VIEWPORT on purpose - see the warning above this block.
await page.waitForFunction(() => {
  // Only tiles that are actually on screen are expected to have resolved. On a
  // non-lazy page that is every tile; on a lazy page it is the ones above the
  // fold, and the rest resolve as you scroll. Filtering here means the SAME
  // predicate works for both, instead of timing out on the lazy page that
  // SKILL.md tells you to build.
  const onScreen = (el) => {
    const r = el.getBoundingClientRect();
    return r.bottom > 0 && r.top < innerHeight;
  };
  const tiles = [...document.querySelectorAll(".tile")].filter(onScreen);
  // Assert PER TILE, not global counts. `.tile .value` count >= tile count
  // false-FAILs a chart/table tile (it has no .value) and false-PASSes when one
  // multi-.value KPI tile inflates the total enough to mask a silently-empty
  // sibling. "Resolved" = each tile shows its OWN content: a value, a chart, a
  // table row, or its error state.
  return tiles.length > 0 &&
    tiles.every((t) => !t.querySelector(".kit-skeleton")) &&
    tiles.every((t) => t.querySelector(".value, canvas, table tbody tr, .is-error"));
}, null, { timeout: 5000 });

// Per tile: flag any tile that is content-empty (nothing rendered at all) or
// whose KPI values are blank/"NaN". A global .value sweep would let a valueless
// tile vanish; walking tiles keeps every one accountable.
const tileReport = await page.$$eval(".tile", (all) =>
  // Scoped to the viewport, exactly like the wait above. A whole-page sweep
  // reports every below-fold tile as empty on a lazy page, which just moves
  // the false failure from the wait into the assertion.
  all
    .filter((t) => { const r = t.getBoundingClientRect(); return r.bottom > 0 && r.top < innerHeight; })
    .map((t, i) => {
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
console.log(`OK: all ${tileReport.length} on-screen tiles rendered content`);
```

## 3. When Playwright is not available: drive Chrome over CDP

Agent sandboxes frequently have a browser but no Playwright and no way to install one. The DevTools Protocol needs only a WebSocket, and **Node 22+ has one built in**, so this path installs nothing. The snippet below was run end to end against a real app before it was written down.

```sh
# Launch once. Pick a port nobody else is on, and your own profile directory so
# you never collide with a browser the user is already running.
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --headless=new --remote-debugging-port=9333 --no-first-run \
  --user-data-dir=/tmp/verify-profile about:blank &
```

```js
// verify.mjs - node verify.mjs <app-url> [port]. No dependencies on Node 22+.
const APP_URL = process.argv[2] ?? "http://localhost:4173/index.html";
const PORT = process.argv[3] ?? 9333;

const targets = await (await fetch(`http://localhost:${PORT}/json/list`)).json();
const page = targets.find((t) => t.type === "page");
const sock = new WebSocket(page.webSocketDebuggerUrl);

let id = 0;
const pending = new Map();
const errors = [];
const send = (method, params = {}) =>
  new Promise((res, rej) => { const i = ++id; pending.set(i, { res, rej });
    sock.send(JSON.stringify({ id: i, method, params })); });

sock.addEventListener("message", (ev) => {
  const msg = JSON.parse(ev.data);
  if (msg.id && pending.has(msg.id)) {
    const { res, rej } = pending.get(msg.id);
    pending.delete(msg.id);
    // REJECT on a protocol error. Resolving with undefined turns a mistyped
    // method into a predicate that is merely false, and then waitFor burns its
    // whole timeout and blames the page.
    if (msg.error) rej(new Error(`CDP: ${msg.error.message}`));
    else res(msg.result);
    return;
  }
  if (msg.method === "Runtime.exceptionThrown") errors.push(msg.params?.exceptionDetails?.text ?? "exception");
  if (msg.method === "Runtime.consoleAPICalled" && msg.params.type === "error") errors.push("console.error");
});

await new Promise((r) => sock.addEventListener("open", r, { once: true }));
await send("Page.enable");
await send("Runtime.enable");
await send("Emulation.setDeviceMetricsOverride",
  { width: 1512, height: 982, deviceScaleFactor: 2, mobile: false });
await send("Page.navigate", { url: APP_URL });

const evaluate = async (expr) =>
  (await send("Runtime.evaluate", { expression: expr, returnByValue: true, awaitPromise: true }))
    .result?.value;

// There is no waitForFunction here; poll your own predicate.
async function waitFor(expr, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await evaluate(expr)) return true;
    await new Promise((r) => setTimeout(r, 250));
  }
  throw new Error(`timed out waiting for: ${expr}`);
}

const ONSCREEN = `(el) => { const r = el.getBoundingClientRect(); return r.bottom > 0 && r.top < innerHeight; }`;

await waitFor(`document.querySelectorAll('.tile').length > 0
  && [...document.querySelectorAll('.tile')].filter(${ONSCREEN})
       .every((t) => t.querySelector('.value, canvas, table tbody tr, .is-error'))`);

console.log(await evaluate(`(() => {
  const onScreen = ${ONSCREEN};
  const tiles = [...document.querySelectorAll('.tile')].filter(onScreen);
  // A canvas reports width 300 untouched, so "has a width" proves nothing.
  // Read the pixels: a blank chart is the failure this is looking for.
  const canvases = [...document.querySelectorAll('canvas')].filter(onScreen);
  const painted = canvases.filter((c) => {
    try { return c.getContext('2d').getImageData(0, 0, c.width, c.height).data.some((v) => v !== 0); }
    catch { return false; }
  }).length;
  return JSON.stringify({
    onScreenTiles: tiles.length,
    errorTiles: tiles.filter((t) => t.querySelector('.is-error')).length,
    stuck: tiles.filter((t) => t.querySelector('.kit-skeleton')).length,
    // Word boundaries on every alternation, or legitimate copy containing
    // "undefined" trips the check.
    badValues: /\\bNaN\\b|\\bundefined\\b|\\bInfinity\\b/.test(document.body.innerText),
    onScreenCanvases: canvases.length,
    canvasesPainted: painted,
  });
})()`));
console.log("runtime errors:", errors.length ? errors : "none");

sock.close();
```

Everything here is scoped to the viewport, for the reason in section 2: a whole-page sweep hard-fails a lazy page. Scroll and re-run the report to cover the deferred tiles.

Three things bite on this path, and each makes a correct page look broken:

- **`captureBeyondViewport: true` rasterises canvases at the wrong scale.** A full-page capture squashes every chart into a corner and you will "fix" a bug that does not exist. Use viewport-only `Page.captureScreenshot` and scroll between shots.
- **Switch themes by clicking the app's own control**, not by setting an attribute on the root. Setting the attribute skips whatever re-resolves chart colours, so the charts keep their construction-time palette and you get a screenshot that looks like a real theming bug.
- **Kill the browser when you finish** and use a port nobody else is on. A stray headless Chrome holding a profile directory is a confusing thing to debug later.

## Gotchas (each cost a real debugging cycle)

- **Wait out the mock's async delay before asserting.** Asserting immediately after `load` reads the skeleton, not the resolved tile, and reports a false "stuck skeleton." Wait until every tile shows its own resolved content (the per-tile `waitForFunction` above), never on `networkidle`, because `publisher.js` keeps the live-reload SSE stream open, so the page never reaches network idle.
- **A missing fixture is a test bug, not empty data.** Reject on an unknown `(model, query)` key so a drifted query string fails loudly, instead of returning `[]` and masquerading as a real empty state.
- **Assert on the rendered DOM, not on `Publisher.query` return values.** The bugs live in the render path (NaN formatting, wrong column read, `|| 0` faking a zero). Reading the query result back proves nothing the model didn't already prove.
- Match the selectors (`.tile .value`, `.is-error`, `.kit-skeleton`, `canvas`) to whatever your app actually emits.
