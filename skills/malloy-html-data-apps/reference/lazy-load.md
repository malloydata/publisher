# Lazy-Loading Tiles Below the Fold

> Defer off-screen tile queries until they scroll into view, so a dashboard with many tiles doesn't fire every query on load. Referenced from `SKILL.md`. Add this once a page has enough tiles that loading them all at once is wasteful or slow.

Three parts, all required — drop any one and you get a subtle bug rather than an obvious one:

1. **`IntersectionObserver`** with a `rootMargin` (~240px) so a tile starts loading just *before* it scrolls into view, not the instant it appears.
2. **A small concurrency cap** so a fast scroll to the bottom doesn't fire twenty queries at once.
3. **Reserve each tile's height** while its skeleton shows, so lazy-loaded tiles don't reflow the page and retrigger the observer.

```js
// lazy.js — observe tiles, run each tile's query once, capped concurrency.
const MARGIN = "240px";  // start loading before the tile is visible
const MAX_INFLIGHT = 3;  // cap concurrent queries

let inflight = 0;
const queue = [];

function pump() {
  while (inflight < MAX_INFLIGHT && queue.length) {
    const run = queue.shift();
    inflight++;
    run().finally(() => { inflight--; pump(); });
  }
}

// loadTile(el) runs the tile's query (from tiles.js) and renders it; returns a Promise.
export function lazyLoad(tileEls, loadTile) {
  const io = new IntersectionObserver((entries, obs) => {
    for (const e of entries) {
      if (!e.isIntersecting) continue;
      const el = e.target;
      obs.unobserve(el);                 // load once; never re-fire
      queue.push(() => loadTile(el));
      pump();
    }
  }, { rootMargin: MARGIN });

  for (const el of tileEls) {
    reserveHeight(el);                   // see below — prevents reflow loops
    io.observe(el);
  }
}

// Reserve space before the query resolves. Without this, an empty tile has ~0
// height, so many tiles intersect at once (false "all visible"), and when each
// resolves the page reflows and the observer re-fires. Give the skeleton a real
// min-height matched to the rendered tile.
function reserveHeight(el) {
  if (!el.style.minHeight) el.style.minHeight = "220px"; // match your tile height
}
```

## Verification gotcha — test on a *small* viewport

On a short page (or a tall default headless viewport like 1280×720+), every tile intersects the `rootMargin` box at once, so *nothing* actually defers — and your test reports a false "everything lazy-loaded correctly" pass while the feature does nothing. **Verify with a deliberately small viewport** so tiles genuinely start off-screen:

```js
// In the Playwright harness (see reference/verification-harness.md):
await page.setViewportSize({ width: 480, height: 520 }); // force real off-screen tiles

const total = (await page.$$(".tile")).length;
const loaded = () => page.$$eval(".tile .value", (e) => e.length);

// Let the top tiles settle, THEN measure. Don't sample right after the first
// value: with MAX_INFLIGHT capped, an eager (broken) page and a correct page
// both show only ~cap tiles at that instant — the guard wouldn't fire. Wait for
// the loaded count to go quiet, then require that NOT ALL tiles loaded.
await page.waitForFunction(() => document.querySelector(".tile .value"));
let prev = -1, settled = await loaded();
while (settled !== prev) { prev = settled; await page.waitForTimeout(150); settled = await loaded(); }
if (settled >= total) throw new Error("nothing deferred — viewport too tall to test lazy-load");

// Scroll INCREMENTALLY, one viewport at a time. A single jump to the bottom
// skips past mid-page tiles — IntersectionObserver never fires for them, their
// queries never run, and the final wait times out even though lazy-load works.
const vh = 520;
const pageH = await page.evaluate(() => document.body.scrollHeight);
for (let y = 0; y <= pageH; y += vh) {
  await page.evaluate((yy) => window.scrollTo(0, yy), y);
  await page.waitForTimeout(150); // let observers fire + queries resolve
}
await page.waitForFunction((n) => document.querySelectorAll(".tile .value").length >= n, total);
```
