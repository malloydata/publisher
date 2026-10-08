<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Performance and layout stability

> What a data app downloads, what it asks the server for, and whether the page holds still while it loads. Referenced from `SKILL.md`. Every rule here came from a real build that passed its functional checks while being slow, heavy, or jumpy: a working app is not yet a polished one.

## Measure first

Record these per page, before and after any change, with the harness in `reference/verification-harness.md`. A change you did not measure is a guess.

| Measure | How | Bar |
|---|---|---|
| Bytes downloaded, by type | sum of response sizes on a cold load | the chart library is usually most of it; see below |
| Queries issued | count of `/query` requests until the page settles | one per tile, no more |
| Identical queries | same model + same query text twice on one page | 0 |
| Layout shift (CLS) | a `layout-shift` `PerformanceObserver`, first visit and repeat visit | under 0.1 first visit, 0 on repeat |
| Time to settled | until no query has been in flight for ~1.5s | depends on the warehouse; measure, do not promise |

Measure embedded as well as standalone (`reference/embedding.md`). Inside an auto-sized frame the whole document is "the viewport", so every shift anywhere on the page counts, and numbers that looked fine standalone (0.01 to 0.07) measured 0.12 to 0.41 embedded on the same build.

## Keep the chart library off the critical path

A vendored chart library is typically 80 to 90% of a data app's bytes, and as a plain `<script>` ahead of the app code it blocks every query until it has downloaded and parsed. It also lets the browser paint the page before the app code can size it, which is where much of the first-visit layout shift comes from.

- **Preload it, load it on the first chart.** Put `<link rel="preload" as="script" href="./vendor/chart.js">` in the `<head>` of pages that draw charts, and have the chart layer inject the script the first time it mounts a chart. The download starts at once, nothing waits for it, and a tile whose rows land before the library does waits only for itself.
- **A page that draws no chart never loads it.** An overview or landing page of numbers and links should not pay for a chart library at all. Measured: 1,464 KB to 348 KB for one such page.
- **Hand back a stand-in while it loads.** Callers usually wire a click handler on the chart instance right after mounting it. Return an object that records `.on(...)` calls and replays them on the real instance once it exists, so the calling code does not change shape:

```js
// charts.js
let libP = null;
function chartLib() {
  if (window.echarts) return Promise.resolve(window.echarts);
  libP ??= new Promise((res, rej) => {
    const s = document.createElement("script");
    s.src = new URL("../vendor/echarts.min.js", import.meta.url).href;
    s.onload = () => res(window.echarts);
    s.onerror = () => { libP = null; rej(new Error("Chart library failed to load")); };
    document.head.append(s);
  });
  return libP;
}

export function mount(host, buildOption) {
  if (!window.echarts) {
    const calls = [], stub = { on: (...a) => (calls.push(a), stub) };
    const mine = (host.__mount = (host.__mount || 0) + 1);   // a newer mount wins
    chartLib().then(() => {
      if (host.__mount !== mine || !host.isConnected) return;
      const inst = mount(host, buildOption);
      calls.forEach((a) => inst.on(...a));
    }).catch((e) => showError(host, e));
    return stub;
  }
  host.__mount = (host.__mount || 0) + 1;
  const inst = echarts.init(host);
  inst.setOption(buildOption());
  return inst;
}
```

### Trim the library to what the app draws

Most chart libraries publish a modular build. A bundle of only the series types and components the app actually uses is typically a third smaller (one build: 1,122 KB to 713 KB, 369 KB to 243 KB gzipped). Inventory what the code passes the library (every series `type:`, and components such as legend, tooltip, visualMap, markLine), bundle exactly those once with a bundler outside the package, and commit the bundle together with its entry file and the command that rebuilt it.

**A trimmed build fails silently.** A series type left out of the bundle still mounts its canvas, throws nothing and logs nothing, and draws an empty box. A canvas count proves nothing. Verify by reading pixels: open every page and every kind of drawer, and require each chart canvas to have drawn pixels, against the full build first and then the trimmed one. Write the list of included types next to the bundle, so the next person to add a chart type knows to rebuild.

## Ask once

- **One query text per resource.** The page's cache, and any prefetch, is keyed on the exact query string. Two pages that read the same tile with different text (one adds `order_by`, one does not) get two cache entries and two round trips for one answer. Where order does not matter to a caller, send the text the other caller sends.
- **A prefetch must match what the destination sends.** Warming the next page's queries on nav hover is worth it only if the warmed text is byte-for-byte what that page will ask for. Check it: list what each page sends and what each prefetch warms, and diff them.
- **Count identical queries on a page.** It should be 0. Two tiles or two drawer panels needing the same rows should share one in-flight request.

## Hold still while loading

Layout shift is the most visible difference between a generated app and a polished one, and almost all of it has three causes.

**1. Script-filled blocks at the top of the page.** A KPI strip, a provenance line or a filter bar that is an empty `<div>` in the HTML and is filled by script appears *after* the first paint, and pushes everything below it down. Measured: about 180px of drop on every page of one app, from three such blocks. Give each its final size in the first paint:

- put placeholder markup in the HTML, built from the same classes the script renders, so the layout reserves the right height at every width (four `.kpi` placeholder cards in the KPI grid, replaced when the numbers arrive);
- give a one-line block a `min-height` while it is `:empty`.

**2. A tile that changes height when its rows land.** A four-line skeleton replaced by a 25-row list shifts everything below it by hundreds of pixels.

- **Size the skeleton from what is coming.** If the query carries a `limit:` or the tile slices to N rows, skeleton N rows at the real row height.
- **Remember the settled height.** Store each tile's rendered height per page, tile and width bucket in `localStorage`, and hold it as `min-height` on the next load. Data that refreshes daily keeps its height, so a returning reader sees the page at its final size from the first frame. Measured: 0 layout shift on repeat visits on every page.

```js
const KEY = "app-size:" + location.pathname;
const sizes = (() => { try { return JSON.parse(localStorage.getItem(KEY) || "{}"); } catch { return {}; } })();
const sizeKey = (host) => host.id && `${host.id}@${Math.round(host.clientWidth / 40)}`;
function holdSize(host) { const k = sizeKey(host); if (k && sizes[k]) host.style.minHeight = sizes[k] + "px"; return k; }
function settleSize(host, k) {                       // call right after render, same task
  host.style.minHeight = "";
  if (!k) return;
  const h = Math.round(host.getBoundingClientRect().height);
  if (h > 0 && sizes[k] !== h) { sizes[k] = h; try { localStorage.setItem(KEY, JSON.stringify(sizes)); } catch {} }
}
```

**3. A tile that re-runs for a new filter.** Dropping back to a skeleton on every scope change makes the page collapse and regrow. Keep the last answer on screen, dimmed and `aria-busy`, at its current height, until the new one lands. And guard the race: number each run per tile, and drop a response that is not the latest, or a slow query for the previous filter paints over the new one.

```js
function load(host, query, render) {
  const seq = (host.__seq = (host.__seq || 0) + 1);
  if (host.__drawn) { host.style.minHeight = host.offsetHeight + "px"; host.classList.add("is-busy"); host.setAttribute("aria-busy", "true"); }
  else showSkeleton(host);
  return Publisher.query(MODEL, query).then((rows) => {
    if (seq !== host.__seq) return;                  // a newer run owns this tile
    host.classList.remove("is-busy"); host.removeAttribute("aria-busy");
    host.__drawn = true;
    render(rows, host);
    host.style.minHeight = "";
  });
}
```

## Respond before the answer arrives

A click that waits on a query before anything visible happens reads as a dead click. The common case is a drill that must first resolve a name to an id: open the drawer immediately, titled with the name and a skeleton, run the lookup inside it, and replace the skeleton in place when the id arrives. Drop the late answer if the reader has closed the drawer or opened another subject meanwhile (`reference/depth-patterns.md`, stale-response guard).

## What the page cannot fix

Compression and cache headers belong to whatever serves `public/`. Check them (`curl -sI -H 'Accept-Encoding: gzip, br' <url>`): a chart library served uncompressed with `max-age=0` is downloaded or revalidated in full on every page. Report it to whoever runs the server rather than working around it in the page.
