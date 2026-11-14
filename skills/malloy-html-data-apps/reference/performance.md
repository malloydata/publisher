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

- **Preload it, load it on the first chart.** Put `<link rel="preload" as="script" href="./vendor/echarts.min.js">` in the `<head>` of pages that draw charts, and have the chart layer inject the script the first time it mounts a chart. The download starts at once, nothing waits for it, and a tile whose rows land before the library does waits only for itself. The preload's `href` and the injected script's `src` must resolve to the same URL, or the preload is a second download that nothing uses.
- **A page that draws no chart never loads it.** An overview or landing page of numbers and links should not pay for a chart library at all. Measured: 1,464 KB to 348 KB for one such page.
- **Hand back a stand-in while it loads.** Callers usually wire a click handler on the chart instance right after mounting it. Return an object that records `.on(...)` calls and replays them on the real instance once it exists, so the calling code does not change shape.

The loader below is written for ECharts (a global `echarts`, click handlers wired with `.on()`). For another library change the file name, the global, and the stand-in to record whatever its callers call; Chart.js, for one, takes its click handler as an option rather than through `.on()`. The URL is resolved against `charts.js` itself, so it is right from a page in any directory as long as `charts.js` sits beside `vendor/` (the layout in `reference/runtime.md`).

```js
// charts.js
let libP = null;
function chartLib() {
  if (window.echarts) return Promise.resolve(window.echarts);
  libP ??= new Promise((res, rej) => {
    const s = document.createElement("script");
    s.src = new URL("./vendor/echarts.min.js", import.meta.url).href;   // same URL as the preload
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
    }).catch((e) => showError(host, e));              // the tile's own error state
    return stub;
  }
  host.__mount = (host.__mount || 0) + 1;
  echarts.getInstanceByDom(host)?.dispose();         // a remount replaces, never merges
  const inst = echarts.init(host);
  inst.setOption(buildOption());
  return inst;
}
```

### Trim the library to what the app draws

Most chart libraries publish a modular build. A bundle of only the series types and components the app actually uses is typically a third smaller (one build: 1,122 KB to 713 KB, 369 KB to 243 KB gzipped). Inventory what the code passes the library (every series `type:`, and components such as legend, tooltip, visualMap, markLine), bundle exactly those once with a bundler outside the package, and commit the bundle together with its entry file and the command that rebuilt it.

**A trimmed build fails silently.** A series type left out of the bundle still mounts its canvas, throws nothing and logs nothing, and draws an empty box. A canvas count proves nothing. Verify by reading pixels: open every page and every kind of drawer, and require each chart canvas to have drawn pixels, against the full build first and then the trimmed one. Write the list of included types next to the bundle, so the next person to add a chart type knows to rebuild.

## Ask once

- **One query text per resource.** `Publisher.query` caches nothing: every call is a request. Any cache the app keeps, any sharing of an in-flight request, and any prefetch are keyed on the exact query string. Two pages that read the same tile with different text (one adds `order_by`, one does not) get two cache entries and two round trips for one answer. Where order does not matter to a caller, send the text the other caller sends.
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
  const unbusy = () => { host.classList.remove("is-busy"); host.removeAttribute("aria-busy"); };
  return Publisher.query(MODEL, query).then((rows) => {
    if (seq !== host.__seq) return;                  // a newer run owns this tile
    unbusy(); host.__drawn = true;
    render(rows, host);
    host.style.minHeight = "";                       // released after the new rows are in
  }, (e) => {
    if (seq !== host.__seq) return;
    unbusy(); showError(host, e);                    // a failed re-run must not stay dimmed and busy
    host.style.minHeight = "";
  });
}
```

## Answer from the last visit (opt-in)

A per-tab cache (`sessionStorage`, or memory) starts every new tab and every next-day visit from nothing: every query is re-sent and every tile waits on it. Measured on a real build: 1.6 to 3.8 s before the first screen filled in a new tab. Most data apps read tables built on a schedule, so the last visit's rows are usually still right. A persistent cache can draw them at once, but it is the one recipe here that can show someone rows they may not read, so it is opt-in and has conditions.

**Use it only when every reader sees the same rows.** Check the model before adding it: no `given:` declarations the app or a host sets per reader, no `#(authorize)` gates, no access filters, nothing that makes the same query text answer differently for different people. If any of those apply, do not persist results; a per-tab cache is the most you can do.

**Why the conditions.** Every `public/` app on a deployment is served from the API's own origin, so they all share one IndexedDB. The page is served without a sign-in, and it cannot tell who is viewing it. A row-level refusal is a 200 with fewer rows, not a 403, so no error marks the cached answer as wrong. A cache keyed on query text alone shows one reader's rows to the next person on the same browser profile, signed in or not, and keeps showing them to someone whose access was withdrawn.

How it stays safe:

- **Nothing paints from the store until this visit has proven it may read.** Every tile sends its query at once, as without a cache. Stored answers are held back until one query on the page has answered successfully, then painted together while the rest of the queries finish. A signed-out visitor, or someone whose access was withdrawn, gets an error before any stored row reaches the screen. The first screen then waits one round trip rather than every query.
- **Every stored answer is re-asked.** There is no window in which the page trusts the store and sends nothing. A stored answer older than the build schedule plus a margin (a day and a half for a daily build) is not painted at all.
- **The key is everything the answer depends on:** the environment and package (the page's own path), the model, the exact query text, and the query's options (`givens`, `filterParams`, a target `environment` or `package`).
- **A 401 or 403 on any query wipes the store**, and that tile shows its error.
- **Repaint only the tile that still shows that query, and only when the rows differ.** A tile whose filter changed while its query was in flight ignores the late answer (the sequence guard in "Hold still while loading"). An unchanged answer must not redraw: it would re-animate charts for no reason.
- **The store is capped.** Every filter combination is a new key; prune the oldest past a few hundred entries.
- **Show how old the number is.** A tile that can draw yesterday's answer should carry the as-of date of its data.

Measured before the session gate was added: the new-tab load drew its first screen in 87 to 93 ms. The gate adds one query's round trip to that.

```js
// cache.js: draw the last visit's answer once this visit has proven it may read.
// Opt-in: only for models whose rows are the same for every reader (see above).
const STALE = 36 * 3600e3, CAP = 300;
const SCOPE = location.pathname.split("/").slice(0, 5).join("/");   // /environments/<env>/packages/<pkg>
let mayRead; const live = new Promise((res) => (mayRead = res));     // resolves on the first good answer
const inflight = new Map();
const db = new Promise((res) => {
  try { const rq = indexedDB.open("app-cache", 1);
    rq.onupgradeneeded = () => rq.result.createObjectStore("q").createIndex("t", "t");
    rq.onsuccess = () => res(rq.result); rq.onerror = rq.onblocked = () => res(null);
  } catch { res(null); }                                   // private mode: no persistence
});
const store = (mode, fn) => db.then((d) => d && new Promise((res) => {
  try { const tx = d.transaction("q", mode); const r = fn(tx.objectStore("q")); tx.oncomplete = () => res(r?.result ?? null); tx.onerror = () => res(null); }
  catch { res(null); }
}));
const wipe = () => store("readwrite", (s) => s.clear());
const prune = () => store("readwrite", (s) => {
  const c = s.count(); c.onsuccess = () => {
    let extra = c.result - CAP; if (extra <= 0) return;
    s.index("t").openCursor().onsuccess = (e) => { const cur = e.target.result; if (cur && extra-- > 0) { cur.delete(); cur.continue(); } };
  };
});

// Resolves with rows for the tile: the stored answer once the page may read, or the
// network answer if that comes first. onFresh(rows, err) fires if the network answer
// arrives after a stored one was drawn and differs, or if the query is refused.
export function cachedQuery(model, q, opts = {}, onFresh) {
  const k = JSON.stringify([SCOPE, model, q, opts]);
  let net = inflight.get(k);                                 // two tiles, one request
  if (!net) {
    net = Publisher.query(model, q, opts).then((rows) => {
      mayRead();
      store("readwrite", (s) => s.put({ t: Date.now(), r: rows }, k)).then(prune);
      return rows;
    }, (e) => { if (e?.status === 401 || e?.status === 403) wipe(); throw e; })
      .finally(() => inflight.delete(k));
    inflight.set(k, net);
  }
  const stored = store("readonly", (s) => s.get(k)).then((box) =>
    box && Date.now() - box.t < STALE ? live.then(() => box.r) : new Promise(() => {}));   // never: wait for the network
  return Promise.race([net.then((r) => ({ r })), stored.then((r) => ({ r, stored: true }))]).then((first) => {
    if (first.stored) net.then(                              // drawn from the store: follow up once
      (rows) => { if (JSON.stringify(rows) !== JSON.stringify(first.r)) onFresh?.(rows, null); },
      (e) => onFresh?.(null, e));
    return first.r;
  });
}
```

## Start on intent (opt-in)

Resting the pointer on something that opens a drawer, or focusing it, usually comes a few hundred milliseconds before the click. Start the drawer's queries then, and the click finds them in flight or answered. Measured on one build: all 7 queries of a market drawer and all 15 of a product drawer (a name lookup included) were in flight before the click, and the click itself sent none. Worth it on pages whose drawers are the main way in; skip it where drawers are rare.

Build each drawer in one function that both the click and the warm-up call. A hand-written list of each drawer's queries, warmed on hover, is a second copy of every drawer's query text and drifts the first time a panel changes. Do not warm by dispatching a synthetic click on the row either: capture-phase listeners still run, a row that is a link navigates, a `<summary>` toggles, and an app that wires its drawers by delegation gets no warm-up at all and no error saying so.

```js
// drawers.js: a drill row names its drawer (data-drawer="market" data-key="42").
// DRAWERS.market(key, body, { warm }) sends the drawer's queries; warm, it stops there.
export function drawerFor(row, { warm = false } = {}) {
  const build = DRAWERS[row.dataset.drawer];
  if (!build) return;
  if (warm) return build(row.dataset.key, null, { warm });   // queries only: nothing rendered, nothing opened
  return build(row.dataset.key, openDrawer(row), { warm });   // openDrawer: the real open (reference/overlays.md)
}
document.addEventListener("click", (e) => { const row = e.target.closest?.("[data-drawer]"); if (row) drawerFor(row); });

// Intent: an 80ms rest, by pointer or keyboard, so a sweep down a list warms nothing;
// at most two drawers warming at once.
let timer = 0, warmingNow = 0;
const warmed = new WeakSet();
const intent = (e) => {
  clearTimeout(timer);
  const row = e.target.closest?.("[data-drawer]");
  if (!row || warmed.has(row) || e.pointerType === "touch") return;
  timer = setTimeout(() => {
    if (warmingNow >= 2) return;
    warmed.add(row); warmingNow++;
    Promise.resolve(drawerFor(row, { warm: true })).catch(() => {}).finally(() => warmingNow--);
  }, 80);
};
document.addEventListener("pointerover", intent);
document.addEventListener("focusin", intent);
```

What makes it safe:

- **A warm build only asks.** It sends the drawer's queries through the same cache as the real open (see "Ask once") and returns; it renders nothing, so no chart instance is left on a detached node, and nothing opens, navigates, or writes.
- **The warm flag is an argument, not a global.** A drawer that first resolves a name to an id passes `{ warm }` on to the step that runs after the lookup, so a warm-up never opens a drawer when the lookup returns.
- **Warm what the click will ask for.** The click and the warm-up run the same `DRAWERS` function, so they send the same query text.
- **Never warm on page load.** Warm-ups wait for intent. Warming every drawer a page could open is a query storm the reader pays for.

## Respond before the answer arrives

A click that waits on a query before anything visible happens reads as a dead click. The common case is a drill that must first resolve a name to an id: open the drawer immediately, titled with the name and a skeleton, run the lookup inside it, and replace the skeleton in place when the id arrives. Drop the late answer if the reader has closed the drawer or opened another subject meanwhile (`reference/depth-patterns.md`, stale-response guard).

## What the page cannot fix

Compression and cache headers belong to whatever serves `public/`. Check them (`curl -sI -H 'Accept-Encoding: gzip, br' <url>`): a chart library served uncompressed is downloaded at full size on the first visit, and with `max-age=0` every later page asks the server again before using it (a conditional request answered 304, one round trip per file). Report it to whoever runs the server rather than working around it in the page.
