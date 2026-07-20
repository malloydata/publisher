---
name: malloy-html-data-apps
description: Build or modify an in-package HTML data app for a Malloy Publisher package (a public/ directory the package serves). Use when the user wants a hand-authored HTML dashboard or web page backed by a package's Malloy models, with no build step.
---

# In-Package HTML Data Apps

> A package becomes a web app by adding a `public/` directory. Publisher serves those files and gives the page `Publisher.query(...)` to run Malloy against the package's models. No build step, no npm, no framework.

## When this is the right tool

| The user wants | Use |
|---|---|
| A hand-authored HTML/JS dashboard, no toolchain | this skill (an HTML data app) |
| A React app with managed components | the Publisher React SDK (out of scope here) |
| An analyst notebook with charts | a Malloy notebook (`.malloynb`) |
| Point-and-click exploration, no code | the Publisher Explorer |

Pick an HTML data app when the user wants full control of the markup and only plain web files.

## Package anatomy

```
my-package/
  publisher.json        # name, version, description
  subscriptions.malloy  # the model(s), stays private
  subscriptions.parquet # data, stays private
  public/               # ONLY this directory is web-served
    index.html
    app.js
```

Only `public/` is reachable over the web, at `/environments/<env>/packages/<pkg>/<file>`. Models, data, and `publisher.json` are private and reached only through the query API, which still applies the model's filters, access modifiers, and authorize rules. There is no flag to set: a `public/` directory is what makes a package an app.

## Build sequence

The agent orchestrates these. Each query and chart step hands off to a focused skill.

1. READ THE MODEL FIRST. Get the model's real source and view names, through your environment's context tool if it has one, or by opening the `.malloy` file directly. Never guess field or view names.
2. SCAFFOLD the package (template below).
3. WRITE THE QUERIES with `skill:malloy-html-data-app-runtime`. Validate each before pasting it into the page, using whatever query tool your environment provides or a running Publisher (see `skill:malloy-html-data-app-runtime`). Malloy syntax questions go to `skill:malloy-queries`.
4. CHOOSE CHARTS with `skill:malloy-charts` when rendering through `<malloy-render>`; otherwise it is your own chart library drawing the returned rows. Vendor any chart library into `public/` and load it locally, not from a CDN, because embedded author JavaScript runs with the viewing user's data authority. (The shipped example loads its chart library from a CDN for brevity; a published or embedded app should vendor it.)
5. EMBED (optional) with `skill:malloy-html-data-app-embedding`.
6. PREVIEW with the local authoring loop (below).
7. VERIFY before you call it done (see "What 'done' means" below). This step is not optional.

The scaffold in step 2 only proves the wiring. It is the start, not the deliverable. What you ship is a production app that meets the recipe below.

## What "done" means (production recipe)

A data app you can defend has all of these. Build to this list, not to the scaffold.

- **Real names, never guessed.** Every source, view, and field name comes from the model you read in step 1. A name you derived or assumed is a bug waiting to surface as an empty tile.
- **DOM-only - never `innerHTML` with interpolated values.** Build every element with `createElement` + `textContent`; do not assign `innerHTML` (or `insertAdjacentHTML`, `document.write`) with any string that contains a model value. Query results render any markup they contain - an XSS vector, and blocked outright under a Trusted-Types CSP. This is a hard build rule, not a lint suggestion: an app that interpolates a model value into `innerHTML` is not done.
- **Modular, not one inline blob.** Split the page into modules per `skill:malloy-html-data-app-runtime` (pure formatting helpers, a chart layer, your tile/query definitions as data, a thin entry point). One source of truth for each tile's model/source/view, no parallel maps that drift.
- **Every tile handles loading, empty, and error on its own.** One failing query must not blank the page. (`skill:malloy-html-data-app-runtime`.)
- **Defensible numbers.** Missing ≠ zero (omit the point, don't plot a fake 0); show the latest non-null value for "current"; guard division with `nullif`; convert units explicitly. (`skill:malloy-html-data-app-runtime`.)
- **Visible assumptions.** When you assume something (two sources joined by month, an in-month proxy that differs from a certified definition) or a metric is incomplete, say so *in the app*: a caption, a footnote, a placeholder card with the reason. The non-technical user cannot see your reasoning; bury a caveat and you have misled them. Don't silently drop a metric you couldn't model. Show a placeholder that names what's missing and why.
- **Looks decent.** Give it real layout, type, and color: a styled card grid with a clear hierarchy, not raw unstyled tables.
- **Vendored libraries.** Chart and helper libraries live in `public/`, loaded locally (step 4).
- **Lazy-load below the fold (once there are many tiles).** Don't fire every tile's query on load. `reference/lazy-load.md` is the recipe: `IntersectionObserver` (rootMargin ~240px) + a small concurrency cap + reserve each tile's height so lazy tiles don't reflow. Includes the verification trap - on a short/tall-default viewport all tiles intersect at once and you get a false "everything deferred" pass, so test on a deliberately small viewport.

### Verify before you call it done

You are building for someone who cannot tell a correct dashboard from a broken one. Verification is your job, not theirs.

- **Validate every query against the model before wiring it in** (step 3): confirm it compiles and that the column names match what your render code reads.
- **Load the finished page and confirm every tile shows real numbers**: not stuck on "Loading…", not an error, not an empty state you didn't intend. In a headless browser, wait on `load` plus a content selector, not network idle (`publisher.js` holds an SSE stream open; see `skill:malloy-html-data-app-runtime`). Don't hand-roll this each time - `reference/verification-harness.md` is a copy-adaptable recipe: a mock `sdk/publisher.js` returning canned rows keyed by `(model, query)`, a `python3 -m http.server` webroot, and Playwright assertions (KPIs non-null, no `.is-error`, no stuck `.kit-skeleton`, a chart/table present). It also documents the false-"stuck skeleton" trap (assert after the mock's async delay, never on `networkidle`).
- **Unit-test any non-trivial pure logic** (a month-join, a de-cumulation, a unit conversion). Keep that logic in DOM-free helpers so `node --test` can cover it, and run it.

## Minimal scaffold

`publisher.json` at the package root:

```json
{ "name": "my-package", "version": "0.0.1", "description": "..." }
```

`public/index.html` is a NEW file you create (make the `public/` directory if it does not exist). Load the runtime root-relative, then query. The examples below use the shipped `html-data-app` package (source `subscriptions`); swap in your own model and a view it defines.

Start with the smallest page that proves the wiring, dumping the rows:

```html
<!doctype html>
<title>My dashboard</title>
<pre id="out"></pre>
<script src="/sdk/publisher.js"></script>
<script>
  Publisher.query("subscriptions.malloy", "run: subscriptions -> plan_mix").then((rows) => {
    document.getElementById("out").textContent = JSON.stringify(rows, null, 2);
  });
</script>
```

Then render the rows. This page builds a table from whatever columns the view returns, so it does not depend on the exact field names:

```html
<!doctype html>
<title>Account mix by plan</title>
<table id="t"><thead></thead><tbody></tbody></table>
<script src="/sdk/publisher.js"></script>
<script>
  Publisher.query("subscriptions.malloy", "run: subscriptions -> plan_mix").then((rows) => {
    const t = document.getElementById("t");
    if (!rows.length) { t.textContent = "No rows."; return; }
    const cols = Object.keys(rows[0]);
    const headRow = t.tHead.insertRow();
    for (const c of cols) {
      const th = document.createElement("th");
      th.textContent = c;
      headRow.appendChild(th);
    }
    for (const r of rows) {
      const tr = t.tBodies[0].insertRow();
      for (const c of cols) tr.insertCell().textContent = r[c];
    }
  });
</script>
```

Build row content with `textContent`, not `innerHTML` with model values: an `innerHTML` table renders any markup a value contains. This is the HTML-output side of the don't-trust-interpolated-values rule that `skill:malloy-html-data-app-runtime` applies to Malloy query strings.

Two invariants break a page most often:

- **The file must live under `public/`.** Publisher serves only `public/`, so a page written anywhere else (for example `/tmp`) is never reachable at `/environments/<env>/packages/<pkg>/<file>`.
- **The script src must be the root-relative `/sdk/publisher.js`**, not a relative path.

A third gotcha: the first argument to `Publisher.query` is the model FILE path (`"subscriptions.malloy"`), not the source name.

## Authoring loop and publishing

Authoring happens locally, then you publish. These are two stages.

### Author locally (with live reload)

Run a local Publisher from the directory that holds your `publisher.config.json` and package folder(s):

```sh
npx @malloy-publisher/server --server_root . --port 4000 --watch-env <env>
```

`--watch-env <env>` (or `PUBLISHER_WATCH=<env>`) mounts that environment's local-dir packages in place (a symlink, not a copy) and watches them: editing a `.malloy` recompiles the package, and editing a `public/` file live-reloads any open page over an SSE stream. Nothing to wire in the page. The app is served at `http://localhost:4000/environments/<env>/packages/<pkg>/index.html`.

`publisher.config.json` (at `--server_root`) declares the environment, its packages, and its connections:

```json
{
  "frozenConfig": false,
  "environments": [
    {
      "name": "<env>",
      "packages": [{ "name": "<pkg>", "location": "./<pkg>" }],
      "connections": []
    }
  ]
}
```

A local package uses a filesystem `location` (`"./<pkg>"`, relative to the directory holding `publisher.config.json`); a remote one uses a GitHub `tree` URL. If one model in the package fails to compile, the **whole package** fails to load, so a stray notebook/model error blanks every tile. (Common one: a `.malloynb` whose cells each `import "x.malloy"`, the notebook compiles as one batch, so the repeated import errors `Cannot redefine 'x'`. Import once in the first cell.)

### Publishing

Publishing an app is publishing its package: get the package into publishable shape and hand it to your host's publishing workflow. A deployed package serves its `public/` app the same way a local one does, at `/environments/<env>/packages/<pkg>/<file>`. A deployed environment has no `--watch-env` live reload, so the loop there is author, publish, then view.
