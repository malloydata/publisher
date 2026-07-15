---
name: html-data-app-runtime
description: Write the JavaScript that drives an in-package HTML data app, calling Publisher.query, building queries from filter state, and handling results and errors. Read before writing the page's data code.
---

# HTML Data App Runtime

> `Publisher.query(modelPath, malloy)` returns an array of plain row objects. Build the Malloy string, let the model do the work, and render the rows with whatever front-end code you like.

The runtime loads from the root-relative `<script src="/sdk/publisher.js">` and adds one global, `window.Publisher`.

## The query contract

| Call | Returns | Use for |
|---|---|---|
| `Publisher.query(modelPath, malloy, opts?)` | `Promise<Array>` of rows | driving your own charts and tables |
| `Publisher.queryFull(modelPath, malloy, opts?)` | `Promise<MalloyResult>` | handing to `<malloy-render>` |

- `modelPath` is the model FILE path within the package, with `/` separators (`"carriers.malloy"`, `"models/events.malloy"`). It is not the source name.
- `malloy` is any query string, written in standard Malloy. This skill covers only the JavaScript glue, not Malloy syntax.
- `opts` (all optional): `sourceName`, `queryName`, `givens` (a `{ name: value }` map bound to the model's Malloy `given:` runtime parameters for this query; safe parameterization, values are bound by the runtime, not string-interpolated), `filterParams` (values for the model's legacy `#(filter)` source filters), `bypassFilters`, and `environment` / `package` (only if the page is served from outside `/environments/<env>/packages/<pkg>/`). `givens` and `filterParams` compose (both apply).

## Structure the app as modules, not one inline script

Past a single tile, an inline `<script>` becomes unmaintainable and untestable. Split the work, and load it without a build step: put your shared libraries first as plain globals, then one ES-module entry point that `import`s your own files.

```html
<!-- Globals first: the runtime, then any vendored chart library. -->
<script src="/sdk/publisher.js"></script>
<script src="./vendor/chart.umd.js"></script>
<!-- One module entry; it imports the rest. ES modules resolve with no bundler. -->
<script type="module" src="./app.js"></script>
```

A separation that keeps each piece testable and changeable on its own:

- **`format.js`**. Pure functions only: number/date formatting, a series-align-by-month helper, status thresholds. No DOM, no globals. This is the file `node --test` can cover directly.
- **`charts.js`**. Turns a prepared data object into a drawn chart; the only file that touches the chart library.
- **`tiles.js`**. Your tiles as *data*: for each, its model/source/view (and target source, if any), plus a pure `build(rows)` that shapes query rows for the chart. This is the single source of truth for what each tile queries.
- **`app.js`**. The thin entry point: reads `tiles.js`, runs the queries, wires results to the DOM. Adding a tile means adding a `tiles.js` entry, not editing `app.js`.

Declare each tile's source and view names once, in `tiles.js`, and have everything else (render code, any agent prompt, tests) read from there. A second copy of those names in another file is the classic drift bug, and a *derived* name (`okr_4_4_2_targets` invented from a tile code) is simply wrong: a target source may have an irregular name or not exist at all. Read the model; don't compute names.

## Patterns that work

These run against the example `carriers` package (source `carriers`; views `by_letter`, `by_size_bucket`, `kpis`). Swap in your own model and view names.

Run a named view:

```js
const rows = await Publisher.query("carriers.malloy", "run: carriers -> by_letter");
```

Refine a view from UI state by appending a `where:`. Restrict the values to ones you control (for example a dropdown populated from the model's own distinct values) and escape each interpolated value with a backslash before quotes and backslashes (Malloy rejects the SQL-style `''` doubling). An unescaped apostrophe in a value breaks out of the literal:

```js
function whereClause(state) {
  const q = (s) => s.replace(/\\/g, "\\\\").replace(/'/g, "\\'"); // backslash-escape for Malloy
  const parts = [];
  if (state.letter) parts.push(`letter = '${q(state.letter)}'`);
  if (state.bucket) parts.push(`size_bucket = '${q(state.bucket)}'`);
  return parts.length ? `where: ${parts.join(", ")}` : "";
}
const rows = await Publisher.query(
  "carriers.malloy",
  `run: carriers -> by_letter + { ${whereClause(state)} }`,
);
```

Do not interpolate free-text or otherwise untrusted input into the query string. Route parameterized input through `opts.givens` (or the legacy `opts.filterParams`) instead: those values are bound by the runtime as typed parameters, not string-interpolated, so they can't inject query syntax. (One nuance: a `filter<T>`-typed given takes Malloy filter syntax as its value, so validate it against a known set like any other input; scalar givens carry no syntax at all.) `opts.givens` is safe *parameterization*, not an authorization boundary: a client-supplied given is client-trusted unless a server upstream (the Credible router, or an operator's per-package config) strips or finalizes it. Where you must build query text from input, constrain it to a known set and escape it, or keep the filtering in model-defined views.

KPI or single-row view. Destructure element zero:

```js
const [kpis] = await Publisher.query("carriers.malloy", "run: carriers -> kpis");
el.textContent = kpis.total;   // the result is an array; kpis.total, not rows.total
```

Refresh a dashboard. Fire the tiles together:

```js
const [byLetter, byBucket, kpisRows] = await Promise.all([
  Publisher.query("carriers.malloy", "run: carriers -> by_letter"),
  Publisher.query("carriers.malloy", "run: carriers -> by_size_bucket"),
  Publisher.query("carriers.malloy", "run: carriers -> kpis"),
]);
```

Prefer defining the views in the model (one per tile, pre-aggregated and sorted) over building long query strings in JS.

Get the numbers right. The fastest way to ship a wrong-but-convincing dashboard is to paper over missing data:

- **Missing is not zero.** When you join two series (actuals to a separately-keyed target) and a key is absent, leave it `null` so the chart skips it. Do not `|| 0`, which plots a real-looking zero and reads as "we hit nothing that month." Align on a normalized key (`"YYYY-MM"`), and let the renderer omit null points:

  ```js
  // monthKey/monthLabel are your own format.js helpers: monthKey normalizes a
  // date to a "YYYY-MM" string; monthLabel formats it for display.
  // target may not cover every actual month; an absent month stays null, never 0.
  const target = new Map(planRows.map((r) => [monthKey(r.plan_month), Number(r.target_revenue)]));
  const data = actualRows.map((r) => ({
    label: monthLabel(r.order_month),
    actual: Number(r.revenue),
    target: target.has(monthKey(r.order_month)) ? target.get(monthKey(r.order_month)) : null,
  }));
  ```

- **"Current" means latest non-null.** For a KPI scorecard, scan back to the last month that actually has data rather than reading the final row, which may be an incomplete current month.
- **Guard division in Malloy, not after.** `avg(paid / nullif(active, 0))`. A `nullif` in the query beats catching `Infinity`/`NaN` in JS.
- **Convert units explicitly.** If the model stores a 0 to 1 fraction and you show a percent, multiply once in `build()` and comment it. Mismatched units are a silent off-by-100.

Loading, empty, and error states. Handle all three; a bare `.then()` that assumes rows leaves the page blank when the query is slow or fails:

```js
const el = document.getElementById("out");
el.textContent = "Loading...";
Publisher.query("carriers.malloy", "run: carriers -> by_letter")
  .then((rows) => {
    if (!rows.length) { el.textContent = "No data."; return; }
    render(rows);
  })
  .catch((err) => {
    el.textContent = `Query failed (${err.status ?? ""}): ${err.response?.message ?? err.message}`;
  });
```

Render through `<malloy-render>`. `queryFull` returns the full Malloy result envelope (the JSON form of the server's result, not a live result object) to hand to the component:

```js
const el = document.querySelector("malloy-render");
el.result = await Publisher.queryFull("carriers.malloy", "run: carriers -> by_letter");
```

Publisher does not serve or bundle `<malloy-render>`; you must obtain a built component bundle matched to your model's Malloy version and vendor it into `public/` yourself, then confirm it accepts the envelope as-is. The shipped example renders rows with a plain chart library instead, so this path is not exercised there. A view tagged in the model (for example `# bar_chart`) drives how it draws.

Validate every query before wiring it into render code, using whatever query tool your environment provides, or by POSTing the query to a running Publisher at `/api/v0/environments/<env>/packages/<pkg>/models/<modelPath>/query` with body `{"compactJson":true,"query":"..."}`, or by running `Publisher.query` once and logging the rows. Malloy names result columns after the `group_by` / `aggregate` field names (`group_by: letter` gives a `letter` column; `aggregate: n is count()` gives an `n` column), so confirm those names against real output before you read them.

If you validate the rendered page in a headless browser (Playwright or Puppeteer), do not wait for network idle: `publisher.js` holds the live-reload SSE stream open, so the page never reaches it. Wait on `domcontentloaded` or `load` plus a content selector instead.

## Context, auth, live reload (all automatic)

- Context. A page served under `/environments/<env>/packages/<pkg>/...` infers its environment and package, so `query` needs no env or package args. Serving from elsewhere? Pass `opts.environment` and `opts.package`.
- Auth. By default the runtime sends cookies (`credentials: include`), so a signed-in user is authenticated with no code. For a bearer token, call `Publisher.setToken(token)` first; `Publisher.setToken(null)` reverts to cookies.
- Live reload. Under `--watch-env`, the page reloads on package changes by itself. Nothing to wire.

## When the app fails

| Symptom | Likely cause and fix |
|---|---|
| 404 or "model not found" | `modelPath` wrong. It is the file path (`"carriers.malloy"`), with `/` separators, not the source name. |
| "source/view not defined" | View or source name guessed. Read the model (your environment's context tool, or open the `.malloy` file) and use the real names. |
| Promise rejects, message starts `Publisher.query:` | Read `error.status` and `error.response` for the server's reason (compile error, missing required parameter, permission). |
| Empty array when you expect rows | Filter value mismatch (case, spelling, type, or a non-ASCII character like `≤` or an en-dash in the literal). Copy the literal verbatim from the model, do not retype the user's paraphrase, and confirm it with a distinct-values query (`run: src -> { group_by: the_dimension }`). Quote strings, use `@` for dates. |
| 400 on a given (ungated source) | An unknown given name (check spelling; names are case-sensitive), a required given left unset, or a value that doesn't fit the declared type. Malloy rejects it when preparing the query; supply declared givens via `opts.givens` with the right shape (see the givens type table). |
| 403 on a query that should be allowed, when passing givens to a gated source | On a source with `#(authorize)`, a bad given (unknown name or wrong-typed value) fails closed in the authorize check, so it looks like access denied rather than validation. Check the given names and values against the model. |
| KPI shows `undefined` | The result is an array. Read `rows[0].field` (or destructure `const [k] = ...`), not `rows.field`. |
| Page loads in dev but is not listed or not served | The file is not under the package's `public/` directory. Publisher serves only `public/`; a page written anywhere else (for example `/tmp`) is never reachable at `/environments/<env>/packages/<pkg>/<file>`. |
| Queries fail only when embedded cross-origin | Cookies are not sent cross-site. Serve same-origin, or pass a bearer token. |
| No live reload | Watch mode is off. Start with `--watch-env <env>`; without it the events stream reports `mode: disabled` and never reloads. |
