<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# HTML Data App Runtime

> `Publisher.query(modelPath, malloy)` returns an array of plain row objects. Build the Malloy string, let the model do the work, and render the rows with whatever front-end code you like.

The runtime loads from the root-relative `<script src="/sdk/publisher.js">` and adds one global, `window.Publisher`.

## The query contract

| Call                                            | Returns                  | Use for                            |
| ----------------------------------------------- | ------------------------ | ---------------------------------- |
| `Publisher.query(modelPath, malloy, opts?)`     | `Promise<Array>` of rows | driving your own charts and tables |
| `Publisher.queryFull(modelPath, malloy, opts?)` | `Promise<MalloyResult>`  | handing to `<malloy-render>`       |

- `modelPath` is the model FILE path within the package, with `/` separators (`"subscriptions.malloy"`, `"models/events.malloy"`). It is not the source name.
- `malloy` is any query string, written in standard Malloy. This skill covers only the JavaScript glue, not Malloy syntax.
- `opts` (all optional): `sourceName`, `queryName` (`queryName` runs a saved query, with `sourceName` qualifying the source a view hangs off; `sourceName` on its own is a 400), `givens` (a `{ name: value }` map bound to the model's Malloy `given:` runtime parameters for this query; safe parameterization, values are bound by Publisher server-side, not string-interpolated), `filterParams` (values for the model's legacy `#(filter)` source filters), `bypassFilters`, and `environment` / `package` (only if the page is served from outside `/environments/<env>/packages/<pkg>/`). `givens` and `filterParams` compose (both apply). With package versioning on, `versionId` queries one published version instead of the package's `latest`. A page served from a package that has versions is redirected to that version's own URL (`/environments/<env>/packages/<pkg>/versions/<v>/public/...`), so it pins every query to its own package to that version and its relative files (`./app.js`, stylesheets) come from that version too; a query that names another `package` gets that package's `latest`. Write relative URLs and nothing else changes.

The runtime also keeps a givens store, so filter state lives in one place and a host can drive it: `Publisher.setGivens({ region: "EMEA" })` merges into the store, `Publisher.givens` reads a copy, and every `query` / `queryFull` binds the store's givens under `opts.givens` (a key in `opts.givens` wins). Nothing re-runs by itself: subscribe with `Publisher.onGivensChange((givens) => refresh(givens))`, which returns an unsubscribe function, and re-query there. Route the page's own filter controls through `setGivens` too, so a click and a command from the host take the same path.

## Structure the app as modules, not one inline script

Past a single tile, an inline `<script>` becomes unmaintainable and untestable. Split the work, and load it without a build step: the runtime first as a plain global, then one ES-module entry point that `import`s your own files.

```html
<head>
  <!-- Start the chart library's download now, without making anything wait for it.
       Only on pages that draw a chart. -->
  <link rel="preload" as="script" href="./vendor/chart.umd.js" />
</head>
...
<script src="/sdk/publisher.js"></script>
<!-- One module entry; it imports the rest. ES modules resolve with no bundler. -->
<script type="module" src="./app.js"></script>
```

**Do not load the chart library with a plain `<script>` ahead of your code.** It is usually most of the page's weight, and loaded that way it holds every query until it has downloaded and parsed. `charts.js` injects it the first time it draws, so the queries start at once. `reference/performance.md` has the loader, and the stand-in that lets callers wire chart clicks before the library has arrived.

A separation that keeps each piece testable and changeable on its own:

- **`format.js`**. Pure functions only: number/date formatting, a series-align-by-month helper, status thresholds. No DOM, no globals. This is the file `node --test` can cover directly.
- **`charts.js`**. Turns a prepared data object into a drawn chart; the only file that touches the chart library, and the one that loads it.
- **`tiles.js`**. Your tiles as _data_: for each, its model/source/view (and target source, if any), plus a pure `build(rows)` that shapes query rows for the chart. This is the single source of truth for what each tile queries.
- **`app.js`**. The thin entry point: reads `tiles.js`, runs the queries, wires results to the DOM. Adding a tile means adding a `tiles.js` entry, not editing `app.js`.

Declare each tile's source and view names once, in `tiles.js`, and have everything else (render code, any agent prompt, tests) read from there. A second copy of those names in another file is the classic drift bug, and a _derived_ name (`okr_4_4_2_targets` invented from a tile code) is simply wrong: a target source may have an irregular name or not exist at all. Read the model; don't compute names.

## Patterns that work

These assume a `subscriptions.malloy` model whose `subscriptions` source defines the views `plan_mix`, `mrr_by_industry`, and `kpis`. The names are illustrative; swap in your own model and view names.

Run a named view:

```js
const rows = await Publisher.query(
  "subscriptions.malloy",
  "run: subscriptions -> plan_mix",
);
```

Refine a view from UI state by appending a `where:`. Restrict the values to ones you control (for example a dropdown populated from the model's own distinct values) and escape each interpolated value with a backslash before quotes and backslashes (Malloy rejects the SQL-style `''` doubling). An unescaped apostrophe in a value breaks out of the literal:

```js
function whereClause(state) {
  const q = (s) => s.replace(/\\/g, "\\\\").replace(/'/g, "\\'"); // backslash-escape for Malloy
  const parts = [];
  if (state.plan) parts.push(`plan = '${q(state.plan)}'`);
  if (state.industry) parts.push(`industry = '${q(state.industry)}'`);
  return parts.length ? `where: ${parts.join(", ")}` : "";
}
const rows = await Publisher.query(
  "subscriptions.malloy",
  `run: subscriptions -> plan_mix + { ${whereClause(state)} }`,
);
```

Do not interpolate free-text or otherwise untrusted input into the query string. Route parameterized input through `opts.givens` (or the legacy `opts.filterParams`) instead: Publisher binds those values server-side as typed parameters, so they are never concatenated into query text and can't inject query syntax. (One nuance: a `filter<T>`-typed given takes Malloy filter syntax as its value, so validate it against a known set like any other input; scalar givens carry no syntax at all.) `opts.givens` is safe _parameterization_, not an authorization boundary on its own: a plain given is whatever the request carries. A given the model declares `#(secure)` is the host's to set: the trusted tier in front of Publisher resolves it from the viewer's verified identity and replaces anything the page sent, which is what makes a gate on it a real boundary (`docs/authorize.md`). A page should never try to set a secure given itself in a deployed app; on a bare local server with no trusted tier in front, whatever the page sends is used, so say so in the app if it lets the viewer pick one. Where you must build query text from input, constrain it to a known set and escape it, or keep the filtering in model-defined views.

KPI or single-row view. Destructure element zero:

```js
const [kpis] = await Publisher.query(
  "subscriptions.malloy",
  "run: subscriptions -> kpis",
);
el.textContent = kpis.active_mrr; // the result is an array; kpis.active_mrr, not rows.active_mrr
```

Refresh a dashboard. Fire the tiles together:

```js
const [planMix, byIndustry, kpisRows] = await Promise.all([
  Publisher.query("subscriptions.malloy", "run: subscriptions -> plan_mix"),
  Publisher.query(
    "subscriptions.malloy",
    "run: subscriptions -> mrr_by_industry",
  ),
  Publisher.query("subscriptions.malloy", "run: subscriptions -> kpis"),
]);
```

Prefer defining the views in the model (one per tile, pre-aggregated and sorted) over building long query strings in JS.

**Same question, same query text.** `Publisher.query` caches nothing, but a cache the app keeps, a shared in-flight request, and any prefetch are all keyed on the exact string. Two pages reading the same view with different text (one adds an `order_by`) pay for two round trips for one answer, and a prefetch that warms different text from what the destination page sends warms nothing. Build each tile's query in one place and reuse that string wherever the same rows are needed.

**Set ARIA and data attributes as attributes.** A small element helper that does `Object.assign(document.createElement(tag), props)` silently drops `aria-modal`, `aria-label`, `aria-busy` and `data-*`: they are not properties, so assigning them does nothing and no error says so. Route any `aria-*`, `data-*` and `role` key through `setAttribute`. And a clickable row is a control: give it `tabIndex = 0` and Enter/Space handling, or keyboard users cannot open what a mouse user can.

Get the numbers right. The fastest way to ship a wrong-but-convincing dashboard is to paper over missing data:

- **Missing is not zero.** When you join two series (actuals to a separately-keyed target) and a key is absent, leave it `null` so the chart skips it. Do not `|| 0`, which plots a real-looking zero and reads as "we hit nothing that month." Align on a normalized key (`"YYYY-MM"`), and let the renderer omit null points:

  ```js
  // monthKey/monthLabel are your own format.js helpers: monthKey normalizes a
  // date to a "YYYY-MM" string; monthLabel formats it for display.
  // target may not cover every actual month; an absent month stays null, never 0.
  const target = new Map(
    planRows.map((r) => [monthKey(r.plan_month), Number(r.target_revenue)]),
  );
  const data = actualRows.map((r) => ({
    label: monthLabel(r.order_month),
    actual: Number(r.revenue),
    target: target.has(monthKey(r.order_month))
      ? target.get(monthKey(r.order_month))
      : null,
  }));
  ```

- **"Current" means latest non-null.** For a KPI scorecard, scan back to the last month that actually has data rather than reading the final row, which may be an incomplete current month.
- **Guard division in Malloy, not after.** `avg(paid / nullif(active, 0))`. A `nullif` in the query beats catching `Infinity`/`NaN` in JS.
- **Convert units explicitly.** If the model stores a 0 to 1 fraction and you show a percent, multiply once in `build()` and comment it. Mismatched units are a silent off-by-100.

Loading, empty, and error states. Handle all three; a bare `.then()` that assumes rows leaves the page blank when the query is slow or fails:

```js
const el = document.getElementById("out");
el.textContent = "Loading...";
Publisher.query("subscriptions.malloy", "run: subscriptions -> plan_mix")
  .then((rows) => {
    if (!rows.length) {
      el.textContent = "No data.";
      return;
    }
    render(rows);
  })
  .catch((err) => {
    el.textContent = `Query failed (${err.status ?? ""}): ${err.response?.message ?? err.message}`;
  });
```

Render through `<malloy-render>`. `queryFull` returns the full Malloy result envelope (the JSON form of the server's result, not a live result object) to hand to the component:

```js
const el = document.querySelector("malloy-render");
el.result = await Publisher.queryFull(
  "subscriptions.malloy",
  "run: subscriptions -> plan_mix",
);
```

Publisher does not serve or bundle `<malloy-render>`; you must obtain a built component bundle matched to your model's Malloy version and vendor it into `public/` yourself, then confirm it accepts the envelope as-is. The `storefront` example draws with Chart.js and has no `<malloy-render>` element in its pages, so the component itself is not exercised there. A view tagged in the model (for example `# bar_chart`) drives how it draws.

Validate every query before wiring it into render code, using whatever query tool your environment provides, or by POSTing the query to a running Publisher at `/api/v0/environments/<env>/packages/<pkg>/models/<modelPath>/query` with body `{"compactJson":true,"query":"..."}`, or by running `Publisher.query` once and logging the rows. Malloy names result columns after the `group_by` / `aggregate` field names (`group_by: plan` gives a `plan` column; `aggregate: account_count` gives an `account_count` column), so confirm those names against real output before you read them.

If you validate the rendered page in a headless browser (Playwright or Puppeteer), do not wait for network idle: `publisher.js` holds the live-reload SSE stream open, so the page never reaches it. Wait on `domcontentloaded` or `load` plus a content selector instead.

## Context, auth, live reload (all automatic)

- Context. The server injects a `publisher-context` block into the page (environment, package, version, API base), and `Publisher.context` is `{ environment, package }` from it, so `query` needs no env or package args. Without it the runtime reads them from a page served under `/environments/<env>/packages/<pkg>/...`. Serving from elsewhere? Pass `opts.environment` and `opts.package`.
- Auth. By default the runtime sends cookies (`credentials: include`), so a signed-in user is authenticated with no code. For a bearer token, call `Publisher.setToken(token)` first; `Publisher.setToken(null)` reverts to cookies. An embedding host can also hand over a token itself (see "Talking to the host" below), and a query that gets a 401 asks the host for one and retries once.
- Live reload. Under `--watch-env`, the page reloads on package changes by itself, except a page served at a version's own URL, which never changes. Nothing to wire.

## Talking to the host

When a page is embedded (`reference/embedding.md`), the host and the page can exchange messages. The runtime does the protocol; the page adds only handlers and emits.

- `Publisher.onGivensChange(fn)` is how a host or an agent drives the page's filters. Register it and the page advertises a `givens.set` command: when the host sends `{ givens: { region: "EMEA" } }` the runtime merges it into the givens store and calls `fn` with the whole store, so `fn` re-runs the tiles. A page that never registers it is never offered the command, so an agent knows not to try. Any filter the page wants a host to be able to set should therefore be a given held in the store, not private variables.
- `Publisher.on(type, fn)` registers a handler for any other message type from the host; `fn(payload)` may return a value, which becomes the reply to a request. Handlers are the page's advertised capabilities: the host sees the registered types in the `ready` event.
- `Publisher.emit(type, payload)` tells the host something. The runtime already emits `ready`, `givens.changed` (after every `setGivens`), `query.ran` (model, source and view names, givens, row count, milliseconds, and the first 2000 characters of the query text, after every query) and `version.unavailable`. Pages may emit `context.update` (what the page is showing) or `chat.prefill` (`{ text }`) for hosts that listen.
- All of it is a no-op when the page is not embedded, or the host is not one the server was configured to allow, so write it without checking.

```js
Publisher.onGivensChange((givens) => refresh(givens)); // host commands and clicks both land here
regionSelect.onchange = () => Publisher.setGivens({ region: regionSelect.value });
```

## When the app fails

| Symptom                                                                      | Likely cause and fix                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| ---------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 404 or "model not found"                                                     | `modelPath` wrong. It is the file path (`"subscriptions.malloy"`), with `/` separators, not the source name.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| "source/view not defined"                                                    | View or source name guessed. Read the model (your environment's context tool, or open the `.malloy` file) and use the real names.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| Promise rejects, message starts `Publisher.query:`                           | Read `error.status` and `error.response` for the server's reason (compile error, missing required parameter, permission).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| Empty array when you expect rows                                             | Filter value mismatch (case, spelling, type, or a non-ASCII character like `≤` or an en-dash in the literal). Copy the literal verbatim from the model, do not retype the user's paraphrase, and confirm it with a distinct-values query (`run: src -> { group_by: the_dimension }`). Quote strings, use `@` for dates.                                                                                                                                                                                                                                                                                                                                              |
| 400 on a given (ungated source)                                              | An unknown given name (check spelling; names are case-sensitive), a required given left unset, or a value that doesn't fit the declared type. Malloy rejects it when preparing the query; supply declared givens via `opts.givens` with the right shape (see the givens type table).                                                                                                                                                                                                                                                                                                                                                                                 |
| 403 on a query that should be allowed, when passing givens to a gated source | On a gated source, a bad given (unknown name or wrong-typed value) fails closed, so it looks like access denied rather than validation. A `#(authorize)` lock also answers 403 for a caller it simply does not admit. Check the given names and values against the model.                                                                                                                                                                                                                                                                                                                                                                                            |
| KPI shows `undefined`                                                        | The result is an array. Read `rows[0].field` (or destructure `const [k] = ...`), not `rows.field`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| A NEW file under `public/` 404s while the rest of the app serves             | The server loaded the package **before** `public/` existed, and it is not watching. Without `--watch-env` the package is published at load as an immutable version under `publisher_data/<env>/<pkg>/<version>/` and static files are served from it, so a file you add afterwards is never seen. Restart with `--watch-env <env>`, or bump `version` in `publisher.json` and publish again; never write into `publisher_data/`. Verified by writing a probe file into the real package and getting a 404 while the served package answered fine - and note the existing row below sends you looking under `public/`, which is exactly the wrong place for this one. |
| Page loads in dev but is not listed or not served                            | The file is not under the package's `public/` directory. Publisher serves only `public/`; a page written anywhere else (for example `/tmp`) is never reachable at `/environments/<env>/packages/<pkg>/<file>`.                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| Queries fail only when embedded cross-origin                                 | Cookies are not sent cross-site. Serve same-origin, or pass a bearer token.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| No live reload                                                               | Watch mode is off. Start with `--watch-env <env>`; without it the events stream reports `mode: disabled` and never reloads.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
