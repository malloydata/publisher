# AGENTS.md: building an in-package HTML data app

Guidance for an AI coding agent working in this package. It mirrors the Malloy
Publisher "html data apps" skill set so the same help is available in clients that
read `AGENTS.md` (Codex, Cursor, Windsurf, Copilot, and others) rather than
Anthropic Agent Skills. The authoritative reference is `docs/html-data-apps.md` in
the Publisher repo.

## What this package is

An in-package HTML data app. The package ships a `public/` directory of plain web
files; Publisher serves them and exposes a runtime, `Publisher.query(...)`, that
runs Malloy against the package's models and returns plain JSON rows. No build
step, no framework. This example (`subscriptions`) is a SaaS subscriptions
dashboard over `subscriptions.parquet`.

```
subscriptions.malloy   # the model, stays private
subscriptions.parquet  # data, stays private
publisher.json         # name, version, description
public/                # ONLY this directory is web-served
  index.html
  embed-test.html
```

Only `public/` is reachable over the web, at
`/environments/<env>/packages/<pkg>/<file>`. The model, data, and `publisher.json`
are private and reached only through the query API, which still applies the
model's governance (filters/givens, access modifiers, authorize rules). A package
is a data app simply by having a `public/` directory.

## How to build or change a page

1. Read the model first. Use the model's real source and view names from
   `subscriptions.malloy`; never guess field or view names. This model defines
   source `subscriptions` with views `kpis`, `mrr_by_month`, `plan_mix`,
   `mrr_by_plan`, `mrr_by_industry`, `mrr_by_country`, `accounts`, and
   `overview`.
2. Write the page under `public/`. A page written anywhere else is never served.
3. Load the runtime root-relative, then query.
4. Validate each query before wiring it into render code (see below).
5. Preview with watch mode.

## The runtime API

Load it once per page with a root-relative tag, so it resolves whatever
environment or package the page is served under:

```html
<script src="/sdk/publisher.js"></script>
```

It adds one global, `window.Publisher`:

- `Publisher.query(modelPath, malloy, opts?)` returns `Promise<Array>` of plain
  row objects, for driving your own charts and tables.
- `Publisher.queryFull(modelPath, malloy, opts?)` returns the full Malloy result
  envelope, for handing to a `<malloy-render>` element.
- `Publisher.embed(selector, { src, token?, height?, allow? })` mounts a
  sandboxed, auto-resizing iframe and returns `{ iframe, destroy() }`.
- `Publisher.context` is `{ environment, package }`, inferred from the page URL.
- `Publisher.setToken(token | null)` sets a bearer token used by all later
  queries on the page; `null` reverts to cookies.

`modelPath` is the model FILE path within the package (`"subscriptions.malloy"`),
with `/` separators. It is not the source name. `opts` may carry `sourceName`,
`queryName`, and `environment` / `package` (only for pages served outside
`/environments/<env>/packages/<pkg>/`). The runtime does not pass per-query
[givens](../../docs/givens.md) values — model-declared given *defaults* still
apply.

## Query patterns

Run a named view:

```js
const rows = await Publisher.query("subscriptions.malloy", "run: subscriptions -> plan_mix");
```

Refine a view with a filter built from UI state:

```js
function whereClause(state) {
  const parts = [];
  if (state.plan) parts.push(`plan = '${state.plan}'`);
  if (state.industry) parts.push(`industry = '${state.industry}'`);
  if (state.country) parts.push(`country = '${state.country}'`);
  return parts.length ? `where: ${parts.join(" and ")}` : "";
}
const rows = await Publisher.query(
  "subscriptions.malloy",
  `run: subscriptions -> plan_mix + { ${whereClause(state)} }`,
);
```

These values are interpolated into the query string, so they must come from
trusted, constrained sources (a dropdown populated from the model's own distinct
values, for example — this page fills its dropdowns that way). Never interpolate
free-text or untrusted input into a `run:` string; keep that filtering in
model-defined views instead.

A single-row KPI view returns a one-element array; read element zero:

```js
const [kpis] = await Publisher.query("subscriptions.malloy", "run: subscriptions -> kpis");
el.textContent = kpis.active_mrr;   // the result is an array; kpis.active_mrr, not rows.active_mrr
```

A dashboard fires its tiles together:

```js
const [kpisRows, byMonth, planMix] = await Promise.all([
  Publisher.query("subscriptions.malloy", "run: subscriptions -> kpis"),
  Publisher.query("subscriptions.malloy", "run: subscriptions -> mrr_by_month"),
  Publisher.query("subscriptions.malloy", "run: subscriptions -> plan_mix"),
]);
```

Prefer defining the views in the model (one per tile, pre-aggregated and sorted)
over building long query strings in JS.

Validate a query before wiring its result into render code: POST it to a running
Publisher at `/api/v0/environments/<env>/packages/<pkg>/models/<modelPath>/query`
with body `{"compactJson":true,"query":"..."}`, or run the `Publisher.query` once
and log the rows. Malloy names result columns after the `group_by` / `aggregate`
field names (`group_by: plan` gives a `plan` column; `aggregate: account_count`
gives an `account_count` column), so confirm those names against real output.

## When the page does not work

- 404 or "model not found": `modelPath` is the file path (`"subscriptions.malloy"`),
  not the source name.
- "source/view not defined": a view or source name was guessed; use the model's
  real names.
- Promise rejects with a message starting `Publisher.query:`: read `error.status`
  and `error.response` for the server's reason.
- Empty array when you expect rows: a filter value mismatch (case, spelling, or
  type — e.g. a `plan` or `status` that doesn't exist). Copy the literal verbatim
  from the model; confirm with a distinct-values query.
- KPI shows `undefined`: the result is an array; read `rows[0].field`.
- Page not served: the file is not under `public/`.

## Embedding in another page

```html
<script src="https://your-publisher/sdk/publisher.js"></script>
<div id="dashboard"></div>
<script>
  const handle = Publisher.embed("#dashboard", {
    src: "https://your-publisher/environments/examples/packages/html-data-app/index.html",
  });
  // handle.destroy() removes the iframe and its listeners.
</script>
```

Omit `height` and the frame auto-sizes: the embedded page measures its own content
and posts its height; the host accepts that message only from the iframe it
created. The page only has to load `/sdk/publisher.js`. Same-origin embeds
authenticate with the browser's cookies; for a cross-origin embed, mint a
short-lived signed token server-side and pass it as `token` (the embedded page
authenticates with it). Never put a long-lived or admin token in client HTML. See
`public/embed-test.html` for a live demo.

## Live reload

Run the server with `--watch-env <env>` (or `PUBLISHER_WATCH=<env>`). Editing a
`.malloy` file recompiles the package; editing a file under `public/` reloads open
pages on its own. Nothing to wire in the page.

## Security

Everything under `public/` is web-served as-is, so keep secrets out of it. Your
protection lives in the model and the database, behind the query API. Served HTML
is framable by any origin by default; set `PUBLISHER_FRAME_ANCESTORS` to restrict
embedding origins for any page that shows sensitive data.
