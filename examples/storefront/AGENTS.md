# AGENTS.md: working in the storefront package

Guidance for an AI coding agent working in this package. It mirrors the Malloy
Publisher skill set so the same help is available in clients that read
`AGENTS.md` (Codex, Cursor, Windsurf, Copilot, and others) rather than Anthropic
Agent Skills. The repo root [`AGENTS.md`](../../AGENTS.md) covers starting the
server and connecting over MCP; this file covers the package itself.

## What this package is

One ecommerce model surfaced four ways, on data that needs no credentials
(DuckDB reads the local Parquet and CSV in place):

```
storefront.malloy      # the model: star schema, measures, views, drill tags
givens.malloy          # shared given: declarations and their control tags
data_app.malloy        # scoped_orders: order_items narrowed by all five givens,
                       #   the source the data app queries
storefront.malloynb    # a notebook, the guided tour
dashboards/            # tagged dashboards, one file each
  _shared.malloy       # a shared include, NOT a dashboard (no # artifact)
  overview.malloy
  category.malloy
  regions.malloy
  seasonality.malloy   # composite: a tile list, no query of its own
data/                  # Parquet + CSV, private
publisher.json         # name, version, description
public/                # ONLY this directory is web-served
  index.html           # the HTML data app: markup + theme, one tab per dashboard
  app/                 # its modules: format, result, charts, table, drill,
                       #   controls, tabs, app
  embed-test.html      # a host page demoing Publisher.embed
  vendor/              # chart library, vendored rather than loaded from a CDN
    chart.umd.js
```

Which surface to change depends on what is being asked for. Add a measure or
view to `storefront.malloy` and every surface gets it. Add a tile to a
dashboard file for a governed, linked page; see
[`docs/dashboards.md`](../../docs/dashboards.md). Add a cell to the notebook for
an analysis narrative. Change `public/index.html` for a hand-authored web page.
[`docs/choosing-a-surface.md`](../../docs/choosing-a-surface.md) is the longer
comparison.

Only `public/` is reachable over the web, at
`/environments/<env>/packages/<pkg>/<file>`. The models, data, and
`publisher.json` are private and reached only through the query API, which still
applies the model's governance (givens, access modifiers, authorize rules). A
package is a data app simply by having a `public/` directory.

## Never guess a name

Use the model's real source, view, and field names. This model defines source
`order_items` (joined to `customers`, `products`, `regions`) with views
`top_products`, `by_category`, `category_performance`, `margin_by_category`,
`top_brands`, `brand_performance`, `top_customers`, `sales_by_state`,
`sales_by_region`, `sales_by_month`, `sales_by_year`, `seasonality`, `by_status`,
and `business_overview`, plus the model-level query `brand_suggest`. Read the file
before editing it, and compile-check every change (`malloy_compile`, or
`POST .../models/<path>/compile`) before wiring a result into anything.

## The data app runtime

Load it once per page with a root-relative tag, so it resolves whatever
environment or package the page is served under:

```html
<script src="/sdk/publisher.js"></script>
```

It adds one global, `window.Publisher`:

- `Publisher.query(modelPath, malloy, opts?)` returns `Promise<Array>` of plain
  row objects, for driving your own charts and tables.
- `Publisher.queryFull(modelPath, malloy, opts?)` returns the full Malloy result
  envelope: rows plus a `schema.fields` entry per column carrying that field's
  annotations. Hand it to a `<malloy-render>` element, or read the tags yourself.
- `Publisher.embed(selector, { src, token?, height?, allow? })` mounts a
  sandboxed, auto-resizing iframe and returns `{ iframe, destroy() }`.
- `Publisher.context` is `{ environment, package }`, inferred from the page URL.
- `Publisher.setToken(token | null)` sets a bearer token used by all later
  queries on the page; `null` reverts to cookies.

`modelPath` is the model FILE path within the package (`"storefront.malloy"`),
with `/` separators. It is not the source name. `opts` may carry `givens`,
`sourceName`, `queryName`, and `environment` / `package` (only for pages served
outside `/environments/<env>/packages/<pkg>/`).

## Query patterns

Run a named view:

```js
const rows = await Publisher.query("storefront.malloy", "run: order_items -> by_category");
```

Filter with [givens](../../docs/givens.md), which the server binds as typed
parameters:

```js
const rows = await Publisher.query("data_app.malloy", "run: scoped_orders -> by_category", {
  givens: { CATEGORY: "Outerwear", MIN_SALE: ">= 100" },
});
```

`scoped_orders` (in `data_app.malloy`) is `order_items` with the five givens
applied, so every tile of the data app shares one filter definition and the
values mean the same thing they mean on a dashboard. A `filter<T>` given takes
Malloy filter syntax as a string (`"Outerwear"`, `"Nike, Levi's"`, `">= 100"`,
`""` for all); other types take the plain value. Givens are per-file: a query can
only reference the givens the file it runs against imports, which is why
`data_app.malloy` exists at all.

Prefer that over building a `where:` clause in JavaScript. If you do interpolate,
the values must come from trusted, constrained sources (a dropdown populated from
the model's own distinct values). Never interpolate free-text or untrusted input
into a `run:` string.

A single-row KPI query returns a one-element array; read element zero:

```js
const [kpis] = await Publisher.query(
  "storefront.malloy",
  "run: order_items -> { aggregate: total_sales, order_count }",
);
el.textContent = kpis.total_sales;   // the result is an array; kpis.total_sales, not rows.total_sales
```

A dashboard fires its tiles together:

```js
const [kpiRows, byCategory, byMonth] = await Promise.all([
  Publisher.query("storefront.malloy", "run: order_items -> { aggregate: total_sales }"),
  Publisher.query("storefront.malloy", "run: order_items -> by_category"),
  Publisher.query("storefront.malloy", "run: order_items -> sales_by_month"),
]);
```

Prefer defining the views in the model (one per tile, pre-aggregated and sorted)
over building long query strings in JS.

Validate a query before wiring its result into render code: POST it to a running
Publisher at `/api/v0/environments/<env>/packages/<pkg>/models/<modelPath>/query`
with body `{"compactJson":true,"query":"..."}`, or run the `Publisher.query` once
and log the rows. Malloy names result columns after the `group_by` / `aggregate`
field names (`group_by: category` gives a `category` column; `aggregate:
total_sales` gives a `total_sales` column), so confirm those names against real
output.

## How the data app is put together

`public/index.html` is markup and theme; the behaviour is in `public/app/`, one
module per job. The page hardcodes as little of the model as possible, and a
change here should keep it that way:

- **Tabs are dashboard slugs** (`app/tabs.js`). A `# drill { to=category }` can
  only land somewhere because a tab is named `category`. Renaming a dashboard
  file means renaming its tab.
- **Tables are metadata-driven** (`app/result.js`, `app/table.js`). Headings come
  from `# label`, formats from `# currency` / `# percent` / `# number=`, and
  right-alignment from Malloy's own `calculation` marker. Do not write a column
  heading into the page; add the tag to the model.
- **Drill is read off the result** (`app/drill.js`). Destinations come from the
  `# drill` tag on the dimension, so making a new column clickable is a model
  edit, not a page edit. Drillable cells carry the class `publisher-drill`, the
  same one the Console uses.
- **Controls are built from the given contracts** (`app/controls.js`), fetched
  from `GET /api/v0/environments/<env>/packages/<pkg>/models/data_app.malloy`.
  Adding a given to `givens.malloy` and importing it in `data_app.malloy` adds a
  widget.
- **State lives in the URL**: `?tab=<slug>` plus one parameter per given, the same
  shape a dashboard page uses.

`packages/app/tests/playwright/package-data-app.spec.ts` covers the tabs, the
control row, the drill affordance, the destination menu, and the URL contract. Run
it after changing anything in `public/`.

## When the page does not work

- 404 or "model not found": `modelPath` is the file path (`"storefront.malloy"`),
  not the source name.
- "source/view not defined": a view or source name was guessed; use the model's
  real names.
- Promise rejects with a message starting `Publisher.query:`: read `error.status`
  and `error.response` for the server's reason.
- Empty array when you expect rows: a filter value mismatch (case, spelling, or
  type, for example a `category` or `status` that doesn't exist). Copy the
  literal verbatim from the model; confirm with a distinct-values query.
- KPI shows `undefined`: the result is an array; read `rows[0].field`.
- Page not served: the file is not under `public/`.
- A dashboard or view you just added is not queryable by name: the package needs
  a reload (`malloy_reloadPackage`, or `GET .../packages/storefront?reload=true`).

## Embedding in another page

```html
<script src="https://your-publisher/sdk/publisher.js"></script>
<div id="dashboard"></div>
<script>
  const handle = Publisher.embed("#dashboard", {
    src: "https://your-publisher/environments/examples/packages/storefront/index.html",
  });
  // handle.destroy() removes the iframe and its listeners.
</script>
```

Omit `height` and the frame auto-sizes: the embedded page measures its own
content and posts its height; the host accepts that message only from the iframe
it created. The page only has to load `/sdk/publisher.js`. Same-origin embeds
authenticate with the browser's cookies; for a cross-origin embed, mint a
short-lived signed token server-side and pass it as `token` (the embedded page
authenticates with it). Never put a long-lived or admin token in client HTML.
`public/embed-test.html` is a live demo.

## Live reload

Run the server with `--watch-env <env>` (or `PUBLISHER_WATCH=<env>`). Editing a
`.malloy` file recompiles the package; editing a file under `public/` reloads
open pages on its own. Nothing to wire in the page.

## Security

Everything under `public/` is web-served as-is, so keep secrets out of it. Your
protection lives in the model and the database, behind the query API. Served HTML
is framable by any origin by default; set `PUBLISHER_FRAME_ANCESTORS` to restrict
embedding origins for any page that shows sensitive data.
[`docs/security-posture.md`](../../docs/security-posture.md) is the fuller
picture.
