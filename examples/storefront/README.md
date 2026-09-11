<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# storefront — the flagship sample package

A small but complete ecommerce semantic model. It's the default package Publisher
serves out of the box, and the one the [React SDK example](../data-app) reads from.

Everything runs on local DuckDB files, Parquet and CSV side by side, read in place with
no conversion step. **No credentials required.**

## What's here

| File | Role |
| --- | --- |
| `data/customers.parquet` | 1,000 customers across all 50 states (id, name, state, city, signup date). |
| `data/products.parquet` | 200 products in 10 categories and 12 brands (id, name, category, brand, cost, retail price). |
| `data/order_items.parquet` | ~25,000 order lines (~11,000 orders) over three years, joining customers to products. |
| `data/regions.csv` | The 50 states mapped to a sales region. A CSV, not Parquet: it's the kind of small lookup you'd keep in a spreadsheet, and `duckdb.table()` reads either format. |
| `storefront.malloy` | The model: `order_items` fact joined to `customers`, `products`, and `regions`, with reusable measures and `# dashboard` views. |
| `storefront.malloynb` | A guided-tour notebook: the business overview dashboard plus growth, seasonality, geography, category, brand, and top-seller views. |
| `givens.malloy` | The data app's filter controls, declared as `given:` parameters with the tags that say how each one renders. |
| `data_app.malloy` | `scoped_orders`: `order_items` narrowed by those givens. Every tile on the page queries it. |
| `dashboards/overview.malloy` | A [dashboard](../../docs/dashboards.md): `## artifact { tiles=[…] }` naming views off `scoped_orders`, laid out by `# colspan` and `# break` on each. Served at `/examples/storefront/dashboards/overview`. The same figures as the model's `business_overview` view, which is the one-query form of the same page. |
| `public/index.html` | A no-build [HTML data app](../../docs/html-data-apps.md): a four-tab Chart.js dashboard. Served at `/environments/examples/packages/storefront/`. |
| `public/app/` | The page's ES modules (state and rendering, controls, charts, tables, formatting). No build step: the browser loads them directly. |
| `public/vendor/chart.umd.js` | Chart.js v4.5.0 (MIT), vendored so the page renders where a CDN is blocked. |
| `public/vendor/malloy-filter.js` | `@malloydata/malloy-filter`, bundled for the browser, so the page escapes filter values with Malloy's own printer. Regenerate with `bun run vendor:malloy-filter`. |
| `tests/` | `node --test` coverage (`bun run test:examples`): the filter encoding, and the controls themselves against a real DOM. |
| `eslint.config.mjs` | Lints `public/app/`, which nothing else reads: no bundler, no typechecker. Run by the root `bun run lint`. |

The data is generated deterministically by [`scripts/generate-example-data.mjs`](../../scripts/generate-example-data.mjs)
(`bun run generate:example-data`) — it has a growth trend and holiday seasonality, so the charts have
something real to show.

## The model at a glance

- **Sources:** `order_items` (fact) with `join_one` to `customers`, `products`, and
  `regions` (the CSV lookup, joined through the customer's state).
- **Measures:** `total_sales`, `total_margin`, `margin_rate`, `order_count`,
  `order_item_count`, `avg_order_value`, `customer_count`, `orders_per_customer`,
  `return_rate`, `percent_of_sales`.
- **Chart views:** `by_category` / `margin_by_category` / `top_brands` / `by_status`
  (`# bar_chart`), `sales_by_month` / `seasonality` (`# line_chart`),
  `sales_by_year` / `sales_by_region` (`# bar_chart`), `sales_by_state` (`# shape_map`).
- **Table views:** `top_products`, `top_customers`, `category_performance`, `brand_performance`.
- **Dashboard:** `business_overview` — `# big_value` KPI tiles + nested charts.

## The data app

`public/index.html` is a dashboard with four tabs (overview, category detail, regions, seasonality):
KPI tiles, charts, and tables, all served by Publisher and driven by `Publisher.queryFull` against
the model's views. No build step, no framework, no npm.

Its control row is not written by hand. The page reads the model's `given:` declarations from the
model metadata endpoint and renders the widget each one asks for, so adding a filter needs no edit to
`public/app/` at all: declare the `given:` in `givens.malloy` **and tag it with a control this page
draws**, import it in `data_app.malloy`, and add a clause naming it to `scoped_orders`' `where:`.

Two of those steps fail quietly, which is why they are worth naming. Without the `where:` clause the
control still renders and the server still accepts the value, because the given is declared, and
nothing is filtered. Without a control tag nothing renders at all: a given the page has no widget for
is skipped in silence. The tags it draws are `control=select`, `control=multiselect`, a `range_min`
and `range_max` pair on a `filter<number>` given, and a `date` type. The pair alone is not enough:
this page sends `>= n` filter syntax, so it draws a slider only for a given that takes a filter.

Picked values are bound as givens rather than pasted into query text, and the filter syntax a
`filter<string>` given takes is printed by Malloy's own filter library (vendored in
`public/vendor/`) rather than by an escaping rule this page maintains. See
[docs/html-data-apps.md](../../docs/html-data-apps.md) and [docs/givens.md](../../docs/givens.md).

![The storefront HTML data app](../../docs/screenshots/storefront-data-app.png)

## Try it

`storefront` ships in Publisher's default config, so with the server running just open
`http://localhost:4000` and pick the **storefront** package. To query it directly:

```bash
API=http://localhost:4000/api/v0/environments/examples/packages/storefront/models/storefront.malloy/query

curl -s -X POST $API -H 'content-type: application/json' \
  -d '{"query":"run: order_items -> top_products"}'

curl -s -X POST $API -H 'content-type: application/json' \
  -d '{"query":"run: order_items -> business_overview"}'
```

Or ask an AI agent over MCP: *"Use Malloy to chart storefront revenue by category."*

## Learn more

- [docs/console.md](../../docs/console.md) — navigate this package in the Publisher Console, the built-in web UI.
- [docs/explorer.md](../../docs/explorer.md) — explore the model with the no-code visual query builder.
- [docs/html-data-apps.md](../../docs/html-data-apps.md) — how `public/index.html` works.
- [docs/ai-agents.md](../../docs/ai-agents.md) — query this model from an AI agent over MCP.
- [examples/data-app](../data-app) — the React SDK app that reads from this package.
