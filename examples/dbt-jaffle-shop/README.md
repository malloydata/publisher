# dbt + Malloy: jaffle-shop, three ways

Three Malloy packages over dbt's [jaffle-shop](https://github.com/dbt-labs/jaffle-shop). They vary
two things independently, one at a time, so each comparison has a single variable:

| Package | Model | Transformation | What the comparison shows |
|---|---|---|---|
| `adopt-mechanical/` | mechanical | dbt's | the ceiling of automated conversion |
| `adopt-rich/` | rich | dbt's | **diff vs above = modelling only** |
| `convert/` | rich | Malloy's | **diff vs above = plumbing only** |

Everything runs with no warehouse, no credentials, and no dbt installed. DuckDB reads the
committed Parquet in place.

```bash
bun run start
# environment `examples`: jaffle-shop-mechanical, jaffle-shop-adopt, jaffle-shop-convert
```

## mechanical → rich: what does modelling buy?

`adopt-mechanical` is a faithful, complete conversion of dbt's semantic layer: every model becomes
a source, every column doc a `#(doc)`, every entity a key or a join, every metric a measure, every
saved query a view. 20 of dbt's 23 metrics, and **all 20 match dbt's own output exactly**. Nothing
was skipped out of laziness.

It is also close to unusable: no entry point, no chart or format tags, money rendering as bare
floats, three of six sources with no measures at all, and dimension tables that are two-column dead
ends when opened. That is not a criticism of the converter. It is the ceiling of what *any*
automated translation reaches, because dbt's YAML does not carry the missing information.

`adopt-rich` is the same six marts and the same 20 reconciled measures, plus the modelling:

- **Cohorts and bands** — `first_order_month`, `spend_band`, `order_size_band`, `is_repeat_buyer`,
  each with its thresholds flagged in its `#(doc)` as a convention rather than a fact.
- **Windows as model entities** — `revenue_growth_mom`, `cumulative_revenue`,
  `avg_order_value_trend`, and `product_leaderboard` with a rank. Not per-chart calculations: views
  that other queries can nest and refine.
- **An entry point** — `order_items -> overview`, a `# dashboard` whose doc says to start there.
- **Render tags** — `# currency`, `# percent`, and a chart type per view. dbt records none of this.
- **Audience extensions** — one base model surfaced differently. `order_items_store_ops` drops
  financial detail; `order_items_margin` adds margin and is gated with
  `#(authorize) "$role = 'finance'"`, so a caller asserting any other role gets **HTTP 403**.

Three of dbt's own definitions are ambiguous enough to produce wrong business answers, and a
faithful conversion preserves the ambiguity perfectly. "What was our revenue?" has two defensible
answers 5.4% apart (tax). "What share of orders are food?" sums to 122%, because dbt's food and
drink flags overlap on 2,164 orders and omit 77. "Revenue from new customers?" answers **$6.36**
via `customer_type` or **$2,586.13** via first orders, a 400x difference, because dbt ships both
definitions and reconciles neither. Every one of those numbers matches dbt exactly, so
reconciliation cannot catch them. The rich packages settle all three; see
[COVERAGE.md](COVERAGE.md).

The useful test is not "is Malloy nicer" but: **can this construct be a reusable, composable model
entity, or does it only exist inside one chart?** A running total, a cohort definition, a bucketing
rule, an audience variant. Where those can only live in a chart or a UI side-car, the model is a
metric catalogue and the analysis lives outside it.

## rich → convert: where does the transformation live?

`convert/` is the same semantic layer — same sources, measures, dimensions, views — reading what
Malloy derives from the raw files instead of what dbt built. The interface does not change:

```malloy
run: orders -> { aggregate: order_total, orders, large_orders, food_orders }
```

```
adopt-mechanical  105826.18          9568  1141  2336
adopt-rich        105826.18          9568  1141  2336
convert           105826.17999999982 9568  1141  2336
```

Integers match exactly. Money differs in the last few decimal places, in whichever direction the
summation order lands, because `convert` computes dollars from cents in floating point while dbt
casts to `numeric(16,2)`.

**There is no marts layer.** dbt splits this into staging views and mart tables; `pipeline.malloy`
is one transformation, and `#@ persist` is a latency knob applied where derivation is expensive.
Only `order_items`, `orders`, and `customers` build a table. `products`, `supplies`, and
`locations` build nothing, because a rename does not earn one.

```bash
malloy-pub materialize --environment examples --package jaffle-shop-convert --wait
# builds 3 tables, not 6
```

`convert/` rebuilds tested marts and dbt's tests do not come with them, so it demonstrates what
moving the pipeline costs and buys rather than recommending it.

## Two constraints worth knowing before copying this

**An `#(authorize)` gate cannot live in a materialized lineage.** `adopt-rich` has the
finance-gated margin source; `convert` deliberately does not. Publisher refuses to build a package
where a gated source sits in a persisted lineage, because an authorize expression is evaluated per
request while a materialized table served frozen carries no gate. It fails closed with that
explanation rather than quietly serving ungated rows.

**Build and semantics stay separate in `convert`.** `customers_built` is derived *from*
`orders_built`, while the semantic `orders` joins `customers`. Fusing them into one source is a
cycle Malloy rejects.

## The data apps

Each package serves a hand-authored dashboard from its `public/` directory, at
`/environments/examples/packages/<pkg>/index.html`. Same six tiles in all three, so the pages are
the comparison rendered:

| | `adopt-mechanical` | `adopt-rich` / `convert` |
|---|---|---|
| Revenue by month | page writes the monthly grouping | `revenue_by_month` view |
| Month-over-month | page computes it in JavaScript | `revenue_growth_mom` view |
| Food vs drink | page writes the join path | `revenue_by_product_type` view |
| Top products | page picks the cutoff | `top_products` view |
| Cohorts | shows the tenure-confounded total, with a note explaining that no correction exists | `spend_per_tenure_month` alongside the raw total |

The numbers agree everywhere they exist: the page-computed month-over-month in the mechanical app
matches the model-defined measure in the rich one. What differs is where the analysis lives, and
therefore who inherits it.

`convert/public/lib/` is **byte-identical** to `adopt-rich/public/lib/` -- same tiles, same view
names, same queries. The app cannot tell whether its marts were built by dbt or by Malloy, which is
the portability claim as a diff rather than an assertion.

No chart library is loaded, from a CDN or otherwise: the charts are inline SVG, because embedded
page JavaScript runs with the viewing user's data authority.

## The data

`dbt build` was run on a deterministic 150-customer sample of jaffle-shop and its output committed
as Parquet: 9,568 orders and 14,250 order items over 12 months (2024-09-01 to 2025-08-31), so the
month-over-month, cohort, and cumulative work has something real to say. `dbt build` is green on
the sample: 43 passed, including all 27 data tests and 3 unit tests.

`adopt-mechanical/data/` and `adopt-rich/data/` hold the marts dbt built; `convert/data/` holds the
raw inputs dbt starts from.

To reproduce: clone jaffle-shop, sample `seeds/jaffle-data/` down to 150 customers (keeping their
orders, those orders' items, and all products, stores, and supplies), then
`dbt seed --vars 'load_source_data: true' && dbt build && dbt docs generate` on a DuckDB profile,
and export `main.*` and `raw.*` to Parquet.

Full metric-by-metric accounting, and every difference from dbt's numbers, in
[COVERAGE.md](COVERAGE.md).

## Doing this to your own dbt project

- [`malloy-dbt-adopt`](../../skills/malloy-dbt-adopt/SKILL.md) — which artifacts to read, the source
  shape that carries dbt's docs, the two naming collisions, every metric type, reconciling against
  dbt's own engine, and why a converted model is not yet a usable one.
- [`malloy-dbt-convert`](../../skills/malloy-dbt-convert/SKILL.md) — moving the transformation into
  Malloy, `#@ persist` as a latency decision, the four traps that compile clean and return wrong
  values, and what leaves the building with the dbt model.
