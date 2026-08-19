# dbt + Malloy: jaffle-shop, two ways

Two Malloy packages over dbt's [jaffle-shop](https://github.com/dbt-labs/jaffle-shop). **Both are
the same semantic layer.** They differ only in what sits underneath it:

| Package | Underneath | The motion |
|---|---|---|
| `adopt/` | the marts **dbt built** (Parquet), dbt pipeline untouched | Adopt: sit on top of what exists |
| `convert/` | marts **Malloy rebuilt** from the raw data, materialized with `#@ persist` | Convert: move the pipeline |

The point of shipping both is that the interface does not change. The same query, naming the same
sources and measures, runs against either package:

```malloy
run: orders -> { aggregate: order_total, orders, large_orders, food_orders }
```

```
adopt    105826.18          9568   1141   2336
convert  105826.18000000007 9568   1141   2336
```

Integers match exactly. Money differs in the last few decimal places, in whichever direction the
summation order lands, because `convert` computes dollars from cents in floating point while dbt
casts to `numeric(16,2)` — one of three differences catalogued in [COVERAGE.md](COVERAGE.md). Where the tables come from is a plumbing
decision the people and agents querying the model never see.

Everything runs with no warehouse, no credentials, and no dbt installed. DuckDB reads the
committed Parquet in place.

```bash
bun run start
# environment `examples`: jaffle-shop-adopt and jaffle-shop-convert
```

Start with [`adopt/jaffle_shop.malloynb`](adopt/jaffle_shop.malloynb). It states the number dbt's
own engine returns beside each Malloy query.

## Where to start: `order_items -> overview`

`order_items` is the entry point. It is the finest grain and it reaches everything: `products`
directly, and `customers` and `locations` through `orders`. Its `overview` view is a
`# dashboard` — revenue KPIs, the monthly trend, product mix, top sellers, and revenue by
location and customer type in one query.

```malloy
run: order_items -> overview
```

The other five sources are entry points too, each with its own measures and views: `orders` for
order-level questions, `customers` for lifetime value, and `products` / `locations` / `supplies`
as catalogs.

**dbt's metrics alone do not make a usable model.** dbt's semantic layer gives you 23 metrics and
3 saved queries, and no indication of where to start, how anything should render, or which
questions matter. The measures here are dbt's; the chart tags (`# currency`, `# percent`,
`# bar_chart`, `# line_chart`), the analysis views, and the dashboard are additions, because dbt
records no display formatting and its saved queries cover three questions. That authoring step is
part of the conversion, not an optional polish pass.

Both packages declare `explores: ["jaffle_shop.malloy"]`, so the staging and mart plumbing in
`convert/` stays out of listings. Each package shows the same six sources and nothing else.

## The short version

**23 dbt metrics: 20 became Malloy measures and all 20 match dbt's output exactly.** Two became
views because a measure cannot hold a window. One is deferred. dbt's 27 data tests and 3 unit
tests are *not* carried over. Full accounting in [COVERAGE.md](COVERAGE.md).

## `adopt/` — the semantic layer over dbt's marts

The default and the recommended shape. dbt keeps the transformations and the tests; Malloy
becomes the layer analysts and agents query, with no warehouse changes.

`jaffle_shop.malloy` holds six sources, one per dbt mart. Each reads its table, projects every
column dbt built carrying dbt's own description as `#(doc)`, then extends that with the keys,
joins, measures, and views:

```malloy
#(doc) Order overview data mart ... One row per order.
source: orders is duckdb.table('data/orders.parquet') -> {
  select:
    #(doc) The unique key of the orders mart.
    order_id
    ...
} extend {
  primary_key: order_id
  join_one: customers with customer_id
  measure: order_total is order_total_raw.sum()
  view: order_metrics is { ... }
}
```

Written from dbt's `target/` artifacts: `semantic_manifest.json` for entities, dimensions,
metrics, and saved queries; `manifest.json` for the descriptions; `catalog.json` for the column
list. **Not** from `osi_document.json` — see COVERAGE.md for the two ways that export produces
wrong numbers.

What the conversion carries:

- **Documentation** — every dbt model and column description, authored once in `schema.yml`.
- **Joins with declared cardinality** — dbt entities name their targets by entity name, so
  there is no join-key guessing.
- **Metric definitions, including the awkward ones** — filtered metrics, ratios, and
  `order_gross_profit`, a derived metric subtracting a cost on `orders` from revenue on
  `order_items` across a one-to-many join. It matches dbt to the cent.
- **The questions people ask** — dbt's three `saved_queries` became views.

## `convert/` — the same layer, over marts Malloy builds

Three files, one per job:

```
staging.malloy      stg_orders, stg_customers, ...     renames and casts over the raw Parquet
marts.malloy        orders_mart, customers_mart, ...   #@ persist; the table layer
jaffle_shop.malloy  orders, customers, ...             the semantic layer (same text as adopt/)
```

What dbt expresses as a `+materialized: table` config plus a model file is one annotation:

```malloy
#@ persist name="orders"
source: orders_mart is stg_orders extend { ... } -> { ... }
```

Including the part that looks like it needs SQL: dbt's `orders.sql` closes with
`row_number() over (partition by customer_id order by ordered_at)`, which is a `calculate:`.

The built table and the semantic source are deliberately separate (`orders_mart` vs `orders`).
That is not cosmetic — `customers_mart` is built *from* `orders_mart`, while the semantic
`orders` joins `customers`. Collapsing the two would make that a cycle.

A standalone Publisher does not build on publish. Trigger it:

```bash
malloy-pub materialize --environment examples --package jaffle-shop-convert --wait
```

All six marts build (`COPY`, 1–23ms each on this sample) and queries then read the built tables.

**`convert/` is a demonstration, not a recommendation.** It rebuilds tested marts, and the tests
do not come with them. See COVERAGE.md for the three real differences from dbt's output.

## The data

`dbt build` was run on a deterministic 150-customer sample of jaffle-shop and its output
committed as Parquet: 9,568 orders and 14,250 order items over 12 months (2024-09-01 to
2025-08-31), so the month-over-month and cumulative metrics have something real to say. The full
jafgen dataset is ~16MB of CSV; this is ~1.6MB of Parquet. `dbt build` is green on the sample: 43
passed, all 27 data tests and 3 unit tests included.

`adopt/data/` holds the marts dbt built. `convert/data/` holds the raw inputs dbt starts from.

To reproduce from scratch: clone jaffle-shop, sample the seeds in `seeds/jaffle-data/` down to
150 customers (keeping their orders and those orders' items, and all products, stores, and
supplies), then `dbt seed --vars 'load_source_data: true' && dbt build && dbt docs generate` on
a DuckDB profile, and export `main.*` and `raw.*` to Parquet.

## Doing this to your own dbt project

Two skills, one per motion:

- [`malloy-dbt-adopt`](../../skills/malloy-dbt-adopt/SKILL.md) — which artifacts to read, the
  source shape that carries dbt's docs, the two naming collisions, every metric type, and how to
  reconcile against dbt's own engine.
- [`malloy-dbt-convert`](../../skills/malloy-dbt-convert/SKILL.md) — moving staging and marts
  into Malloy with `#@ persist`, the four traps that compile clean and return wrong values, and
  what leaves the building with the dbt model.
