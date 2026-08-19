# Coverage: what converted, what did not, and what differs

Every claim here was checked by running both sides on the same data. The dbt side is dbt's own
engine (MetricFlow 0.13.0 via `mf query`), not SQL rewritten by hand.

Sample: the 150-customer subset committed in this example — 9,568 orders, 14,250 order items,
2024-09-01 to 2025-08-31. dbt-core 1.12.2, dbt-duckdb 1.11.0, jaffle_shop 3.0.0. `dbt build` is
green on it: 43 passed, including all 27 data tests and 3 unit tests.

## Model coverage

| dbt model | Malloy | Status |
|---|---|---|
| `orders` | `orders` | converted |
| `order_items` | `order_items` | converted |
| `customers` | `customers` | converted |
| `products` | `products` | converted |
| `supplies` | `supplies` | converted |
| `locations` | `locations` | converted |
| `stg_*` (6 models) | — | skipped in the adopt packages: staging output is already in the marts. Rebuilt in `convert/` instead |
| `metricflow_time_spine` | — | skipped: scaffolding for cumulative metrics; Malloy truncates timestamps natively |

## Metric coverage: 20 measures, 2 views, 1 deferred

All 23 metrics in dbt's semantic manifest are accounted for. **20 became measures and every one
matches dbt's own output** — verified by running each measure against `mf query` on the same
data; see `adopt-rich/jaffle_shop.malloynb`, which states dbt's number beside each query.

| dbt metric | type | Malloy | Verdict |
|---|---|---|---|
| `order_total`, `orders`, `tax_paid`, `order_cost` | simple | measures on `orders` | match |
| `new_customer_orders`, `large_orders`, `food_orders`, `drink_orders` | simple + filter | measures on `orders` | match |
| `revenue`, `food_revenue`, `drink_revenue` | simple | measures on `order_items` | match |
| `food_revenue_pct`, `drink_revenue_pct` | ratio | measures on `order_items` | match |
| `order_gross_profit` | derived, cross-model | measure on `order_items` | match |
| `customers`, `count_lifetime_orders`, `lifetime_spend_pretax`, `lifetime_spend` | simple | measures on `customers` | match |
| `average_order_value` | derived | measure on `customers` | match |
| `average_tax_rate` | simple | measure on `locations` | match |
| `revenue_growth_mom` | derived + offset | **view** on `order_items` | same digits, different scaling — below |
| `cumulative_revenue` | cumulative | **view** on `order_items` | terminal value ties out — below |
| `median_revenue` | simple (median) | — | **deferred** — below |

### The three that are not measures

**`median_revenue` — deferred, not substituted.** dbt returns 6.0. There is no scalar `median` in
this Malloy build, and every documented raw-SQL aggregate escape (`percentile_cont!(x, 0.5)`,
`sql_number(...) { is_aggregate: true }`, the `# is_aggregate` annotation) resolves as a *scalar*
and fails with "Cannot use a scalar field in a measure declaration." Shipping `avg` in its place
would be a different number wearing the same name, so the metric is absent and named here
instead. (`stddev` does work, if the question is really about spread.)

**`revenue_growth_mom` — a view, and off by a factor of 100 by choice.** dbt's expression
multiplies by 100, returning percentage points: **77.43409490333919** for 2025-03. The Malloy
view returns **0.7743409490333919** — identical digits — and carries a `# percent` render tag, so
it *displays* as 77.43%. The raw values differ by 100x; the rendered values agree.

**`cumulative_revenue` — a view, comparable at its terminal value.** The running total ends at
**100441**, exactly dbt's all-time revenue. Its intermediate monthly rows are *not* directly
comparable: dbt joins a day-grain time spine and its `cumulative_type_params.period_agg` is
`first`, so dbt reports the total as of each month's **first** day (2025-07 → 74426) while the
view reports it as of the month's **last** day (2025-06 → 74115). Same series, different
reporting instant.

## Join coverage

| Relationship | dbt | Malloy |
|---|---|---|
| `orders` → `customers` | entity `customer` | `join_one`, backed by a `relationships` test |
| `orders` → `locations` | entity `location` | `join_one` |
| `order_items` → `orders` | entity `order_id` | `join_one`, backed by a `relationships` test |
| `order_items` → `products` | entity `product` | `join_one` |
| `supplies` → `products` | **not declared** | **not emitted** — implied by SKU, but dbt's semantic layer does not declare it, so it was not invented |

## Tests: not carried over

dbt asserts **27 data tests and 3 unit tests** over these marts — `not_null`, `unique`,
`relationships`, `accepted_values`, and `dbt_utils.expression_is_true`. **None are carried into
Malloy**, which has no test framework today. The Malloy layer inherits the marts' correctness; it
does not re-establish it. `dbt build` (or `dbt test`) still has to run.

This is the clearest place dbt currently leads, and the reason this example models *on top of*
dbt's marts rather than replacing them by default.

## Dropped as mechanical

**`orders.order_total_dim`** — a dbt dimension whose `expr` is just its own column
`order_total`. It exists so MetricFlow has a named dimension to filter on. Malloy filters the
column directly, so the alias is dropped and the `large_orders` filter resolved through it to
`order_total_raw >= 20`.

## Naming: six columns renamed

Six dbt metrics share a name with the column they aggregate (`order_total`, `tax_paid`,
`order_cost`, `count_lifetime_orders`, `lifetime_spend_pretax`, `lifetime_spend`). Malloy rejects
a measure that redefines an existing field name ("Cannot redefine 'order_total'"), so the
**projection renames the passthrough column** to `<name>_raw` and the measure keeps dbt's
name. Metric names are what analysts and agents search for, so the metric won the name.

## Why not the OSI export

dbt 1.12 also writes `target/osi_document.json` (Open Semantic Interchange). It is vendor-neutral
and carries datasets, resolved `relationships`, and metrics — genuinely attractive as a converter
input. It is also **lossy in two ways that produce wrong numbers**, both observed on this project:

1. **Filtered metrics reference names that are not columns.** `large_orders` becomes
   `SUM(CASE WHEN order_id__order_total_dim >= 20 THEN 1 END)`. `order_id__order_total_dim` is
   MetricFlow's internal qualified dimension name; the real column is `order_total`. The SQL does
   not run.
2. **`offset_window` is silently dropped.** `revenue_growth_mom` becomes
   `(SUM(order_items.product_price) - SUM(order_items.product_price))*100/SUM(order_items.product_price)`
   — identically zero. dbt emits a warning for `cumulative_revenue` losing meaning in OSI, but
   **no warning for this one**.

`semantic_manifest.json` keeps the structured `filter` and the `offset_window`, so that is what
the model was written from.

## `convert/`: differences from dbt's marts

`convert/` rebuilds dbt's staging and marts in Malloy over the same raw data. All six marts
agree with dbt's built marts on row counts and every aggregate compared (25 values). Three
differences are real and deliberate:

**Money columns are floating point.** dbt's `cents_to_dollars` macro casts to `numeric(16,2)`;
Malloy uses `round(x / 100, 2)` on a double. Sums agree to about 1e-9 relative but are not
bit-identical: `sum(order_total)` comes back as `105826.18000000007` or
`105826.17999999996` depending on summation order, against dbt's exact `105826.18`. For money that matters, keep the cast in dbt.

**`supply_uuid` is a different value.** dbt builds it with
`dbt_utils.generate_surrogate_key(['id','sku'])`, an md5 over the concatenated parts. The Malloy
version uses the concatenation directly. Both are unique keys and nothing joins across the two
layers on it, but the column's values differ by construction.

**77 orders have no items, and the two layers describe them differently.** dbt's `orders.sql`
left-joins an item summary, so those orders get **NULL** for `count_order_items` and NULL for the
`is_food_order` / `is_drink_order` booleans. Malloy's `count(order_items.order_item_id)` returns
**0**, so the booleans are `false`. The sums agree (both total 14,250 items); the per-row values
differ for those 77 orders.

> This one is worth reading twice, because the first version of `convert/marts.malloy` used
> `order_items.count()` and summed to **14,327** — 77 too many, one per itemless order, because
> `count()` counts the outer-join row. Comparing against dbt's marts is what caught it. That is
> the entire argument for reconciling against the incumbent rather than eyeballing the output.

## Added beyond dbt

Three things in these models did not come from dbt, and should not be read as converted:

- **Render tags.** `# currency`, `# percent`, `# bar_chart`, `# line_chart`, `# dashboard`,
  `# big_value`. dbt's semantic layer records no display formatting, so every one of these is a
  judgment call made here.
- **The analysis views and the `overview` dashboard.** dbt ships three `saved_queries`, which
  became `order_metrics`, `revenue_metrics`, and `customer_order_metrics`. Everything else
  (`revenue_by_month`, `revenue_by_product_type`, `top_products`, `revenue_by_location`,
  `revenue_by_customer_type`, `orders_by_month`, `order_mix`, `orders_by_location`,
  `customers_by_type`, `top_customers`, `by_type`, `tax_rate_by_location`, `top_supplies`,
  `overview`) was authored here.
- **Four measures.** `order_item_count`, `product_count`, `supply_count`, and
  `total_supply_cost`, so every source has something to say when someone opens it. dbt declares
  no metrics at all on `products` or `supplies`.

The 20 measures that reconcile against MetricFlow are dbt's, unchanged. These additions sit
alongside them and do not affect those numbers.

## Known Malloy issue

`run: order_items -> overview + { limit: 2 }` fails with `Parser Error: syntax error at or near
")"`: the limit refinement reaches a nested dashboard and emits
`ROW_NUMBER() OVER (PARTITION BY group_set ORDER BY )`. The dashboard itself runs fine; only a
limit refinement on it breaks. Worth reporting upstream.

## The authorize / materialization constraint

`adopt-rich` gates `order_items_margin` with `#(authorize) "$role = 'finance'"`; a caller asserting
the default `analyst` role is refused with HTTP 403, verified against a running Publisher.

`convert` deliberately omits that source. Publisher **refuses to materialize** a package where an
`#(authorize)`-gated source sits in a persisted lineage, with this error:

> An authorize expression is evaluated per request; a materialized-once table served frozen carries
> no gate, so it would be served to everyone, bypassing authorization. This is refused for safety.

That is the correct call, and it is a real design constraint rather than a rough edge: a per-request
gate and a build-once table are incompatible by construction. Gate a source that is not
materialized, or scope rows with a `where:` over a given instead. It is the one place the semantic
layer is not portable between the two foundations.

## Package comparison

| | `adopt-mechanical` | `adopt-rich` | `convert` |
|---|---|---|---|
| Reads | dbt's marts | dbt's marts | raw files |
| dbt metrics reconciled | 20/20 | 20/20 | 20/20 (money to ~1e-9) |
| Views | 3 (dbt's saved queries) | 20 | 20 |
| Chart / format tags | 0 | yes | yes |
| Dimensions added | 0 | cohorts, spend and size bands, repeat-buyer flag | same |
| Window entities | 0 | 4 | 4 |
| Audience extensions | 0 | 2 (one gated) | 1 (gate not permitted) |
| Tables built | none | none | 3 |
