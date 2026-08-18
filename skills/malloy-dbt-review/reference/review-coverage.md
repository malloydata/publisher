# Review Coverage Against dbt (Step 7)

> Compare the finished Malloy model against the dbt project it came from. Every gap gets a reason.
> A gap with a reason is a decision; a gap without one is an omission nobody noticed.

Present three tables and one statement about tests.

## 1. Model coverage

Every dbt model in scope, and what became of it.

| dbt model | Malloy source | Status |
|---|---|---|
| `orders` | `orders` | converted |
| `customers` | `customers` | converted |
| `stg_orders` | - | skipped: staging; its output is in `orders` |
| `metricflow_time_spine` | - | skipped: scaffolding for cumulative metrics |

## 2. Metric coverage

Every metric in `semantic_manifest.json`, with its outcome. Count them explicitly - "20 of 23
became measures, 2 became views, 1 deferred" is the honest headline, and it should agree with the
reconciliation tally.

| dbt metric | type | Malloy | Status |
|---|---|---|---|
| `order_total` | simple | `measure: order_total` | converted, matches |
| `large_orders` | simple + filter | `measure: large_orders` | converted, matches |
| `food_revenue_pct` | ratio | `measure: food_revenue_pct` | converted, matches |
| `order_gross_profit` | derived | `measure: order_gross_profit` | converted, matches (cross-source) |
| `revenue_growth_mom` | derived + offset | `view: revenue_growth_mom` | view, not a measure; convention differs |
| `cumulative_revenue` | cumulative | `view: cumulative_revenue` | view; terminal value ties out |
| `median_revenue` | simple (median) | - | deferred: no scalar median in this build |

## 3. Join coverage

Every relationship dbt declares, plus any the marts imply that dbt does not declare.

| Relationship | Declared by dbt | In Malloy | Note |
|---|---|---|---|
| `orders` to `customers` | entity `customer` | `join_one` | `relationships` test backs the cardinality |
| `order_items` to `orders` | entity `order_id` | `join_one` | |
| `supplies` to `products` | not declared | not emitted | implied by SKU; needs user confirmation |

## 4. Tests

State it plainly, with counts:

> dbt asserts 27 data tests and 3 unit tests over these marts. **None are carried into Malloy** -
> Malloy has no test framework today. The Malloy layer inherits the marts' correctness; it does not
> re-establish it, so `dbt build` (or `dbt test`) must keep running.

If any `join_one` has no `relationships` test behind it, list it here as an unverified cardinality
assumption.

## The claim you are allowed to make

A converted model is "the dbt semantic layer, queryable in Malloy" only when the metric table says
converted and the reconciliation says matches. Otherwise the claim is narrower, and the narrower
claim is the one to make. Specifically, do not describe the conversion as complete when:

- any metric is deferred (say which),
- reconciliation could not run because nothing was queryable (say so),
- a join's cardinality rests on a description rather than a test or a query.
