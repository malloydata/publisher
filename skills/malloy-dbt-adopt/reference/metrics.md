# Translate dbt Metrics (Step 5)

> Every dbt metric type, what it becomes in Malloy, and the three that a measure cannot hold.

Work from `semantic_manifest.json`, never from the OSI export (see SKILL.md). Each metric carries
a `type`, a `type_params`, and an optional structured `filter`.

## Where a metric lands

A simple metric names its own semantic model in `type_params.metric_aggregation_params.semantic_model`. That is the source its measure goes on.

A ratio or derived metric names no model; follow its input metrics to theirs. If every input resolves to the same source, the measure goes there with plain references. If inputs span sources, the measure goes on the source of the first input and references the others **through a declared join**: `revenue - orders.order_cost`. Malloy's symmetric aggregates make that correct across a one-to-many join, which is precisely the case (`order_items` to `orders`) where hand-written SQL double-counts. If no direct join reaches the other source, do not guess a path - record the metric as unconverted.

## `type: simple`

`type_params.expr` is the thing being aggregated; when it is absent, the metric name *is* the column. `metric_aggregation_params.agg` picks the aggregate.

| `agg` | Malloy | Note |
|---|---|---|
| `sum` | `x.sum()` | |
| `sum` with `expr: "1"` | `count()` | dbt's idiom for counting rows |
| `sum` with a `CASE WHEN c THEN x ELSE 0 END` expr | `x.sum() { where: c }` | Equivalent, and states the intent |
| `count_distinct` | `count(x)` | Malloy's `count(x)` is already distinct |
| `count` | `count()` | |
| `average` | `x.avg()` | |
| `min` / `max` | `x.min()` / `x.max()` | |
| `median`, `percentile` | **nothing** | See § What a measure cannot hold |

If the metric name equals its column name, the column was renamed in the projection (`sources.md`); aggregate the renamed column (`order_total_raw.sum()`) and keep the metric's name for the measure.

## `type: simple` with a filter

`filter.where_filters[].where_sql_template` is a MetricFlow template:

```
{{ Dimension('order_id__order_total_dim') }} >= 20
```

Resolve it before emitting. The qualified name is `<entity path>__<dimension name>`; take the last segment, find that dimension on the metric's semantic model, and substitute its `expr` when it has one, else its own name. Then apply the binding's rename. The example resolves to:

```malloy
measure: large_orders is count() { where: order_total_raw >= 20 }
```

**This resolution step is the whole reason not to use the OSI export**, which emits the unresolved `order_id__order_total_dim` as if it were a column.

Drop a trailing `= true` on a boolean; `where: is_food_item` is the idiom. If a template contains a `TimeDimension`, `Entity`, or nested metric reference, do not improvise - record the metric as unconverted with the construct named.

When the aggregate already carries a `{ where: }` from a `CASE WHEN` expr, merge the conditions with `and` rather than nesting a second filter block.

## `type: ratio`

`numerator` and `denominator` are metric references. Guard the division:

```malloy
measure: food_revenue_pct is food_revenue / nullif(revenue, 0)
```

## `type: derived`

`type_params.expr` is arithmetic over aliased input metrics; `type_params.metrics` gives each input's `name` and `alias`. Substitute each alias with the resolved measure reference, then guard a bare division with `nullif`.

```
expr: revenue - cost   metrics: [{name: revenue}, {name: order_cost, alias: cost}]
-> measure: order_gross_profit is revenue - orders.order_cost
```

**Check every input for `offset_window` or `offset_to_grain` first.** If any input is offset, this is not a measure - see below.

## What a measure cannot hold

Three shapes need something other than a measure. Two are expressible in Malloy as views; one is a real gap. Emit the views, name the gap, and never substitute a different function for a metric the project specified.

**A derived metric with an `offset_window`** is a period-over-period comparison: the row for March must see February's value. That is a window, so it is a `calculate:` inside a view.

```malloy
view: revenue_growth_mom is {
  group_by: revenue_month is ordered_at.month
  aggregate: revenue
  calculate: revenue_prev_month is lag(revenue)
  calculate:
    # percent
    revenue_growth_mom is (revenue - lag(revenue)) / nullif(lag(revenue), 0)
  order_by: revenue_month
}
```

dbt returns percentage points (its expr multiplies by 100); a Malloy view returning a fraction with a `# percent` render tag displays the same thing. Say which convention you chose, because the raw values differ by 100x.

**A cumulative metric** is a running total: `calculate: c is sum_cumulative(revenue)` in a view ordered by the time dimension. Note that dbt's cumulative metrics join a time spine and honour `cumulative_type_params.period_agg` - `first` reports the total as of the period's *first* day, so dbt's monthly rows and a month-end running total describe the same series at different instants. Reconcile on the terminal value, and state the convention difference.

**A median or percentile** is a genuine gap in this Malloy build: there is no scalar `median`, and raw-SQL aggregates (`percentile_cont!`, `sql_number(...) { is_aggregate: true }`, `# is_aggregate`) all resolve as scalars and fail with *"Cannot use a scalar field in a measure declaration."* Do not burn cycles on variations. Defer the metric with a documented gap, tell the user, and **do not silently ship `avg` in its place**. `stddev` does work if the question is really about spread. See `skill:malloy-gotchas-modeling`.

## Emit measures complete

A measure is not finished without its `#(doc)` (dbt's `description`) and its render tag. Carry `metric.label` as `# label="..."` only when it differs from the name. Add `# currency` or `# percent` from the metric's meaning - dbt does not record display formatting, so this is a judgment call worth stating rather than guessing silently.
