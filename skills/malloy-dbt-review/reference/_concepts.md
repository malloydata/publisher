# dbt to Malloy Concept Mapping

Shared reference for `propose-fields.md`, `build-bindings.md`, and `build-metrics.md`.

## Structure

| dbt | Malloy | Notes |
|---|---|---|
| `model` (mart) | `source` over the built relation | One source per mart. Name it after the dbt model. |
| `semantic_model.node_relation` | the table in `duckdb.table(...)` / `conn.table(...)` | The only place a dataset is named; see `build-bindings.md`. |
| model `description` | `#(doc)` on the source | |
| column `description` | `#(doc)` on the field | Only lands where the field is declared; see `build-bindings.md`. |
| `entity: {type: primary}` | `primary_key:` | Use the entity's `expr` when set, else its `name`. |
| `entity: {type: foreign}` | `join_one: <target> with <local column>` | The target is the semantic model declaring the **same entity name** as `primary`. |
| `entity: {type: unique/natural}` | nothing | Not a join. Note it and move on. |
| `dimension: {type: categorical}` with a real `expr` | `dimension:` | |
| `dimension: {type: categorical}` aliasing its own column | nothing | MetricFlow scaffolding. Drop it, resolve filters through it. |
| `dimension: {type: time}` | nothing | Malloy truncates natively: `ordered_at.month`. |
| `defaults.agg_time_dimension` | the column a `metric_time` group-by resolves to | |
| `metric` | `measure:`, or a view - see `build-metrics.md` | |
| `metric.label` | `# label="..."` | Only emit when it differs from the name. |
| `saved_query` | `view:` | Emit on the source where every referenced metric resolves. |
| `saved_query.exports` | nothing | dbt's write-back target, not a semantic concept. |
| `data_tests`, `unit_tests` | nothing | Stays dbt's job. Inventory it; see SKILL.md § Governance. |
| `+materialized:`, incremental config | `#@ persist` if warranted | Not a translation. See `skill:malloy-materialization`. |
| `metricflow_time_spine` | nothing | Scaffolding for cumulative metrics. |

## Expressions

dbt metric and dimension `expr` values are SQL. Translate, don't paste.

| dbt / SQL | Malloy |
|---|---|
| `COUNT(*)` | `count()` |
| `COUNT(DISTINCT x)` | `count(x)` - Malloy's `count(x)` is already distinct |
| `SUM(x)` | `x.sum()` |
| `AVG(x)` | `x.avg()` |
| `SUM(CASE WHEN c THEN x ELSE 0 END)` | `x.sum() { where: c }` - equivalent, and reads as what it means |
| `CASE WHEN ... THEN ... ELSE ... END` | `pick ... when ... else ...` |
| `COALESCE(a, b)` | `a ?? b` |
| `CAST(x AS t)` | `x::t` |
| `x IN ('a','b')` | `x ? 'a' \| 'b'` |
| `(x / 100)::numeric(16,2)` (a `cents_to_dollars` macro) | `round(x / 100, 2)` |
| `md5(...)` surrogate key | `concat(...)` of the same parts, or a raw-SQL escape - see `absorb-transforms.md` |
| `= true` on a boolean | the bare column: `where: is_food_item` |

Numeric division must be guarded: `a / nullif(b, 0)`.

## Naming

Two collisions come up on essentially every real dbt project. Both are compile errors, not style preferences.

**A field cannot reference itself.** `dimension: order_total is order_total` fails. A dbt column description therefore cannot be attached to a passthrough column by redeclaring it under the same name.

**A measure cannot take the name of an existing column.** A dbt metric named `order_total` that sums the column `order_total` fails with *"Cannot redefine 'order_total'"*. Something has to move. Prefer keeping the **metric** name, because that is the name analysts and agents search for, and rename the passthrough column in the binding layer instead (`build-bindings.md`). Whichever way you resolve it, resolve it the same way everywhere and say so in your notes.
