# dbt Discovery (Step 1)

> Inventory a dbt project, classify its models, and capture prior-art notes. Does NOT extract
> individual field definitions; that is deferred to `propose-fields.md`.

## 1. Locate the project and its artifacts

Look for `dbt_project.yml`. Read it for `name`, `version`, `model-paths`, `seed-paths`, and the
`models:` config block (which tells you which directories are views versus tables).

Then look for `target/`. Confirm what is present:

| File | Present? | If missing |
|---|---|---|
| `manifest.json` | required | Run `dbt build` (or at least `dbt parse`) |
| `catalog.json` | required for bindings | Run `dbt docs generate` |
| `semantic_manifest.json` | required for metrics | Only exists if the project defines semantic models/metrics |
| `osi_document.json` | ignore | Note it, do not read semantics from it (SKILL.md) |

Ask the user to confirm: "I found a dbt project. Use it as prior art?" And check the artifacts are
current - a `manifest.json` older than the last change to `models/` describes a project that no
longer exists. If you cannot tell, re-run `dbt build && dbt docs generate` or say the artifacts are
unverified.

If there is no `semantic_manifest.json`, the project has no semantic layer to convert. That is a
common and fine state: you still get model and column descriptions, the `ref()` graph, and the
tests. Proposals then come from the data, with dbt supplying documentation only.

## 2. Classify the models

From `manifest.json`, group models by their directory and materialization:

| Layer | Typical config | Treatment |
|---|---|---|
| **Marts** | `+materialized: table` | **Model on these.** The reviewed, tested output. |
| **Staging** | `+materialized: view`, one per raw source, renames and casts | Skip on a first pass. Its output is already in the marts. |
| **Intermediate** | ephemeral or view, joins between staging and marts | Skip; it is a build-order artifact. |
| **Scaffolding** | `metricflow_time_spine` and similar | Skip. Malloy truncates timestamps natively. |
| **Snapshots** | `snapshots/` | Note the SCD2 history; Malloy cannot maintain it today. |

For each mart, record: name, physical relation (`node_relation.relation_name`), description, row
grain as dbt states it, and whether a semantic model exists for it.

Verify the grain by query rather than trusting the description - a mart documented as "one row per
order" is an assertion, and `unique` + `not_null` tests on its key are the evidence. Cite the test
if it exists.

## 3. Extract the join graph from entities

This is the highest-value structural read, and it needs no guessing. In `semantic_manifest.json`,
each semantic model lists `entities`. Build the graph:

- `type: primary` - this model's key. The entity's **name** is the identity other models reference.
- `type: foreign` - a join. Find the model whose `primary` entity has the **same name**; that is the
  target. The local column is the foreign entity's `expr`, or its name when `expr` is null.

```
orders:      order_id (primary), customer (foreign, expr customer_id), location (foreign, expr location_id)
customers:   customer (primary, expr customer_id)
locations:   location (primary, expr location_id)
order_items: order_item (primary), order_id (foreign), product (foreign, expr product_id)

-> orders     join_one customers with customer_id
-> orders     join_one locations with location_id
-> order_items join_one orders    with order_id
-> order_items join_one products  with product_id
```

Note any model with no foreign entity reaching it. dbt's semantic layer may simply not declare a
join that the marts imply (a supplies table related to products by SKU, with no entity declared).
Do not invent it: record it as an available-but-undeclared relationship for the user to confirm.

## 4. Inventory the metrics

Count metrics by `type` (`simple`, `ratio`, `derived`, `cumulative`, `conversion`) and flag up
front the ones that will need special handling, so the scope proposal is honest:

- any `derived` metric with an input carrying `offset_window` or `offset_to_grain`
- any `cumulative` metric
- any `simple` metric with `agg: median` or `percentile`
- any metric whose `filter` uses `TimeDimension`, `Entity`, or a nested metric reference

Also list the `saved_queries`: they are the questions the business already asks, and they become
views.

## 5. Inventory the governance

From `manifest.json`, collect every test attached to the models in scope: `not_null`, `unique`,
`relationships`, `accepted_values`, `dbt_utils.*`, and any `unit_tests`. You are not converting
these. You are recording them, for two reasons: a `relationships` test is the evidence behind a
join's cardinality, and the whole set is what you must tell the user stays dbt's job (SKILL.md
§ Governance).

## 6. Capture prior-art notes

Record, for the scope proposal:

- dbt project name, version, and the git ref or manifest hash the artifacts came from
- marts in scope, and the skip list with a reason per entry
- the join graph, marked as declared-by-dbt versus inferred
- metric counts by type, with the special-handling flags
- the test inventory
- whether a warehouse or local build is queryable, which decides whether reconciliation can run
  at all (`reference/reconcile.md`)
