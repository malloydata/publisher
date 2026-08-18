---
name: malloy-dbt-adopt
description: Build a Malloy semantic layer on top of the marts a dbt project already builds, using dbt's manifest, catalog, and semantic manifest as prior art. dbt keeps the pipeline untouched. Use when a dbt project or its target/ artifacts are present and the user wants their dbt models, metrics, or docs queryable in Malloy. For moving the pipeline itself into Malloy, see malloy-dbt-convert.
---

# dbt: Adopt

> Sit a Malloy semantic layer on top of dbt's marts. dbt keeps EL, staging, the marts, and
> the tests. Malloy becomes the interface analysts and agents query. No warehouse changes.

This is the first move for a team running dbt, and usually the only one they want. The marts
are already reviewed and tested; rebuilding them buys nothing and loses the tests. To move the
pipeline itself, read `skill:malloy-dbt-convert` -- and read this skill first either way,
because both need the same artifacts and hit the same naming rules.

> **Not a blind conversion.** dbt's semantic layer holds real business definitions worth
> carrying over, and mechanical artifacts that exist only because MetricFlow needs them. Emit
> the first, drop the second, and name anything that does not convert instead of approximating it.

## Read these artifacts

Everything needed is in `target/` after `dbt build && dbt docs generate`:

| Artifact | Carries | Needed for |
|---|---|---|
| `semantic_manifest.json` | semantic models, entities, dimensions, metrics, saved queries | joins, measures, views. **The primary input.** |
| `manifest.json` | model and column descriptions, tests, the `ref()` graph | `#(doc)` annotations, the test inventory |
| `catalog.json` | every column with type and ordinal | the column list (only `dbt docs generate` writes it) |

**Do not read semantics from `osi_document.json`.** dbt also exports an Open Semantic
Interchange document. It is vendor-neutral and carries resolved relationships, which makes it
tempting, but it flattens every metric to a SQL string and loses two things that produce wrong
numbers. Observed on dbt's own jaffle-shop:

- A filtered metric emits `SUM(CASE WHEN order_id__order_total_dim >= 20 THEN 1 END)`.
  `order_id__order_total_dim` is MetricFlow's internal qualified dimension name, not a column.
  The SQL does not run.
- A derived metric's `offset_window` is **dropped**, so a month-over-month growth metric
  becomes `(SUM(x) - SUM(x))*100/SUM(x)` -- identically zero. dbt warns that its cumulative
  metric loses meaning in OSI; it does **not** warn about this one.

`semantic_manifest.json` keeps the structured `filter` and the `offset_window`. Read that.

If there is no `semantic_manifest.json`, the project has no semantic layer to carry over. That
is a common and fine state: you still get descriptions, the `ref()` graph, and the tests, and
the field proposals then come from the data.

## The mapping

| dbt | Malloy |
|---|---|
| mart model | `source` over the built relation |
| model / column `description` | `#(doc)` on the source / field |
| `entity: {type: primary}` | `primary_key:` (its `expr`, else its `name`) |
| `entity: {type: foreign}` | `join_one:` -- the target is the model whose **primary entity has the same name** |
| `dimension` with a real `expr` | `dimension:` |
| `dimension` aliasing its own column | nothing -- MetricFlow scaffolding, see below |
| `dimension: {type: time}` | nothing -- Malloy truncates natively (`ordered_at.month`) |
| `metric` | `measure:`, or a view (`reference/metrics.md`) |
| `metric.label` | `# label="..."`, only when it differs from the name |
| `saved_query` | `view:` on the source where its metrics resolve |
| `data_tests`, `unit_tests` | nothing -- stays dbt's job, see **Governance** |
| `+materialized:`, incremental config | nothing here; `skill:malloy-dbt-convert` if warranted |
| `metricflow_time_spine` | nothing -- scaffolding for cumulative metrics |

Entities are the highest-value structural read: cardinality is declared, so no join-key
guessing. A `foreign` entity names its target by entity name, not by table name.

## Reference files

| File | What it covers |
|---|---|
| `reference/sources.md` | The source shape that carries dbt's column docs, and the two naming collisions |
| `reference/metrics.md` | Every metric type, and the three a measure cannot hold |
| `reference/reconcile.md` | Proving the Malloy number matches dbt's, using dbt's own engine |

## What to skip

- **`osi_document.json`** as a semantics source (above).
- **Alias-only dimensions.** A dbt dimension whose `expr` is just its own column
  (`order_total_dim` over `order_total`) exists so MetricFlow has a name to filter on. Malloy
  filters the column directly: drop the alias, resolve any filter through it to the column,
  and note the drop.
- **`metricflow_time_spine`** and similar scaffolding.
- **Materialization and incremental config.** dbt's build mechanics, not semantics.
- **Jinja and macros as text.** Read what a macro *produced* (`target/compiled/`, or the built
  table's schema). Never re-implement templating.
- **The staging layer.** Renames and casts whose output is already in the marts.

## Governance: name the gap

dbt's test framework is the clearest place dbt leads, and a conversion must say so out loud.
Inventory what the project asserts -- `not_null`, `unique`, `relationships`,
`accepted_values`, `dbt_utils.*`, `unit_tests` -- and record that **none of it is carried into
Malloy**. Two things to tell the user plainly:

- `dbt build` (or at least `dbt test`) must keep running. The Malloy layer inherits the marts'
  correctness; it does not re-establish it.
- A `relationships` test is the evidence behind a `join_one`'s cardinality. Cite it when you
  convert the join. With no such test, the cardinality is an assumption -- flag it as one.

Never present a converted model as equivalent to the dbt project when the tests did not come
with it.

## Recording the work

Follow the host workflow's `modeling-notes.md`. dbt-specific entries:

- **Scope:** models in, models skipped, a reason each.
- **Decisions:** every metric that changed shape or name, and the constraint that forced it.
- **Validation:** the reconciliation table from `reference/reconcile.md`.
- **Provenance:** the dbt git ref or `manifest.json` hash the model was built from, so "the dbt
  build and the Malloy model are on the same ref" is recorded rather than assumed.
