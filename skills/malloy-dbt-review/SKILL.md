---
name: malloy-dbt-review
description: Analyze a dbt project as prior art for Malloy modeling. Used during Step 1 (DISCOVER) when a dbt project or its artifacts are present. Reads dbt's manifest, catalog, and semantic manifest to propose Malloy sources, joins, measures, and views over the same marts, and reconciles the numbers against dbt's own semantic layer. Works with or without a live warehouse connection.
---

# dbt Review

> **Purpose:** Evaluate a dbt project as prior art for building a Malloy semantic model. dbt keeps the transformation layer; Malloy becomes the semantic layer over the same marts. This skill coordinates the dbt adapter. The implementation lives in reference files under `reference/`.

> **This is NOT a blind conversion.** dbt's semantic layer encodes real business logic worth carrying over, and also mechanical artifacts that exist only because MetricFlow needs them. Each construct is evaluated before it is emitted, and what does not convert is named with a reason rather than quietly dropped or approximated.

## When to Use

- **Auto-detected:** you find a `dbt_project.yml`, a `models/**/*.sql` tree, or a `target/` directory during discovery, and the user confirms they should be used as prior art.
- **Explicitly requested:** the user says "model from dbt", "convert our dbt metrics", "build a semantic layer on our marts", or points at a dbt project or its `target/` directory.

## The seam: what each side keeps

dbt is not being replaced. The default division of labour:

| dbt keeps | Malloy takes |
| --- | --- |
| EL, raw landing, the messiest staging SQL | The semantic layer: joins, measures, views |
| Python/ML transforms, its package ecosystem | Materialization and rollups (`#@ persist`) |
| Tests and contracts (see **Governance**) | The interface analysts and agents query |

Model **on top of dbt's marts**, not on top of its raw sources. The marts are the reviewed, tested output; re-deriving them in Malloy throws away the tests that guard them. Absorbing dbt's transformations into Malloy is a separate, later decision - see `reference/absorb-transforms.md`.

## Two Modes

| Mode | When | Behavior |
|------|------|----------|
| **dbt + live data** | The warehouse (or a local DuckDB build) is queryable | dbt provides prior art; the data validates it. Full data-driven proposals, and the reconciliation in `reference/reconcile.md` can run. |
| **Artifacts only** | You have `target/*.json` but cannot query | Artifacts provide all context. Proposals are flagged **unvalidated**, and no number is claimed to match. |

In artifacts-only mode, warn the user: "I can read dbt's models and metric definitions, but I cannot query the warehouse, so I can't prove the Malloy numbers match dbt's."

## Read these artifacts, in this order

Everything this skill needs is in dbt's `target/` directory after `dbt build && dbt docs generate`:

| Artifact | Carries | Used for |
|---|---|---|
| `semantic_manifest.json` | Semantic models, entities, dimensions, measures, metrics, saved queries | Joins, measures, views. **The primary input.** |
| `manifest.json` | Model and column descriptions, tests, `ref()` graph | `#(doc)` annotations, governance inventory |
| `catalog.json` | Every column with its type and ordinal | The binding contract (needs `dbt docs generate`) |

**Do not build on `osi_document.json`.** dbt also exports an Open Semantic Interchange document, and it is vendor-neutral and tempting, but it flattens every metric to a SQL string and loses information the conversion needs. Two observed failures on dbt's own jaffle-shop project:

- A filtered metric emits `SUM(CASE WHEN order_id__order_total_dim >= 20 THEN 1 END)`. `order_id__order_total_dim` is MetricFlow's internal qualified dimension name, not a column; the SQL does not run.
- A derived metric's `offset_window` is dropped, so a month-over-month growth metric becomes `(SUM(x) - SUM(x))*100/SUM(x)` - identically zero. dbt warns about its cumulative metric losing meaning in OSI, but does **not** warn about this one.

`semantic_manifest.json` keeps the structured `filter` and the `offset_window`, so it is the input that permits a correct conversion. Note OSI's existence in your notes; do not read semantics from it.

## Reference Files

Each reference file is loaded by the workflow phase that needs it. You do not need to read them all at once.

| Reference File | Phase | What It Does |
|------------|-------|-------------|
| `reference/discover.md` | Step 1 (DISCOVER) | Inventory the dbt project and artifacts, classify models, capture prior-art notes |
| `reference/propose-fields.md` | Step 4 (DEFINE) | Turn entities, dimensions, and metrics into field proposals with evidence |
| `reference/build-bindings.md` | Step 5 (BUILD) | Generate the binding layer: one source per dbt model, carrying dbt's column docs |
| `reference/build-metrics.md` | Step 5 (BUILD) | Translate each dbt metric type to a measure, or to a view when a measure cannot hold it |
| `reference/reconcile.md` | Validation | Use dbt's own semantic layer as the oracle: compare, iterate, record results |
| `reference/absorb-transforms.md` | Later, optional | Move staging/mart SQL into Malloy with `#@ persist`, and when not to |
| `reference/review-coverage.md` | Step 7 (REVIEW) | Compare the Malloy model against dbt: model, metric, and test coverage with reasons for gaps |

### Shared Reference

`reference/_concepts.md` is the dbt-to-Malloy concept mapping table. Referenced by `propose-fields.md`, `build-bindings.md`, and `build-metrics.md`.

## What dbt Provides

- **Model and column descriptions** (`schema.yml`) - documentation authored once, flowing to `#(doc)`, the Explorer UI, and any agent reading the model. This is the highest-value, lowest-risk thing to carry over.
- **Entities** - `type: primary` is a `primary_key`; `type: foreign` is a join, and the entity *name* is what identifies the target model. Cardinality comes free, with no join-key guessing.
- **Metrics** - the business definitions, including filters, ratios, and derived arithmetic, already reviewed by whoever owns the dbt project.
- **Saved queries** - the questions people actually ask, ready to become views.
- **Tests** - an inventory of the constraints the marts are believed to satisfy. Malloy has no test framework today; see **Governance**.

## What to Skip

- **`osi_document.json`** as a semantics source (above).
- **Alias-only dimensions.** A dbt dimension whose `expr` is just its own column (`order_total_dim` over `order_total`) exists because MetricFlow needs a named dimension to filter on. Malloy filters the column directly. Drop the alias, resolve any filter that references it down to the column, and note the drop.
- **`metricflow_time_spine`** and other scaffolding models. A time spine is machinery for cumulative metrics; Malloy truncates timestamps natively and does not need it as a source.
- **Materialization configs** (`+materialized:`, `+schema:`, incremental strategy). These are dbt's build mechanics. If a mart is expensive enough to matter, that becomes one `#@ persist` annotation in Malloy - see `skill:malloy-materialization` - not a translated config.
- **Jinja and macros as text.** Resolve what a macro *produced* by reading the compiled SQL in `target/compiled/` or the built table's schema. Never re-implement a macro's templating in Malloy.
- **dbt's staging layer**, on a first pass. It is renames and casts whose output is already in the marts.

## Governance: name the gap, don't paper over it

dbt's test framework is the clearest place dbt currently leads, and a conversion must say so out loud. Inventory what the dbt project asserts - `not_null`, `unique`, `relationships`, `accepted_values`, `dbt_utils.expression_is_true`, and any unit tests - and record in your notes that these assertions are **not** carried into Malloy and continue to be dbt's job. Never present a converted model as equivalent to the dbt project when the tests did not come with it. Two consequences worth stating to the user:

- Keep running `dbt build` (or at least `dbt test`) on the marts. The Malloy layer inherits their correctness; it does not re-establish it.
- A `relationships` test is the evidence behind a `join_one`. If you convert the join, cite the test that justifies its cardinality; if there is no such test, the cardinality is an assumption and must be flagged as one.

## Recording the work

Follow the host workflow's `modeling-notes.md` (scope, grain and keys, decisions, open decisions, validation). dbt-specific items that belong in it:

- **Scope:** which dbt models are in, which are skipped, each with a reason.
- **Decisions:** every metric that changed shape (a measure that became a view, a name that had to move), with the constraint that forced it.
- **Open decisions:** any metric whose dbt definition you could not verify against data.
- **Validation:** the reconciliation table from `reference/reconcile.md` - which metrics match dbt's own output, and which are deliberately not reproduced.
- **Provenance:** the dbt project's git ref (or `manifest.json`'s hash) that the model was generated from, so "the dbt build and the Malloy model are on the same ref" is a recorded fact rather than an assumption.
