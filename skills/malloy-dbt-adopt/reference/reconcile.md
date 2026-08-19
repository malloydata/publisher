# Reconcile Against dbt (Validation)

> dbt can answer the same questions you just converted. Ask it, compare, iterate until the
> numbers agree, and record the result. Do not hand a converted model over on the strength
> of "it compiles."

"The new number matches the old number" is what stalls semantic-layer migrations. It is also
cheap to establish here, because the incumbent is queryable: you are not comparing against a
spec, you are comparing against the engine that produces today's numbers.

## Use dbt's own engine, not your own SQL

The oracle is dbt's semantic layer, in this order of preference:

1. **MetricFlow locally** - `mf query --metrics <names>` against the same warehouse or DuckDB build. Installed with `dbt-metricflow`.
2. **The dbt Semantic Layer API**, if the project runs on dbt Cloud.
3. **dbt's compiled SQL** for the metric, from `target/`, run directly.
4. **Hand-written SQL** - last resort only. A re-implementation drifts from the definition it is meant to check, and can pass on the exact bug it exists to catch.

Never validate a metric against SQL you wrote from the same understanding that produced the Malloy. That checks your reading twice, not the conversion.

## The loop

For each converted measure:

1. Ask dbt: `mf query --metrics <metric>` (grouped the same way, if the metric is grouped).
2. Ask Malloy: `run: <source> -> { aggregate: <measure> }` via `execute_query`.
3. Compare. Aggregates over identical rows should agree to floating-point noise, not "approximately."
4. On a mismatch, fix and re-run. The usual causes, in the order they actually occur:
   - **A filter that did not resolve.** A MetricFlow qualified dimension name reached the Malloy as if it were a column, or a `= true` was dropped incorrectly.
   - **The wrong column after a binding rename.** Two similar names now exist (`order_total` and `order_total_raw`); aggregating the wrong one compiles fine.
   - **Fan-out across a join.** If a cross-source measure is off by a multiple, the join path is wrong or the reference is not going through the join.
   - **A different grain.** dbt grouped by `metric_time` at day; the view grouped by month.
   - **Stale artifacts.** `semantic_manifest.json` predates the last `dbt build`.

Batch the queries: group every measure by the source it lives on and run one Malloy query per source, so ~20 metrics take a handful of queries rather than twenty round trips.

## Where the result goes

The comparison belongs in the model's own artifacts, not in a bespoke test harness:

- **`modeling-notes.md` § Validation** - the table of metric, dbt value, Malloy value, verdict. This is the host workflow's existing home for reconciliation results; use it rather than inventing a parallel one.
- **A notebook** is the better home when the model ships as a package: each query beside the dbt number it has to match, so the check re-runs by opening it and re-reading the cells. It stays honest because it runs the real model, and a reviewer can see both numbers at once.

State the dbt number **once**. A baseline duplicated into a JSON file and a notebook and a doc goes stale in at least one of them.

## What "reconciled" is allowed to mean

Be precise about the three outcomes, and never blur them:

- **Matches.** Same value to floating-point noise. Say how many of how many.
- **Matches under a stated convention.** Same digits, different scaling or reporting instant - a fraction against percentage points, or a month-start against a month-end running total. Record the exact relationship (for example "agrees digit for digit; dbt returns percentage points, the view returns a fraction") so the difference is a decision on the record, not a discrepancy someone rediscovers later.
- **Not reproduced.** The metric is deferred, with the reason. A deferred metric is not a passing check, and it must not be counted in the match tally.

In artifacts-only mode none of this is available. Say plainly that no number was verified, rather than reporting a conversion as complete.

## What reconciliation does not prove

It proves you computed dbt's definition faithfully. It says nothing about whether that definition answers the question being asked, and the two are easy to confuse when a report says 20 of 20 matched.

Every ambiguity in `reference/ambiguity.md` passes reconciliation: the gross figure that is not revenue, the overlapping flags whose shares sum to 122%, the current-state field that answers an acquisition question 400x too small. They match dbt to the digit, because they *are* dbt's definitions. A green reconciliation and a wrong answer are fully compatible.

So report the two separately, and never let the first stand in for the second: "20 of 20 measures match dbt's own engine" and "three definitions were ambiguous; here is what each was settled to and who confirmed it".
