# `materialization/plan/`

Turns a compiled package's declarations (`#@ persist`, `#(materialize)`,
`#(rollup)`, v1 `#@ preaggregate`, a persisted view) into the tables a
run should build, or a refusal or omission for each. It compiles and never
runs: on the package-load worker its connection refuses SQL.

## What it produces

One `TablePlan` per planned table (`domain/table_plans.ts`), in the compiled
plan's `tables`, keyed by `sourceID`: the planned source, its
build target, its declaring model, its classified gate, its stored-table key
check, and its kind, fixed by the planner that made it:

| Kind          | Planned by                                                                             | Carries               |
| ------------- | -------------------------------------------------------------------------------------- | --------------------- |
| `persist`     | the v1 walk (`collectModelBuildPlan`)                                                  | nothing more          |
| `materialize` | `materialize_plan.ts`, compiled by `materialize_compile.ts`                            | its `MaterializePlan` |
| `pair`        | the same, for an inlined join_many                                                     | its `MaterializePlan` |
| `rollup`      | `rollup_plan.ts` and the v1 synthesis, compiled by `rollup_compile.ts`                 | its rollup plan       |
| `view`        | `persisted_view.ts` (a view or a named query), compiled by `persisted_view_compile.ts` | nothing more          |

Nothing downstream derives a kind: the build, its telemetry, eligibility and
the wire projection read it off the table. A declaration that is not planned
is a refusal (a package warning, and `refusedSources` on the wire) or an
omission (`omittedSources`, `omittedFiles`). `deriveBuildPlan` projects the
tables into the wire `BuildPlan`; `derivePackagePlan` into the
`PackagePlan` (`domain/package_plan.ts`): every planned table as data, the
wire plan and what serving and the load gate read beside it, the one plan
shape that crosses the load worker. The worker and the main thread finish a
package's parts through the one tail (`finishPackagePlan`). The facade holds
the adopted plan as one immutable record (`PlanState`, in `plan_lifecycle.ts`),
and a load, a re-derivation, a run and a package compile read one load-gate
text off it (`load_gate_text.ts`). A run reads the plan only through
`run/run_plan.ts`, which compiles the package and derives its `PackagePlan`
as a load does; only a run's plan compiles the key checks its builds run
(`compileRunKeyChecks`).

## The build path

1. Each model's declarations are read (`declaration.ts`) and each generated
   companion is planned and compiled: `#(materialize)`, persisted views and
   rollups, then the v1 walk over the author's model or its build copy.
2. Each planned table's gate is classified where the model compiled.
3. Per package: rollup key checks, stored-table key checks, connection
   digests, and the refusal of `#(materialize)` tables that would share a
   table.
4. The run (`run/`) builds the tables in build order on their tier, and the
   facade binds what it committed (`serve/`).

## Modules

| Module                                 | Responsibility                                                                                                                                                                                                                                             | Read by                                                                          |
| -------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `build_plan.ts`                        | The package's build plan: one `TablePlan` per planned table, collected model by model, its eligibility, and the wire projection (`deriveBuildPlan`)                                                                                                        | the load worker, `package.ts`, `model.ts`, the facade, most of `run/`            |
| `build_plan_compile.ts`                | A run's build plan compiled on the main thread against live connections, its key checks included (`compileRunKeyChecks`); a load's `PackagePlan` when the worker left it (`computeLoadPlan`); reporting skipped digests and recording eligibility refusals | the facade (`plan_lifecycle.ts`), the materialization service                    |
| `addressing.ts`                        | A persist source's content address and the SQL it is keyed by                                                                                                                                                                                              | the build plan, eligibility, the instructed build                                |
| `declaration.ts`                       | The one reader of materialization's annotations, and the one placement rule (`storage=` and `name=`, beside a route's key or inside it; an empty one refused)                                                                                              | every planner here                                                               |
| `eligibility_collect.ts`               | Which planned sources may build and serve, decided where the compiled sources exist                                                                                                                                                                        | the build plan, the facade                                                       |
| `materialization_eligibility.ts`       | What an eligible source carries into the build                                                                                                                                                                                                             | the build plan, eligibility, the run's instructions                              |
| `persist_source_fields.ts`             | Leaf readers of one compiled persist source (its storage, its columns)                                                                                                                                                                                     | the planners, the run's support and table ownership                              |
| `persist_dynamic_terms.ts`             | Where a source's givens sit, and whether it may be stored and served per caller                                                                                                                                                                            | the planners, the served copy                                                    |
| `access_closure.ts`                    | The relations an access decision reads (a gated or given-reading definition's dependencies), which a relation that reads the current time may not be stored as of its build for                                                                            | the eligibility gate (via the plan's `accessReads`)                              |
| `persist_annotation_validation.ts`     | Publish-time checks of `#@ persist` lines                                                                                                                                                                                                                  | `package.ts`                                                                     |
| `freshness_declaration.ts`             | A rollup's `freshness` group, read strictly, and one rollup's freshness from its measures and base                                                                                                                                                         | the rollup planners and v1 validation                                            |
| `plan_layers.ts`                       | The layered settings (freshness, query metadata) a plan entry resolves                                                                                                                                                                                     | the build plan                                                                   |
| `materialization_config_validation.ts` | Publish-time validation of a package's materialization config                                                                                                                                                                                              | `package.ts`                                                                     |
| `incremental_declaration.ts`           | The incremental keys of a persist tag, and the columns a delta writes                                                                                                                                                                                      | the build plan, incremental policy, the run's incremental apply                  |
| `incremental_policy.ts`                | The gate and advisory findings for an incremental declaration                                                                                                                                                                                              | `package.ts`, the facade                                                         |
| `load_gate_text.ts`                    | The one text invalid annotations fail with (`loadGateText`, with the raw-text `name=` merge), read off a `PackagePlan` by the load, a run's guard, a re-derivation and a package compile                                                                   | the facade, `package.ts`, the run service, `compile_source.ts`                   |
| `rolling_window.ts`                    | Rolling windows on a `#(materialize)` source                                                                                                                                                                                                               | the generator, incremental declaration, incremental apply                        |
| `build_file_generator.ts`              | The `#(materialize)` build companion's text: the generated build sources, pair and key-check queries, level files and generated names                                                                                                                      | the companion compiler, the build plan, the run (the inline checks, run support) |
| `materialize_plan.ts`                  | Planning a model's `#(materialize)` sources: declarations read, refusals, stored columns, pair layout, key checks, extension chains                                                                                                                        | the companion compiler, the build plan, eligibility                              |
| `materialize_read_time.ts`             | A `#(materialize)` source's read-time terms, their declaration and givens, and where each is written                                                                                                                                                       | the planner, the build copy                                                      |
| `materialize_build_copy.ts`            | A `#(materialize)` source's build copy: terms taken out, access modifiers lifted, the parent re-rooted                                                                                                                                                     | the companion compiler, the stored chain                                         |
| `materialize_stored_chain.ts`          | The text a chained `storage=` extension table builds from in its destination                                                                                                                                                                               | the companion compiler, the `storage=` build                                     |
| `materialize_compile.ts`               | Compiling the `#(materialize)` companion and judging each declaration                                                                                                                                                                                      | the build plan                                                                   |
| `v1_build_companion.ts`                | The build copy for a v1 `#@ persist` model without the persistence flag                                                                                                                                                                                    | the build plan                                                                   |
| `persisted_view.ts`                    | `#@ persist` on a view or a named query: its declaration, refusals and companion text                                                                                                                                                                      | the build plan, `model.ts` (for the view and query legs), run telemetry          |
| `persisted_query.ts`                   | `#@ persist` on a named query: reading the declarations, its key findings and its warning                                                                                                                                                                  | the build plan, `persisted_view.ts`                                              |
| `persisted_view_compile.ts`            | Compiling the persisted-view companion, and judging each view and named query                                                                                                                                                                              | the build plan                                                                   |
| `rollup_plan.ts`                       | `#(rollup)` rollups: grain, measures, key checks                                                                                                                                                                                                           | the build plan, `model.ts` (for the rollup leg), the instructed build            |
| `rollup_compile.ts`                    | Compiling the `#(rollup)` companion                                                                                                                                                                                                                        | the build plan                                                                   |
| `refusal_key.ts`                       | A refusal record's key and the tier a declaration would have been built in                                                                                                                                                                                 | every planner that refuses a valid declaration                                   |
| `rollup_measure_grammar.ts`            | Which measures a rollup can store and merge                                                                                                                                                                                                                | the rollup plan                                                                  |
| `preaggregation_annotation.ts`         | Reading the v1 `#@ preaggregate` annotation                                                                                                                                                                                                                | the v1 rollup planner and validation                                             |
| `preaggregation_classifier.ts`         | Additivity of a v1 pre-aggregated measure                                                                                                                                                                                                                  | the v1 rollup planner and validation                                             |
| `preaggregation_namespace.ts`          | Which rollup namespaces may be spliced into a table name                                                                                                                                                                                                   | the v1 rollup planner, the rollup plan                                           |
| `preaggregation_synthesis.ts`          | The v1 `#@ preaggregate` planner                                                                                                                                                                                                                           | the build plan, eligibility, the rollup plan                                     |
| `preaggregation_validation.ts`         | Publish-time validation of `#@ preaggregate`                                                                                                                                                                                                               | `model.ts`, the facade                                                           |

## Dependency rule

Of materialization's layers, it may import `domain/`, `text/` and
`governance/ir`; it also reads server modules outside materialization (the
configuration, the annotation reader, the gate classification and the like).
It must not import `run/`, `serve/` or `service/model.ts` (rule R7), and
plan, run and serve never import one another (R8); the planners that still
log or count are listed (H5), and each logs an error by its code, never its
text (`never_log_rule.spec.ts`). The edges that remain are listed, each with the step
that removes it, in `materialization_allowlists.mjs`.

## Where things are added

A new kind of table is a `TableKind`, a declaration reader, a planner that
sets the kind on the tables it plans, and a gate-policy row
(`domain/gate_policy.ts`). Run and bind do not branch on a kind beyond what
the plan carries. A planner returns findings rather than logging or counting
(H5); the planners that still log are listed in `materialization_allowlists.mjs`.
