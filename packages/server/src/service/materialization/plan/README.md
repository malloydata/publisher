# `materialization/plan/`

Turns a compiled package's declarations (`#@ persist`, `#(materialize)`,
`#(preaggregate)`, v1 `#@ preaggregate`, a persisted view) into the tables a
run should build, or a refusal or omission for each. It compiles and never
runs: on the package-load worker its connection refuses SQL.

## Modules

| Module                                 | Responsibility                                                                                                           | Read by                                                                                                        |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------- |
| `build_plan.ts`                        | The package's build plan: each model's persist sources, builds and refusals, and the wire projection (`deriveBuildPlan`) | the load worker, `package.ts`, `model.ts`, the facade, most of `run/`                                          |
| `build_plan_compile.ts`                | Compiling a package's build plan on the main thread, against its live connections                                        | the facade, the materialization service                                                                        |
| `addressing.ts`                        | A persist source's content address and the SQL it is keyed by                                                            | the build plan, eligibility, the instructed build                                                              |
| `eligibility_collect.ts`               | Which planned sources may build and serve, decided where the compiled sources exist                                      | the build plan, the facade                                                                                     |
| `materialization_eligibility.ts`       | What an eligible source carries into the build                                                                           | the build plan, eligibility, the run's instructions                                                            |
| `persist_source_fields.ts`             | Leaf readers of one compiled persist source (its storage, its public columns)                                            | the planners, the run's support and table ownership                                                            |
| `persist_partition.ts`                 | The `partition=` key of a `#@ persist` tag                                                                               | addressing, the build plan, the run's support                                                                  |
| `persist_dynamic_terms.ts`             | Where a source's givens sit, and whether it may be stored and served per caller                                          | the planners, the served copy                                                                                  |
| `persist_annotation_validation.ts`     | Publish-time checks of `#@ persist` lines                                                                                | `package.ts`                                                                                                   |
| `plan_layers.ts`                       | The layered settings (freshness, query metadata) a plan entry resolves                                                   | the build plan                                                                                                 |
| `materialization_config_validation.ts` | Publish-time validation of a package's materialization config                                                            | `package.ts`                                                                                                   |
| `incremental_declaration.ts`           | The incremental keys of a persist tag, and the columns a delta writes                                                    | the build plan, incremental policy, the run's incremental apply                                                |
| `incremental_policy.ts`                | The gate and advisory findings for an incremental declaration                                                            | `package.ts`, the facade                                                                                       |
| `rolling_window.ts`                    | Rolling windows on a `#(materialize)` source                                                                             | the generator, incremental declaration, the served copy, incremental apply                                     |
| `build_file_generator.ts`              | The `#(materialize)` build companion: the build sources, build copies, stored-chain text and key checks                  | the build plan, the run (instructed build, the storage chain), the served copy, colocated bindings, `model.ts` |
| `materialize_compile.ts`               | Compiling the `#(materialize)` companion and judging each declaration                                                    | the build plan                                                                                                 |
| `cross_connection_joins.ts`            | A build view that leaves out joins across connections, and the refusal it reads                                          | the load worker, `model.ts`, the served copy and its containment                                               |
| `v1_build_companion.ts`                | The build copy for a v1 `#@ persist` model without the persistence flag                                                  | the build plan                                                                                                 |
| `persisted_view.ts`                    | `#@ persist` on a view: its declaration and refusals                                                                     | the build plan, the view and rollup legs, run telemetry                                                        |
| `persisted_view_compile.ts`            | Compiling the persisted-view companion                                                                                   | the build plan, the view and rollup legs                                                                       |
| `rollup_plan.ts`                       | `#(preaggregate)` rollups: grain, measures, key checks                                                                   | the build plan, the rollup legs, the instructed build                                                          |
| `rollup_compile.ts`                    | Compiling the `#(preaggregate)` companion                                                                                | the build plan, the rollup leg                                                                                 |
| `rollup_measure_grammar.ts`            | Which measures a rollup can store and merge                                                                              | the rollup plan and legs                                                                                       |
| `preaggregation_annotation.ts`         | Reading the v1 `#@ preaggregate` annotation                                                                              | the v1 rollup planner and validation                                                                           |
| `preaggregation_classifier.ts`         | Additivity of a v1 pre-aggregated measure                                                                                | the v1 rollup planner and validation                                                                           |
| `preaggregation_namespace.ts`          | Which rollup namespaces may be spliced into a table name                                                                 | the v1 rollup planner, the rollup plan                                                                         |
| `preaggregation_synthesis.ts`          | The v1 `#@ preaggregate` planner                                                                                         | the build plan, eligibility, the rollup plan                                                                   |
| `preaggregation_validation.ts`         | Publish-time validation of `#@ preaggregate`                                                                             | `model.ts`, the facade                                                                                         |
| `preaggregation_compile.ts`            | The runtime factory a generated companion compiles in                                                                    | the companion compilers, the load worker                                                                       |

## Dependency rule

May import: `domain/`, `text/`, `governance/ir`. Must not import `run/`,
`serve/` or `service/model.ts` (rule R7), and plan, run and serve never
import one another (R8). The edges that remain are listed, each with the step
that removes it, in `materialization_allowlists.mjs`.

## Where things are added

A new kind of table is a declaration reader, a planner and a gate-policy row
here. A planner returns findings rather than logging or counting (H5).
