# `materialization/run/`

Makes the tables a run's instructions name and records what it made: claims
the tables, builds each in its tier (colocated in the source's warehouse, or
`storage=` into DuckDB or DuckLake), runs the key checks, commits one manifest
per run and reclaims what no manifest holds. This is where the module's
warehouse SQL, run store, clock, metrics and logs live.

## Modules

| Module                              | Responsibility                                                                                          | Read by                                                                                                                |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `materialization_service.ts`        | The run's front door: builds, status, cancellation, the forwarding seams                                | the controller, `main_server.ts`, the scheduler, `index.ts`                                                            |
| `materialization_scheduler.ts`      | Fires each package's declared schedules                                                                 | `main_server.ts`, `index.ts`                                                                                           |
| `materialization_instructed_run.ts` | A run over instructions, with the gate each source was cleared by                                       | the service, the instructed build, run instructions                                                                    |
| `run_instructions.ts`               | The instructions a self-instructed run derives, and its table names                                     | the service                                                                                                            |
| `run_lifecycle.ts`                  | A run's state machine and its life in this process                                                      | the service, the committer, the reclaimer                                                                              |
| `instructed_build.ts`               | One instructed run's build loop, per tier and in dependency order                                       | the service                                                                                                            |
| `source_build.ts`                   | One source's build: render, classify its stored upstreams, then the colocated or `storage=` tier        | the instructed build, the service                                                                                      |
| `build_strategy.ts`                 | The interface the colocated strategies implement, and the refresh they act on                           | the strategies, the source build                                                                                       |
| `colocated_build_strategy.ts`       | A full rebuild of a colocated table (staging CTAS and swap)                                             | the source build                                                                                                       |
| `colocated_native_types.ts`         | A colocated table's columns as the warehouse's catalog types them                                       | the colocated strategy, incremental build                                                                              |
| `incremental_delta_strategy.ts`     | A colocated table advanced by a delta                                                                   | the source build                                                                                                       |
| `incremental_build.ts`              | The run-level half of incremental materialization: seed or delta, and the ledger                        | the strategies, the instructed build, the service                                                                      |
| `incremental_apply.ts`              | Generating and applying a delta, per dialect                                                            | incremental build and storage, the build session                                                                       |
| `incremental_storage.ts`            | The delta target of a `storage=` table, across two engines                                              | incremental build                                                                                                      |
| `storage_build_strategy.ts`         | The `storage=` tier: read from the warehouse, or stack on stored parents, and load into the destination | the source build, the service                                                                                          |
| `materialization_build_session.ts`  | Build-scoped DuckDB sessions on a destination: the CTAS, the chained build's compile, drops             | the `storage=` tier, incremental storage, `name=` views, the reclaimer, `catalog_admin.ts`, the destination controller |
| `storage_table_load.ts`             | Loading a file into a storage destination as a table                                                    | `catalog_admin.ts`                                                                                                     |
| `same_catalog_build.ts`             | Whether a source and a destination are the same DuckLake catalog                                        | the build session, the `storage=` tier                                                                                 |
| `build_query_tag.ts`                | A build's query-metadata bag in each warehouse's tagging grammar                                        | the build session                                                                                                      |
| `build_read_cost.ts`                | What a build read from the warehouse, from session history                                              | the build session, incremental storage                                                                                 |
| `inline_uniqueness.ts`              | The join_one and pair key checks, and the ledger that skips a proven repeat                             | the instructed build, the service, the committer, the failure classes                                                  |
| `manifest_committer.ts`             | A run's manifest, from commit to serving                                                                | the service                                                                                                            |
| `manifest_loader.ts`                | A fetched build manifest, split by tier                                                                 | `environment.ts`, the committer                                                                                        |
| `materialization_run_support.ts`    | What a build needs from the plan about stored tables, and the environment surface it reads              | most of `run/`, the controller                                                                                         |
| `table_claims.ts`                   | Which tables an active run writes, so two runs never write one at once                                  | the service                                                                                                            |
| `table_ownership.ts`                | Who owns a table an auto-run writes at an author's name                                                 | the instructed build, the service, the reclaimer                                                                       |
| `name_view.ts`                      | The `name=` view over a table, and its swap                                                             | the strategies, the instructed build, the reclaimer                                                                    |
| `name_view_sql.ts`                  | The SQL of a `name=` view, per dialect                                                                  | `name_view.ts`, the instructed build, the reclaimer                                                                    |
| `reclaimer.ts`                      | Dropping the tables and views no manifest holds                                                         | the service, `index.ts`                                                                                                |
| `run_telemetry.ts`                  | The run and build events and counters                                                                   | the service, the instructed build, the lifecycle                                                                       |
| `build_failure_class.ts`            | Whether retrying a failed build could succeed                                                           | the instructed build, the lifecycle, telemetry                                                                         |
| `error_code.ts`                     | An error reduced to a code a log may carry                                                              | the strategies, the build session, telemetry                                                                           |
| `cron_evaluator.ts`                 | Evaluating a 5-field cron expression in UTC                                                             | the scheduler, the facade, package skills                                                                              |

## Dependency rule

May import: `domain/`, `text/`, the host port (`host.ts`). Must not import
`plan/` or `serve/` (rule R8); the edges that remain are listed, each with
the step that removes it, in `materialization_allowlists.mjs`.

## Where things are added

A new dialect's SQL text goes beside the statement it changes; a new tier is
an adapter beside the two strategies.
