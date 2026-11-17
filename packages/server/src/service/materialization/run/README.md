# `materialization/run/`

Makes the tables a run's instructions name and records what it made: claims
the tables, builds each in its tier (colocated in the source's warehouse, or
`storage=` into DuckDB or DuckLake), runs the key checks, commits one manifest
per run and reclaims what no manifest holds. This is where the module's
warehouse SQL, run store, clock, metrics and logs live.

## Modules

| Module                              | Responsibility                                                                                                                                                                          | Read by                                                                                               |
| ----------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `materialization_service.ts`        | The run's front door: builds, status, cancellation; the run collaborators its constructor takes                                                                                         | the controller, `main_server.ts`, the scheduler, `index.ts`                                           |
| `materialization_scheduler.ts`      | Fires each package's declared schedules                                                                                                                                                 | `main_server.ts`, `index.ts`                                                                          |
| `materialization_instructed_run.ts` | A run over instructions, with the gate each source was cleared by                                                                                                                       | the service, the instructed build, run instructions                                                   |
| `run_instructions.ts`               | The instructions a self-instructed run derives, and its table names                                                                                                                     | the service                                                                                           |
| `run_lifecycle.ts`                  | A run's state machine and its life in this process                                                                                                                                      | the service, the committer, the reclaimer                                                             |
| `instructed_build.ts`               | One instructed run's build loop, per tier and in dependency order                                                                                                                       | the service                                                                                           |
| `source_build.ts`                   | One source's build: render, classify its stored upstreams, then the colocated or `storage=` tier                                                                                        | the instructed build, the service                                                                     |
| `colocated_build_strategy.ts`       | The colocated tier's adapter (`ColocatedTier`): a delta, else a staging CTAS and swap in the source's own warehouse; its schema capture; a table's drop and key check on its connection | the source build, the instructed build's key checks, the reclaimer                                    |
| `colocated_native_types.ts`         | A colocated table's columns as the warehouse's catalog types them                                                                                                                       | the colocated strategy, incremental build                                                             |
| `incremental_delta_strategy.ts`     | A colocated incremental table's bounded delta, or `undefined` for a full rebuild                                                                                                        | the colocated tier                                                                                    |
| `incremental_build.ts`              | The run-level half of incremental materialization: seed or delta, and the ledger                                                                                                        | the strategies, the instructed build, the service                                                     |
| `incremental_apply.ts`              | Generating and applying a delta: bounds, probes, range replace, merge and pair delta                                                                                                    | incremental build and storage, the build session                                                      |
| `incremental_storage.ts`            | The delta target of a `storage=` table, across two engines                                                                                                                              | incremental build                                                                                     |
| `postgres_numeric_precision.ts`     | A Postgres `numeric` kept exact: read as text, stored as the DECIMAL that holds it; a delta's guard                                                                                     | the build session, incremental storage, incremental build                                             |
| `storage_build_strategy.ts`         | The `storage=` tier's adapter: read from the warehouse, or stack on stored parents, and load into the destination; a table's drop and key check in its destination                      | the source build, the service, the instructed build's key checks, the reclaimer                       |
| `materialization_build_session.ts`  | Build-scoped DuckDB sessions on a destination: the CTAS, the chained build's compile, drops                                                                                             | the `storage=` tier, incremental storage, table sites, `catalog_admin.ts`, the destination controller |
| `storage_table_load.ts`             | Loading a file into a storage destination as a table                                                                                                                                    | `catalog_admin.ts`                                                                                    |
| `same_catalog_build.ts`             | Whether a source and a destination are the same DuckLake catalog                                                                                                                        | the build session, the `storage=` tier                                                                |
| `build_query_tag.ts`                | A build's query-metadata bag in each warehouse's tagging grammar                                                                                                                        | the build session                                                                                     |
| `build_read_cost.ts`                | What a build read from the warehouse, from session history                                                                                                                              | the build session, incremental storage                                                                |
| `inline_uniqueness.ts`              | The join_one and pair key checks, and the ledger that skips a proven repeat                                                                                                             | the instructed build, the service, the committer, the failure classes                                 |
| `freshness_stamp.ts`                | A standalone run's freshness facts: declared windows stamped on its entries, and which carried tables are past theirs                                                                   | the service                                                                                           |
| `manifest_committer.ts`             | A run's manifest, from commit to serving                                                                                                                                                | the service                                                                                           |
| `manifest_loader.ts`                | A fetched build manifest, split by tier                                                                                                                                                 | `environment.ts`, the committer                                                                       |
| `materialization_run_support.ts`    | What a build needs from the plan about stored tables, and the environment surface it reads                                                                                              | most of `run/`, the controller                                                                        |
| `table_claims.ts`                   | Which tables an active run writes, so two runs never write one at once                                                                                                                  | the service                                                                                           |
| `keyed_lock.ts`                     | One mutex per key                                                                                                                                                                       | table claims, table ownership                                                                         |
| `table_ownership.ts`                | Who owns a table an auto-run writes at an author's name                                                                                                                                 | the instructed build, the service, the reclaimer                                                      |
| `table_site.ts`                     | Where a table's marks are read: its connection, or a session on its destination                                                                                                         | table ownership, the instructed build, the service, the reclaimer, leftover views                     |
| `table_mark_sql.ts`                 | The marks on a table and on a leftover `name=` view, and reading them back                                                                                                              | table ownership, leftover views, table sites, the instructed build, the service, the reclaimer        |
| `warehouse_sql.ts`                  | The SQL text that differs by dialect: literals, probes, the ownership mark, a delta's transaction, the swap, the catalog read, read cost, the query tag                                 | the marks, ownership, leftover views, the incremental refresh, the colocated tier, the build session  |
| `leftover_name_views.ts`            | Dropping this package's own `name=` views an earlier release left                                                                                                                       | the instructed build, the reclaimer                                                                   |
| `reclaimer.ts`                      | Dropping the tables no manifest holds, through each table's tier adapter, and their leftover views                                                                                      | the service, `main_server.ts` (as the service's `reclaimer`), `index.ts`                              |
| `run_telemetry.ts`                  | The run and build events and counters                                                                                                                                                   | the service, the instructed build, the lifecycle                                                      |
| `build_failure_class.ts`            | Whether retrying a failed build could succeed                                                                                                                                           | the instructed build, the lifecycle, telemetry                                                        |
| `error_code.ts`                     | An error reduced to a code a log may carry                                                                                                                                              | the strategies, the build session, telemetry                                                          |
| `cron_evaluator.ts`                 | Evaluating a 5-field cron expression in UTC                                                                                                                                             | the scheduler, the facade, package skills                                                             |
| `tier.ts`                           | The tier port (`TierAdapter`): building, dropping and key-checking a table where it lives; the colocated build's request; each tier's site                                              | the source build, the instructed build, the reclaimer, the two tier adapters                          |

## Lock order

A run's locks are taken in one order, so no two callers wait on each other:

1. The package lock (`withPackageLock`): compiling an unversioned package,
   loading or rebinding it, and a package's first versioned publish, which
   takes its version's lock under it.
2. The version lock (`VersionService.withVersionLock`).
3. The table lock (`TableClaims.withTableLock`), per package.
4. The owned-name lock (`withOwnedNameLock`, table_ownership.ts), keyed by
   `tableIdentity`: held only to check a `name=` table's ownership, build it
   and mark it, and nothing else is taken while it is held. A table the
   package cannot prove its own is retired (`tableNotOwned`, which takes the
   locks above) once the lock is let go.

A run's start takes 1 (unversioned) or 2 then 3 (a version) for its
active-run check. The rename reclaim (`Reclaimer.reclaimRenamedTables`)
takes the same locks in the same order (`withRunLock`) to decide and drop,
and takes them only after its rebind, which needs 1: so no run starts while
it decides, and it never waits on a lock while holding one a start needs.

`TableClaims.withTableLock` refuses, under test, to be taken while an
owned-name lock is held (logged otherwise); the package and version locks
live outside `run/` and are not checked.

## Dependency rule

May import: `domain/`, `text/`, the host port (`host.ts`). Must not import
`plan/` or `serve/` (rule R8); the edges that remain are listed, each with
the step that removes it, in `materialization_allowlists.mjs`.

## Where things are added

A new tier is an adapter beside the two strategies, implementing the tier
port: its build, its drop and its key check.

### Add a dialect

A warehouse dialect is one table in two halves, keyed by Malloy's
`dialectName`:

1. A row in `domain/dialect.ts`: its `DialectName`, its capability flags and
   its leaf budget. The record is total, so a missing capability does not
   compile; a dialect with no row has no capability, and whatever asks
   refuses it or rebuilds in full.
2. Its SQL text in `run/warehouse_sql.ts`. Each function narrows the
   connection's `dialectName` to a `DialectName` and switches over every
   registered dialect, so a new row fails to compile until each switch names
   it: the string literal, the relation, view and ownership probes, the
   ownership mark, the delta's transaction, the swap, the native-type catalog
   read. Each switch's `default` says what an unnamed dialect gets. Add it to
   `dialect_statements.spec.ts`'s dialects and regenerate the golden, so its
   statements are pinned, and give it a catalog row in
   `colocated_native_types.spec.ts`.
3. `quoting.ts`, if its identifier quote is not the double quote.
4. If its connection cannot create a table: `READ_ONLY_DIALECTS`
   (`plan/materialization_eligibility.ts`), which must agree with that
   connection type's `canPersist` (`connection_config.ts`).
5. If a `storage=` build reads it through a DuckDB passthrough: a case in
   `wrapPassthrough` (`run/materialization_build_session.ts`). That switch is
   over a connection type (`FederatedSourceType`), not a dialect, though
   `postgres` and `snowflake` are spelled alike in both.

The dialect-literal ratchet in `layer_rules.spec.ts` counts, in every module
here, every string literal that is exactly a registered dialect (quoted any
way, wherever it stands), every bare object key that is one (`{ postgres: 50
}`), and a dialect-named value compared to any other string literal. Comments
and type tests (`typeof x === "string"`) are not code, and `case` labels are
free in `run/warehouse_sql.ts` only. Each count is frozen in
`materialization_allowlists.mjs`, so a new one is a reviewed change. It reads
spellings, not types: a dialect built from parts, or read from a variable, is
not counted.
