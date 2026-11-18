# `materialization/serve/`

Turns committed manifest entries into what a query may read, and answers a
query from a stored table or declines. Three legs answer: the view leg (a
persisted view called exactly), the rollup leg, and the served copy (the
author's source re-rooted on its stored table, gates and all). Served equals
live, rows, result types and annotations, or the leg declines and the query is
answered live.

## Modules

| Module                       | Responsibility                                                                                                                                    | Read by                                                                 |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| `served_copy.ts`             | The re-root rewriter: a persisted source's declaration pointed at its stored table                                                                | the served copy, the rollup and view legs, containment                  |
| `served_copy_serve.ts`       | The served copy as a leg: compile, guard each re-root, answer or decline                                                                          | `model.ts`, `query_serving.ts`, the facade, the rollup leg              |
| `served_copy_containment.ts` | A re-root that fails to compile steps that source alone back to live                                                                              | the served copy                                                         |
| `served_copy_config.ts`      | The connections a served copy compiles and runs with                                                                                              | the legs, the facade                                                    |
| `bindings.ts`                | How serving reads a bound table: its quoted path, the virtual map, whether SQL reads a table (failing closed for gates), `#@ -persist`            | the served copy, the rollup and view legs, the bind, storage bindings   |
| `composite_member.ts`        | Whether a served answer resolves a composite source to the member live resolves it to                                                             | the served leg (`leg.ts`)                                               |
| `gate_collation.ts`          | Whether a stored copy compares a string column a gate or a re-applied `where:` reads as its source does: the source's collation, read per dialect | the router                                                              |
| `gate_reads.ts`              | What an entry point's row-level gates read, through the fields those are computed from, and those columns' stored types                           | the served copy, the rollup leg, the router, the serve verdict          |
| `bind.ts`                    | The bind: committed entries joined to the plan as bindings and database entries, and what it declines                                             | the facade                                                              |
| `storage_bindings.ts`        | What one model reads of a snapshot's `storage=` tables: its own sources' bindings, and stored column types that do not serve as declared          | `model.ts`                                                              |
| `native_schema_notices.ts`   | The database tables bound with no captured warehouse type for a big-integer column (`database-native-type-missing`)                               | the facade                                                              |
| `leg.ts`                     | A leg (`{ kind, applies, attempt }`), the legs for one pin (`makeLegs`), and the `Answer` every report of a query reads                           | the query pipeline (`model_serving.ts`, `query_serving.ts`)             |
| `rollup_variants.ts`         | The rollup leg                                                                                                                                    | `model.ts`, `query_serving.ts`                                          |
| `join_rollup.ts`             | Answering a joined source's measures from its rollup through the join                                                                             | the rollup leg                                                          |
| `view_variants.ts`           | The view leg, and the variant pieces the query leg shares                                                                                         | `model.ts`, `query_serving.ts`, the query leg                           |
| `query_variants.ts`          | The query leg: persisted named queries, over the view leg's columns, regroup rule and table lookup                                                | `leg.ts` (re-exported for `model.ts`)                                   |
| `stored_typed_source.ts`     | The typed source a persisted result's `storage=` table is read through                                                                            | the view and query legs                                                 |
| `declaring_file_variant.ts`  | The declaring-file overlay the rollup and view legs compile a caller's text over                                                                  | the rollup and view legs                                                |
| `author_imports.ts`          | The `import` lines a variant writes so a caller's text compiles unchanged                                                                         | the declaring-file variant, the rollup leg                              |
| `serve_verdict.ts`           | Whether each persisted source can be served, and why not                                                                                          | `model.ts`, the facade                                                  |
| `query_path.ts`              | The path report: which leg answered, what it read, why the others declined                                                                        | `model.ts`, `model_policy.ts`, `query_serving.ts`, the facade, the legs |

## Dependency rule

May import: `domain/`, `text/`, governance's gate collection and
`filter_binding_guard`. Must not import `plan/` or `run/` (rule R8). Reads
no warehouse and emits no telemetry (H5); the edges that remain are listed,
each with the step that removes it, in `materialization_allowlists.mjs`.

Every leg reads tables through one pin (`domain/binding.ts`): the query
pipeline pins the model's snapshot once, before routing, and passes it to each
leg, the run and the path report. No module here reads the clock or holds
bindings of its own; a leg's compile cache follows the snapshot's bindings by
identity. The served copy's is keyed instead on what its compile reads of the
bindings it may read (everything but their freshness), so a rebind that moves
only a table's data-as-of instant reuses the compiled copy, rebound to the
pin's bindings.

The served copy compiles each pass on the package-load worker when one is
ready and idle (`package_load/served_copy_compile.ts`, through the host's
`compileElsewhere`), and hydrates the compiled model here; with no worker free,
or a job that fails, the pass compiles inline. The worker's compile is the
inline one, so the copy, its gates and its answers are the same either way.

## Where things are added

A new leg is one module here implementing `Leg` (`leg.ts`), an entry in
`makeLegs`, and rows in `domain/leg_decline.ts`.
