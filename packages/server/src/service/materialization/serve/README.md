# `materialization/serve/`

Turns committed manifest entries into what a query may read, and answers a
query from a stored table or declines. Four legs answer: the view leg (a
persisted view called exactly), the query leg (a persisted named query called
exactly), the rollup leg, and the served copy (the author's source re-rooted on
its stored table, gates and all). Served equals live, rows, result types and
annotations, or the leg declines and the query is answered live.

Whether an answer a leg produced is used is one decision, `admitRead`
(`domain/admission.ts`): the row scope, the caller's own filters, the result's
order and grouping, and, past an entry-point gate, the gate policy, exactness,
zone and collation. `read_admission.ts` is its shell: it gathers the facts the
decision asks for, each only when asked. The collation probes read the
warehouse, so they sit behind a port (`CollationProbe`, in
`gate_reads_types.ts`); `gate_collation.ts` implements it, and the decision
reads only the probes' answers.

## Modules

| Module                       | Responsibility                                                                                                                                                                                                               | Read by                                                                 |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| `served_copy.ts`             | The re-root rewriter: a persisted source's declaration pointed at its stored table                                                                                                                                           | the served copy, the rollup and view legs, containment                  |
| `served_copy_serve.ts`       | The served copy as a leg: compile, guard each re-root, answer or decline                                                                                                                                                     | `model.ts`, `query_serving.ts`, the facade, the rollup leg              |
| `served_copy_types.ts`       | The served copy's shared shapes: a compiled copy, one answer in progress, a copy's problem                                                                                                                                   | the served copy, its answer checks                                      |
| `served_copy_checks.ts`      | The served copy's answer-time checks: decline a query the copy would not answer as live does (no re-rooted table read, an opted-out source, another time zone, a record join's grain or annotations, a gate column left out) | the served copy                                                         |
| `served_copy_lanes.ts`       | The served copy's compile cache: per lane, which compiled copy a request reuses, rebinds, joins or starts, and which one a lane keeps                                                                                        | the served copy                                                         |
| `served_copy_containment.ts` | A re-root that fails to compile steps that source alone back to live                                                                                                                                                         | the served copy                                                         |
| `served_copy_config.ts`      | The connections a served copy compiles and runs with                                                                                                                                                                         | the legs, the facade                                                    |
| `bindings.ts`                | How serving reads a bound table: its quoted path, the virtual map, whether SQL reads a table (failing closed for gates), `#@ -persist`                                                                                       | the served copy, the rollup and view legs, the bind, storage bindings   |
| `composite_member.ts`        | Whether a served answer resolves a composite source to the member live resolves it to                                                                                                                                        | the served leg (`leg.ts`)                                               |
| `read_admission.ts`          | The admission shell: per query, gathers the facts `admitRead` decides a stored answer by, the collation probes through a port, and maps its verdict to the router's terms                                                    | `query_serving.ts`                                                      |
| `gate_collation.ts`          | Whether a stored copy compares a string column a gate or a re-applied `where:` reads as its source does: the source's collation, read per dialect; the collation probe port's implementation                                 | the admission shell                                                     |
| `gate_paths.ts`              | The field paths an expression reads, shared by the gate reads and the collation rule                                                                                                                                         | `gate_reads.ts`, `gate_collation.ts`, the rollup leg                    |
| `gate_reads.ts`              | What an entry point's row-level gates read, through the fields those are computed from, and those columns' stored types                                                                                                      | the served copy, the rollup leg, the router, the serve verdict          |
| `gate_reads_types.ts`        | The shape of what an entry point's gates and row scope read, shared by the gate reads and the collation rule                                                                                                                 | `gate_reads.ts`, `gate_collation.ts`                                    |
| `result_reads.ts`            | Which strings a query's result orders, groups or compares for equality by, for the check that a `storage=` copy answers them as its source does                                                                              | the admission shell                                                     |
| `bind.ts`                    | The bind: committed entries joined to the plan as bindings and database entries, and what it declines                                                                                                                        | the facade                                                              |
| `storage_bindings.ts`        | What one model reads of a snapshot's `storage=` tables: its own sources' bindings, and stored column types that do not serve as declared                                                                                     | `model.ts`                                                              |
| `native_schema_notices.ts`   | The database tables bound with no captured warehouse type for a big-integer column (`database-native-type-missing`)                                                                                                          | the facade                                                              |
| `leg.ts`                     | A leg (`{ kind, applies, attempt }`), the legs for one pin (`makeLegs`), and the `Answer` every report of a query reads                                                                                                      | the query pipeline (`model_serving.ts`, `query_serving.ts`)             |
| `rollup_variants.ts`         | The rollup leg                                                                                                                                                                                                               | `model.ts`, `query_serving.ts`                                          |
| `join_rollup.ts`             | Answering a joined source's measures from its rollup through the join                                                                                                                                                        | the rollup leg                                                          |
| `view_variants.ts`           | The view leg, and the variant pieces the query leg shares                                                                                                                                                                    | `model.ts`, `query_serving.ts`, the query leg                           |
| `query_variants.ts`          | The query leg: persisted named queries, over the view leg's columns, regroup rule and table lookup                                                                                                                           | `leg.ts` (re-exported for `model.ts`)                                   |
| `stored_typed_source.ts`     | The typed source a persisted result's `storage=` table is read through                                                                                                                                                       | the view and query legs                                                 |
| `variant_compile_cache.ts`   | One leg's compiled variants by key: shared in flight, failures kept, cleared per context and bindings, bounded                                                                                                               | the view, query and rollup legs                                         |
| `declaring_file_variant.ts`  | The declaring-file overlay the rollup and view legs compile a caller's text over                                                                                                                                             | the rollup and view legs                                                |
| `author_imports.ts`          | The `import` lines a variant writes so a caller's text compiles unchanged                                                                                                                                                    | the declaring-file variant, the rollup leg                              |
| `serve_verdict.ts`           | Whether each persisted source can be served, and why not                                                                                                                                                                     | `model.ts`, the facade                                                  |
| `query_path.ts`              | The path report: which leg answered, what it read, why the others declined                                                                                                                                                   | `model.ts`, `model_policy.ts`, `query_serving.ts`, the facade, the legs |

### `collation/`

The collation reads behind `gate_collation.ts`, one reader per Malloy dialect
whose strings can be read. A reader sends SQL through a `ProbeIO` that
`gate_collation.ts` builds over a connection, and never holds one; the
policy (which side is read, what an equality or an ordering needs, the memo
scopes, failing closed and the refusal text) stays in `gate_collation.ts`.

| Module                  | Responsibility                                                                                                                                                                   |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `collation/probe_io.ts` | What a read goes through (`ProbeIO`: rows, and a BigQuery driver's answers), what it gives (`Collation`), and a dialect's reader (`CollationReader`)                             |
| `collation/duckdb.ts`   | The DuckDB dialect's reader: a source and a stored table compare bytes unless the instance sets a `default_collation` or a `COLLATE` appears in the SQL or a definition it holds |

## Dependency rule

May import: `domain/`, `text/`, governance's gate collection and
`filter_binding_guard`. Must not import `plan/` or `run/` (rule R8). Emits no
metrics; three modules log a failed compile once per variant, then at debug
(H5 lists them). The one warehouse read is the collation probe, behind its
port. The edges that remain are listed, each with the step that removes it,
in `materialization_allowlists.mjs`.

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
