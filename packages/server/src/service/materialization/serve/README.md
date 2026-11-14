# `materialization/serve/`

Turns committed manifest entries into what a query may read, and answers a
query from a stored table or declines. Three legs answer: the view leg (a
persisted view called exactly), the rollup leg, and the served copy (the
author's source re-rooted on its stored table, gates and all). Served equals
live, rows, result types and annotations, or the leg declines and the query is
answered live.

## Modules

| Module                               | Responsibility                                                                                                                  | Read by                                                                    |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| `served_copy.ts`                     | The re-root rewriter: a persisted source's declaration pointed at its stored table                                              | the served copy, the rollup and view legs, containment                     |
| `served_copy_serve.ts`               | The served copy as a leg: compile, guard each re-root, answer or decline                                                        | `model.ts`, `query_serving.ts`, the facade, the rollup leg                 |
| `served_copy_containment.ts`         | A re-root that fails to compile steps that source alone back to live                                                            | the served copy                                                            |
| `served_copy_config.ts`              | The connections a served copy compiles and runs with                                                                            | the legs, the facade                                                       |
| `materialization_serve_transform.ts` | The serve bindings of `storage=` tables and the virtual map that reads them, the type mappers, and the build-side lift and gate | `model.ts`, `query_serving.ts`, the facade, the legs, the `storage=` build |
| `storage_bindings.ts`                | A model's `storage=` bindings and the destinations they resolve against                                                         | `model.ts`, `query_serving.ts`                                             |
| `colocated_bindings.ts`              | Serve bindings for colocated `#(materialize)` tables                                                                            | the facade                                                                 |
| `rollup_variants.ts`                 | The rollup leg                                                                                                                  | `model.ts`, `query_serving.ts`                                             |
| `join_rollup.ts`                     | Answering a joined source's measures from its rollup through the join                                                           | the rollup leg                                                             |
| `view_variants.ts`                   | The view leg                                                                                                                    | `model.ts`, `query_serving.ts`                                             |
| `declaring_file_variant.ts`          | The declaring-file overlay the rollup and view legs compile a caller's text over                                                | the rollup and view legs                                                   |
| `author_imports.ts`                  | The `import` lines a variant writes so a caller's text compiles unchanged                                                       | the declaring-file variant, the rollup leg                                 |
| `serve_verdict.ts`                   | Whether each persisted source can be served, and why not                                                                        | `model.ts`, the facade                                                     |
| `query_path.ts`                      | The path report: which leg answered, what it read, why the others declined                                                      | `model.ts`, `model_policy.ts`, `query_serving.ts`, the facade, the legs    |

## Dependency rule

May import: `domain/`, `text/`, governance's gate collection and
`filter_binding_guard`. Must not import `plan/` or `run/` (rule R8). Reads
no warehouse and emits no telemetry (H5); the edges that remain are listed,
each with the step that removes it, in `materialization_allowlists.mjs`.

## Where things are added

A new leg is one module here, an entry in the router's candidate list and
rows in `domain/leg_decline.ts`.
