# `governance/ir/`

The one reader of Malloy's compiled IR. Malloy keeps its model, source and
query definitions in compiler-internal slots (`_modelDef`, `_sourceDef`,
`_explore`, `_query`); every structural fact the rest of the server needs is
read here, so a Malloy upgrade that moves a slot breaks this directory alone.
Materialization holds itself to that with its IR ratchet
(`materialization/layer_rules.spec.ts`), which allows no IR read outside it.

## Modules

| Module                   | Responsibility                                                                                                                           | Materialization's callers                                                                                                                                                                                                                                 |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `compiled_model.ts`      | The compiler-internal slots of a compiled model or persist source; a source's declared primary key; the time zone a source or stage sets | plan (the build plan, eligibility, dynamic terms, partitions, the companions, the rollup planner), run (the source and storage builds), serve (the served copy, the rollup and view legs, the path report), `stored_column_types.ts`, `workload_shape.ts` |
| `source_graph.ts`        | The source chain: what a source extends, renames or is a query over, and what it reaches                                                 | `text/source_map.ts`                                                                                                                                                                                                                                      |
| `prepared_query.ts`      | A prepared query's shape: its stages, the givens it reads, whether it sets a time zone                                                   | the rollup and view legs, the served copy, the companions                                                                                                                                                                                                 |
| `declared_notes.ts`      | The annotation notes a source's own statement wrote, not ones it inherits                                                                | the build file generator, persisted views                                                                                                                                                                                                                 |
| `pair_flat_query.ts`     | Whether a query over a pair table reads it as live reads its join_many                                                                   | the served copy                                                                                                                                                                                                                                           |
| `joined_field_output.ts` | The output fields a query reads through a join                                                                                           | the served copy                                                                                                                                                                                                                                           |
| `array_query.ts`         | Queries over array fields                                                                                                                | —                                                                                                                                                                                                                                                         |
| `measure_aggregates.ts`  | The aggregates a measure expression is built from                                                                                        | the rollup planner                                                                                                                                                                                                                                        |
| `source_reads.ts`        | The warehouse tables a persist source's build reads; unknown for author SQL                                                              | the build plan's `reads`                                                                                                                                                                                                                                  |

## Dependency rule

Governance code; it imports nothing of materialization but `domain/`
(rule R9). Every module here is pure: it reads what it is handed.
