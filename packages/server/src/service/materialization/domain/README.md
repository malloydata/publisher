# `materialization/domain/`

The vocabulary materialization's layers share: the types a stored table is
described by on its way from plan to run to serve, and the rules that are total
functions of them. Nothing here does I/O, logs, counts, reads configuration or
reads the clock, and every type that crosses the package-load worker is plain
data.

## Modules

| Module                  | Responsibility                                                                                                                                              | Read by                                                                                                                             |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `types.ts`              | The build protocol's types: the wire shapes, the Malloy-facing build manifest and the freshness-aware manifest entry, with the few pure readers of an entry | every layer; the store (`storage/DatabaseInterface.ts`), `model.ts`, `package.ts`, `environment.ts`, the materialization controller |
| `freshness.ts`          | Whether a stored table is fresh enough to read at an instant, and what a stale one does (`fallback`)                                                        | `serve/` (the served copy, rollup, view and storage legs), the facade's serve verdict                                               |
| `storage_table_path.ts` | A storage destination's schema and a stored table's unquoted path in it                                                                                     | the `storage=` build, the reclaimer, `name=` views, the serving legs, `environment.ts`                                              |
| `leg_decline.ts`        | One table of reasons a serving leg declines a query, each with its public `SkipReason` and code                                                             | the three legs, the router (`query_router.ts`, `query_serving.ts`), the path report                                                 |
| `omitted_sources.ts`    | The sources and files a build plan left out because planning them failed (`omittedSources`, `omittedFiles`)                                                 | the companion compilers and the build plan in `plan/`                                                                               |
| `plan_input.ts`         | What a package exposes for its build plan to be compiled                                                                                                    | `plan/build_plan.ts`, the host port (`host.ts`)                                                                                     |
| `chain_build.ts`        | What a chained `storage=` build asks the host port's `chainBuildText` for, and gets back                                                                    | the `storage=` tier, the facade, the served copy                                                                                    |
| `source_aliases.ts`     | Which names share one stored table, and which plan sources a manifest entry stands for                                                                      | the run's plan facts, the facade's serve bindings                                                                                   |

## Dependency rule

May import: itself, `api.ts` types, Malloy's types. Must not import anything
else in `src/` (rule R5 in `eslint.config.mjs`). Anything outside
`materialization/` may import it (R10).

## Where things are added

A concept two layers share lands here, or it is not shared: a new arm of a
union, a row of a table. A new leg decline is a row in `leg_decline.ts`'s
table; its public reason is an existing `SkipReason`, never a new one.
