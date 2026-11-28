# The dbt roadmap

> What this is: the technical plan for running Malloy and Publisher alongside dbt. It covers what
> stays in dbt and what can move, the worked example the plan is built around, the developer loop,
> the open-source primitives Publisher provides, and the phases that deliver them. Nothing in the
> phases below is built yet unless a row says it has shipped. The
> [package format](packages.md), [materialization](materialization.md),
> [pre-aggregation](preaggregation.md), and [classification](classification.md) docs describe the
> shipped pieces this plan builds on; [materialization-migration.md](materialization-migration.md)
> maps the v1 annotations onto materialization v2's `#(materialize)` and `#(rollup)`.

## The goal

A dbt developer should be able to:

1. take a realistic dbt project;
2. put a governed Malloy data model on top of it in minutes;
3. work in one loop where `dbt build` and Publisher stay in step;
4. end up with metrics that are queryable by people and agents, with joins answered correctly and
   access rules in the model, using only open-source software.

**The framing: dbt does the heavy lifting, and the governed data model is the layer you evolve.**

- **dbt keeps** the transformation work that is expensive, stable and rarely changes shape:
  - landing, cleansing and entity resolution;
  - unions and unpivots;
  - heavy multi-way fact building;
  - SCD2 snapshots;
  - data tests.
- **The governed Malloy data model takes the last mile,** the part that changes every time a
  question changes:
  - marts that aggregate a fact, metrics and dashboard aggregates;
  - serving tables;
  - access rules, classification and curation for agents.
- **There, change is cheap:**
  - a new measure or view needs no rebuild;
  - the model chooses what to materialize, and queries route to the stored tables automatically;
  - access controls and classification live in the model;
  - agents get a curated, documented surface.
- **Decomposing is a choice per layer, not a migration.** The guide below says what stays in dbt as
  clearly as what can move. A team can stop at "Malloy on top of every dbt mart" and change nothing
  in dbt.

## Ground rules

- **Publisher never runs dbt.** It reads dbt's artifacts (`semantic_manifest.json`,
  `manifest.json`, `catalog.json`, `run_results.json`) and accepts a signal when a dbt run
  finishes, wherever that run happened. It never invokes dbt or holds dbt's credentials.
- **Open source first.** Capability lands in the open-source Publisher: anything that defines
  _what_ the system can do (a declaration surface, a build mechanic, a check). A managed offering
  can host dbt runs on top of these primitives, and add scheduling against freshness objectives,
  cross-version reuse and fleet operations. Every step of the developer loop below runs from the
  open-source Publisher alone, with no account.
- **Access controls hold only behind a gateway.** Every governance claim in this plan, from
  `#(access_filter)` to gated materialization, is enforced only when an authenticating gateway sets
  the givens and is the only way to reach Publisher
  ([operating model](security-posture.md#operating-model-a-microservice-behind-your-gateway)).

**The single-node floor, as shipped.** A standalone Publisher already runs the whole
materialization loop on one node: an opt-in cron scheduler
([the standalone scheduler](materialization.md#the-standalone-scheduler)); a local run store and
incremental ledger, with an API to hand the ledger back from outside
([supplying the ledger yourself](materialization.md#supplying-the-ledger-yourself)); freshness
windows and fallbacks that age a table at serve time
([freshness](materialization.md#freshness-a-window-and-a-fallback)); an auto-run that reclaims a
renamed table it can prove is its own and lists the ones it keeps
([the rename rule](materialization.md#the-rename-rule)); and a guarded drop route
([the drop route](materialization.md#the-drop-route)). What stays with a hosted control plane: the
two-phase build that also builds indexes, scheduling refresh against freshness objectives rather
than a cron, and distributing manifests across a fleet
([standalone vs. hosted](materialization.md#standalone-vs-hosted-control-plane-deployments)). The
hooks for a host are open: `buildInstructions` on a run, a bound `manifestLocation`, and the ledger
echo.

**The `malloy-pub` CLI is not installable yet.** It lives in [`packages/cli`](../packages/cli) and
is documented in [manage it with the CLI](materialization.md#manage-it-with-the-cli), but it is not
published to npm, so today it runs only from a clone. Publishing it is P0, because most of the
developer loop reaches users through it.

## The decomposition guide: what stays in dbt and what can move

Every row reflects shipped Publisher behaviour.

| dbt layer                                                                 | Recommendation                                | Why                                                                                                                                                                                                                                                                                                                 |
| ------------------------------------------------------------------------- | --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Sources, EL, raw landing**                                              | Stay in dbt or your EL tool                   | A deliberate non-goal.                                                                                                                                                                                                                                                                                              |
| **Staging** (cleansing, entity resolution, unions, unpivots, date spines) | **Stay in dbt**                               | Malloy has no set operations or non-linear pipelines. A `connection.sql()` source can carry staging SQL verbatim, but build ordering between SQL sources is unproven (see [staging](#staging-sql-sources-now-language-later)).                                                                                      |
| **Snapshots (SCD2)**                                                      | **Stay in dbt**                               | Not built in Malloy. Read dbt's snapshot tables as sources (see [SCD2](#state-scd2-as-a-realization-mode)).                                                                                                                                                                                                         |
| **Intermediate / fact building** (heavy multi-way joins)                  | **Mostly dbt**                                | Malloy owns the joins it reads by reference at query time. A wide fan-in DAG of intermediate tables stays in dbt.                                                                                                                                                                                                   |
| **Marts that aggregate a fact** (`group_by` + measures)                   | **Can move: the sweet spot**                  | Two routes, both routed automatically (queries unchanged, and the [path report](materialization.md#which-table-answered-a-query-the-path-report) names the table): `#(rollup) grain=[…]` on the fact's measures, where the smallest covering rollup answers, or a `-> { group_by … }` source with `#(materialize)`. |
| **Incremental marts**                                                     | Can move on **Postgres, BigQuery, Snowflake** | Watermark, merge key and lookback ([incremental refresh](materialization.md#incremental-refresh)). Not on DuckDB or Databricks.                                                                                                                                                                                     |
| **Metrics / semantic layer**                                              | **Move**                                      | Measures, joins, nesting and governance; the importer is P2.                                                                                                                                                                                                                                                        |
| **Exposures, BI extracts, dashboard tables**                              | **Move**                                      | [Reports](reports.md), persisted named queries (a dashboard's result answers every spelling of it), `inline=` tables, MCP and [data apps](html-data-apps.md).                                                                                                                                                       |
| **Tests, contracts, seeds**                                               | **Tests stay in dbt.** Contracts are shared.  | No data-test framework in Malloy yet. Contracts: `compile_model`, the [serve verdict](materialization.md#will-it-serve-the-serve-verdict), column contracts on stored tables, `#(carries)`.                                                                                                                         |
| **Per-tenant variance**                                                   | Outside both, for now                         | Package variants are planned, not built.                                                                                                                                                                                                                                                                            |

**What the layer you evolve buys you,** each tied to a shipped feature:

1. **Change without a rebuild.**
   - A stored table's identity is a content address, not its name.
   - Adding measures, views, joins or extensions over a stored source needs no rebuild, because the
     served copy re-evaluates them at read.
   - Rollups only accelerate: an unbuilt or stale rollup answers from the base.
   - `compile_model` at package scope, and a `dryRun` publish, preview an edit.
2. **Governance in the model.**
   - `index.malloy` [curates the published surface](discovery-and-access.md);
   - [`#(authorize)`](authorize.md) and [`#(access_filter)`](row-level-access.md) on
     [givens](givens.md);
   - [`#(classification)` and `#(carries)`](classification.md);
   - one [access record](access-record.md) per query.
   - Gates survive materialization only with `PUBLISHER_MATERIALIZE_GATED_SOURCES` on
     ([configuration](configuration.md)).
3. **Ready for agents.**
   - `get_context` grounding over MCP, curation, and `#(doc)` (carried from dbt descriptions by the
     importer);
   - stable warning codes where agents retrieve;
   - the eval loop.
4. **Cost and performance.**
   - Materialize only what published sources reach (the `unreachable` verdict);
   - small rollups serve dashboards, and persisted queries back report tiles;
   - DuckLake storage offload (`storage=`, behind a flag);
   - billing tags on every statement.

## Where Malloy and Publisher stand against dbt's capabilities

Scored against what teams use dbt for, weighted by usage in production deployments running the
dbt → Malloy pattern today. "The platform" means the open-source Publisher plus, where a row says
so, a hosted control plane.

| dbt capability                                                                      | Where Malloy and Publisher stand                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | Verdict                                             |
| ----------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------- |
| DAG, `ref()`, build order, selective builds                                         | Run and build-plan DAG, scoped reruns (`sourceNames`), content-addressed reuse, chained builds (a stored table builds from the stored tables it reads, across files), cross-connection destinations (`storage=`), a guarded drop route, and an auto-run that reclaims a renamed table it can prove is its own. Scheduling against freshness objectives is a hosted control plane's; standalone, the scheduler is a cron                                                                                                                                                                                                                                                    | **Closed**                                          |
| Incremental models                                                                  | `refresh="incremental"` with `watermark=`/`merge_key=` on Postgres, BigQuery and Snowflake sources, into the warehouse or a `storage=` destination. Each delta starts at the newest stored watermark, `lookback=` re-reads a window, rows a delta never read are reported (`lateRows`, `persist-incremental-late-rows`), and without `merge_key=` a refresh replaces the whole range it re-reads, so deletions in it carry through. On other dialects the source is refused and answered live until it declares `refresh="full"`                                                                                                                                           | **Closed** (Postgres, BigQuery, Snowflake)          |
| Materialization configs                                                             | One annotation: `#(materialize)` on a source (optionally `storage="…"`), `#(rollup) grain=[…]` on a measure, `#@ persist` on a view or a named query, `#(index)` on a dimension. `name=` is optional and only places the table; without it Publisher picks a readable name, and every database table a build writes carries its package's ownership mark. `#@ preaggregate` is deprecated for `#(rollup)`; v1 `#@ persist` keeps working                                                                                                                                                                                                                                   | **Closed**                                          |
| Test framework (severities, scoping)                                                | Nothing today. Data tests: design in progress. Unit tests (fixture → expected result): cheap once source rebinding lands                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | Open · design in progress                           |
| Enforced contracts                                                                  | _Downstream, in policy terms:_ `#(carries)` on an entry point refuses the package load when any field reachable from it carries a [class](classification.md) the list omits, or none (a preview). _Pre-merge:_ `compile_model` at `package` scope (and a `dryRun` publish) compiles every file as a reload would, without serving the result; the serve verdict and path report say whether a stored table will answer. Still open: _upstream_ (warehouse → model), where compile catches shape errors deep in a model rather than at a named boundary; and the output-space contract, where a removed view field compiles clean and breaks dashboards and agents silently | Partly closed · the rest lands in S4, P4 and `lint` |
| Seeds                                                                               | Package-local CSVs already work: the per-package duckdb reads `.csv`/`.xlsx` in place, versioned with the package. Missing: joinability. The CSV sits on the sandbox connection and the marts on the warehouse connection, and Malloy can't join across them. `storage=` does not close it: a `storage=` build refuses a sandbox source (`storage_source_unsupported`), and the join is refused at compile (`join-connection-mismatch`)                                                                                                                                                                                                                                    | **Mostly closed**                                   |
| SCD2 snapshots                                                                      | The incremental machinery (watermark ledger, stable physical naming, transactional in-place advance, retry discipline) is most of the write path. But the shipped modes can't hold history (`merge` overwrites the prior value; a no-key refresh replaces the range it re-reads), and the cache lifecycle discards accumulated observations. The lifecycle footing exists: stored tables keyed by content address, an ownership mark on every database table a build writes, and a reclaim that never drops a table at an author's `name=` (outside `scope: "version"`) and lists every table it keeps                                                                     | Open · a write path and lifecycle exemptions        |
| Staging query shapes (`UNION ALL`, correlated date explosion, non-linear pipelines) | `connection.sql()` sources are already persistable, so staging SQL can be lifted verbatim                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Open · bridged; closed only by language work        |
| Feature-flag DAG pruning, dispatch hooks, templating                                | Givens cover value substitution; per-tenant variance requires an external build step today                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | Open · package variants                             |
| Macros and packages (reuse across projects and tenants)                             | npm-style package dependencies for Malloy packages are being scoped; override sources (a package declares a typed default, a consumer rebinds it) follow; expression-level macros are language work                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | Open · package dependencies and override sources    |

The remaining gap is four things: governance (tests, contracts, seeds), state (SCD2), staging
expressiveness, and variance. That is why the decomposition guide keeps staging, snapshots and tests
in dbt.

### Stored answers you can trust

Moving a mart into the model is worth doing only if the stored answer is the answer. The rule is
strict: **a stored table answers only when Publisher can show it matches live, and otherwise the
query answers live**; the known exceptions are listed below. Every result carries a
[path report](materialization.md#which-table-answered-a-query-the-path-report) naming the table
that answered and why every other candidate did not. What the check covers today:

- **Row-level gates.** A gated source's table is admitted only when its gate is proven to be the
  entry point's own row filter or `#(authorize)` lock, and the gate is grafted onto the stored table
  and proven again on every read, exactly as on live
  ([gated sources](materialization.md#access_filter-gated-sources-and-materialization)).
- **Collation.** For a Postgres or DuckDB source, a string compares on the stored table as the source
  compares it, or the query answers live (BigQuery compares bytes on both sides; a Snowflake column's
  collation is not yet judged for ordering). A `storage=` copy in DuckLake compares bytes, so a query
  whose result depends on a string's order (an `order_by:`, a top-N, a `min`/`max`, a range
  comparison) over a source whose collation is not binary answers live, as does an equality under a
  nondeterministic collation. A Postgres database-tier table read past a gate or under a scope is
  checked against its source's collation on every answer, matched by identity (provider, locales,
  library version), not by name.
- **Numbers and time.** A Postgres `numeric` is stored exactly or answered live; a big integer past
  2^53 is served with live's JSON type; a `storage=` table is never read past a gate over a column it
  holds as a float; and a value stored in the build's time zone answers live under a query that sets
  its own `timezone:`
  ([stored column types](materialization.md#stored-column-types-follow-the-model-not-the-warehouse)).
- **Served equals live, checked.** A nightly scenario suite runs a real Publisher against a
  throwaway Postgres and a local DuckLake under both settings of the materialization v2 switch,
  asserting each answer's rows and the tier that produced it.

The honest costs. On a `storage=` copy of a Postgres source with a non-binary collation, queries that
order by a string or compare string ranges answer live; under a nondeterministic ICU collation,
equality, joins and `count(x)` answer live too. A `sum` of a Postgres `real` served from a `storage=`
table can differ from live in its last digit, and `freshness.fallback="stale_ok"` serves a table past
its window by design ([freshness](materialization.md#freshness-a-window-and-a-fallback)). The
executed served-against-live evidence is DuckDB, plus Postgres-to-DuckLake through the nightly suite;
BigQuery, Snowflake, Databricks and MotherDuck have none yet
([the v2 plan](materialization-v2-plan.md)).

## The example: jaffle-shop

**The base pipeline:** [`dbt-labs/jaffle-shop`](https://github.com/dbt-labs/jaffle-shop) `main`,
pinned to a commit, on DuckDB.

- It is dbt Labs' canonical sandbox, and the only maintained example with metrics in the current
  semantic-layer spec: 6 semantic models, 23 metrics (simple, ratio, derived with a one-month offset,
  cumulative) and 3 saved queries.
- It builds with no credentials on dbt-core 1.12 and on dbt-oss 2.x, and `mf query` answers its
  metrics locally.
- **Data:** the repo's one year of seeds, or several years generated with jafgen (Apache-2.0) for
  seasonality. The generated set is preferred.

**Cloned at setup, never vendored.** jaffle-shop carries no license, so none of its files are
committed to this repo. The example's setup script clones it at the pinned commit. The fallback, if
that ever stops being workable: Apache-licensed dbt models of our own over jafgen data, starting from
the `dbt init` jaffle template.

**Toolchain:**

- **dbt-core 1.12, dbt-duckdb and dbt-metricflow** as the default: Apache throughout, with `mf` for
  reconciliation in one environment, run with `--no-version-check`.
- **dbt-oss 2.x** as a second CI job. Its Info Schema Parquet files are directly readable by Malloy
  over DuckDB. The default switches once 2.x is the norm and emits Ossie.
- **Not** the proprietary `dbt` package.

**Where it lives:** `examples/dbt-jaffle-shop/`, a scaffolded workspace:

- `setup.sh`: clone at the pinned commit, generate data, `dbt build`;
- the Malloy package over the dbt marts;
- `reconcile/`: the reconciliation harness, which CI runs;
- `showcase.malloy`: a report demonstrating nested drill-down, a fan-out join, a rollup serving a
  dashboard, and a givens-gated view;
- a README that walks the developer loop.

No dbt code or data is committed; it is all generated at setup.

**One target name switches between local and cloud.** The same example runs on a local DuckDB file
or on MotherDuck (including a DuckLake database there) by changing one target name, on both halves:

- **dbt:** `profiles.yml` has a `local` target (a DuckDB file) and a `motherduck` target (an `md:`
  path, with the token read from `MOTHERDUCK_TOKEN`); `--target` or `DBT_TARGET` picks one.
- **Publisher:** the package's connection config names the same target: a `duckdb` connection for
  `local`, a `motherduck` connection for the cloud. `dbt.target` records which target the bindings
  were generated against.
- **Local is the default, with zero credentials.** A clean clone, `setup.sh` and `npm start` need no
  account. The cloud target is opt-in, and its CI job runs only when a token is configured.
- The Malloy model does not change between targets: the importer writes logical `#(dbt) unique_id`
  references, so only the connection and the schema resolve differently.

**Two configurations of the same project, side by side:**

- **A, "Malloy on top of every mart":** dbt builds every jaffle model, and Malloy sits on all the
  marts with the 23 metrics. Nothing in dbt changes. This is the on-ramp.
- **B, "decomposed":**
  - dbt keeps staging, the fact models (`orders`, `order_items`) and the dimensions;
  - the aggregate mart `customers` (lifetime order count, spend, first and last order, customer
    type) becomes a Malloy source with `#(materialize)`;
  - the dashboard grains become `#(rollup)`s on the order facts;
  - governance and curation are added in the model: `#(classification)` on customer PII, a
    store-manager given with `#(access_filter)`, `index.malloy` curation and `#(doc)` from dbt
    descriptions;
  - the example sets `PUBLISHER_MATERIALIZE_GATED_SOURCES=on`, so governed sources are served from
    stored tables with their gates grafted on and re-proven. The README explains the at-rest caveat
    (a stored table holds every caller's rows) and the gateway caveat.

  `dbt build` gets smaller, and the last mile is served from stored tables with queries unchanged.

- **Checks:**
  - A and B reconcile with each other and with `mf query`;
  - served equals live per given, for each store manager;
  - the path report shows which stored table answered.
- **To verify in P1:** that the `customers` mart's logic is a clean `group_by` over orders. If it
  isn't, configuration B moves the mart that is.

## The developer loop

```
dbt build ──► DuckDB marts ──► malloy-pub dbt import ──► bindings/ + generated/ (+ hand-edited)
    │                                 (once, or on demand)                 │
    │                                                                      ▼
    └──── on every dbt build ──► malloy-pub dbt refresh ──► served via REST / MCP / Console
                                  (table-changes: schema refetch              │
                                   + scoped materialize)                      ▼
CI: dbt build → malloy-pub dbt check → publisher --check → reconcile (mf query vs Malloy)
    → malloy-pub dbt exposures
```

| Step       | What the developer does                                                                  | What exists                                                                                                | What to build                                                                                                               |
| ---------- | ---------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| 1. Start   | `npm create @malloy-publisher/malloy-package@latest jaffle -- --dbt ../jaffle-shop`      | the [scaffolder](scaffolding.md), with `--data` only                                                       | a `--dbt <project>` flag that takes a project directory, finds the DuckDB target and seeds the package                      |
| 2. Model   | Get a Malloy source per dbt mart, joins from dbt entities, and measures from dbt metrics | `search_database_schema` gives `source:` lines; nothing reads dbt                                          | **the importer (P2):** `malloy-pub dbt import` writes `bindings/` and `generated/`                                          |
| 3. Docs    | dbt `description:`s appear as `#(doc)` and in `get_context`                              | BigQuery `persist_docs` only                                                                               | the importer writes `#(doc)` from `manifest.json`                                                                           |
| 4. Iterate | Edit and save; watch mode recompiles; ask questions over MCP                             | works today (watch mode, `compile_model`, `reload_package`)                                                | —                                                                                                                           |
| 5. Rebuild | `dbt build` changes a mart, and Publisher picks it up                                    | an explicit reload or publish refetches table schemas; the scoped materialize endpoint takes `sourceNames` | `malloy-pub dbt refresh` over `table-changes` (S6): refetch + scoped materialize for the sources reading the changed models |
| 6. CI      | One PR check covers dbt and Malloy                                                       | `compile_model` at package scope; `publisher --check` is open as a draft (#1934)                           | `malloy-pub dbt check`, and `publisher --check` landed with the shared result format, plus the reconcile step               |
| 7. Lineage | Malloy sources and reports show in dbt's DAG                                             | —                                                                                                          | `malloy-pub dbt exposures` (S8)                                                                                             |
| 8. Drift   | A renamed or dropped dbt column is caught before it breaks a query                       | the column contract on materialized tables                                                                 | `malloy-pub dbt check` (`dbt-column-removed` and the other drift codes); column pins in P4                                  |

### The shared check result format

Every front end that answers "is this right?" reports the same way: `publisher --check`,
`malloy-pub dbt check`, the findings of `malloy-pub dbt import`, the verdicts of
`malloy-pub dbt refresh`, and later `malloy-pub lint`.

| Exit    | Meaning                                                                                                                                                          |
| ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `0`     | checked, and nothing fails                                                                                                                                       |
| `1`     | checked, and at least one finding fails: an `error`, or a `warn` under `--fail-on-warnings`                                                                      |
| `2`     | could not check (a bad manifest, an unsupported selector, an empty selection, a bad config): the answer is unknown, never "clean", and a finding never exits `2` |
| `128+n` | interrupted by signal `n`                                                                                                                                        |

The table covers check commands only; `malloy-pub materialize --wait` keeps its own `2` ("built, not
served").

- **`--format text|github|json`.** The default is `github` when `GITHUB_ACTIONS=true`, otherwise
  `text`. `text` prints one finding per line on stdout,
  `<file>:<line>:<col>: error|warning: <message> (<code>) [<scope>]`, where `<scope>` is `env/pkg`
  for the server and the dbt `unique_id` for the offline commands; logs, summaries and diffs go to
  stderr, so stdout stays parseable. `github` adds an `::error`/`::warning` annotation after each
  line. The server spells the flags `--check-format` / `PUBLISHER_CHECK_FORMAT` and
  `--check-fail-on-warnings`, because its settings are prefixed.
- **JSON** is one document on stdout on every exit path, `2` included: `formatVersion` (1), `tool`,
  `status` (`ok` | `failed` | `error`, matching exit 0 | 1 | 2), `summary` (`errors`, `warnings`),
  `problems[]` and `error` (`{ code, message }` when it could not check). Each problem carries
  `severity` (`error` | `warn`, the API's spelling), a required `code`, `message`, an optional
  position (`file`, `line`, `column`, `endLine`, `endColumn`, 1-based), `environment` and `package`
  (server only), `model` and `subject` (as on a package warning), `upstream` (`{ tool, id }`, for
  example `{ "tool": "dbt", "id": "model.jaffle.fct_orders" }`) and a per-code `detail` object.
- **Codes** share one kebab-case namespace with the package warning codes: compiler and package
  warning codes pass through unchanged; `package-load-failed`, `check-config-missing`,
  `check-config-invalid`, `check-storage-failed` and `check-interrupted` are the check's own; and the
  dbt family is `dbt-*` (listed under [the names](#the-names)). `--allow <code>` demotes a code to
  `warn`; v1 accepts only `dbt-doc-changed`.
- **Drift is an `error` in a check and a `warn` as a package warning at serve time,** so it never
  blocks serving on its own.

## The open-source primitives

A single package or repo can hold the dbt project and the Malloy models and stay in sync on every
edit, run and publish. Publisher provides each piece as an open primitive. A managed offering can
host dbt runs on top of these primitives; Publisher itself never runs dbt, and a dbt run, wherever
it happens, signals Publisher.

| #   | Capability                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | What it enables                                                                                                                                                         | Status                                                                                                                                                                                                                                              |
| --- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| S1  | **A dbt project inside a package.** A `dbt/` directory beside the Malloy files, and a `dbt` block in `publisher.json` (keys under [the names](#the-names)). The key set is closed, like `persist`'s: an unknown key warns `dbt-unknown-key`, and keys a managed offering uses are reserved by name. Publisher ignores the dbt sources when loading models.                                                                                                                                                                                                                                                   | One repo, one package                                                                                                                                                   | design                                                                                                                                                                                                                                              |
| S2  | **Committed code generation, checked for currency.** One generator, `malloy-pub dbt import`, writes `bindings/` (one relation per dbt node) and `generated/` (the semantics), which hand-written files import and extend. The one check, `malloy-pub dbt check`, writes nothing and fails with `dbt-generated-stale` when they are stale against the current manifests.                                                                                                                                                                                                                                      | A dbt metric change can't merge without its Malloy counterpart                                                                                                          | with P2                                                                                                                                                                                                                                             |
| S3  | **Manifest-pinned versions.** At publish, Publisher records `Package.dbt { manifestHash, dbtVersion }` on the package version, and the package's `/status` entry shows `dbt.manifestHash`. The project's git ref is already covered by `contentHash`, `gitRef` and `gitCommitSha`; `dbt.gitCommitSha` is added only when `dbt.project` lies outside the package.                                                                                                                                                                                                                                             | A version pins both halves, rollback restores both, and a mismatch between what dbt built and what Malloy expects is visible. One branch, one pin, with no hand pairing | design                                                                                                                                                                                                                                              |
| S4  | **Manifest-at-publish validation.** A publish, or a `dryRun` publish, given a dbt manifest checks that every dbt relation the model reads exists with compatible columns, and refuses or warns with stable codes (`dbt-relation-missing`, `dbt-column-removed`, `dbt-column-retyped`, …).                                                                                                                                                                                                                                                                                                                    | No publish that serves against tables dbt hasn't built, or has changed                                                                                                  | design; uses the shared check format                                                                                                                                                                                                                |
| S5  | **dbt-relation lineage.** The build plan answers which Malloy sources and stored tables read each dbt relation.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | `table-changes` knows exactly what to rebuild, and lineage spans the dbt DAG and Malloy sources                                                                         | **Mostly shipped.** `PersistSourcePlan.reads` and `upstreamSchemas` are on the build plan ([`api-doc.yaml`](../api-doc.yaml)). What remains is mapping dbt `unique_id`s to those table paths, which the CLI does from `bindings/`. No new API name. |
| S6  | **A run-finished hook.** `POST …/packages/{pkg}/table-changes` with `{changedTables[], failedTables[]}` (items shaped like `TableRead`) answers 202 with the started materialization. It refetches the schemas of the changed tables (S11), re-checks contracts, and rebuilds exactly the dependent sources (via S5) with `forceRefresh`. It is dbt-agnostic, so dlt or Airflow can call it; `malloy-pub dbt refresh --run-results target/run_results.json` is its dbt client. Failed or skipped dbt models leave their dependents serving their last good tables, with a `dbt-upstream-run-failed` warning. | "dbt ran, so Malloy is fresh", from any orchestrator. One run-driven pickup covers metadata and warehouse schema together                                               | The scoped materialize endpoint exists; the endpoint and the mapping are new. A hosted control plane can drive the same mechanics from its own events.                                                                                              |
| S7  | **Consistency during a run, for stored tables.** While a dbt run is in progress, a materialized source whose dbt relations are mid-rebuild keeps serving its last good stored table, or answers live with the serve-verdict reason `upstream_rebuilding`.                                                                                                                                                                                                                                                                                                                                                    | No window where a stored table is rebuilt from half-built dbt tables                                                                                                    | Partly shipped (column contracts, freshness, serve verdicts); the "run in progress" signal and `upstream_rebuilding` are new. It can't protect live sources, which read dbt's tables directly mid-run; only S10 can.                                |
| S8  | **Exposures write-back.** `malloy-pub dbt exposures` writes dbt `exposures:` YAML for Malloy sources (semantic entry points, not only dashboards), reports and data apps, so they appear in dbt's DAG and `dbt build -s +exposure:…` works.                                                                                                                                                                                                                                                                                                                                                                  | Two-way lineage                                                                                                                                                         | P3                                                                                                                                                                                                                                                  |
| S9  | **dbt tests as signals.** Read `run_results.json` test outcomes. A failing test on a relation a source reads surfaces as a package warning, and optionally a verdict, with dbt's severities.                                                                                                                                                                                                                                                                                                                                                                                                                 | Quality gates span both halves                                                                                                                                          | later, with P4                                                                                                                                                                                                                                      |
| S10 | **Schema-parameterized bindings, with deferral.** A source's logical `#(dbt) unique_id` references resolve against a schema chosen per package version or environment (e.g. `dbt_v8`), not a hard-coded one, and fall back to the serving schema for relations a version didn't rebuild. `dbt.schemaMode` chooses one shared schema or a schema per version. See [code moves forward, data stays put](#code-moves-forward-data-stays-put).                                                                                                                                                                   | Per-version isolation: one version's dbt run never rewrites tables another version serves; promote switches schemas atomically                                          | **New.** Overlaps environment-resolved bindings for package variants. The mechanism's name waits for its design. Until it lands, versions share one schema.                                                                                         |
| S11 | **Schema refetch scoped to changed tables.** Refetch only the relations a dbt run changed, not every table a package reads.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Fast, cheap pickup after each dbt run                                                                                                                                   | **New.** Today a reload refetches all of a package's schemas.                                                                                                                                                                                       |
| S12 | **Eject to dbt.** `malloy-pub dbt eject <source>` generates a dbt model from a Malloy source, rewrites the source to read it in the same change, and proves equal results with the reconciliation harness. It never overwrites an existing dbt model by name.                                                                                                                                                                                                                                                                                                                                                | Logic moves both ways: down into the model when it needs to change fast, up into dbt when it hardens                                                                    | **New.** After P2 and S10. Needs a Malloy → SQL model generator (`PersistSource.getSQL()` already produces the bare SELECT).                                                                                                                        |

**Design rules that cut across the table:**

- **Logical references from the first release.** The importer writes logical dbt references
  (`#(dbt) unique_id`, resolved per environment), never hard-coded schema paths, so S10 needs no
  migration of existing packages later.
- **Drift never blocks on its own.** Drift findings are issues with stable codes (note, warning,
  error) that never block serving by themselves, matching the warning codes and serve verdicts
  Publisher already has. Deliberate ignores are committed in code, and a refresh never auto-commits
  changes to the model.
- **The upstream ladder.** A query → a Malloy source → `#(materialize)` → a dbt model, once logic
  has stabilised. S12 is the last rung.
- **New public API fields** (S3's `Package.dbt`, S6's `table-changes`, S7's `upstream_rebuilding`)
  also go into the [`/api/v1` spec](publisher-v1.md).

### Code moves forward, data stays put

The pattern S10 implements, as teams on MotherDuck and other warehouses already run dbt:

- **Environments are separated by write target and credential, not by copying data.** Dev, CI and
  production each write to their own target (a database or schema) with their own credential, and
  the production credential never sits in a developer's environment file.
- **Production inputs are read read-only,** through shares or grants. Every environment reads the
  same upstream data in place; only its own target is writable.
- **A schema per version or pull request is the baseline.** A pull request builds into `ci_pr123`,
  a package version into `dbt_v8`, and promotion runs the same code against the next target.
- **Zero-copy clones and snapshots are optional optimisations,** where the warehouse has them
  (MotherDuck's `CREATE DATABASE … FROM`, Snowflake's clones, `dbt clone`): a clone seeds a version's
  schema cheaply, and a named snapshot gives rollback. S10 works without either.
- **It composes with deferral:** a version's schema holds only what it rebuilt, and reads the
  serving schema for the rest.

### The names

Public names for the dbt integration, fixed ahead of the code so nothing ships under a placeholder.

- **CLI** (package `@malloy-publisher/cli`, binary `malloy-pub`): `dbt import` (one generator,
  writing `bindings/` and `generated/`), `dbt check` (the one check; it writes nothing),
  `dbt refresh --run-results <path> --environment <e> [--package <p>]`, `dbt eject <source>`,
  `dbt exposures`, and `materialize --source <name...>` (repeatable, validated against the build
  plan). `--manifest`, `--semantic-manifest`, `--catalog`, `--select`, `--exclude`, `--connection`
  and `--package-dir` override the `dbt` block. The scaffolder takes `--dbt <project>`.
- **The `publisher.json` `dbt` block,** a closed key set: `dbt.project` (path to the dbt project,
  default `dbt`; the artifact paths default from it), `dbt.target`, `dbt.select`, `dbt.exclude`,
  `dbt.manifest`, `dbt.semanticManifest`, `dbt.catalog`, `dbt.connection`, `dbt.bindings`,
  `dbt.generated` (default `generated`), `dbt.columns`, `dbt.docs`, `dbt.classification.metaKey`,
  `dbt.naming.prefix`, `dbt.naming.overrides`, `dbt.engine` (e.g. `"dbt-core@1.12"`, not `version`,
  which already means the package's semver), and `dbt.schemaMode` (S10). An unknown key warns
  `dbt-unknown-key`.
- **In the model:** `#(dbt) unique_id="…"` ties a source to its dbt node as a logical reference;
  `#(dbt) type="…"` records a column's type; dbt `meta.malloy.access` carries access from dbt.
- **API:** `Package.dbt { manifestHash, dbtVersion }`, `/status`'s `dbt.manifestHash`,
  `POST …/packages/{pkg}/table-changes`, and the serve-verdict reason `upstream_rebuilding`.
- **Codes:**
  - drift: `dbt-binding-added`, `dbt-binding-removed`, `dbt-relation-changed`, `dbt-column-added`,
    `dbt-column-removed`, `dbt-column-retyped`, `dbt-doc-changed`, `dbt-classification-changed`,
    `dbt-access-changed`, `dbt-pin-mode-changed`;
  - importer: `dbt-generated-stale`, `dbt-metric-approximated`, `dbt-metric-skipped`;
  - could not check (exit 2): `dbt-manifest-unreadable`, `dbt-manifest-version-unsupported`,
    `dbt-selector-unsupported`, `dbt-selection-empty`, `dbt-relation-name-refused`,
    `dbt-name-collision`;
  - publish and refresh: `dbt-relation-missing`, `dbt-upstream-run-failed`,
    `dbt-refresh-source-unknown`, `dbt-unknown-key`.

## The importer

**Input:** `semantic_manifest.json`, MetricFlow's own contract, written by dbt-core 1.12 and
dbt-oss 2.x alike. It also reads `manifest.json` for docs and tests, and `catalog.json` for column
types. It does not read `osi_document.json` for dbt projects: dbt's converter currently loses
filtered and offset-derived metrics there.

**Architecture: one shared mapping, several front ends.**

- front ends parse a semantic document into a neutral in-memory model (sources, fields, keys,
  relationships, metrics with their filters, offsets and windows);
- one mapping from that model to Malloy text;
- one coverage report, listing every metric as exact, approximated or skipped, with the reason;
- pluggable reconciliation backends, which check the generated model's numbers against the source
  system's own engine.

The dbt `semantic_manifest.json` front end ships first, because it is lossless.

**Mapping:**

| dbt                           | Malloy                                                    |
| ----------------------------- | --------------------------------------------------------- |
| entities                      | `primary_key` + `join_one` / `join_many`                  |
| dimensions                    | dimensions                                                |
| simple metrics, with filter   | measures with a `{ where: }` filter                       |
| ratio and derived metrics     | measure expressions                                       |
| offset and cumulative metrics | views or window calculations                              |
| saved queries                 | views                                                     |
| conversion metrics            | a generated view, or flagged as unsupported with a reason |

**Output:** `bindings/` (one relation per selected dbt node, with a logical `#(dbt) unique_id`) and
`generated/` (the semantics), committed, which the developer's own files import and extend (S2),
plus the coverage report (`dbt-metric-approximated`, `dbt-metric-skipped`).
**Acceptance:** P1's reconciliation harness, run on the generated model.

### How the 23 jaffle-shop metrics map (to verify in P1)

| Metric type                                                    | Count                  | Malloy shape                                                                                    | Risk                                                                       |
| -------------------------------------------------------------- | ---------------------- | ----------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| simple (incl. filtered: `new_customer_orders`, `large_orders`) | 17                     | a measure; filtered ones are `measure { where: … }`                                             | the name collides with its column ("Cannot redefine"): rename or alias     |
| ratio (`food_revenue_pct`, …)                                  | 2                      | a measure expression                                                                            | numerator and denominator filters must be applied separately               |
| derived (`revenue_growth_mom`, …)                              | 3 (one with an offset) | a measure expression; the offset one is a view with `calculate: lag()`, or a self-join by month | the offset one is not a measure, so it can't be reused freely; document it |
| cumulative (`cumulative_revenue`)                              | 1                      | a view with a running sum over month                                                            | bound the time range: dbt's time spine runs years past the data            |
| saved queries                                                  | 3                      | views                                                                                           | —                                                                          |

### Apache Ossie (formerly OSI) import and export (P2b)

It follows P2, behind the same mapping.

- **Import:** an Ossie front end over `osi_document.json`, reaching sources beyond dbt.
  - **Both versions:** Ossie 0.1.1, which dbt 1.12 and Snowflake emit today, and 0.3, through a thin
    adapter each into the neutral model. 0.3 support is pinned to a dev snapshot and labelled
    experimental until the spec stabilises.
  - **Report what Ossie can't carry:** cumulative windows, offset-derived metrics, and filters left
    in MetricFlow names.
  - **Not for dbt projects** while dbt's converter loses those semantics; for dbt,
    `semantic_manifest.json` stays the input.
- **Export:** Malloy → Ossie. It is lossy by design, since Ossie has no nesting, pipelines, givens or
  authorize: Ossie is the interchange floor, and the coverage report says what was dropped.
- **When to revisit:** once Ossie 0.3 stabilises, carries derived and cumulative metrics, and dbt
  emits it, Ossie could become the default dbt input.

## The Snowflake semantic-view track

A second track on the same core. Semantic views are database objects, not a pipeline, so none of
the dbt developer loop applies (no project in the package, no run hooks, no eject). What is shared is
the neutral model, the mapping, the coverage report and the reconciliation harness.

- **A semantic-view import is the Ossie front end plus a small Snowflake profile,** not a third front
  end. Snowflake's Ossie export (Preview) speaks 0.1.1, and it drops as-of and range joins and
  omits data types. The profile fills those from `DESCRIBE SEMANTIC VIEW` / `GET_DDL`, and flags
  anything it can't.
- **No direct converter** between MetricFlow and semantic views exists to build on.

| Step | What                                                                                                                          | Value                                                                                                                                                                                                                                                             |
| ---- | ----------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| SV1  | **Import** a semantic view → Malloy, through the Ossie front end plus the Snowflake profile                                   | An existing Snowflake semantic model gains nesting, governance, MCP and materialization                                                                                                                                                                           |
| SV2  | **A reconciliation backend** that checks every imported metric × dimension against Snowflake's own `SEMANTIC_VIEW(...)` query | Golden numbers from Snowflake's own engine, proving the mapping before export ships                                                                                                                                                                               |
| SV3  | **Export** Malloy → `CREATE OR ALTER SEMANTIC VIEW`                                                                           | Author, govern and test the model in Malloy, and distribute it to the tools that read semantic views. **Malloy's access controls (givens, `#(authorize)`, `#(access_filter)`) do not travel**; the export docs say so and recommend Snowflake row access policies |
| —    | **Query through a semantic view** from Malloy                                                                                 | **Not viable:** Snowflake forbids joins and window calls in the `FROM` of a semantic-view query, so Malloy's SQL would be rejected. A docs recipe at most                                                                                                         |

**Example:** Snowflake's TPC-H sample semantic view, imported and reconciled number by number. It
needs a Snowflake account, unlike the zero-credential dbt example, so its CI is an opt-in job.

**Sequencing:** SV1 with P2b, since it shares the Ossie front end; SV2 alongside it; SV3 after.

## Phases

Phases with dependencies only. The critical path is **P0 → P1 → P2 → P5**. P3 runs alongside P2;
P4 follows. The Snowflake track follows P2b.

**P0, decisions and housekeeping.** No code.

1. The shared check result format and the new public names: settled (above). What remains is
   moving the format into a shared `docs/check-format.md` that `publisher --check` and the CLI both
   cite.
2. Publish the CLI as `@malloy-publisher/cli`, binary `malloy-pub`, released in lockstep with the
   server by the release workflow.
3. Ask dbt Labs to merge jaffle-shop's license; until then, clone at setup.
4. Find out why the earlier jaffle-shop example PR (#1042) was closed before reusing its mapping
   table and field pitfalls.

**P1, the example by hand.** No new Publisher code.

- `setup.sh`, plus a hand-written Malloy package over the jaffle marts covering all 23 metrics. Each
  metric is a measure, or a view where it must be: offset, cumulative and median metrics can't be
  measures.
- The reconciliation harness runs each metric through `mf query` and through Publisher's REST, and
  diffs them, for configuration A and configuration B.
- Configuration B: the `customers` mart and the dashboard grains as `#(materialize)` / `#(rollup)`,
  a dbt selector that excludes the moved models, and the governance layer.
- `showcase.malloy`.
- **Exit criteria:** a clean clone, `setup.sh` and `npm start` give a served package, with zero
  reconciliation diffs, or each one explained.

**P2, the importer** (`malloy-pub dbt import`, and as an MCP and skill path), with S1–S3.
**Acceptance:** the P1 harness on the generated model.

**P2b, Ossie import and export,** behind the same mapping. **SV1 and SV2** join here.

**P3, the developer-loop plumbing, plus the sync primitives:**

- `malloy-pub materialize --source` in the CLI;
- `table-changes` and `malloy-pub dbt refresh` (S6), and scoped refetch (S11);
- land `publisher --check` (#1934) with the shared format (`--check-format`, the JSON document);
- the scaffolder's `--dbt` flag;
- manifest-at-publish validation (S4), lineage mapping (S5) and run consistency (S7);
- `malloy-pub dbt exposures` (S8);
- `#(doc)` from the manifest.
- **Follow-up: MotherDuck read scaling.** Accept a MotherDuck read-scaling token on the `motherduck`
  connection, which routes queries to a pool of read-only replicas, with a per-user `session_name`
  (from the caller's identity) so each user stays on one replica for cache affinity. The replicas
  are eventually consistent and can trail the writer by minutes, so a replica read can see a table
  older than the one a build or `dbt refresh` just wrote. Reads that must see a just-finished build
  (the post-build contract check, `table-changes`, served-equals-live checks) keep the read-write
  token, and a freshness verdict never treats a replica read as proof that a table is current; the
  docs state the lag.

**P4, drift with column pins.** `malloy-pub dbt import` already writes `bindings/`, one binding
source per selected model; P4 adds column pins to them, and `malloy-pub dbt check` becomes a full
regenerate-and-diff drift gate, so schema drift stays a reviewable file diff, reported with the
`dbt-*` drift codes. dbt tests as signals (S9) land with it. Production teams need it; the example
does not.

**P5, the launch.** Drafted after P1, published once P2 ships, so "one command" is true.

**Later:** S10 (its own design item, and the precondition for per-version isolation), then S12
(eject to dbt), and SV3.

## Beyond P5: moving the seam

The decomposition guide is a snapshot. Each closed gap in the scoring table moves a row of it from
"stay in dbt" towards "can move", one mart at a time, with dbt running below throughout.

### Governance: tests, contracts, seeds

The design requirements come from teams whose test suites made the layer work: _severity is a
routing decision_ (fail where the model owner owns the fix, such as grain violations and model
drift; warn where the data owner does, such as source gaps; a permanently red gate stops being
read); _scope counts to the actionable subset_, so a number means "entities affected now"; and _the
warn stream is a product surface, not logs_, landing where an analyst agent retrieves. Contracts
become typed, named checks at both edges of the model: upstream, binding sources validated against
the warehouse schema; downstream, view output spaces held stable for the dashboards and agents
consuming them. Both start in `malloy-pub lint` and graduate to publish-time enforcement. Seeds need
one mechanic: pushing a package CSV into the warehouse connection as a managed, content-addressed
table, so it can join the marts it enriches.

Part of this has already shipped, and data tests should land in the same shapes:
[`#(carries)`](classification.md#carries-the-classes-an-entry-point-may-expose) as a downstream
contract in policy terms; the [compliance review](compliance-review.md) as a policy check in CI;
package warnings with a stable code, severity and subject, plus the serve verdict; and
`compile_model` at `package` scope as the pre-merge gate.

Unit tests are separate and cheaper: a fixture CSV per source plus an expected result, run on the
per-package duckdb sandbox. They reuse the same source-rebinding mechanism as package variants. Two
limits: dialect-specific expressions and `connection.sql()` sources test on duckdb, not the
warehouse; and a test cannot cross the sandbox/warehouse boundary until rebinding exists.

### State: SCD2 as a realization mode

The incremental machinery already provides most of the write path. Two things are new. **A history
write path:** close-out plus insert with validity columns and a current-row marker, supporting a
timestamp strategy and a check strategy, plus hard-delete close-out. **Lifecycle exemptions:** a
history table is primary data, not a cache, so a definition change carries history forward or
refuses rather than rebuilding from current state, collection never reclaims the only copy, and
retention is a stated posture. Both land in the open-source Publisher, which already owns the
incremental ledger. The footing exists: a reclaim never drops a table at an author's `name=`
(outside `scope: "version"`) and lists each table it keeps, every database table a build writes
carries its package's ownership mark, and stored tables are keyed by content address
([whose table it is](materialization.md#whose-table-it-is)).

### Staging: SQL sources now, language later

- **Lane A, platform:** `connection.sql()` sources as the staging idiom. They are already
  persistable, so lifted staging SQL gains content addressing, scoped reruns and rollback. On
  Postgres, BigQuery or Snowflake it can also build with `storage=` into a DuckLake destination. One
  spike first: confirming that build-plan edges arise between SQL sources, so a pure-SQL staging DAG
  builds in order. Nothing on main shows such an edge yet.
- **Lane B, language:** in measured-usage order, **set operations** first (multi-source entities,
  taxonomy unpivots and past/future splits are all `UNION ALL`), then **correlated table functions**
  (per-row date explosion), then **non-linear pipelines**, then **expression-level macros**, then
  quality-of-life (a `QUALIFY` equivalent, richer ordering, function breadth).

### Variance: per-tenant compilation

- **Package variants:** one authored package, many compiled subsets. Capability flags are declared
  per environment or tenant, and a variant contains every model whose import closure the enabled
  bindings satisfy, so the model set is derived, not configured. It fails closed, and
  environment-resolved bindings make the dev→prod dataset swap configuration (shared with S10).
- **Package dependencies and override sources:** a package declares a versioned dependency on
  another, resolved through a lockfile, so a conformed model is published once and consumed by many
  tenant packages; a shared package declares a typed default source and the consumer supplies its
  own. This is dbt's macro-override pattern at the source level.

A thin per-tenant compile step likely survives every version of this plan; the goal is that
Publisher does it from declarations.

## Non-goals

- **Total conversion.** dbt keeps the work it is better at: EL, Python and ML transforms, its
  package ecosystem, the heaviest staging. The boundary is a per-layer choice, and it never reaches
  zero.
- **Running dbt.** Publisher reads dbt's artifacts and accepts its signals; it never invokes dbt or
  manages dbt's credentials.
- **Absorbing your control plane.** Tenant registries, infrastructure-as-code and promotion policy
  stay yours. Publisher consumes their outputs and provides the hooks.

## Risks

- **The jaffle-shop license.** Mitigated by cloning at setup, with our own models as the fallback.
- **Spec and dbt 2.x churn.** Pin dbt and MetricFlow versions in the example, and re-verify each
  release.
- **Interchange fidelity.** Ossie 0.3 is still breaking, and dbt's Ossie converter loses semantics.
  Read `semantic_manifest.json` for dbt, and treat Ossie as P2b.

## Open questions

Input welcome, especially from teams running dbt below Malloy in production.

- **Binding shape.** Should generated bindings be bare `table()` sources, or carry more (column
  pins, an access-modifier default) so hand-authored tiers can assume less? The current lean is
  column pins, falling back to bare sources.
- **Selector surface.** Which subset of dbt's node-selection syntax matters in practice for choosing
  marts: tags, paths, groups?
- **Knowledge injection.** Does per-tenant documentation and synonym injection deserve a first-class
  package mechanism, or is splicing above matching source declarations the right level?
- **Manifest gates.** Feature-flag derivation needs a manifest with every input enabled; which
  build's manifest should it read?
- **Importer coverage.** Where is the line for conversion metrics and other constructs beyond which
  the answer is "model it natively"?
- **SQL-source dependency edges.** Do build-plan edges arise when one SQL source consumes another
  persisted source? This decides how much of `ref()` Lane A replaces.
- **Where tests are declared:** annotations in the model, or a separate spec file.
- **Freshness objectives on one node.** Whether scheduling refresh against freshness objectives,
  rather than a cron, gets a single-node analog.
- **Ossie as the default dbt input,** once 0.3 stabilises and dbt emits it.
