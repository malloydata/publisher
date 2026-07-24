# Hammer — a scenario harness for the Publisher

Hammer runs **markdown scenarios** against a **real** Publisher server. Each
scenario is a folder with a `scenario.md` that reads like a story — seed some
data, write a model, publish, query, assert on the rows — and one command
executes it end to end: it stands up real infrastructure (a throwaway Postgres,
a local DuckLake), builds and boots the actual server, runs the steps, prints a
check report, and tears everything down. No cloud creds; nothing to hand-wire.

It's deliberately **separate** from the `bun test` unit/integration suites — a
hammer you run by hand for one-off, production-shaped confidence, not a CI gate.

Scenarios live in **suites** under `scenarios/<suite>/`. The harness itself — the
markdown interpreter, the publisher-process/cluster management, the assertion
recorder — is suite-agnostic. Today the one suite, `mz-storage`, exercises the
`storage=` materialization tier (a Postgres source warehouse + a DuckLake
destination), and the run fixtures in `run.ts` are wired for it; a new suite that
needs the same fixtures is just a new folder, one needing different infra extends
`run.ts`.

## Prerequisites

- **docker** (a throwaway `postgres:16` container is auto-spawned)
- **bun**
- A server build. The harness builds it once (`build:server-only`, which bakes
  the DuckDB ducklake/postgres extensions) if `packages/server/dist/server.mjs`
  is absent. Pass `--rebuild` to force a fresh build after code changes.

## Run

```bash
bun hammer/run.ts                          # all scenarios
bun hammer/run.ts --scenarios flat-source  # one (by id substring; comma-separated for several)
bun hammer/run.ts --tags security          # by tag (match-any; comma-separated for several)
bun hammer/run.ts --attention-older-than 30 # show only ⚠ callouts raised ≥30 days ago
bun hammer/run.ts --rebuild                # force a fresh server build first
bun hammer/run.ts --keep                   # leave the pg container + workdir up to inspect
bun hammer/run.ts --reuse-pg               # reuse a running hammer pg container (fast iteration)
```

`--scenarios` and `--tags` narrow together (a scenario must match both).

Flags: `--pg-port` (default 55432), `--port` (14000), `--mcp-port` (14040),
`--quiet`. Exit code is nonzero if any scenario fails.

The harness picks non-default ports so it won't collide with your own stacks.
If you used `--keep`, tear the container down with:

```bash
docker rm -f publisher-hammer-pg
```

## What it does, each run

1. Build the server if needed.
2. Spawn Postgres; create the source db (`hammer_src`) and the DuckLake catalog
   db (`ducklake_catalog`).
3. Seed every selected scenario's source tables; write their packages; generate
   a `publisher.config.json` with a Postgres source connection (`orders_pg`) and
   a DuckLake destination (`lake`, storage = a local dir), plus any connections a
   scenario declared with `## Connection` (each `ducklake` gets its own catalog +
   storage).
4. Run each scenario. Its `## Publisher` sections start real server processes on
   demand (a named publisher per cluster worker, each on its own ports, sharing
   the one config); because `PERSIST_STORAGE_MODE` is read at startup, changing a
   publisher's mode restarts it. Publishers are reused across scenarios.
5. Print a per-scenario check report; tear everything down (unless `--keep`).

## Layout

```
hammer/
  run.ts               # orchestrator / entrypoint
  lib/
    util.ts            # process spawn, polling, logging
    postgres.ts        # throwaway docker Postgres (DuckLake catalog + source warehouse)
    server.ts          # build + spawn a real Publisher server process
    config.ts          # generate publisher.config.json (connections + packages)
    packages.ts        # write generated Malloy packages to disk
    rest.ts            # REST client: build, poll, query (SQL + row values), bind, republish, reclaim
    scenario_md.ts     # the markdown scenario interpreter (parse -> Scenario)
  scenarios/
    framework.ts       # Scenario contract + assertion recorder + ServerControl
    index.ts           # recursively discovers */scenario.md
    mz-storage/        # the current suite (the storage= materialization tier)
      01-flat-source/scenario.md
      02-refinements/scenario.md
      ...                              # (a folder per scenario)
      06-operator-generational/{scenario.md, hooks.ts}   # + hooks.ts for exotic steps
```

## Authoring a scenario (markdown)

Scenarios live under a **suite** folder, `scenarios/<suite>/<NN-name>/`, each with a
`scenario.md` that reads like a story. A YAML front-matter block sets the metadata,
then an H1 title, then ordered sections:

```
---
id: flat-source                # report id + selection key (defaults to folder name)
package: d0                    # default package name for Model/Publish/Query
tags: serve-correctness        # optional labels: --tags filter + report grouping
requires: dialect:snowflake    # optional: capability tokens beyond the defaults;
                               #   unmet -> the scenario is SKIPPED, not failed
---
# Flat persist source: Postgres → DuckLake
```

**Tags** are free-form. One carries meaning to the runner: `needs-attention` marks
a scenario as carrying an open question — it's listed in the report's **⚠ needs
attention** block, along with any `## Note` callout, so follow-ups stay visible
instead of getting lost in a sea of green. Date the callout with
`## Note (since=YYYY-MM-DD)`; `--attention-older-than DAYS` then shows only the
callouts that have gone unaddressed for at least that long (undated ones always
show).

**Requires** lists capability tokens a scenario needs *beyond* the always-present
defaults (`connection:orders_pg`/`dialect:postgres`, `connection:lake`/
`dialect:duckdb`). The run advertises what it provides; a scenario that needs
something else — e.g. a `dialect:snowflake` scenario with no Snowflake connection
wired — is reported **SKIPPED** rather than failed. A `## Connection <name>
(type=…)` declaration adds its own `connection:<name>` token automatically, so a
scenario that declares what it needs won't skip; wiring a brand-new *warehouse*
type (BigQuery/Snowflake, which need creds) still means extending `run.ts` and the
`available` set.

The section vocabulary is small and fixed:

| section | body | effect |
|---|---|---|
| `## Publisher [<name>]` | `- PERSIST_STORAGE_MODE: on` (+ any `- SOME_ENV: value` bullets) | (re)start a publisher at that mode and make it the active target. A `<name>` identifies a distinct, concurrent publisher process (a cluster worker); reusing a name restarts THAT one, new names start more. Nameless = the single `default` publisher; switching its mode = another `## Publisher` (the mode is fixed at process start). Extra `- KEY: value` bullets are passed as environment to the spawned server — for a deployment flag also fixed at startup, e.g. `- PERSIST_COLLISION_ENFORCE: true` (see `collision-enforce-refuses-publish`) |
| `## Data <conn>.<table>` | GFM table, headers `name:type` | seed/replace source rows |
| `## Mutate <conn>.<table>` | GFM table (append) **or** ` ```sql ` block | change source rows mid-run |
| `## SQL <label>` | ` ```sql ` block + `Expect:` table | run raw SQL on the source warehouse, compare rows (e.g. *prove* the source really changed) |
| `## Operator <conn>` | ` ```sql ` block | orchestrator DDL on a destination via the operator's OWN read-write DuckLake client, **external to the publisher** (e.g. `CREATE SCHEMA` on `lake`) |
| `## Connection <name> (type=postgres\|ducklake\|duckdb)` | — | **DECLARE** a connection wired into the config beyond the always-present `orders_pg` + `lake` (a pre-pass artifact, like a package — not a runtime step). `postgres` reuses the source warehouse; `ducklake` gets its OWN catalog + storage (a genuinely separate destination — see `cross-connection-destinations`); `duckdb` is a local-dialect source (see `duckdb-source-not-materializable`). The name becomes a `connection:<name>` capability token |
| `## Connection <conn> (refused)` | ` ```sql ` block | run SQL THROUGH the publisher's `sqlQuery` endpoint; `refused` asserts it's rejected (storage attach is read-only). (Same header as the declaration above — the `type=` attribute picks the declaration form; a ` ```sql ` block picks this one) |
| `## Model [<pkg>/]<path>` | ` ```malloy ` block | write a package model file (re-declaring it mid-run = an edit) |
| `## Publish [<pkg>] (forceRefresh, sources=a[+b], async, label=X)` | optional `expect binding: src -> conn` | load + build the package; `forceRefresh` rebuilds even if unchanged; `sources=` builds only the named persist source(s) (the `sourceNames` filter; `+`-separated), leaving the rest live; `async` fires the build WITHOUT awaiting (see `## Await`) so a following step can observe it in flight |
| `## Await [<label>]` | — | drain an async publish (by `label`, else the oldest pending) and assert it completed |
| `## Build [refused] (orchestrated, pkg=P, pub=, env=, strict)` | `- <src> -> <physicalName> @ <dest>` lines + `reference: <src> [(from=<pub>)]` lines; optional `cites:` | orchestrated (caller-instructed) build: each source builds into the caller-assigned physical name (verified) at the destination; `reference:` reuses an already-built upstream, resolved BY NAME from the latest manifest (the target's, or `from=<pub>`); `refused` asserts it fails |
| `## Delete [<pkg>]` | — | unload + `DELETE` the package from the serving set (asserts it no longer resolves) |
| `## Reclaim [<pkg>]` | — | `DELETE` the latest successful materialization with `?dropTables=true` — the destination-aware physical-table drop (GC). Pair with `## Restart` to prove serving reverts to live once the table is reclaimed (see `reclaim-drop-tables`) |
| `## Republish [refused] [<pkg>]` | `cites: <substring>` | (re)publish through the author-in-the-loop `POST /packages` gate (distinct from `## Publish`, which is a build). Unlike startup/reload (fail-safe, warn-only) this path is strict, so `refused` asserts a 4xx — e.g. a collision under `PERSIST_COLLISION_ENFORCE` (see `collision-enforce-refuses-publish`) |
| `## Build refused [<pkg>]` | `cites: <substring>` | build must be refused — a build that reaches FAILED **or** a package that won't load so the build can't start — citing … |
| `## Rejected <pkg>` | optional `cites: <substring>` | the package's model is invalid: assert it is NOT served (durable `getPackage` probe) and — if `cites:` — confirm the diagnostic via `/compile`. (Uses `getPackage`, not `/status` loadErrors, which is pruned after the first call.) |
| `## Warns [<pkg>]` | `cites: <substring>` | package must surface an operator warning citing … (`/status` warnings) |
| `## Compile <label> (pkg=P[, refused])` | optional ` ```malloy ` + `cites:` | compile-check a model via `/compile` (deterministic). When `refused`, the framework ALSO asserts the package is NOT served — the divergence backstop (a non-compiling model must not be reported as serving) — so a scenario just declares a model invalid and the framework proves both |
| `## Bind [<pkg>] (empty\|clear\|bad\|from=<publisher>\|asof=,fresh=,fallback=)` | — | play the orchestrator: PATCH the ACTIVE publisher's `manifestLocation` — full (re-serve last build), `empty` (drop → live), `clear` (null → live), `bad` (unreachable URI → fetch-fail → live), `from=<publisher>` (bind another publisher's build manifest — the cluster distribute pattern). `asof=<iso>`/`fresh=<seconds>`/`fallback=<live\|stale_ok\|fail>` stamp freshness fields on each bound entry to drive the age-vs-window gate |
| `## Query <label> (again, refused)` | ` ```malloy ` (or reuse by label) + `Expect:` table | run + compare rows; `refused` = the query MUST fail (optional `cites:`) |
| `## Restart [(init)]` | — | reboot the active publisher. Bare: **preserve** the materialization store (no `--init`), so serving re-establishes from the persisted store on load. `(init)` re-copies packages (picks up a mid-run `## Model` edit) and resets the store |
| `## Note [(since=YYYY-MM-DD)]` (or `## Attention`) | `> …` blockquote | a prose callout surfaced in the report's **⚠ needs attention** block — the open question or thing to review (pairs with the `needs-attention` tag); `since` dates it for `--attention-older-than` |
| `## Hook <exportName>` | — | call a `hooks.ts` export, in document order (interleave with markdown steps). Receives a shared `state` object, so a tiny hook can stash a value another hook reads. Reserved for what markdown genuinely can't express (e.g. capturing + comparing an internal `sourceEntityId`) — most flows are pure markdown |

Column types (`order_date:date`, `amount:num`, …) drive both the `CREATE TABLE`
and value coercion. **`PERSIST_STORAGE_MODE` is a server-level setting fixed at
process start** — it is NOT a per-request parameter. A `## Publisher` section
(re)starts the publisher at a mode (defaulting to `on`), and every following step
runs against that publisher; to change the mode you write another `## Publisher`
(a fresh process), which is what `mode-matrix` and `migrate-persist-to-storage` do. There
is no per-step `(mode=…)`. Naming a publisher (`## Publisher p1`) starts a
distinct, concurrent process — a cluster worker — so a scenario can build on one
and `## Bind … (from=p1)` the manifest to another; they share one config, hence
the same warehouse and DuckLake tier (see `multi-publisher-shared-table`).
`(again)` re-runs the most recent query of the same label — the basis of the
routing proof (mutate the source, run the query again, expect the value
**unchanged**). Server-facing steps take `(pub=<name>)` to run against a
specific started publisher instead of the active one — so you can query `p1` and
`p2` side by side without switching active.

Steps also take `(env=<name>)` to run against a specific **environment**, and
`## Model <pkg>/<path> (env=<name>)` registers a package under that environment. A
package name may recur across environments with a different model. Every step
defaults to the primary environment (`default`); naming any other environment adds
it to the generated config. One server process serves all environments (env is
orthogonal to `pub`), and they share the same connections — including the same
DuckLake destination, so a cross-environment collision is observable (see
`cross-environment-same-name`).

Orchestrated (caller-instructed) builds — generational names, cross-run/cross-worker
reference reuse — are first-class markdown now (`## Build (orchestrated, …)`). A
`hooks.ts` is the escape hatch for the rare thing markdown genuinely can't express
(capturing and comparing an internal identity like a `sourceEntityId`); such hooks
should be tiny, run in document order, and share `state` with each other — the flow
stays in the `.md`. Of the current suite only one scenario keeps a hook.
