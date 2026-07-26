# Hammer scenario grammar

The step vocabulary. For what a scenario is *for* — rule vs. observation, when to
write one at all — see [README.md](README.md).

## The format

A scenario is **one markdown file**, `scenarios/<suite>/<NN-name>/scenario.md`,
with three parts:

1. **YAML front matter** — id, default package, tags.
2. **An H1 title** — the rule this scenario states, followed by prose explaining
   it. Prose is for the reader; the harness ignores it.
3. **`##` sections, executed top to bottom.** Each one is a step.

A section header reads as a verb and its argument, with options in parentheses:

```
## Query daily rollup (again)
   ^^^^^ ^^^^^^^^^^^^ ^^^^^^^
   step  argument     attributes
```

What a section *does* with its body depends on the body's **shape**:

| body shape | means |
|---|---|
| GFM table | data — rows to seed, or (under `Expect:`) rows to assert |
| fenced ` ```malloy ` / ` ```sql ` block | the model or query to run |
| `key: value` line | a modifier or assertion — `cites:`, `givens:`, `expect binding:` |
| anything else | prose, ignored |

So a scenario reads as a story and executes as a test, with no separate wiring.

## A whole scenario

This is the baseline case, trimmed. Seed a table, declare a model that persists an
aggregate, build it, query it — then mutate the source and query *again*, where an
unchanged answer proves the query was served from the materialized snapshot rather
than recomputed live.

````markdown
---
id: flat-source
tags: serve-correctness
package: d0
---

# Flat persist source: Postgres → DuckLake

A single aggregate is persisted into DuckLake and served routed at `mode=on`.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |

## Model d0.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.orders')

#@ persist name="d0_daily" storage=lake
source: daily_orders is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Publish

expect binding: daily_orders -> lake

## Query daily rollup

```malloy
run: daily_orders -> { select: order_date, total_amount }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |

## Mutate orders_pg.orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-01      | 1000       |

## Query daily rollup (again)

A **stale** 150 proves it was served from the snapshot, not recomputed.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
````

`scenarios/mz-storage/01-flat-source/` is this scenario in full, including the
`## SQL` step that proves the mutation really landed in the warehouse first.

## Front matter

```
---
id: flat-source                # report id + selection key (defaults to folder name)
package: d0                    # default package name for Model/Publish/Query
tags: serve-correctness        # topic + status labels (see README)
requires: dialect:snowflake    # capability tokens beyond the defaults;
                               #   unmet -> the scenario is SKIPPED, not failed
---
```

**Requires** lists capability tokens a scenario needs *beyond* the always-present
defaults (`connection:orders_pg`/`dialect:postgres`, `connection:lake`/
`dialect:duckdb`). A scenario that needs something else — a `dialect:snowflake`
scenario with no Snowflake connection wired — is reported **SKIPPED** rather than
failed. A `## Connection <name> (type=…)` declaration adds its own
`connection:<name>` token automatically, so a scenario that declares what it needs
won't skip. Wiring a brand-new *warehouse* type (BigQuery/Snowflake, which need
creds) still means extending `run.ts` and the `available` set.

## Sections

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
| `## Manifest [<pkg>]` | `- <source> -> <table> @ <destination>` lines | author the manifest **a host would send**, and bind it — distinct from `## Bind`, which replays one the publisher produced. The interesting entries are the ones the publisher would never generate: a source it refused to build, a stale generation, a table it does not own. Each source is resolved to its `sourceEntityId` through the package build plan, and the captured schema is copied from whichever entry already describes that physical table, so a real build must have produced it first (see `host-binding-honors-row-level-access`) |
| `## Query <label> (again, refused)` | ` ```malloy ` (or reuse by label) + `Expect:` table | run + compare rows; `refused` = the query MUST fail (optional `cites:`). Body keys: `givens: NAME=v; OTHER=v` supplies runtime given values; `columns: exact` asserts the column set |
| `## Restart [(init)]` | — | reboot the active publisher. Bare: **preserve** the materialization store (no `--init`), so serving re-establishes from the persisted store on load. `(init)` re-copies packages (picks up a mid-run `## Model` edit) and resets the store |
| `## Note [(since=YYYY-MM-DD)]` (or `## Attention`) | `> …` blockquote | a prose callout surfaced in the report's **⚠ needs attention** block — the open question or thing to review (pairs with the `needs-attention` tag). `since` dates it for `--attention-older-than`, which filters the **known-red** block by the same date, so a `known-red` scenario wants a dated Note or it can never be aged out |
| `## Hook <exportName>` | — | call a `hooks.ts` export, in document order (interleave with markdown steps). Receives a shared `state` object, so a tiny hook can stash a value another hook reads. **Reserved for what markdown genuinely can't express** — and reaching for one usually means a step is missing (see README, *When not to write a scenario*) |

## Semantics worth knowing

**Column types** (`order_date:date`, `amount:num`, …) drive both the
`CREATE TABLE` and value coercion.

**`PERSIST_STORAGE_MODE` is a server-level setting fixed at process start** — not
a per-request parameter. A `## Publisher` section (re)starts the publisher at a
mode (defaulting to `on`), and every following step runs against that publisher;
to change the mode you write another `## Publisher` (a fresh process), which is
what `mode-matrix` and `migrate-persist-to-storage` do. There is no per-step
`(mode=…)`.

**Named publishers.** `## Publisher p1` starts a distinct, concurrent process — a
cluster worker — so a scenario can build on one and `## Bind … (from=p1)` the
manifest to another. Publishers within a scenario share its config, hence the same
warehouse and DuckLake tier (see `multi-publisher-shared-table`). Server-facing
steps take `(pub=<name>)` to run against a specific started publisher instead of
the active one, so you can query `p1` and `p2` side by side.

**`(again)`** re-runs the most recent query of the same label — the basis of the
routing proof: mutate the source, run the query again, expect the value
**unchanged**.

**Environments.** Steps take `(env=<name>)` to run against a specific environment,
and `## Model <pkg>/<path> (env=<name>)` registers a package under that
environment. A package name may recur across environments with a different model.
Every step defaults to the primary environment (`default`); naming another adds it
to that scenario's config. One server process serves all of a scenario's
environments (env is orthogonal to `pub`), and they share the same connections —
including the same DuckLake destination, so a cross-environment collision is
observable (see `cross-environment-same-name`).

Orchestrated (caller-instructed) builds — generational names, cross-run and
cross-worker reference reuse — are first-class markdown (`## Build (orchestrated,
…)`), not hook territory.

## The parser is strict on purpose

The vocabulary is small and fixed by design. It is not trying to be expressive —
it is trying to make it **hard to write a scenario that reaches below the API**.
`scenario_md.spec.ts` pins this. A malformed scenario **fails itself loudly**
rather than passing quietly or taking down the run:

- an unknown `## Section`, or an unknown attribute on a known one, is an error
  listing what is valid;
- a step that can assert but doesn't (a `## Query` with no `Expect:`, no
  `cites:`, no `(rows=N)`) is an error — a step that verifies nothing looks like
  coverage in the report and isn't;
- only a table labelled `Expect:` is compared, so prose tables in the narrative
  are never mistaken for assertions;
- `${...}` substitutions must resolve to something the harness provides.

A scenario that fails to parse fails only itself; the rest of the run continues.
