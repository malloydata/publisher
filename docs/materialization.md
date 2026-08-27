<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Materialization (Malloy Persistence)

Materialization pre-builds a Malloy source into a physical table so queries read the table instead of recomputing the source. In Publisher it is driven by **Malloy Persistence**: you annotate a source `#@ persist`, and Publisher builds it, records a manifest, and serves queries from the built table.

This doc covers the open-source Publisher's materialization surface — how to declare it, the rules Publisher enforces, how to build on demand or on a schedule, the `malloy-pub` CLI, and how a self-hosted (standalone) deployment differs from a control-plane-driven (hosted) one.

## Declare what to persist

Annotate a source with `#@ persist` (the persistence experiment must be enabled in the model):

```malloy
##! experimental.persistence

source: raw_orders is duckdb.table('data/orders.csv')

#@ persist name="order_summary"
source: order_summary is raw_orders -> {
  group_by: category
  aggregate:
    total_orders is count()
    total_revenue is amount.sum()
}
```

`name=` is the physical table Publisher writes. Persist the sources that are expensive to compute and reused by many queries; leave cheap or rarely-read sources unpersisted. The [`malloy-materialization-tuning`](../skills/malloy-materialization-tuning/SKILL.md) skill helps decide.

`name=` may also name the container the table goes in — `name="analytics.order_summary"` writes `order_summary` into the `analytics` schema/dataset rather than the connection's default one. The container must already exist; Publisher does not create it. On BigQuery a dataset is required, since a table cannot live outside one.

### Opting a reader out: `#@ -persist`

A source that `extend`s a persisted source reads the persisted table too — the extension adds computed fields on top of the stored rows rather than recomputing them. Annotate the extension with `#@ -persist` when you want it recomputed live instead:

```malloy
#@ persist name="order_summary"
source: order_summary is raw_orders -> { … }

// Reads the stored order_summary table, adds a field on top.
source: summary_with_margin is order_summary extend {
  dimension: margin is total_revenue / total_orders
}

// Recomputed from raw on every query — never reads the stored table.
#@ -persist
source: summary_fresh is order_summary extend {
  dimension: margin is total_revenue / total_orders
}
```

Reach for it when a reader must not see stale rows, and remember what it costs: the opted-out source recomputes its whole upstream on every query, so it forgoes exactly the work persistence was there to save. It also keeps the extension from being materialized itself, which matters today because a plain extension of a persisted source is currently treated as a second build target for the same table.

### `#(authorize)`-gated sources and materialization

A source protected by an `#(authorize)` gate — its own, or one carried from a joined or derived
source — is refused for `storage=` and for pre-aggregation, unconditionally. A **colocated**
`#@ persist` (no `storage=`) is different: it is admitted when the gate is *proven* to be the entry
point's own row filter, and refused otherwise.

- **`storage=`** refuses at build time, unconditionally, alongside an unbound parameter or a given
  reference (see [persist-storage-tutorial.md § Eligibility refusals](persist-storage-tutorial.md#eligibility-refusals-refused-at-build-time)):
  a materialized-once table is served frozen to every caller, and the served shape carries no gate
  to re-evaluate. This refusal is unaffected by anything below.
- **A colocated `#@ persist`** is not served frozen with respect to the gate at all: persistence
  changes only where the rows are read FROM, never whether the entry point's own `#(authorize)` is
  re-evaluated — the substitution swaps only the source's relation SQL, and the gate applies as the
  reading query's own `WHERE` on top of it, so filtered rows come back filtered. When the compiler can
  *prove* the gate is the entry point's own row-level filter and nothing else is reachable beneath it,
  the source is eligible and serves correctly filtered from the materialized table. It is still
  refused when that cannot be proven — a gate reachable only through a join (join-only gate
  attribution is not traced), an inherited gate the compiler cannot attribute cleanly, or a gate that
  does not classify as a row filter at all. Drop `#@ persist` from the source, or restructure it so the
  condition is the entry point's own proven row-level gate.
- **`#@ preaggregate`** refuses unconditionally, regardless of the gate's classification. A rollup
  synthesizes a colocated `#@ persist` over an import of the annotated base, and none of the
  pre-aggregation modules has any `#(authorize)` awareness of its own — so this refusal is the only
  thing standing between a gated source and the pre-aggregation tier. It also groups *across* the
  gated column, so the column is not even present in the rolled-up result to filter afterwards, even
  in principle. A refused rollup names `#@ preaggregate` and the gated source rather than the
  synthesized rollup's own name, which the author never wrote.

Every refusal names the source and the remedy; a package carrying one fails to build (or, for
`storage=`, fails that materialization run) rather than silently serving the gated source to
everyone.

### The freshness contract for a gated colocated persist source

Admitting a proven row-level gate applies unconditionally. The refusal it relaxes never fired at
*load*: it fires inside the build path (`deriveSelfInstructions` / `executeInstructedBuild`), so a
package with a colocated `#@ persist` on an `#(authorize)`-gated source already loads, appears in
`plan.sources`, and serves live — what 422'd was its *materialization run*, not the package.

**So such packages already exist.** On upgrade, a run that used to fail succeeds when the gate proves
row-level and attributed to the entry point, and the next auto-run or scheduled build materializes the
source and binds it for serving **with no author action** — a source that served live yesterday serves
from a possibly-stale artifact afterwards, subject to the staleness below.

What goes stale between rebuilds is the **row data**, not the gate. The gate expression and the
querying principal's attributes (givens, roles) are still evaluated live, on every query, against the
frozen table — only the column values the gate filters ON are frozen at build time. So a row whose
access decision changes (say, it changes owner) keeps being served under the OLD decision to the
principal who no longer should see it, until the next rebuild recomputes that column. This is a
narrower staleness than an ordinary persisted source's (which goes stale on every column), but for a
gated source it is a staleness that maps directly onto who can read what — treat it accordingly.

Because row-data-dependent revocation is only as fresh as the artifact, a gated colocated persist
source needs a declaration that says how long a stale access decision may be served. Two controls are
on offer, and only one of them **bounds** that.

**`materialization.freshness` — `{ "window": …, "fallback": "live" }` — is the bound.** The serve path
re-evaluates freshness per query, so once an artifact's data ages past the window it drops out of the
serving set and the query recomputes live, correctly filtered — whether or not any rebuild ever lands.
That is a ceiling no refresh cadence can offer: a build that fails, or a scheduler that is off, leaves
a schedule-only source serving its old decisions indefinitely. The cost is that `freshness` is
[mutually exclusive with `schedule`](#the-persistence-policy-the-publish-gate), which is why advice
framed around a cron steers away from it — for a gated source, take the ceiling. (The window is
enforced from the freshness fields a control plane stamps on the manifest it distributes; a standalone
Publisher's own post-build load binds its entries un-gated.)

**A full rebuild is the refresh that actually re-reads the gate column.** A source with no incremental
declaration rebuilds its whole table on every run, so every run recomputes the values the gate filters
on. An incremental source needs `reseed` to do the same.

**`refresh="incremental"` does not bound revocation.** The [delta](#incremental-refresh) wraps the
seed's own SQL in a predicate over `[covered_through, frontier)`, so a row whose access decision
changes *without its watermark advancing* falls outside every future delta and is never re-read.
Take `orders`, gated with `#(authorize) org_id = $ORG` and declared
`refresh="incremental" watermark="order_date"`: order 7 (`order_date` 2026-01-02) moves from org 1 to
org 2, every later run advances past that date, and principal `ORG: 1` keeps reading it
indefinitely — while the entry
reports `refresh: delta` and an advancing `coveredThrough`, so the cadence reads as healthy.
`merge_key=` does not close it: it changes how a delta is applied, not which rows the delta reads.

A gated source with neither a freshness window nor a full-rebuild cadence is a source whose
revocations have no bound at all.

## The persistence policy (the publish gate)

Package-level persistence policy lives at the root of `publisher.json`:

```json
{
  "name": "orders",
  "materialization": {
    "scope": "version",
    "schedule": "0 6 * * *"
  }
}
```

`scope` at the manifest root is the original home and still works, with a deprecation warning on the package. Declare it inside `materialization` alongside the other build knobs.

Whenever the server rewrites a manifest — any package PATCH, including a description-only one — it writes **both** homes with the same value, so a package edited through the API keeps loading on an older Publisher that reads only the root. An author who moved to the envelope alone will see the root key reappear after such an edit; the two never disagree, and the root form goes away with the deprecation.

Editing a manifest by hand, change `materialization.scope` and delete the root key rather than editing the root copy. Two homes holding different values is not a warning — the package fails to load, disappears from the server, and says so only in `/status` `loadErrors`. The rejection is deliberate (guessing which one the author meant could reuse a table across versions that was never meant to be shared), but it means the reappearing root key is a copy to delete, never one to edit.

Publisher enforces these rules identically at **publish** (strict — rejected), at **PATCH** that edits the policy (strict), at **package load** (warn — the package still serves), and in the **scheduler** (an offending package is skipped):

1. **`scope` is a single package-level mode** — `package` (default) or `version`. There is no per-source scope or per-source schedule; those are declared once for the package, not on individual `#@ persist` sources. Declaring it in both homes with different values is rejected: scope decides whether an artifact is version-owned, so an ambiguous intent is never guessed.
   - `package`: persisted artifacts are reused across the package's published versions while they satisfy freshness.
   - `version`: each artifact is owned by one published version.
2. **A `schedule` cron requires `scope: version`.** A package-scoped lineage is reused across versions, so a single per-version cadence is meaningless.
3. **`schedule` and `freshness` are mutually exclusive.** Declare either a `schedule` (the power tier) or a `freshness` policy (the objective tier), never both.
4. **`schedule` must be a valid 5-field UNIX cron** (`minute hour day-of-month month day-of-week`), evaluated in **UTC**. Extensions (`L`, `W`, `#`, `?`) are rejected, so a garbage cron can't pass publish and then silently never fire.

## Build a materialization

A materialization run compiles the package, builds every `#@ persist` source into its table, writes a manifest, and loads it so queries serve from the built tables. It settles at `MANIFEST_FILE_READY` (success) or `FAILED` / `CANCELLED`.

Each run records a **trigger** in its metadata: `ON_DEMAND` (a manual/API build) or `SCHEDULER` (a scheduled fire). Only one materialization can be active per (environment, package) at a time — a second concurrent build is rejected with HTTP 409, and the scheduler coalesces (skips) rather than stacking a second build.

On demand, via the CLI:

```bash
malloy-pub materialize --environment <env> --package <pkg> --wait
```

## Incremental refresh

By default a refresh rebuilds a persisted source's whole table. A source that only ever gains rows at one end can instead declare that a refresh applies a **bounded delta**:

```malloy
#@ persist name="daily_orders" refresh="incremental" watermark="order_date"
source: daily_orders is orders -> {
  group_by: order_date
  aggregate: revenue is amount.sum()
}
```

`watermark=` names one of the source's own output columns, and it has to be a real, orderable, non-aggregate one — the column a new row's position along is decided by. Publisher records how far the table is materialized (`covered_through`) and each refresh recomputes only `[covered_through, frontier)`. The range is **half-open**, so the frontier value itself is left for the next run on the grounds that rows at the frontier may still be arriving.

Where the frontier comes from depends on the watermark's type: a `date` or `timestamp` watermark takes the run's own start time, while a numeric or string watermark is read from the source (`max(watermark)`). One consequence worth knowing before you test it: two refreshes of a date-watermarked source within the same day produce an empty range and skip, which is correct but looks like nothing happened.

Add `merge_key="col,…"` when a row can be **restated** with a new watermark value — an order that moves to a later day. Publisher then applies the delta as a `MERGE` on the declared identity columns instead of deleting the watermark range and re-inserting it, which is the only strategy that tolerates a row changing which range it belongs to. Without it, a refresh replaces the range wholesale, which is correct exactly when a row's watermark value never changes.

**An invalid declaration fails the package, it does not downgrade it.** The rules below are checked wherever a package is admitted — a publish or PATCH answers 400, and a package **load** fails outright, the same severity a model that does not compile has. So a broken declaration cannot sit in a log while the source quietly rebuilds in full forever: `watermark=` without `refresh="incremental"`, `merge_key=` without `watermark=`, a malformed key value, a watermark that names no materialized column (or names an aggregate, or a type with no ordering), a `calculate:` field, or an unsupported dialect. Every rejection is reported at once, so a model with two broken declarations takes one republish to fix. What is _legal but probably unintended_ stays a warning on the package instead: an unrecognized `#@ persist` key, and a keyless delta.

Details that decide whether a run advances or rebuilds:

- **It is exempt from skip-if-unchanged.** A content address does not move when data does, so an incremental source is instructed on every run and its `covered_through` boundary — not its address — decides whether there is work to do.
- **`forceRefresh` never re-seeds.** It means one thing — build even though the content address is unchanged — and an incremental source is exempt from that carry-forward anyway, so the flag has nothing to say about how one is built. Ask for a full rebuild with `reseed` (`malloy-pub materialize --reseed`, or per source with `BuildInstruction.reseed`). Keeping them separate is what lets a schedule drive deltas at all, since the scheduler forces on every single fire.
- **Two sources that compile to identical SQL share everything.** The content address that keys the boundary is a hash of the connection and the canonical SQL — not the source name, not `name=`, not the model file — so a copy-pasted source body collapses onto one table and one boundary however you name it. If their declarations also differ, neither can ever advance: each refresh finds the other's lineage recorded and rebuilds. Publisher warns when this happens and names both sources, since nothing in the model text shows it.
- **Anything unproven falls back to a full rebuild**, which is always correct and merely expensive: no recorded boundary, a boundary describing a different table or watermark, a table whose columns no longer match what the source computes, or `MERGE` asked for on Postgres 14 or older (it requires 15).
- **Postgres, BigQuery, and Snowflake only**, and that is the SOURCE's dialect — the engine that has to express the bounded range. Declaring it elsewhere is a rejection rather than a silent full refresh, so it never looks like it is advancing when it is not — which does mean a DuckDB source has to say `refresh="full"` to be served at all.
- **`storage=` is supported, and the delta is split across the two engines.** The source warehouse computes the bounded range (the predicate is pushed into its own query, so it never streams rows it will not keep) and the DML lands in the destination, which is where the table is. Everything an author declares means the same thing either way. Two differences worth knowing: a stored table holds exactly the source's public columns, so the rename/`except:` rebuild below does not arise from `getSQL()` projecting more than the schema describes — though changing which columns are public still rebuilds, since the stored table's shape no longer matches; and a CHAINED stored source — one reading another stored source's table — still rebuilds every refresh, reported under its own reason code, because its parent's delta can restate rows below the child's own frontier where no delta of the child's would revisit them.

### Driving it from a control plane

An orchestrated build works the same way, but the host, not the publisher, decides what gets built, so three things are the host's responsibility. Each of the first two fails _quietly_ if you get it wrong — a full rebuild every run, which succeeds and looks like a refresh.

- **Give an incremental source a STABLE physical table name.** The boundary is recorded against the table it was measured on, so a generational name (a fresh one per run) makes every run re-seed. That fallback is not a bug: the newly named table is empty, so applying a delta to it would drop everything the old one held. It is reported under its own reason code, `table_renamed`, to distinguish it from the source's definition having moved (`lineage_changed`) — which, where packages are immutable, means a refresh instructed through a different version than the one that established the boundary rather than an edited model.
- **Do not expect `forceRefresh` to do anything.** On an orchestrated build it means nothing at all: it exists to defeat skip-if-unchanged, which never runs when the host supplies the instructions. It does not re-seed — nothing does, in any mode.
- **Ask for a rebuild with `reseed`.** Per source on the instruction, or run-wide in the request body; the two are OR-ed. That is the escape hatch for a boundary or a table you no longer trust, and the per-source form means one source can rebuild while the rest advance by delta in the same run.

Each manifest entry reports the coverage its source reached on a `ledger` object, whose `coveredThrough` (with its `coveredThroughType`) makes progress something you read rather than infer: a value that advanced between runs is a delta that applied. A skipped source reports the boundary that stays in force, and a seeded one reports the boundary probed from the table it just wrote, so it means the same thing whatever the step did. The boundary is reported only there, alongside the watermark and source address it was measured under, because the value alone is not comparable across runs — see [Supplying the ledger yourself](#supplying-the-ledger-yourself), which is the same object you send back if you hold the ledger. Alongside it, `refresh` says what the run actually DID to the table — `delta` (advanced in place), `full` (rebuilt), or `none` (nothing to apply, the boundary stands) — because nothing else on the entry distinguishes them: every fallback above answers success and rebuilds, so a source quietly rebuilding on every run looks exactly like one advancing. It is present exactly when a source is refreshed incrementally, so its absence means only that this one is not.

### Supplying the ledger yourself

The boundary lives in the publisher's own store, which is per-process. If you dispatch a package's refreshes across several interchangeable workers, that store is on ONE of them: a refresh landing anywhere else finds no boundary and re-seeds. With N workers picked without affinity, roughly one refresh in N is a full rebuild of a source whose whole purpose is avoiding them, and it lands on the live serving name.

The fix is to hold the ledger yourself, and the whole protocol is one sentence: **store each manifest entry's `ledger` object, and send the stored objects back as `buildInstructions.ledger` on the next run.** Every incrementally refreshed source reports one — the table it belongs to, the boundary, and the source definition it was measured under — and none of its fields needs to be understood to be used correctly: only the publisher derives them, and it derives a boundary only after the DML that earned it commits.

When `buildInstructions.ledger` is present — even empty — it _is_ the ledger for that run. The publisher reads every boundary from it, touches its local store on no path (not on a delta, not on a seed), and seeds any incremental source with no entry, which is what a first build looks like: an orchestrator that has stored nothing sends `ledger: []`. When the field is absent the publisher uses its own store, exactly as before, so nothing changes until you opt in — and you can start _storing_ the reported entries before you start sending them.

An entry you send is validated, and an invalid one is an **error**, not a quiet rebuild:

- **At create (a 400, before any work starts):** an entry that is malformed, names a table the run's instructions don't build, names one twice, belongs to a source that doesn't declare `refresh="incremental"`, or carries a `sourceEntityId` that is no longer the source's.
- **The `sourceEntityId` rejection is the one you will meet in normal operation.** It is the publisher's content address for the source (`PersistSourcePlan.sourceEntityId`), so a publish or rollback that changes the source's SQL moves it, and the entry you stored — echoed faithfully — was measured under the old one. The 400 is synchronous and says what to do: delete the entry (the source seeds and reports a fresh one) or set `reseed`.
- **Mid-run (a failed run, rare):** a mismatch only the compiled model reveals, such as a `watermark=` or `merge_key=` that moved without moving the SQL.
- **`reseed` bypasses all of it.** An entry for a source being reseeded is ignored, unvalidated — it is the documented recovery from the 400 above, so it cannot itself trip one.

Facts about the _table_ are not input errors and still rebuild rather than fail, exactly as they do with a local ledger: an emptied or unreadable table, drifted columns, or an entry that lags where the table actually is (the half-open range is idempotent, so a delta from a stale-but-lagging boundary recomputes rows into themselves). `refresh` is how you watch all of this from outside: after you flip, the rate of `refresh: "full"` for unchanged sources should fall to about zero.

One Snowflake-specific mechanic: its driver executes exactly **one statement per call**, each on a possibly different pooled session, so the range-replace's delete-then-insert cannot travel as a `BEGIN;…;COMMIT;` script the way it does on Postgres and BigQuery. On Snowflake the same transaction is carried as a single [Snowflake Scripting](https://docs.snowflake.com/en/developer-guide/snowflake-scripting/blocks) anonymous block (`EXECUTE IMMEDIATE $$…$$`) whose `EXCEPTION` handler rolls back — inside the statement, because a follow-up `ROLLBACK` call would reach a different pooled session than the one holding the open transaction. Snowflake also folds bare identifiers to **UPPERCASE** (Postgres folds to lowercase), which is why the warehouse probes quote their output aliases and why table-path decoding folds per dialect.

### Materializing on Postgres

One Postgres-specific invariant to know if you touch the build path, because it has bitten twice and both times the symptom was far from the cause.

Malloy compiles a query's SQL in a **finalized** form: it appends the dialect's `sqlFinalStage` wherever `Dialect.hasFinalStage` is true. Postgres is the only such dialect, and its final stage is `SELECT row_to_json(finalStage) as row FROM …` — the whole result collapsed into a single JSON column named `row`, which is the shape Malloy's own Postgres driver expects to unwrap.

Anything that **materializes** SQL needs the opposite: the bare `SELECT`, projecting real columns. `PersistSource.getSQL()` is that form ([malloydata/malloy#2964](https://github.com/malloydata/malloy/pull/2964) made it compile unfinalized, after the finalized version made `CREATE TABLE AS` produce a one-JSON-column table), and a compiled query's SQL is not — there is no supported way to ask a query for its unfinalized SQL. So **every build path takes its SQL from `PersistSource.getSQL()`**, never from a `PreparedResult`. The incremental delta follows the same rule: it wraps the seed's own SQL in a range predicate rather than compiling a filtered query, which also makes the delta write the seed's shape by construction.

The reverse direction applies when you hand-write SQL for Postgres to run through a Malloy connection: `runSQL` unwraps each row as `row.row`, so a raw `SELECT max(x) AS m` comes back as `[undefined]` and has to be wrapped in `row_to_json` yourself. Both directions are pinned in `incremental_compiler_contract.spec.ts`.

## The standalone scheduler

A self-hosted Publisher can rebuild packages on their cron cadence with no control plane. The scheduler is **opt-in and off by default**:

```bash
PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER=true \
PUBLISHER_MATERIALIZATION_SCHEDULER_INTERVAL_MS=60000 \
  <start Publisher>
```

See [configuration.md](configuration.md) for the env vars. Fire semantics:

- **Sweeps only already-loaded packages.** It never forces a load; a not-yet-loaded package simply isn't scheduled until something else loads it.
- **Arms to the next occurrence.** On first sight it computes the next cron time (strictly future), so a freshly-scheduled package does **not** fire on the arming tick.
- **Recovers one missed occurrence across a restart.** On first arm after a (re)start it re-anchors from the newest recorded `SCHEDULER` run: if an occurrence came due while the process was down, it fires exactly one catch-up and then jumps forward, rather than skipping it. (A schedule set while the scheduler was _disabled_ has no prior run to anchor from, so it is not caught up on first enable.)
- **Skips what it must not fire:** a control-plane-driven package (one with a `manifestLocation`), a package whose policy is invalid, or one with an unparseable cron.
- **Caps fires per tick** (`…_MAX_FIRES_PER_TICK`) so a burst of due packages doesn't stampede; a capped package fires on a later tick.
- **Coalesces** when a build is already active for the package (the in-flight build covers the occurrence).

> **Never set `PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER` on a control-plane-driven (orchestrated) worker.** A package serving live under a control plane has no `manifestLocation`, so the per-package skip does not cover it — the flag being off is the primary guard against the standalone scheduler double-driving refresh the control plane already owns.

## Manage it with the CLI

```bash
# View / set / clear a package's schedule (set also sets scope: version)
malloy-pub schedule view  --environment <env> --package <pkg>
malloy-pub schedule set "0 6 * * *" --environment <env> --package <pkg>
malloy-pub schedule clear --environment <env> --package <pkg>

# List a package's runs (ID, Status, Trigger, Started, Completed, Error)
malloy-pub list materialization --environment <env> --package <pkg>

# List every package's runs across the environment (adds a leading Package column)
malloy-pub list materialization --environment <env>

# Inspect one run (timings, sourcesBuilt/sourcesReused, manifest entries)
malloy-pub get materialization <id> --environment <env> --package <pkg>

# Drop a materialization record and its physical tables
malloy-pub delete materialization <id> --environment <env> --package <pkg> --drop-tables
```

The `schedule` commands share the server's publish-gate validation: an invalid cron or an illegal scope/freshness combination is rejected, so a rejection means the change was unsafe.

> **`--drop-tables` drops _every_ physical table in that run's manifest**, not just one source's. Auto-run assigns stable table names and carries unchanged sources forward, so an old run's manifest names tables a newer manifest still serves. To remove a persisted source, drop the old run **first, then `materialize --wait`** so every still-persisted source is re-created; dropping a run whose tables the current serving manifest depends on, without rebuilding, breaks queries.

## Standalone vs. hosted (control-plane) deployments

The same package definition behaves differently depending on who drives materialization. Honest divergences to be aware of:

- **Who refreshes.** Standalone: the opt-in scheduler above. Hosted: the control plane drives refresh; the standalone scheduler stays off and skips control-plane-driven packages.
- **Tables-only vs. two-phase.** A standalone fire is a single-phase build that materializes persist sources into **tables**. A hosted deployment runs a two-phase job — tables, then a second pass over indexed dimensions — so index behavior is a hosted-only concern and can't be exercised locally.
- **Per-version tables.** `scope: version` is a policy contract. In the current standalone auto-run, a materialized table's identity is a content address derived from its connection and the source's canonical SQL (the `sourceEntityId`) — not the `#@ persist name` (that is the physical table name) and not the package version; true per-version tables are produced when the control plane assigns versioned build targets. Standalone is the right place to exercise the _policy_ (scope/schedule rules) and single-version builds, not per-version fan-out.
- **Incremental refresh.** Available to both, with the same declarations and the same delta. Standalone, the scheduler drives it; hosted, the control plane does, subject to the three host responsibilities in [Driving it from a control plane](#driving-it-from-a-control-plane) — a stable physical name per incremental source, `reseed` rather than `forceRefresh` to ask for a rebuild, and the boundary read back from each manifest entry.
- **Who owns the boundary.** Standalone it is the publisher's own store, always. A hosted deployment that spreads one package's refreshes over several workers can instead hold the boundary itself and echo it back on the instructions ([Supplying the ledger yourself](#supplying-the-ledger-yourself)), because that store is per-process and a refresh landing on a worker that has never seen the table would otherwise re-seed. The publisher still derives every boundary and still validates every one it is handed.
- **Physical-table GC.** Deleting a materialization with `--drop-tables` drops its tables. Deleting an environment or package removes the materialization **records only** — physical tables are intentionally left in place (physical-table GC is the caller's responsibility), so clean up tables you no longer want explicitly.

## Attribute a build's cost

`materialization.queryMetadata` (and the per-source `#@ persist queryMetadata.*`) tags every statement a build issues — the staging CTAS, the swap, the rename — and every statement a _query_ against the source issues, so the backend's own reporting can attribute both. Separating build cost from query traffic is the context layer's job, not the declaration's: publisher adds `class=materialize` plus the package, source, trigger and run id to a build, and `class=interactive` with none of those to a served query. The block is named for materialization historically; it declares what the source is, not only how it is built. See [query-metadata.md](query-metadata.md).

## Tune for cost and performance

The materialization history (`list` + `get` above) records per-run timings and how many sources were built vs. reused — enough to decide what to persist, what to stop persisting, and how to schedule it. The [`malloy-materialization-tuning`](../skills/malloy-materialization-tuning/SKILL.md) skill walks an agent through reading those signals and proposing (recommendations-only) changes.

## Pre-aggregation

`#@ persist` stores a source you wrote. [Pre-aggregation](preaggregation.md) stores a rollup Publisher derives for you: annotate a measure with a grain, and covered queries read a small pre-grouped table instead of the base, with no change to the queries themselves. Rollups appear in the same build plan (as `origin: "preaggregate"`) and build through the same manifest and scheduler described above.
