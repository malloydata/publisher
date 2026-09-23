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

### `#(access_filter)`-gated sources and materialization

A source protected by a `#(access_filter)` gate — its own, or one carried from a joined or derived
source — is refused for `storage=` and for pre-aggregation, unconditionally. A **colocated**
`#@ persist` (no `storage=`) is different: it is admitted when the gate is _proven_ to be the entry
point's own row filter, and refused otherwise.

- **`storage=`** refuses at build time, unconditionally, alongside an unbound parameter or a given
  the build would substitute (see
  [§ Tenant-scoped sources](#tenant-scoped-sources-where-a-given-may-sit) below and
  [persist-storage-tutorial.md § Eligibility refusals](persist-storage-tutorial.md#eligibility-refusals-refused-at-build-time)):
  a materialized-once table is served frozen to every caller, and the served shape carries no gate
  to re-evaluate. This refusal is unaffected by anything below.
- **A colocated `#@ persist` and givens.** Distinct from the gate question, and decided by WHERE the
  given sits. A given the persisted query is built with — inside the `-> { … }`, or in a field the
  query uses — is substituted at build time with its declaration default, so the artifact holds one
  caller's slice and every caller is served it; that shape is **refused**. A given applied when the
  source is READ — a `where:` in its extend block, or a dimension, measure or join declared there —
  never reaches the build and binds per caller over the materialized rows; that shape is admitted,
  and is the documented form (see [row-level-access.md](row-level-access.md)).

- **A colocated `#@ persist`** is not served frozen with respect to the gate at all: persistence
  changes only where the rows are read FROM, never whether the entry point's own `#(access_filter)` is
  re-evaluated — the substitution swaps only the source's relation SQL, and the gate applies as the
  reading query's own `WHERE` on top of it, so filtered rows come back filtered. When the compiler can
  _prove_ the gate is the entry point's own row-level filter and nothing else is reachable beneath it,
  the source is eligible and serves correctly filtered from the materialized table. It is still
  refused when that cannot be proven — a gate reachable only through a join (join-only gate
  attribution is not traced), an inherited gate the compiler cannot attribute cleanly, or a gate that
  does not classify as a row filter at all. Drop `#@ persist` from the source, or restructure it so the
  condition is the entry point's own proven row-level gate.
- **`#@ preaggregate`** refuses unconditionally, regardless of the gate's classification. A rollup
  synthesizes a colocated `#@ persist` over an import of the annotated base, and none of the
  pre-aggregation modules has any `#(access_filter)` awareness of its own — so this refusal is the only
  thing standing between a gated source and the pre-aggregation tier. It also groups _across_ the
  gated column, so the column is not even present in the rolled-up result to filter afterwards, even
  in principle. A refused rollup names `#@ preaggregate` and the gated source rather than the
  synthesized rollup's own name, which the author never wrote.

Every refusal names the source and the remedy; a package carrying one fails to build (or, for
`storage=`, fails that materialization run) rather than silently serving the gated source to
everyone.

### Tenant-scoped sources: where a given may sit

A source scoped to the caller — `where: org_id = $ORG_ID` — can be materialized into a `storage=`
destination and served per caller. It is built **once**, holding every tenant's rows, and each
caller's term is applied when they read it.

Without this the options are one artifact per tenant, each with its own build, schedule and
freshness, or no materialization at all. One shared artifact replaces both.

That works because of how Malloy builds a persist source: the build SQL is the persisted relation
**alone**. The source's own extend-block `where:` is not in it — it refines the relation when the
relation is read. So the given was never frozen into the artifact, and the serve path re-applies the
term with the value that caller supplied.

What is refused is a given the **build substitutes**, because then the predicate is inside the frozen
rows with one caller's value already in it and nothing downstream can undo it. The only value
available at build time is the declaration's default, so that is whose rows everyone gets.

Put the other way round: a predicate that varies per caller is part of the **question**, not part of
the relation being stored. Materialization stores relations, so such a predicate was never a
candidate for freezing in the first place.

That gives you a rule you can apply to a shape not listed below. **If changing a caller's value would
change the SQL the build runs, the predicate is in the artifact, and the source is refused. If it
would not, the predicate is read-time and is re-applied per caller.**

**Write the source as a query, not as a filtered table.** Both forms below persist a
query — `raw -> { select: * }` — and then refine it. That is not stylistic: a source
written as a plain extension of a table, `source: orders is raw extend { where: org_id =
$ORG_ID }`, stays type `table`, and only a query-shaped source is treated as a build
root. `#@ persist` on one is a **silent no-op** — nothing is built, no error is raised,
and the source is served live exactly as if the annotation were absent. Publisher warns
on the package when it sees an annotated source missing from the build plan, which is
the only signal you get, so reach for the `-> { select: * }` form first.

```malloy
given:
  ORG_ID :: number is 1

// Served per caller: the term is outside the persisted query.
#@ persist name="orders" storage=lake
source: orders is raw -> { select: * } extend {
  where: org_id = $ORG_ID
}

// Refused: the persisted query reads the given, so `org_id = 1` is in the build SQL.
#@ persist name="orders_baked" storage=lake
source: orders_baked is raw -> { where: org_id = $ORG_ID; select: * }
```

The second form is refused as `given_in_persisted_query`, and the refusal names the move that fixes
it. It fires however the given reaches the query — including through the source the query reads, so
`source: scoped is raw extend { where: org_id = $ORG_ID }` followed by
`#@ persist source: r is scoped -> { … }` is refused too: the query reads `scoped`'s filter, so the
value is substituted just the same.

Two positions carry a given that is refused even though the build leaves them out: a declared
`dimension:` or `measure:` that reads a given itself (`dynamic_projection`), and a join's `on:`
condition (`dynamic_join`). Neither is in the artifact, so neither is a leak; they are refused
because the serve shape has nothing that binds the given per caller in that position.

#### Per-user visibility through a joined grant table

A join to a given-scoped source **is** admitted when that source is itself materialized into
storage, joined by name, and admissible under these same rules. This is the visibility idiom: a
source scoped to the caller's org joins a grant table scoped to the caller's org and user, and a
dimension decides what the caller may see by null-checking the join.

```malloy
#@ persist name="grants" storage=lake partition="org_id"
source: grants is raw_grants -> { select: * } extend {
  where: org_id = $ORG_ID and user_id = $USER_ID
}

#@ persist name="opps" storage=lake partition="org_id"
source: opps is raw_opps -> { select: * } extend {
  where: org_id = $ORG_ID
  join_one: g is grants on opp_id = g.opp_id
  dimension:
    visible is restricted = 0 or g.opp_id is not null
    shown_name is pick name when visible else '[Hidden]'
}
```

Neither artifact is filtered by user: a persisted source's joins and dimensions are not in its
build, and `grants` leaves its `where:` out like any other. At read, the serve shape re-emits
`grants` with its terms bound to the caller's values and re-emits the join against that binding,
so `visible` is evaluated over exactly the grant rows the live query would join. The build plan
reports the join under `joinedTerms`, beside the source's own `strippedTerms`, naming the joined
source whose binding the per-caller answer depends on. The rule is transitive: a grant table that
itself joins a further given-scoped source is held to it one level down.

It is refused as `dynamic_joined_where` when the joined source is not persisted, is persisted
colocated (no `storage=`), would itself be refused, or is joined through an inline refinement
(`join_one: g is grants extend { … }`), which compiles to an anonymous source nothing can bind.
Declare the refinement on a named source and join that.

When the joined source's binding is not on the shape — stale past its window, never built, or
refused — the joining source is withheld with it and serves live, rather than serving
`visible` over no grants. Its sibling sources are unaffected.

**A revoked grant stays visible until the grant table rebuilds.** Here the frozen row data *is* the
access decision, so the grant table's freshness is the revocation latency. Give it a short
`freshness.window` with `freshness.fallback="live"`, so that once the grant table ages out the
sources joining it are answered live, from current grants.

The same rule serves an entry point declared as a plain extension: `source: visible is opps extend
{ join_one: … ; where: g.user_id = $USER_ID }` inherits `#@ persist`, builds nothing new (its build
is `opps`'s relation), and is served from `opps`'s table with its own join and term re-applied.

A source that reaches a given-scoped source only through a join its persisted query never
reads is admitted. That rests on the COMPILER: Malloy prunes an unread join out of the build
SQL, so nothing given-derived reaches the artifact. It is a property of Malloy's pruning
rather than of the gate, and worth knowing for anyone changing the compiler version.

#### Joins between materialized sources

A join is served from storage only when the joined source is **also** materialized. So persisting a
caller-scoped dimension table is what makes a join to it servable at all: without it, a query using
the join does not simply lose the join, it loses the tier and is answered live.

This is also how two sources on different connections become joinable. They cannot be queried
together live, but materialized into the same destination they are siblings, and the join compiles.

#### Where the per-caller value comes from

Serving per caller is only a boundary if the caller cannot choose their own value — and
**Publisher does not decide that**. A given's value is whatever the request supplies, so on
a bare Publisher every given is a convenience filter rather than an access control.

`#(secure)` marks a given whose value must come from the HOST rather than the caller. It is
a contract with whatever fronts Publisher: a gateway that authenticates the caller,
resolves their assigned values, and replaces anything the request supplied. Publisher does
not itself strip or resolve it. If nothing in front of Publisher implements that, marking a
given `#(secure)` changes nothing about who can send what.

**A `#(secure)` given must be set-valued**, and the shape that scopes by one is therefore
`in`, not `=`:

```malloy
// A boundary: the caller cannot supply ORG_IDS, and an unassigned caller sees nothing.
#(secure)
given: ORG_IDS :: number[]

#@ persist name="orders" storage=lake
source: orders is raw -> { select: * } extend {
  where: org_id in $ORG_IDS
}
```

The reason is that it has to fail closed. A set has an empty list as a natural
impossible value, so a caller with nothing assigned filters to zero rows whatever
operator the model uses. A scalar has no equivalent — there is no value of `number` that
matches nothing — so a caller with nothing assigned cannot be given one.

This matters because a host implementing the contract has nothing to fail on: a scalar
declaration is the shape it cannot honour, so the safe outcome is that the given is simply
never resolved and the caller's own value is honoured — by a model that reads as though it
were gated. Declare the attribute as a set and scope with `in`.

### `partition=`: laying the artifact out

`#@ persist partition="org_id"` writes the stored table as one directory per distinct value, so an
equality term on that column reads only the files it names. `partition="org_id,day"` nests them in
the order given.

It is a **layout** and carries no isolation. Every stripped term is re-applied at read whether or not
its column is partitioned, so a partition list that omits the column a caller is scoped by costs a
full scan, never a leak — which is why the list is the author's free choice rather than something
derived from the source's filters. Partitioning by a high-cardinality column, or by several columns
at once, buys pruning at the cost of many small files.

Partitioning by the column a caller is scoped by gives a useful asymmetry: **read cost tracks the
caller's own partition, while build cost tracks the whole artifact.** A caller's queries do not get
slower as tenants are added; the build does, since it is one pass over everyone's rows. Pruning
depends on the scoping term being an equality (`=`, `in`) on a partitioned column, and
nothing checks the column's cardinality for you — a partition per caller across very many
callers is the many-small-files case, and it is your judgement to make.

That asymmetry is the argument for one shared artifact over one per tenant. Both have the same read
cost; the per-tenant arrangement also has one build, one schedule and one freshness story *per
tenant*, all of which can drift apart.

`partition=` is part of the source's content address, so changing it rebuilds the table. A source
that declares none addresses exactly as it did before the key existed, so nothing already
materialized is disturbed by this feature.

Each name must be a column of the source's **public** projection: the stored table is narrowed to
that surface, so a hidden or `except:`-ed column is not there to partition by
(`partition_column_not_public`, `partition_column_unknown`). `partition=` requires `storage=` — a
colocated build writes into the source's own warehouse, where the layout is that warehouse's DDL —
and is refused rather than ignored without it (`partition_without_storage`).

### The freshness contract for a gated colocated persist source

Admitting a proven row-level gate applies unconditionally. The refusal it relaxes never fired at
_load_: it fires inside the build path (`deriveSelfInstructions` / `executeInstructedBuild`), so a
package with a colocated `#@ persist` on a `#(access_filter)`-gated source already loads, appears in
`plan.sources`, and serves live — what was refused was its _materialization_, not the package.

**So such packages already exist.** On upgrade, a run that used to fail succeeds when the gate proves
row-level and attributed to the entry point, and the next auto-run or scheduled build materializes the
source and binds it for serving **with no author action** — a source that served live yesterday serves
from a possibly-stale artifact afterwards, subject to the staleness below.

The given refusal above runs the other way, and such packages may also already exist. On upgrade a
colocated `#@ persist` whose persisted query references a given stops materializing: the package
still loads and the source still serves — live, correctly, per caller — but its next materialization
run skips it and records the refusal, and an artifact built before the upgrade is unbound on the next
reload rather than served.
The remedy is to move the given out of the persisted query, which the refusal message names.

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
changes _without its watermark advancing_ falls outside every future delta and is never re-read.
Take `orders`, gated with `#(access_filter) org_id = $ORG` and declared
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

**On a caller-scoped source, the merge matches on more than you declared.** A source whose
term is stripped and re-applied per caller is stored once holding every caller's rows, so a
key you chose against the source as you wrote it — `order_id`, unique within one org — is
ambiguous over what was actually stored, where the same `order_id` appears once per org. A
merge on that key alone would match another caller's row and update it.

So the match also carries the columns those stripped terms filter on, and the effective
identity is your key plus that scope. This restores the relation you chose the key against,
and there is nothing to change in the model: declare `merge_key=` exactly as you would for a
source that is not scoped.

Scoping is all or nothing. If a stripped term resolves to no column of the source — one
reaching through a join, say — the source is refused (`merge_key_scope_unresolved`) rather
than scoped by the terms that do resolve, since a partial scope is narrower than the bare key
but still wider than your relation, and would look like it works. Scope such a source with a
term over its own columns, or drop `merge_key=` and refresh by watermark range.

Two things to know about the narrowed match. A row whose **scope column value changes**
between refreshes no longer matches its stored copy, so it is inserted beside it rather than
updated — a duplicate. The same happens with a term whose columns are combined with `or`
(`where: org_id = $ORG_ID or user_id = $USER_ID`), because the match carries both columns and
is therefore narrower than the disjunction you wrote. Both fail in the same direction: a
duplicated row, never a row belonging to another caller. If either applies to your source,
rebuild it rather than refreshing.

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

**A rollup over a caller-scoped source is refused** (`preaggregate_over_dynamic_source`).
A persisted source may be scoped by a given because the term is left out of the build and
put back when the rows are read; a rollup has no such read. It is stored pre-grouped and
answered from directly, so there is no point at which a caller's term could be applied to
it — and building it reads the base, which applies that base's `where:` with the
declaration's default, so the rollup would hold one caller's aggregate and serve it to
everyone. Roll up a source that is not caller-scoped.
