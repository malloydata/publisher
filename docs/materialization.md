<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Materialization (Malloy Persistence)

Materialization pre-builds a Malloy source into a physical table so queries read the table instead of recomputing the source. In Publisher it is driven by **Malloy Persistence**: you annotate a source `#@ persist`, and Publisher builds it, records a manifest, and serves queries from the built table.

This doc covers the open-source Publisher's materialization surface — how to declare it, the rules Publisher enforces, how to build on demand or on a schedule, the `malloy-pub` CLI, and how a self-hosted (standalone) deployment differs from a control-plane-driven (hosted) one.

> **v1 and v2.** Most of this page documents v1, `#@ persist` on a source and `#@ preaggregate` on a measure, which is what a server runs by default. Materialization v2, `#(materialize)` on a source and `#(preaggregate)` on a measure, is in development behind `PUBLISHER_MATERIALIZE` (default off). With the switch off, Publisher ignores both v2 annotations and answers the source live; a `#(materialize)` it ignores gets no warning and no serve verdict. With it on, write a new materialization as `#(materialize) name="…"` for a table in the source's own warehouse (`name=` is required there) or `#(materialize) storage="…"` for a storage destination, which is served only with `PERSIST_STORAGE_MODE=on` too, and a new rollup as `#(preaggregate) grain=[…]`. `#@ preaggregate` is deprecated in v2; `#@ persist` on a view and `#@ -persist` are not. [materialization-migration.md](materialization-migration.md) is the recipe for moving v1 annotations to v2, and [the served copy](#the-served-copy-in-development-publisher_materialize) documents how v2 serves.

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

A misspelled `#@ persist` persists nothing and the source serves live: `#@persist` (no space), `# persist` or `#(persist)` (another route), and `#@ persits` (a misspelled directive) are each reported in the package's `warnings` with code `annotation-near-miss`, naming the spelling it meant. See [packages.md](packages.md#annotations-that-do-nothing).

`name=` is the physical table Publisher writes. Persist the sources that are expensive to compute and reused by many queries; leave cheap or rarely-read sources unpersisted. The [materialization tuning guide](../skills/malloy-materialization/reference/tuning.md) helps decide.

`name=` may also name the container the table goes in — `name="analytics.order_summary"` writes `order_summary` into the `analytics` schema/dataset rather than the connection's default one. The container must already exist; Publisher does not create it. On BigQuery a dataset is required, since a table cannot live outside one.

The name is a plain identifier path: dot-separated segments of letters, digits, `_` and `-`. Publisher quotes each segment for the target warehouse itself, so case is preserved (`name="OrdersDaily"` creates `OrdersDaily`) and a reserved word works (`name="order"`). Don't quote inside the value: a name holding quotes, spaces, backticks or semicolons, such as `name="\"Orders Daily\""`, is refused when the model loads, because the name is written into the build's DDL.

Each source needs a table of its own. When two different sources resolve to one table in one destination (two models each persisting a source named `summary` with no `name=`, say), each build overwrites the other's, so Publisher serves neither from it: queries on both are answered live, the package carries a warning naming them, and their serve verdicts read `live` with reason `refused`, until each has a `name=` of its own. Such a publish is refused outright, and so is a build of a package already loaded with one, before anything is written; a deployment that must publish one anyway can set `PERSIST_COLLISION_ENFORCE=false`, which turns the refusal back into a warning (the sources are still answered live). The same source reached from two models shares its table legitimately, since its content address is the same: only different sources on one table collide. A `#(materialize)` source on another source's table, whether its name is the default or an explicit `name=`, is never built: it is reported as `materialize-not-built`, naming the other sources.

### Whose table it is

Packages, and environments, can share a connection or a storage destination, so Publisher's own builds (auto-run) never name a table after the source alone:

- **Without `name=`** (a `#@ persist` or `#(materialize) storage=` source, or a rollup), the table is `<source>__p<8 hex>`, the hex a hash of the environment and the package: `order_summary__p3f9c2a1b`. Two packages that each persist a source called `order_summary` on one schema build two tables, and each serves its own rows. A long name is cut and given a short hash so its table segment stays within 50 characters. Under `scope: "version"` the version's suffix takes its place ([package-versions.md](package-versions.md#materializations)).
- **With `name=`**, the table is called exactly that, since the author chose it and a reader outside Malloy may rely on it. So before every build Publisher checks who holds the name, and marks the table it builds with a comment, `malloy-publisher:table owner=<environment>/<package>` (URI-encoded). A name that is free, or held by a table marked as this package's (or this package's own `name=` view), is built. A table with no mark is built only when this package's own committed runs say they wrote it there (a table from before the mark existed), and is then marked. Anything else (a table another package marked, another package's view, a table or view Publisher did not make, or a table with no mark this package's records do not name) refuses that source before anything is written, and it answers live: the run's `failures` names the holder, coded `table-owned-elsewhere`. A holder that cannot be read (the probe fails, or the dialect cannot be asked) refuses it too, coded `table-ownership-unverified`, since building would overwrite whatever is there. The package's older entries for a refused table are retired at once, so it never goes on serving rows another package wrote, and a retired entry never again counts as this package's record of the table (for the unmarked-table rule above, or for a `name=` view). Give one of the sources another `name=`, or drop the other table if nothing needs it.
- **A `name=` that ends the way Publisher's own names do** (`__p` and 8 hex digits, or `__v<version>_` and 8 hex digits) is refused, coded `table-owned-elsewhere`: it could be another package's default-named table, which carries no mark and which that package builds without a check.
- **Two builds of one `name=` at once** on one server are taken in turn, from the check until the mark is written, so the second sees the first one's mark and is refused rather than both building the table.
- **Reuse** (skip-if-unchanged) of a table at a `name=` holds only while the table is still marked as this package's; otherwise the source is rebuilt, and checked as above.
- **Deleting a run with `dropTables=true`** skips a table another package has marked since the run built it: it is that package's now. It is retired, not reported as owed.

Host-orchestrated builds are not checked or renamed: the host chooses their names. The hash and the mark keep packages apart within one server; two servers whose environments and packages have the same names must not point those packages at the same schema. The mark is written right after each build; a server that stops between the build and the mark leaves a table this package's own records name once the run commits, and otherwise a table its next build refuses, which dropping it clears.

**Upgrading from a release before package-owned names.** The first auto-run of each package rebuilds its default-named tables under their new names, once. The old tables are not dropped: the old runs still name them, and deleting those runs with `dropTables=true` drops them. Until that first build a package serves its old tables as it did before, so build every package once after upgrading. A `name=` table two packages both wrote (both their records name it) is claimed by the first of them to build again, and marked; the other is refused it from then on and answers that source live.

### Opting a reader out: `#@ -persist`

`#@ -persist` is Malloy core's own opt-out, and it is not deprecated: Publisher honours it for `#@ persist` sources and, in development, for `#(materialize)` sources alike ([materialization-migration.md](materialization-migration.md#persist-opt-out)).

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

### A query over a persisted source

A source whose query reads a persisted source — the private-fact / public-wrapper idiom — is not a build target of its own, but it reads the stored table too:

```malloy
#@ persist name="orders_fact" storage=lake
source: _orders_fact is raw_orders -> { select: * } extend {
  where: org_id = $ORG_ID
}

// Served from orders_fact's table, with the fact's where: applied per caller.
source: orders is _orders_fact -> { select: * }
source: totals is _orders_fact -> { group_by: org_id; aggregate: n is count() }
```

On a storage destination the wrapper is read by the served copy as written, over the re-rooted fact, when everything it reads is itself stored there. A wrapper whose query or joins reach a source that is not materialized there — a warehouse table, or a persisted source whose table is stale past its window — is served live instead.

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
everyone. A source a compile-time gate refuses is also a package warning, code
`persist-source-refused`, carrying the gate's message, with `subject` the source (a rollup's base
for a rollup).

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
root. `#@ persist` on one is refused rather than ignored: the package loads with a
warning naming the source, and a materialization run that covers it (a whole-package
run, or one that names it) fails with an error naming it, so nothing is built until the
annotation moves or goes. Reach for the `-> { select: * }` form first.

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
because the served copy has nothing that binds the given per caller in that position.

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
build, and `grants` leaves its `where:` out like any other. At read, the served copy reads
`grants` with its terms bound to the caller's values and the join against that binding,
so `visible` is evaluated over exactly the grant rows the live query would join. The build plan
reports the join under `joinedTerms`, beside the source's own `strippedTerms`, naming the joined
source whose binding the per-caller answer depends on. The rule is transitive: a grant table that
itself joins a further given-scoped source is held to it one level down.

It is refused as `dynamic_joined_where` when the joined source is not persisted, is persisted
colocated (no `storage=`), would itself be refused, or is joined through an inline refinement
(`join_one: g is grants extend { … }`), which compiles to an anonymous source nothing can bind.
Declare the refinement on a named source and join that.

When the joined source's binding cannot be reproduced on the shape — stale past its window, never
built, refused, bound on a different destination, or declared with a non-public access modifier —
the joining source is withheld with it and serves live, from current grants. Without that, every
field reading through the join would fail the shape compile, and the fallback that answers a failed
compile would cost every sibling source in the model its joins, dimensions and measures.

**A revoked grant stays visible until the grant table rebuilds.** Here the frozen row data _is_ the
access decision, so the grant table's freshness is the revocation latency — and the
[gated-source staleness rules](#the-freshness-contract-for-a-gated-colocated-persist-source) apply to it in full:

- **Give the grant table a freshness window with `fallback="live"`.** A window is the only control
  that bounds revocation: once the table ages past it, the sources joining it are withheld and
  answered live, whether or not a rebuild ever lands. (`fallback="fail"` drops to live the same way
  today; `stale_ok` keeps serving the stale grants, so it bounds nothing.) A grant table with no window is never stale,
  and serves a revoked grant for as long as its table exists.
- **Do not refresh a grant table incrementally.** A revocation is usually a deleted row, and a
  [delta](#incremental-refresh) reads only rows its watermark admits — a deleted row is never
  re-read, so the stored grant survives every run. Rebuild it in full, or `reseed` it.
- **The window is enforced from the freshness fields a control plane stamps on the manifest it
  distributes.** A standalone Publisher's own post-build load binds its entries un-gated, so there
  the window does not bound anything and only a rebuild does.

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

A host learns which givens it must set from `GET …/packages/{pkg}/entities`: each model's
`givens` lists every given the model surfaces, with `secure: true` on the ones declared
`#(secure)`. It is computed when the package loads, so the host does not compile the package
again. It covers `.malloy` models only, not `.malloynb` notebooks, and a model that failed to
compile has an entry with `error` and no `givens`: read that model's givens as unknown, never as
none. An older server instead lists them as `secureGivens` on the package that
`POST …/packages` and `GET …/packages/{pkg}` return (notebooks included, absent while any file
fails to compile), and this one still does while `PUBLISHER_PACKAGE_INLINE_FIELDS` is `on`.

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
cost; the per-tenant arrangement also has one build, one schedule and one freshness story _per
tenant_, all of which can drift apart.

**Build memory follows the table's width times the partitions in flight, not its row count.** The
build is DuckDB's partitioned COPY, which buffers rows per partition inside the buffer manager and
charges the appender one vector per column for every partition it has met. So for a passthrough-sourced
build (Postgres, BigQuery, Snowflake) the publisher orders the insert's SELECT by the partition columns —
at the top of the INSERT statement, leaving the SELECT it was handed, and so what the warehouse runs,
its query tag and what the source is addressed by, unchanged — runs that insert on one thread, since a
DuckDB-side sort is read in parallel and several appenders each meet every partition again, and sets
`partitioned_write_flush_threshold` on the build session (`PUBLISHER_PARTITIONED_WRITE_FLUSH_THRESHOLD`,
default 8192 rows) so it flushes long before DuckDB's own half-million-row default. Without those, a
308-partition, 120-column source fails against a 768MB `memory_limit` before or shortly after its first
rows, from Postgres and from BigQuery alike; with them it builds in seconds. The sort is DuckDB's, out of
core against the build's spill directory, and it reads the whole result before the first partition file is
written — memory traded for local disk and wall-clock. That disk is the build's working directory under the
process temp directory; in a container with no volume there, a large spill is an ephemeral-storage
eviction rather than a caught error, so give the worker a volume or a limit before building at that scale. `PUBLISHER_PARTITIONED_WRITE_FLUSH_THRESHOLD=off`
turns all three off. An incremental delta into a laid-out table gets the threshold and the single thread
through its session, not the ordered read. A chained build is not changed.

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

### Connections that cannot write

A colocated build creates a table in the source warehouse, so it needs a connection that can. A
connection whose static attributes report `canPersist: false` (SQL Server today: the connector is
read-only) has a plain `#@ persist` refused at load with reason `read_only_dialect`; the source
serves live and the package's warnings carry the refusal. A `storage=` destination is a different
question — it reads from the source and writes elsewhere — and the storage build names the source
types it can read.

### Stored column types follow the model, not the warehouse

A `storage=` table holds what the warehouse returned, and the warehouse's type for a column is not
always the type Malloy declares for the field. Date arithmetic is the common case: Postgres answers
a month truncation, and DuckDB answers `order_date + 1 month`, with a timestamp, though Malloy types
the field as a date. A `sql_boolean` over a 0/1 integer, or a `sql_number` over text, keeps the
warehouse's type too. Served as the stored type, such a column changes answers: a date compared
with a date column no longer compiles, and a boolean groups as `0` and `1`.

A storage build therefore casts each `string`, `boolean` and `date` column to its declared type
(`VARCHAR`, `BOOLEAN`, `DATE`) in the destination, around the warehouse's result; the warehouse
runs exactly the build SQL it ran before. Numbers and timestamps are not cast: every DuckDB numeric
type serves a Malloy `number`, and either DuckDB timestamp type serves a `timestamp` or
`timestamptz`, since builds and queries both run in UTC.
Under a query time zone (`timezone:` in the source's query) a `date` is not cast at build: DuckDB
would take an instant's date in the build session's UTC, not in that zone. A cast that cannot
convert a value (a boolean the warehouse returned as `'open'`) retries the build without the casts
and stores what the warehouse returned, rather than failing a build that used to succeed or storing
`TRY_CAST`'s NULLs.

A column whose stored type still does not serve as its declared type, such as any castable column of
a table built before this cast, is read through a cast to its declared type where that is faithful:
an integer flag as a `boolean`, a naive timestamp as a `date`, and an instant as a `date` when the
source has no query time zone. Every field, join and view built on it still serves from storage. A
column no cast reproduces faithfully (a `number` the warehouse returned as text, a `boolean` stored
as text) is left off the shape when nothing on the shape names it, so only a query that reads it is
answered live; when a dimension, measure, join, filter or view names it, that one source is served
live instead, and the rest of the model keeps serving from storage. An incremental source whose
table holds a type a rebuild would cast re-seeds on its next run, since a delta only inserts into
the columns the table already has. The package reports each such column as a warning with code
`storage-column-type-mismatch`, naming the source, the column, its stored and declared types, and
whether it is cast or answered live. A rebuild stores the castable ones as declared. For the
rest, give the field a native cast in the model so the warehouse returns the declared type, for
example `amount_text::"double"`: a cast to a type Malloy already believes the field has, such as
`::number` here, emits no SQL.

**Columns with no Malloy basic type.** A stored list or struct column is declared as Malloy's array
or record type, so it reads back as it does live, when the source's field is one; a column Malloy has
no faithful declaration for (a MAP, UNION, ENUM, BLOB, INTERVAL or JSON column, a list or struct
holding one, or a list or struct whose field live types as a warehouse-native type) is left out of its
source, so a query reading it is answered live and every other query reads the table. The build no
longer refuses such a table; it refuses (reason `not_duckdb_portable`) only one with no column left
to declare. Each left-out column is counted in `publisher_storage_serve_shape_type_fallback_total`.

**Integers past 2^53.** A stored table's columns are declared to Malloy as plain numbers, which it
reads through a JavaScript number, so 9007199254740993 would come back as 9007199254740992. A
column whose field Malloy types as a big integer (a DuckDB `BIGINT`, `HUGEINT` or `UBIGINT`, a
BigQuery `INT64`, a Snowflake `NUMBER(p,0)`, or a sum or max over one) is therefore read through a
native cast: `::"HUGEINT"` in a `storage=` table, and, in a colocated one (a colocated
`#(materialize)` table, or a v1 `#@ persist` one), the
column's own warehouse type, which each colocated build reads from the warehouse's catalog and
records on its manifest entry (`nativeSchema`). Malloy types the cast as a big integer, so the
value comes back with every digit and the field keeps live's `bigint` subtype; selecting,
grouping, filtering and aggregating it all stay served from storage, and a `primary_key:`,
dimension or measure over it keeps compiling. Because the cast is to exactly the stored type, no
value is narrowed: a DuckDB `UHUGEINT` past 2^127 − 1 is read as `UHUGEINT`, and SQL Server's
`decimal(38,0)` as itself. A `count()` or a sum of 32-bit integers is an ordinary integer live too,
and is not touched. A column no cast reads exactly is left out per query: a query that reads it is
answered live, every other query still reads the table. That is a column stored as a float, whose
digits are already gone; a colocated column whose warehouse type Malloy does not read as a big
integer, such as the `numeric` Postgres stores a sum of `BIGINT`s as; a MySQL integer column, since
MySQL's `CAST` names no integer type; and a colocated column whose type its build did not record (a
table built before this, a manifest whose producer does not record it, or an unqualified table name
the warehouse's `information_schema` finds in more than one schema, where the build cannot tell
which table it wrote). Served never exceeds live: on a source whose live answer itself rounds such values
(Postgres returns its rows through JSON), a `storage=` table reads the column as a plain number,
which rounds the same way, and a colocated one reads it through Postgres's own `bigint` type, on
live's own path ([known limitation 12](materialization-v2-plan.md#known-limitations-to-revisit)).

A run's record (`GET …/packages/{packageName}/materializations/{materializationId}`, and the
listing) returns its build manifest. Each entry names the physical table it built, and a colocated
entry's `nativeSchema` lists every column of that table with its warehouse type, including columns
the model marks private or internal. No row value is returned. Publisher does not authenticate
callers, so the gateway in front of it must restrict the materialization routes to operators (see
[the operating model](security-posture.md#operating-model-a-microservice-behind-your-gateway)).

## Will it serve? The serve verdict

Every source a package materializes (each `#@ persist` source, and each rollup a `#@ preaggregate`
synthesizes; with `PUBLISHER_MATERIALIZE` on, also each `#(materialize)` source, `#(preaggregate)`
rollup and persisted view) carries a **serve verdict**: whether queries are answered from its table, and when
not, why and what to change. A materialized table that no query reads otherwise looks exactly like
a working one: the build succeeds, the manifest binds, and every query is quietly answered live.

`GET …/packages/{pkg}/status` reports the verdicts as `serveVerdicts`, recomputed from the build
plan and the binding on every read:

| `verdict` | Meaning |
| --- | --- |
| `serves` | Its table is bound, and queries can be answered from it. |
| `not_built` | Nothing is bound yet; queries are answered from its table once a build binds it. |
| `live` | No query is answered from it until something changes. `reason` says why and `fixes` says what to change. |

A `live` verdict's `reason`:

| `reason` | Why | Fix |
| --- | --- | --- |
| `refused` | The plan refused to build it (the build plan's `refusedSources` names why). | Remove what the refusal names. |
| `storage_mode` | `storage=` while `PERSIST_STORAGE_MODE` is not `on`. | Turn the mode on, or drop `storage=`. |
| `rollup_over_stored_base` | A rollup over a `storage=` base, left out of the plan. | Declare the rollup on a source the base is built from. |
| `unreachable` | No source on the package's published surface reads it: `index.malloy` hides it, and nothing it publishes is built on it, extends it or joins it. | Publish it, or a source built on it, or drop the annotation. |
| `row_level_gate` | Every query through it carries an `#(authorize)` or `#(access_filter)` gate, which the storage tier and rollups cannot keep yet. | Materialize without `storage=`, or move the gate onto an entry point over the table. |
| `shadowed` | Its storage table is bound, but every model's source of that name is another source. | Rename it. |
| `destination_missing` | `storage=` names a destination this environment does not configure, so no build can write its table. Not reported while `PERSIST_STORAGE_MODE` is `off`, which ignores `storage=`. | Configure the destination, name a configured one, or drop `storage=`. |
| `connection_cannot_hold_table` | Materialized in its own warehouse, on a connection whose type creates no tables. | Add `storage=` naming a configured destination, or drop the annotation. |

`unreachable` is also a publish warning, code `materialization-unreachable`: the table is built
for no query. So are the two deployment facts: `destination_missing` warns with code
`materialization-destination-missing`, and `connection_cannot_hold_table` with
`materialization-connection-cannot-hold-table`. The package still loads and the source serves live;
both are read from the environment on every package read, so configuring the destination clears the
warning with no reload. The other `live` reasons already carry warnings of their own.

**Preview before publishing.** `POST …/packages?dryRun=true` answers the same verdicts, plus the
`buildPlan`, for a package that is not published. For a source stored with `storage=` it also
compiles each named view against the model's served copy, with the source re-rooted on a table
declared from the plan's output columns, which needs no built table, and lists it under `views` as
`serves` or `live` with the compile error. Use it to learn what a change builds and serves without
a publish and a build per attempt.

A view that does not compile against the stored columns (it reaches through a join to a source
that is not materialized, or reads a field the table does not hold) is answered live on its own,
and the source's other views, and every other source's, keep serving from storage. Its `views`
entry is `live` with that view's compile error. When the served copy cannot re-root the source at
all (the table lacks a column the source needs, say), every view of the source is `live`, each
with the reason.

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

Whenever the server rewrites a manifest — any package PATCH, including a description-only one — it writes **both** homes with the same value, so a package edited through the API keeps loading on an older Publisher that reads only the root. An author who moved to the envelope alone will see the root key reappear after such an edit; the two never disagree. This dual write is deprecated too: a later release writes only `materialization.scope`, and then the root form is removed ([#1540](https://github.com/malloydata/publisher/issues/1540)). The same holds for `queryMetadata`, written both at the root (canonical) and as the deprecated `materialization.queryMetadata`.

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

A build ignores the package's [published surface](discovery-and-access.md). It builds every `#@ persist` source in every model, including one `index.malloy` does not export, so a hidden intermediate can be persisted and an exported source that reads it reads the built table.

Each run records a **trigger** in its metadata: `ON_DEMAND` (a manual/API build) or `SCHEDULER` (a scheduled fire). Only one materialization can be active per (environment, package) at a time — a second concurrent build is rejected with HTTP 409, and the scheduler coalesces (skips) rather than stacking a second build. A package with published versions builds per version, under the rules in [package-versions.md](package-versions.md#materializations): a run under `scope: "version"`, or one with `buildInstructions`, holds its own version's slot instead.

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

**An invalid declaration fails the package, it does not downgrade it.** The rules below are checked wherever a package is admitted — a publish or PATCH answers 400, and a package **load** fails outright, the same severity a model that does not compile has. So a broken declaration cannot sit in a log while the source quietly rebuilds in full forever: `watermark=` without `refresh="incremental"`, `merge_key=` without `watermark=`, a malformed key value, a watermark that names no materialized column (or names an aggregate, or a type with no ordering), a `calculate:` field, or an unsupported dialect. Every rejection is reported at once, so a model with two broken declarations takes one republish to fix. What is _legal but probably unintended_ stays a warning on the package instead: an unrecognized `#@ persist` key (code `persist-unknown-key`), and a keyless delta (code `persist-incremental-no-merge-key`).

Details that decide whether a run advances or rebuilds:

- **It is exempt from skip-if-unchanged.** A content address does not move when data does, so an incremental source is instructed on every run and its `covered_through` boundary — not its address — decides whether there is work to do.
- **`forceRefresh` never re-seeds.** It means one thing — build even though the content address is unchanged — and an incremental source is exempt from that carry-forward anyway, so the flag has nothing to say about how one is built. Ask for a full rebuild with `reseed` (`malloy-pub materialize --reseed`, or per source with `BuildInstruction.reseed`). Keeping them separate is what lets a schedule drive deltas at all, since the scheduler forces on every single fire.
- **Two sources that compile to identical SQL share everything.** The content address that keys the boundary is a hash of the connection and the canonical SQL — not the source name, not `name=`, not the model file — so a copy-pasted source body collapses onto one table and one boundary however you name it. If their declarations also differ, neither can ever advance: each refresh finds the other's lineage recorded and rebuilds. Publisher warns when this happens and names both sources (code `persist-incremental-shared-address`), since nothing in the model text shows it.
- **Anything unproven falls back to a full rebuild**, which is always correct and merely expensive: no recorded boundary, a boundary describing a different table or watermark, a table whose columns no longer match what the source computes, or `MERGE` asked for on Postgres 14 or older (it requires 15).
- **Postgres, BigQuery, and Snowflake only**, and that is the SOURCE's dialect — the engine that has to express the bounded range. Declaring it elsewhere is a rejection rather than a silent full refresh, so it never looks like it is advancing when it is not — which does mean a DuckDB source has to say `refresh="full"` to be served at all.
- **`storage=` is supported, and the delta is split across the two engines.** The source warehouse computes the bounded range (the predicate is pushed into its own query, so it never streams rows it will not keep) and the DML lands in the destination, which is where the table is. Everything an author declares means the same thing either way. Two differences worth knowing: a stored table holds exactly the source's public columns, so the rename/`except:` rebuild below does not arise from `getSQL()` projecting more than the schema describes — though changing which columns are public still rebuilds, since the stored table's shape no longer matches; and a CHAINED stored source — one reading another stored source's table — still rebuilds every refresh, reported under its own reason code, because its parent's delta can restate rows below the child's own frontier where no delta of the child's would revisit them.

### Driving it from a control plane

An orchestrated build works the same way, but the host, not the publisher, decides what gets built, so three things are the host's responsibility. Each of the first two fails _quietly_ if you get it wrong — a full rebuild every run, which succeeds and looks like a refresh.

- **Give an incremental source a STABLE physical table name.** The boundary is recorded against the table it was measured on, so a generational name (a fresh one per run) makes every run re-seed. That fallback is not a bug: the newly named table is empty, so applying a delta to it would drop everything the old one held. It is reported under its own reason code, `table_renamed`, to distinguish it from the source's definition having moved (`lineage_changed`) — which, where packages are immutable, means a refresh instructed through a different version than the one that established the boundary rather than an edited model.
- **Do not expect `forceRefresh` to do anything.** On an orchestrated build it means nothing at all: it exists to defeat skip-if-unchanged, which never runs when the host supplies the instructions. It does not re-seed — nothing does, in any mode.
- **Ask for a rebuild with `reseed`.** Per source on the instruction, or run-wide in the request body; the two are OR-ed. That is the escape hatch for a boundary or a table you no longer trust, and the per-source form means one source can rebuild while the rest advance by delta in the same run.

Each manifest entry reports the coverage its source reached on a `ledger` object, whose `coveredThrough` (with its `coveredThroughType`) makes progress something you read rather than infer: a value that advanced between runs is a delta that applied. A skipped source reports the boundary that stays in force, and a seeded one reports the boundary probed from the table it just wrote, so it means the same thing whatever the step did. The boundary is reported only there, alongside the watermark and source address it was measured under, because the value alone is not comparable across runs — see [Supplying the ledger yourself](#supplying-the-ledger-yourself), which is the same object you send back if you hold the ledger. Alongside it, `refresh` says what the run actually DID to the table — `delta` (advanced in place), `full` (rebuilt), or `none` (nothing to apply, the boundary stands) — because nothing else on the entry distinguishes them: every fallback above answers success and rebuilds, so a source quietly rebuilding on every run looks exactly like one advancing. It is present exactly when a source is refreshed incrementally, so its absence means only that this one is not.

### Retiring tables the plan no longer lists

A host that retires a table when its source leaves a version's build plan (a v1 rollup renamed by the `PUBLISHER_V1_SERVED_COPY` cutover, or renamed back by a rollback, moves its `sourceEntityId` with no change to the package) needs to know the plan it is reading is complete. A source absent from `sources` and `refusedSources` has left the plan only when the plan's `omittedSources` and `omittedFiles` are both empty.

`omittedSources` lists, sorted by `sourceID`, each source the plan would have held but left out because planning it **failed**: `{ "sourceID": "<name>@<model URL>", "reason": "<code>" }`. A source the plan decides not to build is refused or warned on instead and is never listed here, and an entry never names a sourceID that is in `sources`. Every entry is a real sourceID; there is no wildcard, because a source may be named anything, `*` included.

`omittedFiles` lists, sorted by `file`, each file whose sources the plan could not name, because the planner that would have named them failed first: `{ "file": "<file URL>", "reason": "<code>" }`. `file` is the URL the file's sourceIDs carry after their `@`: a generated companion's own URL (`<model>.preagg.malloy`, `<model>.preaggregate.malloy`, `<model>.persist-view.malloy`), or, for `#(materialize)`, the author's model file, whose sources the generated ones are presented as. Treat an omitted file as "retire nothing declared in it": any source whose sourceID ends `@<file>` may have been lost.

In both lists `reason` is a code, never the failure's message (which could quote SQL or a value; the message is logged at the Publisher with the model it was planned from), and the set of codes is open, so read an unknown one as a failure:

| `reason` | What failed |
| --- | --- |
| `unkeyable` | Malloy could not key the source's build (an unbound parameter, a given with no value) and no eligibility gate refused it first. |
| `eligibility_check_failed` | An eligibility gate failed with an error rather than a refusal. |
| `projection_failed` | The source's plan entry could not otherwise be computed. |
| `rollup_companion_failed` | The rollup companion (`<model>.preaggregate.malloy`: `#(preaggregate)`, and v1 `#@ preaggregate`) did not plan or compile. |
| `rollup_companion_blanking_failed` | That companion was not built because a `#@` line of the author's files could not be set aside. |
| `materialize_companion_failed` | The generated `#(materialize)` companion did not plan or compile. |
| `persisted_view_companion_failed` | The persisted-view companion did not plan or compile. |
| `v1_build_companion_failed` | The build copy of a model file without `##! experimental.persistence` could not be read or did not compile, so none of its `#@ persist` sources were planned. |
| `persist_annotation_unparseable` | None of the source's `#@` lines parse, so Malloy planned nothing for it. The package warning (`persist-unparseable`) names the line; this entry names the source. |

So: retire an anchor whose source is absent from `sources` and `refusedSources` only when no `omittedSources` entry names the source and no `omittedFiles` entry names the file it is declared in. A plan from a Publisher that predates the fields carries neither; read their absence as "completeness unknown", not as empty. Both fields are on every surface that returns a build plan, `[]` when nothing was lost: `GET …/packages/{pkg}/build-plan`, the dry run (`POST …/packages?dryRun=true`) and the deprecated `Package.buildPlan`. A plan that could not be derived at all (a model that does not compile, an import cycle) still answers 409 `BUILD_PLAN_UNAVAILABLE` rather than a partial plan.

**A mixed fleet mid-rollout** (some workers on a release where the v1 cutover is the only path, some on an earlier one with `PUBLISHER_V1_SERVED_COPY` off) is safe to bind one manifest to: each worker matches a manifest entry only against its own plan, by `sourceEntityId` (a current worker matches a `storage=` entry by source name and `sourceEntityId` together, never by name alone). An entry built on the other side matches nothing and is ignored, so its source answers live until this side builds its own table: it costs a rebuild, never an answer from the other side's table.

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

- **Sweeps already-loaded packages, and every published version in service.** It never forces a package's load; a not-yet-loaded package simply isn't scheduled until something else loads it. A published version's schedule is read from its `publisher.json` without loading it, and the version is loaded only when its cron comes due; archiving a version takes it out of the sweep. See [package-versions.md](package-versions.md#materializations).
- **Arms to the next occurrence.** On first sight it computes the next cron time (strictly future), so a freshly-scheduled package does **not** fire on the arming tick.
- **Recovers one missed occurrence across a restart.** On first arm after a (re)start it re-anchors from the newest recorded `SCHEDULER` run: if an occurrence came due while the process was down, it fires exactly one catch-up and then jumps forward, rather than skipping it. (A schedule set while the scheduler was _disabled_ has no prior run to anchor from, so it is not caught up on first enable.)
- **Skips what it must not fire:** a control-plane-driven package (one with a `manifestLocation`), a package whose policy is invalid, or one with an unparseable cron.
- **Caps fires per tick** (`…_MAX_FIRES_PER_TICK`) so a burst of due packages doesn't stampede; a capped package fires on a later tick.
- **Coalesces** when a build is already active for the package (the in-flight build covers the occurrence).

> **Never set `PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER` on a control-plane-driven (orchestrated) worker.** A package serving live under a control plane has no `manifestLocation`, so the per-package skip does not cover it — the flag being off is the primary guard against the standalone scheduler double-driving refresh the control plane already owns.

## Manage it with the CLI

```bash
# View / set / clear a package's schedule (set also sets scope: version). set and clear edit the
# package, which a published version refuses with 409 PACKAGE_IS_VERSIONED: set the schedule in
# its publisher.json and publish a new version instead.
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

> **`--drop-tables` drops the physical tables in that run's manifest, except any that a still-ready run also names.** Auto-run assigns stable table names and carries unchanged sources forward, so an old run's manifest names tables a newer manifest still serves, and those survive the delete. Deleting a run also re-derives the package's serve bindings from the latest remaining ready run (or clears them, so queries compute live), so a delete does not break queries — it can leave a table behind. To remove a persisted source, delete its annotation, `materialize --wait`, then delete the old runs that name its table; a table no ready run names is dropped.

## Which table answered a query: the path report

Every query result carries `path`, the path report. It says which materialized table answered the query, or that it ran live, and why every other table that could have answered did not. Unlike `servedFrom`, which reports the `storage=` tier only, it covers colocated `#@ persist` tables and pre-aggregation rollups too.

```json
"path": {
  "query": { "sourceName": "daily", "groupedBy": ["region"], "filteredBy": [], "measures": ["total"] },
  "servedBy": { "kind": "materialized", "tableId": "9f1c…", "dataAsOf": "2026-11-01T00:00:00.000Z" },
  "considered": [
    { "tableId": "4ab0…", "kind": "rollup", "outcome": "skipped", "reason": "COUNT_SEMANTICS", "detail": "aggregate count() is computed over the rollup's rows, not from a stored partial" },
    { "tableId": "9f1c…", "kind": "materialized", "outcome": "used" }
  ],
  "notConsidered": [
    { "tableId": "77de…", "kind": "materialized", "sourceName": "by_region", "relationship": "sibling", "built": true }
  ]
}
```

- `tableId` is the table's `sourceEntityId`, the content address the build plan and manifest key on. Sources that build the same SQL share one table, so they share one id.
- `considered` lists the tables on the queried source's chain (the source, what it extends or renames, and the source its query reads from) and the rollups of a source on it, in the order they are tried: `storage=` tables, then colocated rollups, then colocated tables nearest first. A table that was not tried because an earlier one answered is left out.
- `reason` is one of `NOT_BUILT` (not bound to this package), `STALE` (past its freshness window), `CONTRACT` (the entry point has a row-level gate, which stored data cannot keep yet), `MISSING_FIELD` (the query does not compile against the stored form, or reaches a field a rollup does not carry), `COUNT_SEMANTICS` (a rollup cannot reproduce an aggregate the query computes), `GRAIN_TOO_COARSE` (a rollup's grain does not hold the query's groups), `RUN_FAILED` (the stored read failed and the freshness fallback answered live) or `WARNED` (the table answers no query at all: its serve verdict is `live`; see below). The list is closed, and codes may be added.
- A `WARNED` skip carries `code`, the serve verdict's reason the table will not serve: `storage_mode` (a `storage=` table while `PERSIST_STORAGE_MODE` is `off` or `write-only`; the query reads the source's own warehouse, where its colocated table may answer), `unreachable` (nothing the package publishes reaches it), `row_level_gate` (every published entry point over it carries a row-level gate, which stored data cannot keep yet; a rollup only, for a stored table, while `PUBLISHER_MATERIALIZE_GATED_SOURCES` is on), `refused` (the package refuses to serve it) or `shadowed` (its storage table is bound to another source of the same name). Where the verdict says a table will never serve, it replaces what one query alone showed, but only where the router never tried the table: a source the bind refuses reads `WARNED` `refused`, not `NOT_BUILT`, and a table every published entry point gates reads `WARNED` `row_level_gate`, not `CONTRACT`. A skip the router reached by trying the table (`MISSING_FIELD`, `GRAIN_TOO_COARSE`, `COUNT_SEMANTICS`) keeps its own reason, and `STALE` and `RUN_FAILED` stay as they are, since only the query knows them. A `storage=` table and its colocated copy share a `tableId` but each reads its own verdict. The set of codes is open.
- `warnings` has one entry per such table, with its `code`, `tableId`, `sourceName` and a sentence for a person. A table whose source the caller cannot list gets no warning, though its skip keeps its code. The package's own warnings and `GET …/packages/{pkg}/status` carry the full message and the fixes.
- `detail` is in the caller's own terms: the field path or aggregate in the query that a rollup cannot reproduce. A compile error against the stored form or a run-time storage error carries no `detail`, since its message can name storage paths and generated sources; it is in the server log.
- `notConsidered` names up to ten tables near the queried source that the query cannot use: on a source that extends it (`below`) or that shares an ancestor with it (`sibling`). Querying that source instead, or moving an annotation, would let one answer. A source the caller cannot list (hidden by the package's `index.malloy` or the file's exports) is never named, unless the caller may see hidden sources.

The report is assembled after the query runs and never changes which table answers. It is absent only if it could not be assembled.

### The served copy (in development, `PUBLISHER_MATERIALIZE`)

With `PUBLISHER_MATERIALIZE=true`, a query on a built colocated `#(materialize)` source, or (with `PERSIST_STORAGE_MODE=on` as well) a built `storage=` source, may be answered from the model's **served copy**: your own files, held in memory, with the source's definition replaced by its stored table under the same name. Its trailing `extend { … }` blocks, its annotations and every other line are your text, so an extension, a join to it, a view, a named query or a source in a file that imports it reads the stored table with no change to how it is written. The served copy compiles when the package binds its stored tables; a query it cannot answer is answered live. When it answers, the path report's `servedBy` carries `via: "served_copy"`.

- **`#(materialize)`**: the table is the one the source's generated build source builds, which stores the source's rows with its `where:` applied, so a source of any shape re-roots. A computed dimension is stored under its own name, so the served copy drops its definition and the stored column is the field; the dimension's annotations (`#(doc)`, `#(classification)` and the rest) are written on that field, so they read back as before. A timeframe truncation (`order_date.month`) keeps its timeframe: its definition is rewritten in place to truncate the stored column again, so results, charts and filters see a `month` field as they do live. A dimension the source inherits from its base is simply its stored column. A query that sets its own `timezone:` over a table that stores a time-zone-dependent dimension (one declared on a base in another file; see below) is answered live, since the build computed it in the default timezone. A source whose base declares a measure, view or join it inherits is left as written (its table does not carry them) and warned about.
- **`#@ persist storage=`**: only a query-shaped source (`x -> { … }`) is re-rooted, since its table is exactly its query's output; a timeframe column in that output keeps its timeframe too. A source declared by refinement alone stores its root's rows and is left as written; it still reads a re-rooted base by name.
- A reader that opts out with `#@ -persist`, or any source built on one, is never answered from the served copy.
- **Table names.** Without `name=`, a `#(materialize) storage=` source's table takes the source's own name, with its package's suffix in Publisher's own builds ([Whose table it is](#whose-table-it-is)). If another source of the package writes a table of that name in the same destination, the `#(materialize)` source is not built (a `materialize-not-built` warning that says to add `name=`) and serves live. A source that inherits `#@ persist` from the source it extends or renames is already that source's table, so `#(materialize)` on it is not built either, with a warning naming the source it inherits from.
- **Colocated.** A `#(materialize)` source without `storage=` is built into its own warehouse, and needs a `name=` there (without one its table would take the source's own name, which could be the very table it reads; a `materialize-not-built` warning says so, and it serves live). Its build writes that table only where nothing else stands: a `name=` that holds a table no run of the package wrote (one of your own tables, or the very one the source reads), or a view Publisher did not make, fails that source's build before anything is written, naming what holds it, and the source serves live; a table the package's own run wrote there is rebuilt. The served copy reads it on the source's own connection, exactly as it reads a `storage=` table in its destination, so everything on this list applies to it: stored computed dimensions and timeframes, `inline=` joins and pair tables, private and internal fields, `where:` terms re-applied at read, extension tables, gates. It needs no storage tier: `PERSIST_STORAGE_MODE` decides only `storage=` tables. Since the table and the source share a connection, a join to another source on that connection stays one query, so a source reached only through a join, or joined beside a warehouse table, reads its table too. A column whose type the served copy cannot declare as stored (a `timestamptz`, a `json` or driver-specific type, or a nest outside DuckDB, where it is stored as JSON) leaves that source served live. An answer that read only colocated tables carries no `servedFrom` (it read the source's own warehouse, as a colocated `#@ persist` answer does); its path report names the table, with `via: "served_copy"`. A v1 `#@ persist` without `storage=` is built and served in its own warehouse through Malloy's build manifest, as before. A `#(materialize)` source's serve verdict follows the served copy: `serves` where some model's served copy reads its table.
- **Private and internal fields.** A `#(materialize)` source's own `private` and `internal` dimensions, and the `internal` ones it inherits, are stored under their own names: the build reads a copy of the source's file with its own access modifiers lifted (a query cannot select a hidden field), and the served copy keeps your modifiers, so a caller can no more select a hidden column from the stored table than live. A hidden dimension your source's own `extend` defines keeps that definition and is computed at read from the stored columns it reads; one an `include { private: … }` on the source's head hides, or one it inherits, is the stored column, hidden again as you wrote it. A hidden timeframe field (`internal dimension: order_month is ordered_at.month`) is stored too, and keeps its timeframe in the served copy exactly as a public one does, redefined under your modifier; the stored column it reads is `private` there, so no caller reaches it by any path. A field private on a source it extends, which your source cannot read, and an `except:`-ed field are never stored. A served source whose fields are not exactly as hidden as yours is not answered from the copy (`served-copy-not-served`, `fields_not_carried`). A rolling window over a hidden dimension your source's own `extend` defines (`internal dimension: t2 is ts; where: t2 >= now - 60 days`) is applied at read by that `extend`, as you wrote it; a served source whose compiled filters lack it is not answered from the copy (`read_terms_not_kept`). A pair table beside a column an `include` keeps hidden under its own name is served too; an author `rename:` of a stored column behind that `include` is not (Malloy refuses a `rename:` right after an `include`), and that source answers live (`definition_not_parsed`). With the gated switch on, a gated source's hidden columns are stored in the destination too; see below.
- **Rolling windows.** A `#(materialize)` source whose `where:` keeps the last stretch of time, one comparison of a stored timestamp with `now` (`ts >= now - 30 days`, optionally `now` alone or offset by a whole number of one unit), builds with the window as of the build and is re-applied at every read at the read's own clock, a window inherited from the base included, so a read between refreshes trims to its own cut-off. An incremental refresh appends above the watermark and then deletes the rows below the new cut-off, less a day's margin, in the same transaction. A window that keeps rows *older* than a moving cut-off (`ts < now - 30 days`) is left out of the build, so the table holds every row there is when it is built, and is re-applied at every read, own or inherited from a base in the same file; with no refresh interval to bound it, that is the widest set any later read can need, and it needs no scheduler. A two-sided window, `ts >= now - 60 days and ts < now - 30 days` (or the same as two `where:` terms, or the range `ts ? now - 60 days to now - 30 days`), builds its lower bound and leaves its upper bound out; an incremental refresh deletes below the lower bound only, and an upper bound alone deletes nothing. Any other `where:` that reads `now` (an `or`, a bound `and`ed with another condition), directly or through a dimension or a join that reads it (`where: is_recent`), is not a shape Publisher can store, nor is one `where:` term that reads both `now` and a given, nor an upper bound written in another file than the source's model, which the build cannot leave out, nor one inherited from a base that a join named in `inline=` also reads, whose stored columns would hold the rows it leaves out: each leaves the source unbuilt, and it serves live. A query that sets a `timezone:` is answered live from a source with a window, since Malloy takes the offset in that zone's wall time and across a daylight-saving change the read needs up to an hour the table does not hold. A dimension whose value depends on the current time, directly or through another dimension or a join, is never stored; the served copy computes it at read.
- **Dimensions that depend on the caller.** A `#(materialize)` dimension whose value depends on what the caller sets is not stored: one that reads a given (directly or through another dimension), the current time, or the query's time zone (`hour(ts)`, `ts.month`, `ts::date`; a `date`'s own truncations are civil and do not). The table stores its inputs, and the served copy keeps the dimension's definition and computes it at read, so each caller gets its own value and no table holds one caller's. Publisher finds these by compiling small probe queries against your model (never running them) beside its own reading of the compiled model; either one marking a dimension keeps it out. Where the dimension is declared matters, because the served copy computes it from a definition it keeps: the source's own, or one on a base the source extends or renames in the same file, whose trailing `extend { … }` blocks the served copy then keeps ahead of the source's own (the next item). A given-reading dimension declared anywhere else (a source in another file, a parenthesised head, or a base whose fields your source renames, redefines, drops with `except:` or hides) refuses the source (a `materialize-not-built` warning naming the dimension; it serves live), and a time-zone-dependent one declared there is stored as built, so a `timezone:` query over that table is answered live. A source whose time-zone-dependent dimensions are all ones the served copy keeps answers a `timezone:` query from its table, unless its rows depend on the zone: a `where:` with a timestamp literal (`where: ts > @2026-03-08 09:00`), its own or one a base it is built on writes, filters different rows in every zone, and the table holds the rows of the build's, so a `timezone:` query over it is answered live. An `inline=` join through which a given is read is refused the same way, since its stored columns would hold one caller's values.
- **A base scoped by a given.** The common governed shape is a base that keeps one tenant's rows (`source: tenant_orders is orders extend { where: org_id = $ORG_ID }`) with `#(materialize)` extensions over it. A given-reading `where:` the source inherits from a base it extends or renames in the same file is left out of the build, as one of its own is: the build reads a copy of the file with that term written `true`, so the table holds every caller's rows. The served copy keeps that base's trailing `extend { … }` blocks (every base's between it and your source, root first) over the stored table, so the term is applied at read with each caller's value exactly as you wrote it, and any measure, view or join the base declares is your source's too; a stored computed dimension's definition is dropped from them, as from your source's own. A served source whose compiled filters lost a given-reading term is not answered from the copy (`served-copy-not-served`, `read_terms_not_kept`). The base's term is left out only when the served copy can keep it, otherwise the source is not built (a `materialize-not-built` warning that says why) and serves live: the base must be declared in the same file, named as the whole base of the declaration below it (`<name> is <base> extend { … }`, or through a bare rename of it such as `source: b2 is base`, not in a parenthesised head), and the term written in its trailing `extend`; your source must inherit the base's fields unchanged (no `rename:`, redefinition, `except:` or access change past it), so the term reads the column it read on the base; and no join of your source may read that base, since the build copy takes the term out of every reader of the base in it. This holds whatever the base reads, a table (`duckdb.table('orders')`), a SQL source or a query, and with your source or a source between it and the base adding its own `where:` or join. A term that reads its given through a dimension (`where: is_mine`) is not one Publisher can leave out: its build would read the given, so the eligibility gate refuses it (`given_in_persisted_query`) and it serves live.
- **Rolling windows.** A `#(materialize)` source whose `where:` keeps the last stretch of time, one comparison of a stored timestamp with `now` (`ts >= now - 30 days`, optionally `now` alone or offset by a whole number of one unit), builds with the window as of the build and is re-applied at every read at the read's own clock, a window inherited from the base included, so a read between refreshes trims to its own cut-off. An incremental refresh appends above the watermark and then deletes the rows below the new cut-off, less a day's margin, in the same transaction. A window that keeps rows *older* than a moving cut-off (`ts < now - 30 days`) would have to be built as of the end of the refresh interval, which a build on demand does not know, and any other `where:` that reads `now`, directly or through a dimension or a join that reads it (`where: is_recent`), is not a shape Publisher can store, nor is one `where:` term that reads both `now` and a given: each leaves the source unbuilt, and it serves live. A query that sets a `timezone:` is answered live from a source with a window, since Malloy takes the offset in that zone's wall time and across a daylight-saving change the read needs up to an hour the table does not hold. A dimension whose value depends on the current time, directly or through another dimension or a join, is never stored; the served copy computes it at read.
- **Extension tables.** An extension that declares its own `#(materialize)` has a table of its own (one that declares none reads its parent's). When the two are on one tier (both colocated, or both in one `storage=` destination), carry no incremental key and are declared in the same file, its build reads its parent's table rather than recomputing the chain, and runs after the parent's (a colocated one reads it in the warehouse). A `storage=` extension's build runs in the destination over the parent's stored table and reads nothing from the warehouse, so a strict build that refuses the warehouse (`warehouseReach: refuse`) builds it, and its entry says `upstreamReuse: reused`; under `strictUpstreams`, a parent with no table in that destination refuses it. A join the parent inlines (`inline=`) is read in that build from the parent's stored `j__<field>` columns, as the served copy reads it, so an extension that reads `j.<field>` still builds over the parent's table. One whose build over the parent does not compile (a field it reads that the parent's table does not carry, or, for `storage=`, a join to a source the destination does not hold, such as one the parent does not inline) builds over the chain from the warehouse, with the same rows. That fallback is per extension: every other extension in the model still builds over its parent's table. A source elsewhere in the file that reads the parent and would not compile over the parent's table (a sibling extension that falls back itself, say) is left out of the extension's build as an empty source, so it cannot cost the extension its parent's table; if the extension reads that source, its build does not compile and it too builds over the chain from the warehouse. An extension of a source that inlines a join_many always builds over the chain, since the parent's pair table holds each parent once per child. Its served copy keeps the `extend { … }` blocks of every source it is built on in its own file, ahead of its own, so a measure, join or view a parent declares is still the extension's; a stored computed dimension's definition is dropped from them as from its own. A `rename:` in those blocks reads its stored column by the field's new name; one with an access modifier (`internal rename:`) keeps the source from being served from its table. A private or internal field stays exactly as hidden on the extension as on the source you wrote, whichever source declared or hid it. A query on the extension reads its own table, and the path report lists its table and its parents' on the chain, nearest first.
- **Joins are references by default.** A `#(materialize)` source's table holds its own rows only; its joins stay as you wrote them, and read the joined source (from its own table, when that source is materialized) at query time. A dimension whose value reads through a join is computed at read, not stored.
- **`inline=["j"]`** on a join_one stores the dimensions that read through `j` (one hop) and the joined source's own dimensions as `j__<field>` columns, for tools that read the table outside Malloy. The served copy answers the join from those columns, with no join at read: it replaces the join with a record of the stored fields under the join's name, so `j.<field>` reads the table, an unmatched row reads nulls as the live LEFT JOIN does, and on the storage tier a query through it no longer crosses connections. What the columns cannot answer is answered live: a measure, view or join of the joined source (`MISSING_FIELD` in the path report), an aggregate at the join's own grain such as `j.x.sum()`, `j.count()` or a measure that reads one, in `having:` or filtered too (`COUNT_SEMANTICS`; a record would compute it once per row of the source, where the join computes it once per joined row), a `j.*` selection, and a query that sets a `timezone:`. Row-grain aggregates such as `sum(j.x)` and `count(j.k)` read the table. Each joined field keeps its annotations and classification on the stored copy, so a classified joined source stays served, but a query that returns an annotated field of `j` as a column (`group_by: j.name`) is answered live (`MISSING_FIELD`), so the result carries the field's `#(doc)`, `#(classification)` and render tags; filtering on it, aggregating it, or returning it under another name (`n is j.name`) reads the table. If the source's own text reads the join past what the table stores (a join of the joined source), the served copy keeps the join as written and raises a `served-copy-not-served` warning naming the join and why. A join you hide (`internal join_one:` or `private join_one:`) is inlined too: the record keeps the join's modifier and the stored columns behind it are `private`, so a caller reads `j.<field>` exactly where live lets it (an `internal` join through the caller's own `extend`, a `private` one never), while your own dimensions, measures, views and gates over it read the table. A hidden join_many is inlined the same way (see the pair table below). The join must be keyed by one equality on a column of a declared source on the same connection, and a build first checks that key is unique in the joined source: a key held twice fails the build, naming the join, rather than storing a row once per match. When an auto-run's build reads the joined source's `#@ persist` table, the check reads that table too. An auto-run skips the check only when its answer cannot have changed: the joined source is itself a `#@ persist` table that the build reads, that table was not written since a check over it passed in the run before, every other run of the package had finished before that run began, and the check's SQL is the same. Otherwise it runs, on every build: a joined warehouse table's rows change without its address changing, and so do an incremental table's. A control plane's builds (with `buildInstructions`) always run it. Each run records the checks in its `metadata.inlineUniquenessChecks`: the passes the next run may rely on, and whether each check ran or was skipped. Deleting a run's record forgets the passes every other run of the package records, so the next run checks again: the record is part of what shows the table was not written since. `inline=` on a join_cross is refused until a later release. A refused `inline=` (not a join of the source, a join_cross, a join keyed through another join, a join_one with no single key, a join defined in place, one on another connection, a join through which a given is read, or a `j__<field>` column that clashes with one of the source's own) is a `materialize-not-built` package warning naming why, and the source serves live. A join through which a given is read is one whose joined source's `where:`, `on`, fields or own joins read a given (`where: region = $REGION`): its stored columns would hold one caller's rows, so the source is refused ("inline= names `j`, through which a given is read") and answers live for each caller, while the model's other sources build and serve; reference the join instead of inlining it. More generally, a source whose generated build text does not compile is refused on its own (`materialize-not-built`, "its generated build did not compile", a Publisher bug worth reporting) and the rest of the model builds.
- **`inline=["j"]` on a join_many** stores a **pair table**: one row per row of live's LEFT JOIN through the join (each `j__<field>` column beside the source's own), so a parent with no child keeps one row with null child columns. One flag is written at build: `__parent_row` is true on exactly one row per parent. A tool reading the table outside Malloy counts parents with `WHERE __parent_row` and pairs with `count(*)`. The served copy reads the table as the source itself, with no join at read: `j` is a record of the child columns (`j.miles` reads the column on the same row), each measure over the source's own fields aggregates only the `__parent_row` rows, and an aggregate through the join reads every row, the null-extended row of a parent with no child included. Since Malloy 0.0.438 ([malloydata/malloy#3208](https://github.com/malloydata/malloy/pull/3208)) live's `count()`, `sum` and `avg` through the join skip that row, so a sum or average of a stored column of `j` (null there) is served, and a `count()` through the join (`j.count()`, or a measure or view that counts), or a sum or average of a constant or computed value, is answered live (`COUNT_SEMANTICS`; materialization v2 known limitation 40); a `min`, `max` or distinct count is served, as live reads that row too. A many-to-many join_many is served the same way. Each query is checked before the table answers it, and one the flat table cannot reproduce exactly is answered live (path report reason `COUNT_SEMANTICS`): an aggregate over the source's own fields written in the query rather than declared as a measure, a parent measure grouped or filtered by a field of `j`, an aggregate that reads both sides at once or goes through another join, a window or `all()`/`exclude()`, a `select:`, and a join to the source from another source. The joined source's own measures and views are not on the record, so a query that reads one (`j.seg_miles`) is answered live (`MISSING_FIELD`), as is a query that returns a classified or annotated field of `j` as a column, exactly as for an inlined join_one (a record's fields carry no annotations; each child column keeps its field's annotations and class, an inferred class included, by the same rule as a join_one's), and a source whose own text reads one through the join is not served from the table. A dimension of the source that reads through the join_many is computed at read. A query that reads no field of `j` (no field, aggregate, filter, group-by or nest through it) is not answered from the pair table, which would scan every pair row to count each parent once (about 2 to 5.5 times live up to five children a parent): it goes to the next table that can answer it, or live, and the path report shows the pair table skipped with `COUNT_SEMANTICS`. A row-level gate that reads `j` counts as a filter on `j`, so parent measures under it are answered live. Child and mixed queries read the table faster than live (about 0.2 to 0.4 times, up to five children a parent). A join_many you hide (`internal join_many:` or `private join_many:`) is stored the same way: the record keeps the join's modifier and every stored column behind it is `private`, so a caller reads `j.<field>` exactly where live lets it (an `internal` join through the caller's own `extend`, a `private` one never) and the stored columns never, while your own dimensions, measures, views and gates over it read the table; a caller's own `extend` or `include` over a pair source is answered live, as for a public join_many. The pair table needs a declared primary key on every side that is stored, and a joined source with no joins of its own other than the path below it; a path through a join the joined source hides is refused, since nothing reads through it. Anything else is a `materialize-not-built` warning.
- **A path of join_manys**, `inline=["items.options"]`, stores one pair table with one row per row of live's LEFT JOIN through every join of the path (`items__options__<field>` beside `items__<field>` and the source's own), and one more flag per level above the last: `__child_row` is true on one row per order and item (an order with no item included). `items.options.kind` reads the column on the same row. An aggregate at the last join (`items.options.extra.sum()`, a measure over it) is answered from the table, grouped or filtered by any level, under the same rule as one hop: a `count()` there (`items.options.count()`), or at a flagged middle level (`items.count()`), is answered live, since the flag also marks an order with no item. A measure over `items` (`items.price.sum()` declared on the source) is answered from the table when the query reads nothing of `items.options`. Once it does, live aggregates each item once by its key (and an order with no item as none). The table still answers a sum or an average of a stored column of `items` (`items.price.sum()`, `items.price.avg()`) beside `items.options`, when each item has exactly one order: every join down to `items` keyed on the level above's primary key (`join_many: items on items.order_id = id`, with `primary_key: id`), which the build checks unique and never null, so `__child_row` marks each item once whichever order groups it. A count (`items.count()`, which counts an order with no item as one under the flag and as none live), an aggregate of a computed field of `items` (not null on an order with no item), and a level whose join is keyed on anything else (a dimension, or another source's field, where an item may meet several orders) are answered live beside the deeper level. As at one hop, an aggregate written in the query above the last join, or a measure grouped or filtered by a level below its own, is answered live. Naming `"items"` too adds nothing: the path's table answers it. A path through a join_one (`planes.maintenance`) is refused: live counts each maintenance row once however many flights share the plane, which no stored row can reproduce.
- **More than one join_many**, `inline=["items", "payments"]`, stores one table in blocks, one per join_many: each block is that join's own pair table, marked by `__pair_lane` (0, 1, …), with the other blocks' columns null on its rows, so a tool reading the table outside Malloy filters on `__pair_lane` first. Each query is answered from the one block it reads; a query that reads both (`items.count()` beside `payments.count()`, or a filter on one with measures of the other) is answered live, since live counts each over their cross product. A path's table and a table of several blocks refresh incrementally as a one-join table does (below), replacing whole parents.
- Every pair table is built only when the source's primary key, and on a path the key of every level between it and the last join, are unique and never null: each build checks them first (a full build and an incremental refresh alike), as it checks an inlined join_one's key, and a repeated or null key refuses the build (a level's row flag marks one row per key, where live counts each row), so the source answers live. The check is one `GROUP BY` of the source on its primary key, and of each middle level on its key, per build. The source's own check reads the rows the build stores: a given's term or a rolling window's upper bound that the build leaves out is left out of the check too, so a key held by two tenants, or repeated past the window's cut-off, refuses the build. It reads the source itself, so it is never skipped as an unchanged joined table's check can be. Another block's number columns are stored as nulls of a type the union does not widen, so an integer or decimal column keeps its type.
- A row-level gate that reads a join_many the pair table does not inline, at any step of its path (one reached through a join_one included), or another block's join, is answered live: grafted over the pair rows, it would repeat each row once per match. A query that filters, groups or computes a filtered measure through such a join (`where: hub.gates.gate = 'A1'`, `group_by: hub.gates.gate`, or a dimension that reads it) is answered live for the same reason (`COUNT_SEMANTICS`), on a one-hop table as on a path or a table of blocks.
- **An incremental pair table** names a watermark on both sides of the join: `#(materialize) storage="lake" inline=["segments"] refresh="incremental" watermark=["updated_at", "segments.loaded_at"]`. The table stores one more column, `__pair_watermark`: per parent, the greatest of those fields over the parent and every child it has, the same value on every row of one parent. Each refresh replaces every row of each parent whose `__pair_watermark` is at or past where the last refresh reached. The range is open above, unlike any other incremental source's: one child stamped later than the refresh's clock holds its parent's watermark there, so a range that stopped at the clock would leave that parent's own changes behind until the clock caught up; instead the parent is read again on each refresh until then, which replacing a parent whole makes harmless. The refresh reads those parents' rows from the source once, deletes every stored row with their primary keys, writes the new rows, applies the rolling window's delete if there is one, and checks that every parent in the table has exactly one `__parent_row`, all in one DuckDB transaction in the destination. So a child added to an old parent (a late-arriving child), a childless parent gaining its first child, a child restated in place, a restated parent, and a new parent with or without children all reach the table, and the table after each refresh holds what a full rebuild would. What it cannot see is a change that moves no watermark field: a child deleted, or moved to another parent, reaches the table only when its old parent's own watermark field moves too, or at the next full rebuild (`--reseed`); the same rule as any incremental source, whose rows must only arrive or move their watermark forward. Likewise a parent whose watermark fields are null on its own row and on every child's has a null `__pair_watermark`, which no refresh reads: it is stored by a full build only. If the check fails, the delta is rolled back and the table is rebuilt in full. The source warehouse still computes the whole pair table on each refresh (the watermark is a window over each parent's rows, so the range cannot be pushed into the scan): a refresh saves what crosses to the destination and what it writes there, not the warehouse's scan. As for any incremental source, the origin must be Postgres, Snowflake or BigQuery. The refresh is rebuilt in full instead, with a `materialize-pair-full-refresh` warning naming why, and the table still built and served, when the watermark names only the source's own fields (a child added to a stored parent would never be seen), only the joined source's (a new childless parent would not be), a field that is not stored, fields of different types, when `merge_key=` names anything but the source's primary key (the unit is the parent), or when the rolling window reads a field of the join. A list `watermark=` on any other source is a `materialize-not-built` warning.
- **An incremental path or table of several blocks** follows the same rule, with a watermark field at every level: `inline=["items.options"] refresh="incremental" watermark=["updated_at", "items.loaded_at", "items.options.loaded_at"]`, and for blocks a field of every level of every block (`inline=["items.options", "payments"]` adds `"payments.paid_at"`), since a row added at a level with nothing below it moves its parent only through a field of its own level; a level with no field rebuilds in full with `materialize-pair-full-refresh`. Each level's row flag is one row per distinct keys from the parent down, so replacing whole parents keeps it exact, and the refresh checks each one as it checks `__parent_row`. A table of several blocks stores one more column, `__pair_watermark_block`, each block's own greatest per parent; `__pair_watermark` is the greatest across blocks, the same on every row of one parent in every block, so a refresh replaces a parent in all of them at once, and checks every flag within each block and that each parent still has rows in every block. A failed check rolls back and rebuilds in full, as at one join.
- **A join the destination cannot reach.** A stored source keeps its joins, but a joined source that is not stored in the same destination reads another connection, which Malloy cannot join. The served copy leaves that join out (with what reads it), so the queries that do not use it still read the table; a query that does is answered live, and its path report skips the table with `CROSS_CONNECTION`. The package warning names the join and the three fixes: inline it, materialize the joined source into the same destination, or keep the source colocated.
- **Sources from different connections, joined.** A file whose join crosses two origin connections does not compile live. With the switch on it loads with that join left out (and what reads it, each named in a `served-copy-not-served` package warning), its sources build from that, and once both sides are stored in one destination the served copy answers the join there. Until both sides are built, only the queries that reach the join are refused. The join may sit in an imported file. Only what follows from the join is left out: a file with any other compile error fails to load as before. Once both sides are built, the served copy reaches fields the loaded model does not, so the load's `#(carries)` leak and classification-coverage checks run on the served copy's compile too: a source they flag there is not answered from the stored tables (path report `WARNED`), and a `served-copy-not-served` package warning names each field and why (an unclassified one also as `classification-coverage`). Discovery does not list the left-out join or what reads it, and `compile_model` at `file` or `append` scope still reports the join as an error. The per-request `#(carries)` ceiling reads the compile that answers: a query the served copy answers is checked on the served copy's compile (and on live too, when live compiles), so a clean query through the join is served once both sides are built, and one reading a class past the ceiling is refused, served or live ([classification](classification.md#the-ceiling-is-checked-per-query)).

**Gated sources (`PUBLISHER_MATERIALIZE_GATED_SOURCES`, off by default).** Without this switch, a query whose entry point carries a row-level gate (`#(access_filter)`, or an `#(authorize)` that filters rows) is answered from a stored table only when that table is colocated. The served copy keeps the gate's annotations: with this switch on as well, such a query may be answered from the served copy, the gate grafted and proven on the served compile exactly as on live, so the caller gets live's rows read from the stored table; a v1 `#@ preaggregate` rollup stays blocked; a colocated `#(preaggregate)` rollup is read past the gate the same way when every gate reads its grain dimensions ([preaggregation.md](preaggregation.md#preaggregate-materialization-v2-behind-publisher_materialize)); and a query whose own join reaches a gated source stays blocked too (that gate is never grafted on a routed compile), which is answered live. The switch also decides what is stored: on, a `#(materialize)` source, `storage=` or colocated, whose gate is its own and compiles to a row filter or a lock (the bar a colocated `#@ persist` meets) is built, bound and served; off, a gated colocated `#(materialize)` is refused too (its table is read only through the served copy, so it would be built and never read); a gated v1 `#@ persist storage=` source (switch on or off: it stays live), a gate reached only through a join, and every gated source with the switch off are refused as before (`persist-source-refused`, reason `authorize`), so the managed tier never holds a gated source's columns unless the switch is on. The verdict for a stored table reached only through gated entry points reads `serves` rather than `WARNED` `row_level_gate` while the switch is on. Since a `#(materialize)` source stores its private and internal fields, turning the switch on puts a gated source's hidden columns (its `org_id`, say) in the destination as well; off, a gated source is never built. A gate is grafted as a caller-level `extend`, which reads an `internal` field and never a `private` one, so a gate over a private column denies every caller, live and served alike: give the gate's column `internal`. `internal` keeps it out of a plain `select:`, but a caller's own `extend` or `include { public: org_id }` still reads it, live and served alike, only in the rows the gate admits; hide a value no caller may see behind `private`, in a field the gate does not read. A served-copy answer to a gated query is denied (`403`) if governing it on the served copy finds no gate there, and its access record's `path.servedBy` carries `via: "served_copy"`. Publisher does not authenticate callers: the gate holds only behind the gateway that sets the givens from a verified identity ([operating model](security-posture.md#operating-model-a-microservice-behind-your-gateway)). It is off until a security review of storing gated sources' columns in the managed tier.

A served copy that does not compile, or a source it cannot re-root, is a `served-copy-not-served` package warning, and those sources serve as they did without it. The served copy is compiled once per model, so a failure is traced to the source whose re-root it is in, and that source alone steps back (`reroot_not_compiled`: it answers live, after first keeping its inlined join_ones as written, or, for an extension table, its own blocks only); the model's other sources keep their tables. Only a failure outside every re-root costs the whole copy. A model that joins sources on different connections compiles once both sides are built into one destination, and reports that wait under the same code until then. Off (the default), nothing here runs.

To move a v1 `#@ persist` source to `#(materialize)`, and for what each v1 form becomes, see [materialization-migration.md](materialization-migration.md).

### The `name=` view (in development, `PUBLISHER_MATERIALIZE`)

A `#(materialize)` source's table is named by `name=` (or, for a `storage=` source without one, by the source's own name and its package's suffix, see [Whose table it is](#whose-table-it-is)). When a run names the physical table by its generation instead — a control plane's instructions, or a `scope: version` run, whose table carries the version's suffix — Publisher also makes a **view** at the declared `name=` that reads it (a source with no `name=` gets none: its name was Publisher's default, which no reader was promised), so a reader outside Malloy has one stable name for the current table. A bare `name=` is put in the same schema or dataset as the table it reads; a qualified one (`analytics.flights_all`) is used as written. A `storage=` view lives in the destination beside its table and reads it without the catalog name, so a reader attaching the destination under another name still resolves it.

- **When.** Once every source of a run has built, before the run commits, each view is created or repointed: the view and its mark in one statement or transaction, then the mark is read back (a view whose mark does not read back is dropped again and reported `failed`, so the next build never finds its own view in the way). The run's `metadata.nameViews` records each view, the table it reads and an `outcome` (`created`, `repointed`, `unchanged`, `replaced_own_table`, `replaced_by_table`, `name_taken`, `unsupported`, `conflict` or `failed`; the set is open). A view that cannot be made never fails the run. A run whose commit fails puts each view back as it found it.
- **Refused names.** Before a source builds, its `name=` is checked: a name held by a table, by a view Publisher did not mark, or by a view another package made, fails that source's build with nothing written. Publisher never replaces or drops an object it did not mark, nor another package's view. The one table it does replace is its own, below.
- **Moving from self-instructed to generation-named builds.** A self-instructed, unversioned build (the standalone auto-run, `scope: package`) names its table `name=` itself. When a later build of the same package names its table by generation (a control plane's instructions, or a move to `scope: version`), the table at `name=` is one this package wrote, and the view replaces it: the build writes its generation table, then the old table is renamed aside (to `<name>_o<id>`, the old entry's own backup name) and the view made in its place, as one transaction on DuckDB (a file, MotherDuck, or a DuckDB or DuckLake `storage=` destination) and Postgres, so a reader sees the old table or the view reading the new generation, never neither. On the other dialects the rename and the view are separate statements, and a view that cannot be made, or whose mark does not read back, puts the table back. On every dialect the old table stays aside until the run commits: only then is it dropped. A run whose commit fails (or is cancelled) drops the view and renames the table back, as it puts back any view it made, and records `table_restored` (or `restore_failed`, with why) on the failed run's `metadata.nameViews`. A server that stops between the rename and the commit leaves the table aside; the next generation-named build of that source puts it back before it builds, unless another run of the package is building (a self-instructed build rebuilds the table at `name=` instead). While another run of the package is building, a table already under the aside name is never dropped to make room: the view is then not made (`failed`) and the table stays. The run records `replaced_own_table`, with `asideTableName`. "This package's" is decided from Publisher's own records, never from the name: the newest committed run of this environment and package whose manifest names a table at exactly that name, on the same connection and destination, for the source of the same name, with no view made at that name by a later committed run, and the table not marked as another package's since ([Whose table it is](#whose-table-it-is)). Anything else at the name is still refused. Once replaced, the older runs' entries for that table are marked superseded (no reader serves or reuses them again), and deleting such a run with its tables skips the table that is no longer there rather than counting it as a failed drop. On Postgres a reader's own view over the old table refuses the replacement (a renamed table keeps its dependents, so that view would go on reading the old rows): the view is then not made (`failed`), and the table stays, served as before. Once replaced, another loaded version that served the old table through that name is rebound off it and answers that source live, rather than read the new generation through the view. A sweep that lists orphan tables must never drop `<table>_o<id>` where `<table>` is a table a committed run's manifest names (`_o<id>` is that entry's backup name, which no manifest lists): during a replacement it is the table a failed commit puts back, after a crash it is the only copy, which the next build puts back, and otherwise it is dropped with that run's tables. A sweep that sees only the warehouse cannot tell this copy apart, since it carries no mark.
- **And back.** When a self-instructed build of a table called `name=` finds this package's own view there (a generation-named build made it), its table replaces the view: on DuckDB and Postgres the view is dropped in the swap's transaction (in the transaction that creates the table, for a `storage=` build), so a failure leaves the view; on other dialects the view is dropped just before the swap and made again if the swap fails. The run records `replaced_by_table`, with `previousTarget` the generation the view read. That generation's table belongs to the run that built it and goes when that run's tables are dropped. Another package's view, or a view Publisher did not mark, at the name still fails the build.
- **Recognized by its comment mark.** Every view Publisher makes carries the comment `malloy-publisher:name-view owner=<environment>/<package> target=<table>` (each value URI-encoded): a sweep that lists or drops orphan tables must skip objects with this mark, and a dropper that removes `<table>` must settle the view first. The storage destination table listing never lists a view, and its table drop never drops one. The mark is what a sweep that sees only the warehouse can read; the run's `metadata.nameViews` is what tells a drop which views to settle. A view is changed or dropped only when both agree.
- **Cleanup.** Deleting a run with its tables, or reclaiming an archived or deleted version, settles the view before the table goes: a view still reading that table is repointed to the newest other committed run's table for the same view, or dropped with its last generation. A view reading another table is left alone. On Postgres, where a view binds to its table, a rebuild of the very table the view reads repoints the view at the new table inside the swap's transaction, so views a reader built over it survive; only when the new table's columns no longer extend the old ones is the view dropped and made again, which a reader's own view over it refuses (the build then fails with both intact).
- **Dialects.** DuckDB (with MotherDuck and DuckLake), Postgres, BigQuery (a dataset-qualified name), Snowflake, Databricks, Trino and SQL Server (a name with at most a schema). MySQL views cannot carry a comment and Presto's `CREATE VIEW` takes none, so no view is made there (`unsupported`).
- **Not v1.** A `#@ persist name=` source makes no view; its self-assigned table already carries the name.

### Persisted views (in development, `PUBLISHER_MATERIALIZE`)

With `PUBLISHER_MATERIALIZE=true`, `#@ persist` on a view stores the view's result, and that table answers a query only when the query calls the view exactly, or starts from it:

```malloy
source: flights is duckdb.table('flights') extend {
  measure: flight_count is count()
  #@ persist name="flights_by_carrier"
  view: by_carrier is { group_by: carrier; aggregate: flight_count }
}
```

- **What it answers.** `run: flights -> by_carrier`, and `run: flights -> by_carrier -> { … }`, whose later stages read the stored result. Not a refinement (`flights -> by_carrier + { limit: 3 }`, skipped `MISSING_FIELD`), a nest of the view, a hand-written query with the same SQL, or the view called on a source that extends `flights` (its own `where:` or gates could differ): those are answered as they would be without the annotation. The answer keeps the view's name, its columns in their order, its ordering and its render tags, and each column carries the annotations live's does (`#(classification)`, `#(doc)`, `# currency`, `# label`), colocated or `storage=`.
- **Which source it belongs to.** The source whose own statement puts `#@ persist` on the view: its `view:`, a redefinition after `except:`, an alias (`view: by_carrier2 is by_carrier`) carrying its own line, or an `include:` that re-annotates the view (`include { #@ persist public: by_carrier }`). A source that only inherits the view (an extension, a rename, an import, or a `rename:` of the view) does not persist it, and calls on it are answered live; annotate it there to store it there too.
- **Where it is stored.** Without `storage=`, in the source's own warehouse; with `storage=`, in that destination (needs `PERSIST_STORAGE_MODE=on`). The table is built from the source's own rows, never from a table the source itself is persisted to, and rebuilt whole: `watermark=`, `merge_key=` and `partition=` are refused on a view.
- **Not built, with a warning** (`persist-view-not-built`, and the view is answered live): a view whose result reads a given or the current time (it differs per caller or per read); a view whose result nests, stored with `storage=` (persist it without `storage=` instead); an `@env.` reference on its line; and, on a source with a row-level gate (`#(access_filter)`, or an `#(authorize)` that filters rows), a view that does not group by every field the gate reads, under its own name, in one grouping stage with no `limit:`, no `all()` or `exclude()` and no `calculate:`. That last one is a warning, not an error, because the gate may be added later by another author.
- **Gates.** A query on a gated source is answered from a persisted view only with `PUBLISHER_MATERIALIZE_GATED_SOURCES` on as well: the source's gates are written on the view's stored result and grafted and proven there exactly as on live, so each caller gets the groups the gate keeps. With the switch off, such a query is answered live and the path report skips the view with `CONTRACT`. Which gated sources may be stored at all follows the rules above for `#@ persist` (a gated `storage=` source is refused). Publisher does not authenticate callers: the gate holds only behind the gateway that sets the givens from a verified identity ([operating model](security-posture.md#operating-model-a-microservice-behind-your-gateway)).
- **The path report.** A query that calls a persisted view lists its table first, as `kind: "view"`: `used` when it answered (`servedBy.kind` is `view`), else skipped with `NOT_BUILT`, `STALE`, `CONTRACT` or `MISSING_FIELD`. A query that calls no persisted view does not list one. A `storage=` view's answer reports `servedFrom: "storage"`.
- **The build plan** lists each persisted view as a generated source named `<source>__<view>__view`; its serve verdict reads `The persisted view \`<view>\` of \`<source>\``, reached through the source.

Off (the default), `#@ persist` on a view is ignored, as before.

## Standalone vs. hosted (control-plane) deployments

The same package definition behaves differently depending on who drives materialization. Honest divergences to be aware of:

- **Who refreshes.** Standalone: the opt-in scheduler above. Hosted: the control plane drives refresh; the standalone scheduler stays off and skips control-plane-driven packages.
- **Tables-only vs. two-phase.** A standalone fire is a single-phase build that materializes persist sources into **tables**. A hosted deployment runs a two-phase job — tables, then a second pass over indexed dimensions — so index behavior is a hosted-only concern and can't be exercised locally.
- **Per-version tables.** `scope: version` is a policy contract. In the current standalone auto-run, a materialized table's identity is a content address derived from its connection and the source's canonical SQL (the `sourceEntityId`) — not the `#@ persist name` (that is the physical table name) and not the package version; true per-version tables are produced when the control plane assigns versioned build targets. Standalone is the right place to exercise the _policy_ (scope/schedule rules) and single-version builds, not per-version fan-out.
- **Incremental refresh.** Available to both, with the same declarations and the same delta. Standalone, the scheduler drives it; hosted, the control plane does, subject to the three host responsibilities in [Driving it from a control plane](#driving-it-from-a-control-plane) — a stable physical name per incremental source, `reseed` rather than `forceRefresh` to ask for a rebuild, and the boundary read back from each manifest entry.
- **Who owns the boundary.** Standalone it is the publisher's own store, always. A hosted deployment that spreads one package's refreshes over several workers can instead hold the boundary itself and echo it back on the instructions ([Supplying the ledger yourself](#supplying-the-ledger-yourself)), because that store is per-process and a refresh landing on a worker that has never seen the table would otherwise re-seed. The publisher still derives every boundary and still validates every one it is handed.
- **Physical-table GC.** Deleting a materialization with `--drop-tables` drops its tables. Deleting an environment or package removes the materialization **records only** — physical tables are intentionally left in place (physical-table GC is the caller's responsibility), so clean up tables you no longer want explicitly.

## Attribute a build's cost

`queryMetadata` at the `publisher.json` root (and the per-source `#@ persist queryMetadata.*`) tags every statement a build issues — the staging CTAS, the swap, the rename — and every statement a _query_ against the source issues, so the backend's own reporting can attribute both. Separating build cost from query traffic is the context layer's job, not the declaration's: publisher adds `class=materialize` plus the package, source, trigger and run id to a build, and `class=interactive` with none of those to a served query. It sits at the root because it declares what the source is, not only how it is built; its old home, `materialization.queryMetadata`, still works and is deprecated. See [query-metadata.md](query-metadata.md).

## Tune for cost and performance

The materialization history (`list` + `get` above) records per-run timings and how many sources were built vs. reused — enough to decide what to persist, what to stop persisting, and how to schedule it. The [materialization tuning guide](../skills/malloy-materialization/reference/tuning.md) walks an agent through reading those signals and proposing (recommendations-only) changes.

## Pre-aggregation

`#@ persist` stores a source you wrote. [Pre-aggregation](preaggregation.md) stores a rollup Publisher derives for you: annotate a measure with a grain, and covered queries read a small pre-grouped table instead of the base, with no change to the queries themselves. Rollups appear in the same build plan (as `origin: "preaggregate"`) and build through the same manifest and scheduler described above.

With `PUBLISHER_MATERIALIZE` on, a `#(preaggregate)` rollup also answers a query on another source that reads its measures through a join (join-to-rollup; [Pre-aggregation](preaggregation.md) has the rules). A parent with no joined rows counts as no rows and sums nothing, live and from the rollup (Malloy 0.0.438, [malloydata/malloy#3208](https://github.com/malloydata/malloy/pull/3208)), so counts and sums through a `join_many` are served; a `min` or `max` of a value that is not NULL on such a parent's row, and an aggregate at a join keyed on a computed dimension, are still answered live (`COUNT_SEMANTICS`).

**A rollup over a caller-scoped source is refused** (`preaggregate_over_dynamic_source`).
A persisted source may be scoped by a given because the term is left out of the build and
put back when the rows are read; a rollup has no such read. It is stored pre-grouped and
answered from directly, so there is no point at which a caller's term could be applied to
it — and building it reads the base, which applies that base's `where:` with the
declaration's default, so the rollup would hold one caller's aggregate and serve it to
everyone. Roll up a source that is not caller-scoped.
