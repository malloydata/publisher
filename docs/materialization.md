<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Materialization (Malloy Persistence)

Materialization pre-builds a Malloy source into a physical table so queries read the table instead of recomputing the source. In Publisher it is driven by **Malloy Persistence**: you annotate a source `#@ persist`, and Publisher builds it, records a manifest, and serves queries from the built table.

This doc covers the open-source Publisher's materialization surface — how to declare it, the rules Publisher enforces, how to build on demand or on a schedule, the `malloy-pub` CLI, and how a self-hosted (standalone) deployment differs from a control-plane-driven (hosted) one.

> **v1 and v2.** Most of this page documents v1, `#@ persist` on a source and `#@ preaggregate` on a measure, which every server reads. Materialization v2, `#(materialize)` on a source and `#(preaggregate)` on a measure, is on by default; `PUBLISHER_MATERIALIZE=false` turns it off, and the setting is removed one release after the default ships. With the switch off, Publisher ignores both v2 annotations and answers the source live; a `#(materialize)` it ignores gets no warning and no serve verdict. With it on, write a new materialization as `#(materialize)` for a table in the source's own warehouse or `#(materialize) storage="…"` for a storage destination (`name=` is optional on either: it sets the start of the table's name, schema or dataset included, the whole name except under `scope: "version"`, and without it Publisher names the table), which is served only with `PERSIST_STORAGE_MODE=on` too, and a new rollup as `#(preaggregate) grain=[…]`. `#@ preaggregate` is deprecated in v2; `#@ persist` on a view and `#@ -persist` are not. [materialization-migration.md](materialization-migration.md) is the recipe for moving v1 annotations to v2, and [the served copy](#the-served-copy-materialization-v2-publisher_materialize) documents how v2 serves.

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

A misspelled `#@ persist` fails the publish and the load: `#@persist` (no space), `# persist` or `#(persist)` (another route), and `#@ persits` (a misspelled directive) each fail with code `materialization-near-miss`, naming the spelling it meant, as a misspelled `#(materialize)` or `#(preaggregate)` (`#(materialise)`, `#(Materialize)`) does. A near miss of a route that is not a materialization (`#(clasification)`) stays an `annotation-near-miss` warning. See [packages.md](packages.md#annotations-that-do-nothing) and [Fails the load, or refused alone](#fails-the-load-or-refused-alone).

`name=` is the physical table Publisher writes. Persist the sources that are expensive to compute and reused by many queries; leave cheap or rarely-read sources unpersisted. The [materialization tuning guide](../skills/malloy-materialization/reference/tuning.md) helps decide.

`name=` may also name the container the table goes in — `name="analytics.order_summary"` writes `order_summary` into the `analytics` schema/dataset rather than the connection's default one. The container must already exist; Publisher does not create it. On BigQuery a dataset is required, since a table cannot live outside one.

The name is a plain identifier path: dot-separated segments of letters, digits, `_` and `-`. Publisher quotes each segment for the target warehouse itself, so case is preserved (`name="OrdersDaily"` creates `OrdersDaily`) and a reserved word works (`name="order"`). Don't quote inside the value: a name holding quotes, spaces, backticks or semicolons, such as `name="\"Orders Daily\""`, fails the publish and the load (`persist-invalid`, or `materialize-invalid` on `#(materialize)`), because the name is written into the build's DDL.

Each source needs a table of its own. When two different sources' `name=` puts them on one table in one destination (two models each persisting a source at `name="summary"`, say), each build overwrites the other's, so the author's collision fails the publish and the load, code `materialization-table-collision`, naming the sources, until each has a `name=` of its own. That holds on every route: `#@ persist`, `#(materialize)`, persisted views and persisted queries. A source with no `name=` cannot collide this way: it takes a readable name that hashes its model and its name ([Whose table it is](#whose-table-it-is)), so two models each persisting a source named `summary` build two tables. In a published version of a `scope: "version"` package a `name=` only starts the readable name, so it cannot collide either; in an unversioned package a `name=` is written as given whatever the scope, so two equal ones collide. Two names are compared by their table segment: they collide when the containers of the shorter end the longer's, since a name written with fewer containers lands in the connection's default schema or dataset, which the load cannot see (`name="main.t"` and `name="t"` are one table on DuckDB, `proj.ds.t` and `ds.t` on BigQuery). So `name="t"` beside `name="other.t"` fails the load even when the default schema is not `other`: a deliberate false positive, since a missed collision would serve one source the other's rows. Write both names with their container to keep them apart. A collision between a `#@ persist` and a `#(materialize)` names both, and under `PERSIST_COLLISION_ENFORCE=false` neither is built. A deployment that must load one anyway can set `PERSIST_COLLISION_ENFORCE=false`, which relaxes both the publish and the load check: the colliding sources are refused alone (`persist-not-built` or `materialize-not-built`, severity `warn`), neither is built or served from the shared table, both are answered live with serve verdicts `live`, reason `refused`, and the rest of the package loads. The same source reached from two models shares its table legitimately, since its content address is the same: only different sources on one table collide. A clash of names Publisher generates itself (a persisted view's or query's generated build source, a rollup's generated name across models, or a readable-name clash found at run time) is not the author's mistake, and stays a refusal of the source alone.

### Whose table it is

Packages, and environments, can share a connection or a storage destination, so Publisher's own builds (auto-run) never name a table after the source alone:

- **Without `name=`** (a `#@ persist` or `#(materialize)` source with none, a persisted view, or a rollup), the table takes a readable name: `<package>__<file>__<source>__p<8 hex>`, for example `ecommerce__orders__order_summary__pda155503` (in environment `prod`). See [Readable table names](#readable-table-names). Two packages that each persist a source called `order_summary` on one schema build two tables, and each serves its own rows.
- **With `name=`** (on `#@ persist` or `#(materialize)`), the table is called exactly that, since the author chose it and a reader outside Malloy may rely on it. So before every build Publisher checks who holds the name, and marks the table it builds with a comment, `malloy-publisher:table owner=<environment>/<package>` (URI-encoded). A name that is free, or held by a table marked as this package's, is built; so is a name held by a `name=` view an earlier release of this package made (its mark names this package, and the warehouse confirms it still reads the table its mark names), which the build replaces with the table ([leftover `name=` views](#the-name-view-materialization-v2-publisher_materialize)). A table with no mark is built only when this package's own committed runs say they wrote it there (a table from before the mark existed), and is then marked. Anything else (a table another package marked, another package's view, a table or view Publisher did not make, or a table with no mark this package's records do not name) refuses that source before anything is written, and it answers live: the run's `failures` names the holder, coded `table-owned-elsewhere`. A holder that cannot be read (the probe fails, or the dialect cannot be asked) refuses it too, coded `table-ownership-unverified`, since building would overwrite whatever is there. The package's older entries for a refused table are retired at once, so it never goes on serving rows another package wrote, and a retired entry never again counts as this package's record of the table (for the unmarked-table rule above). Give one of the sources another `name=`, or drop the other table if nothing needs it.
- **A `name=` that ends the way Publisher's own names do** (`__p` and 8 hex digits, or `__v<version>_` and 8 hex digits, the shape before readable names) fails the publish and the load, on every route (`persist-invalid`, `materialize-invalid`, `persist-view-invalid`, `persist-query-invalid`): it could be another package's default-named table, which carries no mark and which that package builds without a check. The build-time check stays behind it, and refuses such a name coded `table-owned-elsewhere` should one reach a build.
- **Two builds of one `name=` at once** on one server are taken in turn, from the check until the mark is written, so the second sees the first one's mark and is refused rather than both building the table.
- **Reuse** (skip-if-unchanged) of a table at a `name=` holds only while the table is still marked as this package's; otherwise the source is rebuilt, and checked as above.
- **Deleting a run with `dropTables=true`** skips a table another package has marked since the run built it: it is that package's now. It is retired, not reported as owed.

Host-orchestrated builds are not checked or renamed: the host chooses their names. Under `scope: "version"` a `name=` sets the start of the table's name, schema or dataset included, and the version and hash follow it (`name="analytics.summary_t"` builds `analytics.summary_t__v1_2_0__p<8 hex>`; see [Readable table names](#readable-table-names)), so that table is the version's own and is not checked either. The hash and the mark keep packages apart within one server; two servers whose environments and packages have the same names must not point those packages at the same schema. The mark is written right after each build; a server that stops between the build and the mark leaves a table this package's own records name once the run commits, and otherwise a table its next build refuses, which dropping it clears.

**Upgrading from a release before package-owned names.** The first auto-run of each package rebuilds its default-named tables under their new names, once. The old tables are not dropped: the old runs still name them, and deleting those runs with `dropTables=true` drops them. Until that first build a package serves its old tables as it did before, so build every package once after upgrading. A `name=` table two packages both wrote (both their records name it) is claimed by the first of them to build again, and marked; the other is refused it from then on and answers that source live.

### Readable table names

A table Publisher names itself reads what it is in a warehouse listing:

```
[<container>.]<package>__<file>__<source>[__v<version>]__p<8 hex>
```

- `<package>` is the package's name, and `<file>` the declaring model's base name, with no directory and no extension (`models/sales/orders.malloy` reads `orders`).
- `<source>` is the source the table is built for. A persisted view reads `<source>_<view>`, and a rollup `<base>_by_<grain dimensions>` (`orders_by_category_order_month`).
- `__v<version>` appears only under `scope: "version"`, as the release's numbers (`__v1_2_0`); a prerelease or build suffix is not shown.
- `__p<8 hex>` is always there: 8 hex of a hash over the table's identity (the environment, the package, the full model path, the source and the version). It keeps two packages, or two environments, on one schema apart, and tells apart what reads alike.
- `<container>` is a rollup's `name=` container (its own, or its base's), or a `name=`'s schema or dataset under `scope: "version"`. A `storage=` table is placed in the destination's schema as always.

Each part is lowercased, and every character outside `a`–`z` and `0`–`9` becomes `_` (runs collapse, so `__` only ever separates parts); a part that starts with a digit takes a `t`. The name is fitted to the warehouse's identifier limit, less the 13 characters a build appends to its staging copies: 50 characters on Postgres (and Redshift), MySQL, Trino and Presto, 115 on SQL Server, and 120 elsewhere and in a storage destination. When it does not fit, the package and file are cut first and the source last; the hash is never cut. A hosted deployment names its tables with the same prefix, `<package>__<file>__<source>[__v<version>]`, and a tail of its own.

- **A `name=` sets the start of the name.** In an unversioned or `scope: "package"` package, where a table can sit at a stable name, a `name=` is the table's exact name, schema included, with nothing appended. Under `scope: "version"` it replaces `<package>__<file>__<source>`, and its schema or dataset is the container: `name="analytics.summary_t"` builds `analytics.summary_t__v1_2_0__p<8 hex>`, and `name="summary_t"` builds `summary_t__v1_2_0__p<8 hex>`. The `name=` part is written as you wrote it, lowercased (`name="Summary_T"` builds `summary_t__…`), and cut from its end if the whole does not fit; the version and hash are never cut. Lowercasing is the only change: a `name=` whose table part the name would otherwise rewrite (a space, punctuation such as `-`, a leading digit, or `__`) fails the publish and the load of that version with its route's code (`persist-invalid`, `materialize-invalid`, `persist-view-invalid` or `persist-query-invalid`), as a rollup's `name=` does in any package. Use letters, digits and single underscores, starting with a letter. In an unversioned or `scope: "package"` package the `name=` is the table's exact name and is not checked this way.
- **Two models with the same base name** (`us/orders.malloy` and `eu/orders.malloy`) give names that differ only in the hash. Nothing collides, but a person browsing the schema cannot tell them apart; rename one file (`us_orders.malloy`) if that matters.
- **A table several sources share** (one content address) is named after the source with the smallest model path, then the smallest name.
- **A different `name=` is a different table.** Two sources that compile to the same build SQL (they differ only in gates applied at read, such as `#(authorize)`) share one table only when they name the same one: the same `name=`, no `name=` at all, or the same source reached from two models. Two `name=`s that name different tables build two tables, each under its own name, and each source is bound to its own; a source with no `name=` beside one that has one keeps its own readable table. Two `storage=` sources of one build, each with its own `#@ persist` declaration, that store different columns (one keeps a column `private:`, the other drops it with `except:`) are kept apart the same way, so a column such a source drops is never at rest in its table; an extension that inherits its base's declaration is served from its base's table, which holds what the base stores. When two such sources also share one `name=`, they are two definitions writing one table, an author [collision](#whose-table-it-is): the package does not load until each has a distinct `name=` (under `PERSIST_COLLISION_ENFORCE=false` it loads, an auto-run builds neither, even when only one of them changed, and both answer live). Which table a source gets never depends on build order. Adding or removing a same-SQL source with a `name=` rebuilds once every source of that build with a `name=` of its own and every `storage=` source of it whose stored columns are told apart (an incremental one seeds in full), since each one's table moves to, or back from, an address of its own. Two `name=`s are one table when they meet as the collision check reads them: case folded where the destination folds it (`Orders` and `orders` on DuckDB), and a name with fewer containers meeting one that ends the same way (`main.t` and `t`), so such names share one table; names in different schemas or datasets (`s1.t` and `s2.t`), or quoted names differing in case where the warehouse keeps case (`"T"` and `"t"` on Postgres), get a table each. A bare name that may be either of two such tables (`t` beside `s1.t` and `s2.t`) shares with neither: it is an author collision, so the package does not load (under the opt-out each of the three is refused alone).
- **Two sources whose names would be one table** fail the load, as any author's [collision](#whose-table-it-is) does: neither is served from it. A run never builds the second over the first: should two names still meet (two sources of one model that share a name, such as a `#@ persist` source named like a persisted view's generated build source beside it, or a hash collision), the second source is not built, is answered live, and is listed in the run's `metadata.refusedSources` with reason `table_name_collision`.

**What renames a table.** A table keeps the readable name it was first built under while its source keeps its content address and its place: moving or renaming the model file, renaming a source whose SQL is unchanged, or another source coming to share the table does not rename it, unless another source of the run now takes that name itself (a source renamed beside a new source of its old name), in which case the renamed one moves to its own readable name. A new name comes with a change of the name's container (a `storage=` destination, a rollup's `name=` container, a `name=`'s schema), of a `name=` under `scope: "version"`, or of the source's content address together with any of those or with its file or source name. What happens to the table at the old name is [the rename rule](#the-rename-rule) below. `<file>` is the model that declares the source, wherever it is imported.

**Upgrading to readable names.** A table built before them, at `<source>__p<8 hex>` (or `<source>__v<version>_<8 hex>` under `scope: "version"`), keeps serving until its source's next auto-run, scheduled or on demand, which builds it again under its readable name, and the old table goes by [the rename rule](#the-rename-rule). An incremental source is the exception: its ledger is keyed by its table's name, so it stays at the old name, and is built incrementally there, until it is next reseeded (`--reseed`), which builds it in full under its readable name. Anything that read a default-named table by name outside Malloy has to read the new name; give the source a `name=` where it needs a fixed one.

#### The rename rule

When an auto-run builds a source's table under a new name or in a new place, its old table (the one an earlier run of the package built for the same source, on the same connection) is reclaimed once the run has committed and every loaded copy of the package is bound to it, when Publisher can prove the table is its own, and is otherwise left where it is. It is its own only when all of these hold:

1. **Publisher named it.** The old name ends in one of Publisher's own suffixes (`__p<8 hex>`, or `__v<version>_<8 hex>` from before readable names), which an author's `name=` is refused.
2. **This package's records say so.** A committed auto-run of this package, on the same serving line (the unversioned package, the versions that share `scope: "package"` tables, or the one version under `scope: "version"`), built that name for the same source, on the same connection and in the same destination. A table a host's instructions named is the host's.
3. **No other package's mark is on it** (see [Whose table it is](#whose-table-it-is)).
4. **The replacement is live.** It committed, and it is what the package serves.
5. **Nothing else names it.** No source the package serves now is at a name that may be the same table, and no run outside the serving line (another version's own run, a host's run for another version) names it. "May be the same table" is by name and by place: two storage destinations are one place unless their configs prove them different DuckLakes (different catalog databases or metadata schemas with data paths that do not overlap) or different schemas of one (compared without case, and only for a name with no schema of its own), and a connection's own database is one place with a destination unless the connection provably cannot reach it (a warehouse that cannot attach a DuckLake, or a DuckDB connection with no setup SQL or attached databases). Catalogs and data paths are compared in one spelling (loopback hosts folded, URL schemes lower-cased, local paths resolved through their symlinks), and two catalogs written in different forms are never proven different. Config spelling is not trusted alone: before a table is dropped under the name the package now serves in another place, Publisher asks the table itself, by writing a one-off comment on the old table and reading it back from the served one (its comment is put back). A table that answers as the served one is kept; one that cannot be asked is kept and tried again. An active run that has claimed a name that may be the table, to write (an orchestrated run of another version), keeps it too. So a move between two names for one DuckLake never drops the table it moved to.

The rule is decided, and the tables dropped, under the lock a run's start takes (the package's lock for an unversioned package; the version's and then the package's table lock for a version), from a fresh read of the runs, and only while the run is the newest the package serves and no run of its serving line is active; otherwise nothing is decided, and the records wait for the next auto-run to commit (a host-instructed run never reclaims). The probe that finds a kept table gone runs before the lock, a few tables at a time; on Snowflake and MySQL a table the session cannot see is not taken as gone. A source renamed again before its old table went (A to B to C) is judged against the table the package serves now, so the oldest table still goes. A table a run built under the source's own bare name before package-owned names is never offered: it is not provably this package's.

So these are reclaimed: a table named before readable names; a rollup whose `name=` leaf changed or was removed, or whose container moved; a `name=` prefix or container changed under `scope: "version"` (in the version, as well as when the version is archived); the readable-name table a source had before the author gave it a `name=`; and a table left in a `storage=` destination the source moved from, to another destination or to its own database, or the other way, while that destination is still configured.

**Kept, and listed.** A table at an author's exact `name=` (outside `scope: "version"`) that changed or was removed is never dropped: the author chose it, and a reader outside Malloy may rely on it. Nor is a table in a storage destination that is no longer configured, which Publisher cannot reach. Each is listed on the run record, `metadata.keptTables` (`{sourceEntityId, sourceName, physicalTableName, connectionName, storageDestinationName?, reason}`, `reason` one of `author_name` and `destination_missing`), carried forward by each later run until the table is dropped (its runs deleted with `dropTables=true`) or its name is used again, or until a run finds it gone (dropped by hand), and raised as a package warning, `table-kept-after-rename` (`warn`, `subject` the source or a rollup's base, the message naming the table and where it is). Drop an `author_name` table yourself once nothing reads it. A `destination_missing` table may be one the package still serves, when the destination was renamed rather than removed: check before dropping it.

Out of scope, and left as before: a rename together with an edit to the source's SQL (a new content address), a connection change, and a source removed from the package. Those tables go when the runs that built them are deleted with `dropTables=true`.

A table that is not dropped is logged with the check that kept it (`Keeping a table a readable name replaced`, `guard`, which reads `author_name` for an author's name and `destination_missing` for a destination no longer configured). The run records each table it replaced, so one that is not dropped because the server stopped first, the drop failed or left it in place, the bind failed, or a check that may yet pass kept it (the replacement not committed or not yet served, another run still naming it) is tried again by the package's later runs. Reclaims count as `drop_tables_total{occasion="reclaim"}`. These checks read this server's records only: servers that share a schema must move to this release together, or one may drop a table another still serves.

### Where a table is built: `storage=`

Every annotation that asks for a table reads its place the same way: `#@ persist`, `#@ persist` on a view, `#@ preaggregate`, `#(materialize)` and `#(preaggregate)`.

- `storage="lake"` builds the table in that storage destination. On a `#@` line it may be written beside the route's key (`#@ persist storage="lake"`) or inside it (`#@ persist { storage="lake" }`); both mean the same, and `name=` is read the same two ways. When both are written, the one inside the key wins, even when it names nothing: `#@ persist { storage=["x"] } storage="lake"` fails the load, and is never built in `lake`.
- No `storage=` builds the table in the source's own warehouse, unless a host's build instructions name a storage destination for it (a control plane may choose one for a database-origin source). A table a host built in a storage destination is served from there exactly as a `storage=` table is, a rollup, persisted view and persisted query included: the table's binding, not the annotation, decides where it is read, and a gated source's rollup in a destination is held to the `storage=` rules.
- A `storage=` that names nothing fails the publish and the load: one that is empty or only whitespace (`storage=""`), or that is not text at all (a list, `storage=["lake"]`, or a block, `storage { … }`). So does an `@env.` reference in it. The code is the route's own: `persist-invalid`, `materialize-invalid`, `preaggregate-invalid`, `persist-view-invalid` or `persist-query-invalid`. The build plan's `refusedSources` reasons `empty_storage` and `env_reference` remain in the API but are no longer produced.
- Any other value is a destination name, trimmed and looked up exactly as written. A name no storage destination is configured under is not built; the package warns that the source is answered live (`destination_missing`), and the rest of the package is unaffected.

**What a `storage=` table holds, hidden columns included.** A `#@ persist storage=` table stores every column its source declares, the ones the source hides with `private:` or `internal:` included, as a colocated table and a `#(materialize)` table do. A renamed column is stored under the name the source's query gives it; a column the source's own `extend` drops with `except:` is not stored, unless another source with identical SQL shares the table and stores it (the two share one content address; a fix that keeps such a column out is coming separately). The [served copy](#the-served-copy-materialization-v2-publisher_materialize) reads the table under the source's own text, so it hides those columns again exactly as the source does: a caller can no more select, group by or filter on a hidden column read from the table than live, and is refused in live's words (`'home_region' is internal`, `'west_rows' is private`), while a measure or dimension of yours that reads one (`measure: west_n is count() { where: home_region = 'west' }`) is answered from the table. **Hidden columns are stored, never exposed through Publisher, and readable by anyone with direct access to the destination.** Grant access to a storage destination as you would to the warehouse its sources read; to keep a value out of the destination altogether, leave it out of the source's query rather than hiding it (`except:` keeps it out too, with the caveat above). `#(carries)` holds what a caller can reach, not what a table stores, so a hidden column of a class the source's list leaves out may be stored, and a public field that reads it refuses the package load on every tier alike ([classification](classification.md#carries-the-classes-an-entry-point-may-expose)).

### Opting a reader out: `#@ -persist`

`#@ -persist` is Malloy core's own opt-out, and it is not deprecated: Publisher honours it for `#@ persist` and `#(materialize)` sources alike ([materialization-migration.md](materialization-migration.md#persist-opt-out)).

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
source — is admitted for a `#@ persist`, colocated or `storage=`, when the gate is _proven_ to be
the entry point's own row filter (or an `#(authorize)` lock), and refused otherwise. Both tiers
follow the same rule ([the served copy](#the-served-copy-materialization-v2-publisher_materialize), under gated sources). A
`#@ preaggregate` rollup over it is built and read only under the conditions below.

- **`storage=`** under the entry point's own proven gate is built and served from the destination
  through the served copy, which grafts and proves the gate on every read exactly as on live, as it
  does a colocated table. The table holds every caller's rows, ungated, at rest in the destination,
  where anyone with direct access to the destination reads them all; this is a maintainer-approved
  exception made ahead of the managed tier's security review, for consistency with colocated. A
  gate reachable only through a join, a row filter the source inherits, an unbound parameter and a
  given the build would substitute are refused at build time, as before (see
  [§ Tenant-scoped sources](#tenant-scoped-sources-where-a-given-may-sit) below and
  [persist-storage-tutorial.md § Eligibility refusals](persist-storage-tutorial.md#eligibility-refusals-refused-at-build-time)).
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
- **`#@ preaggregate`** over a gated source is built, and answers a gated query per caller with
  live's rows, only when all of these hold: `PUBLISHER_MATERIALIZE_GATED_SOURCES` is on, the rollup
  is colocated (no `storage=`), and every field each gate reads is a grain dimension the source
  reads through no join. The query is then redeclared over the rollup with its gates, and each gate
  is grafted and proven there as on live. Otherwise the rollup is not built (a
  `preaggregate-not-built` warning, reason `access_filter`, or `authorize` for a lock) and its queries are answered live: with the switch off, in
  a `storage=` destination, when a gate reads a field the grain drops, or when a gate reads through
  a join or the source joins a gated source. A caller's own join into a gated source keeps the
  query off every rollup too. [preaggregation.md](preaggregation.md#preaggregate-materialization-v2-behind-publisher_materialize)
  has the full rule, which v1 and v2 rollups share.
- **Two more conditions hold under every switch**, so that a gate filters the stored rows exactly
  as it filters live's. Each covers every field the gate reads and every field that field is
  computed from, through renames, joins and the stages of a query-shaped source.
  - A query that sets its own `timezone:` (or reads a table built in another zone) answers live
    when the gate reads a time value the table stored in its build's zone: a truncation computed
    in a query stage, or a rollup's time grain. The path report says `served_copy_time_zone` or
    `rollup_time_zone`. A gate over a column the served copy leaves out answers live rather than
    denying every caller.
  - A `storage=` table is never read past a gate whose column it stores as a binary float or a
    128-bit integer (which DuckLake holds as a float), or whose column it cannot be shown to hold
    at all: two exact values can store as one. The type judged is the one recorded when the table
    was built; a table altered in the destination afterwards is not re-read (anyone who can alter
    the destination can read it directly anyway). A database table is built by the source's own
    warehouse in its own types, so only a column its DuckLake catalog holds as a float, as the
    bind reads it, is judged inexact. Such a query answers live. A Postgres `storage=` build that
    cannot read the warehouse's column types fails rather than store a `numeric` as a float.

Every refusal names the source and the remedy; a package carrying one fails to build (or, for
`storage=`, fails that materialization run) rather than silently serving the gated source to
everyone. Every valid declaration that is not built, whatever its mechanism, is one record in the
build plan's `refusedSources`, with a bounded `reason`, and one package warning (severity `warn`)
carrying its message, under its mechanism's not-built code: `persist-not-built` for a `#@ persist`
source, `materialize-not-built` for a `#(materialize)` source, `persist-view-not-built` for a
persisted view and `persist-query-not-built` for a persisted query (`subject` the view or query),
and `preaggregate-not-built` for a rollup (`subject` its base, or `<base>.<measure>` for one
measure). A `#@ persist` source's code was `persist-source-refused`; for one release each such
warning is repeated with that old code and the same message, so a reader that matches the old code
keeps working, and then the repeat is removed. No other mechanism's warning, and no refusal no
earlier release raised (`storage_source_unsupported`, `incremental_unsupported`), is repeated.

### Sources that read the current time

A `#@ persist` source whose **build** reads the current time (`now`) is refused, on both tiers, as
`current_time`: it is not built, it is answered live, and the package carries a
`persist-not-built` warning naming it. Built, its table would hold the rows as of the build and
serve them until the next rebuild, while a live query reads `now` again every time.

The build reads `now` when it sits in the persisted relation:

- the persisted query: its `where:`, a field it computes, any later stage;
- a view the source runs (`source: r is orders -> recent`);
- the `where:`, a dimension or a join of the source that query reads, when the query uses it,
  `select: *` included (`source: recent is raw extend { where: ts > now - 7 days }` followed by
  `#@ persist source: r is recent -> { … }`);
- the query of a query-shaped source the persisted source is built from, at any depth, in its own
  file or an imported one, an upstream `#@ persist` included (a downstream source built over a
  refused one reads its query inline);
- the query of a query-shaped source the persisted query reads through a join.

A base field the query does not read does not refuse it.

`now` in the persisted source's own extend block is not in the build, so it is admitted: a
`where: ts > now - 7 days`, a `dimension: is_past is ts < now` or a measure filtered on `now` written
there is applied at every read, over the stored rows, and answers as live. That is the move the
refusal names:

```malloy
// Built, answers as live: the window is applied at every read.
#@ persist name="events"
source: events is raw -> { select: * } extend { where: ts > now - 7 days }

// Refused (current_time): the window is in the build, frozen as of the build.
#@ persist name="events_frozen"
source: events_frozen is raw -> { where: ts > now - 7 days; select: * }
```

A `#@ preaggregate` or `#(preaggregate)` rollup whose base, measure or grain reads `now` is left
unbuilt by the rollup's own rule (`preaggregate-not-built`, reason `current_time`), including a grain that reads it
through a join to a query-shaped source whose query does, and `#(materialize)` has its own (rolling windows,
below), under which an `inline=` join whose joined rows read `now` refuses the source. `now`
written inside a `conn.sql("...")` string is SQL text Publisher does not read, so it is not refused.

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

A given-reading `where:` the persisted source inherits from a base (`source: scoped is raw extend
{ where: org_id = $ORG_ID }` followed by `#@ persist source: r is scoped extend { … }`) is refused
as `inherited_where_with_given`, on both tiers: only the persisted source's own declaration is
applied again over its table at read, so it is not built and is answered live, under the term.
Move the `where:` onto the persisted source itself to serve it from a table.

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
#@ persist name="grants" storage=lake
source: grants is raw_grants -> { select: * } extend {
  where: org_id = $ORG_ID and user_id = $USER_ID
}

#@ persist name="opps" storage=lake
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
- **The window holds on a standalone server and under a control plane alike.** A standalone run
  stamps the declared window on the manifest it commits, as a control plane stamps the manifest it
  distributes ([Freshness](#freshness-a-window-and-a-fallback)).

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

This does not make two sources on different origin connections joinable. Malloy refuses a join
across connections (`join-connection-mismatch`) for the whole file, so a model that joins them fails
to load, with that diagnostic, whatever is materialized. Put the joined sources on one connection.

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

### `partition=` is not supported

A stored table is written unpartitioned. `partition=` on `#@ persist` or `#(materialize)` is a
retired key, and a package that carries it fails the publish and the load (`persist-retired-key`,
`materialize-retired-key`) with a message that says to remove it. It once laid the table out as one
directory per value; that was a layout and never isolation, since every stripped term is re-applied
at read either way, so removing it costs scan time and changes no caller's answer.

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
framed around a cron steers away from it — for a gated source, take the ceiling. (The window holds
on a standalone server too: see [Freshness](#freshness-a-window-and-a-fallback).)

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
question — it reads from the source and writes elsewhere — and the storage build can read only a
source whose connection it can pass a query through to: Postgres, BigQuery or Snowflake, or a
DuckLake catalog building into that same catalog. A Snowflake read first sets the session settings
Malloy's live connection uses (case-sensitive quoted identifiers, UTC, weeks starting on Sunday),
so a day or week bucket is stored as live computes it whatever the account's defaults. It reads
`TIMEZONE` and `WEEK_START` back on the same session before it reads any rows, and a session that
does not hold them fails the build, so grant the build's role `ALTER SESSION` on both; a refused
`QUOTED_IDENTIFIERS_IGNORE_CASE` is logged and the build goes on. Each column is stored under the name its source declares even where the account folds quoted names to
upper case; a table stored with folded names before this is read by a case-insensitive match. A `storage=` source on any other connection, the
package's own DuckDB sandbox (`duckdb.sql(...)`, `duckdb.table(...)`) and an environment DuckDB
connection included, is refused at load with reason `storage_source_unsupported`, on every route
(`#@ persist`, `#(materialize)`, a persisted view or query, and a `storage=` rollup over such a
base): it is not built, its queries are answered live, the route's `*-not-built` warning names the
connection, and a run builds the rest of the package. Drop `storage=` to build it in its own
connection, or put the source on a warehouse connection. The decision is the connection's
configured type, never its name. A DuckLake source on a catalog other than its destination's is
admitted by the plan, which does not know the destination's catalog, and refused by the run with the
same reason; that source alone is not built, and the run builds the rest.

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

A Postgres `numeric` keeps its exact value. DuckDB's Postgres passthrough reads an unconstrained
`numeric`, a `numeric(p,s)` with p > 38 and every numeric expression (`a + b`, a window sum, a
rollup's partial sum) as DOUBLE, so a storage build reads each such column (and a domain over
`numeric`) as Postgres's text instead and stores it as `DECIMAL(38,s)`, `s` the widest scale it read
with trailing zeros dropped: `sum` and `avg` served from the table equal live. A column no
`DECIMAL(38,s)` holds exactly (more than 38 digits, a `NaN` or an `Infinity`) is stored as that text
rather than rounded, so a query reading it is answered live and the package reports it under
`storage-column-type-mismatch`. DuckDB's DECIMAL arithmetic stops at 38 digits (a product of two
high-scale columns, or a sum past 38 digits), and a stored answer it refuses is answered live
whatever the source's `freshnessFallback`. Finding the columns costs a describe of the passthrough
and, when any column reads as DOUBLE, a type probe over a `LIMIT 0`; neither runs the query. A build
with such a column stages its result in the build session before writing the table, so it writes the
rows twice and reads the warehouse once. An incremental delta is cast into the stored `DECIMAL` only
when its value fits; one that needs more digits rebuilds the table at the wider scale. BigQuery
`NUMERIC` and Snowflake `NUMBER` already arrive as DECIMAL; a BigQuery `BIGNUMERIC` arrives as text
and is answered live. A same-catalog DuckLake build computes in DuckDB, as live does.

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

The same warning, and the same `publisher_storage_column_type_mismatch_total{stage="serve"}`
count, covers every `storage=` table, checked against the columns its build source declares: a
`#@ persist` source's (above), a `#(materialize)` source's, a rollup's, and a persisted view's or
named query's. Every reader takes the column the same way, and the warning says which applies: it
is read through a faithful cast where one gives live's values, and is never read as its stored type
otherwise. A `#(materialize)` source's served copy leaves the column out, so only a query that reads
it is answered live (one whose own measure or view reads it answers live as a whole). A persisted
view or query, and a rollup with such a grain column or partial (any partial, checked against the
type its build source declares, whatever expression it aggregates), declines its table for the
query (`MISSING_FIELD`), which is answered live. A rollup is named by its base source, and a view
or query by its build source.

**A database table altered outside a build.** A table in the source's own database (no `storage=`: a
`#@ persist`, a `#(materialize)` or a rollup) is read with the column types its
warehouse holds when it is bound, not only those its build wrote. Each bind (after a run, a reload,
a restart, or a rebind from the store) reads every bound database table's columns, never per query:
one catalog query per schema or dataset (`information_schema.columns`), one for every Postgres
table, and DuckDB's in-process `DESCRIBE` per table, at most four at once, each bounded by 15
seconds. A column whose type has changed since the build (an `ALTER TABLE` outside Publisher),
including a big integer altered to a decimal, and on any warehouse but DuckDB a timestamp that
gained or lost its time zone, is taken as above: a `#@ persist` or `#(materialize)` table reads it
through a faithful cast or leaves it out, so only a query that reads it is answered live, and a
rollup, persisted view or persisted query, which no cast reaches, is answered live as a whole. It
raises the same `storage-column-type-mismatch` package warning, one per table and column, saying that
the table was altered outside a build and whether the column is cast, answered live, or the whole
table answered live, and counts in the same
`publisher_storage_column_type_mismatch_total{stage="serve"}`, once per bind. A column that is gone
is reported too; one whose new type still reads as its declared type is not. Rebuilding the table
clears it. The log line has the same code and names each changed column. A table
whose columns cannot be read at the bind (missing, unreadable, or slower than the bound) is answered
live, with a `database-table-unread` package warning that names the table, a fixed reason (the
driver's own text, secrets redacted, is in the log alone) and whether a retry is pending, and read
again with backoff (after 1 second, doubling to at most a
minute, ten times), and by any later poll of an unchanged manifest, until a read succeeds, when the
warning clears. A change made after a bind is read at the next one.

**Columns with no Malloy basic type.** A stored list or struct column is declared as Malloy's array
or record type, so it reads back as it does live, when the source's field is one; a column Malloy has
no faithful declaration for (a MAP, UNION, ENUM, BLOB, INTERVAL or JSON column, a list or struct
holding one, a list or struct whose field live types as a warehouse-native type or that holds one, or
a list or struct holding an integer live keeps exact past 2^53, such as a DuckDB `BIGINT[]`) is left out
of its source, so a query reading it is answered live and every other query reads the table. A list or
struct of zoned timestamps is declared `timestamptz`, as live types it. A stored scalar whose field live
types as a warehouse-native type (`sql native`: a `uuid`, or a Postgres `json`, `jsonb` or composite
type, which the destination stores as text) is left out the same way, and a Postgres `bytea` or
`interval` is stored as BLOB or INTERVAL rather than cast to text, so it is left out too: live answers
each in a type or text the stored column does not reproduce. A table built before 0.13.1 keeps a `bytea`
or `interval` column as text until it is rebuilt in full. The build does
not refuse such a table, even one with no column left to declare: the served copy declines that
source at bind, so every query over it is answered live. A `storage=` rollup over a DuckDB source
whose grain is such a column is the exception: every read of it groups by its grain, so it is refused
at plan (`unreadable_grain`, [preaggregation](preaggregation.md)). Each column a `storage=` build stores that
no type declares is counted in `publisher_storage_serve_shape_type_fallback_total` and logged.

**A column the served copy cannot carry sends a `#(materialize)` source live as a whole.** A
`#(materialize)` source leaves a column out per query, as above, only when the column is stored: a
list (a DuckDB `BIGINT[]`, a BigQuery `ARRAY<INT64>`), a MAP, a UUID and the like. Three kinds of
column are never on its table, and each one sends **every** query over the source live, whatever
the query reads, in a `storage=` table and a colocated one (a database origin) alike:

- a struct (record) column of the source's own, such as a DuckDB `STRUCT(a BIGINT)` or a BigQuery
  `STRUCT<…>`: Malloy types it as a join, so the plan stores no column for it and the served copy
  cannot carry the field (`served-copy-not-served`, reason `fields_not_carried`);
- a column of an `inline=` join that no `type:` declares, such as a BigQuery `GEOGRAPHY` or a
  DuckDB `MAP` on the joined source (`served-copy-not-served`, reason `column_types`);
- a nest of a colocated table outside DuckDB, which its warehouse stores as JSON (`column_types`).

The answers are live's, so nothing is wrong, only slower. Each query's path report says
`servedBy: live` and skips the table as `WARNED` with code `served-copy-not-served`; the package
warning of that code names the reason; the path report itself does not. A v1 `#@ persist`
`storage=` source stores a struct column of its own and serves it from the table.

**Integers past 2^53.** A stored table's columns are declared to Malloy as plain numbers, which it
reads through a JavaScript number, so 9007199254740993 would come back as 9007199254740992. A
column whose field Malloy types as a big integer (a DuckDB `BIGINT`, `HUGEINT` or `UBIGINT`, a
BigQuery `INT64`, a Snowflake `NUMBER(p,0)`, or a sum or max over one) is therefore read through a
native cast: `::"HUGEINT"` in a `storage=` table when the column is stored as a 64-bit or narrower
integer or a `DECIMAL(p,0)` (a 128-bit integer column in DuckLake is not exact; see below), and, in a colocated one (a colocated
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
which table it wrote). A colocated column whose type its build did not record is the one exception
on BigQuery: BigQuery has exactly one integer type, so the column is read as `INT64` and still
serves. Either way the package reports the source as a warning with code
`stored-native-schema-missing`, naming its columns and whether each is read as `INT64` or answered
live; the next full rebuild records the types and clears it. Served never exceeds live where Malloy's dialect says live rounds: on a source whose live answer
itself rounds such values (Postgres returns its rows through JSON), a `storage=` table reads the
column as a plain number, which rounds the same way, and a colocated one reads it through
Postgres's own `bigint` type, on live's own path ([known limitation
12](materialization-v2-plan.md#known-limitations-to-revisit)). Where the dialect says live keeps
every digit but the connector does not, served is the exact answer and live rounds: Snowflake's
`NUMBER(p,0)` today (see "Snowflake: live and served differences" below).

**A 128-bit integer in DuckLake.** DuckLake (DuckDB 1.5) writes a `HUGEINT` or `UHUGEINT` column to
its Parquet files as a `DOUBLE`, while its catalog still reports the 128-bit type, so a value past
2^53 is rounded at rest: a sum of `BIGINT`s, 4530000000000225187, reads back as
4530000000000225280. Only rows DuckLake keeps inlined in its catalog stay exact, and a reader
cannot tell which it has. A plain DuckDB table keeps every digit. So:

- A build that computes its columns in DuckDB (a same-catalog DuckLake build, a chained build over a
  stored upstream, or a stored rollup) stores each 128-bit integer column it computes as
  `DECIMAL(38,0)`, which Parquet keeps exactly and the serve side reads with every digit. A value
  past 38 digits fails that cast, and the build stores the 128-bit type instead.
- A 128-bit integer column in a `storage=` table, or in a database table in a DuckLake catalog (on
  a `ducklake` connection, or one a DuckDB connection attached), is never read as exact or
  re-aggregated, whatever its field's type or the source's dialect. A `#@ persist` or
  `#(materialize)` table leaves the column out, so only a query that reads it is answered live
  (an inlined join holding one leaves its source live). A rollup, persisted view or persisted query
  whose table stores one answers live; the path report skips that table with `MISSING_FIELD` and
  names the stored column. The package reports each such column with
  `storage-column-type-mismatch`, handled live.
- A chained build never stacks on a table holding such a column: the downstream recomputes from
  its raw source, so the rounded values are never stored again as exact.
- A 128-bit integer inside a list or struct is not cast, so DuckLake stores that leaf rounded too.
  Such a column is left out at serve (a list or struct holding a big integer has no stored
  declaration, above), and a chained build never stacks on its table, so the rounding is never
  served; only the stored table holds it.
- A table built before this change keeps the 128-bit column, answered live, until its next full
  build; nothing forces that rebuild. (An incremental source is always a passthrough read, whose
  columns arrive in the warehouse's own types, so it never stores such a column.)
- A colocated build on a `ducklake` connection still stores the 128-bit type, which is answered
  live as above.

**Snowflake: live and served differences.** Two Snowflake answers differ between live and served
today, and in both the served answer is the right one; each needs a fix in Malloy's Snowflake
connector, not in Publisher.

- *Integers past 2^53.* Malloy's Snowflake dialect says live keeps every digit of a `NUMBER(p,0)`, so
  a stored read keeps them too (above). Live does not, on any connection Publisher configures: the
  connector gives snowflake-sdk the configured options without its own `jsTreatIntegerAsBigInt`
  default, so the driver returns the integer as a JavaScript number. A `sum()` over a
  `NUMBER(38,0)` past 2^53, such as 4530000000000225187, comes back rounded live
  (4530000000000225300) and exact served. Publisher does not round the served answer to match.
- *`TIMESTAMP_TZ`.* The connector pins the session's text format for `TIMESTAMP_NTZ` and
  `TIMESTAMP_LTZ`, not for `TIMESTAMP_TZ`, so live renders a zoned value in the account's own format
  and offset (`2026-01-15 10:00:00.000 -0800`), and served renders the same instant in UTC ISO form
  (`2026-01-15T18:00:00.000Z`). The instant is the same; only the text differs.

A run's record (`GET …/packages/{packageName}/materializations/{materializationId}`, and the
listing) returns its build manifest. Each entry names the physical table it built, and a colocated
entry's `nativeSchema` lists every column of that table with its warehouse type, including columns
the model marks private or internal. No row value is returned. Publisher does not authenticate
callers, so the gateway in front of it must restrict the materialization routes to operators (see
[the operating model](security-posture.md#operating-model-a-microservice-behind-your-gateway)).

## Fails the load, or refused alone

Every problem with a materialization annotation has one of two outcomes.

**A mistake fails the load.** An annotation that is wrong as written, or code it covers that Publisher cannot plan, fails the package on every path that loads it: a publish or PATCH, a reload, a restart, a watch-mode save, and a load from a control plane's storage that never passes a publish. A publish answers 400; a load reports the package in `/status` `loadErrors`. A failed reload or watch-mode save keeps serving the model compiled before it, marked `stale: true`, as a compile error does. The message has one line per mistake, each ending with its code in brackets:

```
model.malloy: #@ persist source "daily": `nmae=` is not a key `#@` reads, so it does nothing (did you mean `name=`?) [persist-unknown-key]
```

Fix the annotation each line names, then publish or reload. What fails the load:

- A `#@` line that does not parse (`persist-unparseable`), and a `#(materialize)` or `#(preaggregate)` line that does not parse (`materialize-unparseable`, `preaggregate-unparseable`).
- A key the route does not read: every route's key set is closed (`persist-unknown-key`, `materialize-unknown-key`, `preaggregate-unknown-key`). A retired key, `partition=`, `sharing=` or `schedule=` (`persist-retired-key`, `materialize-retired-key`).
- A value that is wrong as written (`persist-invalid`, `materialize-invalid`, `preaggregate-invalid`, `persist-view-invalid`, `persist-query-invalid`): an empty or non-text `storage=`, an `@env.` reference, an unsafe `name=` or one that ends the way Publisher names its own tables, `#(materialize)` beside the source's own `#@ persist`, an `inline=` that is not a list of joins of the source, a carried key of the wrong shape, a list `watermark=` anywhere but an incremental pair table, an incremental key on a persisted view or query, and an invalid [incremental declaration](#incremental-refresh).
- `#@ persist` on a shape Malloy does not plan, such as a filtered pass-through, a table with only added fields (`duckdb.table('x') extend { measure: … }`), or an unbound parameterized source (`persist-not-materializable`).
- A rollup that cannot be what it says (`preaggregate-not-a-measure`, `preaggregate-unknown-grain`, `preaggregate-not-mergeable`), on `#(preaggregate)` and `#@ preaggregate` alike ([preaggregation.md](preaggregation.md)).
- Two different sources on one table in one destination, by their `name=` ([`materialization-table-collision`](#whose-table-it-is)).
- A misspelled route or directive: `#(materialise)`, `#(Materialize)`, `#@persist`, `#@ persits`, `# persist`, `#(persist)` (`materialization-near-miss`). `#(materialize)` and `#(preaggregate)` spelled exactly while `PUBLISHER_MATERIALIZE=false` are ignored, not a failure.

**A valid annotation Publisher cannot build is refused alone.** An annotation that is right, but asks for what this environment, dialect, capacity or governance cannot give (an unconfigured `storage=` destination, a gate, a free parameter or given, a read-only dialect, a `storage=` source on a connection the build cannot pass through, an incremental source on a dialect without a bounded range, a hidden field in a rollup), refuses that item only: it is not built, its queries are answered live with live's rows, the rest of the package loads and serves, and the package carries a warning with severity `warn` saying why.

The code says which outcome it is:

| Outcome | Codes |
| --- | --- |
| Fails the load, never a warning | `*-invalid`, `*-unknown-key`, `*-retired-key`, `*-unparseable`, `persist-not-materializable`, `preaggregate-not-a-measure`, `preaggregate-unknown-grain`, `preaggregate-not-mergeable`, `materialization-table-collision`, `materialization-near-miss` |
| Refused alone, severity `warn`, and a `refusedSources` record with its reason | `persist-not-built`, `materialize-not-built`, `preaggregate-not-built`, `persist-view-not-built`, `persist-query-not-built` |

Warnings about the deployment rather than one item carry codes of their own, such as `materialization-destination-missing` and `materialization-storage-mode` (`storage=` while `PERSIST_STORAGE_MODE` is `off` or `write-only`).

Renamed and retired codes:

- `persist-source-refused` is now `persist-not-built`. For one release each `#@ persist` gate refusal is repeated under it, with the same message, in the package's REST `warnings` (the Console and the MCP tools show it once); then the repeat is removed. Another mechanism's refusal, and a refusal no earlier release raised (`storage_source_unsupported`, `incremental_unsupported`), carries only its own code, without the repeat.
- The rollup refusals `preaggregate-hidden-field`, `preaggregate-access-filter`, `preaggregate-time-zone`, `preaggregate-current-time`, `preaggregate-reads-given`, `preaggregate-unreadable-measure`, `preaggregate-join-level` and `preaggregate-name-clash` are no longer produced: each is `preaggregate-not-built`, with the reason `hidden_field`, `access_filter` (or `authorize` for a lock), `time_zone`, `current_time`, `given_in_persisted_query` or `inherited_where_with_given` (formerly `reads_given`), `unreadable_measure`, `join_level` or `name_clash` on its `refusedSources` record.
- `preaggregate-malformed` is no longer produced: the same mistake is `preaggregate-invalid` or `preaggregate-unparseable`, and fails the load.
- The API keeps three reasons it no longer produces: `RefusedSource.reason` `empty_storage` and `env_reference`, and `OmittedSource.reason` `persist_annotation_unparseable`.

The package manifest's own policy is not an annotation and keeps its own rule: [refused at publish, a warning at load](#the-persistence-policy-the-publish-gate).

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
| `rollup_over_stored_base` | Retained, never reported by a released server: a rollup over a `storage=` base is planned, built and served like any other ([preaggregation.md](preaggregation.md#rollups-in-the-managed-store)). | None needed. |
| `unreachable` | No source on the package's published surface reads it: `index.malloy` hides it, and nothing it publishes is built on it, extends it or joins it. | Publish it, or a source built on it, or drop the annotation. |
| `row_level_gate` | Every query through it carries an `#(authorize)` or `#(access_filter)` gate the table does not keep: a rollup's, or a `storage=` table's that is not its own source's gate (an ungated source read only through gated entry points). A `storage=` table under its own source's gate keeps it (G3), as a colocated one does. | Materialize without `storage=`, or move the gate onto the stored source itself, or onto an entry point over the table. |
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

## Freshness: a window and a fallback

A stored table can say how old its data may be before a query stops reading it:

```malloy
#@ persist freshness.window="24h" freshness.fallback="live"
source: daily is orders -> { group_by: order_date; aggregate: total is amount.sum() }
```

The two keys mean the same thing on every mechanism (`#@ persist`, `#(materialize)` and a rollup's
`#(preaggregate)` or `#@ preaggregate`), on both tiers (colocated and `storage=`), and whether a
standalone run or a control plane built the table.

- **`freshness.window`** is how old the table's data may be: a whole number and one unit, `s`, `m`,
  `h`, `d` or `w` (`90s`, `30m`, `24h`, `7d`). A table is aged from the instant its data is as of
  (the start of the run that built it), not from when the build finished. Age equal to the window is
  still fresh.
- **`freshness.fallback`** is what a query does once the table is past its window: `live` answers
  from the warehouse, `stale_ok` keeps reading the table, and `fail` answers live as `live` does.
  Either way the path report names the table with `STALE` when it did not answer. A table with no
  window is never stale.

Each key resolves on its own, most specific first: the source's own declaration, then the model
file's (`## freshness.window="…"`, or inside `## materialization { … }`), then `materialization.freshness`
in `publisher.json`. A rollup's own declaration comes first, then its base's own, so a rollup and
its base go stale together unless the rollup says otherwise (see
[preaggregation.md](preaggregation.md#freshness)).

**A standalone run stamps the declared window.** Each entry of the manifest it commits carries
`freshnessWindowSeconds`, `freshnessFallback` and `dataAsOf`, the same three fields a control plane
stamps on the manifest it distributes, so the serve path ages the table the same way. A table that
declares nothing carries none of the three fields.

**A table is never fresher than what it read.** A table built from another stored table (a chained
build, or a rollup over its base's table) is anchored at that table's `dataAsOf` when it is older. A
table of unknown age is never read as new: when a run rebuilds a table that declares a window, a
carried table it reads through a chain, whose age is unknown (it declares nothing) or past that
window, is rebuilt in the same run, and a rollup reads its base's table only when the base was built
in this run or its `dataAsOf` is inside the rollup's window; otherwise the rollup builds from the
warehouse.

**A standalone run rebuilds a table past its window.** A run normally carries an unchanged table
forward rather than rebuild it. A table past its declared window at the run's start is rebuilt
instead, as is a windowed table whose entry has no `dataAsOf` (one built before tables were
stamped); a table inside its window is carried, keeping its anchor and taking the window declared
now. A run is what refreshes a table: nothing rebuilds it the moment its window passes, and the
package's `schedule` cron cannot be declared beside a freshness policy
([the publish gate](#the-persistence-policy-the-publish-gate)). So on a standalone server a table
with a window answers live (or stale, under `stale_ok`) once its window passes, until a run started
on demand or by a scheduler of your own rebuilds it.

**Under a control plane, the control plane's manifest decides.** A run with `buildInstructions`
stamps nothing, and the manifest the package binds from `manifestLocation` is read as the control
plane wrote it: its `freshnessWindowSeconds`, `freshnessFallback` and `dataAsOf` apply whatever the
model declares. A reference the run fills in from this server's own records takes no freshness
fields from them. The plan reports each source's resolved declaration under `freshness` for the
control plane to stamp from.

**One grammar, checked at load.** On every mechanism (`#@ persist`, including on a view or a named
query, `#(materialize)`, `#(preaggregate)` and `#@ preaggregate`), a window that is not a whole
number and a unit, or a fallback other than `live`, `stale_ok` or `fail`, fails the load with the
route's `-invalid` code (`persist-invalid`, `materialize-invalid`, `preaggregate-invalid`), and a key
in the group other than `window` and `fallback` with its unknown-key code.

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

A materialization run compiles the package, builds every `#@ persist` source into its table, writes a manifest, and loads it so queries serve from the built tables. It settles at `MANIFEST_FILE_READY` (success) or `FAILED` / `CANCELLED`. An auto-run reaches `MANIFEST_FILE_READY` only after its manifest is loaded into the package it built, so a query sent once the run reports `MANIFEST_FILE_READY` is answered from the new tables. A load that took is `autoLoad: "loaded"` in the run's `metadata`. The load and the move to `MANIFEST_FILE_READY` run under the package's lock, so a reload, publish or version load that lands meanwhile waits and then binds this run's manifest, never the one before it.

If the load does not take, the run still settles at `MANIFEST_FILE_READY`, since its tables were built, with `autoLoad: "failed"`, `autoLoadStep` and `autoLoadError` in its `metadata` and its `error` set to "Built, but not served …". What is served then depends on the step that failed: `reload` (the package's models did not recompile; it keeps the models it had, though a published version is then rebound from the store, which now has this run), `bind` (the models recompiled, so the run's colocated tables are served, but its `storage=` tables are not) or `timeout` (the load did not finish within ten minutes; what is served is undecided until it does). Either way the package's next reload or restart binds this run's manifest from the store. A hung load holds the package's lock until it returns, as a hung reload always has: queries keep being answered, but that package's reloads, publishes and metadata PATCHes wait. `malloy-pub materialize --wait` prints "built but not served" and exits 2 for such a run, and the Console shows it as a warning. A stop that lands while the manifest loads does not cut the load short: the run settles at `MANIFEST_FILE_READY` with `stopIgnored: true`. Other loaded versions of the package that share the run's tables are rebound just after. An orchestrated run never loads its manifest (the host distributes it), so for it `MANIFEST_FILE_READY` means built.

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

**An invalid declaration fails the package, it does not downgrade it.** The rules below are checked wherever a package is admitted — a publish or PATCH answers 400, and a package **load** fails outright, the same severity a model that does not compile has. So a broken declaration cannot sit in a log while the source quietly rebuilds in full forever: an unknown `refresh=` mode, a malformed key value, `watermark=` or `merge_key=` without `refresh="incremental"`, `merge_key=` without `watermark=`, `refresh="incremental"` without `watermark=`, a watermark or merge key that names no materialized column (or names an aggregate, or a watermark of a type with no ordering), or a `merge_key=` that repeats the watermark. Each line of the failure ends with its route's code: `persist-invalid`, `materialize-invalid` or `persist-view-invalid`. Every rejection is reported at once, so a model with two broken declarations takes one republish to fix. An unrecognized `#@ persist` key (`persist-unknown-key`) fails the load too ([Fails the load, or refused alone](#fails-the-load-or-refused-alone)). A keyless delta stays a warning on the package (code `persist-incremental-no-merge-key`), since it is legal but probably unintended.

**A valid declaration this deployment cannot honour refuses the source alone.** An incremental source on a dialect that cannot derive a bounded range (see below), a `merge_key=` on a dialect without `MERGE`, and an incremental source with any `calculate:` window field are not built: the source is refused as `persist-not-built` (a `#@ persist` source), `materialize-not-built` (a `#(materialize)` source) or `persist-view-not-built` (a persisted view), severity `warn`, a run does not build it, it is answered live with live's rows, and the rest of the package loads. Use `refresh="full"` to build it. An invalid declaration on such a dialect still fails the load.

Details that decide whether a run advances or rebuilds:

- **It is exempt from skip-if-unchanged.** A content address does not move when data does, so an incremental source is instructed on every run and its `covered_through` boundary — not its address — decides whether there is work to do.
- **`forceRefresh` never re-seeds.** It means one thing — build even though the content address is unchanged — and an incremental source is exempt from that carry-forward anyway, so the flag has nothing to say about how one is built. Ask for a full rebuild with `reseed` (`malloy-pub materialize --reseed`, or per source with `BuildInstruction.reseed`). Keeping them separate is what lets a schedule drive deltas at all, since the scheduler forces on every single fire.
- **Two sources that compile to identical SQL share everything.** The content address that keys the boundary is a hash of the connection and the canonical SQL — not the source name, not `name=`, not the model file — so a copy-pasted source body collapses onto one table and one boundary however you name it. If their declarations also differ, neither can ever advance: each refresh finds the other's lineage recorded and rebuilds. Publisher warns when this happens and names both sources (code `persist-incremental-shared-address`), since nothing in the model text shows it. The one exception is twins in two places: when one is built in its own warehouse and the other with `storage=` (or the two in different destinations), each `storage=` twin is addressed with its destination folded in, so they are two tables with two boundaries, and a source built over the colocated twin depends on that twin alone. The colocated twin keeps its address; the stored twin rebuilds once under its new one.
- **Anything unproven falls back to a full rebuild**, which is always correct and merely expensive: no recorded boundary, a boundary describing a different table or watermark, a table whose columns no longer match what the source computes, or `MERGE` asked for on Postgres 14 or older (it requires 15).
- **Postgres, BigQuery, and Snowflake only**, and that is the SOURCE's dialect — the engine that has to express the bounded range. Declaring it elsewhere refuses the source alone rather than silently refreshing it in full, so it never looks like it is advancing when it is not — which does mean a DuckDB source has to say `refresh="full"` to be served from a table at all.
- **`storage=` is supported, and the delta is split across the two engines.** The source warehouse computes the bounded range (the predicate is pushed into its own query, so it never streams rows it will not keep) and the DML lands in the destination, which is where the table is. Everything an author declares means the same thing either way. Two differences worth knowing: a stored table holds exactly the columns the source declares (public and hidden, [above](#where-a-table-is-built-storage)), and every statement of a refresh names a renamed column (the watermark and merge keys included) by the name the build emits it under, as the table stores it, so neither a `rename:` nor an `except:` makes a `storage=` source rebuild in full on every refresh (a colocated one still does, `shape_mismatch`, since its table holds every column the build emits) — though changing which columns the source declares still rebuilds once, since the stored table's shape no longer matches; and a CHAINED stored source — one reading another stored source's table — still rebuilds every refresh, reported under its own reason code, because its parent's delta can restate rows below the child's own frontier where no delta of the child's would revisit them.

### Driving it from a control plane

An orchestrated build works the same way, but the host, not the publisher, decides what gets built, so three things are the host's responsibility. Each of the first two fails _quietly_ if you get it wrong — a full rebuild every run, which succeeds and looks like a refresh.

- **Give an incremental source a STABLE physical table name.** The boundary is recorded against the table it was measured on, so a generational name (a fresh one per run) makes every run re-seed. That fallback is not a bug: the newly named table is empty, so applying a delta to it would drop everything the old one held. It is reported under its own reason code, `table_renamed`, to distinguish it from the source's definition having moved (`lineage_changed`) — which, where packages are immutable, means a refresh instructed through a different version than the one that established the boundary rather than an edited model.
- **Do not expect `forceRefresh` to do anything.** On an orchestrated build it means nothing at all: it exists to defeat skip-if-unchanged, which never runs when the host supplies the instructions. It does not re-seed — nothing does, in any mode.
- **Ask for a rebuild with `reseed`.** Per source on the instruction, or run-wide in the request body; the two are OR-ed. That is the escape hatch for a boundary or a table you no longer trust, and the per-source form means one source can rebuild while the rest advance by delta in the same run.

Each manifest entry reports the coverage its source reached on a `ledger` object, whose `coveredThrough` (with its `coveredThroughType`) makes progress something you read rather than infer: a value that advanced between runs is a delta that applied. A skipped source reports the boundary that stays in force, and a seeded one reports the boundary probed from the table it just wrote, so it means the same thing whatever the step did. The boundary is reported only there, alongside the watermark and source address it was measured under, because the value alone is not comparable across runs — see [Supplying the ledger yourself](#supplying-the-ledger-yourself), which is the same object you send back if you hold the ledger. Alongside it, `refresh` says what the run actually DID to the table — `delta` (advanced in place), `full` (rebuilt), or `none` (nothing to apply, the boundary stands) — because nothing else on the entry distinguishes them: every fallback above answers success and rebuilds, so a source quietly rebuilding on every run looks exactly like one advancing. It is present exactly when a source is refreshed incrementally, so its absence means only that this one is not.

### Retiring tables the plan no longer lists

A host that retires a table when its source leaves a version's build plan (a v1 rollup renamed by the v1 cutover in 0.13.0, or renamed back by a rollback, moves its `sourceEntityId` with no change to the package) needs to know the plan it is reading is complete. A source absent from `sources` and `refusedSources` has left the plan only when the plan's `omittedSources` and `omittedFiles` are both empty.

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
| `persist_annotation_unparseable` | Retained in the API, no longer produced: a `#@` line that does not parse now fails the load (`persist-unparseable`), so no plan is derived to list the source in. |

So: retire an anchor whose source is absent from `sources` and `refusedSources` only when no `omittedSources` entry names the source and no `omittedFiles` entry names the file it is declared in. A plan from a Publisher that predates the fields carries neither; read their absence as "completeness unknown", not as empty. Both fields are on every surface that returns a build plan, `[]` when nothing was lost: `GET …/packages/{pkg}/build-plan`, the dry run (`POST …/packages?dryRun=true`) and the deprecated `Package.buildPlan`. A plan that could not be derived at all (a model that does not compile, an import cycle) still answers 409 `BUILD_PLAN_UNAVAILABLE` rather than a partial plan.

**A mixed fleet mid-rollout** (some workers on a release where the v1 cutover is the only path, some on a release before 0.13.0, which shipped it) is safe to bind one manifest to: each worker matches a manifest entry only against its own plan, by `sourceEntityId` (a current worker matches a `storage=` entry by source name and `sourceEntityId` together, never by name alone). An entry built on the other side matches nothing and is ignored, so its source answers live until this side builds its own table: it costs a rebuild, never an answer from the other side's table.

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

A package or version bound to a host manifest (one with a `manifestLocation`) refuses a local on-demand run with 409 before anything is built, since it serves the host's tables and never this server's (a run that passes `buildInstructions` is still accepted), and the scheduler skips it.

> **Never set `PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER` on a control-plane-driven (orchestrated) worker.** A package serving live under a control plane has no `manifestLocation`, so the per-package skip does not cover it — the flag being off is the primary guard against the standalone scheduler double-driving refresh the control plane already owns.

## Manage it with the CLI

```bash
# View / set / clear a package's schedule (set also sets scope: version). set and clear apply
# only to a package with no published versions. A published version is immutable, so the server
# refuses them with 409 PACKAGE_IS_VERSIONED, and the CLI adds where its schedule belongs:
# "materialization": { "scope": "version", "schedule": "<cron>" } in its publisher.json,
# then bump version and publish the new version. view prints the version it read.
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

Every query result carries `path`, the path report. It says which materialized table answered the query, or that it ran live, and why every other table that could have answered did not. `servedFrom` names the tier that answered (`storage`, or `database` for a table in the source's own database), as `path.servedBy.tier` does.

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

- `tableId` is the table's `sourceEntityId`, the content address the build plan and manifest key on. Sources that build the same SQL and name the same table share one table, so they share one id.
- `kind` is `materialized` (a stored source), `rollup`, `view` (a persisted view's table), `query` (a persisted named query's table) or, on `servedBy`, `live`. A client treats a kind it does not know as a stored table it cannot name.
- `considered` lists the tables on the queried source's chain (the source, what it extends or renames, and the source its query reads from) and the rollups of a source on it: `storage=` tables, then colocated rollups, then colocated tables nearest first. That is not the order they are tried: the router tries a persisted view or query first, then rollups (coarsest first), then the served copy. A table that was not tried because an earlier one answered is left out.
- `reason` is one of `NOT_BUILT` (not bound to this package), `STALE` (past its freshness window), `CONTRACT` (the entry point has a row-level gate, which stored data cannot keep yet), `MISSING_FIELD` (the query does not compile against the stored form, or reaches a field a rollup does not carry), `COUNT_SEMANTICS` (a rollup cannot reproduce an aggregate the query computes), `GRAIN_TOO_COARSE` (a rollup's grain does not hold the query's groups), `RUN_FAILED` (the stored read failed and the freshness fallback answered live) or `WARNED` (the table answers no query at all: its serve verdict is `live`; see below). The list is closed, and codes may be added.
- A `WARNED` skip carries `code`, the serve verdict's reason the table will not serve: `storage_mode` (a `storage=` table while `PERSIST_STORAGE_MODE` is `off` or `write-only`; the query reads the source's own warehouse, where its colocated table may answer), `unreachable` (nothing the package publishes reaches it), `row_level_gate` (every published entry point over it carries a row-level gate, which stored data cannot keep yet; a rollup only, for a stored table, while `PUBLISHER_MATERIALIZE_GATED_SOURCES` is on), `refused` (the package refuses to serve it) or `shadowed` (its storage table is bound to another source of the same name). Where the verdict says a table will never serve, it replaces what one query alone showed, but only where the router never tried the table: a source the bind refuses reads `WARNED` `refused`, not `NOT_BUILT`, and a table every published entry point gates reads `WARNED` `row_level_gate`, not `CONTRACT`. A skip the router reached by trying the table (`MISSING_FIELD`, `GRAIN_TOO_COARSE`, `COUNT_SEMANTICS`) keeps its own reason, and `STALE` and `RUN_FAILED` stay as they are, since only the query knows them. A `storage=` table and its colocated copy share a `tableId` but each reads its own verdict. The set of codes is open.
- `warnings` has one entry per such table, with its `code`, `tableId`, `sourceName` and a sentence for a person. A table whose source the caller cannot list gets no warning, though its skip keeps its code. The package's own warnings and `GET …/packages/{pkg}/status` carry the full message and the fixes.
- `detail` is in the caller's own terms: the field path or aggregate in the query that a rollup cannot reproduce. A compile error against the stored form or a run-time storage error carries no `detail`, since its message can name storage paths and generated sources; it is in the server log.
- `notConsidered` names up to ten tables near the queried source that the query cannot use: on a source that extends it (`below`) or that shares an ancestor with it (`sibling`). Querying that source instead, or moving an annotation, would let one answer. A source the caller cannot list (hidden by the package's `index.malloy` or the file's exports) is never named, unless the caller may see hidden sources.

The report is assembled after the query runs and never changes which table answers. It is absent only if it could not be assembled.

### The served copy (materialization v2, `PUBLISHER_MATERIALIZE`)

With `PUBLISHER_MATERIALIZE` on (the default), a query on a built colocated `#(materialize)` source, or (with `PERSIST_STORAGE_MODE=on` as well) a built `storage=` source, may be answered from the model's **served copy**: your own files, held in memory, with the source's definition replaced by its stored table under the same name. Its trailing `extend { … }` blocks, its annotations and every other line are your text, so an extension, a join to it, a view, a named query or a source in a file that imports it reads the stored table with no change to how it is written. The served copy compiles when the package binds its stored tables; a query it cannot answer is answered live. When it answers, the path report's `servedBy` carries `via: "served_copy"`.

- **`#(materialize)`**: the table is the one the source's generated build source builds, which stores the source's rows with its `where:` applied, so a source of any shape re-roots. A computed dimension is stored under its own name, so the served copy drops its definition and the stored column is the field; the dimension's annotations (`#(doc)`, `#(classification)` and the rest) are written on that field, so they read back as before. A timeframe truncation (`order_date.month`) keeps its timeframe: its definition is rewritten in place to truncate the stored column again, so results, charts and filters see a `month` field as they do live. A dimension the source inherits from its base is simply its stored column. A query that sets its own `timezone:` over a table that stores a time-zone-dependent dimension (one declared on a base; see below) is answered live, since the build computed it in the default timezone. A source whose base declares a measure, view or join it inherits is left as written (its table does not carry them) and warned about.
- **`#@ persist storage=`**: only a query-shaped source (`x -> { … }`) is re-rooted, since its table is exactly its query's output; a timeframe column in that output keeps its timeframe too. A source declared by refinement alone stores its root's rows and is left as written; it still reads a re-rooted base by name.
- A reader that opts out with `#@ -persist`, or any source built on one, is never answered from the served copy.
- **Table names.** A `#(materialize)` source's `name=` names its table and places it, as on `#@ persist` ([Whose table it is](#whose-table-it-is)): an unversioned or `scope: "package"` run writes the table at exactly that name, schema or dataset included, checked and marked as this package's; a `scope: "version"` run takes that name, schema or dataset included, as the start of the table's name, lowercased, and appends the version and hash (`summary_t__v1_2_0__p<8 hex>`; a `name=` the readable name would otherwise rewrite fails the load, [readable table names](#readable-table-names)); a host-orchestrated run writes the physical name the host assigns, in that name's container. No view is made at `name=` in any of these. Without `name=`, its table takes its [readable name](#readable-table-names) in Publisher's own builds. If another source of the package writes the same table in the same destination (the `name=`s compared as the run writes them, case-folded on a destination that folds case: DuckDB and every `storage=` destination, Trino, Presto, Databricks, SQL Server; MySQL is compared as written, since its folding depends on `lower_case_table_names`), the publish and the load fail (`materialization-table-collision`, saying to give each source a distinct `name=`; with `PERSIST_COLLISION_ENFORCE=false` the `#(materialize)` source is refused alone as `materialize-not-built` and serves live). Only two equal author `name=`s can meet: a defaulted name is a readable name, hashed over the source's model and name, so it never meets another source's table, and in a published version of a `scope: "version"` package every name, `name=` included, is such a readable name (an unversioned package writes a `name=` as given whatever its scope). A source whose table name changes (a `name=` added, removed or changed) is rebuilt under the new name by the next self-instructed run, never carried forward at the old one, which goes by [the rename rule](#the-rename-rule): reclaimed when it was Publisher's name, kept and listed when it was an author's exact `name=`. A source that inherits `#@ persist` from the source it extends or renames is already that source's table, so `#(materialize)` on it is not built either, with a warning naming the source it inherits from.
- **Colocated.** A `#(materialize)` source without `storage=` is built into its own warehouse: at its `name=` when it has one (a table there that this package did not write, or a view Publisher did not make, fails that source's build before anything is written, as on `#@ persist`), otherwise under the source's name and its package's (or version's) suffix, a name no table of yours carries. On BigQuery a table needs a dataset, so a colocated `#(materialize)` there with no dataset-qualified `name=` is not built (a `materialize-not-built` warning says to give it one, reason `needs_qualified_name`) and serves live; a colocated `#@ persist` source, persisted view or query, and a rollup with no container are refused at plan the same way, each under its own `*-not-built` code, rather than failing at build. The served copy reads it on the source's own connection, exactly as it reads a `storage=` table in its destination, so everything on this list applies to it: stored computed dimensions and timeframes, `inline=` joins and pair tables, private and internal fields, `where:` terms re-applied at read, extension tables, gates. It needs no storage tier: `PERSIST_STORAGE_MODE` decides only `storage=` tables. Since the table and the source share a connection, a join to another source on that connection stays one query, so a source reached only through a join, or joined beside a warehouse table, reads its table too. Its columns are the plan's, typed as Malloy declares them, plus each zoned timestamp its build read back from the table it wrote, so a `timestamptz` column is declared as one, as live types it and as on a `storage=` table, and a driver-specific type live types as `sql native` (a UUID) is left out, so only a query that reads it is answered live. A nest outside DuckDB, where it is stored as JSON, leaves that source served live as a whole, as a struct column or an undeclarable inlined column does on either tier (see "A column the served copy cannot carry" under [stored column types](#stored-column-types-follow-the-model-not-the-warehouse)), and so does a `timestamptz` in a table built before its build read its columns back, whose columns come from the plan. An answer that read only colocated tables reports `servedFrom: "database"` (it read the source's own database, as a colocated `#@ persist` answer does); its path report names the table, with `via: "served_copy"` and `tier: "database"`. A v1 `#@ persist` without `storage=` is built in its own warehouse too, and since the v1 cutover the served copy reads its table the same way. A `#(materialize)` source's serve verdict follows the served copy: `serves` where some model's served copy reads its table.
- **Private and internal fields.** A `#(materialize)` source's own `private` and `internal` dimensions, and the `internal` ones it inherits, are stored under their own names: the build reads a copy of the source's file with its own access modifiers lifted (a query cannot select a hidden field), and the served copy keeps your modifiers, so a caller can no more select a hidden column from the stored table than live. A hidden dimension your source's own `extend` defines keeps that definition and is computed at read from the stored columns it reads; one an `include { private: … }` on the source's head hides, or one it inherits, is the stored column, hidden again as you wrote it. A hidden timeframe field (`internal dimension: order_month is ordered_at.month`) is stored too, and keeps its timeframe in the served copy exactly as a public one does, redefined under your modifier; the stored column it reads is `private` there, so no caller reaches it by any path. A field private on a source it extends, which your source cannot read, and an `except:`-ed field are never stored. A served source whose fields are not exactly as hidden as yours is not answered from the copy (`served-copy-not-served`, `fields_not_carried`). A rolling window over a hidden dimension your source's own `extend` defines (`internal dimension: t2 is ts; where: t2 >= now - 60 days`) is applied at read by that `extend`, as you wrote it; a served source whose compiled filters lack it is not answered from the copy (`read_terms_not_kept`). A pair table beside a column an `include` keeps hidden under its own name is served too; an author `rename:` of a stored column behind that `include` is not (Malloy refuses a `rename:` right after an `include`), and that source answers live (`definition_not_parsed`). With the gated switch on, a gated source's hidden columns are stored in the destination too; see below.
- **Rolling windows.** A `#(materialize)` source whose `where:` keeps the last stretch of time, one comparison of a stored timestamp with `now` (`ts >= now - 30 days`, optionally `now` alone or offset by a whole number of one unit), builds with the window as of the build and is re-applied at every read at the read's own clock, a window inherited from the base included, so a read between refreshes trims to its own cut-off. An incremental refresh appends above the watermark and then deletes the rows below the new cut-off, less a day's margin, in the same transaction. A window that keeps rows *older* than a moving cut-off (`ts < now - 30 days`) is left out of the build, so the table holds every row there is when it is built, and is re-applied at every read, own or inherited from a base in the same file; with no refresh interval to bound it, that is the widest set any later read can need, and it needs no scheduler. A two-sided window, `ts >= now - 60 days and ts < now - 30 days` (or the same as two `where:` terms, or the range `ts ? now - 60 days to now - 30 days`), builds its lower bound and leaves its upper bound out; an incremental refresh deletes below the lower bound only, and an upper bound alone deletes nothing. Any other `where:` that reads `now` (an `or`, a bound `and`ed with another condition), directly or through a dimension or a join that reads it (`where: is_recent`), is not a shape Publisher can store, nor is one `where:` term that reads both `now` and a given, nor an upper bound written in another file than the source's model, which the build cannot leave out, nor one inherited from a base that a join named in `inline=` also reads, whose stored columns would hold the rows it leaves out: each leaves the source unbuilt, and it serves live. A query that sets a `timezone:` is answered live from a source with a window, since Malloy takes the offset in that zone's wall time and across a daylight-saving change the read needs up to an hour the table does not hold. A dimension whose value depends on the current time, directly or through another dimension or a join, is never stored; the served copy computes it at read. An `inline=` join whose joined rows read `now` (its own `where:` or `on`, or the query of a query-shaped source it joins) leaves the source unbuilt too, since its stored columns would hold the rows as of the build. A dimension that reads such a join, like one that reads `now` itself, is computed at read only when the materialized source declares it; declared on a base the source extends, it leaves the source unbuilt (`materialize-not-built`, "the dimension … reads the current time and is not declared on this source"), so declare it on the materialized source.
- **Dimensions that depend on the caller.** A `#(materialize)` dimension whose value depends on what the caller sets is not stored: one that reads a given (directly or through another dimension), the current time, or the query's time zone (`hour(ts)`, `ts.month`, `ts::date`; a `date`'s own truncations are civil and do not). The table stores its inputs, and the served copy keeps the dimension's definition and computes it at read, so each caller gets its own value and no table holds one caller's. Publisher finds these by compiling small probe queries against your model (never running them) beside its own reading of the compiled model; either one marking a dimension keeps it out. Where the dimension is declared matters, because the served copy computes it from a definition it keeps, and it keeps only the materialized source's own declaration. A dimension that reads a given or the current time and is declared on a base (in the same file or another) refuses the source (a `materialize-not-built` warning naming the dimension and saying to declare it on the materialized source itself; it serves live), and a time-zone-dependent one declared there is stored as built, so a query over that table that sets a time zone is answered live, whether the query writes `timezone:` itself or runs on a source that declares one (`ny is orders extend { timezone: 'America/New_York' }`, or anything on its chain). A source whose time-zone-dependent dimensions are all ones the served copy keeps answers a `timezone:` query from its table, unless its rows depend on the zone: a `where:` with a timestamp literal (`where: ts > @2026-03-08 09:00`), its own or one a base it is built on writes, filters different rows in every zone, and the table holds the rows of the build's, so a `timezone:` query over it is answered live. An `inline=` join through which a given is read is refused the same way, since its stored columns would hold one caller's values.
- **A base scoped by a given.** The common governed shape is a base that keeps one tenant's rows (`source: tenant_orders is orders extend { where: org_id = $ORG_ID }`) with extensions over it. Only a materialized source's **own** given-reading `where:` is left out of the build and applied again at read; the served copy keeps only that source's declaration. A given-reading `where:` the source inherits from a base, wherever the base is declared (the same file or another, one or several `extend` or rename hops up, or a materialized parent), refuses the source at plan: it is not built and is answered live, under the term, with a `materialize-not-built` warning that says to move the given-reading `where:` onto the materialized source itself. To serve the shape from a table, write the term on the source you materialize: `source: tenant_orders is orders extend { where: org_id = $ORG_ID }` with `#(materialize)` on it, and extensions over `tenant_orders` that declare none read its table, its term applied for each caller. An extension that declares its own `#(materialize)` over such a parent is refused the same way, and its queries are answered from the parent's table. This is a speed limit, not a correctness one: the refused source answers exactly what it answers live. A term that reads its given through a dimension (`where: is_mine`) is refused for the dimension, as above. The source's own term must also read only fields its table stores, its own dimensions: one over a join's field or a measure cannot be applied again over the table and refuses the source (reason `given_where_over_unstored_field`), as a rollup's term over a field its grain drops does. A served source whose compiled filters lost a given-reading term is still never answered from the copy (`served-copy-not-served`, `read_terms_not_kept`).
- **Rolling windows.** A `#(materialize)` source whose `where:` keeps the last stretch of time, one comparison of a stored timestamp with `now` (`ts >= now - 30 days`, optionally `now` alone or offset by a whole number of one unit), builds with the window as of the build and is re-applied at every read at the read's own clock, a window inherited from the base included, so a read between refreshes trims to its own cut-off. An incremental refresh appends above the watermark and then deletes the rows below the new cut-off, less a day's margin, in the same transaction. A window that keeps rows *older* than a moving cut-off (`ts < now - 30 days`) would have to be built as of the end of the refresh interval, which a build on demand does not know, and any other `where:` that reads `now`, directly or through a dimension or a join that reads it (`where: is_recent`), is not a shape Publisher can store, nor is one `where:` term that reads both `now` and a given: each leaves the source unbuilt, and it serves live. A query that sets a `timezone:` is answered live from a source with a window, since Malloy takes the offset in that zone's wall time and across a daylight-saving change the read needs up to an hour the table does not hold. A dimension whose value depends on the current time, directly or through another dimension or a join, is never stored; the served copy computes it at read.
- **Extension tables.** An extension that declares its own `#(materialize)` has a table of its own (one that declares none reads its parent's). When the two are on one tier (both colocated, or both in one `storage=` destination), carry no incremental key, its build reads its parent's table rather than recomputing the chain, and runs after the parent's (a colocated one reads it in the warehouse). The parent may be declared in another file: when the extension's file imports the parent's file directly, and every source between them is declared in one of the two files, the extension's build imports a copy of the parent's file with the parent re-rooted on its table, and Publisher compiles the extension both ways and keeps that build only when it stores the same columns, with the same types, as the build over the chain. A chain that passes through a third file, an import it cannot rewrite, or a build that does not prove the same columns builds over the chain from the warehouse, with the same rows. Across files, as in one, a windowed extension never reads a parent's table of unknown or stale age: the run rebuilds the parent first. A `storage=` extension's build runs in the destination over the parent's stored table and reads nothing from the warehouse, so a strict build that refuses the warehouse (`warehouseReach: refuse`) builds it, and its entry says `upstreamReuse: reused`; under `strictUpstreams`, a parent with no table in that destination refuses it. A join the parent inlines (`inline=`) is read in that build from the parent's stored `j__<field>` columns, as the served copy reads it, so an extension that reads `j.<field>` still builds over the parent's table. One whose build over the parent does not compile (a field it reads that the parent's table does not carry, or, for `storage=`, a join to a source the destination does not hold, such as one the parent does not inline) builds over the chain from the warehouse, with the same rows. That fallback is per extension: every other extension in the model still builds over its parent's table. A source elsewhere in the file that reads the parent and would not compile over the parent's table (a sibling extension that falls back itself, say) is left out of the extension's build as an empty source, so it cannot cost the extension its parent's table; if the extension reads that source, its build does not compile and it too builds over the chain from the warehouse. An extension of a source that inlines a join_many always builds over the chain, since the parent's pair table holds each parent once per child. Its served copy keeps the `extend { … }` blocks of every source it is built on in its own file, ahead of its own, so a measure, join or view a parent declares is still the extension's; a stored computed dimension's definition is dropped from them as from its own. A `rename:` in those blocks reads its stored column by the field's new name; one with an access modifier (`internal rename:`) keeps the source from being served from its table. A private or internal field stays exactly as hidden on the extension as on the source you wrote, whichever source declared or hid it. A query on the extension reads its own table, and the path report lists its table and its parents' on the chain, nearest first.
- **Joins are references by default.** A `#(materialize)` source's table holds its own rows only; its joins stay as you wrote them, and read the joined source (from its own table, when that source is materialized) at query time. A dimension whose value reads through a join is computed at read, not stored.
- **`inline=["j"]`** on a join_one stores the dimensions that read through `j` (one hop) and the joined source's own dimensions as `j__<field>` columns, for tools that read the table outside Malloy. The served copy answers the join from those columns, with no join at read: it replaces the join with a record of the stored fields under the join's name, so `j.<field>` reads the table, an unmatched row reads nulls as the live LEFT JOIN does, and on the storage tier a query through it no longer crosses connections. What the columns cannot answer is answered live: a measure, view or join of the joined source (`MISSING_FIELD` in the path report), an aggregate at the join's own grain such as `j.x.sum()`, `j.count()` or a measure that reads one, in `having:` or filtered too (`COUNT_SEMANTICS`; a record would compute it once per row of the source, where the join computes it once per joined row), a `j.*` selection, and a query that sets a `timezone:`. Row-grain aggregates such as `sum(j.x)` and `count(j.k)` read the table. Each joined field keeps its annotations and classification on the stored copy, so a classified joined source stays served, but a query that returns an annotated field of `j` as a column (`group_by: j.name`) is answered live (`MISSING_FIELD`), so the result carries the field's `#(doc)`, `#(classification)` and render tags; filtering on it, aggregating it, or returning it under another name (`n is j.name`) reads the table. If the source's own text reads the join past what the table stores (a join of the joined source), the served copy keeps the join as written and raises a `served-copy-not-served` warning naming the join and why. A join you hide (`internal join_one:` or `private join_one:`) is inlined too: the record keeps the join's modifier and the stored columns behind it are `private`, so a caller reads `j.<field>` exactly where live lets it (an `internal` join through the caller's own `extend`, a `private` one never), while your own dimensions, measures, views and gates over it read the table. A hidden join_many is inlined the same way (see the pair table below). The join must be keyed by one equality on a column of a declared source on the same connection, and a build first checks that key is unique in the joined source: a key held twice fails the build, naming the join, rather than storing a row once per match. When an auto-run's build reads the joined source's `#@ persist` table, the check reads that table too. An auto-run skips the check only when its answer cannot have changed: the joined source is itself a `#@ persist` table that the build reads, that table was not written since a check over it passed in the run before, every other run of the package had finished before that run began, and the check's SQL is the same. Otherwise it runs, on every build: a joined warehouse table's rows change without its address changing, and so do an incremental table's. A control plane's builds (with `buildInstructions`) always run it. Each run records the checks in its `metadata.inlineUniquenessChecks`: the passes the next run may rely on, and whether each check ran or was skipped. Deleting a run's record forgets the passes every other run of the package records, so the next run checks again: the record is part of what shows the table was not written since. `inline=` on a join_cross is refused until a later release. An `inline=` that is not a list of names, or that names something that is not a join of the source, fails the publish and the load (`materialize-invalid`). A refused `inline=` (a join_cross, a join keyed through another join, a join_one with no single key, a join defined in place, one on another connection, a join through which a given is read, or a `j__<field>` column that clashes with one of the source's own) is a `materialize-not-built` package warning naming why, and the source serves live. A join through which a given is read is one whose joined source's `where:`, `on`, fields or own joins read a given (`where: region = $REGION`): its stored columns would hold one caller's rows, so the source is refused ("inline= names `j`, through which a given is read") and answers live for each caller, while the model's other sources build and serve; reference the join instead of inlining it. More generally, a source whose generated build text does not compile is refused on its own (`materialize-not-built`, "its generated build did not compile", a Publisher bug worth reporting) and the rest of the model builds.
- **`inline=["j"]` on a join_many** stores a **pair table**: one row per row of live's LEFT JOIN through the join (each `j__<field>` column beside the source's own), so a parent with no child keeps one row with null child columns. One flag is written at build: `__parent_row` is true on exactly one row per parent. A tool reading the table outside Malloy counts parents with `WHERE __parent_row` and pairs with `count(*)`. The served copy reads the table as the source itself, with no join at read: `j` is a record of the child columns (`j.miles` reads the column on the same row), each measure over the source's own fields aggregates only the `__parent_row` rows, and an aggregate through the join reads every row, the null-extended row of a parent with no child included. Since Malloy 0.0.438 ([malloydata/malloy#3208](https://github.com/malloydata/malloy/pull/3208)) live's `count()`, `sum` and `avg` through the join skip that row (they read only the rows where `j`'s key is not null). So the served copy re-declares each measure that counts through the join (`seg_n is j.count()`, a filtered count, a ratio over one), or sums or averages a value that is not a stored column of `j` (`j.sum(1)`, `j.sum(j.miles ?? 0)`, `j.avg(1)`, a column of the source summed through the join, a stored computed field of `j`), over the rows where `j`'s stored key is not null, and serves it; a view or query that reads such a measure is served too. A sum or average of a stored column of `j` (null on that row) is served as written, and a `min`, `max` or distinct count is served, as live reads that row too. Every `count()`, `sum` and `avg` through the join is served only when `j`'s key is unique and never null on a row of the joined source: a child with a NULL key is one live skips and the table holds, and two equal children of one key are stored as one row. Each auto-run build checks that key over the joined source once the table is written, without failing the build, and records the answer beside the table; where the check failed, or no record is held (a table a host's manifest binds), those aggregates are answered live (`COUNT_SEMANTICS`). Still answered live (materialization v2 known limitation 40): a count, or a sum or average of such a value, written in the query or in a view's own `aggregate:` (`aggregate: n is j.count()`) rather than in a measure, and a measure that pairs one with a `min` or `max` of a value not null on that row. A many-to-many join_many is served the same way. Each query is checked before the table answers it, and one the flat table cannot reproduce exactly is answered live (path report reason `COUNT_SEMANTICS`): an aggregate over the source's own fields written in the query rather than declared as a measure, a parent measure grouped or filtered by a field of `j`, an aggregate that reads both sides at once or goes through another join, a window or `all()`/`exclude()`, a `select:`, and a join to the source from another source. The joined source's own measures and views are not on the record, so a query that reads one (`j.seg_miles`) is answered live (`MISSING_FIELD`), as is a query that returns a classified or annotated field of `j` as a column, exactly as for an inlined join_one (a record's fields carry no annotations; each child column keeps its field's annotations and class, an inferred class included, by the same rule as a join_one's), and a source whose own text reads one through the join is not served from the table. A dimension of the source that reads through the join_many is computed at read. A query that reads no field of `j` (no field, aggregate, filter, group-by or nest through it) is not answered from the pair table, which would scan every pair row to count each parent once (about 2 to 5.5 times live up to five children a parent): it goes to the next table that can answer it, or live, and the path report shows the pair table skipped with `COUNT_SEMANTICS`. A row-level gate that reads `j` counts as a filter on `j`, so parent measures under it are answered live. Child and mixed queries read the table faster than live (about 0.2 to 0.4 times, up to five children a parent). A join_many you hide (`internal join_many:` or `private join_many:`) is stored the same way: the record keeps the join's modifier and every stored column behind it is `private`, so a caller reads `j.<field>` exactly where live lets it (an `internal` join through the caller's own `extend`, a `private` one never) and the stored columns never, while your own dimensions, measures, views and gates over it read the table; a caller's own `extend` or `include` over a pair source is answered live, as for a public join_many. The pair table needs a declared primary key on every side that is stored, and a joined source with no joins of its own other than the path below it; a path through a join the joined source hides is refused, since nothing reads through it. Anything else is a `materialize-not-built` warning.
- **A path of join_manys**, `inline=["items.options"]`, stores one pair table with one row per row of live's LEFT JOIN through every join of the path (`items__options__<field>` beside `items__<field>` and the source's own), and one more flag per level above the last: `__child_row` is true on one row per order and item (an order with no item included). `items.options.kind` reads the column on the same row. An aggregate at the last join (`items.options.extra.sum()`, a measure over it) is answered from the table, grouped or filtered by any level, under the same rule as one hop: a measure that counts there (`items.options.count()`), or at a flagged middle level (`items.count()`), is re-declared over the rows where that level's stored key is not null, since the flag also marks an order with no item, and a count written in the query is answered live. A measure over `items` (`items.price.sum()` declared on the source) is answered from the table when the query reads nothing of `items.options`. Once it does, live aggregates each item once by its key (and an order with no item as none). The table still answers a sum or an average of a stored column of `items` (`items.price.sum()`, `items.price.avg()`), and a measure re-declared over `items`' key (`items.count()`, an average of a computed field of `items`), beside `items.options`, when each item has exactly one order: every join down to `items` keyed on the level above's primary key (`join_many: items on items.order_id = id`, with `primary_key: id`), which the build checks unique and never null, so `__child_row` marks each item once whichever order groups it. A count or an aggregate of a computed field of `items` written in the query, and a level whose join is keyed on anything else (a dimension, or another source's field, where an item may meet several orders), are answered live beside the deeper level. As at one hop, an aggregate written in the query above the last join, or a measure grouped or filtered by a level below its own, is answered live. Naming `"items"` too adds nothing: the path's table answers it. A path through a join_one (`planes.maintenance`) is refused: live counts each maintenance row once however many flights share the plane, which no stored row can reproduce.
- **More than one join_many**, `inline=["items", "payments"]`, stores one table in blocks, one per join_many: each block is that join's own pair table, marked by `__pair_lane` (0, 1, …), with the other blocks' columns null on its rows, so a tool reading the table outside Malloy filters on `__pair_lane` first. Each query is answered from the one block it reads; a query that reads both (`items.count()` beside `payments.count()`, or a filter on one with measures of the other) is answered live, since live counts each over their cross product. A path's table and a table of several blocks refresh incrementally as a one-join table does (below), replacing whole parents.
- Every pair table is built only when the source's primary key, and on a path the key of every level between it and the last join, are unique and never null: each build checks them first (a full build and an incremental refresh alike), as it checks an inlined join_one's key, and a repeated or null key refuses the build (a level's row flag marks one row per key, where live counts each row), so the source answers live. The check is one `GROUP BY` of the source on its primary key, and of each middle level on its key, per build. The last join's key is checked by one more `GROUP BY` of its joined source, after the build and without failing it (above): a count, sum or average through the last join is served only where it passed. The source's own check reads the rows the build stores: a given's term or a rolling window's upper bound that the build leaves out is left out of the check too, so a key held by two tenants, or repeated past the window's cut-off, refuses the build. It reads the source itself, so it is never skipped as an unchanged joined table's check can be. Another block's number columns are stored as nulls of a type the union does not widen, so an integer or decimal column keeps its type.
- A row-level gate that reads a join_many the pair table does not inline, at any step of its path (one reached through a join_one included), or another block's join, is answered live: grafted over the pair rows, it would repeat each row once per match. A query that filters, groups or computes a filtered measure through such a join (`where: hub.gates.gate = 'A1'`, `group_by: hub.gates.gate`, or a dimension that reads it) is answered live for the same reason (`COUNT_SEMANTICS`), on a one-hop table as on a path or a table of blocks.
- **An incremental pair table** names a watermark on both sides of the join: `#(materialize) storage="lake" inline=["segments"] refresh="incremental" watermark=["updated_at", "segments.loaded_at"]`. The table stores one more column, `__pair_watermark`: per parent, the greatest of those fields over the parent and every child it has, the same value on every row of one parent. Each refresh replaces every row of each parent whose `__pair_watermark` is at or past where the last refresh reached. The range is open above, unlike any other incremental source's: one child stamped later than the refresh's clock holds its parent's watermark there, so a range that stopped at the clock would leave that parent's own changes behind until the clock caught up; instead the parent is read again on each refresh until then, which replacing a parent whole makes harmless. The refresh reads those parents' rows from the source once, deletes every stored row with their primary keys, writes the new rows, applies the rolling window's delete if there is one, and checks that every parent in the table has exactly one `__parent_row`, all in one DuckDB transaction in the destination. So a child added to an old parent (a late-arriving child), a childless parent gaining its first child, a child restated in place, a restated parent, and a new parent with or without children all reach the table, and the table after each refresh holds what a full rebuild would. What it cannot see is a change that moves no watermark field: a child deleted, or moved to another parent, reaches the table only when its old parent's own watermark field moves too, or at the next full rebuild (`--reseed`); the same rule as any incremental source, whose rows must only arrive or move their watermark forward. Likewise a parent whose watermark fields are null on its own row and on every child's has a null `__pair_watermark`, which no refresh reads: it is stored by a full build only. If the check fails, the delta is rolled back and the table is rebuilt in full. The source warehouse still computes the whole pair table on each refresh (the watermark is a window over each parent's rows, so the range cannot be pushed into the scan): a refresh saves what crosses to the destination and what it writes there, not the warehouse's scan. As for any incremental source, the origin must be Postgres, Snowflake or BigQuery. The refresh is rebuilt in full instead, with a `materialize-pair-full-refresh` warning naming why, and the table still built and served, when the watermark names only the source's own fields (a child added to a stored parent would never be seen), only the joined source's (a new childless parent would not be), a field that is not stored, fields of different types, when `merge_key=` names anything but the source's primary key (the unit is the parent), when the rolling window reads a field of the join, or when the table is colocated in a warehouse other than DuckDB (the delta is applied only in DuckDB: give it `storage=` to refresh it in place). A list `watermark=` on any other source fails the publish and the load (`materialize-invalid`).
- **An incremental path or table of several blocks** follows the same rule, with a watermark field at every level: `inline=["items.options"] refresh="incremental" watermark=["updated_at", "items.loaded_at", "items.options.loaded_at"]`, and for blocks a field of every level of every block (`inline=["items.options", "payments"]` adds `"payments.paid_at"`), since a row added at a level with nothing below it moves its parent only through a field of its own level; a level with no field rebuilds in full with `materialize-pair-full-refresh`. Each level's row flag is one row per distinct keys from the parent down, so replacing whole parents keeps it exact, and the refresh checks each one as it checks `__parent_row`. A table of several blocks stores one more column, `__pair_watermark_block`, each block's own greatest per parent; `__pair_watermark` is the greatest across blocks, the same on every row of one parent in every block, so a refresh replaces a parent in all of them at once, and checks every flag within each block and that each parent still has rows in every block. A failed check rolls back and rebuilds in full, as at one join.
- **A join the destination cannot reach.** A stored source keeps its joins, but a joined source that is not stored in the same destination reads another connection, which Malloy cannot join. The served copy leaves that join out (with what reads it), so the queries that do not use it still read the table; a query that does is answered live, and its path report skips the table with `CROSS_CONNECTION`. The package warning names the join and the three fixes: inline it, materialize the joined source into the same destination, or keep the source colocated.
- **Sources from different origin connections, joined.** A file whose own join crosses two origin connections is invalid Malloy, and Publisher serves it no differently: the package fails to load with Malloy's `join-connection-mismatch` diagnostic (a `loadErrors` entry on `/status`, and the publish response), and the rest of its environment loads. Materializing both sides into one destination does not change that. Put the joined sources on one connection.

**Gated sources (`PUBLISHER_MATERIALIZE_GATED_SOURCES`, off by default).** A source whose gate is proven to be its entry point's own row filter or lock (an `#(access_filter)`, an `#(authorize)` lock, or both, including a lock it inherits from a source it derives from or extends; not a row filter it inherits, not a gate reached through a join, and not one whose build would substitute a given) is built and served past that gate with this switch off, on either tier: a v1 `#@ persist` or a `#(materialize)`, colocated or `storage=` (G3). Colocated and `storage=` behave the same: the table holds every caller's rows, ungated, and the served copy grafts and proves the gate on every read exactly as on live, so each caller gets live's rows, or live's denial. A colocated table stays in the author's warehouse. A `storage=` table puts every row of the gated source, ungated, at rest in the destination, where anyone with direct access to the destination can read it; this is a maintainer-approved exception made ahead of the security review of the managed tier, for consistency between the tiers, and that review covers it. Without this switch, a query whose entry point carries a row-level gate (`#(access_filter)`, or an `#(authorize)`) is answered from a stored table only when every table the answer reads is such a table: a colocated one, or a `storage=` one that plan admitted past the own gate of a source stored in it. The table counts, not the name: a v1 extension under its own gate shares its base's table, so that table counts, and so does any other source that shares a gate-kept source's table (same SQL, same content address). A gated entry point joining such an ungated sharer is then served from the table. Nothing leaks: the rows are the same, and the entry point's own gate is grafted and proven on the answer as on live. An answer that would also read another `storage=` table (an ungated source's, through a join, say) is answered live. With this switch on as well, such a query may be answered from the served copy whatever tables it reads, the gate grafted and proven on the served compile exactly as on live; a colocated rollup, v1 `#@ preaggregate` or `#(preaggregate)`, is read past the gate the same way when every gate reads its grain dimensions through no join ([preaggregation.md](preaggregation.md#preaggregate-materialization-v2-behind-publisher_materialize)), as is a persisted view; and a query whose own join reaches a gated source stays blocked (that gate is never grafted on a routed compile), which is answered live. The switch also decides what else is stored: on, a colocated `#(preaggregate)` rollup over a gated source is built and read past the gate; off, one is built and read past it only under G3, when every gate of its base is the base's own and reads grain dimensions only (maintainer decision ahead of the security review, which covers it), and any other is not planned (`preaggregate-not-built`, reason `access_filter`). A gate reached only through a join, a row filter the source inherits, and a gated source whose build would substitute a given are refused on every tier, switch on or off (`persist-not-built` or `materialize-not-built`, reason `access_filter` for a row filter, `authorize` for a lock), and a `storage=` rollup over a gated source is neither built nor read until the review. The verdict for a stored table reached only through gated entry points reads `serves` rather than `WARNED` `row_level_gate` while the switch is on, and for a table admitted past its own gate either way. Since a `#(materialize)` source stores its private and internal fields, a gated `storage=` one puts its hidden columns (its `org_id`, say) in the destination as well. A gate is grafted as a caller-level `extend`, which reads an `internal` field and never a `private` one, so a gate over a private column denies every caller, live and served alike: give the gate's column `internal`. `internal` keeps it out of a plain `select:`, but a caller's own `extend` or `include { public: org_id }` still reads it, live and served alike, only in the rows the gate admits; hide a value no caller may see behind `private`, in a field the gate does not read. A caller's `rename:` or `except:` of the gate's column, a raw `connection.table(...)` or `.sql(...)` read, and a caller's own join into the gated source are answered exactly as live. A served-copy answer to a gated query is denied (`403`) if governing it on the served copy finds no gate there, and its access record's `path.servedBy` carries `via: "served_copy"`. Publisher does not authenticate callers: the gate holds only behind the gateway that sets the givens from a verified identity ([operating model](security-posture.md#operating-model-a-microservice-behind-your-gateway)), and it holds only on reads through Publisher, never on a direct read of the destination. **Never configure a destination's catalog, schema, data path or DuckDB file as an environment connection that models can name.** Stored tables are named predictably, so through such a connection any package could read another package's gated rows by name. Publisher logs a warning at environment load when a DuckLake connection names a destination's catalog and metadata schema, or a data path that overlaps the destination's. The check is not exhaustive ([security-posture.md](security-posture.md#row-level-access-rows-are-protected-the-schema-is-not)). The switch is off until a security review of storing gated sources' columns in the managed tier.

A served copy that does not compile, or a source it cannot re-root, is a `served-copy-not-served` package warning, and those sources serve as they did without it. The served copy is compiled once per model, so a failure is traced to the source whose re-root it is in, and that source alone steps back (`reroot_not_compiled`: it answers live, after first keeping its inlined join_ones as written, or, for an extension table, its own blocks only); the model's other sources keep their tables. Only a failure outside every re-root costs the whole copy. A served copy whose re-root moves a join across connections, and that cannot leave the join out, compiles once both sides are built into one destination, and reports that wait under the same code until then. With `PUBLISHER_MATERIALIZE=false`, nothing here runs.

To move a v1 `#@ persist` source to `#(materialize)`, and for what each v1 form becomes, see [materialization-migration.md](materialization-migration.md).

### The `name=` view (materialization v2, `PUBLISHER_MATERIALIZE`)

Publisher makes no `name=` view. A `#(materialize)` source's `name=` sets the start of its table's name, schema or dataset included: the whole name where the table can sit at a stable name (an unversioned or `scope: "package"` run), and the prefix a `scope: "version"` run appends its version and hash to ([Table names](#the-served-copy-materialization-v2-publisher_materialize)). A run that names the table by its generation (a `scope: "version"` run, or a host's instructions) gets no alias view at `name=`, and no build, commit or reclaim creates, repoints or restores one.

Earlier releases made a view at a `#(materialize)` source's `name=` when a run named its table by generation (a host's instructions, or a `scope: version` run). Such a view may still be in a warehouse or a storage destination, carrying the comment `malloy-publisher:name-view owner=<environment>/<package> target=<table>` (each value URI-encoded), and its run's metadata may still list it under `nameViews`; runs no longer record that key. What Publisher still does with one, and only with one marked as this package's:

- **Reclaim.** Deleting a run with its tables (`dropTables=true`), or reclaiming an archived or deleted version's tables, drops each view the package's runs recorded over a table being dropped, before the table, while the view still carries this package's mark and the warehouse confirms it reads that table and nothing else: on Postgres from the view's recorded dependencies, on SQL Server from its expression dependencies, elsewhere from its definition (an unqualified name read in the view's own schema). A view repointed to another table (`CREATE OR REPLACE VIEW` keeps the comment), or one that also joins or filters on another relation, is left, and so is one whose reads cannot be proven.
- **A table at the name.** A `name=` build (`#@ persist name=` or `#(materialize) name=`; standalone, unversioned or `scope: "package"`) whose name holds this package's own marked view, still reading the table its mark names, replaces the view with its table: on DuckDB and Postgres the view is dropped in the swap's own transaction (in the transaction that creates the table, for a `storage=` build), so a build that fails leaves the view; elsewhere it is dropped just before the build. On Postgres a reader's own view over it fails the build with an error saying so, and both are left.
- **A table a view replaced.** Deleting a run whose table such a view replaced skips the table that is no longer there rather than counting it as a failed drop.

A view without the mark, or marked as another package's, is never changed or dropped. A view no reclaim reaches (its package deleted without `dropTables=true`, or its table still named by a committed run) stays where it is: find it by its comment mark and drop it by hand once nothing reads it. A table an interrupted replacement left under `<table>_o<id>` is dropped with its base table, as before.

### Persisted views (materialization v2, `PUBLISHER_MATERIALIZE`)

With `PUBLISHER_MATERIALIZE` on (the default), `#@ persist` on a view stores the view's result, and that table answers a query only when the query calls the view exactly, or starts from it:

```malloy
source: flights is duckdb.table('flights') extend {
  measure: flight_count is count()
  #@ persist name="flights_by_carrier"
  view: by_carrier is { group_by: carrier; aggregate: flight_count }
}
```

- **What it answers.** `run: flights -> by_carrier`, and `run: flights -> by_carrier -> { … }`, whose later stages read the stored result. Not a refinement (`flights -> by_carrier + { limit: 3 }`, skipped `MISSING_FIELD`), a nest of the view, a hand-written query with the same SQL, or the view called on a source that extends `flights` (its own `where:` or gates could differ): those are answered as they would be without the annotation. So is a query text that, anywhere outside the call itself (a statement before the `run:` or a later stage, `-> { extend: { join_many: f is flights … } }`), names the run target, any named query, or any source from another file: the answer gives `flights`'s name to the stored result, so such a name could read the stored rows (skipped `MISSING_FIELD`). A later stage that joins a source declared in another file answers live; join it in the model instead. The caller's own definitions before the `run:` (a report's definition cells), a dimension-only `extend:` and a join to another source of the declaring file are served. The answer keeps the view's name, its columns in their order, its ordering and its render tags, and each column carries the annotations live's does (`#(classification)`, `#(doc)`, `# currency`, `# label`), colocated or `storage=`.
- **Which source it belongs to.** The source whose own statement puts `#@ persist` on the view: its `view:`, a redefinition after `except:`, an alias (`view: by_carrier2 is by_carrier`) carrying its own line, or an `include:` that re-annotates the view (`include { #@ persist public: by_carrier }`). A source that only inherits the view (an extension, a rename, an import, or a `rename:` of the view) does not persist it, and calls on it are answered live; annotate it there to store it there too.
- **Where it is stored.** Without `storage=`, in the source's own warehouse; with `storage=`, in that destination (needs `PERSIST_STORAGE_MODE=on`). The table is built from the source's own rows, never from a table the source itself is persisted to, and rebuilt whole.
- **Fails the publish and the load** (`persist-view-invalid`): an incremental key (`watermark=`, `merge_key=`), an `@env.` reference, an empty `storage=`, or a `name=` that ends the way Publisher's own names do. A `#@` line that does not parse is `persist-view-unparseable`, an unknown key is `persist-unknown-key`, and the retired `partition=` and `realization=` are `persist-retired-key`. A `#@` line spanning several lines is still refused alone (`persist-view-not-built`).
- **Not built, with a warning** (`persist-view-not-built`, severity `warn`, and the view is answered live): a view whose result reads a given (reason `given_in_persisted_query`, as for any build that reads one) or the current time (`current_time`): it differs per caller or per read; a view whose result nests, on either tier (reason `nested_result`: a nest's stored rows cannot be rebuilt exactly, so no read could use its table; persist a view over the same data without the nest instead); and, on a source with a row-level gate (`#(access_filter)`, or an `#(authorize)` that filters rows), a view that does not group by every field the gate reads, under its own name, in one grouping stage with no `limit:`, no `all()` or `exclude()` and no `calculate:`. That last one is a warning, not an error, because the gate may be added later by another author.
- **Gates.** A query on a gated source is answered from a persisted view only with `PUBLISHER_MATERIALIZE_GATED_SOURCES` on as well: the source's gates are written on the view's stored result and grafted and proven there exactly as on live, so each caller gets the groups the gate keeps. With the switch off, such a query is answered live, and the view over a gated source is not built on either tier (`persist-view-not-built`, reason `access_filter` for a row filter, `authorize` for a lock): it could not be read past the gate, so no table holds the gated groups. With the switch on, a colocated view over a gated source is built and read past the gate; a `storage=` view over one is refused either way. Publisher does not authenticate callers: the gate holds only behind the gateway that sets the givens from a verified identity ([operating model](security-posture.md#operating-model-a-microservice-behind-your-gateway)).
- **The path report.** A query that calls a persisted view lists its table first, as `kind: "view"`: `used` when it answered (`servedBy.kind` is `view`), else skipped with `NOT_BUILT`, `STALE`, `CONTRACT` or `MISSING_FIELD`. A query that calls no persisted view does not list one. A `storage=` view's answer reports `servedFrom: "storage"`, a colocated one's `servedFrom: "database"`.
- **The build plan** lists each persisted view as a generated source named `<source>__<view>__view`; its serve verdict reads `The persisted view \`<view>\` of \`<source>\``, reached through the source.

With `PUBLISHER_MATERIALIZE=false`, a persisted view is not built and is answered live, but its line is still read, as a persisted query's is: an invalid one fails the publish and the load as above, and a valid one carries `persist-view-not-built` (reason `materialization_off`).

### Persisted named queries (materialization v2, `PUBLISHER_MATERIALIZE`)

With `PUBLISHER_MATERIALIZE` on (the default), `#@ persist` on a top-level `query:` stores the query's result, and that table answers a query only when it calls the query exactly, or starts from it:

```malloy
source: flights is duckdb.table('flights') extend {
  measure: flight_count is count()
}

#@ persist
query: top_carriers is flights -> { group_by: carrier; aggregate: flight_count; order_by: flight_count desc; limit: 10 }
```

- **What it answers.** `run: top_carriers` in every spelling a caller sends: the REST or MCP `queryName`, a single-query report naming it, a report tile naming it, and `run: top_carriers` written by hand; and `run: top_carriers -> { … }`, whose later stages read the stored result. Not a refinement (`run: top_carriers + { where: … }`, skipped `MISSING_FIELD`: a `where:` and the query's `limit:` do not commute), nor any use of the query inside another definition (`source: s is top_carriers`, `query: q2 is top_carriers -> { … }`, a join to it), which are answered as they would be without the annotation; as for a persisted view, so is a query text that, anywhere outside the call, names the run target, any named query (the query itself included), or any source from another file (skipped `MISSING_FIELD`). A later stage that joins a source declared in another file answers live; join it in the model instead. The answer keeps the query's name, its columns in their order, each column's kind (an aggregate is still a measure) and annotations, its ordering and its render tags. Rows that tie on the query's `order_by:`, or come from a query or later stage with no `order_by:`, may come back in another order than live's, as two live runs may. A linear report is a notebook, which answers live, as for persisted views.
- **Where it is stored and what it is called.** Without `storage=`, in the source's own warehouse; with `storage=`, in that destination. The build plan lists it as a generated source named `<query>__query` (the `__query` marks it a query's, so a source of the same name never shares its table), and its default table name is that name with the package's suffix. `name=` is honoured as on any `#@ persist` line. It is rebuilt whole.
- **Fails the publish and the load** (`persist-query-invalid`): an incremental key (`watermark=`, `merge_key=`), an `@env.` reference, an empty `storage=`, or a `name=` that ends the way Publisher's own names do. A `#@` line that does not parse is `persist-query-unparseable`, an unknown key is `persist-unknown-key`, and the retired `partition=` and `realization=` are `persist-retired-key`. A `#@` line spanning several lines is still refused alone (`persist-query-not-built`).
- **Not built, with a warning** (`persist-query-not-built`, severity `warn`, and the query is answered live): a query whose result reads a given (reason `given_in_persisted_query`) or the current time (`current_time`), through its own stages or its source's `where:` (its result differs per caller or per read); a result that nests. A query whose result the variant cannot regroup row for row (the persisted view's rule: its last stage must group by at least one dimension and nest nothing) is built but answered live (skipped `MISSING_FIELD`): one that aggregates with no `group_by:`. Also not built: the governance cases below.
- **Gates, fail closed.** A query that reaches a row-level gate (`#(access_filter)`, or an `#(authorize)`) is never built, whatever `PUBLISHER_MATERIALIZE_GATED_SOURCES` says: its source's own gate, one it inherits from a source it extends, renames or derives from, one on a source it joins, and one an inline or imported run target reaches. It is answered live under the gate, and the query leg is never tried past an entry-point gate (the route counter counts it `leg="query"`, `skipped`, `CONTRACT`). To serve a gated result from a table, persist a view on the gated source, which follows the persisted-view rule. Publisher does not authenticate callers: the gate holds only behind the gateway that sets the givens from a verified identity ([operating model](security-posture.md#operating-model-a-microservice-behind-your-gateway)).
- **The path report.** A query that calls a persisted query lists its table first, as `kind: "query"`: `used` when it answered (`servedBy.kind` is `query`), else skipped with `NOT_BUILT`, `STALE`, `CONTRACT` or `MISSING_FIELD`. Its serve verdict reads `The persisted query \`<query>\``.

With `PUBLISHER_MATERIALIZE=false`, a persisted query is not built and is answered live (its line is still read: an invalid one fails the load), and the package carries `persist-query-not-built`, which names the workaround that works either way: persist a source over the query and query that source.

```malloy
#@ persist
source: top_carriers_result is top_carriers
```

A misspelled key on the query's `#@` line fails the load as on a source (`persist-unknown-key`). A persisted query that loads never refuses a build run.

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

**A rollup over a caller-scoped source is refused**, with the reason every mechanism gives the same case: `given_in_persisted_query` when its build reads the given, `given_where_over_unstored_field` for the source's own `where:` over a field the grain drops, and `inherited_where_with_given` for one it inherits. (0.14.0 reported every such rollup as `preaggregate_over_dynamic_source`, which is no longer produced.)
A rollup is stored pre-grouped, so the only term it can apply per caller is the source's own given-reading `where:` over grain dimensions, which [Pre-aggregation](preaggregation.md) serves; any other given its build reads would hold one caller's aggregate and serve it to everyone.
