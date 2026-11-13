<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Moving from v1 materialization to `#(materialize)` and `#(preaggregate)`

> **In development.** `#(materialize)` and `#(preaggregate)` are materialization v2's annotations.
> Publisher reads them only with `PUBLISHER_MATERIALIZE` on (`materialize.enabled`, default off),
> and they are still being built ([the plan](materialization-v2-plan.md)). With the switch off,
> Publisher ignores both and answers the source live: a `#(materialize)` source gets no warning and
> no serve verdict, and a `#(preaggregate)` line is an `annotation-near-miss` warning. A `#(materialize)`
> source is served only from a `storage=` destination today, which also needs
> `PERSIST_STORAGE_MODE=on`. Nothing on this page is required: every v1 annotation keeps working as
> [materialization.md](materialization.md) describes it, with the switch on or off.

This page is the recipe for moving a package from v1 materialization (`#@ persist`,
`#@ preaggregate`) to v2. It maps each v1 spelling to its v2 spelling, with before and after Malloy,
and says what changes, what stays the same, and how to check the result. Each section says whether
the v2 form works on `main` today. "Works" means it was checked against `main` with both switches
on: a package loaded, built into a DuckLake destination from a Postgres warehouse, and queried, with
each answer compared to live.

## At a glance

| v1 spelling | v2 spelling | On `main` today |
| --- | --- | --- |
| `#@ persist storage="lake"` on a source | [`#(materialize) storage="lake"`](#storage-build-into-a-destination-and-serve-from-it) | Works. Watch for [a base with measures, views or joins](#a-base-with-measures-views-or-joins) |
| `#@ persist name="t"` with no `storage=` (colocated) | [`#(materialize) name="t"`](#colocated-build-into-the-sources-own-warehouse) | Not built. Keep `#@ persist` |
| `#@ persist … refresh="incremental" watermark="col"` | [the same keys on `#(materialize)`](#incremental-refresh) | Works with `storage=` |
| `#@ preaggregate grain="a, b"` on a measure | [`#(preaggregate) grain=["a", "b"]`](#preaggregate), one line per grain | Works |
| `#@ persist` on a view | [unchanged](#persist-on-a-view) | Works; built only with the switch on |
| `#@ -persist` on a reader | [unchanged](#persist-opt-out) | Works |

## What is deprecated, and when

Moving to `#(materialize)` is optional. `#@ persist` itself is not deprecated: a v1 package keeps its
meaning, and `#@ persist` on a view stays the way to persist a view. `#@ -persist` is not deprecated
either: it is Malloy core's own opt-out, which Malloy's persistence documentation teaches, and
Publisher keeps honouring it under `#(materialize)` too ([below](#persist-opt-out)). One v1 form has
no future in v2:

| v1 form           | Why                                           | v2 form                            |
| ----------------- | --------------------------------------------- | ---------------------------------- |
| `#@ preaggregate` | Renamed onto the `#(…)` routes Publisher owns | [`#(preaggregate)`](#preaggregate) |

It follows the Publisher v1 plan's rule for a deprecated form: a package warning through 0.x,
refused at 1.0.0, and the reader removed in 1.1. Its warning, `deprecated-preaggregate-annotation`,
comes one per measure that declares `#@ preaggregate`, naming the source (`subject`) and the file
that declares it (`model`). Today it shows only with `PUBLISHER_MATERIALIZE` on, where
`#(preaggregate)` is read, built and served; it starts for everyone in the release that ships v2's
annotations. A source that extends or imports an annotated one is not reported again. The warning
does not change what the package builds or serves: `#@ preaggregate` keeps working, and its rollups
still build and serve.

## The recipe

1. **Run a server with both switches on**, somewhere other than production:
   `PUBLISHER_MATERIALIZE=true` and `PERSIST_STORAGE_MODE=on`, with the storage destination your
   `storage=` names configured ([storage destinations](connections.md#storage-destinations)). Both
   are read at startup. A server with the switch off ignores every v2 annotation, and says nothing
   about a `#(materialize)` it ignores, so check this first: the server's startup log lists
   `materialize.enabled=true from PUBLISHER_MATERIALIZE` among the settings it was given.
2. **Find the v1 annotations.** `grep -rn -e '#@ *persist' -e '#@ *preaggregate' --include='*.malloy' .`
   lists them. Leave `#@ -persist` lines and `#@ persist` on views as they are.
3. **Rewrite each one** by the sections below. Move a source at a time if the package is large: v1
   and v2 annotations can sit in one package, and in one file.
4. **Preview the publish.** `POST /api/v0/environments/{env}/packages?dryRun=true` with
   `{"name": "<package>", "location": "<path>"}` loads the tree, runs every publish check and
   discards it. The answer's `warnings` carry each v2 problem by code ([below](#warnings-to-expect)),
   and `serveVerdicts` lists every source the package would materialize. A source missing from
   `serveVerdicts` will not be built, and its warning says why. A malformed `#(materialize)` or
   `#(preaggregate)` (an unknown key, a line that does not parse) refuses the publish here.
5. **Build.** On a standalone server nothing builds on publish: run
   `malloy-pub materialize --environment <env> --package <pkg> --wait`, or
   `POST …/packages/{pkg}/materializations`. Expect every migrated source to build once, even with
   `name=` unchanged ([addresses](#addresses-what-moves-once)). An incremental source starts again
   from a full build.
6. **Read the serve verdicts.** `GET …/packages/{pkg}/status` reports `serveVerdicts`. Each migrated
   source should read `serves`. `live` with reason `served_copy` means its table is built but no
   query will read it; the package's `served-copy-not-served` warning names why.
7. **Read the path report.** Every query result carries `path`
   ([the path report](materialization.md#which-table-answered-a-query-the-path-report)). A
   `#(materialize)` answer reads `servedBy.kind: "materialized"` with `via: "served_copy"`, a
   `#(preaggregate)` answer reads `kind: "rollup"`, and a persisted view reads `kind: "view"`. A
   table that was tried and not used is in `considered` with its `reason` (and, for `WARNED`, a
   `code`).
8. **Compare served with live.** Add a scratch model file that declares one opted-out reader per
   migrated source, run the same query on the source and on its reader, and compare the rows:

   ```malloy
   import "orders.malloy"

   #@ -persist
   source: east_orders_live is east_orders extend {}
   ```

   `run: east_orders -> { group_by: category; aggregate: order_count }` should report
   `via: "served_copy"`, and the same query on `east_orders_live` should report
   `servedBy.kind: "live"` with the same rows. Declare the reader in a file: a `#@ -persist` source
   written in the query text itself is not opted out, and is answered from the table. Compare right
   after a full build, since an incremental table legitimately lags rows that arrive with an old
   watermark. Delete the scratch file when you are done.

### Warnings to expect

| Code | What it means | What to do |
| --- | --- | --- |
| `materialize-not-built` | Publisher cannot build this `#(materialize)` source; the message names why (colocated, a refused `inline=`, a name another source's table holds, and so on). It serves live. | Fix what it names, or keep the v1 form. |
| `materialize-unknown-key`, `materialize-unparseable` | The line cannot be read as written. Severity `error`: a publish is refused. | Fix the key or the quoting. |
| `annotation-near-miss` | A misspelled route, such as `#(materialise)`. Nothing is built. | Fix the spelling. |
| `served-copy-not-served` | The table is built, but the served copy cannot answer from it; the message names why. | See [a base with measures, views or joins](#a-base-with-measures-views-or-joins) and [known limitations](#known-limitations-you-may-hit). |
| `materialize-pair-full-refresh` | An incremental pair table rebuilds in full; the message names why. | Name a watermark field on each side of the join. |
| `persist-view-not-built` | A persisted view cannot be built. | See [persisted views](materialization.md#persisted-views-in-development-publisher_materialize). |
| `deprecated-preaggregate-annotation` | A measure still uses `#@ preaggregate`. | Rewrite it as `#(preaggregate)`. |

## What changes when you move

`#(materialize)` stores the source you put it on, as written: every dimension under its own name,
computed or not (its private and internal ones too), with the source's `where:` applied. v1 stores
whatever the `#@ persist` source's own query produces. The served copy then reads that table under
the source's own name, so its extensions, views, joins, named queries and importing files are
unchanged ([the served copy](materialization.md#the-served-copy-in-development-publisher_materialize)).

- **No `##! experimental.persistence` in your file** for a `#(materialize)` source. The flag lives
  in the build source Publisher generates. A file that also has v1 `#@ persist` sources still needs
  it: a v1 file without the flag is skipped by the build, as before. Removing that requirement for
  v1 files (D3) is part of the v1 cutover, which has not landed.
- **It is served by the served copy, never the build manifest.** A colocated v1 table is served
  through Malloy's build manifest, and a v1 `storage=` table through the storage serve shape (or,
  with the switch on, through the served copy when the source is query-shaped). A `#(materialize)`
  table is served only through the served copy, which is why a colocated one is not built yet.
- **An extension does not inherit `#(materialize)`.** Only the source whose own statement carries it
  is built. A source that extends or renames it is not a second table: it reads the materialized
  source's table through the served copy, as its own text says. To give an extension a table of its
  own, put `#(materialize)` on it too
  ([extension tables](materialization.md#the-served-copy-in-development-publisher_materialize)).
- **One annotation per source.** A source that carries both `#(materialize)` and its own
  `#@ persist` is not built, and is reported as `materialize-not-built`.
- **A declaration Publisher cannot build serves live, with a warning.** It is not refused at
  publish. The warning is `materialize-not-built`, and it names the reason. The exception is a
  declaration that cannot be read as written (below): it is not built either, and a publish is
  refused.
- **`#(materialize)` takes only these keys:** `name`, `storage`, `refresh`, `watermark`,
  `merge_key`, `partition`, `freshness.*`, `queryMetadata.*`, and `inline` (v2 only;
  [joins](#joins-reference-by-default-inline-to-store-them)). Any other key, a misspelled `name=`
  for example, is malformed rather than a default table: the source is not built, the package
  warning is `materialize-unknown-key` at severity `error`, naming the key it was likely meant to
  be, and a publish is refused. A package already published keeps loading with the warning. A
  `#(materialize)` line that does not parse (`name="t` with no closing quote) is malformed the same
  way, as `materialize-unparseable`. v1's `realization=` has no v2 form. A misspelled route, such as
  `#(materialise)`, is an `annotation-near-miss` warning.

## What stays the same

- **Queries.** Callers name the same sources, fields and views, and get the same rows.
- **Building and refreshing.** The build plan, the manifest, `malloy-pub materialize`, `--reseed`,
  `--force-refresh`, the standalone scheduler and the package's `materialization` block in
  `publisher.json` (`schedule`, `freshness`, `scope`) work for `#(materialize)` sources as for v1
  ones. A `#(materialize)` source appears in the plan, the manifest, warnings and the path report
  under your source name; the generated build source never does.
- **Names.** `name=` follows the v1 rules: a plain identifier path, which may name a container
  (`name="analytics.order_summary"`). Keep the same `name=` when you move, and the first build after
  the move replaces the old table under the same name. Two sources may not resolve to one table.
- **Freshness, `partition=` and `queryMetadata`** are copied onto the generated build source as
  written.

## Form by form

### `storage=`: build into a destination and serve from it

**Works**, with `PERSIST_STORAGE_MODE=on`. When a query is answered from the table, the path
report's `servedBy` carries `via: "served_copy"`.

```malloy
// v1
##! experimental.persistence
#@ persist name="east_orders" storage="lake"
source: east_orders is orders -> { select: * } extend {
  where: region = 'east'
}

// v2
#(materialize) name="east_orders" storage="lake"
source: east_orders is orders extend {
  where: region = 'east'
}
```

`name=` is optional with `storage=`. Without one, the table takes the source's name. The destination
is Publisher's, and the existing checks refuse two sources that land on one table.

A v1 `#@ persist` on a source declared only by refinement (`source: x is orders extend { … }`)
stores its root table's rows. `#(materialize)` on the same source stores the source's own rows, with
its `where:` already applied.

<a id="a-base-with-measures-views-or-joins"></a>

### A base with measures, views or joins

The v2 example above serves from its table only when `orders` declares no measure, view or join. If
it does, `east_orders` inherits fields its stored table does not carry, so the served copy leaves it
as written: the table is built, the serve verdict reads `live` with reason `served_copy`, the package
warns `served-copy-not-served` ("This source inherits fields … that its stored table does not
carry"), and every query is answered live. This is the trap a migration hits most, because a v1
query-shaped source (`orders -> { select: * }`) never inherited its base's measures in the first
place. Either of these serves it:

```malloy
// Keep the query shape: its output carries no measures,
// so declare the ones you need after it.
#(materialize) name="east_orders" storage="lake"
source: east_orders is orders -> { select: * } extend {
  where: region = 'east'
  measure: order_count is count()
}

// Or build over a base that declares no measures, views or joins.
source: orders_base is pg.table('public.orders') extend {
  primary_key: id
}

#(materialize) name="east_orders_b" storage="lake"
source: east_orders_b is orders_base extend {
  where: region = 'east'
  measure: order_count is count()
}
```

Or materialize the base as well: an extension of a `#(materialize)` source keeps its parent's
measures, views and joins, and has a table of its own
([extension tables](materialization.md#the-served-copy-in-development-publisher_materialize)).

### Colocated: build into the source's own warehouse

**Not built yet. Keep `#@ persist`.** A `#(materialize)` source without `storage=` is not built
until the served copy can serve a table on the source's own connection, which is part of phase 3's
v1 cutover (plan row [18](materialization-v2-plan.md#known-limitations-to-revisit)). Until then it
gets a `materialize-not-built` warning ("colocated #(materialize) is not served yet; use storage= or
#@ persist") and its queries are answered live from its warehouse. Keep the v1 form, or use
`storage=`, for anything that needs a table now.

The v2 spelling, for when it lands, needs `name=`:

```malloy
// v1
##! experimental.persistence
#@ persist name="order_summary"
source: order_summary is orders -> {
  group_by: category
  aggregate: total_revenue is amount.sum()
}

// v2: not built on main yet
#(materialize) name="order_summary"
source: order_summary is orders -> {
  group_by: category
  aggregate: total_revenue is amount.sum()
}
```

A colocated `#(materialize)` must name its table. Its default would be the source's own name, which
for a source over `conn.table('orders')` is often the very table it reads.

### A query-shaped source

**Works** with `storage=`. v2 stores the query's output rows, plus any dimension that the `extend`
after the query adds.

```malloy
// v1: stores region and total; big is computed on every read
#@ persist name="region_totals" storage="lake"
source: region_totals is orders -> {
  group_by: region
  aggregate: total is amount.sum()
} extend {
  dimension: big is total > 20
}

// v2: stores region, total and big
#(materialize) name="region_totals" storage="lake"
source: region_totals is orders -> {
  group_by: region
  aggregate: total is amount.sum()
} extend {
  dimension: big is total > 20
}
```

You do not have to move a query-shaped v1 source. With the switch on, a query-shaped `#@ persist
storage=` source is also served through the served copy, as written.

<a id="incremental-refresh"></a>

### `refresh="incremental"`, `watermark=`, `merge_key=`

**Works** with `storage=`. The keys are copied onto the generated build source and checked by the
[same rules](materialization.md#incremental-refresh), including the dialect rule: the origin must be
Postgres, BigQuery or Snowflake, so a DuckDB source must say `refresh="full"`. A
[rolling window](#a-rolling-window-a-current-time-where) adds a delete below the moving cut-off to
each incremental refresh.

```malloy
// v1
##! experimental.persistence
#@ persist name="daily_orders" storage="lake" refresh="incremental" watermark="order_date"
source: daily_orders is orders -> {
  group_by: order_date
  aggregate: revenue is amount.sum()
}

// v2
#(materialize) name="daily_orders" storage="lake" refresh="incremental" watermark="order_date"
source: daily_orders is orders -> {
  group_by: order_date
  aggregate: revenue is amount.sum()
}
```

The first build after the move starts the table again from a full build. The address moved (see
[Addresses](#addresses-what-moves-once)), so the recorded boundary no longer applies. A colocated
incremental source stays on `#@ persist` for now, as every colocated source does.

v2 adds one form v1 has no equivalent for: a source that inlines a join_many (a pair table) names a
watermark on each side of the join as a list, `watermark=["updated_at", "lines.updated_at"]`, and
each refresh replaces whole parents ([joins](#joins-reference-by-default-inline-to-store-them)).

### Freshness, `partition=` and `queryMetadata`

**Works.** Each is copied onto the generated build source as written. A per-source freshness window
appears in the build plan, and the served copy drops a stale table per query, as v1 does.

```malloy
// v1
#@ persist name="orders_all" storage="lake" freshness.window="24h" partition="org_id" queryMetadata.team="ops"

// v2
#(materialize) name="orders_all" storage="lake" freshness.window="24h" partition="org_id" queryMetadata.team="ops"
```

### A `where:` that reads a given

**Works**, with `storage=`. The given-reading term is left out of the build and applied at read
with each caller's value, as a v1 `#@ persist storage=` source does when the term is in its
`extend`. v2 needs no query shape for it.

```malloy
// v1
##! experimental { persistence givens }
given: ORG_ID :: number is 1
#@ persist name="scoped" storage="lake"
source: scoped is orders -> { select: * } extend { where: org_id = $ORG_ID }

// v2
##! experimental { givens }
given: ORG_ID :: number is 1
#(materialize) storage="lake"
source: scoped is orders extend { where: org_id = $ORG_ID }
```

The gates v1 applies to a given still apply, because the generated build source is judged like any
`#@ persist` source: a given in a declared `dimension:` or `measure:`, or in a join's `on:`, is
refused, and the source serves live (plan row [5](materialization-v2-plan.md#known-limitations-to-revisit)).

### A rolling window: a current-time `where:`

**Works**, v2 only. A `where:` that keeps the last stretch of time (`ts >= now - 30 days`, on a timestamp) builds as
of the build and is re-applied at every read at the read's own clock. A window that keeps rows older
than a moving cut-off (`ts < now - 30 days`), and other shapes that read `now`, leave the source
unbuilt, and it serves live (plan row [6](materialization-v2-plan.md#known-limitations-to-revisit)).
The rules are in
[materialization.md](materialization.md#the-served-copy-in-development-publisher_materialize).

```malloy
#(materialize) storage="lake" refresh="full"
source: recent_orders is orders extend {
  where: updated_at >= now - 30 days
}
```

### Joins: reference by default, `inline=` to store them

**Works**, v2 only. A `#(materialize)` table holds the source's own rows, and its joins read the
joined source at query time, from that source's table when it is materialized in the same
destination. A joined source that is not stored in the destination is left out of the served copy,
so a query that reaches it is answered live (`CROSS_CONNECTION`); inline the join, or materialize
the joined source into the same destination.

`inline=["j"]` on a join_one stores what reads through `j` as `j__<field>` columns, after checking
the join key is unique, and the served copy answers `j.<field>` from those columns with no join at
read. On a join_many it stores a pair table, one row per row of live's LEFT JOIN, and the served
copy answers child and mixed queries from it; a query that reads no field of the join skips it
(`COUNT_SEMANTICS`).

```malloy
#(materialize) storage="lake" inline=["regions"]
source: orders_wide is orders extend {
  join_one: regions with region
  measure: order_count is count()
}

#(materialize) storage="lake" inline=["lines"] refresh="incremental" watermark=["updated_at", "lines.updated_at"]
source: orders_lines is orders extend {
  join_many: lines on lines.order_id = id
  measure: order_count is count()
}
```

v1 has no equivalent of `inline=`, so there is nothing to migrate here; the limits are in
[materialization.md](materialization.md#the-served-copy-in-development-publisher_materialize) and
the [known limitations](#known-limitations-you-may-hit) below.

### A source with a row-level gate

**Behind its own switch, off by default.** With `PUBLISHER_MATERIALIZE_GATED_SOURCES` on as well, a
`#(materialize) storage=` source whose own `#(authorize)` or `#(access_filter)` compiles to a row
filter is built and served from the served copy with the gate grafted on. Off, it is refused as a
gated `#@ persist storage=` source is, and serves live. It stays off until a security review of
storing gated sources' columns in the managed tier. A gate holds only behind the gateway that sets
the givens from a verified identity
([operating model](security-posture.md#operating-model-a-microservice-behind-your-gateway)).

<a id="persist-on-a-view"></a>

### `#@ persist` on a view

**No rewrite.** `#@ persist` on a view is the v2 spelling too. With `PUBLISHER_MATERIALIZE` on, the
view's result is built and answers a query only when the query calls the view exactly
(`run: sales -> by_category`) or starts from it; the path report reads `servedBy.kind: "view"`.
With the switch off, Publisher does not build it.

```malloy
source: sales is orders extend {
  measure:
    order_count is count()
    revenue is amount.sum()
  #@ persist name="sales_by_category"
  view: by_category is { group_by: category; aggregate: revenue, order_count }
}
```

A refinement (`run: sales -> by_category + { limit: 2 }`) is answered live and reports the view's
table skipped with `MISSING_FIELD`. The rules are in
[persisted views](materialization.md#persisted-views-in-development-publisher_materialize).

<a id="persist-opt-out"></a>

### `#@ -persist`

**Supported, with no rewrite.** `#@ -persist` is Malloy core's opt-out: on a reader of a persisted
source, it makes the reader recompute rather than read the table. Keep it when you move the source
it reads to `#(materialize)`: a materialized source serves every reader except one that opts out
this way. The opted-out reader, and any source built on it, recomputes live, and the served copy
declines it (its path report's skip carries the code `persist-opt-out`). It is also the
served-versus-live check of [step 8](#the-recipe).

```malloy
// v1
#@ persist name="east_orders" storage="lake"
source: east_orders is orders -> { select: * } extend { where: region = 'east' }

#@ -persist
source: east_fresh is east_orders extend { dimension: region_upper is upper(region) }

// v2: only the persisted source's annotation changes
#(materialize) name="east_orders" storage="lake"
source: east_orders is orders -> { select: * } extend { where: region = 'east' }

#@ -persist
source: east_fresh is east_orders extend { dimension: region_upper is upper(region) }
```

<a id="preaggregate"></a>

### `#@ preaggregate`

**Works, and deprecated.** With `PUBLISHER_MATERIALIZE` on, write `#(preaggregate)` on the measure;
`#@ preaggregate` keeps working, with a deprecation warning while the switch is on. The grain is a
list, and each line is still one rollup, so a measure with two grains keeps two lines:

```malloy
// v1
source: sales is orders extend {
  #@ preaggregate grain="category"
  #@ preaggregate grain="category, region"
  measure: revenue is amount.sum()
}

// v2
source: sales is orders extend {
  #(preaggregate) grain=["category"]
  #(preaggregate) grain=["category", "region"]
  measure: revenue is amount.sum()
}
```

`namespace=` and `storage=` carry over unchanged (one or the other, never both). A query the rollup
answers reports `servedBy.kind: "rollup"`. What differs from `#@ preaggregate`:

- **Keys are closed.** `grain=[…]` is required. Any other key, a grain that is not a list of the
  source's own dimensions, or the annotation on anything but a measure refuses a publish.
- **Measures are read from their text.** Sums, counts, mins and maxes, filtered or not, and
  expressions of them. `avg`, a distinct count, `stddev`, `all()` and `exclude()` refuse a publish;
  text Publisher does not read with certainty is a warning, and the measure serves live.
- **It belongs to the source that writes it.** An extension or an import does not repeat it.
- **It answers more.** A `#(preaggregate)` rollup answers named views, extensions of its source,
  queries that reach its source through a join (colocated rollups only), and join-level
  `max`/`min`, and it builds over a `#@ persist storage=` base, which a v1 rollup does not.
- **A new table.** A v2 rollup's table is not the v1 rollup's, so it builds once after the move.

[preaggregation.md](preaggregation.md#preaggregate-materialization-v2-behind-publisher_materialize)
has the full list.

## What is not migrated

- **Colocated `#@ persist`** (no `storage=`), incremental or not: keep it until colocated
  `#(materialize)` is served (row [18](materialization-v2-plan.md#known-limitations-to-revisit)).
  Its file keeps `##! experimental.persistence`.
- **`#@ persist` on a view** and **`#@ -persist`**: these are the v2 spellings already.
- **`realization=`** has no v2 form; on `#(materialize)` it is `materialize-unknown-key`.
- **A gated source** with `storage=`: refused unless `PUBLISHER_MATERIALIZE_GATED_SOURCES` is on,
  as v1 refuses it.
- **An incremental source on a DuckDB origin**: refused, as in v1. Say `refresh="full"`.

## Known limitations you may hit

Each of these answers live, costs extra, or loses metadata; none serves a wrong answer. Row numbers
are in the plan's [known limitations](materialization-v2-plan.md#known-limitations-to-revisit).

| What you wrote | What happens | Plan |
| --- | --- | --- |
| `#(materialize)` over a base that declares a measure, view or join | Built, never read: `served-copy-not-served`, answered live. See [above](#a-base-with-measures-views-or-joins). | The served copy's call 2 |
| `#(materialize)` with no `storage=` | Not built: `materialize-not-built` | Row 18 |
| An extension table whose parent is in another file | Served, but built from the warehouse rather than over the parent's table, so its build scans the warehouse | P10 (same-file only) |
| `inline=` naming a `private` or `internal` join_many | Not built: `materialize-not-built` | Row 24 |
| `inline=` on a second join_many, a join_many more than one hop away, or a joined source with joins of its own | Not built: `materialize-not-built` | Row 9 |
| `inline=` on a join_cross | Not built: `materialize-not-built` | — |
| A dimension, or an inherited `where:` term, that reads a given | The source is refused and serves live | Row 5 |
| An upper-bound rolling window (`ts < now - 30 days`) | Not built; serves live | Row 6 |
| A query with its own `timezone:` over a stored truncation or rolling window | Answered live | Row 7 |
| A hidden stored column beside your own `rename:` of a stored column, or a pair table's renames | Answered live (in the second case, the whole model's served copy) | Row 25 |
| A query on a gated entry point, over a rollup | Answered live (`CONTRACT`); a rollup over a gated base is not built | Row 1 |
| A query through a join to a joined source's `#(preaggregate) storage=` rollup | Answered live (`CROSS_CONNECTION`) | Join-to-rollup (A7) |
| A `count()` through a join_many, against a joined source's rollup | Answered live (`COUNT_SEMANTICS`) until Publisher's Malloy carries the fix | Question 12 |
| A `storage=` number column | Answers equal live; the integer or decimal subtype reads back as `number` | Row 12 |
| An annotated field of an inlined join returned as a column | Answered live, so the result keeps the field's annotations | Row 10 |

## Addresses: what moves once

A table's content address (`sourceEntityId`) is a hash of its connection and the SQL that builds
it. The build plan, the manifest and reuse all key on it.

- **Moving a source to `#(materialize)` gives it a new address.** The build query is now the
  generated one (`src -> { select: … }`), not the v1 source's own SQL, even when the rows are the
  same. The first run after the move misses reuse and builds the table. An incremental source starts
  from a full build. Keep `name=` and the table is replaced in place.
- **P12, the storage destination in the address, is a separate move.** With
  `PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION` on, every `storage=` table is re-addressed once
  ([configuration](configuration.md)). P2, which leaves a given-reading `where:` out of the build,
  ships with it and moves no existing address. Turn the switch on in the same rollout as your
  migration, so each `storage=` table rebuilds once and not twice. Make the first run after it a full
  one.

## What a control plane must do

- **Build plan and wire names.** A `#(materialize)` source appears under the author's source name
  and model path, and its `sourceID` is the author's. The generated build source (`<src>__mat`) and
  its companion model never reach the plan, a run's `sourceNames`, the manifest, warnings or the path
  report. Anchors keyed on (version, source name, model path) stay put.
- **Expect one rebuild per migrated source.** Its `sourceEntityId` changes, so reuse misses and an
  incremental source re-seeds. A table Publisher names itself (`name=`, or the source's name) is
  rebuilt in place. A table the host instructed under a generational name is the host's to reclaim.
- **Set the switch the same on every worker.** `PUBLISHER_MATERIALIZE` and
  `PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION` are read at startup. Workers that disagree plan
  different sources or compute different addresses for one source.
- **Read warning codes as open strings.** This page names `deprecated-preaggregate-annotation`
  (with the switch on), `materialize-not-built`, `materialize-unknown-key`,
  `materialize-unparseable`, `materialize-pair-full-refresh`, `served-copy-not-served` and
  `persist-view-not-built`, beside `materialization-destination-missing` and
  `materialization-connection-cannot-hold-table`. Only the two malformed codes are errors.
- **A rolling window deletes as well as appends.** An incremental refresh of a source with a
  rolling window deletes the rows below the new cut-off in the same transaction.
- **A `name=` view is a warehouse object Publisher marks.** When a run names a `#(materialize)`
  table by generation, Publisher also makes a view at `name=`, carrying the comment
  `malloy-publisher:name-view …`; a sweeper, a table dropper and package delete must recognize it
  ([the `name=` view](materialization.md#the-name-view-in-development-publisher_materialize)).
  Generations and promote arrive in phase 5. See
  [the plan's control-plane contract](materialization-v2-plan.md#ownership).
